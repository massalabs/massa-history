//! AWS DynamoDB one-shot importer.
//!
//! Walks every `(period, thread)` from `head` (the indexer's
//! `last_final_slot.period` at startup, capped by
//! `cfg.max_period` if set) down to `(0, 0)`, asks DDB for the slot's
//! contents, and ships the result via `Event::LegacyPatch`. Exits when
//! it reaches genesis.
//!
//! ### Idempotency
//!
//! The worker never re-queries DDB for slots the local indexer has
//! already settled (FINAL + block_body_stored). That's the *resume*
//! contract: if the operator stops the indexer mid-run, restarting
//! with `enabled = true` picks up exactly where the previous attempt
//! left off (modulo a few slots that may need to be re-tested at the
//! frontier). It also means the importer becomes a no-op as soon as
//! the cluster's normal peer-sync has fully covered legacy slots, so
//! leaving the toggle on doesn't silently rack up AWS charges.
//!
//! ### Non-blocking by construction
//!
//! Runs in its own tokio task. The only shared resource is the ingest
//! `EventTx` channel — exactly the same channel the live-stream and
//! peer workers use. The ingest worker drains `Event::LegacyPatch`
//! through [`crate::peer::patch::apply_legacy_patch`], which is
//! lowest-precedence by construction:
//!
//!   * never overwrites a richer local row,
//!   * never overrides an existing `execution_trail_hash`,
//!   * leaves the `exec_output_final` / `transfers_stored` flags
//!     unset so subsequent peer / live patches can still ship the
//!     parts legacy doesn't carry.
//!
//! That means the live indexer keeps making forward progress on the
//! head while this worker fills the historical tail.
//!
//! ### Transient DDB errors
//!
//! An AWS/`ProvisionedThroughput` blip is **not** "legacy had no data".
//! Failed lookups leave the slot uncovered and are retried in-process
//! (backoff) and, if any remain at end of pass, via a full re-sweep.
//! Only a confirmed `Ok(None)` from DDB is recorded as a FINAL miss.
//! Do **not** disable `[legacy_ddb]` until a pass completes with
//! `errored = 0`.

use crate::{
    db::Db,
    ingest::{Event, EventTx},
    legacy::source::LegacySource,
    metrics::Metrics,
    model::{BlockStatus, SlotState, SlotStatus},
    proto::indexer::v1::FinalSlotResponse,
};
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Attempts per slot before counting a hard error for the pass.
const FETCH_ATTEMPTS: u32 = 8;

/// Tunables for the one-shot importer.
#[derive(Debug, Clone)]
pub struct OneShotConfig {
    /// Inclusive upper bound on the period the importer starts at.
    /// `None` ⇒ start at the indexer's current `last_final_slot.period`.
    pub max_period: Option<u64>,
    /// Inclusive lower bound. The importer stops after processing
    /// `period == min_period`. `None` ⇒ walk all the way down to
    /// `(0, 0)`.
    ///
    /// Used to do a **bounded re-import** of a specific historical
    /// range — e.g. when a transient DDB outage caused a contiguous
    /// batch of slots to be skipped during the first run.
    pub min_period: Option<u64>,
    /// Pause inserted before each per-slot DDB lookup. Acts as a crude
    /// RPS cap. With `concurrency > 1` this throttles each in-flight
    /// fetch independently, so the effective steady-state RPS is
    /// roughly `concurrency / (rate_limit + ddb_latency)`. Set to zero
    /// to let concurrency alone govern the rate (the fast-finish mode).
    pub rate_limit: Duration,
    /// Number of threads per period. Always 32 in mainnet; configurable
    /// for tests.
    pub thread_count: u8,
    /// How many per-slot DDB lookups may be in flight at once. `1`
    /// reproduces the original strictly-sequential walk; higher values
    /// fan out across the (latency-bound) DDB round-trips so a full
    /// genesis sweep finishes in hours rather than months. Slots that
    /// are skipped locally (already covered, see `slot_already_covered`)
    /// never occupy a concurrency slot beyond the cheap RocksDB read.
    pub concurrency: usize,
}

impl Default for OneShotConfig {
    fn default() -> Self {
        Self {
            max_period: None,
            min_period: None,
            rate_limit: Duration::from_millis(50),
            thread_count: 32,
            concurrency: 1,
        }
    }
}

/// Outcome of attempting to settle one `(period, thread)` slot. Carried
/// back from the concurrent fetch futures to the single consumer loop so
/// tallying + channel sends stay single-threaded (no shared atomics).
enum FetchKind {
    /// Legacy had data; ship the patch.
    Filled(Box<crate::proto::indexer::v1::FinalSlotResponse>),
    /// Legacy genuinely had no row for this slot (miss / out of window).
    Empty,
    /// A higher-precedence source already covered the slot — no DDB call
    /// was made.
    Skipped,
    /// The DDB lookup errored; the string is the formatted error.
    Error(String),
}

struct SlotOutcome {
    period: u64,
    thread: u8,
    kind: FetchKind,
}

/// Run the importer to completion. Returns when:
///   * a full pass finishes with **zero** hard DDB errors, OR
///   * the ingest channel closes (indexer shutdown).
///
/// Transient AWS failures are retried per-slot with backoff. Any slot
/// that still fails after that is left **uncovered** (never written as
/// a miss) and the whole window is re-swept until those gaps are filled
/// or confirmed empty. Abandoning errored slots would silently drop
/// history that legacy still holds.
pub async fn run_oneshot_import(
    db: Db,
    source: Arc<dyn LegacySource>,
    tx: EventTx,
    cfg: OneShotConfig,
    metrics: Option<Arc<Metrics>>,
) {
    let concurrency = cfg.concurrency.max(1);
    info!(
        max_period = ?cfg.max_period,
        min_period = ?cfg.min_period,
        rate_limit_ms = cfg.rate_limit.as_millis() as u64,
        thread_count = cfg.thread_count,
        concurrency,
        fetch_attempts = FETCH_ATTEMPTS,
        "AWS one-shot importer starting"
    );

    // Determine the starting period. If the operator capped it, use
    // the cap. Otherwise wait until the indexer has its first FINAL
    // slot, so we don't try to import garbage before the live stream
    // has settled.
    let head_period = match cfg.max_period {
        Some(p) => p,
        None => match wait_for_head(&db, &tx).await {
            Some(p) => p,
            None => {
                info!("AWS one-shot importer: shutdown before head was known");
                return;
            }
        },
    };

    let floor = cfg.min_period.unwrap_or(0);
    let mut pass: u32 = 0;

    loop {
        if tx.is_closed() {
            return;
        }
        pass += 1;
        info!(
            pass,
            start_period = head_period,
            floor,
            "AWS one-shot importer: walking down"
        );

        let errored = run_oneshot_pass(
            &db,
            &source,
            &tx,
            &cfg,
            metrics.as_ref(),
            head_period,
            floor,
            concurrency,
            pass,
        )
        .await;

        if tx.is_closed() {
            return;
        }
        if errored == 0 {
            info!(
                pass,
                "AWS one-shot importer COMPLETE (errored=0) — safe to disable [legacy_ddb] and restart to stop hitting AWS"
            );
            return;
        }
        warn!(
            pass,
            errored,
            "AWS one-shot: pass finished with hard errors — uncovered slots will be retried after pause"
        );
        sleep(Duration::from_secs(30)).await;
    }
}

/// One descending sweep over `[floor, head_period]`. Returns the number
/// of slots that still failed after per-slot retries (left uncovered).
async fn run_oneshot_pass(
    db: &Db,
    source: &Arc<dyn LegacySource>,
    tx: &EventTx,
    cfg: &OneShotConfig,
    metrics: Option<&Arc<Metrics>>,
    head_period: u64,
    floor: u64,
    concurrency: usize,
    pass: u32,
) -> u64 {
    // Descending (period, thread) cursor across the whole window. The
    // iterator is lazy, so the 100M+ slot space costs no memory — the
    // `buffer_unordered` adapter pulls items only as concurrency frees
    // up.
    let thread_count = cfg.thread_count;
    let slot_seq = (floor..=head_period)
        .rev()
        .flat_map(move |p| (0..thread_count).map(move |t| (p, t)));

    let mut buffered = futures::stream::iter(slot_seq)
        .map(|(period, thread)| {
            let source = source.clone();
            let db = db.clone();
            let rate = cfg.rate_limit;
            async move {
                // Cheap local check first: a slot a higher-precedence
                // source (live / peer) OR a prior legacy run already
                // settled never costs a DDB read. We treat any FINAL
                // slot that already has a block body (or transfers, or
                // is a miss) as covered — see `slot_already_covered`.
                // This is what makes a restart resume cheaply instead
                // of re-billing every slot done so far.
                if let Ok(Some(s)) = db.read_slot(period, thread) {
                    if slot_already_covered(&s) {
                        return SlotOutcome {
                            period,
                            thread,
                            kind: FetchKind::Skipped,
                        };
                    }
                }
                if rate > Duration::ZERO {
                    sleep(rate).await;
                }
                let kind = fetch_slot_resilient(source.as_ref(), period, thread).await;
                SlotOutcome {
                    period,
                    thread,
                    kind,
                }
            }
        })
        .buffer_unordered(concurrency);

    let mut imported = 0u64;
    let mut skipped = 0u64;
    let mut errored = 0u64;
    let mut last_log = tokio::time::Instant::now();

    while let Some(outcome) = buffered.next().await {
        if tx.is_closed() {
            debug!("AWS one-shot: ingest channel closed");
            break;
        }
        match outcome.kind {
            FetchKind::Skipped => {
                skipped += 1;
            }
            FetchKind::Empty => {
                // Confirmed no row in DDB. Record a FINAL miss so we never
                // re-bill AWS and peers can serve `final_known=true`.
                // Never do this on Error — that would hide real history.
                if let Some(m) = metrics {
                    m.legacy_ddb_rpcs_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let miss = FinalSlotResponse {
                    period: outcome.period,
                    thread: u32::from(outcome.thread),
                    final_known: true,
                    is_miss: true,
                    ..Default::default()
                };
                if tx
                    .send(Event::LegacyPatch(Box::new(miss)))
                    .await
                    .is_err()
                {
                    debug!("AWS one-shot: ingest channel closed");
                    break;
                }
                imported += 1;
            }
            FetchKind::Error(e) => {
                errored += 1;
                if let Some(m) = metrics {
                    m.legacy_ddb_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    m.legacy_ddb_rpcs_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    error = %e,
                    period = outcome.period,
                    thread = outcome.thread,
                    pass,
                    "AWS one-shot: fetch still errored after retries — leaving uncovered"
                );
            }
            FetchKind::Filled(resp) => {
                if let Some(m) = metrics {
                    m.legacy_ddb_rpcs_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    m.legacy_ddb_slots_filled_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if tx.send(Event::LegacyPatch(resp)).await.is_err() {
                    debug!("AWS one-shot: ingest channel closed");
                    break;
                }
                imported += 1;
            }
        }

        // Periodic progress. With unordered concurrency the periods
        // complete roughly — but not strictly — in descending order,
        // so `current_period` is an approximate frontier indicator.
        if last_log.elapsed() >= Duration::from_secs(10) {
            info!(
                pass,
                imported,
                skipped,
                errored,
                current_period = outcome.period,
                "AWS one-shot: progress"
            );
            last_log = tokio::time::Instant::now();
        }
    }

    info!(pass, imported, skipped, errored, "AWS one-shot: pass finished");
    errored
}

/// Ask DDB for a slot, retrying transient errors with exponential backoff.
/// `Ok(None)` (confirmed empty) and `Ok(Some)` return immediately.
async fn fetch_slot_resilient(
    source: &dyn LegacySource,
    period: u64,
    thread: u8,
) -> FetchKind {
    let mut last_err = String::new();
    for attempt in 0..FETCH_ATTEMPTS {
        match source.fetch_slot(period, thread).await {
            Ok(Some(fetch)) => return FetchKind::Filled(Box::new(fetch.resp)),
            Ok(None) => return FetchKind::Empty,
            Err(e) => {
                last_err = e.to_string();
                if attempt + 1 >= FETCH_ATTEMPTS {
                    break;
                }
                let backoff =
                    Duration::from_millis(200u64.saturating_mul(2u64.pow(attempt.min(6))));
                warn!(
                    error = %last_err,
                    period,
                    thread,
                    attempt = attempt + 1,
                    attempts = FETCH_ATTEMPTS,
                    backoff_ms = backoff.as_millis() as u64,
                    "AWS one-shot: fetch errored, retrying"
                );
                sleep(backoff).await;
            }
        }
    }
    FetchKind::Error(last_err)
}

/// Block until the indexer has its first FINAL slot, or the ingest
/// channel closes. Returns `Some(period)` once a head exists, `None`
/// on shutdown.
async fn wait_for_head(db: &Db, tx: &EventTx) -> Option<u64> {
    loop {
        if tx.is_closed() {
            return None;
        }
        match db.read_last_final_slot() {
            Ok(Some(s)) => return Some(s.period),
            Ok(None) => {}
            Err(e) => {
                warn!(error = %e, "AWS one-shot: read_last_final_slot");
            }
        }
        sleep(Duration::from_secs(5)).await;
    }
}

/// True iff the local row is "good enough" to skip the AWS query.
///
/// Definition: any FINAL slot that is a miss, OR already carries a
/// block body (`block_body_stored`), OR already carries transfers
/// (`transfers_stored`).
///
/// ### Why `block_body_stored` counts as covered
///
/// The legacy fetch is **atomic per slot**: a single `fetch_slot`
/// pulls the block, its operations, and every sub-transfer row in one
/// shot (see `legacy::source::fetch_slot`), then ships them together in
/// one `LegacyPatch`. So once a slot has a block body locally, it has
/// already been fully legacy-imported (block + ops + transfers) — there
/// is nothing more AWS can add. Re-querying it on a restart would just
/// re-bill the same reads.
///
/// This is the predicate that makes the importer **cheaply resumable**:
/// after a restart it skips, with only a local RocksDB read, every slot
/// any prior run (or live ingest, or a peer) already settled, and
/// resumes issuing DDB queries at the frontier where it left off — the
/// "don't re-query already loaded slots" guarantee.
///
/// (The earlier, stricter predicate keyed only on `transfers_stored`
/// because a long-retired importer version shipped blocks without
/// sub-transfers; that version no longer runs against this database, so
/// `block_body_stored` is now a sound "fully imported" signal. A
/// targeted re-import of a suspect range is still possible via the
/// `min_period` / `max_period` window if ever needed.)
fn slot_already_covered(s: &SlotState) -> bool {
    if s.status != SlotStatus::Final {
        return false;
    }
    if s.is_miss {
        return true;
    }
    s.completeness.block_body_stored || s.completeness.transfers_stored
}

// `apply_legacy_patch` writes through `BlockStatus::Final` for the
// blocks it ships. Nothing references `BlockStatus` from this module
// directly; the import touches only `SlotState`. Keep the type in
// scope for documentation/cross-link clarity.
const _: Option<BlockStatus> = None;

// ---------------------------------------------------------------------------
// Unit tests — exercise the worker's logic against a stub source.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_block_id, mk_test_user_addr};
    use crate::legacy::{LegacyFetch, StubLegacySource};
    use crate::model::{Slot, SlotCompleteness, SlotState, SlotStatus, StoredBlock};
    use crate::proto::indexer::v1::FinalSlotResponse;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn open_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        (db, dir)
    }

    /// Build a `LegacyFetch` carrying a real block — exactly what the
    /// production source emits.
    fn make_fetch(period: u64, thread: u8) -> LegacyFetch {
        let block_id = mk_test_block_id(period as u64 + 1000);
        let creator = mk_test_user_addr(1);
        let block = StoredBlock {
            id: block_id.clone(),
            slot: Slot::new(period, thread),
            creator,
            parents: vec![],
            operation_ids: vec![],
            endorsements: vec![],
            endorsement_ids: vec![],
            denunciations: vec![],
            current_version: 0,
            announced_version: None,
            operations_hash: String::new(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_header_b64: String::new(),
            status: BlockStatus::Final,
            first_seen_ts_ms: 0,
        };
        let resp = FinalSlotResponse {
            period,
            thread: thread as u32,
            final_known: true,
            is_miss: false,
            execution_trail_hash: String::new(),
            final_block_id: block_id.to_string(),
            block: Some(crate::codec::block_to_peer_pb(&block).unwrap()),
            ..Default::default()
        };
        LegacyFetch { resp, rpcs: 1 }
    }

    fn fast_cfg(max_period: u64) -> OneShotConfig {
        OneShotConfig {
            max_period: Some(max_period),
            min_period: None,
            rate_limit: Duration::from_millis(0),
            thread_count: 2,
            concurrency: 4,
        }
    }

    /// `slot_already_covered`: a FINAL slot counts as covered once it
    /// has a block body (`block_body_stored`) — because the legacy
    /// fetch is atomic per slot (block + ops + transfers ship in one
    /// patch) — or transfers, or is a miss. This is the predicate that
    /// makes a restart resume cheaply without re-querying AWS for
    /// slots a prior run already imported.
    #[test]
    fn slot_covered_definition() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        assert!(!slot_already_covered(&s), "fresh = not covered");

        s.status = SlotStatus::Final;
        assert!(!slot_already_covered(&s), "final w/o any stored part = not covered");

        // exec_output_final alone (no block body, no transfers) is NOT a
        // "fully imported" signal — the importer still wants the block.
        s.completeness.exec_output_final = true;
        assert!(
            !slot_already_covered(&s),
            "exec_output_final alone is NOT enough to skip a legacy fetch"
        );

        // A stored block body == fully legacy-imported (block + ops +
        // sub-transfers arrive together), so the slot is covered and a
        // restart must NOT re-query AWS for it.
        s.completeness.block_body_stored = true;
        assert!(
            slot_already_covered(&s),
            "final + block_body_stored = covered (don't re-query already-loaded slots)"
        );

        // transfers_stored alone is also covered.
        let mut s2 = SlotState::fresh(Slot::new(2, 0), 0);
        s2.status = SlotStatus::Final;
        s2.completeness.transfers_stored = true;
        assert!(slot_already_covered(&s2), "final + transfers_stored = covered");

        // final + miss = covered.
        let mut s3 = SlotState::fresh(Slot::new(3, 0), 0);
        s3.status = SlotStatus::Final;
        s3.is_miss = true;
        assert!(slot_already_covered(&s3), "final + miss = covered");

        // Candidate is never covered regardless of flags.
        let mut s4 = SlotState::fresh(Slot::new(4, 0), 0);
        s4.completeness.block_body_stored = true;
        s4.status = SlotStatus::Candidate;
        assert!(!slot_already_covered(&s4), "candidate is never covered");
    }

    /// End-to-end: the importer walks down from `max_period` to (0,0)
    /// and emits a `LegacyPatch` for every slot — filled when the stub
    /// has data, FINAL miss when DDB/stub returns empty — then exits.
    #[tokio::test]
    async fn imports_two_slots_and_exits() {
        let (db, _dir) = open_db();
        // Stub has data for (1,0) and (0,1) only. Window is period 0..=1
        // × threads 0..=1 → four slots: two filled + two empty-misses.
        let mut rows: HashMap<(u64, u8), LegacyFetch> = HashMap::new();
        rows.insert((1, 0), make_fetch(1, 0));
        rows.insert((0, 1), make_fetch(0, 1));
        let stub: Arc<dyn LegacySource> = Arc::new(StubLegacySource::new(rows));

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(8);
        let cfg = fast_cfg(1);

        let h = tokio::spawn(run_oneshot_import(db, stub, tx.clone(), cfg, None));
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("importer didn't exit")
            .expect("task panicked");

        let mut count = 0;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, Event::LegacyPatch(_)) {
                count += 1;
            }
        }
        assert_eq!(
            count, 4,
            "two filled + two confirmed-empty misses, then stop"
        );
    }

    /// The importer skips slots a richer source (live/peer) has
    /// already covered (FINAL + transfers_stored).
    #[tokio::test]
    async fn skips_already_covered_slots() {
        let (db, _dir) = open_db();

        // Pre-populate (1,0) as already covered by a richer source:
        // block_body + transfers_stored = peer/live signature.
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            transfers_stored: true,
            ..Default::default()
        };
        db.write_slot(&s).unwrap();

        // Stub has a fetch for (1,0) that should NEVER be issued.
        let mut rows: HashMap<(u64, u8), LegacyFetch> = HashMap::new();
        rows.insert((1, 0), make_fetch(1, 0));
        rows.insert((0, 0), make_fetch(0, 0));
        let stub_concrete = Arc::new(StubLegacySource::new(rows));
        let stub: Arc<dyn LegacySource> = stub_concrete.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(8);
        let cfg = fast_cfg(1);

        let h = tokio::spawn(run_oneshot_import(
            db.clone(),
            stub,
            tx.clone(),
            cfg,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("importer didn't exit")
            .expect("task panicked");

        // Stub was called for the 3 non-covered slots: (1,1), (0,0), (0,1).
        // (1,0) was pre-covered and must not have been queried.
        let calls = stub_concrete
            .call_count
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(calls, 3, "covered slot was skipped, others queried");

        // (0,0) filled + (1,1) and (0,1) confirmed-empty misses.
        let mut count = 0;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, Event::LegacyPatch(_)) {
                count += 1;
            }
        }
        assert_eq!(count, 3);
    }

    /// `min_period` bounds the walk on the bottom side. With
    /// `max_period=3, min_period=2, thread_count=2`, the worker
    /// touches exactly (3,0), (3,1), (2,0), (2,1) — four slots —
    /// and exits without descending to period 1 or 0.
    #[tokio::test]
    async fn min_period_bounds_the_walk() {
        let (db, _dir) = open_db();
        // Stub has data for every slot in 0..=3 × 0..=1. We expect
        // the worker to query only the (3, *) and (2, *) ones.
        let mut rows: HashMap<(u64, u8), LegacyFetch> = HashMap::new();
        for p in 0u64..=3 {
            for t in 0u8..=1 {
                rows.insert((p, t), make_fetch(p, t));
            }
        }
        let stub_concrete = Arc::new(StubLegacySource::new(rows));
        let stub: Arc<dyn LegacySource> = stub_concrete.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(16);
        let cfg = OneShotConfig {
            max_period: Some(3),
            min_period: Some(2),
            rate_limit: Duration::from_millis(0),
            thread_count: 2,
            concurrency: 4,
        };

        let h = tokio::spawn(run_oneshot_import(db, stub, tx.clone(), cfg, None));
        tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .expect("importer didn't exit")
            .expect("task panicked");

        let calls = stub_concrete
            .call_count
            .load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(calls, 4, "exactly (3,0) (3,1) (2,0) (2,1) were queried");

        let mut count = 0;
        while let Ok(ev) = rx.try_recv() {
            if matches!(ev, Event::LegacyPatch(_)) {
                count += 1;
            }
        }
        assert_eq!(count, 4, "all 4 in-window slots emitted a patch");
    }

    /// Closing the channel mid-walk causes the worker to bail out
    /// without finishing the sweep.
    #[tokio::test]
    async fn shuts_down_when_channel_closes() {
        let (db, _dir) = open_db();
        let stub: Arc<dyn LegacySource> = Arc::new(StubLegacySource::new(HashMap::new()));
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(8);
        // Drop the receiver immediately → channel is closed.
        drop(rx);
        let cfg = OneShotConfig {
            max_period: Some(10_000_000),
            min_period: None,
            rate_limit: Duration::from_millis(0),
            thread_count: 32,
            concurrency: 8,
        };
        let h = tokio::spawn(run_oneshot_import(db, stub, tx, cfg, None));
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("worker didn't shut down")
            .expect("task panicked");
    }
}
