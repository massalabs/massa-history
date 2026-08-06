//! Unified backfill worker — one loop, one direction.
//!
//! Walks the slot space backwards from `last_final_slot.period` down to
//! `(0, 0)`, then wraps around and starts again. For every `(period, thread)`
//! the worker visits:
//!
//!   * **already complete** (FINAL with all expected stream parts present):
//!     skip — a single RocksDB get + a couple of bool checks, no RPC.
//!   * **speculative** (`status != Final`): skip — the live node stream
//!     owns this slot until it transitions to FINAL.
//!   * **gap or incomplete-FINAL**: ask peers (round-robin) for the
//!     missing parts and ship the response as `Event::PeerPatch`. If no
//!     peer can supply the slot, write nothing and move on — the next
//!     sweep retries.
//!
//! ### Bulk range path
//!
//! The walker scans windows of `range_periods` periods locally first.
//! When a window is **densely** missing (≥ `range_sparse_threshold`
//! slots), it pulls the whole window with one `StreamFinalSlots` range
//! call per logical peer instead of one round-trip per slot — turning
//! deep-history catch-up from ~10 slots/s into hundreds per second.
//! Sparse windows and small bulk leftovers (e.g. slots only reachable
//! through a session-only peer, which cannot carry range streams) still
//! use the per-slot path. Each applied slot is paced by `apply_pause`
//! so bulk catch-up never starves the live ingest channel it shares
//! with the node stream. `StreamFinalSlots` has been part of the peer
//! protocol from the start, so mixed-version fleets interoperate: an
//! old server simply serves the same stream it always could, and an
//! old client keeps fetching per-slot.
//!
//! ### What this replaces
//!
//! Three older mechanisms are folded into this one walker:
//!
//!   * the old newest-first scanner that only re-visited rows already in
//!     `cf_slot` (couldn't reach genuine gaps),
//!   * the dedicated history filler that walked `lowest_known_period - 1`
//!     downward to `min_period` and could outrun the data source, leaving
//!     swiss-cheese coverage,
//!   * the legacy DDB fallback inside the regular scanner — AWS is now a
//!     one-shot importer (see `legacy::oneshot`), not a runtime fallback.
//!
//! ### Cost
//!
//! Once the chain is fully covered, a complete sweep is essentially a
//! sequential RocksDB iteration of `cf_slot`. With ~5 µs per get on the
//! production hardware, the entire 145 M-slot mainnet history sweeps in
//! roughly 12 minutes — perfectly fine for a background worker. The
//! `rate_limit` only applies when an RPC fires, not when a slot is
//! skipped, so dense-coverage sweeps stay fast.
//!
//! ### Misses are valid
//!
//! A slot can legitimately be `is_miss = true` (no block was produced).
//! That's a perfectly complete FINAL state — the worker treats it as
//! "covered" and skips it on every subsequent sweep. The peer protocol
//! distinguishes "no block in this slot" (miss) from "I don't know this
//! slot" (`final_known = false`), and only the latter triggers a retry.

use crate::{
    db::Db,
    ingest::{Event, EventTx},
    metrics::Metrics,
    model::{SlotState, SlotStatus, StreamsExpected},
    peer::client::PeerPool,
    proto::indexer::v1::FinalSlotParts,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Per-message timeout while consuming a `StreamFinalSlots` body. A stalled
/// stream is dropped and whatever arrived so far stays applied (idempotent);
/// the remaining slots are mopped up per-slot or retried next sweep.
const STREAM_MSG_TIMEOUT: Duration = Duration::from_secs(20);

/// Tunables for the unified backfill loop.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Pause inserted **only after a peer RPC**. The skip path costs
    /// nothing and is not throttled. Keep this small (≤ 50 ms) on the
    /// LAN — peer queries are cheap and the dataset is large.
    pub rate_limit: Duration,

    /// Pause when the worker has just walked all the way to `(0, 0)`
    /// before starting a fresh sweep from the new head. A small value
    /// keeps the worker re-checking the head frequently for new gaps;
    /// a larger one is friendlier to the cluster when everything is
    /// already converged.
    pub wrap_pause: Duration,

    /// Pause when the indexer hasn't seen its first FINAL slot yet
    /// (so we don't have a head to walk from). Applies at startup
    /// only — once the live stream emits one final slot, the worker
    /// proceeds and won't hit this branch again until shutdown.
    pub idle_pause: Duration,

    /// Streams the operator wants ingested. A part the operator has
    /// **disabled** is never asked for, even if a peer could supply it,
    /// so disabled-stream slots don't stay queued forever.
    pub expected_streams: StreamsExpected,

    /// Default parts mask handed to peers. Mirrors `expected_streams`
    /// in production. Parameterised for tests.
    pub parts: FinalSlotParts,

    /// Number of threads per period. Always 32 in mainnet; configurable
    /// for tests.
    pub thread_count: u8,

    /// Window size (in periods) the walker scans locally before deciding
    /// between the bulk range path and per-slot fetches. With 32 threads,
    /// 16 periods = 512 slots — matching the server-side
    /// `stream_limit_cap` so one range stream can cover a full window.
    pub range_periods: u64,

    /// `limit` passed to `StreamFinalSlots`. Server caps at its own
    /// `stream_limit_cap` (512 by default) regardless.
    pub range_limit: u32,

    /// Minimum number of missing slots in a window before the bulk range
    /// path is used. Below this, per-slot fetches are cheaper than
    /// streaming the entire window (a range stream ships every FINAL slot
    /// in range, including ones we already have).
    pub range_sparse_threshold: usize,

    /// Pause between applying two slots received from a range stream.
    /// This is the receive-side load throttle: PeerPatch events share the
    /// ingest channel with the live node stream, so bulk catch-up must be
    /// paced to never starve real-time processing.
    pub apply_pause: Duration,

    /// Bandwidth budget for bulk catch-up, in bytes/second (0 = uncapped).
    /// Each applied slot extends the pause proportionally to its encoded
    /// size so big slots don't blow the budget. Keeping this well below a
    /// home link's capacity avoids bufferbloat that would delay health
    /// RPCs and other peer traffic sharing the same path.
    pub apply_bandwidth: u64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            rate_limit: Duration::from_millis(20),
            wrap_pause: Duration::from_secs(30),
            idle_pause: Duration::from_secs(5),
            expected_streams: StreamsExpected::all(),
            parts: FinalSlotParts {
                block: true,
                exec_output: true,
                transfers: true,
            },
            thread_count: 32,
            range_periods: 16,
            range_limit: 512,
            range_sparse_threshold: 24,
            apply_pause: Duration::from_millis(2),
            apply_bandwidth: 2_000_000,
        }
    }
}

/// Run the backfill worker until the ingest channel closes.
///
/// The worker is **idempotent across restarts**: any slot it leaves
/// alone (e.g. shutdown mid-sweep) will be revisited on the next start
/// and either skipped (already complete) or filled (gap). No state is
/// persisted between sweeps — the source of truth is always `cf_slot`.
pub async fn run_backfill(
    db: Db,
    pool: PeerPool,
    tx: EventTx,
    cfg: BackfillConfig,
    metrics: Option<Arc<Metrics>>,
) {
    info!(
        static_peers = pool.len(),
        rate_limit_ms = cfg.rate_limit.as_millis() as u64,
        wrap_pause_s = cfg.wrap_pause.as_secs(),
        thread_count = cfg.thread_count,
        "unified backfill worker starting"
    );

    loop {
        if tx.is_closed() {
            break;
        }

        // Wait for at least one logical peer (static URL and/or inbound
        // SyncSession). Sessions can appear after startup when a remote
        // dials us — do not exit permanently on an empty pool.
        if pool.is_empty() {
            debug!("backfill: waiting for peers / sync sessions");
            sleep(cfg.idle_pause).await;
            continue;
        }

        let head = match db.read_last_final_slot() {
            Ok(Some(s)) => s.period,
            Ok(None) => {
                debug!("backfill: no last_final_slot yet, idling");
                sleep(cfg.idle_pause).await;
                continue;
            }
            Err(e) => {
                warn!(error = %e, "backfill: read_last_final_slot failed");
                sleep(cfg.idle_pause).await;
                continue;
            }
        };

        if let Some(m) = &metrics {
            m.backfill_passes_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        debug!(head, "backfill: new sweep");

        let mut period = head;
        loop {
            if tx.is_closed() {
                return;
            }

            let win_lo = period.saturating_sub(cfg.range_periods.saturating_sub(1));

            // Local scan of the window: cheap RocksDB gets, unthrottled —
            // identical cost to the old per-slot skip path.
            let mut needy: Vec<(u64, u8)> = Vec::new();
            for p in (win_lo..=period).rev() {
                for thread in 0..cfg.thread_count {
                    match db.read_slot(p, thread) {
                        Ok(row) => {
                            if needs_fetch(row.as_ref(), &cfg.expected_streams, &cfg.parts)
                                .is_some()
                            {
                                needy.push((p, thread));
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, period = p, thread, "backfill: read_slot failed");
                        }
                    }
                }
            }

            if !needy.is_empty() {
                if needy.len() >= cfg.range_sparse_threshold {
                    // Dense window → one range stream per logical peer.
                    // Slots no peer supplied stay in `needy`.
                    if !range_fill_window(
                        &pool,
                        &tx,
                        &cfg,
                        metrics.as_ref(),
                        win_lo,
                        period,
                        &mut needy,
                    )
                    .await
                    {
                        return;
                    }
                }
                // Sparse window from the start, or a small bulk leftover
                // (e.g. slots only reachable via a session-only peer):
                // per-slot fetches. Large leftovers (no source has the
                // range at all) are skipped — the next sweep retries.
                if !needy.is_empty() && needy.len() < cfg.range_sparse_threshold {
                    for (p, t) in std::mem::take(&mut needy) {
                        if !visit_slot(&db, &pool, &tx, &cfg, metrics.as_ref(), p, t).await {
                            // visit_slot returned false → channel closed
                            return;
                        }
                    }
                }
            }

            if win_lo == 0 {
                break;
            }
            period = win_lo - 1;
        }

        debug!("backfill: reached (0,0), pausing before next sweep");
        sleep(cfg.wrap_pause).await;
    }
    info!("backfill worker stopping");
}

/// Bulk-fill a dense window `[win_lo, win_hi]` (all threads) via
/// `StreamFinalSlots`, trying each logical peer at most once until every
/// missing slot is covered. Only slots still present in `needy` are
/// applied — responses for slots we already hold are discarded without a
/// write. Every apply is paced by `cfg.apply_pause` so bulk catch-up
/// cannot starve the live ingest path sharing the same channel.
///
/// Returns `false` only if the ingest channel closed (caller bails out).
/// Slots remaining in `needy` afterwards were not supplied by any peer.
async fn range_fill_window(
    pool: &PeerPool,
    tx: &EventTx,
    cfg: &BackfillConfig,
    metrics: Option<&Arc<Metrics>>,
    win_lo: u64,
    win_hi: u64,
    needy: &mut Vec<(u64, u8)>,
) -> bool {
    // Union of operator-enabled parts — same mask `needs_fetch` requests
    // for a fully-missing row.
    let parts = FinalSlotParts {
        block: cfg.expected_streams.filled_blocks && cfg.parts.block,
        exec_output: cfg.expected_streams.slot_execution_outputs && cfg.parts.exec_output,
        transfers: cfg.expected_streams.transfers && cfg.parts.transfers,
    };
    let mut remaining: HashSet<(u64, u8)> = needy.iter().copied().collect();

    for peer_id in pool.logical_peer_ids().await {
        if remaining.is_empty() {
            break;
        }
        if tx.is_closed() {
            return false;
        }
        if let Some(m) = metrics {
            m.backfill_rpcs_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            m.backfill_range_streams_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let mut stream = match pool
            .stream_range_from_peer(
                &peer_id,
                (win_lo, 0),
                (win_hi, cfg.thread_count.saturating_sub(1)),
                parts,
                cfg.range_limit,
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                // Session-only peers land here too (no unary route) — the
                // per-slot mop-up covers them.
                debug!(peer = %peer_id, win_lo, win_hi, err = %e, "range stream unavailable");
                continue;
            }
        };
        loop {
            match tokio::time::timeout(STREAM_MSG_TIMEOUT, stream.message()).await {
                Ok(Ok(Some(resp))) => {
                    if !resp.final_known {
                        continue;
                    }
                    let Ok(t) = u8::try_from(resp.thread) else {
                        continue;
                    };
                    // Apply only what we actually miss; everything else is
                    // discarded without touching the DB.
                    if !remaining.remove(&(resp.period, t)) {
                        continue;
                    }
                    if let Some(m) = metrics {
                        m.backfill_slots_filled_total
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    // Receive-side throttle: protect live ingest (fixed
                    // floor) and the network path (byte-proportional part —
                    // saturating a home uplink causes bufferbloat that
                    // delays health RPCs for every peer on that path).
                    let mut pause = cfg.apply_pause;
                    if cfg.apply_bandwidth > 0 {
                        let bytes = prost::Message::encoded_len(&resp);
                        let bw = Duration::from_secs_f64(
                            bytes as f64 / cfg.apply_bandwidth as f64,
                        );
                        pause = pause.max(bw);
                    }
                    if tx.send(Event::PeerPatch(Box::new(resp))).await.is_err() {
                        debug!("backfill: ingest channel closed mid-range");
                        return false;
                    }
                    sleep(pause).await;
                }
                Ok(Ok(None)) => break, // clean end of stream
                Ok(Err(e)) => {
                    // Partial result is fine — applied slots are durable and
                    // idempotent; leftovers retry via mop-up / next sweep.
                    debug!(peer = %peer_id, err = %e, "range stream error (partial ok)");
                    break;
                }
                Err(_) => {
                    debug!(peer = %peer_id, "range stream stalled; dropping");
                    break;
                }
            }
        }
    }

    needy.retain(|k| remaining.contains(k));
    sleep(cfg.rate_limit).await;
    true
}

/// Process one `(period, thread)`. Returns `false` only if the ingest
/// channel has closed mid-flight (caller should bail out).
async fn visit_slot(
    db: &Db,
    pool: &PeerPool,
    tx: &EventTx,
    cfg: &BackfillConfig,
    metrics: Option<&Arc<Metrics>>,
    period: u64,
    thread: u8,
) -> bool {
    let row = match db.read_slot(period, thread) {
        Ok(opt) => opt,
        Err(e) => {
            warn!(error = %e, period, thread, "backfill: read_slot failed");
            return true;
        }
    };

    let needed = match needs_fetch(row.as_ref(), &cfg.expected_streams, &cfg.parts) {
        Some(p) => p,
        None => return true, // skip — complete or speculative
    };

    if let Some(m) = metrics {
        m.backfill_rpcs_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    match pool.fetch_final_slot(period, thread, needed).await {
        Ok(Some(resp)) if resp.final_known => {
            if let Some(m) = metrics {
                m.backfill_slots_filled_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if tx.send(Event::PeerPatch(Box::new(resp))).await.is_err() {
                debug!("backfill: ingest channel closed");
                return false;
            }
        }
        Ok(_) => {
            // Either Ok(None) or Ok(Some) with final_known=false.
            // No source has the data right now — write nothing, move
            // on. Next sweep retries (case (a) per design).
            debug!(period, thread, "backfill: no peer has FINAL");
        }
        Err(e) => {
            // Transient error — same treatment as Ok(None). The pool
            // already logged the per-peer details.
            debug!(error = %e, period, thread, "backfill: peer fetch errored");
        }
    }

    sleep(cfg.rate_limit).await;
    true
}

/// Decide whether `(period, thread)` needs an RPC issued to peers.
///
/// Returns `Some(parts_mask)` describing what to ask for, or `None` to
/// signal "skip this slot".
///
/// Strict cases:
/// - `None` row OR `Unknown` parent-gap stub → ask for everything the
///   operator has enabled.
/// - `Final` row missing some enabled parts → ask only for the
///   missing ones.
/// - `Final` row complete-for-enabled-streams → skip (cheapest path,
///   single bool conjunction).
/// - `Candidate` row → skip; the live stream is still settling it.
fn needs_fetch(
    row: Option<&SlotState>,
    enabled: &StreamsExpected,
    requestable: &FinalSlotParts,
) -> Option<FinalSlotParts> {
    let Some(s) = row else {
        // Genuine gap. Ask for everything enabled.
        let parts = FinalSlotParts {
            block: enabled.filled_blocks && requestable.block,
            exec_output: enabled.slot_execution_outputs && requestable.exec_output,
            transfers: enabled.transfers && requestable.transfers,
        };
        return if parts.block || parts.exec_output || parts.transfers {
            Some(parts)
        } else {
            None
        };
    };

    match s.status {
        SlotStatus::Unknown => {
            // Parent-gap stub — same treatment as a genuine gap.
            let parts = FinalSlotParts {
                block: enabled.filled_blocks && requestable.block,
                exec_output: enabled.slot_execution_outputs && requestable.exec_output,
                transfers: enabled.transfers && requestable.transfers,
            };
            if parts.block || parts.exec_output || parts.transfers {
                Some(parts)
            } else {
                None
            }
        }
        SlotStatus::Final => {
            let c = s.completeness;
            // Structural completeness for the genesis walker: once a FINAL
            // slot has its block body (or is a miss), stop re-querying peers
            // for exec/transfers on every sweep.
            //
            // Why: legacy imports and peer patches leave
            // `transfers_stored` / `exec_output_final` false when those
            // lists are empty (see `apply_legacy_patch`). Asking peers
            // again does not flip the flags — the peer returns the same
            // empty lists — so the walker was burning ~50 ms × tens of
            // millions of already-bodied slots per sweep and never
            // reaching genesis. Live ingest still fills exec/transfers
            // for recent slots as they arrive from the node.
            if c.block_body_stored || s.is_miss {
                return None;
            }
            let block = enabled.filled_blocks && requestable.block;
            if block {
                Some(FinalSlotParts {
                    block: true,
                    exec_output: enabled.slot_execution_outputs && requestable.exec_output,
                    transfers: enabled.transfers && requestable.transfers,
                })
            } else {
                None
            }
        }
        SlotStatus::Candidate => None,
    }
}

// ---------------------------------------------------------------------------
// Unit tests — pure logic only. The integration test in
// `tests/peer_backfill.rs` exercises the end-to-end peer flow.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Slot, SlotCompleteness, SlotState, SlotStatus};

    fn all_parts() -> FinalSlotParts {
        FinalSlotParts {
            block: true,
            exec_output: true,
            transfers: true,
        }
    }

    #[test]
    fn no_row_at_all_requests_everything() {
        let m = needs_fetch(None, &StreamsExpected::all(), &all_parts()).unwrap();
        assert!(m.block && m.exec_output && m.transfers);
    }

    #[test]
    fn unknown_stub_requests_everything() {
        let s = SlotState::fresh(Slot::new(1, 0), 0);
        let m = needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).unwrap();
        assert!(m.block && m.exec_output && m.transfers);
    }

    #[test]
    fn final_complete_skipped() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            exec_output_candidate: true,
            transfers_stored: true,
        };
        assert!(needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).is_none());
    }

    #[test]
    fn final_miss_skipped_when_block_body_marked() {
        // A miss slot has block_body_stored=true (vacuously) and is_miss=true.
        // Even if exec/transfers flags are still false, the walker must
        // not re-query — empty peer replies would never flip those flags
        // and would wedge genesis catch-up.
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.is_miss = true;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: false,
            ..Default::default()
        };
        assert!(needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).is_none());
    }

    #[test]
    fn final_miss_fully_settled_is_skipped() {
        // Once a peer ships exec_output and transfers, the miss is
        // fully covered and the worker must walk past it on every
        // subsequent sweep — no perpetual retries.
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.is_miss = true;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: true,
            ..Default::default()
        };
        assert!(needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).is_none());
    }

    #[test]
    fn candidate_slot_skipped() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Candidate;
        assert!(needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).is_none());
    }

    /// With transfers disabled in `[streams]`, a slot that's complete
    /// on every enabled axis must NOT keep being re-queried.
    #[test]
    fn disabled_stream_does_not_gate_completeness() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: false, // operator disabled the transfers stream
            ..Default::default()
        };
        let enabled = StreamsExpected {
            filled_blocks: true,
            slot_execution_outputs: true,
            transfers: false,
        };
        assert!(needs_fetch(Some(&s), &enabled, &all_parts()).is_none());
    }

    #[test]
    fn final_with_body_skips_even_if_transfers_flag_false() {
        // Regression: perpetual transfer re-fetch on bodied FINAL slots
        // prevented indexer2 from walking below ~period 3.4M.
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: false,
            ..Default::default()
        };
        assert!(needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).is_none());
    }

    #[test]
    fn final_without_body_requests_block() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: false,
            ..Default::default()
        };
        let m = needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).unwrap();
        assert!(m.block);
        assert!(m.exec_output);
        assert!(m.transfers);
    }

    // -----------------------------------------------------------------------
    // End-to-end visit_slot tests (no real network — uses an empty pool
    // that always returns Ok(None)).
    // -----------------------------------------------------------------------
    use crate::db::Db;
    use crate::ingest::Event;

    fn open_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        (db, dir)
    }

    fn fast_cfg() -> BackfillConfig {
        BackfillConfig {
            rate_limit: Duration::from_millis(0),
            wrap_pause: Duration::from_millis(0),
            idle_pause: Duration::from_millis(0),
            thread_count: 4,
            ..Default::default()
        }
    }

    /// `run_backfill` idles when no peers are configured and exits when
    /// the ingest channel closes.
    #[tokio::test]
    async fn run_with_empty_pool_exits_quickly() {
        let (db, _dir) = open_db();
        let pool = PeerPool::with_db(vec![], "test", db.clone());
        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(8);
        let h = tokio::spawn(run_backfill(db, pool, tx, fast_cfg(), None));
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Closing the receiver makes `tx.is_closed()` true inside the worker.
        drop(rx);
        tokio::time::timeout(Duration::from_secs(2), h)
            .await
            .expect("run_backfill stuck with empty pool")
            .expect("task panicked");
    }

    /// When `last_final_slot` is unset, the worker idles instead of
    /// running a sweep. We assert by scheduling a shutdown after a
    /// short wait and confirming no events were emitted.
    #[tokio::test]
    async fn idle_when_no_last_final_slot() {
        let (db, _dir) = open_db();
        let pool = PeerPool::with_db(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
            db.clone(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(8);
        let h = tokio::spawn(run_backfill(db, pool, tx.clone(), fast_cfg(), None));
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(1), h).await;
        assert!(
            rx.try_recv().is_err(),
            "worker must not emit anything when no head exists"
        );
    }

    /// A complete-FINAL slot is skipped: visit_slot fires no RPC and
    /// emits no event.
    #[tokio::test]
    async fn visit_skips_complete_final_slot() {
        let (db, _dir) = open_db();
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: true,
            ..Default::default()
        };
        db.write_slot(&s).unwrap();

        // Pool must NOT be empty (otherwise visit_slot would early-exit
        // somewhere else). We point at an unreachable host so any RPC
        // would error — the test should NEVER actually call out.
        let pool = PeerPool::with_db(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
            db.clone(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(8);
        let cfg = fast_cfg();
        let cont = visit_slot(&db, &pool, &tx, &cfg, None, 1, 0).await;
        assert!(cont, "channel still open");
        assert!(rx.try_recv().is_err(), "no event emitted for complete slot");
    }

    /// A genuine gap (no row) triggers an RPC; the unreachable peer
    /// errors, and visit_slot writes nothing — confirming the case-(a)
    /// behaviour the operator asked for.
    #[tokio::test]
    async fn visit_writes_nothing_when_no_source_has_data() {
        let (db, _dir) = open_db();
        let pool = PeerPool::with_db(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
            db.clone(),
        );
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Event>(8);
        let cfg = fast_cfg();
        // Slot doesn't exist at all in the DB.
        let cont = visit_slot(&db, &pool, &tx, &cfg, None, 42, 1).await;
        assert!(cont);
        assert!(rx.try_recv().is_err(), "no source available → no patch");
        // Crucially: the slot is STILL absent, so the next sweep retries.
        assert!(
            db.read_slot(42, 1).unwrap().is_none(),
            "must not fabricate a row"
        );
    }
}
