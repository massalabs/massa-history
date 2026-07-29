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
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

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
    if pool.is_empty() {
        info!("backfill worker not starting: no peers configured");
        return;
    }
    info!(
        peers = pool.len(),
        rate_limit_ms = cfg.rate_limit.as_millis() as u64,
        wrap_pause_s = cfg.wrap_pause.as_secs(),
        thread_count = cfg.thread_count,
        "unified backfill worker starting"
    );

    loop {
        if tx.is_closed() {
            break;
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

            for thread in 0..cfg.thread_count {
                if tx.is_closed() {
                    return;
                }

                if !visit_slot(
                    &db,
                    &pool,
                    &tx,
                    &cfg,
                    metrics.as_ref(),
                    period,
                    thread,
                )
                .await
                {
                    // visit_slot returned false → channel closed
                    return;
                }
            }

            if period == 0 {
                break;
            }
            period -= 1;
        }

        debug!("backfill: reached (0,0), pausing before next sweep");
        sleep(cfg.wrap_pause).await;
    }
    info!("backfill worker stopping");
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
            let block = enabled.filled_blocks
                && requestable.block
                && !c.block_body_stored
                && !s.is_miss;
            let exec = enabled.slot_execution_outputs
                && requestable.exec_output
                && !c.exec_output_final;
            let transfers =
                enabled.transfers && requestable.transfers && !c.transfers_stored;
            if !block && !exec && !transfers {
                None
            } else {
                Some(FinalSlotParts {
                    block,
                    exec_output: exec,
                    transfers,
                })
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
        // exec_output_final and transfers_stored may still be missing —
        // those are still legitimate fetches.
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.is_miss = true;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: false,
            ..Default::default()
        };
        let m = needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).unwrap();
        assert!(!m.block, "miss slot must not re-request block body");
        assert!(m.exec_output);
        assert!(m.transfers);
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
    fn final_partial_asks_only_for_missing() {
        let mut s = SlotState::fresh(Slot::new(1, 0), 0);
        s.status = SlotStatus::Final;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: false,
            ..Default::default()
        };
        let m = needs_fetch(Some(&s), &StreamsExpected::all(), &all_parts()).unwrap();
        assert!(!m.block);
        assert!(!m.exec_output);
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

    /// `run_backfill` should return immediately when no peers are configured.
    #[tokio::test]
    async fn run_with_empty_pool_exits_quickly() {
        let (db, _dir) = open_db();
        let pool = PeerPool::new(vec![], "test");
        let (tx, _rx) = tokio::sync::mpsc::channel::<Event>(8);
        // Run with a deadline — if the worker fails to bail out it'll
        // be killed by the test runner, which surfaces as a hang.
        let h = tokio::spawn(run_backfill(db, pool, tx, fast_cfg(), None));
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
        let pool = PeerPool::new(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
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
        let pool = PeerPool::new(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
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
        let pool = PeerPool::new(
            vec![crate::peer::client::PeerConfig {
                name: "fake".into(),
                url: "http://127.0.0.1:1".into(),
            }],
            "test",
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
