//! One-shot, operator-driven repair tasks (`[repair]` in `indexer.toml`).
//!
//! Two background tasks, both **idempotent** and **checkpointed** in
//! `cf_meta` so an indexer restart resumes instead of redoing work, and
//! both feeding the single-writer ingest channel like every other
//! producer (no writes from these tasks bypass `apply_*_patch`):
//!
//! * [`run_reconstruct_transfers`] — for FINAL slots in a period range
//!   whose node transfer list never arrived (`transfers_stored == false`;
//!   the transfers stream was `Unimplemented` on nodes built without
//!   `execution-info` for four months), rebuild the *exact* movements we
//!   can derive from data already on disk: one `OpTransactionCoins` row
//!   per successfully executed `Transaction` operation. Rewards, fees and
//!   SC-internal transfers cannot be derived without re-execution and are
//!   deliberately **not** invented. Rows are shipped as
//!   `Event::LegacyPatch`, which writes transfers without settling
//!   `transfers_stored`, so a real node list from a peer can still replace
//!   the reconstruction later (`apply_transfers_part` clears the slot's
//!   rows first).
//!
//! * [`run_pull_parts`] — for FINAL slots in a period range, pull the
//!   selected parts (`block` / `exec_output` / `transfers`) that are
//!   missing locally from peers via bulk `StreamFinalSlots`, one
//!   descending pass. Used after a node is rebuilt with `execution-info`
//!   on one host to propagate its transfers to the hosts whose nodes
//!   were still lacking the stream during that window. Slots no peer can
//!   supply are left alone — there is no perpetual retry here; the
//!   regular walker's `recent_incomplete_periods` rule covers the head.
//!
//! Progress is exposed via the `massa_indexer_repair_*` counters and
//! `/v1/backfill/status`.

use crate::{
    db::Db,
    ingest::{Event, EventTx},
    legacy::decode::legacy_op_to_transfer,
    metrics::Metrics,
    model::{ExecStatus, OperationKind, Slot, SlotStatus},
    peer::{
        backfill::{range_fill_window_opts, BackfillConfig},
        client::PeerPool,
    },
    proto::indexer::v1::{FinalSlotParts, FinalSlotResponse},
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, info, warn};

/// Checkpoint every this many periods (cheap `cf_meta` put).
const CHECKPOINT_EVERY: u64 = 64;
/// Breather between periods so the shared ingest channel is never
/// saturated by a repair task while live frames are flowing.
const PERIOD_PAUSE: Duration = Duration::from_millis(2);

/// Tunables for [`run_reconstruct_transfers`].
#[derive(Debug, Clone)]
pub struct ReconstructConfig {
    pub from_period: u64,
    /// Inclusive upper bound (already resolved: `0` in the TOML means
    /// "head at startup" and is substituted by the caller).
    pub to_period: u64,
    pub thread_count: u8,
    /// Chain timing used to stamp `block_timestamp_ms` on rebuilt rows.
    pub genesis_timestamp_ms: i64,
    pub t0_ms: i64,
}

fn reconstruct_ckpt_key(cfg: &ReconstructConfig) -> String {
    format!(
        "repair:reconstruct_transfers:{}-{}:next_period",
        cfg.from_period, cfg.to_period
    )
}

fn slot_timestamp_ms(cfg: &ReconstructConfig, slot: Slot) -> i64 {
    let per_thread = cfg.t0_ms / i64::from(cfg.thread_count.max(1));
    cfg.genesis_timestamp_ms
        + (slot.period as i64).saturating_mul(cfg.t0_ms)
        + i64::from(slot.thread) * per_thread
}

/// Build the reconstruction patch for one slot, or `None` when there is
/// nothing to rebuild (slot not FINAL, is a miss, already has the node's
/// transfer list, or no successfully executed `Transaction` op).
pub fn reconstruct_slot(db: &Db, cfg: &ReconstructConfig, slot: Slot) -> crate::Result<Option<FinalSlotResponse>> {
    let Some(state) = db.read_slot(slot.period, slot.thread)? else {
        return Ok(None);
    };
    if state.status != SlotStatus::Final
        || state.is_miss
        || state.completeness.transfers_stored
        || state.executed_op_ids.is_empty()
    {
        return Ok(None);
    }
    let Some(block_id) = state.final_block_id.as_ref() else {
        return Ok(None);
    };
    let block_id_str = block_id.to_string();
    let ts = slot_timestamp_ms(cfg, slot);
    let mut resp = FinalSlotResponse {
        period: slot.period,
        thread: u32::from(slot.thread),
        final_known: true,
        is_miss: false,
        final_block_id: block_id_str.clone(),
        ..Default::default()
    };
    let mut index: u32 = 0;
    for op_id in &state.executed_op_ids {
        let Some(op) = db.read_op(op_id)? else { continue };
        if op.kind != OperationKind::Transaction || op.final_exec_status != Some(ExecStatus::Ok) {
            continue;
        }
        if let Some(t) = legacy_op_to_transfer(&op, slot, &block_id_str, index, ts) {
            resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));
            index += 1;
        }
    }
    if resp.transfers.is_empty() {
        return Ok(None);
    }
    Ok(Some(resp))
}

/// Walk `[from_period, to_period]` ascending, shipping a reconstruction
/// patch for every slot that needs one. Resumes from the `cf_meta`
/// checkpoint; exits when the range is done or the ingest channel closes.
pub async fn run_reconstruct_transfers(
    db: Db,
    tx: EventTx,
    cfg: ReconstructConfig,
    metrics: Option<Arc<Metrics>>,
) {
    let key = reconstruct_ckpt_key(&cfg);
    let start = match db.meta_get_str(&key) {
        Ok(Some(s)) => s.parse::<u64>().unwrap_or(cfg.from_period).max(cfg.from_period),
        Ok(None) => cfg.from_period,
        Err(e) => {
            warn!(error = %e, "repair: reconstruct_transfers checkpoint unreadable; starting from from_period");
            cfg.from_period
        }
    };
    if start > cfg.to_period {
        info!(
            from = cfg.from_period,
            to = cfg.to_period,
            "repair: reconstruct_transfers already COMPLETE for this range (checkpoint); nothing to do"
        );
        return;
    }
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        resume_at = start,
        "repair: reconstruct_transfers starting"
    );

    let mut rows_total = 0u64;
    let mut slots_total = 0u64;
    let mut period = start;
    while period <= cfg.to_period {
        if tx.is_closed() {
            return;
        }
        for thread in 0..cfg.thread_count {
            let slot = Slot::new(period, thread);
            if let Some(m) = &metrics {
                m.repair_slots_scanned_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            match reconstruct_slot(&db, &cfg, slot) {
                Ok(Some(resp)) => {
                    let n = resp.transfers.len() as u64;
                    if tx.send(Event::LegacyPatch(Box::new(resp))).await.is_err() {
                        return;
                    }
                    rows_total += n;
                    slots_total += 1;
                    if let Some(m) = &metrics {
                        m.repair_transfers_reconstructed_total
                            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                Ok(None) => {}
                Err(e) => warn!(error = %e, period, thread, "repair: reconstruct_slot failed"),
            }
        }
        if period % CHECKPOINT_EVERY == 0 {
            if let Err(e) = db.meta_put_str(&key, &(period + 1).to_string()) {
                warn!(error = %e, "repair: checkpoint write failed");
            }
            if period % (CHECKPOINT_EVERY * 256) == 0 {
                info!(period, slots_total, rows_total, "repair: reconstruct_transfers progress");
            }
        }
        period += 1;
        sleep(PERIOD_PAUSE).await;
        tokio::task::yield_now().await;
    }
    if let Err(e) = db.meta_put_str(&key, &(cfg.to_period + 1).to_string()) {
        warn!(error = %e, "repair: final checkpoint write failed");
    }
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        slots_total,
        rows_total,
        "repair: reconstruct_transfers COMPLETE — safe to disable [repair.reconstruct_transfers]"
    );
}

/// Tunables for [`run_pull_parts`].
#[derive(Debug, Clone)]
pub struct PullPartsConfig {
    pub from_period: u64,
    /// Inclusive upper bound (already resolved from `0` = head).
    pub to_period: u64,
    pub parts: FinalSlotParts,
    pub thread_count: u8,
    /// Restrict to slots with a successfully executed `CallSC` /
    /// `ExecuteSC` (see `[repair.pull_parts] require_sc_ops`).
    pub require_sc_ops: bool,
}

fn pull_ckpt_key(cfg: &PullPartsConfig) -> String {
    format!(
        "repair:pull_parts:{}-{}:b{}e{}t{}{}:next_hi",
        cfg.from_period,
        cfg.to_period,
        u8::from(cfg.parts.block),
        u8::from(cfg.parts.exec_output),
        u8::from(cfg.parts.transfers),
        if cfg.require_sc_ops { ":sc" } else { "" }
    )
}

/// Did this slot execute at least one `CallSC` / `ExecuteSC` successfully?
/// Those are the only slots in which ABI sub-transfers can exist.
pub fn slot_has_successful_sc_op(db: &Db, state: &crate::model::SlotState) -> crate::Result<bool> {
    for op_id in &state.executed_op_ids {
        if let Some(op) = db.read_op(op_id)? {
            if matches!(op.kind, OperationKind::CallSc | OperationKind::ExecuteSc)
                && op.final_exec_status == Some(ExecStatus::Ok)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Does this FINAL slot lack one of the requested parts (and, when
/// `require_sc_ops`, did it execute an SC call)?
pub fn slot_lacks_parts(
    db: &Db,
    slot: Slot,
    parts: &FinalSlotParts,
    require_sc_ops: bool,
) -> crate::Result<bool> {
    let Some(s) = db.read_slot(slot.period, slot.thread)? else {
        return Ok(false);
    };
    if s.status != SlotStatus::Final {
        return Ok(false);
    }
    let c = s.completeness;
    let lacking = (parts.block && !s.is_miss && !c.block_body_stored)
        || (parts.exec_output && !c.exec_output_final)
        || (parts.transfers && !c.transfers_stored);
    if !lacking {
        return Ok(false);
    }
    if require_sc_ops && !slot_has_successful_sc_op(db, &s)? {
        return Ok(false);
    }
    Ok(true)
}

/// One descending pass over `[from_period, to_period]` in windows of
/// `range_periods`, bulk-pulling the requested parts for FINAL slots that
/// lack them. Resumes from the `cf_meta` checkpoint.
pub async fn run_pull_parts(
    db: Db,
    pool: PeerPool,
    tx: EventTx,
    cfg: PullPartsConfig,
    metrics: Option<Arc<Metrics>>,
) {
    let key = pull_ckpt_key(&cfg);
    let start_hi = match db.meta_get_str(&key) {
        Ok(Some(s)) => s.parse::<u64>().unwrap_or(cfg.to_period).min(cfg.to_period),
        Ok(None) => cfg.to_period,
        Err(e) => {
            warn!(error = %e, "repair: pull_parts checkpoint unreadable; starting from to_period");
            cfg.to_period
        }
    };
    if start_hi < cfg.from_period {
        info!(
            from = cfg.from_period,
            to = cfg.to_period,
            "repair: pull_parts already COMPLETE for this range (checkpoint); nothing to do"
        );
        return;
    }
    // The bulk path applies only slots we list as needy and requests
    // exactly `parts`; `expected_streams` is set so the mask is honoured.
    let bf = BackfillConfig {
        parts: cfg.parts,
        expected_streams: crate::model::StreamsExpected {
            filled_blocks: cfg.parts.block,
            slot_execution_outputs: cfg.parts.exec_output,
            transfers: cfg.parts.transfers,
        },
        thread_count: cfg.thread_count,
        ..BackfillConfig::default()
    };
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        resume_hi = start_hi,
        parts = ?cfg.parts,
        "repair: pull_parts starting"
    );

    let mut applied_total = 0u64;
    let mut left_total = 0u64;
    let mut hi = start_hi;
    loop {
        if tx.is_closed() {
            return;
        }
        if pool.is_empty() {
            debug!("repair: pull_parts waiting for peers");
            sleep(Duration::from_secs(5)).await;
            continue;
        }
        let lo = hi
            .saturating_sub(bf.range_periods.saturating_sub(1))
            .max(cfg.from_period);

        let mut needy: Vec<(u64, u8)> = Vec::new();
        for p in (lo..=hi).rev() {
            for t in 0..cfg.thread_count {
                if let Some(m) = &metrics {
                    m.repair_slots_scanned_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                match slot_lacks_parts(&db, Slot::new(p, t), &cfg.parts, cfg.require_sc_ops) {
                    Ok(true) => needy.push((p, t)),
                    Ok(false) => {}
                    Err(e) => warn!(error = %e, period = p, thread = t, "repair: read_slot failed"),
                }
            }
        }
        if !needy.is_empty() {
            let before = needy.len();
            if !range_fill_window_opts(&pool, &tx, &bf, metrics.as_ref(), lo, hi, &mut needy, true)
                .await
            {
                return;
            }
            let applied = (before - needy.len()) as u64;
            applied_total += applied;
            left_total += needy.len() as u64;
            if let Some(m) = &metrics {
                m.repair_pull_slots_applied_total
                    .fetch_add(applied, std::sync::atomic::Ordering::Relaxed);
            }
        }

        if let Err(e) = db.meta_put_str(&key, &lo.saturating_sub(1).to_string()) {
            warn!(error = %e, "repair: pull_parts checkpoint write failed");
        }
        if lo <= cfg.from_period || lo == 0 {
            break;
        }
        hi = lo - 1;
        tokio::task::yield_now().await;
    }
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        applied_total,
        left_total,
        "repair: pull_parts COMPLETE — safe to disable [repair.pull_parts]"
    );
}

/// Tunables for [`run_legacy_sub_transfers`].
#[derive(Debug, Clone)]
pub struct LegacySubTransfersConfig {
    pub from_period: u64,
    /// Inclusive upper bound (already resolved from `0` = head).
    pub to_period: u64,
    pub thread_count: u8,
    pub genesis_timestamp_ms: i64,
    pub t0_ms: i64,
    /// Concurrent DDB lookups in flight.
    pub concurrency: usize,
}

fn legacy_sub_ckpt_key(cfg: &LegacySubTransfersConfig) -> String {
    format!(
        "repair:legacy_sub_transfers:{}-{}:next_period",
        cfg.from_period, cfg.to_period
    )
}

/// Ascending pass over `[from_period, to_period]`: for every FINAL slot
/// that lacks the node's transfer list and executed an SC call
/// successfully, fetch the legacy storer's `_N` ABI sub-transfer rows
/// and ship them as `Event::LegacyPatch` (transfers only). Rows already
/// present for the slot (by id) are skipped, new rows are numbered after
/// the existing ones, so re-runs are idempotent. Checkpointed.
pub async fn run_legacy_sub_transfers(
    db: Db,
    source: Arc<crate::legacy::DdbLegacySource>,
    tx: EventTx,
    cfg: LegacySubTransfersConfig,
    metrics: Option<Arc<Metrics>>,
) {
    use futures::stream::StreamExt;
    let key = legacy_sub_ckpt_key(&cfg);
    let start = match db.meta_get_str(&key) {
        Ok(Some(s)) => s.parse::<u64>().unwrap_or(cfg.from_period).max(cfg.from_period),
        Ok(None) => cfg.from_period,
        Err(e) => {
            warn!(error = %e, "repair: legacy_sub_transfers checkpoint unreadable; starting from from_period");
            cfg.from_period
        }
    };
    if start > cfg.to_period {
        info!(
            from = cfg.from_period,
            to = cfg.to_period,
            "repair: legacy_sub_transfers already COMPLETE for this range (checkpoint); nothing to do"
        );
        return;
    }
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        resume_at = start,
        concurrency = cfg.concurrency,
        "repair: legacy_sub_transfers starting"
    );
    let rc = ReconstructConfig {
        from_period: cfg.from_period,
        to_period: cfg.to_period,
        thread_count: cfg.thread_count,
        genesis_timestamp_ms: cfg.genesis_timestamp_ms,
        t0_ms: cfg.t0_ms,
    };

    let mut queried = 0u64;
    let mut rows_total = 0u64;
    let mut slots_total = 0u64;
    let mut errors = 0u64;
    let mut period = start;
    while period <= cfg.to_period {
        if tx.is_closed() {
            return;
        }
        // Local pre-filter for the period: only SC slots without the node
        // list are worth a DDB round-trip.
        let mut targets: Vec<(Slot, String, HashSet<String>, u32)> = Vec::new();
        for thread in 0..cfg.thread_count {
            let slot = Slot::new(period, thread);
            if let Some(m) = &metrics {
                m.repair_slots_scanned_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            let Ok(Some(state)) = db.read_slot(slot.period, slot.thread) else { continue };
            if state.status != SlotStatus::Final
                || state.is_miss
                || state.completeness.transfers_stored
                || state.executed_op_ids.is_empty()
            {
                continue;
            }
            let Some(bid) = state.final_block_id.as_ref() else { continue };
            match slot_has_successful_sc_op(&db, &state) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    warn!(error = %e, period, thread, "repair: read_op failed");
                    continue;
                }
            }
            let existing = db.iter_transfers_for_slot(slot.period, slot.thread).unwrap_or_default();
            let ids: HashSet<String> = existing.iter().map(|t| t.id.clone()).collect();
            let next_index = existing
                .iter()
                .map(|t| t.index_in_slot + 1)
                .max()
                .unwrap_or(0);
            targets.push((slot, bid.to_string(), ids, next_index));
        }

        if !targets.is_empty() {
            let mut fetches = futures::stream::iter(targets.into_iter().map(|(slot, bid, ids, next_index)| {
                let source = source.clone();
                let ts = slot_timestamp_ms(&rc, slot);
                async move {
                    let r = source
                        .fetch_sub_transfers(slot.period, slot.thread, Some(&bid), next_index, ts, &ids)
                        .await;
                    (slot, bid, r)
                }
            }))
            .buffer_unordered(cfg.concurrency.max(1));
            while let Some((slot, bid, r)) = fetches.next().await {
                queried += 1;
                match r {
                    Ok(rows) if rows.is_empty() => {}
                    Ok(rows) => {
                        let n = rows.len() as u64;
                        let mut resp = FinalSlotResponse {
                            period: slot.period,
                            thread: u32::from(slot.thread),
                            final_known: true,
                            is_miss: false,
                            final_block_id: bid,
                            ..Default::default()
                        };
                        for t in &rows {
                            resp.transfers.push(crate::codec::transfer_to_peer_pb(t));
                        }
                        if tx.send(Event::LegacyPatch(Box::new(resp))).await.is_err() {
                            return;
                        }
                        rows_total += n;
                        slots_total += 1;
                        if let Some(m) = &metrics {
                            m.repair_transfers_reconstructed_total
                                .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                            m.legacy_ddb_slots_filled_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        errors += 1;
                        if let Some(m) = &metrics {
                            m.legacy_ddb_errors_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        warn!(error = %e, period = slot.period, thread = slot.thread, "repair: legacy sub-transfer fetch failed (left as is)");
                    }
                }
                if let Some(m) = &metrics {
                    m.legacy_ddb_rpcs_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        if period % CHECKPOINT_EVERY == 0 {
            if let Err(e) = db.meta_put_str(&key, &(period + 1).to_string()) {
                warn!(error = %e, "repair: checkpoint write failed");
            }
            if period % (CHECKPOINT_EVERY * 256) == 0 {
                info!(period, queried, slots_total, rows_total, errors, "repair: legacy_sub_transfers progress");
            }
        }
        period += 1;
        tokio::task::yield_now().await;
    }
    if let Err(e) = db.meta_put_str(&key, &(cfg.to_period + 1).to_string()) {
        warn!(error = %e, "repair: final checkpoint write failed");
    }
    info!(
        from = cfg.from_period,
        to = cfg.to_period,
        queried,
        slots_total,
        rows_total,
        errors,
        "repair: legacy_sub_transfers COMPLETE — safe to disable [repair.legacy_sub_transfers]"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
    use crate::model::{OperationDetails, SlotCompleteness, SlotState, StoredOperation};

    fn open_db() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        (db, dir)
    }

    fn cfg(from: u64, to: u64) -> ReconstructConfig {
        ReconstructConfig {
            from_period: from,
            to_period: to,
            thread_count: 2,
            genesis_timestamp_ms: 1_000_000,
            t0_ms: 16_000,
        }
    }

    fn op(id: crate::ids::OperationId, kind: OperationKind, status: Option<ExecStatus>, amount: u64) -> StoredOperation {
        StoredOperation {
            id,
            creator: mk_test_user_addr(1),
            target: None,
            kind,
            expire_period: 1,
            fee_nmas: 5,
            thread: 0,
            inclusions: vec![],
            candidate_exec_status: status,
            final_exec_status: status,
            details: OperationDetails {
                amount_nmas: Some(amount),
                recipient_address: Some(mk_test_user_addr(2).to_string()),
                ..Default::default()
            },
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        }
    }

    fn final_slot(db: &Db, slot: Slot, ops: Vec<crate::ids::OperationId>, transfers_stored: bool) {
        let mut s = SlotState::fresh(slot, 0);
        s.status = SlotStatus::Final;
        s.final_block_id = Some(mk_test_block_id(slot.period));
        s.executed_op_ids = ops;
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored,
            ..Default::default()
        };
        db.write_slot(&s).unwrap();
    }

    #[test]
    fn reconstructs_only_successful_transactions() {
        let (db, _dir) = open_db();
        let slot = Slot::new(10, 1);
        let ok_tx = mk_test_op_id(1);
        let failed_tx = mk_test_op_id(2);
        let call = mk_test_op_id(3);
        db.write_op(&op(ok_tx.clone(), OperationKind::Transaction, Some(ExecStatus::Ok), 42)).unwrap();
        db.write_op(&op(failed_tx.clone(), OperationKind::Transaction, Some(ExecStatus::Failed), 7)).unwrap();
        db.write_op(&op(call.clone(), OperationKind::CallSc, Some(ExecStatus::Ok), 9)).unwrap();
        final_slot(&db, slot, vec![ok_tx.clone(), failed_tx, call], false);

        let resp = reconstruct_slot(&db, &cfg(0, 100), slot).unwrap().expect("patch");
        assert_eq!(resp.transfers.len(), 1);
        let t = crate::codec::transfer_from_peer_pb(resp.transfers[0].clone()).unwrap();
        assert_eq!(t.operation_id.as_deref(), Some(ok_tx.to_string().as_str()));
        assert_eq!(t.value, crate::model::TransferValue::Coins { nmas: 42 });
        assert_eq!(t.origin, crate::model::CoinOrigin::OpTransactionCoins);
        // genesis + period*t0 + thread*(t0/threads)
        assert_eq!(t.block_timestamp_ms, 1_000_000 + 10 * 16_000 + 8_000);
    }

    #[test]
    fn skips_slots_that_already_have_node_transfers_or_no_ops() {
        let (db, _dir) = open_db();
        let tx_id = mk_test_op_id(1);
        db.write_op(&op(tx_id.clone(), OperationKind::Transaction, Some(ExecStatus::Ok), 42)).unwrap();
        final_slot(&db, Slot::new(20, 0), vec![tx_id.clone()], true);
        assert!(reconstruct_slot(&db, &cfg(0, 100), Slot::new(20, 0)).unwrap().is_none());
        final_slot(&db, Slot::new(21, 0), vec![], false);
        assert!(reconstruct_slot(&db, &cfg(0, 100), Slot::new(21, 0)).unwrap().is_none());
        assert!(reconstruct_slot(&db, &cfg(0, 100), Slot::new(22, 0)).unwrap().is_none());
    }

    /// End-to-end through the ingest worker: rows land in `cf_transfer`,
    /// `transfers_stored` stays false (a real list may still replace
    /// them), the checkpoint is written, and a rerun is a no-op.
    #[tokio::test]
    async fn run_reconstruct_writes_rows_and_checkpoints() {
        let (db, _dir) = open_db();
        let slot = Slot::new(64, 0);
        let tx_id = mk_test_op_id(1);
        db.write_op(&op(tx_id.clone(), OperationKind::Transaction, Some(ExecStatus::Ok), 42)).unwrap();
        final_slot(&db, slot, vec![tx_id], false);

        let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);
        let sse = crate::sse::SseHub::new(16);
        let ingest = crate::ingest::Ingest::new(db.clone(), sse, rx);
        let ingest_handle = tokio::spawn(ingest.run());

        let c = cfg(60, 70);
        run_reconstruct_transfers(db.clone(), tx.clone(), c.clone(), None).await;
        drop(tx);
        ingest_handle.await.unwrap();

        let rows = db.iter_transfers_for_slot(64, 0).unwrap();
        assert_eq!(rows.len(), 1);
        let s = db.read_slot(64, 0).unwrap().unwrap();
        assert!(!s.completeness.transfers_stored, "reconstruction must not settle the flag");
        assert_eq!(
            db.meta_get_str(&reconstruct_ckpt_key(&c)).unwrap().as_deref(),
            Some("71")
        );

        // Rerun: checkpoint says complete → returns immediately, no writes.
        let (tx2, mut rx2) = tokio::sync::mpsc::channel::<Event>(8);
        run_reconstruct_transfers(db.clone(), tx2, c, None).await;
        assert!(rx2.try_recv().is_err());
    }

    #[test]
    fn slot_lacks_parts_reads_completeness() {
        let (db, _dir) = open_db();
        final_slot(&db, Slot::new(5, 0), vec![], false);
        let only_t = FinalSlotParts { block: false, exec_output: false, transfers: true };
        let only_e = FinalSlotParts { block: false, exec_output: true, transfers: false };
        assert!(slot_lacks_parts(&db, Slot::new(5, 0), &only_t, false).unwrap());
        assert!(!slot_lacks_parts(&db, Slot::new(5, 0), &only_e, false).unwrap());
        // Unknown slot / candidate → not a pull target.
        assert!(!slot_lacks_parts(&db, Slot::new(6, 0), &only_t, false).unwrap());
    }

    /// `require_sc_ops` keeps only slots with a successful CallSC/ExecuteSC.
    #[test]
    fn require_sc_ops_filters_slots() {
        let (db, _dir) = open_db();
        let only_t = FinalSlotParts { block: false, exec_output: false, transfers: true };
        let tx_id = mk_test_op_id(1);
        let call_ok = mk_test_op_id(2);
        let call_failed = mk_test_op_id(3);
        db.write_op(&op(tx_id.clone(), OperationKind::Transaction, Some(ExecStatus::Ok), 1)).unwrap();
        db.write_op(&op(call_ok.clone(), OperationKind::CallSc, Some(ExecStatus::Ok), 0)).unwrap();
        db.write_op(&op(call_failed.clone(), OperationKind::CallSc, Some(ExecStatus::Failed), 0)).unwrap();
        final_slot(&db, Slot::new(10, 0), vec![tx_id], false);
        final_slot(&db, Slot::new(11, 0), vec![call_ok], false);
        final_slot(&db, Slot::new(12, 0), vec![call_failed], false);
        assert!(!slot_lacks_parts(&db, Slot::new(10, 0), &only_t, true).unwrap(), "no SC op");
        assert!(slot_lacks_parts(&db, Slot::new(11, 0), &only_t, true).unwrap(), "successful CallSC");
        assert!(!slot_lacks_parts(&db, Slot::new(12, 0), &only_t, true).unwrap(), "failed CallSC");
        // Without the filter all three are targets.
        assert!(slot_lacks_parts(&db, Slot::new(10, 0), &only_t, false).unwrap());
    }
}
