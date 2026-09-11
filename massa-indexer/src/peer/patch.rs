//! Apply a peer-shipped `FinalSlotResponse` to our local RocksDB.
//!
//! The backfill worker fans out `GetFinalSlot` calls and sends every
//! `final_known == true` response through the ingest channel as
//! `Event::PeerPatch`. The ingest worker invokes [`apply_peer_patch`] on the
//! single-writer side, keeping atomicity invariants consistent with the live
//! node stream.
//!
//! Invariants honoured:
//!
//! 1. **Local data has priority.** A peer patch never overwrites an existing
//!    `execution_trail_hash` or a FINAL block already on disk. Mismatches are
//!    logged and the conflicting parts skipped.
//! 2. **Completeness only moves forward.** Parts already marked present in
//!    `SlotCompleteness` are not re-applied, so repeated backfill of the same
//!    slot is a cheap no-op.
//! 3. **Idempotency.** Reapplying the same patch twice produces the same
//!    RocksDB state as applying it once.
//!
//! Parent-gap discovery ([`ensure_parent_stub`]) inserts an `Unknown` `SlotState`
//! stub for the slot immediately preceding the one we just learned about in the
//! same thread, making it a natural backfill target on the next scan.

use crate::{
    db::Db,
    ids::{BlockId, OperationId},
    ingest::{
        denunciation_hash, reconcile_async_from_transfers, reconcile_deferred_from_transfers,
        SlotSseEvent,
    },
    model::{
        BlockStatus, Slot, SlotCompleteness, SlotState, SlotStatus, StoredBlock, StoredEndorsement,
        StoredOperation, StoredScEvent, StoredTransfer,
    },
    proto::indexer::v1::{FinalSlotParts, FinalSlotResponse},
    sse::SseHub,
    Result,
};
use tracing::{debug, info, warn};

/// Decode a peer-shipped protobuf row, logging decode failures as warnings
/// and returning `None` so the caller can `continue` / skip. Centralises
/// the boilerplate that used to appear once per row kind in this file.
fn decode_or_warn<T, E: std::fmt::Display>(r: std::result::Result<T, E>, kind: &str) -> Option<T> {
    match r {
        Ok(v) => Some(v),
        Err(e) => {
            warn!(error = %e, kind, "decode peer pb (skipped)");
            None
        }
    }
}

/// Outcome of applying a peer patch — useful for tests to assert the right
/// thing happened without reading the DB twice.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PatchOutcome {
    /// True if we promoted the slot from < FINAL to FINAL.
    pub became_final: bool,
    /// True if the block body was missing locally and we stored it.
    pub block_applied: bool,
    /// True if we wrote a FINAL exec_output this call.
    pub exec_applied: bool,
    /// True if we wrote transfers this call.
    pub transfers_applied: bool,
    /// True if the peer's trail hash disagreed with ours — we kept local.
    pub trail_mismatch: bool,
    /// True if the peer said `final_known == false` (no data).
    pub empty: bool,
    /// True if the peer proposed a different FINAL verdict (block / miss)
    /// and chain linkage proved the peer right: the local verdict was
    /// replaced and the slot rebuilt from the peer's parts.
    pub divergence_repaired: bool,
    /// True if the peer proposed a different FINAL verdict but linkage
    /// confirmed the local one (or was inconclusive): local kept.
    pub divergence_kept_local: bool,
}

/// FINAL verdict for a slot, as recorded locally or proposed by a peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Miss,
    Block(BlockId),
}

impl Verdict {
    fn of_local(state: &SlotState) -> Option<Verdict> {
        if state.is_miss {
            Some(Verdict::Miss)
        } else {
            state.final_block_id.clone().map(Verdict::Block)
        }
    }

    fn of_peer(resp: &FinalSlotResponse, final_block_id: &Option<BlockId>) -> Option<Verdict> {
        if resp.is_miss {
            Some(Verdict::Miss)
        } else {
            final_block_id.clone().map(Verdict::Block)
        }
    }
}

/// What the chain-linkage arbiter concluded about two competing FINAL
/// verdicts for the same slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arbitration {
    /// Later blocks in the thread build on the peer's block (or skip the
    /// slot when the peer says miss): the peer is right.
    AdoptPeer,
    /// Later blocks build on the local block: local is right.
    KeepLocal,
    /// No later FINAL block with a stored body within the lookahead, or
    /// the linkage points at a third block: cannot decide, keep local.
    Inconclusive,
}

/// How many periods ahead / behind the disputed slot to look for the
/// neighbouring FINAL blocks in the same thread. Misses are rare enough
/// that a few hundred periods always contain a block; the bound only
/// exists so a dispute at the head cannot trigger an unbounded scan.
const LINKAGE_LOOKAROUND: u64 = 512;

/// `parents[thread]` of the first FINAL non-miss block **after** `slot`
/// in the same thread whose body we hold. Every Massa block header lists
/// one parent per thread — the previous block in that thread — so this
/// is the chain's own statement of which block was final at (or before)
/// `slot`.
fn next_parent_link(db: &Db, slot: Slot) -> Result<Option<BlockId>> {
    for p in (slot.period + 1)..=(slot.period + LINKAGE_LOOKAROUND) {
        let Some(s) = db.read_slot(p, slot.thread)? else { continue };
        if s.status != SlotStatus::Final || s.is_miss {
            continue;
        }
        let Some(bid) = s.final_block_id.as_ref() else { continue };
        let Some(block) = db.read_block(bid)? else { continue };
        return Ok(block.parents.get(slot.thread as usize).cloned());
    }
    Ok(None)
}

/// `final_block_id` of the last FINAL non-miss slot **before** `slot` in
/// the same thread. When the disputed slot really was a miss, the next
/// block's parent link points here.
fn prev_final_block(db: &Db, slot: Slot) -> Result<Option<BlockId>> {
    let lo = slot.period.saturating_sub(LINKAGE_LOOKAROUND);
    for p in (lo..slot.period).rev() {
        let Some(s) = db.read_slot(p, slot.thread)? else { continue };
        if s.status != SlotStatus::Final || s.is_miss {
            continue;
        }
        if let Some(bid) = s.final_block_id.as_ref() {
            return Ok(Some(bid.clone()));
        }
    }
    Ok(None)
}

/// Walk forward from `slot` along our *own* FINAL rows in the thread and
/// look for a broken link: a later FINAL block whose `parents[thread]` is
/// not the block our rows name as the previous FINAL block. A dead fork
/// that a node finalised for several periods is internally consistent
/// (each fork block names the previous fork block), so a single
/// next-parent check cannot see it; the chain only betrays it where the
/// real chain rejoins. Returns the parent id that rejoin block names —
/// the "anchor" the real chain continues from — when such a break exists
/// within the lookaround, `None` when our chain is consistent.
fn dead_fork_anchor(db: &Db, slot: Slot, local: &Verdict) -> Result<Option<BlockId>> {
    let mut prev: Option<BlockId> = match local {
        Verdict::Block(b) => Some(b.clone()),
        Verdict::Miss => prev_final_block(db, slot)?,
    };
    for p in (slot.period + 1)..=(slot.period + LINKAGE_LOOKAROUND) {
        let Some(s) = db.read_slot(p, slot.thread)? else { continue };
        if s.status != SlotStatus::Final || s.is_miss {
            continue;
        }
        let Some(bid) = s.final_block_id.clone() else { continue };
        if let Some(block) = db.read_block(&bid)? {
            if let Some(par) = block.parents.get(slot.thread as usize) {
                if Some(par) != prev.as_ref() {
                    return Ok(Some(par.clone()));
                }
            }
        }
        prev = Some(bid);
    }
    Ok(None)
}

/// True when `target` is reachable from `anchor` by following
/// `parents[thread]` through block bodies we hold (bounded).
fn reachable_via_parents(db: &Db, anchor: &BlockId, target: &BlockId, thread: u8) -> Result<bool> {
    let mut cur = anchor.clone();
    for _ in 0..64 {
        if cur == *target {
            return Ok(true);
        }
        let Some(block) = db.read_block(&cur)? else { return Ok(false) };
        let Some(par) = block.parents.get(thread as usize) else { return Ok(false) };
        cur = par.clone();
    }
    Ok(false)
}

/// Decide between a local and a peer FINAL verdict using the parent links
/// of neighbouring FINAL blocks. Pure function of local data plus the two
/// verdicts; never contacts the network.
///
/// Two layers:
/// 1. the immediate next FINAL block in the thread names its parent — if
///    that is the peer's block (or skips ours), the peer is right; if it
///    is ours, we are;
/// 2. otherwise, look for a *rejoin* further ahead ([`dead_fork_anchor`]).
///    If our chain is broken there, every local block between the break
///    and the last consistent block is a dead fork: adopt the peer when
///    its block is the anchor itself (or an ancestor of it through bodies
///    we hold), or when it says the slot was a miss.
pub fn arbitrate_verdicts(
    db: &Db,
    slot: Slot,
    local: &Verdict,
    peer: &Verdict,
) -> Result<Arbitration> {
    if local == peer {
        return Ok(Arbitration::KeepLocal);
    }
    let Some(next_parent) = next_parent_link(db, slot)? else {
        return Ok(Arbitration::Inconclusive);
    };
    let immediate = match (local, peer) {
        (Verdict::Block(x), Verdict::Block(y)) => {
            if next_parent == *y {
                Some(Arbitration::AdoptPeer)
            } else if next_parent == *x {
                Some(Arbitration::KeepLocal)
            } else {
                None
            }
        }
        (Verdict::Miss, Verdict::Block(y)) => {
            if next_parent == *y {
                Some(Arbitration::AdoptPeer)
            } else {
                None
            }
        }
        (Verdict::Block(x), Verdict::Miss) => {
            if next_parent == *x {
                Some(Arbitration::KeepLocal)
            } else if prev_final_block(db, slot)?.as_ref() == Some(&next_parent) {
                // The next block skips our slot entirely and builds on
                // the previous block in the thread: our block was never
                // final, the slot was a miss.
                Some(Arbitration::AdoptPeer)
            } else {
                None
            }
        }
        (Verdict::Miss, Verdict::Miss) => Some(Arbitration::KeepLocal),
    };
    if let Some(a) = immediate {
        // A locally consistent immediate link can still be a dead fork
        // (fork block naming fork block). Only trust "keep local" when the
        // chain ahead of us is unbroken.
        if a == Arbitration::KeepLocal {
            if let Some(anchor) = dead_fork_anchor(db, slot, local)? {
                return arbitrate_in_dead_fork(db, slot, local, peer, &anchor);
            }
        }
        return Ok(a);
    }
    match dead_fork_anchor(db, slot, local)? {
        Some(anchor) => arbitrate_in_dead_fork(db, slot, local, peer, &anchor),
        None => Ok(Arbitration::Inconclusive),
    }
}

fn arbitrate_in_dead_fork(
    db: &Db,
    slot: Slot,
    local: &Verdict,
    peer: &Verdict,
    anchor: &BlockId,
) -> Result<Arbitration> {
    Ok(match (local, peer) {
        (Verdict::Block(_), Verdict::Miss) => Arbitration::AdoptPeer,
        (_, Verdict::Block(y)) => {
            if y == anchor || reachable_via_parents(db, anchor, y, slot.thread)? {
                Arbitration::AdoptPeer
            } else {
                Arbitration::Inconclusive
            }
        }
        (Verdict::Miss, Verdict::Miss) => Arbitration::KeepLocal,
    })
}

/// Apply a *legacy*-source `FinalSlotResponse` (i.e. one stitched together
/// from the archived block-storer DDB tables — see `crate::legacy`).
///
/// Distinct from [`apply_peer_patch`] so we never let the legacy fallback
/// "claim" parts it cannot actually source. Legacy carries blocks,
/// operations, endorsements and synthetic Type=Transaction transfers, but
/// **not** SC events, async-pool rows, deferred-call rows, or any of the
/// non-transaction transfer kinds (block rewards, ABI sub-transfers,
/// async-msg coins…). To preserve future-fill capacity, we only mark
/// `block_body_stored` complete here; `exec_output_final` and
/// `transfers_stored` stay `false` so a later peer patch (or a live node
/// stream that catches up) can supply the missing bits.
///
/// Idempotent: running this twice on the same slot is a cheap no-op.
pub fn apply_legacy_patch(
    db: &Db,
    sse: &SseHub,
    resp: &FinalSlotResponse,
    now_ms: i64,
) -> Result<PatchOutcome> {
    let thread = match u8::try_from(resp.thread) {
        Ok(t) => t,
        Err(_) => return Ok(PatchOutcome::default()),
    };
    let slot = Slot::new(resp.period, thread);
    if !resp.final_known {
        return Ok(PatchOutcome {
            empty: true,
            ..Default::default()
        });
    }

    let mut out = PatchOutcome::default();
    let mut state = db
        .read_slot(slot.period, slot.thread)?
        .unwrap_or_else(|| SlotState::fresh(slot, now_ms));

    let final_block_id_opt = if resp.final_block_id.is_empty() {
        None
    } else {
        match BlockId::parse(resp.final_block_id.clone()) {
            Ok(b) => Some(b),
            Err(e) => {
                warn!(err = %e, id = %resp.final_block_id, "legacy sent invalid final_block_id");
                None
            }
        }
    };

    // Promote slot to FINAL if it wasn't already. Legacy never overwrites
    // an existing FINAL verdict (matches §7.1 / first-final-wins), and
    // intentionally does NOT touch `execution_trail_hash` — legacy
    // doesn't carry one. A real peer patch later can fill it in.
    if state.status != SlotStatus::Final {
        state.status = SlotStatus::Final;
        state.is_miss = resp.is_miss;
        if !resp.is_miss {
            if let Some(bid) = &final_block_id_opt {
                state.final_block_id = Some(bid.clone());
                if !state.candidate_block_ids.iter().any(|b| b == bid) {
                    state.candidate_block_ids.push(bid.clone());
                }
            }
        }
        out.became_final = true;
    }

    if out.became_final {
        if let Some(prev) = db.read_last_final_slot()? {
            if (slot.period, slot.thread) > (prev.period, prev.thread) {
                db.update_last_final_slot(&slot)?;
            }
        } else {
            db.update_last_final_slot(&slot)?;
        }
    }

    // Block body — same logic as peer patch, but we mark only this one
    // bit on completeness.
    let block_part_requested = resp.block.is_some();
    if block_part_requested && !state.completeness.block_body_stored && !resp.is_miss {
        if let Some(block) = apply_block_part(db, resp, now_ms)? {
            if !state.candidate_block_ids.iter().any(|b| b == &block.id) {
                state.candidate_block_ids.push(block.id.clone());
            }
            state.completeness.block_body_stored = true;
            out.block_applied = true;
        }
    }
    if resp.is_miss {
        state.completeness.block_body_stored = true;
    }

    // executed_op_ids — populate if missing locally. Legacy's `Status=2`
    // (Executed) flag is authoritative for the historical slot. We do
    // NOT set `exec_output_final = true` because we have no SC events
    // or async-pool data to ship; a real peer patch with the full
    // exec_output is still needed for those.
    if state.executed_op_ids.is_empty() && !resp.executed_op_ids.is_empty() {
        state.executed_op_ids = resp
            .executed_op_ids
            .iter()
            .filter_map(|s| OperationId::parse(s.clone()).ok())
            .collect();
        out.exec_applied = true;
    }

    // Transfers — legacy ships:
    //
    //   * synthesised `<op_id>:N` rows for each executed
    //     Type=Transaction op (one transfer per op),
    //   * explicit `<op_id>_N` rows for ABI sub-transfers and slot-
    //     bound coin movements.
    //
    // We write every shipped transfer unconditionally. Same-key
    // overwrites are intentional: re-importing the same slot with a
    // newer decoder (e.g. one that derives `operation_id` from the
    // sub-transfer hash prefix) UPGRADES the existing row without
    // having to wipe the column family. The on-disk key is
    // `(period, thread, index_in_slot)`, and our two ID schemes
    // (`:N` vs `_N`) keep live and legacy rows on disjoint keys
    // unless they actually collide on `index_in_slot` — and in that
    // case the live ingest path is the one that wins because it
    // processes the slot AFTER `transfers_stored` flips to true,
    // which makes the legacy importer skip the slot entirely.
    //
    // We deliberately don't set `transfers_stored = true` here so a
    // future peer / live patch can still ship the precise CoinOrigin
    // (legacy collapses ABI vs reward vs slash into coarse buckets).
    if !resp.transfers.is_empty() {
        for t_pb in &resp.transfers {
            let Some(t) = decode_or_warn::<StoredTransfer, _>(
                crate::codec::transfer_from_peer_pb(t_pb.clone()),
                "transfer",
            ) else {
                continue;
            };
            db.write_transfer(&t)?;
            out.transfers_applied = true;
        }
    }

    // Block-status finalisation: only safe to run if we've actually got
    // an exec verdict locally. Legacy alone cannot promote the winning
    // block to FINAL (no execution_trail_hash, no exec_output_final),
    // so we defer that to the live stream / peer paths. We DO however
    // need to make sure the FINAL block id we just learned is at least
    // marked candidate-or-Final so the explorer renders it correctly.
    if state.completeness.exec_output_final && state.completeness.block_body_stored {
        apply_finality_to_blocks(db, state.final_block_id.as_ref(), &state.candidate_block_ids)?;
    }

    state.last_updated_ts_ms = now_ms;
    db.write_slot(&state)?;

    ensure_parent_stub(db, slot, now_ms)?;

    if out != PatchOutcome::default() {
        debug!(
            period = slot.period,
            thread = slot.thread,
            became_final = out.became_final,
            block = out.block_applied,
            exec = out.exec_applied,
            transfers = out.transfers_applied,
            "legacy patch applied"
        );
    }
    sse.broadcast(SlotSseEvent::SlotUpdated(state));
    Ok(out)
}

/// Apply a peer's `FinalSlotResponse`. Returns an [`PatchOutcome`] and
/// broadcasts a `SlotUpdated` SSE event if the slot row changed.
pub fn apply_peer_patch(
    db: &Db,
    sse: &SseHub,
    resp: &FinalSlotResponse,
    now_ms: i64,
) -> Result<PatchOutcome> {
    let thread = match u8::try_from(resp.thread) {
        Ok(t) => t,
        Err(_) => return Ok(PatchOutcome::default()),
    };
    let slot = Slot::new(resp.period, thread);
    if !resp.final_known {
        return Ok(PatchOutcome {
            empty: true,
            ..Default::default()
        });
    }

    let mut out = PatchOutcome::default();

    // Load or initialise local slot state.
    let mut state = db
        .read_slot(slot.period, slot.thread)?
        .unwrap_or_else(|| SlotState::fresh(slot, now_ms));

    // --- Reconcile status / trail hash ------------------------------------
    let peer_trail = if resp.execution_trail_hash.is_empty() {
        None
    } else {
        Some(resp.execution_trail_hash.clone())
    };
    let final_block_id_opt = if resp.final_block_id.is_empty() {
        None
    } else {
        match BlockId::parse(resp.final_block_id.clone()) {
            Ok(b) => Some(b),
            Err(e) => {
                warn!(err = %e, id = %resp.final_block_id, "peer sent invalid final_block_id");
                None
            }
        }
    };
    // Which parts the peer actually honoured. Old servers (< 0.0.2) leave
    // this empty and `completeness_known = false`; every settlement below
    // then degrades to the historical "payload present ⇒ applied" rule.
    let echo = resp.completeness_known;
    let honoured: FinalSlotParts = resp.parts.unwrap_or_default();

    let became_final = if state.status != SlotStatus::Final {
        state.status = SlotStatus::Final;
        state.is_miss = resp.is_miss;
        if let Some(t) = peer_trail.clone() {
            state.execution_trail_hash = Some(t);
        }
        if !resp.is_miss {
            if let Some(bid) = &final_block_id_opt {
                state.final_block_id = Some(bid.clone());
                if !state.candidate_block_ids.iter().any(|b| b == bid) {
                    state.candidate_block_ids.push(bid.clone());
                }
            }
        }
        out.became_final = true;
        true
    } else {
        // Both sides are FINAL. If the *verdicts* differ (different block,
        // or block vs miss) one node was on a dead fork when it finalised
        // this slot — seen at protocol upgrades. First-final-wins cannot
        // settle that; the chain can: later blocks in the thread name
        // their parent. Adopt the peer only when linkage proves it.
        let local_v = Verdict::of_local(&state);
        let peer_v = Verdict::of_peer(resp, &final_block_id_opt);
        if let (Some(lv), Some(pv)) = (local_v, peer_v) {
            if lv != pv {
                match arbitrate_verdicts(db, slot, &lv, &pv)? {
                    Arbitration::AdoptPeer => {
                        warn!(
                            period = slot.period,
                            thread = slot.thread,
                            local = ?lv,
                            peer = ?pv,
                            "divergent FINAL verdict: chain linkage confirms peer; repairing local slot"
                        );
                        adopt_peer_verdict(db, &mut state, resp, &final_block_id_opt, peer_trail.clone())?;
                        out.divergence_repaired = true;
                    }
                    other => {
                        info!(
                            period = slot.period,
                            thread = slot.thread,
                            local = ?lv,
                            peer = ?pv,
                            arbitration = ?other,
                            "divergent FINAL verdict: keeping local"
                        );
                        out.divergence_kept_local = true;
                        if let (Some(existing), Some(incoming)) =
                            (state.execution_trail_hash.as_deref(), peer_trail.as_deref())
                        {
                            out.trail_mismatch = existing != incoming;
                        }
                        state.last_updated_ts_ms = now_ms;
                        db.write_slot(&state)?;
                        return Ok(out);
                    }
                }
            }
        }
        if !out.divergence_repaired {
            // Same verdict: first-final-wins trail-hash check (§7.1 / §8.3).
            if let (Some(existing), Some(incoming)) =
                (state.execution_trail_hash.as_deref(), peer_trail.as_deref())
            {
                if existing != incoming {
                    warn!(
                        period = slot.period,
                        thread = slot.thread,
                        existing,
                        incoming,
                        "peer trail-hash mismatch; keeping local FINAL (§7.1)"
                    );
                    out.trail_mismatch = true;
                    // Still write the slot back below (no-op) to touch last_updated.
                    state.last_updated_ts_ms = now_ms;
                    db.write_slot(&state)?;
                    return Ok(out);
                }
            } else if state.execution_trail_hash.is_none() {
                // We were FINAL without a trail hash (edge case) — accept peer's.
                state.execution_trail_hash = peer_trail.clone();
            }
        }
        false
    };

    // Update last_final_slot bookkeeping.
    if became_final {
        if let Some(prev) = db.read_last_final_slot()? {
            if (slot.period, slot.thread) > (prev.period, prev.thread) {
                db.update_last_final_slot(&slot)?;
            }
        } else {
            db.update_last_final_slot(&slot)?;
        }
    }

    // --- Block body -------------------------------------------------------
    let block_part_requested = resp.block.is_some();
    if block_part_requested && !resp.is_miss {
        // Body missing locally → store it. Body already present (typical
        // for a stale Candidate that saw the block live) → still walk the
        // peer's operation rows to merge FINAL execution statuses the
        // node never delivered to us.
        if let Some(block) = apply_block_part(db, resp, now_ms)? {
            if !state.candidate_block_ids.iter().any(|b| b == &block.id) {
                state.candidate_block_ids.push(block.id.clone());
            }
            if !state.completeness.block_body_stored {
                state.completeness.block_body_stored = true;
                out.block_applied = true;
            }
        }
    }
    if resp.is_miss {
        // Miss slots trivially satisfy "block body stored" — no body exists.
        state.completeness.block_body_stored = true;
    }

    // --- Exec output ------------------------------------------------------
    // The exec_output part ships FOUR things: sc_events, executed_op_ids,
    // sc_event_count, and async_msgs. Any of them being non-empty means
    // there is payload to apply. An *empty* exec part is settled only
    // when a >= 0.0.2 peer explicitly says it honoured the part and holds
    // the FINAL exec output (`has_exec_output`): "this slot genuinely had
    // no exec payload" is a fact, not a gap. Old peers cannot say that,
    // so with them empty stays unsettled (historical behaviour).
    let exec_part_present = !resp.executed_op_ids.is_empty()
        || resp.sc_event_count > 0
        || !resp.sc_events.is_empty()
        || !resp.async_msgs.is_empty();
    let exec_asserted = echo && honoured.exec_output && resp.has_exec_output;
    if (exec_part_present || exec_asserted) && !state.completeness.exec_output_final {
        apply_exec_part(db, resp)?;
        if state.executed_op_ids.is_empty() {
            state.executed_op_ids = resp
                .executed_op_ids
                .iter()
                .filter_map(|s| OperationId::parse(s.clone()).ok())
                .collect();
        }
        state.sc_event_count = resp.sc_event_count;
        state.completeness.exec_output_final = true;
        out.exec_applied = true;
    }

    // --- Transfers --------------------------------------------------------
    // Same rule: a non-empty list is applied; an empty list is applied
    // (and the flag settled) only when the peer asserts it holds the
    // FINAL transfer list for the slot.
    let transfers_part_present = !resp.transfers.is_empty() || !resp.deferred_calls.is_empty();
    let transfers_asserted = echo && honoured.transfers && resp.has_transfers;
    if (transfers_part_present || transfers_asserted) && !state.completeness.transfers_stored {
        apply_transfers_part(db, resp, now_ms)?;
        state.completeness.transfers_stored = true;
        out.transfers_applied = true;
    }

    // --- Final block status propagation ----------------------------------
    // The slot is FINAL here, so every block we hold for it can be settled
    // (winner → Final, others → Discarded) regardless of whether the exec
    // part arrived. Idempotent: only blocks whose status changes are
    // rewritten.
    apply_finality_to_blocks(db, state.final_block_id.as_ref(), &state.candidate_block_ids)?;

    state.last_updated_ts_ms = now_ms;
    db.write_slot(&state)?;

    // Parent-gap discovery: ensure the previous slot in the same thread has
    // at least an Unknown stub, so the backfill scanner will attempt to
    // pull it next pass.
    ensure_parent_stub(db, slot, now_ms)?;

    if out != PatchOutcome::default() {
        debug!(
            period = slot.period,
            thread = slot.thread,
            became_final = out.became_final,
            block = out.block_applied,
            exec = out.exec_applied,
            transfers = out.transfers_applied,
            repaired = out.divergence_repaired,
            "peer patch applied"
        );
    }
    if became_final || out.divergence_repaired {
        sse.broadcast(SlotSseEvent::SlotFinal(state.clone()));
    }
    sse.broadcast(SlotSseEvent::SlotUpdated(state));
    Ok(out)
}

/// Replace the local FINAL verdict with the peer's after chain linkage
/// proved the local one wrong. Everything derived from the old verdict
/// (exec output, transfers, completeness) is dropped so the parts shipped
/// in the same response rebuild the slot; the superseded local block is
/// marked `Discarded` so block pages stop calling it final.
fn adopt_peer_verdict(
    db: &Db,
    state: &mut SlotState,
    resp: &FinalSlotResponse,
    peer_block: &Option<BlockId>,
    peer_trail: Option<String>,
) -> Result<()> {
    let slot = state.slot;
    if let Some(old) = state.final_block_id.take() {
        if let Some(mut b) = db.read_block(&old)? {
            if b.status != BlockStatus::Discarded {
                b.status = BlockStatus::Discarded;
                db.write_block(&b)?;
            }
        }
    }
    db.clear_sc_events_for_slot(slot.period, slot.thread)?;
    db.clear_transfers_for_slot(slot.period, slot.thread)?;

    state.is_miss = resp.is_miss;
    state.final_block_id = if resp.is_miss { None } else { peer_block.clone() };
    if let Some(bid) = state.final_block_id.as_ref() {
        if !state.candidate_block_ids.iter().any(|b| b == bid) {
            state.candidate_block_ids.push(bid.clone());
        }
    }
    state.execution_trail_hash = peer_trail;
    state.executed_op_ids.clear();
    state.sc_event_count = 0;
    state.completeness = SlotCompleteness {
        block_body_stored: resp.is_miss,
        ..Default::default()
    };
    Ok(())
}

// ---------------------------------------------------------------------------
// Part-specific apply helpers
// ---------------------------------------------------------------------------

fn apply_block_part(db: &Db, resp: &FinalSlotResponse, now_ms: i64) -> Result<Option<StoredBlock>> {
    let Some(block_pb) = resp.block.clone() else {
        return Ok(None);
    };
    let Some(block) = decode_or_warn::<StoredBlock, _>(
        crate::codec::block_from_peer_pb(block_pb),
        "block",
    ) else {
        return Ok(None);
    };
    // Only write if we don't already have it. `write_block` is otherwise
    // idempotent but this spares us an unnecessary WriteBatch.
    if db.read_block(&block.id)?.is_none() {
        db.write_block(&block)?;
    }

    // Operations: write when completely absent. When a local row exists
    // (the block arrived live but its FINAL exec never did — restart
    // hole), merge only what the local row lacks: the FINAL execution
    // status and, if we never recorded one, the inclusion site. A
    // locally-observed status is never regressed by a peer's view.
    for op_pb in &resp.operations {
        let Some(op) = decode_or_warn::<StoredOperation, _>(
            crate::codec::operation_from_peer_pb(op_pb.clone()),
            "operation",
        ) else {
            continue;
        };
        match db.read_op(&op.id)? {
            None => db.write_op(&op)?,
            Some(mut local) => {
                let mut changed = false;
                if local.final_exec_status.is_none() && op.final_exec_status.is_some() {
                    local.final_exec_status = op.final_exec_status;
                    local.candidate_exec_status = op.final_exec_status;
                    changed = true;
                }
                if local.inclusions.is_empty() && !op.inclusions.is_empty() {
                    local.inclusions = op.inclusions.clone();
                    changed = true;
                }
                if changed {
                    db.write_op(&local)?;
                }
            }
        }
    }

    // Endorsements — `write_endorsement` is first-write-wins on its own.
    for endo_pb in &resp.endorsements {
        let Some(end) = decode_or_warn::<StoredEndorsement, _>(
            crate::codec::endorsement_from_peer_pb(endo_pb.clone()),
            "endorsement",
        ) else {
            continue;
        };
        db.write_endorsement(&end)?;
    }

    // Denunciations. The peer ships them embedded in the block body, but
    // `db.write_block` only persists `cf_block` + creator index — it does
    // NOT populate `cf_denunciation` or its address / recent indexes. We
    // mirror `ingest::handle_block` here so a peer-backfilled slot produces
    // the same `cf_denunciation` rows as a live-node-ingested one. The
    // write is idempotent so replaying the same patch twice is a no-op.
    for d in &block.denunciations {
        let entry = crate::model::StoredDenunciationEntry {
            hash: denunciation_hash(d),
            slot: crate::ingest::denunciation_slot(d),
            kind: crate::ingest::denunciation_kind_label(d).into(),
            denounced_addr: crate::ingest::denunciation_target_address(d),
            denunciation: d.clone(),
            included_block_id: Some(block.id.clone()),
            included_slot: Some(crate::ingest::denunciation_slot(d)),
            first_seen_ts_ms: now_ms,
        };
        if let Err(e) = db.write_denunciation(&entry) {
            warn!(error = %e, "write_denunciation (peer patch)");
        }
    }

    Ok(Some(block))
}

fn apply_exec_part(db: &Db, resp: &FinalSlotResponse) -> Result<()> {
    // Replace the SC event set for the slot. The peer's list is authoritative
    // for a FINAL slot.
    let thread = resp.thread as u8;
    db.clear_sc_events_for_slot(resp.period, thread)?;
    for ev_pb in &resp.sc_events {
        let Some(ev) = decode_or_warn::<StoredScEvent, _>(
            crate::codec::sc_event_from_peer_pb(ev_pb.clone()),
            "sc_event",
        ) else {
            continue;
        };
        db.write_sc_event(&ev)?;
    }
    // Async-pool rows the peer shipped alongside the exec_output part. We
    // don't clear the pool CF first — `write_async_msg` is an upsert that
    // preserves `first_seen_ts_ms` and refreshes the per-slot index when
    // `last_slot` moves, so replaying the same patch (or applying it on
    // top of a partially-observed row) converges to the same state.
    for m_pb in &resp.async_msgs {
        let Some(m) = decode_or_warn(
            crate::codec::async_msg_from_peer_pb(m_pb.clone()),
            "async_msg",
        ) else {
            continue;
        };
        db.write_async_msg(&m)?;
    }
    Ok(())
}

fn apply_transfers_part(db: &Db, resp: &FinalSlotResponse, now_ms: i64) -> Result<()> {
    let thread = resp.thread as u8;
    let slot = Slot::new(resp.period, thread);
    db.clear_transfers_for_slot(resp.period, thread)?;
    // Materialise transfers first — identical to `ingest::handle_transfers`
    // pass 1 — then re-run the reconcile hooks below.
    let mut decoded_rows: Vec<StoredTransfer> = Vec::with_capacity(resp.transfers.len());
    for t_pb in &resp.transfers {
        let Some(t) = decode_or_warn::<StoredTransfer, _>(
            crate::codec::transfer_from_peer_pb(t_pb.clone()),
            "transfer",
        ) else {
            continue;
        };
        db.write_transfer(&t)?;
        decoded_rows.push(t);
    }

    // Upsert any deferred-call rows the peer shipped. This is a belt-and-
    // braces guarantee: the reconcile pass below derives the same rows
    // from transfers alone, but shipping them explicitly lets a peer with
    // richer history (e.g. a row mutated across multiple slots) seed the
    // receiver before the reconcile logic runs.
    for d_pb in &resp.deferred_calls {
        let Some(d) = decode_or_warn(
            crate::codec::deferred_call_from_peer_pb(d_pb.clone()),
            "deferred_call",
        ) else {
            continue;
        };
        db.write_deferred_call(&d)?;
    }

    // Mirror `ingest::handle_transfers`: once transfers are durable for the
    // slot, re-run the async/deferred reconcile passes. This is what keeps
    // peer-filled slots consistent with live-ingested slots: a slot whose
    // exec_output never arrived locally (different peer order) still gets
    // its `DeferredCall*` transfers turned into `StoredDeferredCall` rows,
    // and async messages that were executed/cancelled in this slot get
    // their state promoted from Pending/Consumed → Executed/Cancelled.
    reconcile_async_from_transfers(db, slot, &decoded_rows, now_ms)?;
    reconcile_deferred_from_transfers(db, slot, &decoded_rows, now_ms)?;

    Ok(())
}

fn apply_finality_to_blocks(
    db: &Db,
    final_block_id: Option<&BlockId>,
    candidates: &[BlockId],
) -> Result<()> {
    for bid in candidates {
        let Some(mut b) = db.read_block(bid)? else { continue };
        let new_status = if Some(bid) == final_block_id {
            BlockStatus::Final
        } else {
            BlockStatus::Discarded
        };
        if b.status != new_status {
            b.status = new_status;
            db.write_block(&b)?;
        }
    }
    if let Some(bid) = final_block_id {
        if !candidates.iter().any(|c| c == bid) {
            if let Some(mut b) = db.read_block(bid)? {
                if b.status != BlockStatus::Final {
                    b.status = BlockStatus::Final;
                    db.write_block(&b)?;
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Parent-gap discovery
// ---------------------------------------------------------------------------

/// Ensure that the slot immediately preceding `slot` in the same thread has
/// *at least* an `Unknown` row in `cf_slot`. If the row already exists we
/// leave it untouched; if it doesn't we insert a fresh stub with empty
/// completeness, which makes it a natural backfill target on the next scan.
///
/// We intentionally only walk one step backwards per call. Each finalised
/// slot triggers exactly one stub insertion, so a chain of N missing slots
/// takes N scanner cycles to light up (each one fills a gap, which finalises
/// a slot, which stubs the next…). This is simple and self-terminating.
pub fn ensure_parent_stub(db: &Db, slot: Slot, now_ms: i64) -> Result<()> {
    if slot.period == 0 {
        return Ok(());
    }
    let prev_period = slot.period - 1;
    if db.read_slot(prev_period, slot.thread)?.is_some() {
        return Ok(());
    }
    let stub = SlotState {
        slot: Slot::new(prev_period, slot.thread),
        status: SlotStatus::Unknown,
        is_miss: false,
        final_block_id: None,
        candidate_block_ids: Vec::new(),
        execution_trail_hash: None,
        executed_op_ids: Vec::new(),
        sc_event_count: 0,
        completeness: SlotCompleteness::default(),
        first_seen_ts_ms: now_ms,
        last_updated_ts_ms: now_ms,
    };
    db.write_slot(&stub)?;
    info!(
        period = prev_period,
        thread = slot.thread,
        "parent-gap stub inserted (§8.4)"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Unit tests — pure DB, no network.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OperationInclusion, StoredOperation};
    use tempfile::tempdir;

    fn open_db() -> (Db, tempfile::TempDir, SseHub) {
        let dir = tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        (db, dir, SseHub::new(16))
    }

    fn fresh_resp(period: u64, thread: u32) -> FinalSlotResponse {
        FinalSlotResponse {
            period,
            thread,
            final_known: true,
            is_miss: false,
            execution_trail_hash: "trailX".into(),
            ..Default::default()
        }
    }

    #[test]
    fn applies_empty_final_verdict() {
        let (db, _dir, sse) = open_db();
        let resp = fresh_resp(10, 0);
        let out = apply_peer_patch(&db, &sse, &resp, 1).unwrap();
        assert!(out.became_final);
        assert!(!out.block_applied);
        assert!(!out.exec_applied);
        let s = db.read_slot(10, 0).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final);
        assert_eq!(s.execution_trail_hash.as_deref(), Some("trailX"));
    }

    #[test]
    fn preserves_local_trail_hash_on_mismatch() {
        let (db, _dir, sse) = open_db();
        let mut state = SlotState::fresh(Slot::new(5, 0), 0);
        state.status = SlotStatus::Final;
        state.execution_trail_hash = Some("localhash".into());
        db.write_slot(&state).unwrap();

        let mut resp = fresh_resp(5, 0);
        resp.execution_trail_hash = "peerhash".into();
        let out = apply_peer_patch(&db, &sse, &resp, 2).unwrap();
        assert!(out.trail_mismatch);
        assert!(!out.block_applied);
        let back = db.read_slot(5, 0).unwrap().unwrap();
        assert_eq!(back.execution_trail_hash.as_deref(), Some("localhash"));
    }

    #[test]
    fn ensure_parent_stub_inserts_previous_slot() {
        let (db, _dir, _sse) = open_db();
        ensure_parent_stub(&db, Slot::new(100, 3), 42).unwrap();
        let s = db.read_slot(99, 3).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Unknown);
        assert_eq!(s.first_seen_ts_ms, 42);
    }

    #[test]
    fn ensure_parent_stub_noop_if_exists() {
        let (db, _dir, _sse) = open_db();
        let mut pre = SlotState::fresh(Slot::new(99, 3), 100);
        pre.status = SlotStatus::Final;
        db.write_slot(&pre).unwrap();
        ensure_parent_stub(&db, Slot::new(100, 3), 42).unwrap();
        let s = db.read_slot(99, 3).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final); // untouched
    }

    #[test]
    fn applies_operations_without_overwriting_local() {
        use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
        let (db, _dir, sse) = open_db();
        let creator = mk_test_user_addr(7);
        let op_id = mk_test_op_id(7);
        let local_block_id = mk_test_block_id(70);
        let peer_block_id = mk_test_block_id(71);

        // Local op already present with a candidate status.
        let mut local = StoredOperation {
            id: op_id.clone(),
            creator: creator.clone(),
            target: None,
            kind: crate::model::OperationKind::Transaction,
            expire_period: 10,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(5, 0),
                block_id: local_block_id,
            }],
            candidate_exec_status: Some(crate::model::ExecStatus::Ok),
            final_exec_status: None,
            details: Default::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        db.write_op(&local).unwrap();

        // Peer ships the same op id with a fresh/empty shape — our apply
        // MUST NOT overwrite the richer local copy.
        let peer_op = StoredOperation {
            candidate_exec_status: None,
            inclusions: Vec::new(),
            ..local.clone()
        };
        let mut resp = fresh_resp(5, 0);
        resp.operations
            .push(crate::codec::operation_to_peer_pb(&peer_op).unwrap());
        let block = StoredBlock {
            id: peer_block_id.clone(),
            slot: Slot::new(5, 0),
            creator: creator.clone(),
            parents: vec![],
            operation_ids: vec![op_id.clone()],
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
        resp.block = Some(crate::codec::block_to_peer_pb(&block).unwrap());
        resp.final_block_id = peer_block_id.to_string();
        apply_peer_patch(&db, &sse, &resp, 10).unwrap();

        local = db.read_op(&op_id).unwrap().unwrap();
        assert_eq!(local.candidate_exec_status, Some(crate::model::ExecStatus::Ok));
        assert_eq!(local.inclusions.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Denunciations: embedded in the peer-shipped block body MUST be lifted
    // into `cf_denunciation` + its address / recent indexes.
    // -----------------------------------------------------------------------
    #[test]
    fn applies_denunciations_from_block_body() {
        use crate::ids::{mk_test_block_id, mk_test_user_addr};
        use crate::model::StoredDenunciation;
        let (db, _dir, sse) = open_db();
        let creator = mk_test_user_addr(3);
        let denounced = mk_test_user_addr(4);
        let block_id = mk_test_block_id(11);

        let d_addr = StoredDenunciation::Address {
            slot: Slot::new(7, 0),
            address_denounced: denounced.to_string(),
            slashed_nmas: 1_000,
        };
        let d_header = StoredDenunciation::BlockHeader {
            public_key: "pub".into(),
            slot: Slot::new(7, 0),
            hash_1: "h1".into(),
            hash_2: "h2".into(),
            signature_1: "s1".into(),
            signature_2: "s2".into(),
        };

        let block = StoredBlock {
            id: block_id.clone(),
            slot: Slot::new(7, 0),
            creator,
            parents: vec![],
            operation_ids: vec![],
            endorsements: vec![],
            endorsement_ids: vec![],
            denunciations: vec![d_addr.clone(), d_header.clone()],
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

        let mut resp = fresh_resp(7, 0);
        resp.final_block_id = block_id.to_string();
        resp.block = Some(crate::codec::block_to_peer_pb(&block).unwrap());
        apply_peer_patch(&db, &sse, &resp, 42).unwrap();

        // Both denunciations should now be materialised in cf_denunciation
        // with the right included_block_id / included_slot.
        let hash_addr = crate::ingest::denunciation_hash(&d_addr);
        let hash_header = crate::ingest::denunciation_hash(&d_header);
        let got_addr = db.read_denunciation(&hash_addr).unwrap().expect("addr row");
        let got_header = db
            .read_denunciation(&hash_header)
            .unwrap()
            .expect("header row");
        assert_eq!(got_addr.included_block_id.as_ref(), Some(&block_id));
        assert_eq!(got_addr.included_slot, Some(Slot::new(7, 0)));
        assert_eq!(got_addr.kind, "address");
        assert_eq!(got_header.kind, "block_header");

        // The recent index should also pick them up.
        let page = db.iter_denunciations_recent(None, 10).unwrap();
        assert_eq!(page.items.len(), 2);
    }

    // -----------------------------------------------------------------------
    // Async messages: peer-shipped async_msgs land in cf_async_msg + its
    // secondary indexes, and the `idx_async_by_last_slot` index tracks the
    // row's per-slot attribution.
    // -----------------------------------------------------------------------
    #[test]
    fn applies_async_msgs_from_exec_part() {
        use crate::ids::mk_test_user_addr;
        use crate::model::{AsyncMsgState, StoredAsyncMsg};
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(9, 0);
        let sender = mk_test_user_addr(1);
        let dest = mk_test_user_addr(2);

        let m = StoredAsyncMsg {
            id: "async-42".into(),
            sender: Some(sender.clone()),
            destination: Some(dest.clone()),
            handler: Some("tick".into()),
            coins_nmas: 100,
            max_gas: 1_000,
            fee_nmas: 1,
            emission_slot: Some(slot),
            validity_start: None,
            validity_end: None,
            state: AsyncMsgState::Pending,
            last_slot: Some(slot),
            data_hex: None,
            trigger: None,
            can_be_executed: true,
            first_seen_ts_ms: 0,
            last_updated_ts_ms: 0,
        };

        let mut resp = fresh_resp(slot.period, slot.thread as u32);
        resp.async_msgs
            .push(crate::codec::async_msg_to_peer_pb(&m));
        let out = apply_peer_patch(&db, &sse, &resp, 5).unwrap();
        assert!(out.exec_applied, "exec part with async_msgs should apply");

        // Primary row.
        let got = db.read_async_msg("async-42").unwrap().expect("row");
        assert_eq!(got.sender, Some(sender.clone()));
        assert_eq!(got.destination, Some(dest.clone()));
        assert_eq!(got.coins_nmas, 100);

        // Idempotency: reapplying the same patch must be a no-op.
        let out2 = apply_peer_patch(&db, &sse, &resp, 6).unwrap();
        assert!(
            !out2.exec_applied,
            "second apply must skip (exec_output_final already set)"
        );

        // Per-last-slot index is populated — the peer service relies on it.
        let enumerated = db
            .iter_async_msgs_by_last_slot(slot.period, slot.thread, 10)
            .unwrap();
        assert_eq!(enumerated.len(), 1);
        assert_eq!(enumerated[0].id, "async-42");
    }

    // -----------------------------------------------------------------------
    // Deferred calls: peer ships transfers tagged with deferred_call_id +
    // DeferredCall* origin; receiver re-runs reconcile_deferred_from_transfers
    // to materialise cf_deferred_call rows, mirroring live ingest.
    // -----------------------------------------------------------------------
    #[test]
    fn transfers_reconcile_deferred_calls_on_patch() {
        use crate::ids::mk_test_user_addr;
        use crate::model::{
            CoinOrigin, DeferredCallState, StoredTransfer, TransferValue,
        };
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(12, 0);
        let caller = mk_test_user_addr(5);

        // A DeferredCallRegister transfer: caller → runtime, tags the
        // deferred_call_id.
        let t = StoredTransfer {
            slot,
            index_in_slot: 0,
            id: "t1".into(),
            block_id: Some("b1".into()),
            block_timestamp_ms: 0,
            operation_id: Some("op1".into()),
            from: Some(caller.to_string()),
            to: None,
            value: TransferValue::Coins { nmas: 500 },
            origin: CoinOrigin::DeferredCallRegister,
            async_msg_id: None,
            deferred_call_id: Some("dcall-9".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };

        let mut resp = fresh_resp(slot.period, slot.thread as u32);
        resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));

        let out = apply_peer_patch(&db, &sse, &resp, 77).unwrap();
        assert!(out.transfers_applied);

        let got = db.read_deferred_call("dcall-9").unwrap().expect("row");
        assert_eq!(got.state, DeferredCallState::Registered);
        assert_eq!(got.sender, Some(caller));
        assert_eq!(got.registered_slot, Some(slot));

        // Per-last-slot index is populated too.
        let enumerated = db
            .iter_deferred_calls_by_last_slot(slot.period, slot.thread, 10)
            .unwrap();
        assert_eq!(enumerated.len(), 1);
        assert_eq!(enumerated[0].id, "dcall-9");
    }

    // -----------------------------------------------------------------------
    // Async messages shipped by the peer + a later transfer in the same
    // slot must end up with the "executed" state after reconciliation.
    // -----------------------------------------------------------------------
    // -----------------------------------------------------------------------
    // Legacy patch: weaker semantics than peer patch — verifies that we
    // do NOT lock down completeness flags legacy can't actually fill.
    // -----------------------------------------------------------------------
    #[test]
    fn legacy_patch_only_marks_block_body_complete() {
        use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
        let (db, _dir, sse) = open_db();
        let creator = mk_test_user_addr(1);
        let block_id = mk_test_block_id(2);

        let mut resp = fresh_resp(11, 0);
        // Legacy never has an execution_trail_hash.
        resp.execution_trail_hash = String::new();
        resp.final_block_id = block_id.to_string();

        // Block — legacy ALWAYS ships this when it has data.
        let block = StoredBlock {
            id: block_id.clone(),
            slot: Slot::new(11, 0),
            creator: creator.clone(),
            parents: vec![],
            operation_ids: vec![mk_test_op_id(99)],
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
        resp.block = Some(crate::codec::block_to_peer_pb(&block).unwrap());
        // Legacy ships executed_op_ids (but no sc_events / async_msgs).
        resp.executed_op_ids = vec![mk_test_op_id(99).to_string()];

        let out = apply_legacy_patch(&db, &sse, &resp, 50).unwrap();
        assert!(out.became_final, "slot promoted to FINAL");
        assert!(out.block_applied, "block body written");
        assert!(out.exec_applied, "executed_op_ids populated");
        assert!(!out.transfers_applied, "no transfers shipped here");

        let s = db.read_slot(11, 0).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final);
        assert!(
            s.completeness.block_body_stored,
            "block_body_stored set"
        );
        assert!(
            !s.completeness.exec_output_final,
            "exec_output_final left FALSE so a peer can still fill sc_events"
        );
        assert!(
            !s.completeness.transfers_stored,
            "transfers_stored left FALSE so a peer can still fill missing kinds"
        );
        assert_eq!(s.executed_op_ids.len(), 1);
        assert!(
            s.execution_trail_hash.is_none(),
            "legacy never claims a trail hash"
        );
    }

    #[test]
    fn legacy_patch_is_idempotent() {
        use crate::ids::{mk_test_block_id, mk_test_user_addr};
        let (db, _dir, sse) = open_db();
        let creator = mk_test_user_addr(1);
        let block_id = mk_test_block_id(2);
        let block = StoredBlock {
            id: block_id.clone(),
            slot: Slot::new(13, 0),
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
        let mut resp = fresh_resp(13, 0);
        resp.execution_trail_hash = String::new();
        resp.final_block_id = block_id.to_string();
        resp.block = Some(crate::codec::block_to_peer_pb(&block).unwrap());

        let _ = apply_legacy_patch(&db, &sse, &resp, 1).unwrap();
        let out2 = apply_legacy_patch(&db, &sse, &resp, 2).unwrap();
        assert!(!out2.became_final, "slot already FINAL");
        assert!(!out2.block_applied, "block already stored");
    }

    /// Legacy MUST NOT downgrade a slot already filled by a real peer.
    /// Specifically: if a peer patch already set `exec_output_final`, the
    /// legacy patch must leave that bit alone (and likewise transfers).
    #[test]
    fn legacy_patch_never_overrides_richer_local_state() {
        use crate::ids::mk_test_op_id;
        let (db, _dir, sse) = open_db();
        let mut state = SlotState::fresh(Slot::new(20, 0), 0);
        state.status = SlotStatus::Final;
        state.completeness.exec_output_final = true;
        state.completeness.transfers_stored = true;
        state.executed_op_ids = vec![mk_test_op_id(1)];
        state.execution_trail_hash = Some("local_trail".into());
        db.write_slot(&state).unwrap();

        let mut resp = fresh_resp(20, 0);
        resp.execution_trail_hash = String::new();
        // Legacy thinks a different op was executed.
        resp.executed_op_ids = vec![mk_test_op_id(7).to_string()];
        let _ = apply_legacy_patch(&db, &sse, &resp, 100).unwrap();

        let s = db.read_slot(20, 0).unwrap().unwrap();
        assert_eq!(
            s.executed_op_ids[0],
            mk_test_op_id(1),
            "richer local executed_op_ids preserved"
        );
        assert_eq!(s.execution_trail_hash.as_deref(), Some("local_trail"));
        assert!(s.completeness.exec_output_final);
        assert!(s.completeness.transfers_stored);
    }

    /// Legacy ships synthetic Type=Transaction transfers but does NOT mark
    /// `transfers_stored` complete — a real peer may later carry the full
    /// transfer set (block rewards, ABI sub-transfers, async msg coins).
    #[test]
    fn legacy_patch_appends_transfers_without_marking_complete() {
        use crate::ids::mk_test_user_addr;
        use crate::model::{CoinOrigin, StoredTransfer, TransferValue};
        let (db, _dir, sse) = open_db();
        let from = mk_test_user_addr(1);
        let to = mk_test_user_addr(2);

        let t = StoredTransfer {
            slot: Slot::new(30, 0),
            index_in_slot: 0,
            id: "synth-0".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: Some(from.to_string()),
            to: Some(to.to_string()),
            value: TransferValue::Coins { nmas: 42 },
            origin: CoinOrigin::OpTransactionCoins,
            operation_id: Some("op1".into()),
            async_msg_id: None,
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        let mut resp = fresh_resp(30, 0);
        resp.execution_trail_hash = String::new();
        resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));

        let out = apply_legacy_patch(&db, &sse, &resp, 1).unwrap();
        assert!(out.transfers_applied);

        let written = db.iter_transfers_for_slot(30, 0).unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0].id, "synth-0");

        let s = db.read_slot(30, 0).unwrap().unwrap();
        assert!(
            !s.completeness.transfers_stored,
            "transfers_stored stays false for legacy"
        );

        // Reapplying the same patch must be IDEMPOTENT — same end
        // state, no duplicate rows. We do still count it as
        // "transfers_applied" because the legacy patch path
        // unconditionally overwrites (so a newer decoder shipping a
        // richer row at the same key can upgrade in place — see
        // §9.5 in ARCHITECTURE.md).
        let _out2 = apply_legacy_patch(&db, &sse, &resp, 2).unwrap();
        let again = db.iter_transfers_for_slot(30, 0).unwrap();
        assert_eq!(again.len(), 1, "no duplicate transfer rows on re-apply");
        assert_eq!(again[0].id, "synth-0", "row identity stable on re-apply");
    }

    /// Re-applying a legacy patch with a richer payload (e.g. a
    /// new decoder version that fills in `operation_id` from the
    /// hash prefix) MUST upgrade the on-disk row in place.
    #[test]
    fn legacy_patch_overwrites_with_richer_row() {
        use crate::model::{CoinOrigin, StoredTransfer, TransferValue};
        let (db, _dir, sse) = open_db();

        // First apply: bare row, operation_id=None (mimics the
        // pre-fix sub-transfer decoder).
        let bare = StoredTransfer {
            slot: Slot::new(30, 0),
            index_in_slot: 0,
            id: "Op_0".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: None,
            value: TransferValue::Coins { nmas: 100 },
            origin: CoinOrigin::Other { code: 0 },
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        let mut resp = fresh_resp(30, 0);
        resp.execution_trail_hash = String::new();
        resp.transfers.push(crate::codec::transfer_to_peer_pb(&bare));
        let _ = apply_legacy_patch(&db, &sse, &resp, 1).unwrap();
        let stored = db.iter_transfers_for_slot(30, 0).unwrap();
        assert_eq!(stored[0].operation_id, None);

        // Second apply: same (slot, index, id) but with the parent
        // op id derived from the hash AND the proper origin bucket.
        let rich = StoredTransfer {
            operation_id: Some("Op".into()),
            origin: CoinOrigin::AbiTransferCoins,
            ..bare
        };
        let mut resp = fresh_resp(30, 0);
        resp.execution_trail_hash = String::new();
        resp.transfers.push(crate::codec::transfer_to_peer_pb(&rich));
        let _ = apply_legacy_patch(&db, &sse, &resp, 2).unwrap();

        let upgraded = db.iter_transfers_for_slot(30, 0).unwrap();
        assert_eq!(upgraded.len(), 1, "no duplicate after upgrade");
        assert_eq!(
            upgraded[0].operation_id.as_deref(),
            Some("Op"),
            "operation_id was upgraded from the richer payload"
        );
        assert!(matches!(upgraded[0].origin, CoinOrigin::AbiTransferCoins));
    }

    #[test]
    fn async_exec_promotes_to_executed_via_transfer() {
        use crate::ids::mk_test_user_addr;
        use crate::model::{
            AsyncMsgState, CoinOrigin, StoredAsyncMsg, StoredTransfer, TransferValue,
        };
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(14, 0);
        let sender = mk_test_user_addr(1);
        let dest = mk_test_user_addr(2);

        let m = StoredAsyncMsg {
            id: "async-x".into(),
            sender: Some(sender.clone()),
            destination: Some(dest.clone()),
            handler: Some("fire".into()),
            coins_nmas: 100,
            max_gas: 1_000,
            fee_nmas: 0,
            emission_slot: Some(slot),
            validity_start: None,
            validity_end: None,
            state: AsyncMsgState::Pending,
            last_slot: Some(slot),
            data_hex: None,
            trigger: None,
            can_be_executed: true,
            first_seen_ts_ms: 0,
            last_updated_ts_ms: 0,
        };
        let t = StoredTransfer {
            slot,
            index_in_slot: 0,
            id: "tx-async".into(),
            block_id: Some("b1".into()),
            block_timestamp_ms: 0,
            operation_id: None,
            from: None,
            to: Some(dest.to_string()),
            value: TransferValue::Coins { nmas: 100 },
            origin: CoinOrigin::AsyncMsgCoins,
            async_msg_id: Some("async-x".into()),
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };

        let mut resp = fresh_resp(slot.period, slot.thread as u32);
        resp.async_msgs
            .push(crate::codec::async_msg_to_peer_pb(&m));
        resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));

        apply_peer_patch(&db, &sse, &resp, 100).unwrap();

        let got = db.read_async_msg("async-x").unwrap().expect("row");
        assert_eq!(
            got.state,
            AsyncMsgState::Executed,
            "AsyncMsgCoins transfer must promote Pending → Executed on patch"
        );
    }

    // -----------------------------------------------------------------------
    // Restart-hole healing, completeness echo and the chain-linkage arbiter.
    // -----------------------------------------------------------------------

    fn mk_block(id: BlockId, slot: Slot, parents: Vec<BlockId>, ops: Vec<OperationId>) -> StoredBlock {
        StoredBlock {
            id,
            slot,
            creator: crate::ids::mk_test_user_addr(1),
            parents,
            operation_ids: ops,
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
            status: BlockStatus::SeenCandidate,
            first_seen_ts_ms: 0,
        }
    }

    fn mk_op(id: OperationId, slot: Slot, block: BlockId) -> StoredOperation {
        StoredOperation {
            id,
            creator: crate::ids::mk_test_user_addr(2),
            target: None,
            kind: crate::model::OperationKind::Transaction,
            expire_period: 10,
            fee_nmas: 0,
            thread: slot.thread,
            inclusions: vec![OperationInclusion { slot, block_id: block }],
            candidate_exec_status: Some(crate::model::ExecStatus::Ok),
            final_exec_status: None,
            details: Default::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        }
    }

    /// Write a FINAL slot at `(period, 0)` holding `block` (with body) so
    /// linkage checks have a "next block" to inspect.
    fn write_final_with_body(db: &Db, slot: Slot, mut block: StoredBlock) {
        block.status = BlockStatus::Final;
        let mut s = SlotState::fresh(slot, 0);
        s.status = SlotStatus::Final;
        s.final_block_id = Some(block.id.clone());
        s.candidate_block_ids = vec![block.id.clone()];
        s.completeness = SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            transfers_stored: true,
            ..Default::default()
        };
        db.write_block(&block).unwrap();
        db.write_slot(&s).unwrap();
    }

    /// The restart hole: block body arrived live (Candidate), the FINAL
    /// exec never did. A peer's FINAL patch must promote the slot, settle
    /// the block as Final and merge the op's FINAL execution status.
    #[test]
    fn stale_candidate_is_promoted_and_ops_get_final_status() {
        use crate::ids::{mk_test_block_id, mk_test_op_id};
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(20, 0);
        let bid = mk_test_block_id(20);
        let oid = mk_test_op_id(20);

        // Local: candidate slot with body, op without final status.
        db.write_block(&mk_block(bid.clone(), slot, vec![], vec![oid.clone()]))
            .unwrap();
        db.write_op(&mk_op(oid.clone(), slot, bid.clone())).unwrap();
        let mut s = SlotState::fresh(slot, 0);
        s.status = SlotStatus::Candidate;
        s.candidate_block_ids = vec![bid.clone()];
        s.completeness.block_body_stored = true;
        s.completeness.exec_output_candidate = true;
        db.write_slot(&s).unwrap();

        // Peer: same block FINAL, op executed OK, exec part with the op id.
        let mut peer_block = mk_block(bid.clone(), slot, vec![], vec![oid.clone()]);
        peer_block.status = BlockStatus::Final;
        let mut peer_op = mk_op(oid.clone(), slot, bid.clone());
        peer_op.final_exec_status = Some(crate::model::ExecStatus::Ok);
        let mut resp = fresh_resp(20, 0);
        resp.final_block_id = bid.to_string();
        resp.block = Some(crate::codec::block_to_peer_pb(&peer_block).unwrap());
        resp.operations
            .push(crate::codec::operation_to_peer_pb(&peer_op).unwrap());
        resp.executed_op_ids.push(oid.to_string());

        let out = apply_peer_patch(&db, &sse, &resp, 7).unwrap();
        assert!(out.became_final);
        assert!(!out.block_applied, "body was already local");
        assert!(out.exec_applied);

        let back = db.read_slot(20, 0).unwrap().unwrap();
        assert_eq!(back.status, SlotStatus::Final);
        assert_eq!(back.final_block_id.as_ref(), Some(&bid));
        assert!(back.completeness.exec_output_final);
        assert_eq!(back.executed_op_ids, vec![oid.clone()]);

        let blk = db.read_block(&bid).unwrap().unwrap();
        assert_eq!(blk.status, BlockStatus::Final, "block settled despite body being local");

        let op = db.read_op(&oid).unwrap().unwrap();
        assert_eq!(op.final_exec_status, Some(crate::model::ExecStatus::Ok));
    }

    /// A >= 0.0.2 peer that honoured the exec/transfers parts and asserts
    /// it holds them lets us settle the flags on an *empty* payload; an
    /// old peer (no echo) does not.
    #[test]
    fn completeness_echo_settles_empty_parts_only_when_asserted() {
        let (db, _dir, sse) = open_db();
        let honoured = FinalSlotParts {
            block: true,
            exec_output: true,
            transfers: true,
        };

        // Old peer: empty parts, no echo → flags stay unsettled.
        let old = fresh_resp(40, 0);
        apply_peer_patch(&db, &sse, &old, 1).unwrap();
        let s = db.read_slot(40, 0).unwrap().unwrap();
        assert!(!s.completeness.exec_output_final);
        assert!(!s.completeness.transfers_stored);

        // New peer, honoured the parts but does NOT hold them → unsettled.
        let mut new_missing = fresh_resp(40, 0);
        new_missing.completeness_known = true;
        new_missing.parts = Some(honoured);
        apply_peer_patch(&db, &sse, &new_missing, 2).unwrap();
        let s = db.read_slot(40, 0).unwrap().unwrap();
        assert!(!s.completeness.exec_output_final);
        assert!(!s.completeness.transfers_stored);

        // New peer, holds both → settled, even though payloads are empty.
        let mut new_has = fresh_resp(40, 0);
        new_has.completeness_known = true;
        new_has.parts = Some(honoured);
        new_has.has_exec_output = true;
        new_has.has_transfers = true;
        let out = apply_peer_patch(&db, &sse, &new_has, 3).unwrap();
        assert!(out.exec_applied && out.transfers_applied);
        let s = db.read_slot(40, 0).unwrap().unwrap();
        assert!(s.completeness.exec_output_final);
        assert!(s.completeness.transfers_stored);

        // A new peer that only honoured `transfers` must not settle exec.
        let mut only_t = fresh_resp(41, 0);
        only_t.completeness_known = true;
        only_t.parts = Some(FinalSlotParts {
            block: false,
            exec_output: false,
            transfers: true,
        });
        only_t.has_exec_output = true;
        only_t.has_transfers = true;
        apply_peer_patch(&db, &sse, &only_t, 4).unwrap();
        let s = db.read_slot(41, 0).unwrap().unwrap();
        assert!(!s.completeness.exec_output_final);
        assert!(s.completeness.transfers_stored);
    }

    /// Two indexers finalised different blocks for the same slot (dead
    /// fork at an upgrade). The next FINAL block in the thread names its
    /// parent — that settles it. Here the peer's block is the real one.
    #[test]
    fn arbiter_adopts_peer_block_confirmed_by_next_parent() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(100, 0);
        let x = mk_test_block_id(100); // local phantom
        let y = mk_test_block_id(101); // real final block
        let z = mk_test_block_id(102); // next block, builds on y

        // Local: FINAL with phantom X, no body (nobody has it).
        let mut s = SlotState::fresh(slot, 0);
        s.status = SlotStatus::Final;
        s.final_block_id = Some(x.clone());
        s.candidate_block_ids = vec![x.clone()];
        s.execution_trail_hash = Some("local-trail".into());
        db.write_slot(&s).unwrap();
        // We also hold X as a candidate body from the dead fork.
        db.write_block(&mk_block(x.clone(), slot, vec![], vec![])).unwrap();

        // Next slot in thread 0: final block Z with parents[0] = Y.
        write_final_with_body(&db, Slot::new(101, 0), mk_block(z, Slot::new(101, 0), vec![y.clone()], vec![]));

        // Peer proposes Y with body.
        let mut peer_block = mk_block(y.clone(), slot, vec![], vec![]);
        peer_block.status = BlockStatus::Final;
        let mut resp = fresh_resp(100, 0);
        resp.execution_trail_hash = "peer-trail".into();
        resp.final_block_id = y.to_string();
        resp.block = Some(crate::codec::block_to_peer_pb(&peer_block).unwrap());

        let out = apply_peer_patch(&db, &sse, &resp, 9).unwrap();
        assert!(out.divergence_repaired);
        assert!(!out.divergence_kept_local);
        assert!(out.block_applied);

        let back = db.read_slot(100, 0).unwrap().unwrap();
        assert_eq!(back.status, SlotStatus::Final);
        assert_eq!(back.final_block_id.as_ref(), Some(&y));
        assert_eq!(back.execution_trail_hash.as_deref(), Some("peer-trail"));
        assert!(back.completeness.block_body_stored);
        assert_eq!(db.read_block(&x).unwrap().unwrap().status, BlockStatus::Discarded);
        assert_eq!(db.read_block(&y).unwrap().unwrap().status, BlockStatus::Final);

        // Re-applying is a no-op (verdicts now agree).
        let out2 = apply_peer_patch(&db, &sse, &resp, 10).unwrap();
        assert!(!out2.divergence_repaired && !out2.divergence_kept_local);
    }

    /// Same setup but the chain builds on the *local* block: peer is on
    /// the dead fork, local is kept untouched.
    #[test]
    fn arbiter_keeps_local_block_confirmed_by_next_parent() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(200, 0);
        let x = mk_test_block_id(200);
        let y = mk_test_block_id(201);
        let z = mk_test_block_id(202);

        write_final_with_body(&db, slot, mk_block(x.clone(), slot, vec![], vec![]));
        write_final_with_body(&db, Slot::new(201, 0), mk_block(z, Slot::new(201, 0), vec![x.clone()], vec![]));

        let mut resp = fresh_resp(200, 0);
        resp.final_block_id = y.to_string();
        let out = apply_peer_patch(&db, &sse, &resp, 9).unwrap();
        assert!(out.divergence_kept_local);
        assert!(!out.divergence_repaired);
        let back = db.read_slot(200, 0).unwrap().unwrap();
        assert_eq!(back.final_block_id.as_ref(), Some(&x));
        assert_eq!(db.read_block(&x).unwrap().unwrap().status, BlockStatus::Final);
    }

    /// Local says miss although a body was seen; the next block builds on
    /// that body → the miss verdict was wrong, adopt the peer's block.
    #[test]
    fn arbiter_repairs_wrong_miss_verdict() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(300, 0);
        let b = mk_test_block_id(300);
        let z = mk_test_block_id(302);

        let mut s = SlotState::fresh(slot, 0);
        s.status = SlotStatus::Final;
        s.is_miss = true;
        s.candidate_block_ids = vec![b.clone()];
        s.completeness.block_body_stored = true;
        db.write_slot(&s).unwrap();
        db.write_block(&mk_block(b.clone(), slot, vec![], vec![])).unwrap();
        write_final_with_body(&db, Slot::new(301, 0), mk_block(z, Slot::new(301, 0), vec![b.clone()], vec![]));

        let mut peer_block = mk_block(b.clone(), slot, vec![], vec![]);
        peer_block.status = BlockStatus::Final;
        let mut resp = fresh_resp(300, 0);
        resp.final_block_id = b.to_string();
        resp.block = Some(crate::codec::block_to_peer_pb(&peer_block).unwrap());

        let out = apply_peer_patch(&db, &sse, &resp, 9).unwrap();
        assert!(out.divergence_repaired);
        let back = db.read_slot(300, 0).unwrap().unwrap();
        assert!(!back.is_miss);
        assert_eq!(back.final_block_id.as_ref(), Some(&b));
        assert!(back.completeness.block_body_stored);
        assert_eq!(db.read_block(&b).unwrap().unwrap().status, BlockStatus::Final);
    }

    /// Local holds block X, peer says miss, and the next block's parent
    /// is the block *before* our slot: X was never final.
    #[test]
    fn arbiter_adopts_miss_when_chain_skips_local_block() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let prev = mk_test_block_id(398);
        let x = mk_test_block_id(400);
        let z = mk_test_block_id(402);

        write_final_with_body(&db, Slot::new(398, 0), mk_block(prev.clone(), Slot::new(398, 0), vec![], vec![]));
        write_final_with_body(&db, Slot::new(400, 0), mk_block(x.clone(), Slot::new(400, 0), vec![prev.clone()], vec![]));
        write_final_with_body(&db, Slot::new(401, 0), mk_block(z, Slot::new(401, 0), vec![prev.clone()], vec![]));

        let mut resp = fresh_resp(400, 0);
        resp.is_miss = true;
        let out = apply_peer_patch(&db, &sse, &resp, 9).unwrap();
        assert!(out.divergence_repaired);
        let back = db.read_slot(400, 0).unwrap().unwrap();
        assert!(back.is_miss);
        assert!(back.final_block_id.is_none());
        assert_eq!(db.read_block(&x).unwrap().unwrap().status, BlockStatus::Discarded);
    }

    /// A node that finalised a dead fork for several periods leaves an
    /// internally consistent segment (each fork block names the previous
    /// fork block). Mirrors indexer3 after the MAIN.5.0 upgrade: real
    /// chain `…111 → X@113 → R@116 → …119`, local fork
    /// `Y@113 → F1@114 → F2@115 → F3@116 → F4@117`. Applying the peer's
    /// verdicts newest-first must unwind the whole segment.
    #[test]
    fn arbiter_unwinds_dead_fork_segment_via_rejoin() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let t = 0u8;
        let b111 = mk_test_block_id(111);
        let x = mk_test_block_id(113); // real block at 113 (body missing everywhere)
        let r = mk_test_block_id(116); // real block at 116, parents = X
        let b119 = mk_test_block_id(119); // real block at 119, parents = R (rejoin)
        let y = mk_test_block_id(1130);
        let f1 = mk_test_block_id(1140);
        let f2 = mk_test_block_id(1150);
        let f3 = mk_test_block_id(1160);
        let f4 = mk_test_block_id(1170);

        // Local view: consistent chain up to 111, then the dead fork, then
        // the real chain rejoins at 119 naming R (which we do not hold).
        write_final_with_body(&db, Slot::new(111, t), mk_block(b111.clone(), Slot::new(111, t), vec![], vec![]));
        write_final_with_body(&db, Slot::new(113, t), mk_block(y.clone(), Slot::new(113, t), vec![b111.clone()], vec![]));
        write_final_with_body(&db, Slot::new(114, t), mk_block(f1.clone(), Slot::new(114, t), vec![y.clone()], vec![]));
        write_final_with_body(&db, Slot::new(115, t), mk_block(f2.clone(), Slot::new(115, t), vec![f1.clone()], vec![]));
        write_final_with_body(&db, Slot::new(116, t), mk_block(f3.clone(), Slot::new(116, t), vec![f2.clone()], vec![]));
        write_final_with_body(&db, Slot::new(117, t), mk_block(f4.clone(), Slot::new(117, t), vec![f3.clone()], vec![]));
        write_final_with_body(&db, Slot::new(119, t), mk_block(b119, Slot::new(119, t), vec![r.clone()], vec![]));

        // Peer verdicts, applied newest-first like the range stream does.
        // 117: miss.
        let mut p117 = fresh_resp(117, 0);
        p117.is_miss = true;
        assert!(apply_peer_patch(&db, &sse, &p117, 1).unwrap().divergence_repaired, "117 → miss");
        // 116: R with body (parents[0] = X).
        let mut rb = mk_block(r.clone(), Slot::new(116, t), vec![x.clone()], vec![]);
        rb.status = BlockStatus::Final;
        let mut p116 = fresh_resp(116, 0);
        p116.final_block_id = r.to_string();
        p116.block = Some(crate::codec::block_to_peer_pb(&rb).unwrap());
        assert!(apply_peer_patch(&db, &sse, &p116, 2).unwrap().divergence_repaired, "116 → R");
        // 115, 114: misses.
        let mut p115 = fresh_resp(115, 0);
        p115.is_miss = true;
        assert!(apply_peer_patch(&db, &sse, &p115, 3).unwrap().divergence_repaired, "115 → miss");
        let mut p114 = fresh_resp(114, 0);
        p114.is_miss = true;
        assert!(apply_peer_patch(&db, &sse, &p114, 4).unwrap().divergence_repaired, "114 → miss");
        // 113: X, no body available anywhere.
        let mut p113 = fresh_resp(113, 0);
        p113.final_block_id = x.to_string();
        assert!(apply_peer_patch(&db, &sse, &p113, 5).unwrap().divergence_repaired, "113 → X");

        // Final local state matches the real chain.
        let s113 = db.read_slot(113, t).unwrap().unwrap();
        assert_eq!(s113.final_block_id.as_ref(), Some(&x));
        assert!(!s113.is_miss && !s113.completeness.block_body_stored);
        for p in [114u64, 115, 117] {
            let s = db.read_slot(p, t).unwrap().unwrap();
            assert!(s.is_miss, "slot {p} must be a miss");
            assert!(s.final_block_id.is_none());
        }
        let s116 = db.read_slot(116, t).unwrap().unwrap();
        assert_eq!(s116.final_block_id.as_ref(), Some(&r));
        assert!(s116.completeness.block_body_stored);
        for dead in [&y, &f1, &f2, &f3, &f4] {
            assert_eq!(db.read_block(dead).unwrap().unwrap().status, BlockStatus::Discarded);
        }
        // Chain is now consistent: no anchor, and a re-offer of the fork
        // verdicts is refused.
        assert!(dead_fork_anchor(&db, Slot::new(113, t), &Verdict::Block(x.clone())).unwrap().is_none());
        let mut fork_again = fresh_resp(113, 0);
        fork_again.final_block_id = y.to_string();
        let out = apply_peer_patch(&db, &sse, &fork_again, 6).unwrap();
        assert!(out.divergence_kept_local && !out.divergence_repaired);
    }

    /// The real-chain holder (indexer1's view) must never yield to a fork
    /// verdict, even when the real block's body is missing.
    #[test]
    fn arbiter_real_chain_without_body_keeps_local_against_fork() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let t = 0u8;
        let b111 = mk_test_block_id(111);
        let x = mk_test_block_id(113);
        let r = mk_test_block_id(116);
        let y = mk_test_block_id(1130);
        write_final_with_body(&db, Slot::new(111, t), mk_block(b111, Slot::new(111, t), vec![], vec![]));
        // 113 = X, FINAL, body missing.
        let mut s = SlotState::fresh(Slot::new(113, t), 0);
        s.status = SlotStatus::Final;
        s.final_block_id = Some(x.clone());
        db.write_slot(&s).unwrap();
        for p in [114u64, 115] {
            let mut m = SlotState::fresh(Slot::new(p, t), 0);
            m.status = SlotStatus::Final;
            m.is_miss = true;
            m.completeness.block_body_stored = true;
            db.write_slot(&m).unwrap();
        }
        write_final_with_body(&db, Slot::new(116, t), mk_block(r, Slot::new(116, t), vec![x.clone()], vec![]));

        let mut fork = fresh_resp(113, 0);
        fork.final_block_id = y.to_string();
        let mut yb = mk_block(y.clone(), Slot::new(113, t), vec![], vec![]);
        yb.status = BlockStatus::Final;
        fork.block = Some(crate::codec::block_to_peer_pb(&yb).unwrap());
        let out = apply_peer_patch(&db, &sse, &fork, 1).unwrap();
        assert!(out.divergence_kept_local);
        assert_eq!(db.read_slot(113, t).unwrap().unwrap().final_block_id.as_ref(), Some(&x));
        // A fork "block" offered for a real miss is refused too.
        let mut fork_block_at_miss = fresh_resp(114, 0);
        fork_block_at_miss.final_block_id = mk_test_block_id(1140).to_string();
        let out = apply_peer_patch(&db, &sse, &fork_block_at_miss, 2).unwrap();
        assert!(out.divergence_kept_local);
        assert!(db.read_slot(114, t).unwrap().unwrap().is_miss);
    }

    /// No later block to consult → inconclusive → local kept, nothing
    /// rewritten.
    #[test]
    fn arbiter_inconclusive_keeps_local() {
        use crate::ids::mk_test_block_id;
        let (db, _dir, sse) = open_db();
        let slot = Slot::new(500, 0);
        let x = mk_test_block_id(500);
        let y = mk_test_block_id(501);
        write_final_with_body(&db, slot, mk_block(x.clone(), slot, vec![], vec![]));

        let mut resp = fresh_resp(500, 0);
        resp.final_block_id = y.to_string();
        let out = apply_peer_patch(&db, &sse, &resp, 9).unwrap();
        assert!(out.divergence_kept_local);
        assert_eq!(
            arbitrate_verdicts(&db, slot, &Verdict::Block(x.clone()), &Verdict::Block(y)).unwrap(),
            Arbitration::Inconclusive
        );
        let back = db.read_slot(500, 0).unwrap().unwrap();
        assert_eq!(back.final_block_id.as_ref(), Some(&x));
    }
}
