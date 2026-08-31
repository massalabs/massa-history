//! Ingestion state machine.
//!
//! Single writer per indexer. All mutations are funneled through an `mpsc`
//! channel into this loop so that a transition on a given slot is atomic.
//!
//! The loop handles the following event kinds, all optional — a stream that
//! is disabled in `[streams]` simply never produces events here, and the
//! slot-completeness logic ignores the corresponding bit when deciding
//! whether a slot is "done":
//!   * `Block` — from `NewFilledBlocksServer` (wraps `FilledBlock`).
//!   * `Exec`  — from `NewSlotExecutionOutputsServer` (wraps `SlotExecutionOutput`).
//!   * `Transfers` — from `NewTransfersInfoServer`.
//!   * `PeerPatch` — a `FinalSlotResponse` shipped by another indexer via
//!     the peer protocol (see `peer::patch::apply_peer_patch`).
//!
//! All transitions are **idempotent**: re-applying the same event produces
//! exactly the same RocksDB state.

use crate::{
    db::Db,
    ids::{Address, BlockId, OperationId},
    model::{
        AsyncMsgState, AsyncTrigger, BlockStatus, CoinOrigin, DatastoreEntry, DeferredCallState,
        ExecStatus, OperationDetails, OperationInclusion, OperationKind, Slot, SlotState,
        SlotStatus, StoredAsyncMsg, StoredBlock, StoredDenunciation, StoredDenunciationEntry,
        StoredDeferredCall, StoredEndorsement, StoredOperation, StoredScEvent, StoredTransfer,
        TransferValue,
    },
    proto::massa::{api::v1 as api, model::v1 as m},
    sse::SseHub,
    Error, Result,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use prost::Message;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug)]
pub enum Event {
    Block(Box<m::FilledBlock>),
    Exec(Box<m::SlotExecutionOutput>),
    Transfers(Box<api::NewTransfersInfoServerResponse>),
    /// A `FinalSlotResponse` received from a peer indexer via the backfill
    /// worker. Handled on the single-writer thread exactly like a live event
    /// (see `peer::patch::apply_peer_patch`) to preserve ordering invariants.
    PeerPatch(Box<crate::proto::indexer::v1::FinalSlotResponse>),
    /// A `FinalSlotResponse` synthesised from the legacy block-storer
    /// DDB tables (see `crate::legacy`). Routed through `apply_legacy_patch`
    /// rather than `apply_peer_patch` because legacy data is partial:
    /// blocks/operations/endorsements are authoritative but SC events,
    /// async-pool rows and most transfer kinds are not — see
    /// `peer::patch::apply_legacy_patch` for the full precedence rules.
    LegacyPatch(Box<crate::proto::indexer::v1::FinalSlotResponse>),
    Tick,
}

pub type EventTx = mpsc::Sender<Event>;
pub type EventRx = mpsc::Receiver<Event>;

/// Published to SSE subscribers after every slot-affecting ingest.
///
/// The frontend filters these by `type` for the per-topic SSE routes
/// (`/v1/stream/blocks`, `/v1/stream/final`, etc.). Keeping them on a single
/// broadcast channel avoids maintaining one replay ring per topic; the extra
/// bandwidth is negligible since each frame is a single JSON object.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SlotSseEvent {
    SlotUpdated(SlotState),
    /// Published when a new `StoredBlock` is written (both seen_candidate and
    /// final/discarded transitions). Carries the block for the /stream/blocks
    /// consumer.
    BlockSeen(StoredBlock),
    /// Published when a slot flips from non-final to final.
    SlotFinal(SlotState),
    /// Published per-operation when the op is first ingested from a block.
    OpSeen(StoredOperation),
    /// Published per-event at ingest time.
    EventSeen(StoredScEvent),
    Heartbeat { server_time_ms: i64 },
}

pub struct Ingest {
    pub db: Db,
    pub sse: SseHub,
    pub rx: EventRx,
    /// Shared Prometheus counters. Optional so unit tests can construct an
    /// `Ingest` without a full metrics dependency chain.
    pub metrics: Option<std::sync::Arc<crate::metrics::Metrics>>,
}

impl Ingest {
    pub fn new(db: Db, sse: SseHub, rx: EventRx) -> Self {
        Self { db, sse, rx, metrics: None }
    }

    pub fn with_metrics(mut self, metrics: std::sync::Arc<crate::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn bump(&self, f: impl Fn(&crate::metrics::Metrics)) {
        if let Some(m) = &self.metrics {
            f(m);
        }
    }

    pub async fn run(mut self) {
        info!("ingest worker running");
        while let Some(ev) = self.rx.recv().await {
            match ev {
                Event::Block(fb) => {
                    if let Err(e) = self.handle_block(*fb) {
                        warn!(error = %e, "handle_block");
                        self.bump(|m| {
                            m.ingest_events_dropped_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    } else {
                        self.bump(|m| {
                            m.ingest_blocks_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                }
                Event::Exec(out) => {
                    if let Err(e) = self.handle_exec(*out) {
                        warn!(error = %e, "handle_exec");
                        self.bump(|m| {
                            m.ingest_events_dropped_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    } else {
                        self.bump(|m| {
                            m.ingest_exec_outputs_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                }
                Event::Transfers(resp) => {
                    if let Err(e) = self.handle_transfers(*resp) {
                        warn!(error = %e, "handle_transfers");
                        self.bump(|m| {
                            m.ingest_events_dropped_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    } else {
                        self.bump(|m| {
                            m.ingest_transfers_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                }
                Event::PeerPatch(resp) => {
                    if let Err(e) = crate::peer::apply_peer_patch(
                        &self.db,
                        &self.sse,
                        resp.as_ref(),
                        now_ms(),
                    ) {
                        warn!(error = %e, "apply_peer_patch");
                        self.bump(|m| {
                            m.ingest_events_dropped_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    } else {
                        self.bump(|m| {
                            m.ingest_peer_patches_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                }
                Event::LegacyPatch(resp) => {
                    if let Err(e) = crate::peer::apply_legacy_patch(
                        &self.db,
                        &self.sse,
                        resp.as_ref(),
                        now_ms(),
                    ) {
                        warn!(error = %e, "apply_legacy_patch");
                        self.bump(|m| {
                            m.ingest_events_dropped_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    } else {
                        self.bump(|m| {
                            m.ingest_legacy_patches_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        });
                    }
                }
                Event::Tick => {
                    let ts = now_ms();
                    self.sse.broadcast(SlotSseEvent::Heartbeat { server_time_ms: ts });
                }
            }
        }
        info!("ingest worker stopping");
    }

    // -----------------------------------------------------------------------
    // E_BLOCK
    // -----------------------------------------------------------------------
    fn handle_block(&mut self, fb: m::FilledBlock) -> Result<()> {
        let header = fb
            .header
            .as_ref()
            .ok_or_else(|| Error::other("FilledBlock missing header"))?;
        let block_hdr = header
            .content
            .as_ref()
            .ok_or_else(|| Error::other("SignedBlockHeader missing content"))?;
        let block_id_str = header.secure_hash.clone();
        if block_id_str.is_empty() {
            return Err(Error::other("empty block id"));
        }
        let block_id = BlockId::parse(&block_id_str).map_err(Error::other)?;

        let slot = block_hdr
            .slot
            .as_ref()
            .ok_or_else(|| Error::other("block header missing slot"))?;
        let slot = Slot::new(slot.period, slot.thread as u8);

        let creator = Address::parse(&header.content_creator_address).map_err(Error::other)?;

        let parents = block_hdr
            .parents
            .iter()
            .filter_map(|p| BlockId::parse(p.clone()).ok())
            .collect::<Vec<_>>();

        let now_ms = now_ms();

        // Decode full endorsements + denunciations for archival.
        let endorsements_full: Vec<StoredEndorsement> = block_hdr
            .endorsements
            .iter()
            .filter_map(|se| decode_endorsement(se, &block_id, slot, now_ms))
            .collect();
        let endorsement_ids: Vec<String> =
            endorsements_full.iter().map(|e| e.id.clone()).collect();

        // Persist endorsements into their own CF for per-id lookups.
        for end in &endorsements_full {
            self.db.write_endorsement(end)?;
        }

        let denunciations_full: Vec<StoredDenunciation> = block_hdr
            .denunciations
            .iter()
            .map(decode_denunciation)
            .collect();

        // Persist every denunciation in its own CF so a future address-page
        // tab can list slashes quickly and /v1/denunciations/{hash} works.
        // We content-address each denunciation with a SHA-256 over its
        // structured JSON; the node's proto has no `secure_hash` for
        // denunciations.
        for d in &denunciations_full {
            let hash = denunciation_hash(d);
            let slot = denunciation_slot(d);
            let kind_label = denunciation_kind_label(d);
            let addr = denunciation_target_address(d);
            let entry = StoredDenunciationEntry {
                hash,
                slot,
                kind: kind_label.into(),
                denounced_addr: addr,
                denunciation: d.clone(),
                included_block_id: Some(block_id.clone()),
                included_slot: Some(slot),
                first_seen_ts_ms: now_ms,
            };
            if let Err(e) = self.db.write_denunciation(&entry) {
                warn!(error = %e, "write_denunciation");
            }
        }

        let mut operation_ids = Vec::with_capacity(fb.operations.len());
        for filled_op in &fb.operations {
            let Some(signed_op) = &filled_op.operation else { continue };
            let op_id_str = if !filled_op.operation_id.is_empty() {
                filled_op.operation_id.clone()
            } else {
                signed_op.secure_hash.clone()
            };
            if op_id_str.is_empty() {
                continue;
            }
            let op_id = match OperationId::parse(&op_id_str) {
                Ok(x) => x,
                Err(e) => {
                    warn!(op_id = %op_id_str, err = %e, "invalid op id");
                    continue;
                }
            };
            operation_ids.push(op_id.clone());

            let mut stored = match self.db.read_op(&op_id)? {
                Some(s) => s,
                None => {
                    let content = signed_op
                        .content
                        .as_ref()
                        .ok_or_else(|| Error::other("op missing content"))?;
                    let op_type_ref = content.op.as_ref();
                    let (kind, target, details) = classify_op(op_type_ref);
                    let fee_nmas = content
                        .fee
                        .as_ref()
                        .map(|a| a.mantissa)
                        .unwrap_or(0);
                    let creator_addr = Address::parse(&signed_op.content_creator_address)
                        .map_err(Error::other)?;
                    StoredOperation {
                        id: op_id.clone(),
                        creator: creator_addr,
                        target,
                        kind,
                        expire_period: content.expire_period,
                        fee_nmas,
                        thread: slot.thread,
                        inclusions: Vec::new(),
                        candidate_exec_status: None,
                        final_exec_status: None,
                        details,
                        signature: signed_op.signature.clone(),
                        content_creator_pub_key: signed_op.content_creator_pub_key.clone(),
                        serialized_size: signed_op.serialized_size,
                        raw_signed_op_b64: B64.encode(signed_op.encode_to_vec()),
                        first_seen_ts_ms: now_ms,
                    }
                }
            };
            stored.thread = slot.thread;
            // Record this block as an inclusion site if we've never seen this
            // (slot, block) pair for this op. Stable ordering: we keep the
            // first-seen pair at index 0 so `inclusions[0]` mirrors
            // `first_included_*`.
            let already = stored
                .inclusions
                .iter()
                .any(|inc| inc.block_id == block_id && inc.slot == slot);
            if !already {
                stored.inclusions.push(OperationInclusion {
                    slot,
                    block_id: block_id.clone(),
                });
            }
            self.db.write_op(&stored)?;
            // Broadcast a per-op frame so /v1/stream/operations (+ address
            // filters) don't have to wait for the slot update.
            self.sse.broadcast(SlotSseEvent::OpSeen(stored));
        }

        // Load the slot state first so we can determine the correct initial
        // status (E_BLOCK can arrive after the slot is already FINAL).
        let mut slot_state = self
            .db
            .read_slot(slot.period, slot.thread)?
            .unwrap_or_else(|| SlotState::fresh(slot, now_ms));

        let initial_block_status = if slot_state.status == SlotStatus::Final {
            if slot_state.final_block_id.as_ref() == Some(&block_id) {
                BlockStatus::Final
            } else {
                BlockStatus::Discarded
            }
        } else {
            BlockStatus::SeenCandidate
        };

        let raw_header_b64 = B64.encode(header.encode_to_vec());

        let prior = self.db.read_block(&block_id)?;
        let block_row = match prior {
            Some(mut b) => {
                if b.operation_ids.is_empty() {
                    b.operation_ids = operation_ids.clone();
                }
                b.endorsements = endorsements_full.clone();
                b.endorsement_ids = endorsement_ids.clone();
                b.denunciations = denunciations_full.clone();
                // Only upgrade seen_candidate → final/discarded; never downgrade.
                if b.status == BlockStatus::SeenCandidate {
                    b.status = initial_block_status;
                }
                b
            }
            None => StoredBlock {
                id: block_id.clone(),
                slot,
                creator: creator.clone(),
                parents,
                operation_ids: operation_ids.clone(),
                endorsements: endorsements_full,
                endorsement_ids,
                denunciations: denunciations_full,
                current_version: block_hdr.current_version,
                announced_version: block_hdr.announced_version,
                operations_hash: block_hdr.operations_hash.clone(),
                signature: header.signature.clone(),
                content_creator_pub_key: header.content_creator_pub_key.clone(),
                serialized_size: header.serialized_size,
                raw_signed_header_b64: raw_header_b64,
                status: initial_block_status,
                first_seen_ts_ms: now_ms,
            },
        };
        self.db.write_block(&block_row)?;
        self.sse.broadcast(SlotSseEvent::BlockSeen(block_row.clone()));

        if !slot_state.candidate_block_ids.iter().any(|b| *b == block_id) {
            slot_state.candidate_block_ids.push(block_id.clone());
        }
        slot_state.completeness.block_body_stored = true;
        slot_state.last_updated_ts_ms = now_ms;
        if slot_state.status == SlotStatus::Unknown {
            slot_state.status = SlotStatus::Candidate;
        }
        self.db.write_slot(&slot_state)?;

        // Parent-gap discovery (§8.4): make sure the slot right before ours in
        // this thread has at least an Unknown row, so the backfill scanner
        // will pick it up on the next pass if we're missing data there.
        crate::peer::ensure_parent_stub(&self.db, slot, now_ms)?;

        debug!(
            period = slot.period,
            thread = slot.thread,
            block = %block_id,
            n_ops = operation_ids.len(),
            "block ingested"
        );
        self.sse.broadcast(SlotSseEvent::SlotUpdated(slot_state));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // E_EXEC
    // -----------------------------------------------------------------------
    fn handle_exec(&mut self, outer: m::SlotExecutionOutput) -> Result<()> {
        let status = m::ExecutionOutputStatus::try_from(outer.status)
            .unwrap_or(m::ExecutionOutputStatus::Unspecified);
        let exec = outer
            .execution_output
            .ok_or_else(|| Error::other("SlotExecutionOutput missing execution_output"))?;
        let slot_pb = exec
            .slot
            .as_ref()
            .ok_or_else(|| Error::other("execution_output missing slot"))?;
        let slot = Slot::new(slot_pb.period, slot_pb.thread as u8);
        let is_final = matches!(status, m::ExecutionOutputStatus::Final);
        let is_candidate = matches!(status, m::ExecutionOutputStatus::Candidate);
        if !is_final && !is_candidate {
            // ignore: UNSPECIFIED / UNKNOWN / READ_ONLY variants
            return Ok(());
        }

        // Optional trail hash from state changes.
        let trail_hash = exec
            .state_changes
            .as_ref()
            .and_then(|sc| sc.execution_trail_hash_change.as_ref())
            .and_then(|ch| match ch.change.as_ref() {
                Some(m::set_or_keep_string::Change::Set(sv)) => Some(sv.clone()),
                _ => None,
            });

        let now_ms = now_ms();

        let mut state = self
            .db
            .read_slot(slot.period, slot.thread)?
            .unwrap_or_else(|| SlotState::fresh(slot, now_ms));

        // First-final-wins (§7.1)
        if state.status == SlotStatus::Final {
            if is_final {
                if let (Some(existing), Some(incoming)) =
                    (state.execution_trail_hash.as_deref(), trail_hash.as_deref())
                {
                    if existing != incoming {
                        warn!(
                            period = slot.period,
                            thread = slot.thread,
                            existing,
                            incoming,
                            "FINAL trail-hash divergence ignored (first-wins)"
                        );
                    }
                }
            }
            return Ok(());
        }

        state.status = if is_final { SlotStatus::Final } else { SlotStatus::Candidate };
        if let Some(h) = trail_hash {
            state.execution_trail_hash = Some(h);
        }

        // SC events: always replace the slot's event set (idempotent for FINAL
        // since we recompute the same value).
        self.db.clear_sc_events_for_slot(slot.period, slot.thread)?;
        let mut sc_count = 0u32;
        for (i, ev) in exec.events.iter().enumerate() {
            let (emitters, callers, op_id) = extract_event_context(ev);
            let stored = StoredScEvent {
                slot,
                index_in_slot: i as u32,
                data: stringify_bytes(&ev.data),
                emitter_addrs: emitters,
                caller_addrs: callers,
                status: state.status,
                op_id,
            };
            self.db.write_sc_event(&stored)?;
            self.sse.broadcast(SlotSseEvent::EventSeen(stored));
            sc_count += 1;
        }
        state.sc_event_count = sc_count;

        if is_final {
            state.completeness.exec_output_final = true;
            state.is_miss = exec.block_id.is_none();
            if let Some(bid_s) = exec.block_id.as_ref() {
                if let Ok(bid) = BlockId::parse(bid_s.clone()) {
                    state.final_block_id = Some(bid);
                }
            }
            self.db.update_last_final_slot(&slot)?;

            // Propagate finality to blocks stored for this slot:
            //  - the winner (final_block_id) gets BlockStatus::Final
            //  - any other candidate we've seen gets BlockStatus::Discarded
            // This makes block detail pages converge to the right status without
            // the client having to cross-reference slot state.
            self.apply_finality_to_blocks(
                state.final_block_id.as_ref(),
                &state.candidate_block_ids,
            )?;
        } else {
            state.completeness.exec_output_candidate = true;
            self.db.update_last_candidate_slot(&slot)?;
        }

        // Apply per-operation execution statuses from executed_ops_changes.
        // Only ops we've already stored (via E_BLOCK) get updated; others are
        // silently ignored and will pick up the right status once their block
        // body arrives (the node re-emits on reconnect).
        if let Some(sc) = exec.state_changes.as_ref() {
            let op_ids_in_slot =
                self.apply_executed_ops_changes(&sc.executed_ops_changes, is_final)?;
            if !op_ids_in_slot.is_empty() && state.executed_op_ids.is_empty() {
                state.executed_op_ids = op_ids_in_slot;
            }
        }

        // Async-pool ingestion (§9.2 & §5.1). We only materialise async-pool
        // rows from FINAL frames: candidate frames can be reverted when a
        // fork loses the race, and the node faithfully re-emits everything
        // on the next FINAL so nothing is lost by skipping them here.
        //
        // NOTE: at this point we have already written the SC events + op
        // statuses for the slot, but the transfer stream for this slot may
        // arrive after us (it runs on a separate bidi RPC with its own
        // backpressure). That's fine — `apply_async_pool_changes` only
        // needs the `AsyncPoolChangeEntry` data from `state_changes`; any
        // cross-referencing with transfers happens on the transfer path
        // (see `reconcile_async_delete_from_transfers`) once the transfer
        // list is durable.
        if is_final {
            if let Some(sc) = exec.state_changes.as_ref() {
                self.apply_async_pool_changes(slot, &sc.async_pool_changes, now_ms)?;
            }
        }

        state.last_updated_ts_ms = now_ms;
        self.db.write_slot(&state)?;

        // Parent-gap discovery (§8.4): stub the previous slot in this thread
        // on every FINAL exec so the backfill scanner can walk history
        // backwards one slot at a time. Candidates don't trigger — they might
        // never finalise.
        if is_final {
            crate::peer::ensure_parent_stub(&self.db, slot, now_ms)?;
        }

        debug!(
            period = slot.period,
            thread = slot.thread,
            is_final,
            sc_count,
            "exec ingested"
        );
        if is_final {
            self.sse.broadcast(SlotSseEvent::SlotFinal(state.clone()));
            if let Some(m) = &self.metrics {
                m.slots_finalized_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if state.is_miss {
                    m.slots_missed_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        self.sse.broadcast(SlotSseEvent::SlotUpdated(state));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // E_TRANSFERS
    // -----------------------------------------------------------------------
    fn handle_transfers(&mut self, resp: api::NewTransfersInfoServerResponse) -> Result<()> {
        let Some(slot_pb) = resp.slot.as_ref() else {
            return Err(Error::other("NewTransfersInfoServerResponse missing slot"));
        };
        let slot = Slot::new(slot_pb.period, slot_pb.thread as u8);
        let now_ms = now_ms();
        let block_id = resp.block_id.clone();
        let block_ts_ms = resp.timestamp;

        // Each response carries the *full* FINAL transfer list for the slot,
        // so we treat it as a replacement of the slot's transfer rows.
        self.db.clear_transfers_for_slot(slot.period, slot.thread)?;

        // --- Pass 1: decode proto transfers into our in-memory rows.
        // We keep the node's attributions verbatim here; operation inference
        // happens in pass 2 below.
        //
        // Zero-value transfers are intentionally dropped here. They carry no
        // economic signal (the SC runtime emits `coin` transfers for every
        // call even when the attached coin amount is 0), and indexing them
        // would clutter every address/operation/slot transfer view with
        // visual noise. Rolls / deferred credits with count 0 are
        // uninteresting for the same reason.
        //
        // NB: we re-number `index_in_slot` after the filter so the stored
        // indices stay dense; the `id` field from the node is still the
        // canonical per-transfer identity.
        let mut rows: Vec<StoredTransfer> = Vec::with_capacity(resp.transfers_info.len());
        let mut next_idx: u32 = 0;
        for t in resp.transfers_info.iter() {
            let value = match t.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(m::transfer_value::Value::Rolls(n)) => TransferValue::Rolls { count: *n },
                Some(m::transfer_value::Value::Coins(a)) => TransferValue::Coins {
                    nmas: native_amount_to_nmas(a),
                },
                Some(m::transfer_value::Value::DeferredCredits(a)) => {
                    TransferValue::DeferredCredits {
                        nmas: native_amount_to_nmas(a),
                    }
                }
                None => TransferValue::Unknown,
            };
            if is_zero_value(&value) {
                continue;
            }
            let origin = coin_origin_from_i32(t.origin);
            let i = next_idx;
            next_idx += 1;
            rows.push(StoredTransfer {
                slot,
                index_in_slot: i,
                id: t.id.clone(),
                block_id: block_id.clone(),
                block_timestamp_ms: block_ts_ms,
                from: t.from_address.clone(),
                to: t.to_address.clone(),
                value,
                origin,
                operation_id: t.operation_id.clone(),
                async_msg_id: t.async_msg_id.clone(),
                deferred_call_id: t.deferred_call_id.clone(),
                denunciation_index: t.denunciation_index.as_ref().map(format_denunciation_index),
                is_final: true,
                first_seen_ts_ms: now_ms,
            });
        }

        // --- Pass 2: infer the parent `operation_id` for ABI-level side
        // effects emitted while a user operation was being executed.
        //
        // The node tags only the *operation-root* transfers (OpTransactionFees,
        // OpCallscCoins, OpRollBuy …) with `operation_id`. Child transfers
        // produced from inside the SC VM (abi_transfer_coins, abi_call_coins,
        // datastore_storage, set_bytecode_storage, create_sc_storage, etc.)
        // are left unattributed — which is why a `claim` SC call would show
        // its fee but not the coins actually paid out.
        //
        // Transfers are emitted in strict execution order within a slot, so
        // we walk them linearly and stick the currently-active `operation_id`
        // on every child transfer until we hit a boundary (a new root op,
        // async message, deferred call, denunciation slash, or a reward).
        //
        // This is exactly the grouping `NewSlotAbiCallStacks` would give us
        // for free; re-deriving it here keeps the feature cheap and works
        // without subscribing to the call-stack stream.
        let mut current_op: Option<String> = None;
        for r in rows.iter_mut() {
            if r.operation_id.is_some() {
                current_op = r.operation_id.clone();
                continue;
            }
            // Hard boundaries: rewards, denunciation slashes and events owned
            // by async messages / deferred calls are unrelated to any user op.
            if r.async_msg_id.is_some()
                || r.deferred_call_id.is_some()
                || r.denunciation_index.is_some()
            {
                current_op = None;
                continue;
            }
            if is_reward_origin(&r.origin) {
                current_op = None;
                continue;
            }
            // ABI side-effects & SC storage housekeeping ride with the active op.
            if is_op_child_origin(&r.origin) {
                if let Some(ref op) = current_op {
                    r.operation_id = Some(op.clone());
                }
            }
        }

        for r in &rows {
            self.db.write_transfer(r)?;
        }

        // Cross-reference transfers with the async pool rows we ingested
        // from `state_changes` (if any): DELETE + AsyncMsgCoins → Executed,
        // DELETE + AsyncMsgCancel → Cancelled. We also create minimal rows
        // for async ids we see here but never saw in the pool stream, so
        // the explorer can resolve every async_msg_id on a transfer.
        reconcile_async_from_transfers(&self.db, slot, &rows, now_ms)?;
        // Deferred calls don't appear in `state_changes` at all on the
        // node proto we subscribe to; the transfer stream is the sole
        // source.
        reconcile_deferred_from_transfers(&self.db, slot, &rows, now_ms)?;

        // Mark completeness on the slot so the UI knows transfers are in.
        if let Some(mut state) = self.db.read_slot(slot.period, slot.thread)? {
            if !state.completeness.transfers_stored {
                state.completeness.transfers_stored = true;
                state.last_updated_ts_ms = now_ms;
                self.db.write_slot(&state)?;
                self.sse.broadcast(SlotSseEvent::SlotUpdated(state));
            }
        } else {
            let mut fresh = SlotState::fresh(slot, now_ms);
            fresh.completeness.transfers_stored = true;
            self.db.write_slot(&fresh)?;
            self.sse.broadcast(SlotSseEvent::SlotUpdated(fresh));
        }

        debug!(
            period = slot.period,
            thread = slot.thread,
            n = resp.transfers_info.len(),
            "transfers ingested"
        );
        Ok(())
    }

    /// Materialise `AsyncPoolChangeEntry` rows emitted by the node's
    /// `state_changes` into `cf_async_msg` (+ secondary indexes).
    ///
    /// The proto spec encodes three kinds of change:
    ///
    /// * `SET(msg_id, created_message)` — the pool grew by one. We upsert a
    ///   `StoredAsyncMsg` with the full message body. If a row already
    ///   exists for this id (because we've seen it before and it was
    ///   updated), the upsert preserves `first_seen_ts_ms`.
    ///
    /// * `UPDATE(msg_id, updated_message)` — one or more fields changed.
    ///   Each `AsyncMessageUpdate` field is a `SetOrKeep`; we apply the
    ///   `Set`s over the current row and leave every `Keep` untouched.
    ///   If no row exists yet (we tuned in mid-flight), we synthesise a
    ///   minimal row from the updates so the explorer still has
    ///   *something* to render.
    ///
    /// * `DELETE(msg_id)` — the pool shrank. Two reasons the node does
    ///   this: the message executed (successfully or otherwise) or it was
    ///   cancelled (expired / filter never hit / refund). We can't tell
    ///   the reasons apart from the pool event alone — the transfer
    ///   stream for the same slot contains a row with `CoinOrigin::
    ///   AsyncMsgCoins` (executed) or `CoinOrigin::AsyncMsgCancel`
    ///   (cancelled) for the same `async_msg_id`. We therefore mark the
    ///   row as `Consumed` here and let [`Self::handle_transfers`]
    ///   upgrade it to `Executed` or `Cancelled` once the transfers
    ///   arrive. We deliberately *keep* the terminal row so the
    ///   explorer's `/v1/async/:id` page keeps working forever; the
    ///   storage cost is tiny.
    ///
    /// `slot` is used to fill `last_slot`. `now_ms` is the ingestion
    /// wall-clock and becomes `last_updated_ts_ms` + `first_seen_ts_ms`
    /// for rows we create here.
    fn apply_async_pool_changes(
        &mut self,
        slot: Slot,
        entries: &[m::AsyncPoolChangeEntry],
        now_ms: i64,
    ) -> Result<()> {
        for entry in entries {
            let id = entry.async_message_id.clone();
            if id.is_empty() {
                continue;
            }
            let Some(val) = entry.value.as_ref() else {
                continue;
            };
            let kind = match m::AsyncPoolChangeType::try_from(val.r#type) {
                Ok(k) => k,
                Err(_) => continue,
            };
            match kind {
                m::AsyncPoolChangeType::Set => {
                    let Some(m::async_pool_change_value::Message::CreatedMessage(msg)) =
                        val.message.as_ref()
                    else {
                        continue;
                    };
                    let row = async_msg_from_proto(&id, msg, slot, now_ms);
                    self.db.write_async_msg(&row)?;
                }
                m::AsyncPoolChangeType::Update => {
                    let Some(m::async_pool_change_value::Message::UpdatedMessage(upd)) =
                        val.message.as_ref()
                    else {
                        continue;
                    };
                    let mut existing = self
                        .db
                        .read_async_msg(&id)?
                        .unwrap_or_else(|| StoredAsyncMsg {
                            id: id.clone(),
                            sender: None,
                            destination: None,
                            handler: None,
                            coins_nmas: 0,
                            max_gas: 0,
                            fee_nmas: 0,
                            emission_slot: None,
                            validity_start: None,
                            validity_end: None,
                            state: AsyncMsgState::Pending,
                            last_slot: Some(slot),
                            data_hex: None,
                            trigger: None,
                            can_be_executed: false,
                            first_seen_ts_ms: now_ms,
                            last_updated_ts_ms: now_ms,
                        });
                    apply_async_update(&mut existing, upd);
                    existing.last_slot = Some(slot);
                    existing.last_updated_ts_ms = now_ms;
                    self.db.write_async_msg(&existing)?;
                }
                m::AsyncPoolChangeType::Delete => {
                    // Keep a terminal row so historical lookups still work.
                    // If we've never seen this id (unusual — it'd mean the
                    // node skipped the SET), we still persist a stub so
                    // `/v1/async/:id` doesn't return 404 and transfers
                    // reconciliation can find it.
                    let mut existing = self
                        .db
                        .read_async_msg(&id)?
                        .unwrap_or_else(|| StoredAsyncMsg {
                            id: id.clone(),
                            sender: None,
                            destination: None,
                            handler: None,
                            coins_nmas: 0,
                            max_gas: 0,
                            fee_nmas: 0,
                            emission_slot: None,
                            validity_start: None,
                            validity_end: None,
                            state: AsyncMsgState::Pending,
                            last_slot: Some(slot),
                            data_hex: None,
                            trigger: None,
                            can_be_executed: false,
                            first_seen_ts_ms: now_ms,
                            last_updated_ts_ms: now_ms,
                        });
                    // Don't downgrade a row that's already been
                    // classified by the transfer stream for this slot.
                    if !matches!(
                        existing.state,
                        AsyncMsgState::Executed | AsyncMsgState::Cancelled
                    ) {
                        existing.state = AsyncMsgState::Consumed;
                    }
                    existing.last_slot = Some(slot);
                    existing.last_updated_ts_ms = now_ms;
                    self.db.write_async_msg(&existing)?;
                }
                m::AsyncPoolChangeType::Unspecified => {}
            }
        }
        Ok(())
    }


    /// Write per-op candidate_exec / final_exec status from
    /// `ExecutedOpsChangeEntry` rows. Returns the list of op ids we saw.
    fn apply_executed_ops_changes(
        &mut self,
        entries: &[m::ExecutedOpsChangeEntry],
        is_final: bool,
    ) -> Result<Vec<OperationId>> {
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            let Ok(op_id) = OperationId::parse(entry.operation_id.clone()) else {
                continue;
            };
            out.push(op_id.clone());
            let status = entry.value.as_ref().map(|v| v.status).unwrap_or(0);
            let new_exec_status = match m::OperationExecutionStatus::try_from(status) {
                Ok(m::OperationExecutionStatus::Success) => ExecStatus::Ok,
                Ok(m::OperationExecutionStatus::Failed) => ExecStatus::Failed,
                _ => continue, // UNSPECIFIED → skip
            };
            let Some(mut op) = self.db.read_op(&op_id)? else {
                // We haven't seen the block body yet — the exec status will be
                // re-applied the next time FINAL exec replays for this slot.
                continue;
            };
            if is_final {
                // First final wins. Don't overwrite a previously-set final.
                if op.final_exec_status.is_none() {
                    op.final_exec_status = Some(new_exec_status);
                    op.candidate_exec_status = Some(new_exec_status);
                    self.db.write_op(&op)?;
                }
            } else {
                // Candidate: mirror latest candidate value.
                if op.candidate_exec_status != Some(new_exec_status) {
                    op.candidate_exec_status = Some(new_exec_status);
                    self.db.write_op(&op)?;
                }
            }
        }
        Ok(out)
    }

    /// Transition every block we know for a just-finalized slot.
    /// Idempotent: repeated final events will overwrite blocks with the same
    /// value, which is cheap and safe.
    fn apply_finality_to_blocks(
        &mut self,
        final_block_id: Option<&BlockId>,
        candidates: &[BlockId],
    ) -> Result<()> {
        for bid in candidates {
            let Some(mut b) = self.db.read_block(bid)? else { continue };
            let new_status = if Some(bid) == final_block_id {
                BlockStatus::Final
            } else {
                BlockStatus::Discarded
            };
            if b.status != new_status {
                b.status = new_status;
                self.db.write_block(&b)?;
            }
        }
        // The final block may have been stored under its id before we ever
        // added it to the slot's candidate list (late E_BLOCK arrival).
        if let Some(bid) = final_block_id {
            if !candidates.iter().any(|c| c == bid) {
                if let Some(mut b) = self.db.read_block(bid)? {
                    if b.status != BlockStatus::Final {
                        b.status = BlockStatus::Final;
                        self.db.write_block(&b)?;
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Shared reconciliation primitives.
//
// Both the live-node ingest path (`Ingest::handle_transfers`) and the peer-
// patch apply path (`peer::patch::apply_transfers_part`) need to turn a set
// of `StoredTransfer` rows into async-pool / deferred-call upserts. The
// functions below take `&Db` directly so the peer code can call them
// without needing an `Ingest` handle.
// ---------------------------------------------------------------------------

/// Reconcile async-pool rows against a slot's transfer stream.
///
/// For every transfer that carries an `async_msg_id`:
///
/// * `AsyncMsgCoins`  → promote to `Executed`.
/// * `AsyncMsgCancel` → promote to `Cancelled`.
/// * anything else is ignored (no-op).
///
/// Existing `Executed` / `Cancelled` rows are never downgraded. Rows that
/// did not exist yet (transfers pointing at ids we never saw in the
/// `state_changes` stream) are minted on the spot so the explorer can
/// resolve every `async_msg_id` it renders.
pub(crate) fn reconcile_async_from_transfers(
    db: &crate::db::Db,
    slot: Slot,
    rows: &[StoredTransfer],
    now_ms: i64,
) -> Result<()> {
    for r in rows {
        let Some(id) = r.async_msg_id.as_ref() else {
            continue;
        };
        let new_state = match r.origin {
            CoinOrigin::AsyncMsgCoins => AsyncMsgState::Executed,
            CoinOrigin::AsyncMsgCancel => AsyncMsgState::Cancelled,
            _ => continue,
        };
        let mut existing = db.read_async_msg(id)?.unwrap_or_else(|| StoredAsyncMsg {
            id: id.clone(),
            sender: None,
            destination: None,
            handler: None,
            coins_nmas: 0,
            max_gas: 0,
            fee_nmas: 0,
            emission_slot: None,
            validity_start: None,
            validity_end: None,
            state: new_state,
            last_slot: Some(slot),
            data_hex: None,
            trigger: None,
            can_be_executed: false,
            first_seen_ts_ms: now_ms,
            last_updated_ts_ms: now_ms,
        });
        if matches!(existing.state, AsyncMsgState::Pending | AsyncMsgState::Consumed) {
            existing.state = new_state;
        }
        if existing.sender.is_none() {
            existing.sender = r.from.as_deref().and_then(|s| Address::parse(s.to_string()).ok());
        }
        if existing.destination.is_none() {
            existing.destination =
                r.to.as_deref().and_then(|s| Address::parse(s.to_string()).ok());
        }
        if let crate::model::TransferValue::Coins { nmas } = r.value {
            if existing.coins_nmas == 0 {
                existing.coins_nmas = nmas;
            }
        }
        existing.last_slot = Some(slot);
        existing.last_updated_ts_ms = now_ms;
        db.write_async_msg(&existing)?;
    }
    Ok(())
}

/// Derive deferred-call lifecycle from the transfer stream.
///
/// The node does not emit `deferred_calls_changes` on the `state_changes`
/// message we subscribe to — every deferred-call state transition *does*
/// produce one or more transfers tagged with `deferred_call_id` and a
/// `DeferredCall*` `CoinOrigin`. We upsert a `StoredDeferredCall` row per
/// id per slot:
///
/// * `DeferredCallRegister`      — first sighting. Creates a `Registered`
///   row, storing the registration slot and the caller as sender.
/// * `DeferredCallCoins`         — handler ran and paid out coins to the
///   target. Upgrades the row to `Executed` and fills `target_address` +
///   `target_slot`.
/// * `DeferredCallFail`          — handler trapped. Upgrades to `Failed`.
/// * `DeferredCallCancel`        — user cancel or registration expired.
///   Upgrades to `Cancelled`.
/// * `DeferredCallStorageRefund` — accompanies terminal transitions; we
///   just touch `last_slot` / timestamps.
///
/// Terminal states are sticky: once a row is Executed/Failed/Cancelled, a
/// trailing transfer (e.g. a refund after a cancel) never regresses it.
pub(crate) fn reconcile_deferred_from_transfers(
    db: &crate::db::Db,
    slot: Slot,
    rows: &[StoredTransfer],
    now_ms: i64,
) -> Result<()> {
    for r in rows {
        let Some(id) = r.deferred_call_id.as_ref() else {
            continue;
        };
        let mut existing = db.read_deferred_call(id)?.unwrap_or_else(|| {
            StoredDeferredCall {
                id: id.clone(),
                sender: None,
                target_address: None,
                target_function: None,
                parameter_hex: None,
                coins_nmas: 0,
                max_gas: 0,
                target_slot: None,
                registered_slot: None,
                state: DeferredCallState::Registered,
                last_slot: Some(slot),
                first_seen_ts_ms: now_ms,
                last_updated_ts_ms: now_ms,
            }
        });

        let parsed_from = r
            .from
            .as_deref()
            .and_then(|s| Address::parse(s.to_string()).ok());
        let parsed_to = r
            .to
            .as_deref()
            .and_then(|s| Address::parse(s.to_string()).ok());

        let new_state = match r.origin {
            CoinOrigin::DeferredCallRegister => {
                if existing.registered_slot.is_none() {
                    existing.registered_slot = Some(slot);
                }
                if existing.sender.is_none() {
                    existing.sender = parsed_from.clone();
                }
                DeferredCallState::Registered
            }
            CoinOrigin::DeferredCallCoins => {
                if existing.target_slot.is_none() {
                    existing.target_slot = Some(slot);
                }
                if existing.target_address.is_none() {
                    existing.target_address = parsed_to.clone();
                }
                DeferredCallState::Executed
            }
            CoinOrigin::DeferredCallFail => DeferredCallState::Failed,
            CoinOrigin::DeferredCallCancel => DeferredCallState::Cancelled,
            CoinOrigin::DeferredCallStorageRefund => existing.state,
            _ => continue,
        };

        if let crate::model::TransferValue::Coins { nmas } = r.value {
            if nmas > existing.coins_nmas {
                existing.coins_nmas = nmas;
            }
        }

        let terminal = matches!(
            existing.state,
            DeferredCallState::Executed
                | DeferredCallState::Failed
                | DeferredCallState::Cancelled
        );
        // Terminal states are sticky: once Executed/Failed/Cancelled, later
        // transfers (e.g. a StorageRefund trailing a Cancel) must not
        // regress the lifecycle.
        if !terminal {
            existing.state = new_state;
        }
        existing.last_slot = Some(slot);
        existing.last_updated_ts_ms = now_ms;
        db.write_deferred_call(&existing)?;
    }
    Ok(())
}

fn classify_op(
    op_type: Option<&m::OperationType>,
) -> (OperationKind, Option<Address>, OperationDetails) {
    let Some(op_type) = op_type else {
        return (OperationKind::Unknown, None, OperationDetails::default());
    };
    let Some(t) = op_type.r#type.as_ref() else {
        return (OperationKind::Unknown, None, OperationDetails::default());
    };
    match t {
        m::operation_type::Type::Transaction(tx) => {
            let target = Address::parse(tx.recipient_address.clone()).ok();
            let d = OperationDetails {
                amount_nmas: tx.amount.as_ref().map(|a| a.mantissa),
                recipient_address: Some(tx.recipient_address.clone()),
                ..Default::default()
            };
            (OperationKind::Transaction, target, d)
        }
        m::operation_type::Type::RollBuy(rb) => {
            let d = OperationDetails {
                roll_count: Some(rb.roll_count),
                ..Default::default()
            };
            (OperationKind::RollBuy, None, d)
        }
        m::operation_type::Type::RollSell(rs) => {
            let d = OperationDetails {
                roll_count: Some(rs.roll_count),
                ..Default::default()
            };
            (OperationKind::RollSell, None, d)
        }
        m::operation_type::Type::ExecuteSc(e) => {
            // Full archival: keep the bytecode and every datastore entry.
            let d = OperationDetails {
                bytecode_hex: Some(hex_encode(&e.data)),
                bytecode_size: Some(e.data.len() as u64),
                max_coins_nmas: Some(e.max_coins),
                max_gas: Some(e.max_gas),
                datastore_keys: Some(e.datastore.len() as u32),
                datastore: e
                    .datastore
                    .iter()
                    .map(|entry| DatastoreEntry {
                        key_hex: hex_encode(&entry.key),
                        value_hex: hex_encode(&entry.value),
                    })
                    .collect(),
                ..Default::default()
            };
            (OperationKind::ExecuteSc, None, d)
        }
        m::operation_type::Type::CallSc(c) => {
            let target = Address::parse(c.target_address.clone()).ok();
            // Full archival: no truncation — we need byte-exact reconstruction.
            let d = OperationDetails {
                target_address: Some(c.target_address.clone()),
                target_function: Some(c.target_function.clone()),
                parameter_hex: Some(hex_encode(&c.parameter)),
                parameter_len: Some(c.parameter.len() as u64),
                max_gas: Some(c.max_gas),
                coins_nmas: c.coins.as_ref().map(|a| a.mantissa),
                ..Default::default()
            };
            (OperationKind::CallSc, target, d)
        }
    }
}

fn decode_endorsement(
    se: &m::SignedEndorsement,
    including_block: &BlockId,
    including_slot: Slot,
    now_ms: i64,
) -> Option<StoredEndorsement> {
    let content = se.content.as_ref()?;
    let slot_pb = content.slot.as_ref()?;
    let slot = Slot::new(slot_pb.period, slot_pb.thread as u8);
    let creator = Address::parse(se.content_creator_address.clone()).ok()?;
    Some(StoredEndorsement {
        id: se.secure_hash.clone(),
        slot,
        index: content.index,
        endorsed_block_id: content.endorsed_block.clone(),
        content_creator_pub_key: se.content_creator_pub_key.clone(),
        content_creator_address: creator,
        signature: se.signature.clone(),
        serialized_size: se.serialized_size,
        included_block_id: including_block.to_string(),
        included_slot: including_slot,
        first_seen_ts_ms: now_ms,
    })
}

fn decode_denunciation(d: &m::Denunciation) -> StoredDenunciation {
    match d.entry.as_ref() {
        Some(m::denunciation::Entry::BlockHeader(b)) => {
            let slot = b
                .slot
                .as_ref()
                .map(|s| Slot::new(s.period, s.thread as u8))
                .unwrap_or(Slot::new(0, 0));
            StoredDenunciation::BlockHeader {
                public_key: b.public_key.clone(),
                slot,
                hash_1: b.hash_1.clone(),
                hash_2: b.hash_2.clone(),
                signature_1: b.signature_1.clone(),
                signature_2: b.signature_2.clone(),
            }
        }
        Some(m::denunciation::Entry::Endorsement(e)) => {
            let slot = e
                .slot
                .as_ref()
                .map(|s| Slot::new(s.period, s.thread as u8))
                .unwrap_or(Slot::new(0, 0));
            StoredDenunciation::Endorsement {
                public_key: e.public_key.clone(),
                slot,
                index: e.index,
                hash_1: e.hash_1.clone(),
                hash_2: e.hash_2.clone(),
                signature_1: e.signature_1.clone(),
                signature_2: e.signature_2.clone(),
            }
        }
        Some(m::denunciation::Entry::Address(a)) => {
            let slot = a
                .slot
                .as_ref()
                .map(|s| Slot::new(s.period, s.thread as u8))
                .unwrap_or(Slot::new(0, 0));
            let slashed = a.slashed.as_ref().map(|v| v.mantissa).unwrap_or(0);
            StoredDenunciation::Address {
                address_denounced: a.address_denounced.clone(),
                slot,
                slashed_nmas: slashed,
            }
        }
        None => StoredDenunciation::Unknown,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(*b >> 4) as usize] as char);
        out.push(HEX[(*b & 0x0f) as usize] as char);
    }
    out
}

fn extract_event_context(
    ev: &m::ScExecutionEvent,
) -> (Vec<Address>, Vec<Address>, Option<OperationId>) {
    let ctx = match ev.context.as_ref() {
        Some(c) => c,
        None => return (vec![], vec![], None),
    };
    let call_stack: Vec<Address> = ctx
        .call_stack
        .iter()
        .filter_map(|s| Address::parse(s.clone()).ok())
        .collect();
    let emitters = call_stack.last().cloned().into_iter().collect();
    let callers = call_stack.first().cloned().into_iter().collect();
    let op_id = ctx
        .origin_operation_id
        .as_ref()
        .and_then(|x| OperationId::parse(x.clone()).ok());
    (emitters, callers, op_id)
}

/// SC event payloads arrive as raw bytes. Most are UTF-8 strings emitted by
/// the SC; fall back to lossy decoding otherwise. We keep this conversion in
/// one place so the REST layer can ship the events as plain JSON strings.
fn stringify_bytes(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) => s.to_string(),
        Err(_) => String::from_utf8_lossy(b).into_owned(),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// MAS `NativeAmount` is stored at scale=9 (nanoMAS). Every usage we have
/// ever shipped assumes that scale, so we expose `mantissa` directly.
fn native_amount_to_nmas(a: &m::NativeAmount) -> u64 {
    a.mantissa
}

/// Map proto `CoinOrigin` (i32) to our strongly-typed enum. Unknown numeric
/// values are preserved as `CoinOrigin::Other { code }` so new origins added
/// by the node don't break ingestion.
fn coin_origin_from_i32(code: i32) -> CoinOrigin {
    match m::CoinOrigin::try_from(code) {
        Ok(m::CoinOrigin::Unspecified) => CoinOrigin::Unspecified,
        Ok(m::CoinOrigin::BlockReward) => CoinOrigin::BlockReward,
        Ok(m::CoinOrigin::DeferredCallFail) => CoinOrigin::DeferredCallFail,
        Ok(m::CoinOrigin::DeferredCallCancel) => CoinOrigin::DeferredCallCancel,
        Ok(m::CoinOrigin::DeferredCallCoins) => CoinOrigin::DeferredCallCoins,
        Ok(m::CoinOrigin::DeferredCallRegister) => CoinOrigin::DeferredCallRegister,
        Ok(m::CoinOrigin::DeferredCallStorageRefund) => CoinOrigin::DeferredCallStorageRefund,
        Ok(m::CoinOrigin::EndorsementReward) => CoinOrigin::EndorsementReward,
        Ok(m::CoinOrigin::EndorsedReward) => CoinOrigin::EndorsedReward,
        Ok(m::CoinOrigin::Slash) => CoinOrigin::Slash,
        Ok(m::CoinOrigin::OpRollBuy) => CoinOrigin::OpRollBuy,
        Ok(m::CoinOrigin::OpRollSell) => CoinOrigin::OpRollSell,
        Ok(m::CoinOrigin::OpCallscCoins) => CoinOrigin::OpCallscCoins,
        Ok(m::CoinOrigin::ReadOnlyFnCallFees) => CoinOrigin::ReadOnlyFnCallFees,
        Ok(m::CoinOrigin::ReadOnlyFnCallCoins) => CoinOrigin::ReadOnlyFnCallCoins,
        Ok(m::CoinOrigin::ReadOnlyBytecodeExecFees) => CoinOrigin::ReadOnlyBytecodeExecFees,
        Ok(m::CoinOrigin::SetBytecodeStorage) => CoinOrigin::SetBytecodeStorage,
        Ok(m::CoinOrigin::AbiCallCoins) => CoinOrigin::AbiCallCoins,
        Ok(m::CoinOrigin::AbiTransferCoins) => CoinOrigin::AbiTransferCoins,
        Ok(m::CoinOrigin::AbiTransferForCoins) => CoinOrigin::AbiTransferForCoins,
        Ok(m::CoinOrigin::AbiSendMsgCoins) => CoinOrigin::AbiSendMsgCoins,
        Ok(m::CoinOrigin::AbiSendMsgFees) => CoinOrigin::AbiSendMsgFees,
        Ok(m::CoinOrigin::OpRollSellDeferredMas) => CoinOrigin::OpRollSellDeferredMas,
        Ok(m::CoinOrigin::OpExecutescFees) => CoinOrigin::OpExecutescFees,
        Ok(m::CoinOrigin::OpTransactionCoins) => CoinOrigin::OpTransactionCoins,
        Ok(m::CoinOrigin::OpTransactionFees) => CoinOrigin::OpTransactionFees,
        Ok(m::CoinOrigin::AsyncMsgCoins) => CoinOrigin::AsyncMsgCoins,
        Ok(m::CoinOrigin::AsyncMsgCancel) => CoinOrigin::AsyncMsgCancel,
        Ok(m::CoinOrigin::CreateScStorage) => CoinOrigin::CreateScStorage,
        Ok(m::CoinOrigin::DatastoreStorage) => CoinOrigin::DatastoreStorage,
        Ok(m::CoinOrigin::DeferredCredit) => CoinOrigin::DeferredCredit,
        Err(_) => CoinOrigin::Other { code: code as u32 },
    }
}

/// True if the transfer carries no economic value. The node emits zero-value
/// transfers for SC calls that happen to attach 0 coins / 0 rolls; indexing
/// them would clutter every per-address and per-operation view with pure
/// noise. Per user request (2026-04), we drop them at ingestion time.
fn is_zero_value(v: &TransferValue) -> bool {
    match v {
        TransferValue::Coins { nmas } => *nmas == 0,
        TransferValue::DeferredCredits { nmas } => *nmas == 0,
        TransferValue::Rolls { count } => *count == 0,
        TransferValue::Token { raw, .. } => raw.bytes().all(|b| b == b'0') || raw.is_empty(),
        TransferValue::Unknown => true,
    }
}

/// Rewards and validator economics that are emitted *around* operation
/// execution but are not part of any op's call stack.
fn is_reward_origin(o: &CoinOrigin) -> bool {
    matches!(
        o,
        CoinOrigin::BlockReward
            | CoinOrigin::EndorsementReward
            | CoinOrigin::EndorsedReward
            | CoinOrigin::Slash
            | CoinOrigin::DeferredCredit
    )
}

/// Origins that are only ever produced *inside* a user operation's execution
/// frame. When we see one without an `operation_id`, the active op at that
/// point in the slot stream is its true parent.
fn is_op_child_origin(o: &CoinOrigin) -> bool {
    matches!(
        o,
        CoinOrigin::AbiCallCoins
            | CoinOrigin::AbiTransferCoins
            | CoinOrigin::AbiTransferForCoins
            | CoinOrigin::AbiSendMsgCoins
            | CoinOrigin::AbiSendMsgFees
            | CoinOrigin::CreateScStorage
            | CoinOrigin::DatastoreStorage
            | CoinOrigin::SetBytecodeStorage
    )
}

/// Compact stringification of a `DenunciationIndex` — we fall back to the
/// enum variant name if the denunciation payload is missing.
fn format_denunciation_index(idx: &m::DenunciationIndex) -> String {
    match idx.entry.as_ref() {
        Some(m::denunciation_index::Entry::BlockHeader(bh)) => {
            let slot = bh
                .slot
                .as_ref()
                .map(|s| format!("({},{})", s.period, s.thread))
                .unwrap_or_else(|| "()".into());
            format!("block_header@{slot}")
        }
        Some(m::denunciation_index::Entry::Endorsement(e)) => {
            let slot = e
                .slot
                .as_ref()
                .map(|s| format!("({},{})", s.period, s.thread))
                .unwrap_or_else(|| "()".into());
            format!("endorsement@{slot}#{}", e.index)
        }
        None => "unknown".to_string(),
    }
}

/// Deterministic content-addressed hash for a `StoredDenunciation`. We use
/// the tagged JSON serialization to keep the hash stable across schema
/// additions as long as the relevant inner fields don't change.
pub fn denunciation_hash(d: &StoredDenunciation) -> String {
    let canon = serde_json::to_vec(d).unwrap_or_default();
    let digest = Sha256::digest(&canon);
    hex_encode(&digest)
}

pub(crate) fn denunciation_slot(d: &StoredDenunciation) -> Slot {
    match d {
        StoredDenunciation::BlockHeader { slot, .. } => *slot,
        StoredDenunciation::Endorsement { slot, .. } => *slot,
        StoredDenunciation::Address { slot, .. } => *slot,
        StoredDenunciation::Unknown => Slot::new(0, 0),
    }
}

pub(crate) fn denunciation_kind_label(d: &StoredDenunciation) -> &'static str {
    match d {
        StoredDenunciation::BlockHeader { .. } => "block_header",
        StoredDenunciation::Endorsement { .. } => "endorsement",
        StoredDenunciation::Address { .. } => "address",
        StoredDenunciation::Unknown => "unknown",
    }
}

/// Best-effort address extraction. The node's proto only carries a public
/// key for block-header / endorsement denunciations; deriving the address
/// from the PK requires the Massa PK → address ABI which we don't bundle
/// yet. When that's not available we fall back to `None`. Explicit address
/// denunciations are always resolvable.
pub(crate) fn denunciation_target_address(d: &StoredDenunciation) -> Option<Address> {
    match d {
        StoredDenunciation::Address { address_denounced, .. } => {
            Address::parse(address_denounced.clone()).ok()
        }
        _ => None,
    }
}

// Convenience builders for the four stream requests. Each helper is a
// one-liner so the gRPC consumer can focus on reconnection / backoff logic
// without having to know the per-stream filter shape.
pub fn filled_blocks_request() -> api::NewFilledBlocksServerRequest {
    api::NewFilledBlocksServerRequest { filters: vec![] }
}
pub fn slot_exec_request() -> api::NewSlotExecutionOutputsServerRequest {
    api::NewSlotExecutionOutputsServerRequest { filters: vec![] }
}
pub fn transfers_info_request() -> api::NewTransfersInfoServerRequest {
    // Optional per-address filter left empty — we want every transfer.
    api::NewTransfersInfoServerRequest { address: None }
}
// ---------------------------------------------------------------------------
// Async-pool helpers
// ---------------------------------------------------------------------------

/// Lift a proto `AsyncMessage` (from an `AsyncPoolChangeValue::SET`) into
/// a fresh [`StoredAsyncMsg`]. Only called when the pool introduced a
/// new row for this id.
fn async_msg_from_proto(
    id: &str,
    msg: &m::AsyncMessage,
    slot: Slot,
    now_ms: i64,
) -> StoredAsyncMsg {
    let sender = Address::parse(msg.sender.clone()).ok();
    let destination = Address::parse(msg.destination.clone()).ok();
    let emission_slot = msg.emission_slot.as_ref().map(slot_from_pb);
    let validity_start = msg.validity_start.as_ref().map(slot_from_pb);
    let validity_end = msg.validity_end.as_ref().map(slot_from_pb);
    let coins_nmas = msg.coins.as_ref().map(|a| a.mantissa).unwrap_or(0);
    let fee_nmas = msg.fee.as_ref().map(|a| a.mantissa).unwrap_or(0);
    let trigger = msg.trigger.as_ref().map(|t| AsyncTrigger {
        address: t.address.clone(),
        datastore_key_hex: t.datastore_key.as_ref().map(|b| hex_encode(b)),
    });
    let data_hex = if msg.data.is_empty() {
        None
    } else {
        Some(hex_encode(&msg.data))
    };

    StoredAsyncMsg {
        id: id.to_string(),
        sender,
        destination,
        handler: if msg.handler.is_empty() {
            None
        } else {
            Some(msg.handler.clone())
        },
        coins_nmas,
        max_gas: msg.max_gas,
        fee_nmas,
        emission_slot,
        validity_start,
        validity_end,
        state: AsyncMsgState::Pending,
        last_slot: Some(slot),
        data_hex,
        trigger,
        can_be_executed: msg.can_be_executed,
        first_seen_ts_ms: now_ms,
        last_updated_ts_ms: now_ms,
    }
}

/// Apply the `SetOrKeep` fields of an `AsyncMessageUpdate` on top of an
/// existing row, leaving every `Keep` / unset field untouched. Keeping
/// the helper isolated makes it easy to unit-test the merge rules
/// independently of the DB layer.
fn apply_async_update(existing: &mut StoredAsyncMsg, upd: &m::AsyncMessageUpdate) {
    use m::{
        set_or_keep_async_message_trigger::Change as CTrig,
        set_or_keep_bool::Change as CBool,
        set_or_keep_bytes::Change as CBytes,
        set_or_keep_slot::Change as CSlot,
        set_or_keep_string::Change as CString,
        set_or_keep_uint64::Change as CU64,
    };

    if let Some(CSlot::Set(v)) = upd.emission_slot.as_ref().and_then(|c| c.change.as_ref()) {
        existing.emission_slot = Some(slot_from_pb(v));
    }
    // `emission_index` has no home in our schema; ignored on purpose.
    if let Some(CString::Set(v)) = upd.sender.as_ref().and_then(|c| c.change.as_ref()) {
        existing.sender = Address::parse(v.clone()).ok();
    }
    if let Some(CString::Set(v)) = upd.destination.as_ref().and_then(|c| c.change.as_ref()) {
        existing.destination = Address::parse(v.clone()).ok();
    }
    if let Some(CString::Set(v)) = upd.handler.as_ref().and_then(|c| c.change.as_ref()) {
        existing.handler = if v.is_empty() { None } else { Some(v.clone()) };
    }
    if let Some(CU64::Set(v)) = upd.max_gas.as_ref().and_then(|c| c.change.as_ref()) {
        existing.max_gas = *v;
    }
    if let Some(CU64::Set(v)) = upd.fee.as_ref().and_then(|c| c.change.as_ref()) {
        existing.fee_nmas = *v;
    }
    if let Some(CU64::Set(v)) = upd.coins.as_ref().and_then(|c| c.change.as_ref()) {
        existing.coins_nmas = *v;
    }
    if let Some(CSlot::Set(v)) = upd.validity_start.as_ref().and_then(|c| c.change.as_ref()) {
        existing.validity_start = Some(slot_from_pb(v));
    }
    if let Some(CSlot::Set(v)) = upd.validity_end.as_ref().and_then(|c| c.change.as_ref()) {
        existing.validity_end = Some(slot_from_pb(v));
    }
    if let Some(CBytes::Set(v)) = upd.data.as_ref().and_then(|c| c.change.as_ref()) {
        existing.data_hex = if v.is_empty() {
            None
        } else {
            Some(hex_encode(v))
        };
    }
    if let Some(CTrig::Set(t)) = upd.trigger.as_ref().and_then(|c| c.change.as_ref()) {
        existing.trigger = Some(AsyncTrigger {
            address: t.address.clone(),
            datastore_key_hex: t.datastore_key.as_ref().map(|b| hex_encode(b)),
        });
    }
    if let Some(CBool::Set(v)) = upd.can_be_executed.as_ref().and_then(|c| c.change.as_ref()) {
        existing.can_be_executed = *v;
    }
}

/// Convert the proto `Slot` wrapper into our domain type.
fn slot_from_pb(s: &m::Slot) -> Slot {
    Slot::new(s.period, s.thread as u8)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::AsyncByAddr;

    fn mk_env() -> Ingest {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let sse = SseHub::new(32);
        let (_tx, rx) = mpsc::channel::<Event>(32);
        std::mem::forget(dir);
        Ingest::new(db, sse, rx)
    }

    fn exec_output(
        period: u64,
        thread: u8,
        is_final: bool,
        trail: Option<&str>,
    ) -> m::SlotExecutionOutput {
        let status = if is_final {
            m::ExecutionOutputStatus::Final as i32
        } else {
            m::ExecutionOutputStatus::Candidate as i32
        };
        let state_changes = trail.map(|t| m::StateChanges {
            ledger_changes: vec![],
            async_pool_changes: vec![],
            executed_ops_changes: vec![],
            executed_denunciations_changes: vec![],
            execution_trail_hash_change: Some(m::SetOrKeepString {
                change: Some(m::set_or_keep_string::Change::Set(t.to_string())),
            }),
        });
        m::SlotExecutionOutput {
            status,
            execution_output: Some(m::ExecutionOutput {
                slot: Some(m::Slot { period, thread: thread as u32 }),
                block_id: None,
                events: vec![],
                state_changes,
            }),
        }
    }

    #[tokio::test]
    async fn final_wins_and_trail_conflict_is_ignored() {
        let mut ingest = mk_env();
        ingest
            .handle_exec(exec_output(1, 0, true, Some("trail-A")))
            .unwrap();
        ingest
            .handle_exec(exec_output(1, 0, true, Some("trail-B")))
            .unwrap();
        let s = ingest.db.read_slot(1, 0).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final);
        assert_eq!(s.execution_trail_hash.as_deref(), Some("trail-A"));
    }

    #[tokio::test]
    async fn candidate_then_final_upgrade() {
        let mut ingest = mk_env();
        ingest
            .handle_exec(exec_output(2, 1, false, Some("trail-C")))
            .unwrap();
        let s = ingest.db.read_slot(2, 1).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Candidate);
        ingest
            .handle_exec(exec_output(2, 1, true, Some("trail-F")))
            .unwrap();
        let s = ingest.db.read_slot(2, 1).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final);
        assert_eq!(s.execution_trail_hash.as_deref(), Some("trail-F"));
        assert!(s.completeness.exec_output_final);
    }

    #[tokio::test]
    async fn final_is_idempotent() {
        let mut ingest = mk_env();
        for _ in 0..3 {
            ingest.handle_exec(exec_output(3, 0, true, Some("t-x"))).unwrap();
        }
        let s = ingest.db.read_slot(3, 0).unwrap().unwrap();
        assert_eq!(s.status, SlotStatus::Final);
        assert_eq!(s.execution_trail_hash.as_deref(), Some("t-x"));
    }

    #[tokio::test]
    async fn finality_propagates_to_blocks() {
        let mut ingest = mk_env();
        let slot = Slot::new(10, 1);
        let bid_winner = crate::ids::mk_test_block_id(10);
        let bid_loser = crate::ids::mk_test_block_id(11);
        let creator = crate::ids::mk_test_user_addr(12);

        for bid in [&bid_winner, &bid_loser] {
            let b = StoredBlock {
                id: bid.clone(),
                slot,
                creator: creator.clone(),
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
                status: BlockStatus::SeenCandidate,
                first_seen_ts_ms: 0,
            };
            ingest.db.write_block(&b).unwrap();
        }

        let mut s = SlotState::fresh(slot, 0);
        s.candidate_block_ids = vec![bid_winner.clone(), bid_loser.clone()];
        s.completeness.block_body_stored = true;
        s.status = SlotStatus::Candidate;
        ingest.db.write_slot(&s).unwrap();

        let mut e = exec_output(10, 1, true, None);
        e.execution_output.as_mut().unwrap().block_id = Some(bid_winner.to_string());
        ingest.handle_exec(e).unwrap();

        let w = ingest.db.read_block(&bid_winner).unwrap().unwrap();
        let l = ingest.db.read_block(&bid_loser).unwrap().unwrap();
        assert_eq!(w.status, BlockStatus::Final);
        assert_eq!(l.status, BlockStatus::Discarded);
    }

    #[tokio::test]
    async fn late_block_gets_correct_status_after_finality() {
        let mut ingest = mk_env();
        let _slot = Slot::new(11, 2);
        let winner = crate::ids::mk_test_block_id(20);

        // Finalize the slot first (miss or by a yet-unknown block body).
        let mut e = exec_output(11, 2, true, None);
        e.execution_output.as_mut().unwrap().block_id = Some(winner.to_string());
        ingest.handle_exec(e).unwrap();

        // The block body arrives AFTER finalization. It should be stored as Final.
        // (We construct a minimal FilledBlock from scratch.)
        let fb = m::FilledBlock {
            header: Some(m::SignedBlockHeader {
                content: Some(m::BlockHeader {
                    current_version: 0,
                    announced_version: None,
                    slot: Some(m::Slot { period: 11, thread: 2 }),
                    parents: vec![],
                    operations_hash: String::new(),
                    endorsements: vec![],
                    denunciations: vec![],
                }),
                signature: String::new(),
                content_creator_pub_key: String::new(),
            content_creator_address: crate::ids::mk_test_user_addr(21).to_string(),
            secure_hash: winner.to_string(),
                serialized_size: 0,
            }),
            operations: vec![],
        };
        ingest.handle_block(fb).unwrap();

        let stored = ingest.db.read_block(&winner).unwrap().unwrap();
        assert_eq!(stored.status, BlockStatus::Final);
    }

    #[tokio::test]
    async fn executed_ops_changes_update_op_status() {
        let mut ingest = mk_env();
        let op_id = crate::ids::mk_test_op_id(30);
        // Seed a stored op (as if E_BLOCK had already ingested it).
        let op = StoredOperation {
            id: op_id.clone(),
            creator: crate::ids::mk_test_user_addr(31),
            target: None,
            kind: OperationKind::Transaction,
            expire_period: 42,
            fee_nmas: 1,
            thread: 3,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(5, 3),
                block_id: crate::ids::mk_test_block_id(32),
            }],
            candidate_exec_status: None,
            final_exec_status: None,
            details: OperationDetails::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        ingest.db.write_op(&op).unwrap();

        // First push a CANDIDATE exec with SUCCESS.
        let mut sc = m::StateChanges {
            ledger_changes: vec![],
            async_pool_changes: vec![],
            executed_ops_changes: vec![m::ExecutedOpsChangeEntry {
                operation_id: op_id.to_string(),
                value: Some(m::ExecutedOpsChangeValue {
                    status: m::OperationExecutionStatus::Success as i32,
                    slot: Some(m::Slot { period: 5, thread: 3 }),
                }),
            }],
            executed_denunciations_changes: vec![],
            execution_trail_hash_change: None,
        };
        let mut e = exec_output(5, 3, false, None);
        e.execution_output.as_mut().unwrap().state_changes = Some(sc.clone());
        ingest.handle_exec(e).unwrap();

        let stored = ingest.db.read_op(&op_id).unwrap().unwrap();
        assert_eq!(stored.candidate_exec_status, Some(ExecStatus::Ok));
        assert_eq!(stored.final_exec_status, None);

        // Then the FINAL exec with FAILED — we expect both to be set to Failed.
        sc.executed_ops_changes[0].value.as_mut().unwrap().status =
            m::OperationExecutionStatus::Failed as i32;
        let mut e = exec_output(5, 3, true, None);
        e.execution_output.as_mut().unwrap().state_changes = Some(sc);
        ingest.handle_exec(e).unwrap();

        let stored = ingest.db.read_op(&op_id).unwrap().unwrap();
        assert_eq!(stored.candidate_exec_status, Some(ExecStatus::Failed));
        assert_eq!(stored.final_exec_status, Some(ExecStatus::Failed));
    }

    #[tokio::test]
    async fn ignores_unspecified_status() {
        let mut ingest = mk_env();
        let mut e = exec_output(4, 0, true, Some("x"));
        e.status = m::ExecutionOutputStatus::Unspecified as i32;
        ingest.handle_exec(e).unwrap();
        assert!(ingest.db.read_slot(4, 0).unwrap().is_none());
    }

    // -----------------------------------------------------------------
    // Async pool / deferred call ingestion
    // -----------------------------------------------------------------

    fn mk_async_message(
        sender: &str,
        destination: &str,
        handler: &str,
        coins: u64,
        max_gas: u64,
    ) -> m::AsyncMessage {
        m::AsyncMessage {
            emission_slot: Some(m::Slot { period: 100, thread: 0 }),
            emission_index: 0,
            sender: sender.to_string(),
            destination: destination.to_string(),
            handler: handler.to_string(),
            max_gas,
            fee: Some(m::NativeAmount { mantissa: 100, scale: 9 }),
            coins: Some(m::NativeAmount { mantissa: coins, scale: 9 }),
            validity_start: Some(m::Slot { period: 100, thread: 0 }),
            validity_end: Some(m::Slot { period: 200, thread: 0 }),
            data: b"payload".to_vec(),
            trigger: None,
            can_be_executed: true,
        }
    }

    fn exec_output_with_async(
        period: u64,
        thread: u8,
        is_final: bool,
        entries: Vec<m::AsyncPoolChangeEntry>,
    ) -> m::SlotExecutionOutput {
        let status = if is_final {
            m::ExecutionOutputStatus::Final as i32
        } else {
            m::ExecutionOutputStatus::Candidate as i32
        };
        m::SlotExecutionOutput {
            status,
            execution_output: Some(m::ExecutionOutput {
                slot: Some(m::Slot { period, thread: thread as u32 }),
                block_id: None,
                events: vec![],
                state_changes: Some(m::StateChanges {
                    ledger_changes: vec![],
                    async_pool_changes: entries,
                    executed_ops_changes: vec![],
                    executed_denunciations_changes: vec![],
                    execution_trail_hash_change: None,
                }),
            }),
        }
    }

    #[tokio::test]
    async fn async_pool_set_creates_pending_row() {
        let mut ingest = mk_env();
        let sender = crate::ids::mk_test_user_addr(40);
        let dest = crate::ids::mk_test_sc_addr(41);
        let msg = mk_async_message(
            &sender.to_string(),
            &dest.to_string(),
            "onMsg",
            123_456,
            1_000_000,
        );
        let entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-42".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Set as i32,
                message: Some(m::async_pool_change_value::Message::CreatedMessage(msg)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(50, 0, true, vec![entry]))
            .unwrap();

        let got = ingest.db.read_async_msg("AM-42").unwrap().unwrap();
        assert_eq!(got.state, AsyncMsgState::Pending);
        assert_eq!(got.coins_nmas, 123_456);
        assert_eq!(got.max_gas, 1_000_000);
        assert_eq!(got.handler.as_deref(), Some("onMsg"));
        assert_eq!(got.sender.as_ref().unwrap().to_string(), sender.to_string());
        assert_eq!(
            got.destination.as_ref().unwrap().to_string(),
            dest.to_string()
        );
        assert_eq!(got.last_slot, Some(Slot::new(50, 0)));
        assert!(got.first_seen_ts_ms > 0);
        assert_eq!(got.data_hex.as_deref(), Some("7061796c6f6164"));

        // Secondary indexes should resolve.
        let by_sender = ingest
            .db
            .iter_async_by_addr(&sender, AsyncByAddr::Sender, None, 16)
            .unwrap();
        assert_eq!(by_sender.items.len(), 1);
        let by_dest = ingest
            .db
            .iter_async_by_addr(&dest, AsyncByAddr::Destination, None, 16)
            .unwrap();
        assert_eq!(by_dest.items.len(), 1);
    }

    #[tokio::test]
    async fn async_pool_update_merges_into_existing() {
        let mut ingest = mk_env();
        let sender = crate::ids::mk_test_user_addr(50);
        let dest = crate::ids::mk_test_sc_addr(51);
        let created = mk_async_message(
            &sender.to_string(),
            &dest.to_string(),
            "v1",
            10,
            1_000,
        );
        // SET first
        let set_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-50".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Set as i32,
                message: Some(m::async_pool_change_value::Message::CreatedMessage(created)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(10, 0, true, vec![set_entry]))
            .unwrap();
        let ts1 = ingest.db.read_async_msg("AM-50").unwrap().unwrap().first_seen_ts_ms;

        // UPDATE: bump coins, change handler, flip can_be_executed
        let upd = m::AsyncMessageUpdate {
            emission_slot: None,
            emission_index: None,
            sender: None,
            destination: None,
            handler: Some(m::SetOrKeepString {
                change: Some(m::set_or_keep_string::Change::Set("v2".into())),
            }),
            max_gas: None,
            fee: None,
            coins: Some(m::SetOrKeepUint64 {
                change: Some(m::set_or_keep_uint64::Change::Set(999)),
            }),
            validity_start: None,
            validity_end: None,
            data: None,
            trigger: None,
            can_be_executed: Some(m::SetOrKeepBool {
                change: Some(m::set_or_keep_bool::Change::Set(false)),
            }),
        };
        let upd_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-50".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Update as i32,
                message: Some(m::async_pool_change_value::Message::UpdatedMessage(upd)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(11, 0, true, vec![upd_entry]))
            .unwrap();

        let got = ingest.db.read_async_msg("AM-50").unwrap().unwrap();
        assert_eq!(got.handler.as_deref(), Some("v2"));
        assert_eq!(got.coins_nmas, 999);
        assert!(!got.can_be_executed);
        // first-seen preserved through upsert
        assert_eq!(got.first_seen_ts_ms, ts1);
        assert_eq!(got.state, AsyncMsgState::Pending);
        assert_eq!(got.last_slot, Some(Slot::new(11, 0)));
    }

    #[tokio::test]
    async fn async_pool_delete_marks_consumed_then_transfers_promote_to_executed() {
        let mut ingest = mk_env();
        let sender = crate::ids::mk_test_user_addr(60);
        let dest = crate::ids::mk_test_sc_addr(61);
        let created = mk_async_message(
            &sender.to_string(),
            &dest.to_string(),
            "h",
            7,
            1,
        );
        let set_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-60".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Set as i32,
                message: Some(m::async_pool_change_value::Message::CreatedMessage(created)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(20, 0, true, vec![set_entry]))
            .unwrap();

        let del_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-60".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Delete as i32,
                message: None,
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(21, 0, true, vec![del_entry]))
            .unwrap();
        let got = ingest.db.read_async_msg("AM-60").unwrap().unwrap();
        assert_eq!(got.state, AsyncMsgState::Consumed);

        // Now simulate the transfer stream carrying the AsyncMsgCoins hint.
        let slot = Slot::new(21, 0);
        let transfer = StoredTransfer {
            slot,
            index_in_slot: 0,
            id: "T-0".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: Some(sender.to_string()),
            to: Some(dest.to_string()),
            value: crate::model::TransferValue::Coins { nmas: 7 },
            origin: CoinOrigin::AsyncMsgCoins,
            operation_id: None,
            async_msg_id: Some("AM-60".into()),
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_async_from_transfers(&ingest.db, slot, &[transfer], 100).unwrap();
        let got = ingest.db.read_async_msg("AM-60").unwrap().unwrap();
        assert_eq!(got.state, AsyncMsgState::Executed);
    }

    #[tokio::test]
    async fn async_transfers_cancel_promotes_to_cancelled() {
        let ingest = mk_env();
        let slot = Slot::new(30, 0);
        let transfer = StoredTransfer {
            slot,
            index_in_slot: 0,
            id: "T-1".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: Some(crate::ids::mk_test_user_addr(70).to_string()),
            value: crate::model::TransferValue::Coins { nmas: 5 },
            origin: CoinOrigin::AsyncMsgCancel,
            operation_id: None,
            async_msg_id: Some("AM-70".into()),
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        // Row didn't exist before: should be minted on the fly.
        reconcile_async_from_transfers(&ingest.db, slot, &[transfer], 42).unwrap();
        let got = ingest.db.read_async_msg("AM-70").unwrap().unwrap();
        assert_eq!(got.state, AsyncMsgState::Cancelled);
        assert_eq!(got.last_slot, Some(slot));
    }

    #[tokio::test]
    async fn deferred_transfers_register_then_execute() {
        let ingest = mk_env();
        let caller = crate::ids::mk_test_user_addr(80);
        let target = crate::ids::mk_test_sc_addr(81);

        // REGISTER at slot 100
        let slot_reg = Slot::new(100, 0);
        let reg = StoredTransfer {
            slot: slot_reg,
            index_in_slot: 0,
            id: "T-2".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: Some(caller.to_string()),
            to: None,
            value: crate::model::TransferValue::Coins { nmas: 5_000 },
            origin: CoinOrigin::DeferredCallRegister,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: Some("DC-80".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_deferred_from_transfers(&ingest.db, slot_reg, &[reg], 10).unwrap();
        let got = ingest.db.read_deferred_call("DC-80").unwrap().unwrap();
        assert_eq!(got.state, DeferredCallState::Registered);
        assert_eq!(got.sender.as_ref().unwrap().to_string(), caller.to_string());
        assert_eq!(got.registered_slot, Some(slot_reg));
        assert_eq!(got.coins_nmas, 5_000);

        // EXECUTE at slot 150
        let slot_exec = Slot::new(150, 0);
        let exec = StoredTransfer {
            slot: slot_exec,
            index_in_slot: 0,
            id: "T-3".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: Some(target.to_string()),
            value: crate::model::TransferValue::Coins { nmas: 5_000 },
            origin: CoinOrigin::DeferredCallCoins,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: Some("DC-80".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_deferred_from_transfers(&ingest.db, slot_exec, &[exec], 20).unwrap();
        let got = ingest.db.read_deferred_call("DC-80").unwrap().unwrap();
        assert_eq!(got.state, DeferredCallState::Executed);
        assert_eq!(got.target_slot, Some(slot_exec));
        assert_eq!(
            got.target_address.as_ref().unwrap().to_string(),
            target.to_string()
        );
        // sender (caller) preserved from the REGISTER row
        assert_eq!(got.sender.as_ref().unwrap().to_string(), caller.to_string());
        // registered_slot preserved (we never downgrade)
        assert_eq!(got.registered_slot, Some(slot_reg));
    }

    #[tokio::test]
    async fn deferred_transfers_fail_and_cancel_are_terminal() {
        let ingest = mk_env();

        let slot_f = Slot::new(200, 0);
        let fail = StoredTransfer {
            slot: slot_f,
            index_in_slot: 0,
            id: "T-4".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: None,
            value: crate::model::TransferValue::Coins { nmas: 0 },
            origin: CoinOrigin::DeferredCallFail,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: Some("DC-FAIL".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_deferred_from_transfers(&ingest.db, slot_f, &[fail], 30).unwrap();
        assert_eq!(
            ingest
                .db
                .read_deferred_call("DC-FAIL")
                .unwrap()
                .unwrap()
                .state,
            DeferredCallState::Failed,
        );

        // A subsequent storage refund must not downgrade the terminal state.
        let refund = StoredTransfer {
            slot: slot_f,
            index_in_slot: 1,
            id: "T-4b".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: None,
            value: crate::model::TransferValue::Coins { nmas: 0 },
            origin: CoinOrigin::DeferredCallStorageRefund,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: Some("DC-FAIL".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_deferred_from_transfers(&ingest.db, slot_f, &[refund], 31).unwrap();
        assert_eq!(
            ingest
                .db
                .read_deferred_call("DC-FAIL")
                .unwrap()
                .unwrap()
                .state,
            DeferredCallState::Failed,
        );

        let slot_c = Slot::new(201, 0);
        let cancel = StoredTransfer {
            slot: slot_c,
            index_in_slot: 0,
            id: "T-5".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: None,
            to: None,
            value: crate::model::TransferValue::Coins { nmas: 0 },
            origin: CoinOrigin::DeferredCallCancel,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: Some("DC-CXL".into()),
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        reconcile_deferred_from_transfers(&ingest.db, slot_c, &[cancel], 40).unwrap();
        assert_eq!(
            ingest
                .db
                .read_deferred_call("DC-CXL")
                .unwrap()
                .unwrap()
                .state,
            DeferredCallState::Cancelled,
        );
    }

    #[tokio::test]
    async fn async_never_downgrades_from_terminal() {
        let mut ingest = mk_env();
        // Seed an already-Executed row (as if transfers ran first).
        let seed = StoredAsyncMsg {
            id: "AM-T".into(),
            sender: None,
            destination: None,
            handler: None,
            coins_nmas: 0,
            max_gas: 0,
            fee_nmas: 0,
            emission_slot: None,
            validity_start: None,
            validity_end: None,
            state: AsyncMsgState::Executed,
            last_slot: None,
            data_hex: None,
            trigger: None,
            can_be_executed: true,
            first_seen_ts_ms: 1,
            last_updated_ts_ms: 1,
        };
        ingest.db.write_async_msg(&seed).unwrap();

        // DELETE should not push Executed back to Consumed.
        let del = m::AsyncPoolChangeEntry {
            async_message_id: "AM-T".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Delete as i32,
                message: None,
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(99, 0, true, vec![del]))
            .unwrap();
        let got = ingest.db.read_async_msg("AM-T").unwrap().unwrap();
        assert_eq!(got.state, AsyncMsgState::Executed);
    }

    #[tokio::test]
    async fn async_update_for_unseen_id_creates_stub() {
        let mut ingest = mk_env();
        // We never saw the SET (e.g. we joined the node mid-stream). An
        // UPDATE for an unknown id must still produce a row so the
        // explorer can resolve it later.
        let upd = m::AsyncMessageUpdate {
            emission_slot: None,
            emission_index: None,
            sender: Some(m::SetOrKeepString {
                change: Some(m::set_or_keep_string::Change::Set(
                    crate::ids::mk_test_user_addr(90).to_string(),
                )),
            }),
            destination: None,
            handler: Some(m::SetOrKeepString {
                change: Some(m::set_or_keep_string::Change::Set("late".into())),
            }),
            max_gas: None,
            fee: None,
            coins: None,
            validity_start: None,
            validity_end: None,
            data: None,
            trigger: None,
            can_be_executed: None,
        };
        let upd_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-STUB".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Update as i32,
                message: Some(m::async_pool_change_value::Message::UpdatedMessage(upd)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(7, 0, true, vec![upd_entry]))
            .unwrap();
        let got = ingest.db.read_async_msg("AM-STUB").unwrap().unwrap();
        assert_eq!(got.handler.as_deref(), Some("late"));
        assert_eq!(got.state, AsyncMsgState::Pending);
    }

    #[tokio::test]
    async fn async_update_address_refreshes_secondary_indexes() {
        let mut ingest = mk_env();
        let old_sender = crate::ids::mk_test_user_addr(100);
        let new_sender = crate::ids::mk_test_user_addr(101);
        let dest = crate::ids::mk_test_sc_addr(102);
        let created = mk_async_message(
            &old_sender.to_string(),
            &dest.to_string(),
            "h",
            1,
            1,
        );
        let set_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-IDX".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Set as i32,
                message: Some(m::async_pool_change_value::Message::CreatedMessage(created)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(1, 0, true, vec![set_entry]))
            .unwrap();
        assert_eq!(
            ingest
                .db
                .iter_async_by_addr(&old_sender, AsyncByAddr::Sender, None, 16)
                .unwrap()
                .items
                .len(),
            1
        );

        // Rewrite sender with an UPDATE.
        let upd = m::AsyncMessageUpdate {
            emission_slot: None,
            emission_index: None,
            sender: Some(m::SetOrKeepString {
                change: Some(m::set_or_keep_string::Change::Set(new_sender.to_string())),
            }),
            destination: None,
            handler: None,
            max_gas: None,
            fee: None,
            coins: None,
            validity_start: None,
            validity_end: None,
            data: None,
            trigger: None,
            can_be_executed: None,
        };
        let upd_entry = m::AsyncPoolChangeEntry {
            async_message_id: "AM-IDX".into(),
            value: Some(m::AsyncPoolChangeValue {
                r#type: m::AsyncPoolChangeType::Update as i32,
                message: Some(m::async_pool_change_value::Message::UpdatedMessage(upd)),
            }),
        };
        ingest
            .handle_exec(exec_output_with_async(2, 0, true, vec![upd_entry]))
            .unwrap();

        // Old sender's index should be empty, new one should have it.
        assert_eq!(
            ingest
                .db
                .iter_async_by_addr(&old_sender, AsyncByAddr::Sender, None, 16)
                .unwrap()
                .items
                .len(),
            0
        );
        assert_eq!(
            ingest
                .db
                .iter_async_by_addr(&new_sender, AsyncByAddr::Sender, None, 16)
                .unwrap()
                .items
                .len(),
            1
        );
    }
}
