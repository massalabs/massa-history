//! Legacy DDB source: turns a `(period, thread)` request into a
//! `FinalSlotResponse` carrying everything we can recover from the
//! archived block-storer tables.
//!
//! Wired into the backfill scanner via [`scan_legacy_pass`]. The scanner
//! consults the legacy source ONLY after every configured peer has
//! responded with `final_known = false` (or errored out), preserving the
//! precedence rule the user requested:
//!
//!   1. Live node stream (always wins),
//!   2. Other indexers (via peer backfill — first-final-wins),
//!   3. Legacy DDB (gap-filler, additive).
//!
//! ## Trait shape
//!
//! [`LegacySource`] returns an `Option<FinalSlotResponse>` so the caller
//! can distinguish "we had no data" (`Ok(None)`) from "we hit an error"
//! (`Err`). Wrapping the trait lets the unit tests fake the DDB layer
//! out completely — see the in-module fixture tests at the bottom.
//!
//! ## What we ship
//!
//! Per-slot lookup walks the legacy tables:
//!
//!   * `BlocksMainnet.PointInTimeAndHashIndex` — find the block hash for
//!     the slot. Legacy stored at most one block per slot (the FINAL one),
//!     so a single result row settles the verdict.
//!   * `BlocksMainnet[Hash]` — pull the full `BlockWrapper.Raw` to decode
//!     the block header + embedded endorsements + denunciations.
//!   * `OperationsMainnet[Hash]` (BatchGetItem, fanned out from
//!     `block.operations`) — pull every top-level op the block referenced.
//!   * `OperationsMainnet.PointInTimeAndHashIndex` — query *every* row
//!     for the slot (top-level ops + sub-transfers). Sub-transfer rows
//!     have `Hash` ending in `_<n>` and carry the actual coin movement
//!     that happened during execution: ABI sub-transfers from CallSC,
//!     fee transfers, block rewards, endorsement rewards, slash
//!     transfers, async-msg coin movements, etc.
//!   * Synthesise transfers from both the top-level Type=Transaction
//!     ops AND every executed sub-transfer row.
//!
//! What we DON'T ship: SC events, async-pool rows, deferred-call rows,
//! slot trail hashes. Those stay open for a real peer (or the live
//! node) to fill later — `apply_legacy_patch` leaves the
//! `exec_output_final` / `transfers_stored` completeness bits unset
//! so the regular peer scanner can re-cover the slot when richer data
//! becomes available.

use crate::{
    legacy::{
        config::LegacyDdbCfg,
        ddb::{AttributeValue, DdbClient, DdbError, Item, QueryRequest},
        decode::{
            decode_block_row, decode_op_row, item_bytes, item_num_i64, item_num_u32, item_str,
            item_str_opt, legacy_op_to_transfer, legacy_sub_transfer_to_transfer,
            point_in_time_to_slot, BlockRow, DecodeError, DecodedOp, OpRow, SubTransferRow,
        },
        point_in_time,
    },
    proto::indexer::v1::FinalSlotResponse,
};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};

/// Errors the legacy source can raise. Mostly transparent wrappers over
/// the DDB client / decode layer — kept distinct so log lines say
/// "legacy" rather than "ddb".
#[derive(Debug, Error)]
pub enum LegacyError {
    #[error("legacy ddb: {0}")]
    Ddb(#[from] DdbError),
    #[error("legacy decode: {0}")]
    Decode(#[from] DecodeError),
    #[error("legacy: {0}")]
    Other(String),
}

/// What [`LegacySource::fetch_slot`] returns. Wraps a fully-built
/// `FinalSlotResponse` so the caller can ship it through the existing
/// `Event::LegacyPatch` channel without re-deriving anything.
pub struct LegacyFetch {
    pub resp: FinalSlotResponse,
    /// Number of DDB RPCs the lookup consumed (1 for `BlocksMainnet
    /// Query`, optional +1 for `BlocksMainnet GetItem`, +1 per 100
    /// op rows in `OperationsMainnet BatchGet`, +1 for the
    /// `OperationsMainnet PointInTimeAndHashIndex` Query that pulls
    /// sub-transfers). Surfaced for metrics / debugging.
    pub rpcs: u32,
}

/// Trait abstraction over the legacy DDB lookup. Mockable in tests.
///
/// Implementations are expected to be cheap to clone (the production
/// `DdbLegacySource` wraps an `Arc<DdbClient>`).
#[tonic::async_trait]
pub trait LegacySource: Send + Sync {
    /// Try to assemble a `FinalSlotResponse` for `(period, thread)`.
    /// Returns `Ok(None)` when legacy has no block at that slot
    /// (e.g. miss / out of window) — the caller leaves the slot alone
    /// and waits for a real peer to settle it.
    async fn fetch_slot(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Option<LegacyFetch>, LegacyError>;
}

/// Production source: hits real DynamoDB.
#[derive(Clone)]
pub struct DdbLegacySource {
    cfg: Arc<LegacyDdbCfg>,
    ddb: DdbClient,
}

impl DdbLegacySource {
    pub fn new(cfg: Arc<LegacyDdbCfg>) -> Result<Self, LegacyError> {
        let ddb = DdbClient::new(cfg.clone())?;
        Ok(Self { cfg, ddb })
    }

    /// Helper that queries the BlocksMainnet GSI for the block hash at
    /// `(period, thread)`. Returns the row's `Hash` and the full row
    /// already pre-fetched (legacy projects every column onto the GSI,
    /// so a single `Query` is enough).
    async fn find_block_row(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Option<Item>, LegacyError> {
        let pit = point_in_time(period, thread);
        let mut vals = HashMap::new();
        vals.insert(":p".into(), AttributeValue::n_num(pit));
        let resp = self
            .ddb
            .query(QueryRequest {
                table_name: &self.cfg.blocks_table,
                index_name: Some("PointInTimeAndHashIndex"),
                key_condition_expression: "PointInTime = :p".into(),
                expression_attribute_values: vals,
                limit: Some(1),
                ..Default::default()
            })
            .await?;
        Ok(resp.items.into_iter().next())
    }

    /// Query every `OperationsMainnet` row whose `PointInTime` matches
    /// the slot. Returns top-level operations *and* sub-transfers
    /// (rows with `_` in `Hash`). The GSI projection
    /// (`PointInTimeAndHashIndex`) does not include `Raw`, so the
    /// caller must `BatchGetItem` the rows it needs full payloads
    /// for — typically just the top-level ones.
    ///
    /// Pagination loop terminates when DDB stops returning a
    /// `LastEvaluatedKey`. In practice mainnet slots have at most a
    /// few hundred rows even on busy days, so a single page is
    /// usually enough; the loop is there for safety.
    async fn list_slot_rows(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Vec<Item>, LegacyError> {
        let pit = point_in_time(period, thread);
        let mut out: Vec<Item> = Vec::new();
        let mut last_key: Option<Item> = None;
        loop {
            let mut vals = HashMap::new();
            vals.insert(":p".into(), AttributeValue::n_num(pit));
            let resp = self
                .ddb
                .query(QueryRequest {
                    table_name: &self.cfg.operations_table,
                    index_name: Some("PointInTimeAndHashIndex"),
                    key_condition_expression: "PointInTime = :p".into(),
                    expression_attribute_values: vals,
                    exclusive_start_key: last_key.clone(),
                    ..Default::default()
                })
                .await?;
            out.extend(resp.items);
            match resp.last_evaluated_key {
                Some(k) if !k.is_empty() => last_key = Some(k),
                _ => break,
            }
        }
        Ok(out)
    }
}

#[tonic::async_trait]
impl LegacySource for DdbLegacySource {
    async fn fetch_slot(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Option<LegacyFetch>, LegacyError> {
        if !self.cfg.within_window(period) {
            // Operator opted out of querying past the legacy cut-off —
            // pretend "no data" so the scanner doesn't even count an RPC.
            debug!(period, thread, "legacy: outside max_period window");
            return Ok(None);
        }
        let mut rpcs = 1u32; // Query
        let Some(blk_row) = self.find_block_row(period, thread).await? else {
            return Ok(None);
        };
        // The GSI projects all attributes including `Raw`, so we usually
        // don't need a second GetItem. But the projection may have been
        // tweaked at any point in the past, so we fall back to GetItem
        // if `Raw` is missing.
        let blk_row = if blk_row.contains_key("Raw") {
            blk_row
        } else {
            rpcs += 1;
            let hash = item_str(&blk_row, "Hash")?.to_string();
            let mut key = Item::new();
            key.insert("Hash".into(), AttributeValue::s_str(&hash));
            self.ddb
                .get_item(&self.cfg.blocks_table, key)
                .await?
                .item
                .ok_or_else(|| LegacyError::Other(format!(
                    "legacy: BlocksMainnet[{hash}] vanished between Query and GetItem",
                )))?
        };

        let block_hash = item_str(&blk_row, "Hash")?.to_string();
        let creator = item_str(&blk_row, "CreatorAddress")?.to_string();
        let raw = item_bytes(&blk_row, "Raw")?;
        let pit = item_num_u32(&blk_row, "PointInTime")?;
        let slot = point_in_time_to_slot(pit);
        if slot.period != period || slot.thread != thread {
            return Err(LegacyError::Other(format!(
                "legacy: row PIT decode {:?} mismatches request ({period},{thread})",
                slot
            )));
        }

        let now_ms = now_ms();
        let block_row_view = BlockRow {
            hash: &block_hash,
            raw: &raw,
            slot,
            creator_address: &creator,
        };
        let (block, op_ids, _embedded_endos) = decode_block_row(&block_row_view, now_ms)?;

        // Build the final response shell.
        let mut resp = FinalSlotResponse {
            period,
            thread: thread as u32,
            final_known: true,
            is_miss: false, // Legacy never stored miss slots.
            execution_trail_hash: String::new(),
            final_block_id: block_hash.clone(),
            ..Default::default()
        };

        // Block + embedded endorsements/denunciations go on the wire as
        // a single `StoredBlockPb`. The embedded endorsements are also
        // shipped explicitly so the receiver writes them into
        // `cf_endorsement` (peer patch already does this — we mirror).
        match crate::codec::block_to_peer_pb(&block) {
            Ok(b) => resp.block = Some(b),
            Err(e) => {
                warn!(error = %e, "legacy: encode block_to_peer_pb");
            }
        }
        for endo in &block.endorsements {
            match crate::codec::endorsement_to_peer_pb(endo) {
                Ok(p) => resp.endorsements.push(p),
                Err(e) => warn!(error = %e, "legacy: encode endorsement_to_peer_pb"),
            }
        }

        // We process operations + sub-transfers in two passes so the
        // synth-vs-row anti-duplicate check (see below) has perfect
        // information: collect every sub-transfer row first, so we
        // know which top-level Type=Transaction ops are already
        // covered by an explicit `_N` row and must therefore NOT be
        // re-synthesised.
        rpcs += 1;
        let slot_rows = self.list_slot_rows(period, thread).await?;
        let covered_top_level_op_ids = covered_op_ids_from_rows(&slot_rows);

        // Operations + executed_op_ids — fan out via BatchGetItem in
        // chunks of 100 (DDB hard limit).
        if !op_ids.is_empty() {
            let mut decoded_ops: Vec<DecodedOp> = Vec::with_capacity(op_ids.len());
            for chunk in op_ids.chunks(100) {
                rpcs += 1;
                let mut keys: Vec<Item> = Vec::with_capacity(chunk.len());
                for h in chunk {
                    let mut k = Item::new();
                    k.insert("Hash".into(), AttributeValue::s_str(h));
                    keys.push(k);
                }
                let batch = self
                    .ddb
                    .batch_get(&self.cfg.operations_table, keys)
                    .await?;
                let rows = batch
                    .responses
                    .get(&self.cfg.operations_table)
                    .cloned()
                    .unwrap_or_default();
                for row in rows {
                    let hash = match item_str(&row, "Hash") {
                        Ok(s) => s.to_string(),
                        Err(e) => {
                            warn!(error = %e, "legacy: op row missing Hash");
                            continue;
                        }
                    };
                    let block_h = item_str_opt(&row, "BlockHash").map(str::to_string);
                    let status = item_num_i64(&row, "Status").unwrap_or(0);
                    let raw = match item_bytes(&row, "Raw") {
                        Ok(b) => b,
                        Err(e) => {
                            warn!(error = %e, hash = %hash, "legacy: op row missing Raw");
                            continue;
                        }
                    };
                    let block_h = block_h.unwrap_or_else(|| block_hash.clone());
                    let row_view = OpRow {
                        hash: &hash,
                        raw: &raw,
                        status,
                        block_hash: &block_h,
                        slot,
                    };
                    match decode_op_row(&row_view, now_ms) {
                        Ok(d) => decoded_ops.push(d),
                        Err(e) => warn!(error = %e, "legacy: decode op_row"),
                    }
                }
            }

            // Ship operations on the wire. The receiver only writes ops
            // that aren't already present locally (peer patch logic),
            // so order doesn't matter.
            for d in &decoded_ops {
                match crate::codec::operation_to_peer_pb(&d.stored) {
                    Ok(p) => resp.operations.push(p),
                    Err(e) => warn!(error = %e, "legacy: encode operation_to_peer_pb"),
                }
            }

            // executed_op_ids = subset that the legacy storer marked Executed.
            for d in &decoded_ops {
                if d.executed {
                    resp.executed_op_ids.push(d.stored.id.to_string());
                }
            }

            // Synthetic transfers: one per successfully-executed
            // Type=Transaction op that does NOT already have an
            // explicit `<op_id>_N` sub-transfer row in DDB.
            //
            // In practice mainnet Type=Transaction ops never produce a
            // companion sub-transfer (the row IS the transfer), so this
            // synthesises every executed Transaction. The
            // `covered_top_level_op_ids` filter is purely defensive: if
            // the legacy storer ever emitted both shapes for the same
            // op (e.g. for a future op type), we ship only the explicit
            // row to avoid double-counting the same coin movement.
            let block_ts_ms = now_ms; // legacy never recorded a ts; use now.
            for (i, d) in decoded_ops.iter().enumerate() {
                if !d.executed {
                    continue;
                }
                if covered_top_level_op_ids.contains(d.stored.id.as_str()) {
                    debug!(
                        op_id = %d.stored.id,
                        period, thread,
                        "legacy: skipping op-to-transfer synth (sub-transfer row exists)"
                    );
                    continue;
                }
                if let Some(t) = legacy_op_to_transfer(
                    &d.stored,
                    slot,
                    &block_hash,
                    i as u32,
                    block_ts_ms,
                ) {
                    resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));
                }
            }
        }

        // Sub-transfers (ABI sub-transfers from CallSC, slot-bound
        // rewards / slashes / async-msg coin movements, …). These
        // are the rows we collected at the top of `fetch_slot` for
        // the synth-deduplication pass. Decode each one into a
        // `StoredTransfer` and ship it with a packed `index_in_slot`
        // that follows the synthesised batch.
        let mut sub_index_in_slot: u32 = decoded_op_count_top_level(&resp);
        for row in &slot_rows {
            let hash = match item_str(row, "Hash") {
                Ok(h) => h,
                Err(_) => continue,
            };
            // Sub-transfer rows have `_` in their hash. Top-level ops
            // were already shipped above via `BatchGetItem`.
            if !hash.contains('_') {
                continue;
            }
            let status = item_num_i64(row, "Status").unwrap_or(0);
            let original_op_id = item_str_opt(row, "OriginalOperationID");
            let slot_and_asc = item_str_opt(row, "SlotAndAscIndex");
            let creator_address = match item_str(row, "CreatorAddress") {
                Ok(s) => s,
                Err(_) => "",
            };
            let other_address = item_str_opt(row, "OtherAddress");
            // Amount is a Map { Mantissa: N, Scale: N }; we only care
            // about the Mantissa (legacy always used Scale = 9 for
            // coin sub-transfers).
            let amount_nmas = native_amount_mantissa(row, "Amount").unwrap_or(0);

            let row_view = SubTransferRow {
                hash,
                status,
                original_op_id,
                slot_and_asc_index: slot_and_asc,
                creator_address,
                other_address,
                amount_nmas,
            };
            if let Some(t) = legacy_sub_transfer_to_transfer(
                &row_view,
                slot,
                Some(&block_hash),
                sub_index_in_slot,
                now_ms,
            ) {
                resp.transfers.push(crate::codec::transfer_to_peer_pb(&t));
                sub_index_in_slot = sub_index_in_slot.saturating_add(1);
            }
        }

        Ok(Some(LegacyFetch { resp, rpcs }))
    }
}

/// Count how many top-level (non-sub-transfer) entries are already in
/// the response's `transfers` list. Used as the starting offset for
/// the sub-transfer indices so that legacy transfer rows have stable,
/// non-colliding `index_in_slot` values.
fn decoded_op_count_top_level(resp: &FinalSlotResponse) -> u32 {
    resp.transfers.len() as u32
}

/// Walk a slot's `OperationsMainnet` rows and collect every
/// `OriginalOperationID` referenced by an explicit sub-transfer row
/// (i.e. one whose `Hash` contains `_`). Used by [`fetch_slot`] to
/// suppress synthesised top-level Type=Transaction transfers when the
/// matching `<op_id>_N` row already provides the precise coin
/// movement — preventing double-counting of the same transfer.
///
/// Slot-bound sub-transfers (block / endorsement rewards, slashes,
/// async-msg coin movements) carry `OriginalOperationID = "0"` and
/// are intentionally excluded: they don't shadow any top-level op
/// synthesis, only their own dedicated row.
fn covered_op_ids_from_rows(rows: &[Item]) -> std::collections::HashSet<String> {
    let mut covered = std::collections::HashSet::new();
    for row in rows {
        let Some(hash) = row.get("Hash").and_then(|v| v.as_str()) else {
            continue;
        };
        if !hash.contains('_') {
            continue;
        }
        if let Some(orig) = row.get("OriginalOperationID").and_then(|v| v.as_str()) {
            if !orig.is_empty() && orig != "0" {
                covered.insert(orig.to_string());
            }
        }
    }
    covered
}

/// Read a `NativeAmount` map (`{ "Mantissa": <N>, "Scale": <N> }`) and
/// return its mantissa as `u64`. Returns `None` if the field is missing
/// or malformed. Legacy always wrote Scale = 9 for coin amounts, so we
/// don't bother re-scaling.
fn native_amount_mantissa(row: &Item, field: &str) -> Option<u64> {
    let v = row.get(field)?;
    let m = v.m.as_ref()?;
    m.get("Mantissa")?.as_num()?.parse::<u64>().ok()
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A tiny in-memory `LegacySource` for tests, returning canned responses
/// keyed by `(period, thread)`. Built from real production DDB rows in
/// the unit tests.
///
/// Compiled under `#[cfg(any(test, feature = "test-exports"))]` so
/// integration tests outside this crate can reach for it too.
#[cfg(any(test, feature = "test-exports"))]
pub struct StubLegacySource {
    rows: std::sync::Mutex<HashMap<(u64, u8), LegacyFetch>>,
    pub call_count: std::sync::atomic::AtomicU32,
}

#[cfg(any(test, feature = "test-exports"))]
impl StubLegacySource {
    pub fn new(rows: HashMap<(u64, u8), LegacyFetch>) -> Self {
        Self {
            rows: std::sync::Mutex::new(rows),
            call_count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[cfg(any(test, feature = "test-exports"))]
#[tonic::async_trait]
impl LegacySource for StubLegacySource {
    async fn fetch_slot(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Option<LegacyFetch>, LegacyError> {
        self.call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Pull the entry out — `LegacyFetch` isn't `Clone` (proto types
        // aren't), so the stub is one-shot per slot. The real source is
        // one-shot too (hits DDB once).
        Ok(self.rows.lock().unwrap().remove(&(period, thread)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::legacy::ddb::AttributeValue;

    #[tokio::test]
    async fn stub_source_returns_none_for_unknown_slot() {
        let src = StubLegacySource::new(HashMap::new());
        assert!(src.fetch_slot(1, 0).await.unwrap().is_none());
        assert_eq!(
            src.call_count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    fn row(hash: &str, original_op_id: Option<&str>) -> Item {
        let mut r = Item::new();
        r.insert("Hash".into(), AttributeValue::s_str(hash));
        if let Some(o) = original_op_id {
            r.insert("OriginalOperationID".into(), AttributeValue::s_str(o));
        }
        r
    }

    #[test]
    fn covered_op_ids_extracts_only_op_driven_subs() {
        let rows = vec![
            row("OAlphaBetaGammaDeltaEpsilonZetaEta12345678901234567890", None),
            row(
                "OAlphaBetaGammaDeltaEpsilonZetaEta12345678901234567890_0",
                Some("OAlphaBetaGammaDeltaEpsilonZetaEta12345678901234567890"),
            ),
            row("REWARD_block_0", Some("0")),
            row("REWARD_endo_0", None),
            row(
                "OOtherOpHashHere1234567890_3",
                Some("OOtherOpHashHere1234567890"),
            ),
        ];
        let covered = covered_op_ids_from_rows(&rows);
        assert_eq!(covered.len(), 2);
        assert!(covered.contains("OAlphaBetaGammaDeltaEpsilonZetaEta12345678901234567890"));
        assert!(covered.contains("OOtherOpHashHere1234567890"));
    }

    #[test]
    fn covered_op_ids_handles_missing_or_zero_orig() {
        let rows = vec![
            row("X_0", Some("")),
            row("Y_0", Some("0")),
            row("Z_0", None),
        ];
        let covered = covered_op_ids_from_rows(&rows);
        assert!(
            covered.is_empty(),
            "rows w/o real OriginalOperationID must not suppress synthesis"
        );
    }

    #[test]
    fn covered_op_ids_ignores_top_level_rows() {
        let rows = vec![
            row(
                "O1nK8xCM65DmunW9qXQRMCTfTV27g6afVTLrLCjenZi1aktgnc",
                None,
            ),
        ];
        let covered = covered_op_ids_from_rows(&rows);
        assert!(covered.is_empty());
    }
}
