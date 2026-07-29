//! Decode legacy block-storer DDB rows into our domain types.
//!
//! The legacy storer wrote the **same** Massa node protobuf types we
//! already compile in `proto.rs` (`massa.model.v1.BlockWrapper`,
//! `OperationWrapper`, `SignedEndorsement`), so prost decodes them
//! directly. The flat columns on each row (`Status`, `PointInTime`,
//! `BlockHash`, …) provide the bits the protobuf doesn't carry.
//!
//! ## Output shape
//!
//! All decoders return the corresponding `Stored*` value plus, when
//! relevant, side-effects derived from the row:
//!
//! * [`decode_block_row`] returns `(StoredBlock, Vec<op_id>)`. The
//!   `Vec<op_id>` is the set of operation hashes the block references —
//!   the orchestrator uses it to drive a `BatchGetItem` against the
//!   `Operations*` table.
//! * [`decode_op_row`] returns a `StoredOperation` plus an optional
//!   `executed: bool` flag derived from the legacy `Status` column. The
//!   orchestrator collects executed op ids into the slot's
//!   `executed_op_ids` field.
//! * [`decode_endorsement_row`] returns a `StoredEndorsement`, fully
//!   populated.
//! * [`legacy_op_to_transfer`] turns a Type=Transaction operation into a
//!   synthetic [`StoredTransfer`] — legacy didn't have a dedicated
//!   transfers table, so we reconstruct the natural ones from the op
//!   itself.
//!
//! Every decoder is fallible, but failures produce a [`DecodeError`]
//! the worker can log + skip — we never panic on malformed legacy data.

use crate::ids::{Address, BlockId, OperationId};
use crate::legacy::ddb::Item;
use crate::model::{
    BlockStatus, CoinOrigin, DatastoreEntry, OperationDetails, OperationKind, Slot, StoredBlock,
    StoredDenunciation, StoredEndorsement, StoredOperation, StoredTransfer, TransferValue,
};
use crate::proto::massa::model::v1 as m;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use prost::Message;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("invalid id: {0}")]
    InvalidId(String),
    #[error("proto decode: {0}")]
    Proto(String),
    #[error("invalid attribute: {0}")]
    Attr(String),
}

impl From<prost::DecodeError> for DecodeError {
    fn from(e: prost::DecodeError) -> Self {
        DecodeError::Proto(e.to_string())
    }
}

/// Inputs the block decoder needs (the proto bytes + the flat columns).
pub struct BlockRow<'a> {
    pub hash: &'a str,
    pub raw: &'a [u8],
    /// `(period, thread)` derived by the caller from `PointInTime`.
    pub slot: Slot,
    pub creator_address: &'a str,
}

/// Decode a `BlocksMainnet` row.
///
/// Returns `(StoredBlock, Vec<op_id_str>, Vec<StoredEndorsement>)` so the
/// orchestrator can:
///   1. Persist the block via `apply_peer_patch`.
///   2. Look up each operation hash in `OperationsMainnet`.
///   3. Surface the embedded endorsement records (which we already
///      decoded for free from the block header).
pub fn decode_block_row(
    row: &BlockRow<'_>,
    now_ms: i64,
) -> Result<(StoredBlock, Vec<String>, Vec<StoredEndorsement>), DecodeError> {
    let bw = m::BlockWrapper::decode(row.raw)?;
    let block = bw.block.ok_or(DecodeError::MissingField("block"))?;
    let hdr = block.header.ok_or(DecodeError::MissingField("block.header"))?;
    let block_hdr = hdr
        .content
        .as_ref()
        .ok_or(DecodeError::MissingField("block.header.content"))?;

    let block_id = BlockId::parse(row.hash)
        .map_err(|e| DecodeError::InvalidId(format!("block hash {}: {e}", row.hash)))?;
    let creator = Address::parse(row.creator_address.to_string())
        .map_err(|e| DecodeError::InvalidId(format!("creator {}: {e}", row.creator_address)))?;
    let parents = block_hdr
        .parents
        .iter()
        .filter_map(|p| BlockId::parse(p.clone()).ok())
        .collect::<Vec<_>>();

    // Endorsements & denunciations — same logic as live ingest.
    let endorsements: Vec<StoredEndorsement> = block_hdr
        .endorsements
        .iter()
        .filter_map(|se| decode_signed_endorsement(se, &block_id, row.slot, now_ms))
        .collect();
    let endorsement_ids: Vec<String> = endorsements.iter().map(|e| e.id.clone()).collect();

    let denunciations: Vec<StoredDenunciation> = block_hdr
        .denunciations
        .iter()
        .map(decode_denunciation)
        .collect();

    let operation_ids = block.operations.clone();

    let raw_signed_header_b64 = B64.encode(hdr.encode_to_vec());

    let stored = StoredBlock {
        id: block_id,
        slot: row.slot,
        creator,
        parents,
        // We populate `operation_ids` only after the orchestrator confirms
        // each id parses; it would be wasteful to redo that here, so we
        // just stash the raw display strings on `endorsement_ids` etc.
        // and let the caller resolve them via OperationId::parse.
        operation_ids: operation_ids
            .iter()
            .filter_map(|s| OperationId::parse(s.clone()).ok())
            .collect(),
        endorsements: endorsements.clone(),
        endorsement_ids,
        denunciations,
        current_version: block_hdr.current_version,
        announced_version: block_hdr.announced_version,
        operations_hash: block_hdr.operations_hash.clone(),
        signature: hdr.signature.clone(),
        content_creator_pub_key: hdr.content_creator_pub_key.clone(),
        serialized_size: hdr.serialized_size,
        raw_signed_header_b64,
        // The legacy storer doesn't tag candidate vs final per row, but it
        // only ever stored what the node had finalised, so every legacy
        // block is FINAL by construction.
        status: BlockStatus::Final,
        first_seen_ts_ms: now_ms,
    };

    Ok((stored, operation_ids, endorsements))
}

/// One row of `OperationsMainnet`.
pub struct OpRow<'a> {
    pub hash: &'a str,
    pub raw: &'a [u8],
    /// Status flat column (`0=Unknown`, `1=Executed`, `2=Failed`) per the
    /// legacy `OperationStatus` enum.
    pub status: i64,
    /// Hash of the block the legacy storer claimed this op was included in.
    pub block_hash: &'a str,
    /// `(period, thread)` derived from `PointInTime`.
    pub slot: Slot,
}

/// Decoded operation + the executed flag derived from `OpRow.status`.
pub struct DecodedOp {
    pub stored: StoredOperation,
    /// True if this op was executed *successfully* at the legacy slot
    /// (mirrors `Status == 1` / `OperationStatusExecuted`). Failed ops
    /// (Status == 2) and unknown ones don't make it into a slot's
    /// `executed_op_ids` set in our model — only successful ops do.
    /// Both kinds nonetheless go through the regular write path so the
    /// op's detail page still surfaces them.
    pub executed: bool,
}

pub fn decode_op_row(row: &OpRow<'_>, now_ms: i64) -> Result<DecodedOp, DecodeError> {
    let wrapper = m::OperationWrapper::decode(row.raw)?;
    let signed = wrapper
        .operation
        .ok_or(DecodeError::MissingField("operation_wrapper.operation"))?;
    let content = signed
        .content
        .as_ref()
        .ok_or(DecodeError::MissingField("operation.content"))?;

    let op_id = OperationId::parse(row.hash)
        .map_err(|e| DecodeError::InvalidId(format!("op id {}: {e}", row.hash)))?;
    let block_id = BlockId::parse(row.block_hash)
        .map_err(|e| DecodeError::InvalidId(format!("block id {}: {e}", row.block_hash)))?;
    let creator = Address::parse(signed.content_creator_address.clone())
        .map_err(|e| DecodeError::InvalidId(format!("op creator: {e}")))?;
    let (kind, target, details) = classify_op_type(content.op.as_ref());
    let fee_nmas = content.fee.as_ref().map(|a| a.mantissa).unwrap_or(0);

    let executed = row.status == 1; // OperationStatusExecuted

    let stored = StoredOperation {
        id: op_id.clone(),
        creator,
        target,
        kind,
        expire_period: content.expire_period,
        fee_nmas,
        thread: row.slot.thread,
        inclusions: vec![crate::model::OperationInclusion {
            slot: row.slot,
            block_id: block_id.clone(),
        }],
        candidate_exec_status: None,
        // Legacy data is FINAL: stamp the corresponding status. We pass
        // the executed status only when Status==Executed; failed ops
        // get `Failed`, unknown ones skip the field.
        final_exec_status: match row.status {
            1 => Some(crate::model::ExecStatus::Ok),
            2 => Some(crate::model::ExecStatus::Failed),
            _ => None,
        },
        details,
        signature: signed.signature.clone(),
        content_creator_pub_key: signed.content_creator_pub_key.clone(),
        serialized_size: signed.serialized_size,
        raw_signed_op_b64: B64.encode(signed.encode_to_vec()),
        first_seen_ts_ms: now_ms,
    };

    Ok(DecodedOp { stored, executed })
}

/// Decode a `EndorsementsMainnet` row (used as a *fallback*: the same
/// endorsements are already embedded in the block header, but if the
/// caller wants to verify per-id we expose this entry-point too).
pub fn decode_endorsement_row(
    hash: &str,
    raw: &[u8],
    block_hash: &str,
    block_slot: Slot,
    now_ms: i64,
) -> Result<StoredEndorsement, DecodeError> {
    let signed = m::SignedEndorsement::decode(raw)?;
    let block_id = BlockId::parse(block_hash)
        .map_err(|e| DecodeError::InvalidId(format!("endo block id: {e}")))?;
    decode_signed_endorsement(&signed, &block_id, block_slot, now_ms)
        // The endorsement's `secure_hash` should match the row key but we
        // tolerate divergence (legacy storer occasionally re-encoded with
        // different signature paths) by trusting the row key.
        .map(|mut e| {
            e.id = hash.to_string();
            e
        })
        .ok_or_else(|| DecodeError::MissingField("endorsement.content"))
}

/// Build a synthetic `StoredTransfer` for a legacy "Type=Transaction"
/// operation. Returns `None` for non-Transaction ops or zero-amount ones.
///
/// The `block_timestamp_ms` is computed by the orchestrator from the
/// slot via the meta row's `genesis_timestamp_ms` + `t0_ms` — we don't
/// have anything per-row.
pub fn legacy_op_to_transfer(
    op: &StoredOperation,
    slot: Slot,
    block_id: &str,
    index_in_slot: u32,
    block_timestamp_ms: i64,
) -> Option<StoredTransfer> {
    if op.kind != OperationKind::Transaction {
        return None;
    }
    let amount = op.details.amount_nmas?;
    if amount == 0 {
        // Match live ingest: drop zero-amount transfers, they carry no
        // economic signal.
        return None;
    }
    let to = op.details.recipient_address.clone()?;
    Some(StoredTransfer {
        slot,
        index_in_slot,
        // Synthesise a stable id that won't collide with live ingest's
        // node-supplied ids. Format matches `<op_id>:<index>` which is
        // unambiguous and human-friendly.
        id: format!("{}:{}", op.id, index_in_slot),
        block_id: Some(block_id.to_string()),
        block_timestamp_ms,
        from: Some(op.creator.to_string()),
        to: Some(to),
        value: TransferValue::Coins { nmas: amount },
        origin: CoinOrigin::OpTransactionCoins,
        operation_id: Some(op.id.to_string()),
        async_msg_id: None,
        deferred_call_id: None,
        denunciation_index: None,
        is_final: true,
        first_seen_ts_ms: block_timestamp_ms,
    })
}

/// Decoded view of a legacy sub-transfer row from `OperationsMainnet`.
///
/// The legacy storer emitted one row per coin movement that happened
/// during a slot's execution — both **op-driven** (an ABI sub-transfer
/// emitted by a CallSC, an explicit fee transfer, …) and **slot-bound**
/// (block rewards, endorsement rewards, slashes, async-msg coin
/// movements). All such rows share:
///
/// * `Hash` ending in `_<n>` (so you can tell them apart from top-level
///   ops at scan time);
/// * `OriginalOperationID` set to the parent op's secure hash for
///   op-driven transfers, `"0"` for slot-bound ones;
/// * `SlotAndAscIndex` set to a non-`"0"` string for slot-bound
///   transfers, `"0"` for op-driven;
/// * `Type = 3` (Transfer) regardless of the parent op's type — the
///   legacy code mapped every sub-transfer to that bucket;
/// * `CreatorAddress` / `OtherAddress` carrying the actual sender +
///   recipient of the sub-transfer (NOT the parent op's creator);
/// * `Amount` in nanoMAS (Mantissa) with `Scale = 9`.
pub struct SubTransferRow<'a> {
    pub hash: &'a str,
    pub status: i64,
    pub original_op_id: Option<&'a str>,
    pub slot_and_asc_index: Option<&'a str>,
    pub creator_address: &'a str,
    pub other_address: Option<&'a str>,
    pub amount_nmas: u64,
}

/// Decode a sub-transfer row into a [`StoredTransfer`].
///
/// Returns `None` if `Status != Executed` (failed transfers were
/// stored with status=0/2 and have no economic effect) or the amount
/// is zero. The `index_in_slot` is supplied by the caller — usually
/// the `TransferIndex` projected from the row, or a running counter
/// if the column is unavailable.
///
/// `block_id` is `Some(...)` for op-driven transfers (we know which
/// block executed the parent op) and slot-bound rewards (attributed to
/// the slot's first block). Pass `None` if no block context is
/// available.
pub fn legacy_sub_transfer_to_transfer(
    row: &SubTransferRow<'_>,
    slot: Slot,
    block_id: Option<&str>,
    index_in_slot: u32,
    block_timestamp_ms: i64,
) -> Option<StoredTransfer> {
    // Status: 0 = Unknown, 1 = Executed, 2 = Failed (legacy enum).
    if row.status != 1 {
        return None;
    }
    if row.amount_nmas == 0 {
        return None;
    }

    // Op-driven: the hash prefix `O...._N` carries the parent op id
    // even when the column was left blank. Empirically (mainnet
    // 2024-2026) every sub-transfer row encodes its parent that way,
    // and the dedicated `OriginalOperationID` column is filled only
    // intermittently. Derive from the hash first; fall back to the
    // column for forward compatibility.
    let parent_op_from_hash: Option<&str> = row.hash.split_once('_').map(|(prefix, _)| prefix);
    let column_op_id = row
        .original_op_id
        .filter(|s| !s.is_empty() && *s != "0");
    let parent_op = parent_op_from_hash.or(column_op_id);
    let op_driven = parent_op.is_some();
    let slot_bound = matches!(row.slot_and_asc_index, Some(s) if !s.is_empty() && s != "0");

    // Best-effort `CoinOrigin`. Legacy didn't store the precise origin
    // (block reward vs endorsement reward vs slash, etc.), so we pick
    // the most-likely bucket based on the structural cues we have.
    // Live-stream peers can override this later — `apply_legacy_patch`
    // never blocks an exec_output ship.
    let origin = if op_driven {
        // ABI-driven coin transfer. The vast majority are
        // `AbiTransferCoins`; legacy can't distinguish from
        // `AbiCallCoins` etc. so we pick the umbrella variant.
        CoinOrigin::AbiTransferCoins
    } else if slot_bound {
        // Slot-bound. Could be a block reward, an endorsed/endorsement
        // reward, a slash, an async-msg coin movement, … legacy
        // collapsed all of those into one row shape. We pick
        // `BlockReward` as the most-frequent bucket; an explorer
        // looking at a deeply-historic slot will see it labelled as
        // such until / unless a live peer ships the precise origin.
        CoinOrigin::BlockReward
    } else {
        // Neither op-driven nor slot-bound. Shouldn't happen in
        // production data, but the code path stays compatible —
        // unknown bucket with no special semantics.
        CoinOrigin::Other { code: 0 }
    };

    let id = row.hash.to_string();
    let from = if row.creator_address.is_empty() {
        None
    } else {
        Some(row.creator_address.to_string())
    };
    let to = row
        .other_address
        .filter(|s| !s.is_empty() && *s != "0")
        .map(str::to_string);

    Some(StoredTransfer {
        slot,
        index_in_slot,
        id,
        block_id: block_id.map(str::to_string),
        block_timestamp_ms,
        from,
        to,
        value: TransferValue::Coins {
            nmas: row.amount_nmas,
        },
        origin,
        operation_id: parent_op.map(str::to_string),
        async_msg_id: None,
        deferred_call_id: None,
        denunciation_index: None,
        is_final: true,
        first_seen_ts_ms: block_timestamp_ms,
    })
}

// ---------------------------------------------------------------------------
// Helpers — these mirror `ingest::classify_op` / `ingest::decode_endorsement`
// / `ingest::decode_denunciation` exactly. Duplicated rather than reused
// to keep the legacy module self-contained (no pub-vis from `ingest`).
// ---------------------------------------------------------------------------

fn classify_op_type(
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
        m::operation_type::Type::RollBuy(rb) => (
            OperationKind::RollBuy,
            None,
            OperationDetails {
                roll_count: Some(rb.roll_count),
                ..Default::default()
            },
        ),
        m::operation_type::Type::RollSell(rs) => (
            OperationKind::RollSell,
            None,
            OperationDetails {
                roll_count: Some(rs.roll_count),
                ..Default::default()
            },
        ),
        m::operation_type::Type::ExecuteSc(e) => {
            let d = OperationDetails {
                bytecode_hex: Some(hex::encode(&e.data)),
                bytecode_size: Some(e.data.len() as u64),
                max_coins_nmas: Some(e.max_coins),
                max_gas: Some(e.max_gas),
                datastore_keys: Some(e.datastore.len() as u32),
                datastore: e
                    .datastore
                    .iter()
                    .map(|entry| DatastoreEntry {
                        key_hex: hex::encode(&entry.key),
                        value_hex: hex::encode(&entry.value),
                    })
                    .collect(),
                ..Default::default()
            };
            (OperationKind::ExecuteSc, None, d)
        }
        m::operation_type::Type::CallSc(c) => {
            let target = Address::parse(c.target_address.clone()).ok();
            let d = OperationDetails {
                target_address: Some(c.target_address.clone()),
                target_function: Some(c.target_function.clone()),
                parameter_hex: Some(hex::encode(&c.parameter)),
                parameter_len: Some(c.parameter.len() as u64),
                max_gas: Some(c.max_gas),
                coins_nmas: c.coins.as_ref().map(|a| a.mantissa),
                ..Default::default()
            };
            (OperationKind::CallSc, target, d)
        }
    }
}

fn decode_signed_endorsement(
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

// ---------------------------------------------------------------------------
// Item helpers — pull typed fields out of raw DDB rows.
// ---------------------------------------------------------------------------

pub fn item_str<'a>(row: &'a Item, field: &'static str) -> Result<&'a str, DecodeError> {
    row.get(field)
        .and_then(|av| av.as_str())
        .ok_or(DecodeError::MissingField(field))
}

pub fn item_str_opt<'a>(row: &'a Item, field: &str) -> Option<&'a str> {
    row.get(field).and_then(|av| av.as_str())
}

pub fn item_num_i64(row: &Item, field: &'static str) -> Result<i64, DecodeError> {
    let av = row.get(field).ok_or(DecodeError::MissingField(field))?;
    av.as_num()
        .ok_or_else(|| DecodeError::Attr(format!("{field} is not numeric")))?
        .parse::<i64>()
        .map_err(|e| DecodeError::Attr(format!("{field} parse: {e}")))
}

pub fn item_num_u32(row: &Item, field: &'static str) -> Result<u32, DecodeError> {
    let n = item_num_i64(row, field)?;
    u32::try_from(n).map_err(|_| DecodeError::Attr(format!("{field} not u32")))
}

pub fn item_bytes(row: &Item, field: &'static str) -> Result<Vec<u8>, DecodeError> {
    let av = row.get(field).ok_or(DecodeError::MissingField(field))?;
    av.bytes().ok_or(DecodeError::Attr("not B-encoded".into()))
}

/// Inverse of `legacy::point_in_time`: decode the legacy descending
/// `PointInTime` back into `(period, thread)`.
pub fn point_in_time_to_slot(pit: u32) -> Slot {
    let combined = (u32::MAX - pit) as u64;
    let period = combined / 100;
    let thread = (combined % 100) as u8;
    Slot::new(period, thread)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixture: a real `BlocksMainnet.Raw` payload sampled from production.
    /// 7291 bytes, period 4_500_000, thread 0.
    const BLOCK_4M_T0: &[u8] = include_bytes!("test_fixtures/block_4M_t0.bin");

    /// Fixture: a real `OperationsMainnet.Raw` payload (Type=Transfer).
    /// 396 bytes, op id `O12fxbma…`, block hash `B12akgwp…`, period 355440 thread 4.
    const OP_REAL: &[u8] = include_bytes!("test_fixtures/op_real.bin");

    /// Fixture: a real `EndorsementsMainnet.Raw` payload, 326 bytes,
    /// id `E1YAiBs…`, block id `B127Z9Bq…`.
    const ENDO_T0: &[u8] = include_bytes!("test_fixtures/endo_t0.bin");

    #[test]
    fn point_in_time_roundtrip() {
        for (p, t) in [
            (0u64, 0u8),
            (1, 0),
            (1_000_000, 5),
            (4_500_000, 0),
            (4_500_000, 31),
        ] {
            let pit = crate::legacy::point_in_time(p, t);
            let s = point_in_time_to_slot(pit);
            assert_eq!(s.period, p);
            assert_eq!(s.thread, t);
        }
    }

    /// Decode a real production block payload end-to-end.
    #[test]
    fn decodes_real_block_row() {
        let row = BlockRow {
            hash: "B127Z9Bqpvw9spCv1t8HJs9ZVhdx5zM5rEymcxXkqdCRM1C6ySQW",
            raw: BLOCK_4M_T0,
            slot: Slot::new(4_000_000, 0),
            creator_address: "AU122Em8qkqegdLb1eyH8rdkSCNEf7RZLeTJve4Q2inRPGiTJ2xNv",
        };
        let (block, op_ids, endorsements) =
            decode_block_row(&row, 1_700_000_000_000).expect("decode");
        assert_eq!(block.slot, Slot::new(4_000_000, 0));
        assert_eq!(
            block.id.to_string(),
            "B127Z9Bqpvw9spCv1t8HJs9ZVhdx5zM5rEymcxXkqdCRM1C6ySQW"
        );
        assert_eq!(
            block.creator.to_string(),
            "AU122Em8qkqegdLb1eyH8rdkSCNEf7RZLeTJve4Q2inRPGiTJ2xNv"
        );
        assert_eq!(block.status, BlockStatus::Final);
        // The block itself had no operations (matches the OperationCount=0
        // from the DDB scan), and the parents list is the four-block per-thread
        // tip set.
        assert!(op_ids.is_empty());
        assert_eq!(block.parents.len(), 32);
        // Endorsements: a real block always carries 16 endorsements (or
        // the genesis-window equivalent). At minimum we expect the decode
        // to succeed and produce some.
        assert!(!endorsements.is_empty(), "endorsements decoded");
        assert!(block.endorsement_ids.len() == endorsements.len());
        // Sanity: endorsement ids are non-empty bs58check strings.
        for e in &endorsements {
            assert!(e.id.starts_with('E'));
            assert!(!e.signature.is_empty());
            assert_eq!(e.included_slot, Slot::new(4_000_000, 0));
        }
        // The block row also re-encodes its raw header for byte-exact
        // archival — non-empty after decode.
        assert!(!block.raw_signed_header_b64.is_empty());
    }

    #[test]
    fn decodes_real_op_row() {
        // Op `O12fxbma…` is Status=Failed (2), Type=Transfer (3),
        // amount=1 nMAS, fee=1 nMAS, in block `B12akgwp…`, slot
        // (355440, 4).
        let row = OpRow {
            hash: "O12fxbmaVz5rophPjVFta7Ckhc6sMJJV9BSe1FNNRfPbr6Z4SSN1",
            raw: OP_REAL,
            status: 2, // Failed
            block_hash: "B12akgwpixwbR9LY6kc4BRMh73fmCPGrMLEbyFwhbBvEaGEL2nWC",
            slot: Slot::new(355_440, 4),
        };
        let decoded = decode_op_row(&row, 1_700_000_000_000).expect("decode");
        assert_eq!(decoded.stored.kind, OperationKind::Transaction);
        assert!(!decoded.executed, "Status=2 ⇒ not Executed");
        assert_eq!(
            decoded.stored.final_exec_status,
            Some(crate::model::ExecStatus::Failed)
        );
        assert_eq!(decoded.stored.fee_nmas, 1);
        assert_eq!(decoded.stored.details.amount_nmas, Some(1));
        assert_eq!(
            decoded.stored.details.recipient_address.as_deref(),
            Some("AU12jjVoTxzscvvTcwsdKErNaM1aLTUApcAeBN4cHXNEWQBCNpbEv"),
        );
        assert_eq!(decoded.stored.thread, 4);
        assert_eq!(decoded.stored.inclusions.len(), 1);
        assert_eq!(decoded.stored.inclusions[0].slot, Slot::new(355_440, 4));
        // The synthetic transfer should NOT be produced for a failed op
        // (zero economic effect to capture).
        let t = legacy_op_to_transfer(
            &decoded.stored,
            Slot::new(355_440, 4),
            "B12akgwpixwbR9LY6kc4BRMh73fmCPGrMLEbyFwhbBvEaGEL2nWC",
            0,
            1_700_000_000_000,
        );
        // The function returns Some for any Type=Transaction with non-zero
        // amount — failed status is a different concern handled by the
        // orchestrator (it must filter on `executed`).
        let t = t.expect("transfer constructed");
        assert_eq!(t.value, TransferValue::Coins { nmas: 1 });
        assert_eq!(t.origin, CoinOrigin::OpTransactionCoins);
    }

    #[test]
    fn decodes_real_endorsement_row() {
        let s = decode_endorsement_row(
            "E1YAiBsAdMnN9UkCxLZTkaDCV7mUzYEYEuxnHxnKR4FKz1KdA9P",
            ENDO_T0,
            "B127Z9Bqpvw9spCv1t8HJs9ZVhdx5zM5rEymcxXkqdCRM1C6ySQW",
            Slot::new(4_000_000, 0),
            500,
        )
        .expect("decode");
        assert_eq!(s.id, "E1YAiBsAdMnN9UkCxLZTkaDCV7mUzYEYEuxnHxnKR4FKz1KdA9P");
        assert_eq!(s.included_slot, Slot::new(4_000_000, 0));
        assert_eq!(
            s.included_block_id,
            "B127Z9Bqpvw9spCv1t8HJs9ZVhdx5zM5rEymcxXkqdCRM1C6ySQW"
        );
        assert!(!s.signature.is_empty());
    }

    #[test]
    fn legacy_op_to_transfer_skips_non_transaction() {
        let mut op = StoredOperation {
            id: OperationId::parse("O12fxbmaVz5rophPjVFta7Ckhc6sMJJV9BSe1FNNRfPbr6Z4SSN1")
                .unwrap(),
            creator: Address::parse(
                "AU1Fp7uBP2TXxDty2HdTE3ZE3XQ4cNXnG3xuo8TkQLJtyxC7FKhx".to_string(),
            )
            .unwrap(),
            target: None,
            kind: OperationKind::CallSc,
            expire_period: 1,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![],
            candidate_exec_status: None,
            final_exec_status: None,
            details: OperationDetails::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        // Non-transaction → no synthetic transfer.
        assert!(
            legacy_op_to_transfer(&op, Slot::new(1, 0), "B-X", 0, 1).is_none(),
            "non-transaction must not produce a transfer"
        );
        // Transaction with zero amount → also dropped.
        op.kind = OperationKind::Transaction;
        op.details = OperationDetails {
            amount_nmas: Some(0),
            recipient_address: Some("AU12...".into()),
            ..Default::default()
        };
        assert!(legacy_op_to_transfer(&op, Slot::new(1, 0), "B-X", 0, 1).is_none());
    }

    #[test]
    fn sub_transfer_recovers_parent_op_from_hash() {
        // Real-shape sub-transfer row: empty `OriginalOperationID`
        // and `SlotAndAscIndex` columns, parent op id encoded in
        // the hash prefix (`<op_id>_<n>`).
        let row = SubTransferRow {
            hash: "O1226JNGYis98LcRqvNh3LPx1xhSDVen484ZBsjP5weLTgN9ya2s_0",
            status: 1, // Executed
            original_op_id: Some(""),
            slot_and_asc_index: Some(""),
            creator_address: "AS12qzyNBDnwqq2vYwvUMHzrtMkVp6nQGJJ3TETVKF5HCd4yymzJP",
            other_address: Some("AU1WQayS4eFx5Cu7ktRV2vJ39NqCRi63UUF4FHfPXkzkxYT53q78"),
            amount_nmas: 162_689_323_534,
        };
        let t =
            legacy_sub_transfer_to_transfer(&row, Slot::new(4_542_727, 8), Some("B-test"), 0, 100)
                .expect("decoded");
        assert_eq!(
            t.id,
            "O1226JNGYis98LcRqvNh3LPx1xhSDVen484ZBsjP5weLTgN9ya2s_0"
        );
        assert_eq!(
            t.operation_id.as_deref(),
            Some("O1226JNGYis98LcRqvNh3LPx1xhSDVen484ZBsjP5weLTgN9ya2s"),
            "operation_id must be derived from the hash prefix even when the column is empty"
        );
        assert!(
            matches!(t.origin, CoinOrigin::AbiTransferCoins),
            "op-driven sub-transfer must carry the AbiTransferCoins bucket, got {:?}",
            t.origin
        );
        assert_eq!(t.value, TransferValue::Coins { nmas: 162_689_323_534 });
    }

    #[test]
    fn sub_transfer_zero_amount_is_dropped() {
        let row = SubTransferRow {
            hash: "O12abc_0",
            status: 1,
            original_op_id: Some(""),
            slot_and_asc_index: Some(""),
            creator_address: "AS12...",
            other_address: Some("AU1..."),
            amount_nmas: 0,
        };
        assert!(legacy_sub_transfer_to_transfer(&row, Slot::new(1, 0), None, 0, 1).is_none());
    }

    #[test]
    fn sub_transfer_failed_status_is_dropped() {
        let row = SubTransferRow {
            hash: "O12abc_0",
            status: 2, // Failed
            original_op_id: Some(""),
            slot_and_asc_index: Some(""),
            creator_address: "AS12...",
            other_address: Some("AU1..."),
            amount_nmas: 1000,
        };
        assert!(legacy_sub_transfer_to_transfer(&row, Slot::new(1, 0), None, 0, 1).is_none());
    }

    #[test]
    fn item_helpers() {
        use crate::legacy::ddb::AttributeValue;
        let mut row = Item::new();
        row.insert("S".into(), AttributeValue::s_str("abc"));
        row.insert("N".into(), AttributeValue::n_num(42));
        let mut bv = AttributeValue::default();
        bv.b = Some(B64.encode([1u8, 2, 3]));
        row.insert("B".into(), bv);

        assert_eq!(item_str(&row, "S").unwrap(), "abc");
        assert!(matches!(
            item_str(&row, "missing"),
            Err(DecodeError::MissingField("missing"))
        ));
        assert_eq!(item_num_i64(&row, "N").unwrap(), 42);
        assert!(item_num_u32(&row, "N").is_ok());
        assert_eq!(item_bytes(&row, "B").unwrap(), vec![1, 2, 3]);
    }
}
