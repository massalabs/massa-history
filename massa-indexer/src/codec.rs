//! Binary value codec.
//!
//! Every RocksDB value and every peer-protocol payload is a protobuf
//! message defined in `proto/indexer/v1/storage.proto`. This module
//! converts between those wire types and the domain `Stored*` Rust
//! structs in `crate::model`.
//!
//! ## Why a conversion layer?
//!
//! We could have replaced the Rust types with the prost-generated ones
//! directly and avoided the boilerplate here, but that would have
//! leaked proto-isms (snake_case field names, `has_*` booleans,
//! untyped `bytes` for ids) into the REST surface and domain logic.
//! Keeping two parallel type worlds and translating at the boundary
//! keeps each surface clean.
//!
//! The REST layer still serialises the domain types with serde_json,
//! so when we read a row we decode proto → domain → serde_json with
//! two format hops. This is fine: RocksDB reads dwarf CPU cost of
//! JSON formatting, and the conversion code here is amortised on the
//! writer side.
//!
//! ## Unknown-variant policy
//!
//! All enum-with-fields conversions fall back to the `Unknown` variant
//! of the corresponding Rust enum when the proto oneof is empty (or
//! when decoded from a peer speaking a newer version that added a
//! variant). This matches how the rest of the ingest path handles
//! forward-compat.

use prost::Message;

use crate::ids::{Address, BlockId, EndorsementId, OperationId};
use crate::model::{
    AsyncMsgState, AsyncTrigger, BlockStatus, CoinOrigin, DatastoreEntry, DeferredCallState,
    ExecStatus, MetaRow, OperationDetails, OperationInclusion, OperationKind, Slot,
    SlotCompleteness, SlotState, SlotStatus, StoredAsyncMsg, StoredBlock, StoredDeferredCall,
    StoredDenunciation, StoredDenunciationEntry, StoredEndorsement, StoredOperation,
    StoredScEvent, StoredTransfer, TransferValue,
};
use crate::proto::indexer::v1 as pb;

// ---------------------------------------------------------------------------
// Error / generic helpers
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("protobuf encode: {0}")]
    Encode(#[from] prost::EncodeError),
    #[error("{0}")]
    Custom(String),
}

impl From<CodecError> for crate::error::Error {
    fn from(e: CodecError) -> Self {
        crate::error::Error::other(format!("codec: {e}"))
    }
}

fn err(msg: impl Into<String>) -> CodecError {
    CodecError::Custom(msg.into())
}

// ---------------------------------------------------------------------------
// Id helpers
// ---------------------------------------------------------------------------

fn block_id_to_key(id: &BlockId) -> Vec<u8> {
    id.as_bytes().to_vec()
}
fn block_id_from_key(bytes: &[u8]) -> Result<BlockId, CodecError> {
    BlockId::from_key_bytes(bytes).ok_or_else(|| err("invalid block id key bytes"))
}
fn op_id_to_key(id: &OperationId) -> Vec<u8> {
    id.as_bytes().to_vec()
}
fn op_id_from_key(bytes: &[u8]) -> Result<OperationId, CodecError> {
    OperationId::from_key_bytes(bytes).ok_or_else(|| err("invalid operation id key bytes"))
}
fn endo_id_to_key(id: &EndorsementId) -> Vec<u8> {
    id.as_bytes().to_vec()
}
fn endo_id_from_key(bytes: &[u8]) -> Result<EndorsementId, CodecError> {
    EndorsementId::from_key_bytes(bytes).ok_or_else(|| err("invalid endorsement id key bytes"))
}
fn addr_to_key(a: &Address) -> Vec<u8> {
    a.as_bytes().to_vec()
}
fn addr_from_key(bytes: &[u8]) -> Result<Address, CodecError> {
    Address::from_key_bytes(bytes).ok_or_else(|| err("invalid address key bytes"))
}

// ---------------------------------------------------------------------------
// Slot
// ---------------------------------------------------------------------------

fn slot_to_pb(s: Slot) -> pb::SlotPb {
    pb::SlotPb {
        period: s.period,
        thread: s.thread as u32,
    }
}

fn slot_from_pb(p: &pb::SlotPb) -> Result<Slot, CodecError> {
    Ok(Slot {
        period: p.period,
        thread: u8::try_from(p.thread).map_err(|_| err("thread > 255"))?,
    })
}

fn slot_from_opt(p: Option<&pb::SlotPb>) -> Result<Slot, CodecError> {
    slot_from_pb(p.ok_or_else(|| err("missing slot"))?)
}

// ---------------------------------------------------------------------------
// Slot status / exec status / block status enums
// ---------------------------------------------------------------------------

fn slot_status_to_pb(s: SlotStatus) -> i32 {
    match s {
        SlotStatus::Unknown => pb::SlotStatusPb::SlotStatusUnknown as i32,
        SlotStatus::Candidate => pb::SlotStatusPb::SlotStatusCandidate as i32,
        SlotStatus::Final => pb::SlotStatusPb::SlotStatusFinal as i32,
    }
}

fn slot_status_from_pb(i: i32) -> SlotStatus {
    match pb::SlotStatusPb::try_from(i).unwrap_or(pb::SlotStatusPb::SlotStatusUnknown) {
        pb::SlotStatusPb::SlotStatusCandidate => SlotStatus::Candidate,
        pb::SlotStatusPb::SlotStatusFinal => SlotStatus::Final,
        _ => SlotStatus::Unknown,
    }
}

fn exec_status_to_pb(s: ExecStatus) -> i32 {
    match s {
        ExecStatus::Ok => pb::ExecStatusPb::ExecStatusOk as i32,
        ExecStatus::Failed => pb::ExecStatusPb::ExecStatusFailed as i32,
    }
}

fn exec_status_from_pb(i: i32) -> Option<ExecStatus> {
    match pb::ExecStatusPb::try_from(i).ok()? {
        pb::ExecStatusPb::ExecStatusOk => Some(ExecStatus::Ok),
        pb::ExecStatusPb::ExecStatusFailed => Some(ExecStatus::Failed),
        pb::ExecStatusPb::ExecStatusUnspecified => None,
    }
}

fn block_status_to_pb(s: BlockStatus) -> i32 {
    match s {
        BlockStatus::SeenCandidate => pb::BlockStatusPb::BlockStatusSeenCandidate as i32,
        BlockStatus::Final => pb::BlockStatusPb::BlockStatusFinal as i32,
        BlockStatus::Discarded => pb::BlockStatusPb::BlockStatusDiscarded as i32,
    }
}

fn block_status_from_pb(i: i32) -> BlockStatus {
    match pb::BlockStatusPb::try_from(i).unwrap_or(pb::BlockStatusPb::BlockStatusUnspecified) {
        pb::BlockStatusPb::BlockStatusFinal => BlockStatus::Final,
        pb::BlockStatusPb::BlockStatusDiscarded => BlockStatus::Discarded,
        _ => BlockStatus::SeenCandidate,
    }
}

// ---------------------------------------------------------------------------
// SlotCompleteness
// ---------------------------------------------------------------------------

fn completeness_to_pb(c: SlotCompleteness) -> pb::SlotCompletenessPb {
    pb::SlotCompletenessPb {
        block_body_stored: c.block_body_stored,
        exec_output_final: c.exec_output_final,
        exec_output_candidate: c.exec_output_candidate,
        transfers_stored: c.transfers_stored,
    }
}

fn completeness_from_pb(p: Option<pb::SlotCompletenessPb>) -> SlotCompleteness {
    let p = p.unwrap_or_default();
    SlotCompleteness {
        block_body_stored: p.block_body_stored,
        exec_output_final: p.exec_output_final,
        exec_output_candidate: p.exec_output_candidate,
        transfers_stored: p.transfers_stored,
    }
}

// ---------------------------------------------------------------------------
// SlotState
// ---------------------------------------------------------------------------

pub fn encode_slot_state(s: &SlotState) -> Vec<u8> {
    let pb = pb::SlotStatePb {
        slot: Some(slot_to_pb(s.slot)),
        status: slot_status_to_pb(s.status),
        is_miss: s.is_miss,
        final_block_id_key: s
            .final_block_id
            .as_ref()
            .map(block_id_to_key)
            .unwrap_or_default(),
        candidate_block_id_keys: s
            .candidate_block_ids
            .iter()
            .map(block_id_to_key)
            .collect(),
        execution_trail_hash: s.execution_trail_hash.clone().unwrap_or_default(),
        executed_op_id_keys: s.executed_op_ids.iter().map(op_id_to_key).collect(),
        sc_event_count: s.sc_event_count,
        completeness: Some(completeness_to_pb(s.completeness)),
        first_seen_ts_ms: s.first_seen_ts_ms,
        last_updated_ts_ms: s.last_updated_ts_ms,
    };
    pb.encode_to_vec()
}

pub fn decode_slot_state(bytes: &[u8]) -> Result<SlotState, CodecError> {
    let p = pb::SlotStatePb::decode(bytes)?;
    Ok(SlotState {
        slot: slot_from_opt(p.slot.as_ref())?,
        status: slot_status_from_pb(p.status),
        is_miss: p.is_miss,
        final_block_id: if p.final_block_id_key.is_empty() {
            None
        } else {
            Some(block_id_from_key(&p.final_block_id_key)?)
        },
        candidate_block_ids: p
            .candidate_block_id_keys
            .iter()
            .map(|b| block_id_from_key(b))
            .collect::<Result<_, _>>()?,
        execution_trail_hash: if p.execution_trail_hash.is_empty() {
            None
        } else {
            Some(p.execution_trail_hash)
        },
        executed_op_ids: p
            .executed_op_id_keys
            .iter()
            .map(|b| op_id_from_key(b))
            .collect::<Result<_, _>>()?,
        sc_event_count: p.sc_event_count,
        completeness: completeness_from_pb(p.completeness),
        first_seen_ts_ms: p.first_seen_ts_ms,
        last_updated_ts_ms: p.last_updated_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// MetaRow
// ---------------------------------------------------------------------------

pub fn encode_meta_row(m: &MetaRow) -> Vec<u8> {
    let pb = pb::MetaRowPb {
        network: m.network.clone(),
        genesis_timestamp_ms: m.genesis_timestamp_ms,
        t0_ms: m.t0_ms,
        thread_count: m.thread_count as u32,
        last_final_slot: m.last_final_slot.map(slot_to_pb),
        has_last_final_slot: m.last_final_slot.is_some(),
        last_candidate_slot: m.last_candidate_slot.map(slot_to_pb),
        has_last_candidate_slot: m.last_candidate_slot.is_some(),
        build_version: m.build_version.clone(),
        created_at_ms: m.created_at_ms,
        updated_at_ms: m.updated_at_ms,
    };
    pb.encode_to_vec()
}

pub fn decode_meta_row(bytes: &[u8]) -> Result<MetaRow, CodecError> {
    let p = pb::MetaRowPb::decode(bytes)?;
    Ok(MetaRow {
        network: p.network,
        genesis_timestamp_ms: p.genesis_timestamp_ms,
        t0_ms: p.t0_ms,
        thread_count: u8::try_from(p.thread_count).map_err(|_| err("thread_count > 255"))?,
        last_final_slot: if p.has_last_final_slot {
            Some(slot_from_opt(p.last_final_slot.as_ref())?)
        } else {
            None
        },
        last_candidate_slot: if p.has_last_candidate_slot {
            Some(slot_from_opt(p.last_candidate_slot.as_ref())?)
        } else {
            None
        },
        build_version: p.build_version,
        created_at_ms: p.created_at_ms,
        updated_at_ms: p.updated_at_ms,
    })
}

// Bare Slot roundtrip — used for last_final_slot / last_candidate_slot
// which are stored as standalone rows under `cf_meta`.
pub fn encode_slot(s: &Slot) -> Vec<u8> {
    slot_to_pb(*s).encode_to_vec()
}
pub fn decode_slot(bytes: &[u8]) -> Result<Slot, CodecError> {
    slot_from_pb(&pb::SlotPb::decode(bytes)?)
}

// ---------------------------------------------------------------------------
// StoredEndorsement
// ---------------------------------------------------------------------------

fn endorsement_to_pb(e: &StoredEndorsement) -> Result<pb::StoredEndorsementPb, CodecError> {
    let id = EndorsementId::parse(&e.id).map_err(err)?;
    let endorsed_block_id = BlockId::parse(&e.endorsed_block_id).map_err(err)?;
    let included_block_id = BlockId::parse(&e.included_block_id).map_err(err)?;
    Ok(pb::StoredEndorsementPb {
        id_key: endo_id_to_key(&id),
        slot: Some(slot_to_pb(e.slot)),
        index: e.index,
        endorsed_block_id_key: block_id_to_key(&endorsed_block_id),
        content_creator_pub_key: e.content_creator_pub_key.clone(),
        content_creator_address_key: addr_to_key(&e.content_creator_address),
        signature: e.signature.clone(),
        serialized_size: e.serialized_size,
        included_block_id_key: block_id_to_key(&included_block_id),
        included_slot: Some(slot_to_pb(e.included_slot)),
        first_seen_ts_ms: e.first_seen_ts_ms,
    })
}

fn endorsement_from_pb(p: pb::StoredEndorsementPb) -> Result<StoredEndorsement, CodecError> {
    Ok(StoredEndorsement {
        id: endo_id_from_key(&p.id_key)?.to_string(),
        slot: slot_from_opt(p.slot.as_ref())?,
        index: p.index,
        endorsed_block_id: block_id_from_key(&p.endorsed_block_id_key)?.to_string(),
        content_creator_pub_key: p.content_creator_pub_key,
        content_creator_address: addr_from_key(&p.content_creator_address_key)?,
        signature: p.signature,
        serialized_size: p.serialized_size,
        included_block_id: block_id_from_key(&p.included_block_id_key)?.to_string(),
        included_slot: slot_from_opt(p.included_slot.as_ref())?,
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

pub fn encode_endorsement(e: &StoredEndorsement) -> Result<Vec<u8>, CodecError> {
    Ok(endorsement_to_pb(e)?.encode_to_vec())
}
pub fn decode_endorsement(bytes: &[u8]) -> Result<StoredEndorsement, CodecError> {
    endorsement_from_pb(pb::StoredEndorsementPb::decode(bytes)?)
}

// ---------------------------------------------------------------------------
// StoredDenunciation + StoredDenunciationEntry
// ---------------------------------------------------------------------------

fn denunciation_to_pb(d: &StoredDenunciation) -> pb::StoredDenunciationPb {
    use pb::stored_denunciation_pb::Kind;
    let kind = match d {
        StoredDenunciation::BlockHeader {
            public_key,
            slot,
            hash_1,
            hash_2,
            signature_1,
            signature_2,
        } => Kind::BlockHeader(pb::DenunciationBlockHeaderPb {
            public_key: public_key.clone(),
            slot: Some(slot_to_pb(*slot)),
            hash_1: hash_1.clone(),
            hash_2: hash_2.clone(),
            signature_1: signature_1.clone(),
            signature_2: signature_2.clone(),
        }),
        StoredDenunciation::Endorsement {
            public_key,
            slot,
            index,
            hash_1,
            hash_2,
            signature_1,
            signature_2,
        } => Kind::Endorsement(pb::DenunciationEndorsementPb {
            public_key: public_key.clone(),
            slot: Some(slot_to_pb(*slot)),
            index: *index,
            hash_1: hash_1.clone(),
            hash_2: hash_2.clone(),
            signature_1: signature_1.clone(),
            signature_2: signature_2.clone(),
        }),
        StoredDenunciation::Address {
            address_denounced,
            slot,
            slashed_nmas,
        } => Kind::Address(pb::DenunciationAddressPb {
            address_denounced: address_denounced.clone(),
            slot: Some(slot_to_pb(*slot)),
            slashed_nmas: *slashed_nmas,
        }),
        StoredDenunciation::Unknown => Kind::Unknown(pb::DenunciationUnknownPb {}),
    };
    pb::StoredDenunciationPb { kind: Some(kind) }
}

fn denunciation_from_pb(p: pb::StoredDenunciationPb) -> Result<StoredDenunciation, CodecError> {
    use pb::stored_denunciation_pb::Kind;
    Ok(match p.kind {
        Some(Kind::BlockHeader(x)) => StoredDenunciation::BlockHeader {
            public_key: x.public_key,
            slot: slot_from_opt(x.slot.as_ref())?,
            hash_1: x.hash_1,
            hash_2: x.hash_2,
            signature_1: x.signature_1,
            signature_2: x.signature_2,
        },
        Some(Kind::Endorsement(x)) => StoredDenunciation::Endorsement {
            public_key: x.public_key,
            slot: slot_from_opt(x.slot.as_ref())?,
            index: x.index,
            hash_1: x.hash_1,
            hash_2: x.hash_2,
            signature_1: x.signature_1,
            signature_2: x.signature_2,
        },
        Some(Kind::Address(x)) => StoredDenunciation::Address {
            address_denounced: x.address_denounced,
            slot: slot_from_opt(x.slot.as_ref())?,
            slashed_nmas: x.slashed_nmas,
        },
        Some(Kind::Unknown(_)) | None => StoredDenunciation::Unknown,
    })
}

pub fn encode_denunciation_entry(d: &StoredDenunciationEntry) -> Result<Vec<u8>, CodecError> {
    let pb = pb::StoredDenunciationEntryPb {
        hash: hex::decode(&d.hash).map_err(|e| err(format!("hex hash: {e}")))?,
        slot: Some(slot_to_pb(d.slot)),
        kind: d.kind.clone(),
        denounced_addr_key: d.denounced_addr.as_ref().map(addr_to_key).unwrap_or_default(),
        has_denounced_addr: d.denounced_addr.is_some(),
        denunciation: Some(denunciation_to_pb(&d.denunciation)),
        included_block_id_key: d
            .included_block_id
            .as_ref()
            .map(block_id_to_key)
            .unwrap_or_default(),
        has_included_block_id: d.included_block_id.is_some(),
        included_slot: d.included_slot.map(slot_to_pb),
        has_included_slot: d.included_slot.is_some(),
        first_seen_ts_ms: d.first_seen_ts_ms,
    };
    Ok(pb.encode_to_vec())
}

pub fn decode_denunciation_entry(bytes: &[u8]) -> Result<StoredDenunciationEntry, CodecError> {
    let p = pb::StoredDenunciationEntryPb::decode(bytes)?;
    Ok(StoredDenunciationEntry {
        hash: hex::encode(&p.hash),
        slot: slot_from_opt(p.slot.as_ref())?,
        kind: p.kind,
        denounced_addr: if p.has_denounced_addr {
            Some(addr_from_key(&p.denounced_addr_key)?)
        } else {
            None
        },
        denunciation: denunciation_from_pb(
            p.denunciation.ok_or_else(|| err("missing denunciation payload"))?,
        )?,
        included_block_id: if p.has_included_block_id {
            Some(block_id_from_key(&p.included_block_id_key)?)
        } else {
            None
        },
        included_slot: if p.has_included_slot {
            Some(slot_from_opt(p.included_slot.as_ref())?)
        } else {
            None
        },
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// StoredBlock
// ---------------------------------------------------------------------------

pub fn encode_block(b: &StoredBlock) -> Result<Vec<u8>, CodecError> {
    let endorsements = b
        .endorsements
        .iter()
        .map(endorsement_to_pb)
        .collect::<Result<Vec<_>, _>>()?;
    let raw_signed_header = if b.raw_signed_header_b64.is_empty() {
        Vec::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.decode(&b.raw_signed_header_b64)
            .map_err(|e| err(format!("raw_signed_header b64: {e}")))?
    };
    let pb = pb::StoredBlockPb {
        id_key: block_id_to_key(&b.id),
        slot: Some(slot_to_pb(b.slot)),
        creator_key: addr_to_key(&b.creator),
        parent_keys: b.parents.iter().map(block_id_to_key).collect(),
        operation_id_keys: b.operation_ids.iter().map(op_id_to_key).collect(),
        endorsements,
        endorsement_ids: b.endorsement_ids.clone(),
        denunciations: b.denunciations.iter().map(denunciation_to_pb).collect(),
        current_version: b.current_version,
        announced_version: b.announced_version.unwrap_or(0),
        has_announced_version: b.announced_version.is_some(),
        operations_hash: b.operations_hash.clone(),
        signature: b.signature.clone(),
        content_creator_pub_key: b.content_creator_pub_key.clone(),
        serialized_size: b.serialized_size,
        raw_signed_header,
        status: block_status_to_pb(b.status),
        first_seen_ts_ms: b.first_seen_ts_ms,
    };
    Ok(pb.encode_to_vec())
}

pub fn decode_block(bytes: &[u8]) -> Result<StoredBlock, CodecError> {
    let p = pb::StoredBlockPb::decode(bytes)?;
    let raw_signed_header_b64 = if p.raw_signed_header.is_empty() {
        String::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.encode(&p.raw_signed_header)
    };
    Ok(StoredBlock {
        id: block_id_from_key(&p.id_key)?,
        slot: slot_from_opt(p.slot.as_ref())?,
        creator: addr_from_key(&p.creator_key)?,
        parents: p
            .parent_keys
            .iter()
            .map(|b| block_id_from_key(b))
            .collect::<Result<_, _>>()?,
        operation_ids: p
            .operation_id_keys
            .iter()
            .map(|b| op_id_from_key(b))
            .collect::<Result<_, _>>()?,
        endorsements: p
            .endorsements
            .into_iter()
            .map(endorsement_from_pb)
            .collect::<Result<_, _>>()?,
        endorsement_ids: p.endorsement_ids,
        denunciations: p
            .denunciations
            .into_iter()
            .map(denunciation_from_pb)
            .collect::<Result<_, _>>()?,
        current_version: p.current_version,
        announced_version: if p.has_announced_version {
            Some(p.announced_version)
        } else {
            None
        },
        operations_hash: p.operations_hash,
        signature: p.signature,
        content_creator_pub_key: p.content_creator_pub_key,
        serialized_size: p.serialized_size,
        raw_signed_header_b64,
        status: block_status_from_pb(p.status),
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// StoredOperation
// ---------------------------------------------------------------------------

fn op_kind_to_pb(k: &OperationKind) -> i32 {
    let v = match k {
        OperationKind::Transaction => pb::OperationKindPb::OpKindTransaction,
        OperationKind::RollBuy => pb::OperationKindPb::OpKindRollBuy,
        OperationKind::RollSell => pb::OperationKindPb::OpKindRollSell,
        OperationKind::ExecuteSc => pb::OperationKindPb::OpKindExecuteSc,
        OperationKind::CallSc => pb::OperationKindPb::OpKindCallSc,
        OperationKind::Unknown => pb::OperationKindPb::OpKindUnknown,
    };
    v as i32
}

fn op_kind_from_pb(i: i32) -> OperationKind {
    match pb::OperationKindPb::try_from(i).unwrap_or(pb::OperationKindPb::OpKindUnknown) {
        pb::OperationKindPb::OpKindTransaction => OperationKind::Transaction,
        pb::OperationKindPb::OpKindRollBuy => OperationKind::RollBuy,
        pb::OperationKindPb::OpKindRollSell => OperationKind::RollSell,
        pb::OperationKindPb::OpKindExecuteSc => OperationKind::ExecuteSc,
        pb::OperationKindPb::OpKindCallSc => OperationKind::CallSc,
        _ => OperationKind::Unknown,
    }
}

fn details_to_pb(d: &OperationDetails) -> pb::OperationDetailsPb {
    pb::OperationDetailsPb {
        amount_nmas: d.amount_nmas.unwrap_or(0),
        has_amount_nmas: d.amount_nmas.is_some(),
        recipient_address: d.recipient_address.clone().unwrap_or_default(),
        has_recipient_address: d.recipient_address.is_some(),
        roll_count: d.roll_count.unwrap_or(0),
        has_roll_count: d.roll_count.is_some(),
        target_address: d.target_address.clone().unwrap_or_default(),
        has_target_address: d.target_address.is_some(),
        target_function: d.target_function.clone().unwrap_or_default(),
        has_target_function: d.target_function.is_some(),
        parameter_hex: d.parameter_hex.clone().unwrap_or_default(),
        has_parameter_hex: d.parameter_hex.is_some(),
        parameter_len: d.parameter_len.unwrap_or(0),
        has_parameter_len: d.parameter_len.is_some(),
        coins_nmas: d.coins_nmas.unwrap_or(0),
        has_coins_nmas: d.coins_nmas.is_some(),
        max_gas: d.max_gas.unwrap_or(0),
        has_max_gas: d.max_gas.is_some(),
        bytecode_hex: d.bytecode_hex.clone().unwrap_or_default(),
        has_bytecode_hex: d.bytecode_hex.is_some(),
        bytecode_size: d.bytecode_size.unwrap_or(0),
        has_bytecode_size: d.bytecode_size.is_some(),
        max_coins_nmas: d.max_coins_nmas.unwrap_or(0),
        has_max_coins_nmas: d.max_coins_nmas.is_some(),
        datastore: d
            .datastore
            .iter()
            .map(|de| pb::DatastoreEntryPb {
                key_hex: de.key_hex.clone(),
                value_hex: de.value_hex.clone(),
            })
            .collect(),
        datastore_keys: d.datastore_keys.unwrap_or(0),
        has_datastore_keys: d.datastore_keys.is_some(),
    }
}

fn details_from_pb(p: pb::OperationDetailsPb) -> OperationDetails {
    OperationDetails {
        amount_nmas: p.has_amount_nmas.then_some(p.amount_nmas),
        recipient_address: p.has_recipient_address.then_some(p.recipient_address),
        roll_count: p.has_roll_count.then_some(p.roll_count),
        target_address: p.has_target_address.then_some(p.target_address),
        target_function: p.has_target_function.then_some(p.target_function),
        parameter_hex: p.has_parameter_hex.then_some(p.parameter_hex),
        parameter_len: p.has_parameter_len.then_some(p.parameter_len),
        coins_nmas: p.has_coins_nmas.then_some(p.coins_nmas),
        max_gas: p.has_max_gas.then_some(p.max_gas),
        bytecode_hex: p.has_bytecode_hex.then_some(p.bytecode_hex),
        bytecode_size: p.has_bytecode_size.then_some(p.bytecode_size),
        max_coins_nmas: p.has_max_coins_nmas.then_some(p.max_coins_nmas),
        datastore: p
            .datastore
            .into_iter()
            .map(|e| DatastoreEntry {
                key_hex: e.key_hex,
                value_hex: e.value_hex,
            })
            .collect(),
        datastore_keys: p.has_datastore_keys.then_some(p.datastore_keys),
    }
}

pub fn encode_operation(o: &StoredOperation) -> Result<Vec<u8>, CodecError> {
    let raw_signed_op = if o.raw_signed_op_b64.is_empty() {
        Vec::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.decode(&o.raw_signed_op_b64)
            .map_err(|e| err(format!("raw_signed_op b64: {e}")))?
    };
    let pb = pb::StoredOperationPb {
        id_key: op_id_to_key(&o.id),
        creator_key: addr_to_key(&o.creator),
        target_key: o.target.as_ref().map(addr_to_key).unwrap_or_default(),
        has_target: o.target.is_some(),
        kind: op_kind_to_pb(&o.kind),
        expire_period: o.expire_period,
        fee_nmas: o.fee_nmas,
        thread: o.thread as u32,
        inclusions: o
            .inclusions
            .iter()
            .map(|i| pb::OperationInclusionPb {
                slot: Some(slot_to_pb(i.slot)),
                block_id_key: block_id_to_key(&i.block_id),
            })
            .collect(),
        candidate_exec_status: o.candidate_exec_status.map(exec_status_to_pb).unwrap_or(0),
        has_candidate_exec_status: o.candidate_exec_status.is_some(),
        final_exec_status: o.final_exec_status.map(exec_status_to_pb).unwrap_or(0),
        has_final_exec_status: o.final_exec_status.is_some(),
        details: Some(details_to_pb(&o.details)),
        signature: o.signature.clone(),
        content_creator_pub_key: o.content_creator_pub_key.clone(),
        serialized_size: o.serialized_size,
        raw_signed_op,
        first_seen_ts_ms: o.first_seen_ts_ms,
    };
    Ok(pb.encode_to_vec())
}

pub fn decode_operation(bytes: &[u8]) -> Result<StoredOperation, CodecError> {
    let p = pb::StoredOperationPb::decode(bytes)?;
    let raw_signed_op_b64 = if p.raw_signed_op.is_empty() {
        String::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.encode(&p.raw_signed_op)
    };
    Ok(StoredOperation {
        id: op_id_from_key(&p.id_key)?,
        creator: addr_from_key(&p.creator_key)?,
        target: if p.has_target {
            Some(addr_from_key(&p.target_key)?)
        } else {
            None
        },
        kind: op_kind_from_pb(p.kind),
        expire_period: p.expire_period,
        fee_nmas: p.fee_nmas,
        thread: u8::try_from(p.thread).map_err(|_| err("thread > 255"))?,
        inclusions: p
            .inclusions
            .into_iter()
            .map(|i| -> Result<_, CodecError> {
                Ok(OperationInclusion {
                    slot: slot_from_opt(i.slot.as_ref())?,
                    block_id: block_id_from_key(&i.block_id_key)?,
                })
            })
            .collect::<Result<_, _>>()?,
        candidate_exec_status: if p.has_candidate_exec_status {
            exec_status_from_pb(p.candidate_exec_status)
        } else {
            None
        },
        final_exec_status: if p.has_final_exec_status {
            exec_status_from_pb(p.final_exec_status)
        } else {
            None
        },
        details: details_from_pb(p.details.unwrap_or_default()),
        signature: p.signature,
        content_creator_pub_key: p.content_creator_pub_key,
        serialized_size: p.serialized_size,
        raw_signed_op_b64,
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// StoredScEvent
// ---------------------------------------------------------------------------

pub fn encode_sc_event(e: &StoredScEvent) -> Result<Vec<u8>, CodecError> {
    let pb = pb::StoredScEventPb {
        slot: Some(slot_to_pb(e.slot)),
        index_in_slot: e.index_in_slot,
        data: e.data.clone(),
        emitter_addr_keys: e.emitter_addrs.iter().map(addr_to_key).collect(),
        caller_addr_keys: e.caller_addrs.iter().map(addr_to_key).collect(),
        status: slot_status_to_pb(e.status),
        op_id_key: e.op_id.as_ref().map(op_id_to_key).unwrap_or_default(),
        has_op_id: e.op_id.is_some(),
    };
    Ok(pb.encode_to_vec())
}

pub fn decode_sc_event(bytes: &[u8]) -> Result<StoredScEvent, CodecError> {
    let p = pb::StoredScEventPb::decode(bytes)?;
    Ok(StoredScEvent {
        slot: slot_from_opt(p.slot.as_ref())?,
        index_in_slot: p.index_in_slot,
        data: p.data,
        emitter_addrs: p
            .emitter_addr_keys
            .iter()
            .map(|b| addr_from_key(b))
            .collect::<Result<_, _>>()?,
        caller_addrs: p
            .caller_addr_keys
            .iter()
            .map(|b| addr_from_key(b))
            .collect::<Result<_, _>>()?,
        status: slot_status_from_pb(p.status),
        op_id: if p.has_op_id {
            Some(op_id_from_key(&p.op_id_key)?)
        } else {
            None
        },
    })
}

// ---------------------------------------------------------------------------
// StoredTransfer
// ---------------------------------------------------------------------------

fn transfer_value_to_pb(v: &TransferValue) -> pb::TransferValuePb {
    use pb::transfer_value_pb::Value;
    let value = match v {
        TransferValue::Coins { nmas } => Value::Coins(pb::TransferCoinsPb { nmas: *nmas }),
        TransferValue::Rolls { count } => Value::Rolls(pb::TransferRollsPb { count: *count }),
        TransferValue::DeferredCredits { nmas } => {
            Value::DeferredCredits(pb::TransferDeferredCreditsPb { nmas: *nmas })
        }
        TransferValue::Unknown => Value::Unknown(pb::TransferUnknownPb {}),
    };
    pb::TransferValuePb { value: Some(value) }
}

fn transfer_value_from_pb(p: pb::TransferValuePb) -> TransferValue {
    use pb::transfer_value_pb::Value;
    match p.value {
        Some(Value::Coins(c)) => TransferValue::Coins { nmas: c.nmas },
        Some(Value::Rolls(r)) => TransferValue::Rolls { count: r.count },
        Some(Value::DeferredCredits(d)) => TransferValue::DeferredCredits { nmas: d.nmas },
        Some(Value::Unknown(_)) | None => TransferValue::Unknown,
    }
}

fn coin_origin_to_pb(o: &CoinOrigin) -> pb::CoinOriginPb {
    use pb::CoinOriginKindPb as K;
    let kind = match o {
        CoinOrigin::Unspecified => K::CoinOriginUnspecified,
        CoinOrigin::BlockReward => K::CoinOriginBlockReward,
        CoinOrigin::DeferredCallFail => K::CoinOriginDeferredCallFail,
        CoinOrigin::DeferredCallCancel => K::CoinOriginDeferredCallCancel,
        CoinOrigin::DeferredCallCoins => K::CoinOriginDeferredCallCoins,
        CoinOrigin::DeferredCallRegister => K::CoinOriginDeferredCallRegister,
        CoinOrigin::DeferredCallStorageRefund => K::CoinOriginDeferredCallStorageRefund,
        CoinOrigin::EndorsementReward => K::CoinOriginEndorsementReward,
        CoinOrigin::EndorsedReward => K::CoinOriginEndorsedReward,
        CoinOrigin::Slash => K::CoinOriginSlash,
        CoinOrigin::OpRollBuy => K::CoinOriginOpRollBuy,
        CoinOrigin::OpRollSell => K::CoinOriginOpRollSell,
        CoinOrigin::OpCallscCoins => K::CoinOriginOpCallscCoins,
        CoinOrigin::ReadOnlyFnCallFees => K::CoinOriginReadOnlyFnCallFees,
        CoinOrigin::ReadOnlyFnCallCoins => K::CoinOriginReadOnlyFnCallCoins,
        CoinOrigin::ReadOnlyBytecodeExecFees => K::CoinOriginReadOnlyBytecodeExecFees,
        CoinOrigin::SetBytecodeStorage => K::CoinOriginSetBytecodeStorage,
        CoinOrigin::AbiCallCoins => K::CoinOriginAbiCallCoins,
        CoinOrigin::AbiTransferCoins => K::CoinOriginAbiTransferCoins,
        CoinOrigin::AbiTransferForCoins => K::CoinOriginAbiTransferForCoins,
        CoinOrigin::AbiSendMsgCoins => K::CoinOriginAbiSendMsgCoins,
        CoinOrigin::AbiSendMsgFees => K::CoinOriginAbiSendMsgFees,
        CoinOrigin::OpRollSellDeferredMas => K::CoinOriginOpRollSellDeferredMas,
        CoinOrigin::OpExecutescFees => K::CoinOriginOpExecutescFees,
        CoinOrigin::OpTransactionCoins => K::CoinOriginOpTransactionCoins,
        CoinOrigin::OpTransactionFees => K::CoinOriginOpTransactionFees,
        CoinOrigin::AsyncMsgCoins => K::CoinOriginAsyncMsgCoins,
        CoinOrigin::AsyncMsgCancel => K::CoinOriginAsyncMsgCancel,
        CoinOrigin::CreateScStorage => K::CoinOriginCreateScStorage,
        CoinOrigin::DatastoreStorage => K::CoinOriginDatastoreStorage,
        CoinOrigin::DeferredCredit => K::CoinOriginDeferredCredit,
        // Forward-compat: emitted as UNSPECIFIED + other_code.
        CoinOrigin::Other { .. } => K::CoinOriginUnspecified,
    };
    let other_code = if let CoinOrigin::Other { code } = o {
        *code
    } else {
        0
    };
    pb::CoinOriginPb {
        kind: kind as i32,
        other_code,
    }
}

fn coin_origin_from_pb(p: pb::CoinOriginPb) -> CoinOrigin {
    use pb::CoinOriginKindPb as K;
    let k = K::try_from(p.kind).unwrap_or(K::CoinOriginUnspecified);
    if matches!(k, K::CoinOriginUnspecified) && p.other_code != 0 {
        return CoinOrigin::Other { code: p.other_code };
    }
    match k {
        K::CoinOriginUnspecified => CoinOrigin::Unspecified,
        K::CoinOriginBlockReward => CoinOrigin::BlockReward,
        K::CoinOriginDeferredCallFail => CoinOrigin::DeferredCallFail,
        K::CoinOriginDeferredCallCancel => CoinOrigin::DeferredCallCancel,
        K::CoinOriginDeferredCallCoins => CoinOrigin::DeferredCallCoins,
        K::CoinOriginDeferredCallRegister => CoinOrigin::DeferredCallRegister,
        K::CoinOriginDeferredCallStorageRefund => CoinOrigin::DeferredCallStorageRefund,
        K::CoinOriginEndorsementReward => CoinOrigin::EndorsementReward,
        K::CoinOriginEndorsedReward => CoinOrigin::EndorsedReward,
        K::CoinOriginSlash => CoinOrigin::Slash,
        K::CoinOriginOpRollBuy => CoinOrigin::OpRollBuy,
        K::CoinOriginOpRollSell => CoinOrigin::OpRollSell,
        K::CoinOriginOpCallscCoins => CoinOrigin::OpCallscCoins,
        K::CoinOriginReadOnlyFnCallFees => CoinOrigin::ReadOnlyFnCallFees,
        K::CoinOriginReadOnlyFnCallCoins => CoinOrigin::ReadOnlyFnCallCoins,
        K::CoinOriginReadOnlyBytecodeExecFees => CoinOrigin::ReadOnlyBytecodeExecFees,
        K::CoinOriginSetBytecodeStorage => CoinOrigin::SetBytecodeStorage,
        K::CoinOriginAbiCallCoins => CoinOrigin::AbiCallCoins,
        K::CoinOriginAbiTransferCoins => CoinOrigin::AbiTransferCoins,
        K::CoinOriginAbiTransferForCoins => CoinOrigin::AbiTransferForCoins,
        K::CoinOriginAbiSendMsgCoins => CoinOrigin::AbiSendMsgCoins,
        K::CoinOriginAbiSendMsgFees => CoinOrigin::AbiSendMsgFees,
        K::CoinOriginOpRollSellDeferredMas => CoinOrigin::OpRollSellDeferredMas,
        K::CoinOriginOpExecutescFees => CoinOrigin::OpExecutescFees,
        K::CoinOriginOpTransactionCoins => CoinOrigin::OpTransactionCoins,
        K::CoinOriginOpTransactionFees => CoinOrigin::OpTransactionFees,
        K::CoinOriginAsyncMsgCoins => CoinOrigin::AsyncMsgCoins,
        K::CoinOriginAsyncMsgCancel => CoinOrigin::AsyncMsgCancel,
        K::CoinOriginCreateScStorage => CoinOrigin::CreateScStorage,
        K::CoinOriginDatastoreStorage => CoinOrigin::DatastoreStorage,
        K::CoinOriginDeferredCredit => CoinOrigin::DeferredCredit,
    }
}

pub fn encode_transfer(t: &StoredTransfer) -> Vec<u8> {
    let pb = pb::StoredTransferPb {
        slot: Some(slot_to_pb(t.slot)),
        index_in_slot: t.index_in_slot,
        id: t.id.clone(),
        block_id: t.block_id.clone().unwrap_or_default(),
        has_block_id: t.block_id.is_some(),
        block_timestamp_ms: t.block_timestamp_ms,
        from: t.from.clone().unwrap_or_default(),
        has_from: t.from.is_some(),
        to: t.to.clone().unwrap_or_default(),
        has_to: t.to.is_some(),
        value: Some(transfer_value_to_pb(&t.value)),
        origin: Some(coin_origin_to_pb(&t.origin)),
        operation_id: t.operation_id.clone().unwrap_or_default(),
        has_operation_id: t.operation_id.is_some(),
        async_msg_id: t.async_msg_id.clone().unwrap_or_default(),
        has_async_msg_id: t.async_msg_id.is_some(),
        deferred_call_id: t.deferred_call_id.clone().unwrap_or_default(),
        has_deferred_call_id: t.deferred_call_id.is_some(),
        denunciation_index: t.denunciation_index.clone().unwrap_or_default(),
        has_denunciation_index: t.denunciation_index.is_some(),
        is_final: t.is_final,
        first_seen_ts_ms: t.first_seen_ts_ms,
    };
    pb.encode_to_vec()
}

pub fn decode_transfer(bytes: &[u8]) -> Result<StoredTransfer, CodecError> {
    let p = pb::StoredTransferPb::decode(bytes)?;
    Ok(StoredTransfer {
        slot: slot_from_opt(p.slot.as_ref())?,
        index_in_slot: p.index_in_slot,
        id: p.id,
        block_id: p.has_block_id.then_some(p.block_id),
        block_timestamp_ms: p.block_timestamp_ms,
        from: p.has_from.then_some(p.from),
        to: p.has_to.then_some(p.to),
        value: transfer_value_from_pb(p.value.unwrap_or_default()),
        origin: coin_origin_from_pb(p.origin.unwrap_or_default()),
        operation_id: p.has_operation_id.then_some(p.operation_id),
        async_msg_id: p.has_async_msg_id.then_some(p.async_msg_id),
        deferred_call_id: p.has_deferred_call_id.then_some(p.deferred_call_id),
        denunciation_index: p.has_denunciation_index.then_some(p.denunciation_index),
        is_final: p.is_final,
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// StoredAsyncMsg + StoredDeferredCall
// ---------------------------------------------------------------------------

fn async_state_to_pb(s: AsyncMsgState) -> i32 {
    match s {
        AsyncMsgState::Pending => pb::AsyncMsgStatePb::AsyncMsgStatePending as i32,
        AsyncMsgState::Executed => pb::AsyncMsgStatePb::AsyncMsgStateExecuted as i32,
        AsyncMsgState::Cancelled => pb::AsyncMsgStatePb::AsyncMsgStateCancelled as i32,
        AsyncMsgState::Consumed => pb::AsyncMsgStatePb::AsyncMsgStateConsumed as i32,
    }
}

fn async_state_from_pb(i: i32) -> AsyncMsgState {
    match pb::AsyncMsgStatePb::try_from(i) {
        Ok(pb::AsyncMsgStatePb::AsyncMsgStatePending) => AsyncMsgState::Pending,
        Ok(pb::AsyncMsgStatePb::AsyncMsgStateExecuted) => AsyncMsgState::Executed,
        Ok(pb::AsyncMsgStatePb::AsyncMsgStateCancelled) => AsyncMsgState::Cancelled,
        Ok(pb::AsyncMsgStatePb::AsyncMsgStateConsumed) => AsyncMsgState::Consumed,
        // Unknown future variant → default to `Pending` so we don't accidentally
        // retire something we can't interpret yet.
        Err(_) => AsyncMsgState::Pending,
    }
}

fn deferred_state_to_pb(s: DeferredCallState) -> i32 {
    match s {
        DeferredCallState::Registered => pb::DeferredCallStatePb::DeferredCallStateRegistered as i32,
        DeferredCallState::Executed => pb::DeferredCallStatePb::DeferredCallStateExecuted as i32,
        DeferredCallState::Failed => pb::DeferredCallStatePb::DeferredCallStateFailed as i32,
        DeferredCallState::Cancelled => pb::DeferredCallStatePb::DeferredCallStateCancelled as i32,
    }
}

fn deferred_state_from_pb(i: i32) -> DeferredCallState {
    match pb::DeferredCallStatePb::try_from(i) {
        Ok(pb::DeferredCallStatePb::DeferredCallStateRegistered) => DeferredCallState::Registered,
        Ok(pb::DeferredCallStatePb::DeferredCallStateExecuted) => DeferredCallState::Executed,
        Ok(pb::DeferredCallStatePb::DeferredCallStateFailed) => DeferredCallState::Failed,
        Ok(pb::DeferredCallStatePb::DeferredCallStateCancelled) => DeferredCallState::Cancelled,
        Err(_) => DeferredCallState::Registered,
    }
}

fn trigger_to_pb(t: &AsyncTrigger) -> pb::AsyncTriggerPb {
    pb::AsyncTriggerPb {
        address: t.address.clone(),
        datastore_key_hex: t.datastore_key_hex.clone().unwrap_or_default(),
        has_datastore_key: t.datastore_key_hex.is_some(),
    }
}

fn trigger_from_pb(p: &pb::AsyncTriggerPb) -> AsyncTrigger {
    AsyncTrigger {
        address: p.address.clone(),
        datastore_key_hex: p.has_datastore_key.then(|| p.datastore_key_hex.clone()),
    }
}

pub fn encode_async_msg(m: &StoredAsyncMsg) -> Vec<u8> {
    let pb = pb::StoredAsyncMsgPb {
        id: m.id.clone(),
        sender_key: m.sender.as_ref().map(addr_to_key).unwrap_or_default(),
        has_sender: m.sender.is_some(),
        destination_key: m.destination.as_ref().map(addr_to_key).unwrap_or_default(),
        has_destination: m.destination.is_some(),
        handler: m.handler.clone().unwrap_or_default(),
        has_handler: m.handler.is_some(),
        coins_nmas: m.coins_nmas,
        max_gas: m.max_gas,
        fee_nmas: m.fee_nmas,
        emission_slot: m.emission_slot.map(slot_to_pb),
        has_emission_slot: m.emission_slot.is_some(),
        validity_start: m.validity_start.map(slot_to_pb),
        has_validity_start: m.validity_start.is_some(),
        validity_end: m.validity_end.map(slot_to_pb),
        has_validity_end: m.validity_end.is_some(),
        state: async_state_to_pb(m.state),
        last_slot: m.last_slot.map(slot_to_pb),
        has_last_slot: m.last_slot.is_some(),
        data_hex: m.data_hex.clone().unwrap_or_default(),
        has_data: m.data_hex.is_some(),
        trigger: m.trigger.as_ref().map(trigger_to_pb),
        has_trigger: m.trigger.is_some(),
        can_be_executed: m.can_be_executed,
        first_seen_ts_ms: m.first_seen_ts_ms,
        last_updated_ts_ms: m.last_updated_ts_ms,
    };
    pb.encode_to_vec()
}

pub fn decode_async_msg(bytes: &[u8]) -> Result<StoredAsyncMsg, CodecError> {
    let p = pb::StoredAsyncMsgPb::decode(bytes)?;
    Ok(StoredAsyncMsg {
        id: p.id,
        sender: if p.has_sender {
            Some(addr_from_key(&p.sender_key)?)
        } else {
            None
        },
        destination: if p.has_destination {
            Some(addr_from_key(&p.destination_key)?)
        } else {
            None
        },
        handler: p.has_handler.then_some(p.handler),
        coins_nmas: p.coins_nmas,
        max_gas: p.max_gas,
        fee_nmas: p.fee_nmas,
        emission_slot: if p.has_emission_slot {
            Some(slot_from_opt(p.emission_slot.as_ref())?)
        } else {
            None
        },
        validity_start: if p.has_validity_start {
            Some(slot_from_opt(p.validity_start.as_ref())?)
        } else {
            None
        },
        validity_end: if p.has_validity_end {
            Some(slot_from_opt(p.validity_end.as_ref())?)
        } else {
            None
        },
        state: async_state_from_pb(p.state),
        last_slot: if p.has_last_slot {
            Some(slot_from_opt(p.last_slot.as_ref())?)
        } else {
            None
        },
        data_hex: p.has_data.then_some(p.data_hex),
        trigger: p.has_trigger.then(|| {
            p.trigger
                .as_ref()
                .map(trigger_from_pb)
                .unwrap_or_default()
        }),
        can_be_executed: p.can_be_executed,
        first_seen_ts_ms: p.first_seen_ts_ms,
        last_updated_ts_ms: p.last_updated_ts_ms,
    })
}

pub fn encode_deferred_call(d: &StoredDeferredCall) -> Vec<u8> {
    let pb = pb::StoredDeferredCallPb {
        id: d.id.clone(),
        sender_key: d.sender.as_ref().map(addr_to_key).unwrap_or_default(),
        has_sender: d.sender.is_some(),
        target_address_key: d
            .target_address
            .as_ref()
            .map(addr_to_key)
            .unwrap_or_default(),
        has_target_address: d.target_address.is_some(),
        target_function: d.target_function.clone().unwrap_or_default(),
        has_target_function: d.target_function.is_some(),
        parameter_hex: d.parameter_hex.clone().unwrap_or_default(),
        has_parameter_hex: d.parameter_hex.is_some(),
        coins_nmas: d.coins_nmas,
        max_gas: d.max_gas,
        target_slot: d.target_slot.map(slot_to_pb),
        has_target_slot: d.target_slot.is_some(),
        registered_slot: d.registered_slot.map(slot_to_pb),
        has_registered_slot: d.registered_slot.is_some(),
        state: deferred_state_to_pb(d.state),
        last_slot: d.last_slot.map(slot_to_pb),
        has_last_slot: d.last_slot.is_some(),
        first_seen_ts_ms: d.first_seen_ts_ms,
        last_updated_ts_ms: d.last_updated_ts_ms,
    };
    pb.encode_to_vec()
}

pub fn decode_deferred_call(bytes: &[u8]) -> Result<StoredDeferredCall, CodecError> {
    let p = pb::StoredDeferredCallPb::decode(bytes)?;
    Ok(StoredDeferredCall {
        id: p.id,
        sender: if p.has_sender {
            Some(addr_from_key(&p.sender_key)?)
        } else {
            None
        },
        target_address: if p.has_target_address {
            Some(addr_from_key(&p.target_address_key)?)
        } else {
            None
        },
        target_function: p.has_target_function.then_some(p.target_function),
        parameter_hex: p.has_parameter_hex.then_some(p.parameter_hex),
        coins_nmas: p.coins_nmas,
        max_gas: p.max_gas,
        target_slot: if p.has_target_slot {
            Some(slot_from_opt(p.target_slot.as_ref())?)
        } else {
            None
        },
        registered_slot: if p.has_registered_slot {
            Some(slot_from_opt(p.registered_slot.as_ref())?)
        } else {
            None
        },
        state: deferred_state_from_pb(p.state),
        last_slot: if p.has_last_slot {
            Some(slot_from_opt(p.last_slot.as_ref())?)
        } else {
            None
        },
        first_seen_ts_ms: p.first_seen_ts_ms,
        last_updated_ts_ms: p.last_updated_ts_ms,
    })
}

// ---------------------------------------------------------------------------
// Peer wire conversions
// ---------------------------------------------------------------------------
//
// The `FinalSlotResponse` in `peer.proto` ships `Stored*Pb` directly. These
// helpers go from domain -> pb (for the server side that builds a
// response) and pb -> domain (for the client side that applies a patch).

pub fn block_to_peer_pb(b: &StoredBlock) -> Result<pb::StoredBlockPb, CodecError> {
    let endorsements = b
        .endorsements
        .iter()
        .map(endorsement_to_pb)
        .collect::<Result<Vec<_>, _>>()?;
    let raw_signed_header = if b.raw_signed_header_b64.is_empty() {
        Vec::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.decode(&b.raw_signed_header_b64)
            .map_err(|e| err(format!("raw_signed_header b64: {e}")))?
    };
    Ok(pb::StoredBlockPb {
        id_key: block_id_to_key(&b.id),
        slot: Some(slot_to_pb(b.slot)),
        creator_key: addr_to_key(&b.creator),
        parent_keys: b.parents.iter().map(block_id_to_key).collect(),
        operation_id_keys: b.operation_ids.iter().map(op_id_to_key).collect(),
        endorsements,
        endorsement_ids: b.endorsement_ids.clone(),
        denunciations: b.denunciations.iter().map(denunciation_to_pb).collect(),
        current_version: b.current_version,
        announced_version: b.announced_version.unwrap_or(0),
        has_announced_version: b.announced_version.is_some(),
        operations_hash: b.operations_hash.clone(),
        signature: b.signature.clone(),
        content_creator_pub_key: b.content_creator_pub_key.clone(),
        serialized_size: b.serialized_size,
        raw_signed_header,
        status: block_status_to_pb(b.status),
        first_seen_ts_ms: b.first_seen_ts_ms,
    })
}

pub fn block_from_peer_pb(p: pb::StoredBlockPb) -> Result<StoredBlock, CodecError> {
    let raw_signed_header_b64 = if p.raw_signed_header.is_empty() {
        String::new()
    } else {
        use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
        B64.encode(&p.raw_signed_header)
    };
    Ok(StoredBlock {
        id: block_id_from_key(&p.id_key)?,
        slot: slot_from_opt(p.slot.as_ref())?,
        creator: addr_from_key(&p.creator_key)?,
        parents: p
            .parent_keys
            .iter()
            .map(|b| block_id_from_key(b))
            .collect::<Result<_, _>>()?,
        operation_ids: p
            .operation_id_keys
            .iter()
            .map(|b| op_id_from_key(b))
            .collect::<Result<_, _>>()?,
        endorsements: p
            .endorsements
            .into_iter()
            .map(endorsement_from_pb)
            .collect::<Result<_, _>>()?,
        endorsement_ids: p.endorsement_ids,
        denunciations: p
            .denunciations
            .into_iter()
            .map(denunciation_from_pb)
            .collect::<Result<_, _>>()?,
        current_version: p.current_version,
        announced_version: if p.has_announced_version {
            Some(p.announced_version)
        } else {
            None
        },
        operations_hash: p.operations_hash,
        signature: p.signature,
        content_creator_pub_key: p.content_creator_pub_key,
        serialized_size: p.serialized_size,
        raw_signed_header_b64,
        status: block_status_from_pb(p.status),
        first_seen_ts_ms: p.first_seen_ts_ms,
    })
}

pub fn operation_to_peer_pb(o: &StoredOperation) -> Result<pb::StoredOperationPb, CodecError> {
    // Reuse encode_operation logic via decode-less path.
    let bytes = encode_operation(o)?;
    Ok(pb::StoredOperationPb::decode(bytes.as_slice())?)
}
pub fn operation_from_peer_pb(p: pb::StoredOperationPb) -> Result<StoredOperation, CodecError> {
    let bytes = p.encode_to_vec();
    decode_operation(&bytes)
}

pub fn endorsement_to_peer_pb(e: &StoredEndorsement) -> Result<pb::StoredEndorsementPb, CodecError> {
    endorsement_to_pb(e)
}
pub fn endorsement_from_peer_pb(p: pb::StoredEndorsementPb) -> Result<StoredEndorsement, CodecError> {
    endorsement_from_pb(p)
}

pub fn sc_event_to_peer_pb(e: &StoredScEvent) -> Result<pb::StoredScEventPb, CodecError> {
    Ok(pb::StoredScEventPb::decode(encode_sc_event(e)?.as_slice())?)
}
pub fn sc_event_from_peer_pb(p: pb::StoredScEventPb) -> Result<StoredScEvent, CodecError> {
    decode_sc_event(&p.encode_to_vec())
}

pub fn transfer_to_peer_pb(t: &StoredTransfer) -> pb::StoredTransferPb {
    pb::StoredTransferPb::decode(encode_transfer(t).as_slice())
        .expect("just-encoded transfer must decode")
}
pub fn transfer_from_peer_pb(p: pb::StoredTransferPb) -> Result<StoredTransfer, CodecError> {
    decode_transfer(&p.encode_to_vec())
}

pub fn async_msg_to_peer_pb(m: &StoredAsyncMsg) -> pb::StoredAsyncMsgPb {
    pb::StoredAsyncMsgPb::decode(encode_async_msg(m).as_slice())
        .expect("just-encoded async msg must decode")
}
pub fn async_msg_from_peer_pb(p: pb::StoredAsyncMsgPb) -> Result<StoredAsyncMsg, CodecError> {
    decode_async_msg(&p.encode_to_vec())
}

pub fn deferred_call_to_peer_pb(d: &StoredDeferredCall) -> pb::StoredDeferredCallPb {
    pb::StoredDeferredCallPb::decode(encode_deferred_call(d).as_slice())
        .expect("just-encoded deferred call must decode")
}
pub fn deferred_call_from_peer_pb(
    p: pb::StoredDeferredCallPb,
) -> Result<StoredDeferredCall, CodecError> {
    decode_deferred_call(&p.encode_to_vec())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
    use crate::model::{BlockStatus, OperationInclusion, OperationKind, Slot};

    #[test]
    fn slot_state_roundtrip() {
        let s = SlotState {
            slot: Slot::new(10, 3),
            status: SlotStatus::Final,
            is_miss: false,
            final_block_id: Some(mk_test_block_id(1)),
            candidate_block_ids: vec![mk_test_block_id(2), mk_test_block_id(3)],
            execution_trail_hash: Some("deadbeef".into()),
            executed_op_ids: vec![mk_test_op_id(1), mk_test_op_id(2)],
            sc_event_count: 7,
            completeness: SlotCompleteness {
                block_body_stored: true,
                exec_output_final: true,
                exec_output_candidate: false,
                transfers_stored: true,
            },
            first_seen_ts_ms: 100,
            last_updated_ts_ms: 200,
        };
        let bytes = encode_slot_state(&s);
        let s2 = decode_slot_state(&bytes).unwrap();
        assert_eq!(serde_json::to_value(&s).unwrap(), serde_json::to_value(&s2).unwrap());
    }

    #[test]
    fn block_roundtrip() {
        let b = StoredBlock {
            id: mk_test_block_id(42),
            slot: Slot::new(7, 2),
            creator: mk_test_user_addr(1),
            parents: vec![mk_test_block_id(1), mk_test_block_id(2)],
            operation_ids: vec![mk_test_op_id(1)],
            endorsements: vec![],
            endorsement_ids: vec!["E1".into()],
            denunciations: vec![],
            current_version: 1,
            announced_version: Some(2),
            operations_hash: "abc".into(),
            signature: "sig".into(),
            content_creator_pub_key: "pk".into(),
            serialized_size: 10,
            raw_signed_header_b64: String::new(),
            status: BlockStatus::Final,
            first_seen_ts_ms: 1234,
        };
        let bytes = encode_block(&b).unwrap();
        let b2 = decode_block(&bytes).unwrap();
        assert_eq!(b2.id, b.id);
        assert_eq!(b2.slot, b.slot);
        assert_eq!(b2.creator, b.creator);
        assert_eq!(b2.announced_version, b.announced_version);
    }

    #[test]
    fn operation_roundtrip() {
        let op = StoredOperation {
            id: mk_test_op_id(1),
            creator: mk_test_user_addr(1),
            target: Some(mk_test_user_addr(2)),
            kind: OperationKind::CallSc,
            expire_period: 100,
            fee_nmas: 10_000,
            thread: 5,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(10, 5),
                block_id: mk_test_block_id(1),
            }],
            candidate_exec_status: Some(ExecStatus::Ok),
            final_exec_status: Some(ExecStatus::Failed),
            details: OperationDetails {
                coins_nmas: Some(42),
                target_function: Some("f".into()),
                ..Default::default()
            },
            signature: "s".into(),
            content_creator_pub_key: "pk".into(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 1,
        };
        let bytes = encode_operation(&op).unwrap();
        let op2 = decode_operation(&bytes).unwrap();
        assert_eq!(op.id, op2.id);
        assert_eq!(op.kind, op2.kind);
        assert_eq!(op.candidate_exec_status, op2.candidate_exec_status);
        assert_eq!(op.final_exec_status, op2.final_exec_status);
        assert_eq!(op.details.coins_nmas, op2.details.coins_nmas);
        assert_eq!(op.details.target_function, op2.details.target_function);
    }

    #[test]
    fn transfer_value_roundtrip() {
        for v in [
            TransferValue::Coins { nmas: 1000 },
            TransferValue::Rolls { count: 5 },
            TransferValue::DeferredCredits { nmas: 42 },
            TransferValue::Unknown,
        ] {
            let pb = transfer_value_to_pb(&v);
            let back = transfer_value_from_pb(pb);
            assert_eq!(
                serde_json::to_value(&v).unwrap(),
                serde_json::to_value(&back).unwrap()
            );
        }
    }

    #[test]
    fn coin_origin_forward_compat() {
        // Encode `Other { code: 99 }`, ensure it round-trips through
        // proto (which doesn't have a direct enum variant for it).
        let c = CoinOrigin::Other { code: 99 };
        let pb = coin_origin_to_pb(&c);
        let back = coin_origin_from_pb(pb);
        assert_eq!(back, CoinOrigin::Other { code: 99 });
    }

    #[test]
    fn partial_eq_coin_origin() {
        // CoinOrigin doesn't derive PartialEq for all variants with the
        // same shape, so the assert above only works if we added it.
        // Guard here that the derive is present (tests compile).
        let _ = CoinOrigin::Unspecified == CoinOrigin::Unspecified;
    }
}
