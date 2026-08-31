//! Domain types stored in RocksDB.
//!
//! We keep these plain `serde`-serializable Rust structs rather than using
//! protobuf for the **stored** value format. The spec §0 says "protobuf
//! wherever possible"; we depart here because:
//!   * Storage values are read back by the REST layer and shipped as JSON;
//!     using `serde` end-to-end avoids a proto→JSON conversion step and keeps
//!     the code short.
//!   * We still use protobuf on the wire (gRPC from the node). The conversion
//!     from proto to these types lives in `ingest.rs`.
//!   * We do not attempt on-disk schema migrations. The operator is expected
//!     to wipe `db.path` between incompatible indexer releases (the indexer
//!     is a derived cache; the node is the source of truth).
//!
//! ## Archival invariant
//!
//! To allow a future operator to fully rebuild state from our RocksDB, every
//! "first-class" entity (block header, operation, endorsement, denunciation)
//! stores **both** its structured, human-friendly JSON view **and** the raw
//! protobuf bytes we received from the node, base64-encoded. The raw payload
//! is sufficient to reconstruct the original `SignedBlockHeader` /
//! `SignedOperation` byte-for-byte.

use crate::ids::{Address, BlockId, OperationId};
use serde::{Deserialize, Serialize};

/// Simple (period, thread) tuple, JSON-serialized as an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Slot {
    pub period: u64,
    pub thread: u8,
}
impl Slot {
    pub fn new(period: u64, thread: u8) -> Self {
        Self { period, thread }
    }
}

/// Execution status of an operation at some tier (candidate or final).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecStatus {
    Ok,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotStatus {
    Unknown,
    Candidate,
    Final,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStatus {
    SeenCandidate,
    Final,
    Discarded,
}

/// Which pieces of data the indexer is configured to ingest from the node.
/// Derived from `[streams]` in the config and used by slot-completeness /
/// backfill logic so disabled streams don't leave slots eternally "in
/// progress".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamsExpected {
    pub filled_blocks: bool,
    pub slot_execution_outputs: bool,
    pub transfers: bool,
}

impl StreamsExpected {
    pub fn all() -> Self {
        Self {
            filled_blocks: true,
            slot_execution_outputs: true,
            transfers: true,
        }
    }
}

/// Per-slot bitmap tracking which pieces of data we've persisted for the
/// slot. `is_complete` consults `StreamsExpected` so that parts we never
/// subscribe to don't gate completeness.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotCompleteness {
    pub block_body_stored: bool,
    pub exec_output_final: bool,
    pub exec_output_candidate: bool,
    pub transfers_stored: bool,
}

impl SlotCompleteness {
    /// A slot is "complete" when every **enabled** stream's part is
    /// present. Miss slots trivially satisfy the block-body requirement.
    pub fn is_complete(&self, is_miss: bool, streams: &StreamsExpected) -> bool {
        let block_ok =
            !streams.filled_blocks || is_miss || self.block_body_stored;
        let exec_ok = !streams.slot_execution_outputs || self.exec_output_final;
        let transfers_ok = !streams.transfers || self.transfers_stored;
        block_ok && exec_ok && transfers_ok
    }
}

/// Full row stored at `cf_slot[slot_key]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotState {
    pub slot: Slot,
    pub status: SlotStatus,
    pub is_miss: bool,

    #[serde(default)]
    pub final_block_id: Option<BlockId>,
    #[serde(default)]
    pub candidate_block_ids: Vec<BlockId>,

    #[serde(default)]
    pub execution_trail_hash: Option<String>,

    #[serde(default)]
    pub executed_op_ids: Vec<OperationId>,
    #[serde(default)]
    pub sc_event_count: u32,

    #[serde(default)]
    pub completeness: SlotCompleteness,

    pub first_seen_ts_ms: i64,
    pub last_updated_ts_ms: i64,
}

impl SlotState {
    pub fn fresh(slot: Slot, now_ms: i64) -> Self {
        Self {
            slot,
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
        }
    }
}

// ---------------------------------------------------------------------------
// Endorsements
// ---------------------------------------------------------------------------

/// Fully-archived endorsement. We keep every byte from the proto so a future
/// reconstruction tool can regenerate the original `SignedEndorsement` bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEndorsement {
    /// `secure_hash` from `SignedEndorsement` — canonical endorsement id.
    pub id: String,
    /// The slot that this endorsement endorses (the slot of the parent block,
    /// not the slot of the including block).
    pub slot: Slot,
    /// Index of the endorsement inside its including block.
    pub index: u32,
    /// Id of the block being endorsed.
    pub endorsed_block_id: String,
    pub content_creator_pub_key: String,
    pub content_creator_address: Address,
    pub signature: String,
    pub serialized_size: u64,
    /// Id of the block that included this endorsement, if we saw it via a
    /// `FilledBlock`. An endorsement re-appearing in multiple blocks (same
    /// endorser → multiple candidates) will end up keyed by `secure_hash` so
    /// this records the first sighting.
    pub included_block_id: String,
    pub included_slot: Slot,
    pub first_seen_ts_ms: i64,
}

// ---------------------------------------------------------------------------
// Denunciations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoredDenunciation {
    BlockHeader {
        public_key: String,
        slot: Slot,
        hash_1: String,
        hash_2: String,
        signature_1: String,
        signature_2: String,
    },
    Endorsement {
        public_key: String,
        slot: Slot,
        index: u32,
        hash_1: String,
        hash_2: String,
        signature_1: String,
        signature_2: String,
    },
    Address {
        address_denounced: String,
        slot: Slot,
        slashed_nmas: u64,
    },
    Unknown,
}

/// Top-level row for the `cf_denunciation` CF. The inner `denunciation` is
/// the structured denunciation payload as it was stored on the including
/// block; `hash` is a deterministic, content-addressed id (SHA-256 over a
/// canonical JSON serialization of the payload) so we can expose a stable
/// `/v1/denunciations/{hash}` URL even though the proto denunciation has no
/// natural `secure_hash` field of its own.
///
/// `denounced_addr` is the address the denunciation is "against" — used as
/// the prefix of the secondary index (`idx_denunciation_by_addr`) so the
/// address dashboard can list slashes cheaply. It is `None` when we cannot
/// derive an address from the denunciation (public-key-only entries of
/// `BlockHeader` / `Endorsement` where the node's proto does not expose an
/// address).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDenunciationEntry {
    /// SHA-256 hex digest of the structured denunciation JSON.
    pub hash: String,
    pub slot: Slot,
    /// Canonical kind label (`block_header`, `endorsement`, `address`,
    /// `unknown`) mirroring the `StoredDenunciation` tag. Promoted for easy
    /// filtering.
    pub kind: String,
    #[serde(default)]
    pub denounced_addr: Option<Address>,
    pub denunciation: StoredDenunciation,
    /// Id of the block that included this denunciation (first sighting).
    #[serde(default)]
    pub included_block_id: Option<BlockId>,
    #[serde(default)]
    pub included_slot: Option<Slot>,
    pub first_seen_ts_ms: i64,
}

// ---------------------------------------------------------------------------
// Blocks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredBlock {
    pub id: BlockId,
    pub slot: Slot,
    pub creator: Address,
    pub parents: Vec<BlockId>,
    pub operation_ids: Vec<OperationId>,

    /// Full endorsement records embedded in the block header, in the order
    /// they appeared. Ids are also mirrored in `endorsement_ids` for quick
    /// scanning / indexing.
    #[serde(default)]
    pub endorsements: Vec<StoredEndorsement>,
    #[serde(default)]
    pub endorsement_ids: Vec<String>,

    /// Full denunciations embedded in the block header.
    #[serde(default)]
    pub denunciations: Vec<StoredDenunciation>,

    // --- Archival envelope ------------------------------------------------
    /// Protocol version this block was produced under.
    #[serde(default)]
    pub current_version: u32,
    /// Optional "upgrade-announce" version.
    #[serde(default)]
    pub announced_version: Option<u32>,
    /// Hash over all operations, as sent in the block header. Used by the
    /// protocol to prove the op set without carrying it.
    #[serde(default)]
    pub operations_hash: String,
    /// Signature of the block header.
    #[serde(default)]
    pub signature: String,
    /// Block-creator public key (redundant with creator address but required
    /// for byte-exact signature verification).
    #[serde(default)]
    pub content_creator_pub_key: String,
    /// Size, in bytes, of the serialized signed block header.
    #[serde(default)]
    pub serialized_size: u64,
    /// Raw protobuf-encoded `SignedBlockHeader` message, base64-encoded.
    /// Re-decoding this yields the exact bytes the node originally emitted.
    #[serde(default)]
    pub raw_signed_header_b64: String,

    pub status: BlockStatus,
    pub first_seen_ts_ms: i64,
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Transaction,
    RollBuy,
    RollSell,
    ExecuteSc,
    CallSc,
    Unknown,
}

/// A single datastore entry passed with an `ExecuteSC`. Keys and values are
/// both arbitrary bytes so we hex-encode them to keep the JSON parseable.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DatastoreEntry {
    pub key_hex: String,
    pub value_hex: String,
}

/// Per-kind operation detail payload. Flat `Option` fields so JSON consumers
/// can treat them as an open extension surface without having to pattern-match
/// a discriminator (the `OperationKind` already tells the consumer which
/// fields apply). For archival completeness `parameter_hex` and `bytecode_hex`
/// are never truncated — the UI layer performs its own length-capping.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OperationDetails {
    // transaction
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount_nmas: Option<u64>,
    // transaction — recipient address (also mirrored in `StoredOperation.target`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient_address: Option<String>,

    // roll_buy / roll_sell
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roll_count: Option<u64>,

    // call_sc
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_function: Option<String>,
    /// Hex-encoded call parameter payload (full length, no cap).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter_hex: Option<String>,
    /// Parameter length in bytes (redundant with `parameter_hex` but convenient).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter_len: Option<u64>,
    /// call_sc: extra coins the caller is willing to forward to the target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coins_nmas: Option<u64>,
    /// call_sc / execute_sc: gas cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_gas: Option<u64>,

    // execute_sc
    /// Full SC bytecode, hex-encoded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytecode_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytecode_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_coins_nmas: Option<u64>,
    /// execute_sc datastore entries (hex keys and values).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub datastore: Vec<DatastoreEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datastore_keys: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOperation {
    pub id: OperationId,
    pub creator: Address,
    #[serde(default)]
    pub target: Option<Address>,
    pub kind: OperationKind,
    pub expire_period: u64,
    pub fee_nmas: u64,
    pub thread: u8,
    /// Every (slot, block) pair in which this operation has ever been
    /// observed. An operation can legitimately appear in several blocks:
    ///   * competing candidate blocks at the same slot (forks),
    ///   * re-inclusion in a later slot if the first block was discarded,
    ///   * cross-thread propagation between a block and its endorsement-only
    ///     children.
    ///
    /// We append to this list at ingestion time (dedup'd on `block_id`).
    /// `inclusions[0]` is always the earliest-seen (slot, block) pair.
    #[serde(default)]
    pub inclusions: Vec<OperationInclusion>,
    #[serde(default)]
    pub candidate_exec_status: Option<ExecStatus>,
    #[serde(default)]
    pub final_exec_status: Option<ExecStatus>,
    /// Kind-specific payload (amount, function, parameter-hex, …).
    #[serde(default)]
    pub details: OperationDetails,

    // --- Archival envelope ------------------------------------------------
    /// Signature of the operation content.
    #[serde(default)]
    pub signature: String,
    /// Public key of the operation creator.
    #[serde(default)]
    pub content_creator_pub_key: String,
    /// Size, in bytes, of the serialized signed operation.
    #[serde(default)]
    pub serialized_size: u64,
    /// Raw protobuf-encoded `SignedOperation`, base64-encoded.
    #[serde(default)]
    pub raw_signed_op_b64: String,

    pub first_seen_ts_ms: i64,
}

/// A single (slot, block) pair where an operation was included. See
/// `StoredOperation::inclusions` for why we store more than one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationInclusion {
    pub slot: Slot,
    pub block_id: BlockId,
}

// ---------------------------------------------------------------------------
// Transfers
// ---------------------------------------------------------------------------

/// The three kinds of "value" a transfer can carry. Matches the proto
/// `TransferValue.value` oneof exactly.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransferValue {
    /// A MAS coin movement, in nanoMAS.
    Coins { nmas: u64 },
    /// A roll movement (integer roll count).
    Rolls { count: u64 },
    /// A deferred-credit release, in nanoMAS.
    DeferredCredits { nmas: u64 },
    /// A whitelisted MRC-20 token movement. Never stored in `cf_transfer`
    /// (that CF stays a 1:1 mirror of the node's transfer stream / peer
    /// wire). Produced only when converting `StoredTokenTransfer` rows for
    /// the public REST API.
    Token {
        contract: String,
        symbol: String,
        name: String,
        decimals: u8,
        /// Raw integer amount (u256) as a decimal string — JSON numbers
        /// cannot carry the full range.
        raw: String,
    },
    /// Value proto field was present but empty.
    Unknown,
}

/// Reason the transfer happened. Direct mirror of proto `CoinOrigin`, with
/// the naming kept `snake_case` so it is easy to parse client-side. The
/// `Unknown(u32)` variant keeps us forward-compatible if the node adds a new
/// origin before we update the indexer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoinOrigin {
    Unspecified,
    BlockReward,
    DeferredCallFail,
    DeferredCallCancel,
    DeferredCallCoins,
    DeferredCallRegister,
    DeferredCallStorageRefund,
    EndorsementReward,
    EndorsedReward,
    Slash,
    OpRollBuy,
    OpRollSell,
    OpCallscCoins,
    ReadOnlyFnCallFees,
    ReadOnlyFnCallCoins,
    ReadOnlyBytecodeExecFees,
    SetBytecodeStorage,
    AbiCallCoins,
    AbiTransferCoins,
    AbiTransferForCoins,
    AbiSendMsgCoins,
    AbiSendMsgFees,
    OpRollSellDeferredMas,
    OpExecutescFees,
    OpTransactionCoins,
    OpTransactionFees,
    AsyncMsgCoins,
    AsyncMsgCancel,
    CreateScStorage,
    DatastoreStorage,
    DeferredCredit,
    /// Whitelisted MRC-20 transfer (REST-only; not a node CoinOrigin).
    Mrc20Transfer,
    /// Whitelisted MRC-20 mint / bridge-in / wrap (REST-only).
    Mrc20Mint,
    /// Whitelisted MRC-20 burn / bridge-out / unwrap (REST-only).
    Mrc20Burn,
    Other { code: u32 },
}

/// A single transfer emitted by a `NewTransfersInfoServer` response. One row
/// per `ExecTransferInfo` element, enriched with the response-level slot,
/// block id, and timestamp. FINAL-only — these records are archival.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTransfer {
    pub slot: Slot,
    pub index_in_slot: u32,
    /// Proto `ExecTransferInfo.id` — an opaque short string tying the
    /// transfer to its execution-trace stack frame.
    pub id: String,
    /// Block that executed this transfer (None for slot-level rewards etc.).
    pub block_id: Option<String>,
    /// Response-level `timestamp` (millis since epoch), exactly as emitted
    /// by the node.
    pub block_timestamp_ms: i64,
    /// Sender address. `None` means minted (e.g. block reward).
    pub from: Option<String>,
    /// Recipient address. `None` means burned.
    pub to: Option<String>,
    pub value: TransferValue,
    pub origin: CoinOrigin,
    /// If the transfer was triggered by a user operation, its id.
    pub operation_id: Option<String>,
    /// If it was triggered by an async message, the message id.
    pub async_msg_id: Option<String>,
    /// If it was triggered by a deferred call, the deferred call id.
    pub deferred_call_id: Option<String>,
    /// If it was a slash, the denunciation index (serialized as its string
    /// form for now — proto `DenunciationIndex`).
    pub denunciation_index: Option<String>,
    /// Always true in the current pipeline (FINAL stream only). Kept so the
    /// REST shape is forward-compatible with a candidate transfer feed.
    #[serde(default = "default_true")]
    pub is_final: bool,
    pub first_seen_ts_ms: i64,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredScEvent {
    pub slot: Slot,
    pub index_in_slot: u32,
    /// Human-readable data: we store the events as the raw strings emitted by
    /// the SC (proto field `ScExecutionEvent.data`).
    pub data: String,
    #[serde(default)]
    pub emitter_addrs: Vec<Address>,
    #[serde(default)]
    pub caller_addrs: Vec<Address>,
    /// Tagged candidate vs final (FINAL overrides CANDIDATE at ingest).
    pub status: SlotStatus,
    #[serde(default)]
    pub op_id: Option<OperationId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaRow {
    pub network: String,
    pub genesis_timestamp_ms: i64,
    pub t0_ms: i64,
    pub thread_count: u8,
    #[serde(default)]
    pub last_final_slot: Option<Slot>,
    #[serde(default)]
    pub last_candidate_slot: Option<Slot>,
    pub build_version: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

// ---------------------------------------------------------------------------
// Async pool + deferred calls (spec §5.1)
// ---------------------------------------------------------------------------

/// Lifecycle state of an async message.
///
/// We derive this from the `AsyncPoolChange` tag plus a best-effort
/// cross-reference to the transfer stream emitted at the same slot:
///
/// * `Pending`   — the pool has the message queued (node emitted `SET` or
///                 `UPDATE`) but the handler has not yet run. This is the
///                 "waiting room" state.
/// * `Executed`  — the message was consumed via a normal handler run. We
///                 mark this when we see a `DELETE` in the same slot as a
///                 transfer with origin `AsyncMsgCoins` for the same id.
/// * `Cancelled` — the message was consumed via a refund path (expired,
///                 filter never triggered, destination rejected).
///                 Signalled by a `DELETE` alongside a transfer of origin
///                 `AsyncMsgCancel` for the same id.
/// * `Consumed`  — a `DELETE` we could not unambiguously classify as either
///                 executed or cancelled. Kept so the explorer can still
///                 show the row as "no longer pending" without guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AsyncMsgState {
    #[default]
    Pending,
    Executed,
    Cancelled,
    Consumed,
}

/// Trigger filter attached to an async message (`AsyncMessageTrigger` in the
/// node proto). Stored verbatim so the explorer can show operators exactly
/// which address/key will unlock the message.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AsyncTrigger {
    /// Contract address the message is waiting on.
    pub address: String,
    /// Optional datastore key whose presence/update fires the filter. Hex
    /// string because the node emits arbitrary bytes.
    pub datastore_key_hex: Option<String>,
}

/// An async message observed in a slot's `SlotExecutionOutput.state_changes`.
/// We keep every intermediate status (queued, cancelled, emitted, executed)
/// plus timing so the explorer can render a lifecycle.
///
/// `first_seen_ts_ms` is set exactly once (on the first `SET` for this id)
/// and preserved on subsequent `UPDATE`/`DELETE` frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAsyncMsg {
    pub id: String,
    pub sender: Option<Address>,
    pub destination: Option<Address>,
    pub handler: Option<String>,
    pub coins_nmas: u64,
    pub max_gas: u64,
    pub fee_nmas: u64,
    pub emission_slot: Option<Slot>,
    pub validity_start: Option<Slot>,
    pub validity_end: Option<Slot>,
    /// `AsyncMsgState` rendered as a snake_case string for backwards
    /// compatibility with JSON clients that already treated this field as
    /// opaque. Stored as an enum on the wire / in prost.
    #[serde(default)]
    pub state: AsyncMsgState,
    /// Slot where we last observed any change for this id. Useful for
    /// sorting the full-pool listing by recency.
    pub last_slot: Option<Slot>,
    /// Raw message body (hex). Kept for anyone who wants to inspect the
    /// payload without a second round-trip to the node.
    #[serde(default)]
    pub data_hex: Option<String>,
    /// Filter trigger, if any. `None` = ready as soon as validity_start.
    #[serde(default)]
    pub trigger: Option<AsyncTrigger>,
    /// Matches `AsyncMessage.can_be_executed`.
    #[serde(default)]
    pub can_be_executed: bool,
    pub first_seen_ts_ms: i64,
    pub last_updated_ts_ms: i64,
}

/// Lifecycle state of a deferred call.
///
/// The node proto we subscribe to does not emit `deferred_calls_changes`
/// directly; instead we derive state by watching the transfer stream for
/// rows whose `deferred_call_id` is set and whose `CoinOrigin` is one of
/// the `DeferredCall*` variants:
///
/// * `Registered` — first sighting via `DeferredCallRegister` (caller
///                  burns bookings + registration fee).
/// * `Executed`   — saw a `DeferredCallCoins` (handler ran successfully,
///                  target received the attached coins) at the target slot.
/// * `Failed`     — saw a `DeferredCallFail` (handler ran but trapped);
///                  the node still pays out a storage refund.
/// * `Cancelled`  — saw a `DeferredCallCancel` (user-initiated cancel or
///                  slot elapsed without execution); refunds coins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeferredCallState {
    #[default]
    Registered,
    Executed,
    Failed,
    Cancelled,
}

/// A deferred call registered with the node's deferred-call registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDeferredCall {
    pub id: String,
    pub sender: Option<Address>,
    pub target_address: Option<Address>,
    pub target_function: Option<String>,
    pub parameter_hex: Option<String>,
    pub coins_nmas: u64,
    pub max_gas: u64,
    pub target_slot: Option<Slot>,
    pub registered_slot: Option<Slot>,
    /// Lifecycle state; see [`DeferredCallState`]. The serialised form is
    /// kept snake_case, so older clients treating this as an opaque string
    /// continue to work.
    #[serde(default)]
    pub state: DeferredCallState,
    /// Slot where we last observed any change for this id.
    pub last_slot: Option<Slot>,
    pub first_seen_ts_ms: i64,
    pub last_updated_ts_ms: i64,
}
