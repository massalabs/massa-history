// Mirrors massa-indexer/src/model.rs exactly. Keep in sync.
// (v1 TODO: generate from OpenAPI.)

export type Network = "mainnet" | "buildnet";

export interface Slot {
  period: number;
  thread: number;
}

export interface SlotCompleteness {
  block_body_stored: boolean;
  exec_output_final: boolean;
  exec_output_candidate: boolean;
  transfers_stored: boolean;
}

// Slot-level status tag (matches Rust SlotStatus enum).
export type SlotStatus = "unknown" | "candidate" | "final";

// Block-level status tag (matches Rust BlockStatus enum).
export type BlockStatus = "seen_candidate" | "final" | "discarded";

export interface SlotState {
  schema_version: number;
  slot: Slot;
  status: SlotStatus;
  is_miss: boolean;
  final_block_id: string | null;
  candidate_block_ids: string[];
  execution_trail_hash: string | null;
  executed_op_ids: string[];
  sc_event_count: number;
  completeness: SlotCompleteness;
  first_seen_ts_ms: number;
  last_updated_ts_ms: number;
}

export interface StoredBlock {
  schema_version: number;
  id: string;
  slot: Slot;
  creator: string;
  parents: string[];
  operation_ids: string[];
  endorsement_ids: string[];
  denunciation_hashes: string[];
  status: BlockStatus;
  first_seen_ts_ms: number;
}

export type OperationKind =
  | "transaction"
  | "roll_buy"
  | "roll_sell"
  | "execute_sc"
  | "call_sc"
  | "unknown";

export type ExecStatus = "ok" | "failed";

/** Kind-specific fields for an operation. All are optional; which ones are
 * populated depends on `StoredOperation.kind`. Mirrors the Rust
 * `OperationDetails` struct. */
export interface OperationDetails {
  amount_nmas?: number | null;
  roll_count?: number | null;
  target_function?: string | null;
  parameter_hex?: string | null;
  parameter_len?: number | null;
  coins_nmas?: number | null;
  max_gas?: number | null;
  bytecode_size?: number | null;
  max_coins_nmas?: number | null;
  datastore_keys?: number | null;
}

export interface StoredOperation {
  schema_version: number;
  id: string;
  creator: string;
  target: string | null;
  kind: OperationKind;
  expire_period: number;
  fee_nmas: number;
  thread: number;
  /** @deprecated Prefer `inclusions[0]`. Omitted by current indexer API. */
  first_included_slot?: Slot | null;
  /** @deprecated Prefer `inclusions[0]`. Omitted by current indexer API. */
  first_included_block_id?: string | null;
  /** Every (slot, block) pair in which the op has been observed. Index 0
   *  is the earliest-seen inclusion. Empty while the op is still in the
   *  pool / not yet seen in a block. */
  inclusions?: OperationInclusion[];
  candidate_exec_status: ExecStatus | null;
  final_exec_status: ExecStatus | null;
  details?: OperationDetails;
  first_seen_ts_ms: number;
}

export interface OperationInclusion {
  slot: Slot;
  block_id: string;
}

/** Strongly-typed payload of a transfer. Mirrors `model::TransferValue`. */
export type TransferValue =
  | { kind: "coins"; nmas: number }
  | { kind: "rolls"; count: number }
  | { kind: "deferred_credits"; nmas: number }
  | { kind: "unknown" };

/** Strongly-typed reason the transfer happened. Mirrors `model::CoinOrigin`. */
export type CoinOrigin =
  | { kind: "unspecified" }
  | { kind: "block_reward" }
  | { kind: "deferred_call_fail" }
  | { kind: "deferred_call_cancel" }
  | { kind: "deferred_call_coins" }
  | { kind: "deferred_call_register" }
  | { kind: "deferred_call_storage_refund" }
  | { kind: "endorsement_reward" }
  | { kind: "endorsed_reward" }
  | { kind: "slash" }
  | { kind: "op_roll_buy" }
  | { kind: "op_roll_sell" }
  | { kind: "op_callsc_coins" }
  | { kind: "read_only_fn_call_fees" }
  | { kind: "read_only_fn_call_coins" }
  | { kind: "read_only_bytecode_exec_fees" }
  | { kind: "set_bytecode_storage" }
  | { kind: "abi_call_coins" }
  | { kind: "abi_transfer_coins" }
  | { kind: "abi_transfer_for_coins" }
  | { kind: "abi_send_msg_coins" }
  | { kind: "abi_send_msg_fees" }
  | { kind: "op_roll_sell_deferred_mas" }
  | { kind: "op_executesc_fees" }
  | { kind: "op_transaction_coins" }
  | { kind: "op_transaction_fees" }
  | { kind: "async_msg_coins" }
  | { kind: "async_msg_cancel" }
  | { kind: "create_sc_storage" }
  | { kind: "datastore_storage" }
  | { kind: "deferred_credit" }
  | { kind: "other"; code: number };

export interface StoredTransfer {
  schema_version: number;
  slot: Slot;
  index_in_slot: number;
  id: string;
  block_id: string | null;
  block_timestamp_ms: number;
  from: string | null;
  to: string | null;
  value: TransferValue;
  origin: CoinOrigin;
  operation_id: string | null;
  async_msg_id: string | null;
  deferred_call_id: string | null;
  denunciation_index: string | null;
  is_final: boolean;
  first_seen_ts_ms: number;
}

export interface StoredScEvent {
  schema_version: number;
  slot: Slot;
  index_in_slot: number;
  data: string;
  emitter_addrs: string[];
  caller_addrs: string[];
  status: SlotStatus;
  op_id: string | null;
}

export interface StatusMeta {
  build_version: string;
  schema_version: number;
  network: string;
  genesis_timestamp_ms: number;
  t0_ms: number;
  thread_count: number;
  last_final_slot: Slot | null;
  last_candidate_slot: Slot | null;
  created_at_ms: number;
  updated_at_ms: number;
}

export interface RowCount {
  cf: string;
  rows: number;
}

export interface Status {
  build_version: string;
  meta: StatusMeta;
  network: string;
  node_grpc_url: string;
  last_final_slot: Slot | null;
  last_candidate_slot: Slot | null;
  row_counts: RowCount[];
  uptime_secs: number;
}

export interface Envelope<T> {
  network: string;
  /** Backend sets this when the current page is not the last one. Drives
   *  the "Next" button enabled state on paginated list views. */
  has_more?: boolean;
  cursor_next?: string | null;
  data: T;
}

export interface HealthResp {
  status: "ok";
  build_version: string;
  network: string;
  uptime_secs: number;
}

// ---------------------------------------------------------------------------
// v1 extensions
// ---------------------------------------------------------------------------

export interface StoredEndorsement {
  schema_version: number;
  id: string;
  slot: Slot;
  index: number;
  endorsed_block_id: string;
  content_creator_pub_key: string;
  content_creator_address: string;
  signature: string;
  serialized_size: number;
  included_block_id: string;
  included_slot: Slot;
  first_seen_ts_ms: number;
}

export type StoredDenunciation =
  | {
      kind: "block_header";
      public_key: string;
      slot: Slot;
      hash_1: string;
      hash_2: string;
      signature_1: string;
      signature_2: string;
    }
  | {
      kind: "endorsement";
      public_key: string;
      slot: Slot;
      index: number;
      hash_1: string;
      hash_2: string;
      signature_1: string;
      signature_2: string;
    }
  | {
      kind: "address";
      address_denounced: string;
      slot: Slot;
      slashed_nmas: number;
    }
  | { kind: "unknown" };

export interface StoredDenunciationEntry {
  schema_version: number;
  hash: string;
  slot: Slot;
  kind: "block_header" | "endorsement" | "address" | "unknown";
  denounced_addr: string | null;
  denunciation: StoredDenunciation;
  included_block_id: string | null;
  included_slot: Slot | null;
  first_seen_ts_ms: number;
}

export interface ChartPoint {
  ts_ms: number;
  value: number;
}

/** A deferred call registered with the node's deferred-call registry — a
 * scheduled smart-contract call that will execute (best-effort) at
 * `target_slot`. Different concept from `DeferredCreditEntry` above which
 * is a queued MAS release. */
export interface StoredDeferredCall {
  id: string;
  sender: string | null;
  target_address: string | null;
  target_function: string | null;
  parameter_hex: string | null;
  coins_nmas: number;
  max_gas: number;
  target_slot: Slot | null;
  registered_slot: Slot | null;
  state:
    | "pending"
    | "executed"
    | "failed"
    | "cancelled"
    | "unknown";
  last_slot: Slot | null;
  first_seen_ts_ms: number;
  last_updated_ts_ms: number;
}

/** A single deferred-credit entry on an address — MAS that have been
 * queued for release at a specific future slot (typically a few cycles
 * after a roll sell). `nmas` is a decimal string for precision safety. */
export interface DeferredCreditEntry {
  slot: Slot;
  nmas: string;
}

// Snapshot of an address's live chain state, fetched from the local node
// via `GET /v1/addresses/:addr/node_state`. Balances are reported in nMAS
// as decimal strings to preserve precision (JS Number can't hold balances
// > 9 M MAS without losing low-end digits).
//
// `active_rolls` are the rolls actually producing blocks in the current PoS
// cycle (≤ `final_rolls`). `deferred_credits_*` lists MAS queued for
// release at a specific future slot — usually because the address recently
// sold rolls.
export interface AddressNodeState {
  address: string;
  final_balance_nmas: string;
  candidate_balance_nmas: string;
  final_rolls: number;
  candidate_rolls: number;
  active_rolls: number;
  deferred_credits_final: DeferredCreditEntry[];
  deferred_credits_candidate: DeferredCreditEntry[];
  queried_at_ms: number;
}

