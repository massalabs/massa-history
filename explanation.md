# Indexer V2 — Internals explainer

This document describes, in enough detail to reason about or re-implement, the
internals of `massa-indexer` as they exist in the tree today. Three topics are
covered:

1. The **RocksDB layout** — which column families exist and what lives in each.
2. The **secondary indexes**: how each key is assembled byte-by-byte, and why.
3. The **sync / backfill system** — how an indexer catches up from the node, from
   other indexers, and how it recovers from gaps.

All file references below point to the actual implementation; this document is
descriptive, not normative — the code is the source of truth.

--------------------------------------------------------------------------------

## 1. Database structure

### 1.1 Storage engine

A single `rocksdb::DB` instance is opened at `config.db.path` via
`Db::open` (`massa-indexer/src/db.rs`). The handle is wrapped in an `Arc` and
cloned freely between threads — RocksDB is internally synchronised, we never
need our own mutex around reads. Options:

- `create_if_missing = true`, `create_missing_column_families = true`
- **No explicit compression settings** — we let RocksDB pick its defaults,
  which already apply `Snappy` on L1+ and `ZSTD` on the bottom level on
  recent versions. Reverting to defaults reduces the number of knobs that
  can drift between releases.
- `write_buffer_size = config.db.write_buffer_size_mb × 1 MiB`
- `increase_parallelism(min(cpu, 8))`

### 1.2 Value format

Every value is a [`prost`](https://crates.io/crates/prost)-encoded
`*Pb` message defined in
`massa-indexer/proto/indexer/v1/storage.proto` and wired through
`src/codec.rs`. Protobuf buys us three things over the ad-hoc JSON we
shipped in v0:

- **Compactness** — one byte of tag per field, varint integers, no key
  strings repeated per row.
- **Forward/backward compatibility** — unknown fields are ignored on
  decode, new optional fields default to their zero value on old data.
- **Peer-protocol reuse** — the same `*Pb` messages travel over the
  `GetFinalSlot`/`StreamFinalSlots` gRPC API (§3.3). Peers and the DB
  share one schema instead of maintaining two parallel surfaces.

`src/codec.rs` owns the `Stored* ⇄ *Pb` conversions. Each `encode_*` /
`decode_*` pair is unit-tested for round-trip equality in
`src/codec.rs::tests`.

### 1.2.1 On-disk schema versioning

`cf_meta[row]` holds a `MetaRowPb` that, among other things, carries
`schema_version`. `Db::open` writes the current `SCHEMA_VERSION` when the
database is empty and refuses to start if it finds a row from a different
version (the error message points operators at the reset procedure). This
sits in front of the store so that a future migration has one obvious place
to hook in. The current policy is **"wipe and rebuild"** — we don't ship
migrations between versions during v0.x, we bump the number and delete the
directory.

### 1.3 Column families

Declared in `db.rs` as `pub const CF_*` / `IDX_*` and enumerated in `ALL_CFS`.
There are three kinds of CF:

- **Primary CFs** — one row per domain object, keyed by its natural identifier.
- **Secondary-index CFs** (`idx_*`) — zero-byte values, keys encode `(lookup,
  ordering, primary-id)` so that prefix scans yield the ordered primary ids
  directly.
- **Bookkeeping CFs** — `cf_meta`, `cf_peer_state`, `cf_slot_candidate_block`.

| Column family                  | Primary key                                      | Value / purpose                                                                                          |
| ------------------------------ | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `cf_meta`                      | ASCII string (`row`, `last_final_slot`, `last_candidate_slot`) | `MetaRowPb` / `Slot` (encoded as the `SlotPb` nested in `LastSlotPb`). One well-known key per row. |
| `cf_slot`                      | `slot_key(p,t)` (9 B)                            | `SlotStatePb` — status, miss flag, final/candidate block ids, trail hash, exec'd op ids, SC-event count, `SlotCompleteness` bitmap, first-seen / last-updated timestamps. |
| `cf_block`                     | `BlockId::key_bytes()` (33 B — category + 32-byte hash) | `StoredBlockPb` (full archival): header info, parents, ops, embedded endorsements, denunciations, raw base-64 header, status (`SeenCandidate` / `Final` / `Discarded`). |
| `cf_op`                        | `OperationId::key_bytes()` (33 B)                | `StoredOperationPb`: creator/target, kind, fee, expire, per-block `inclusions[]`, candidate+final exec status, full raw signed-op b64, type-specific `details`. |
| `cf_endorsement`               | `EndorsementId::key_bytes()` (33 B)              | `StoredEndorsementPb`: slot endorsed, index in block, creator, signature, including block id if known.    |
| `cf_sc_event`                  | `sc_event_key(p,t,idx)` (13 B)                   | `StoredScEventPb`: slot, index-in-slot, emitters, callers, optional op id, status, utf-8 data.             |
| `cf_transfer`                  | `transfer_key(p,t,idx)` (13 B)                   | `StoredTransferPb`: slot, block id, op id (optional), from/to, amount, coin origin, status.                |
| `cf_denunciation`              | content-addressed SHA-256 (32 raw bytes)         | `StoredDenunciationEntryPb`: `kind`, denounced address (if any), full raw denunciation, including block.   |
| `cf_slot_candidate_block`      | `slot_key(p,t) ‖ block_id.key_bytes()` (9 + 33 B) | Empty value. Lets us enumerate every block id that was ever a candidate for a slot.                      |
| `cf_async_msg`                 | message id bytes                                 | `StoredAsyncMsgPb` — async-pool message (state: `Pending` / `Executed` / `Cancelled` / `Consumed`). See §3.2.1.           |
| `cf_deferred_call`             | call id bytes                                    | `StoredDeferredCallPb` — deferred-call record (state: `Registered` / `Executed` / `Failed` / `Cancelled`). See §3.2.2.  |
| `cf_peer_state`                | peer name                                        | Peer observability (last seen, last health snapshot). Written by the peer pool.                          |

All secondary-index CFs are listed below in §2, co-located with the key
formats they use.

### 1.4 `cf_meta` canonical keys

```1213:1216:massa-indexer/src/db.rs
pub mod meta_keys {
    pub const ROW: &str = "row";
    pub const LAST_FINAL_SLOT: &str = "last_final_slot";
    pub const LAST_CANDIDATE_SLOT: &str = "last_candidate_slot";
}
```

`row` is the catch-all `MetaRow` (network, created-at, build version, last
schema-version placeholder). `last_final_slot` / `last_candidate_slot` are
single-slot JSON values used by the REST `/v1/status`, by the gRPC consumer
loop to know where to resume, and by the peer `GetHealth` response.

### 1.5 `SlotState` and `SlotCompleteness`

`SlotState` (`src/model.rs`) aggregates everything we know about a slot in
one JSON row. Crucial invariants:

- `status` progresses `Unknown → Candidate → Final` (never downgrades).
- `final_block_id` is only populated on the `Final` transition.
- `candidate_block_ids[]` contains every block ever seen at that slot; when
  the slot finalises, we walk this list and mark the winner `Final` and the
  rest `Discarded` (see `apply_finality_to_blocks` in both `ingest.rs` and
  `peer/patch.rs`).
- `completeness` is a 4-bit bitmap — `block_body_stored`, `exec_output_final`,
  `exec_output_candidate`, `transfers_stored`.
- `SlotCompleteness::is_complete(is_miss, &StreamsExpected)` returns true iff
  every **enabled** stream's bit is set. Disabled streams are treated as if
  they were already complete (see §3.2).

```99:110:massa-indexer/src/model.rs
impl SlotCompleteness {
    pub fn is_complete(&self, is_miss: bool, streams: &StreamsExpected) -> bool {
        let block_ok =
            !streams.filled_blocks || is_miss || self.block_body_stored;
        let exec_ok = !streams.slot_execution_outputs || self.exec_output_final;
        let transfers_ok = !streams.transfers || self.transfers_stored;
        block_ok && exec_ok && transfers_ok
    }
}
```

--------------------------------------------------------------------------------

## 2. Secondary indexes and key layout

### 2.1 Primitives (see `src/keys.rs` and `src/ids.rs`)

Every index key is concatenated from a small set of fixed-width byte blobs.

- `slot_key(period, thread)` — **9 bytes**, `period_be (8) ‖ thread (1)`.
  Used anywhere we want ascending-slot iteration (forward = oldest → newest).
  This matches Massa's native `Slot::to_bytes_key()` so we don't need any
  custom padding on slots.
- `rslot_key(period, thread)` — **9 bytes**, bitwise-NOT of `slot_key`.
  Used anywhere we want descending-slot iteration (forward = newest → oldest).
  This is the trick that makes newest-first indexes cost one `seek + forward`
  scan instead of an iterator reverse.
- `id.key_bytes()` — **33 bytes**, `category(1) ‖ hash(32)`. Implemented on
  `BlockId`, `OperationId`, `EndorsementId`, and `Address`. `Id::parse()`
  does a full `bs58-check` decode of the Massa display string (with CRC
  check) so only cryptographically well-formed ids can ever land in the
  store.
- Integer sub-indexes (`index_in_slot`) are encoded **big-endian** so
  prefix scans see them in numeric order.

Every key in the indexer is **fixed length**: secondary-index keys are a
concatenation of known-size components, which makes prefix scans exact —
no escaping or null-byte trickery required.

### 2.2 Primary key layouts

| CF                   | Key bytes                                                       | Source                                        |
| -------------------- | --------------------------------------------------------------- | --------------------------------------------- |
| `cf_slot`            | `period_be(8) ‖ thread(1)`                                      | `keys::slot_key`                              |
| `cf_block`           | `BlockId::key_bytes()` (33 B)                                   | `db::write_block`                             |
| `cf_op`              | `OperationId::key_bytes()` (33 B)                               | `db::write_op`                                |
| `cf_endorsement`     | `EndorsementId::key_bytes()` (33 B)                             | `db::write_endorsement`                       |
| `cf_sc_event`        | `slot_key(9) ‖ index_in_slot_be(4)`                             | `keys::sc_event_key` (13 B)                   |
| `cf_transfer`        | `slot_key(9) ‖ index_in_slot_be(4)`                             | `keys::transfer_key` (13 B, aliases `sc_event_key`) |
| `cf_denunciation`    | raw SHA-256 of the structured denunciation (32 B)               | `ingest::denunciation_hash`                   |

Both `cf_sc_event` and `cf_transfer` pair `(slot_key, index_in_slot)` so that
a forward scan under `prefix = slot_key(p,t)` walks the slot's rows in
emission order — that's how `iter_sc_events_for_slot` /
`iter_transfers_for_slot` work without touching any index CF.

### 2.3 Secondary indexes — exact key formats

All `idx_*` CFs store **empty values**; the key *is* the data. Layouts:

| Index CF                        | Key layout                                                                        | Sort order under prefix              | Writers (batched into primary write) |
| ------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------ | ------------------------------------ |
| `idx_block_by_creator`          | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ block_id.key_bytes()(33)`                  | newest-first by slot, then block id  | `write_block`                        |
| `idx_op_by_creator`             | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ op_id.key_bytes()(33)`                     | newest-first by **first inclusion** slot | `write_op` (uses `op.inclusions[0].slot`) |
| `idx_op_by_target`              | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ op_id.key_bytes()(33)`                     | same                                 | `write_op` (target-bearing ops only) |
| `idx_endorsement_by_creator`    | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ endorsement_id.key_bytes()(33)`            | newest-first by endorsed slot        | `write_endorsement`                  |
| `idx_transfer_by_addr`          | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4) ‖ tag(1)`                      | newest-first; `tag=1` → sender, `tag=2` → receiver | `write_transfer` (emits both sides)  |
| `idx_transfer_by_op`            | `op_id.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4)`                              | newest-first                         | `write_transfer`                     |
| `idx_transfer_by_block`         | `block_id.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4)`                           | newest-first                         | `write_transfer`                     |
| `idx_denunciation_by_addr`      | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ hash(32)`                                  | newest-first                         | `write_denunciation` (victim address only) |
| `idx_sc_event_by_emitter`       | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4)`                               | newest-first                         | `write_sc_event` — one row per emitter in the event |
| `idx_sc_event_by_caller`        | `addr.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4)`                               | same                                 | `write_sc_event` — one row per caller |
| `idx_sc_event_by_op`            | `op_id.key_bytes()(33) ‖ rslot_key(9) ‖ index_be(4)`                              | newest-first                         | `write_sc_event` when event has op id |
| `idx_async_by_sender`           | `sender.key_bytes()(33) ‖ msg_id`                                                 | ascending msg id (ordering by sender only matters as a prefix filter) | `write_async_msg` |
| `idx_async_by_dest`             | `dest.key_bytes()(33) ‖ msg_id`                                                   | same                                 | `write_async_msg`                    |
| `idx_async_by_last_slot`        | `rslot_key(9) ‖ msg_id`                                                           | newest-first                         | `write_async_msg` — peer backfill uses this to ship every async row whose `last_slot` matches the requested slot (§3.3). |
| `idx_deferred_by_sender`        | `sender.key_bytes()(33) ‖ call_id`                                                | same                                 | `write_deferred_call`                |
| `idx_deferred_by_target`        | `target.key_bytes()(33) ‖ call_id`                                                | same                                 | `write_deferred_call`                |
| `idx_deferred_by_last_slot`     | `rslot_key(9) ‖ call_id`                                                          | newest-first                         | `write_deferred_call` — same role for deferred calls (§3.3). |

Notes on the `idx_transfer_by_addr` tag byte: without it, a wallet that both
sends and receives a transfer in the same slot would collapse the sender and
receiver rows into a single key. The tag keeps them distinct and lets us
reconstruct the role when we iterate. When the same (p, t, index) pair shows
up twice in a scan (self-transfer), `iter_transfers_by_addr` de-duplicates by
`(p, t, index)` so the result list stays clean.

### 2.4 Prefix-scan shape

Every "list by X" query in the REST layer boils down to:

1. Compute `prefix = x.key_bytes()` (33 B for addresses/ids, 9 B for a slot
   prefix).
2. Pick a starting key: either `prefix` (first page) or the `after` cursor
   the client just sent us (subsequent pages).
3. `db.iterator_cf(CF_IDX, IteratorMode::From(start, Direction::Forward))`.
4. If `start == cursor`, skip the first row so pagination is exclusive.
5. Break out as soon as the key no longer starts with `prefix`.
6. Decode the trailing `(rslot, id)` with the matching `parse_idx_*` helper,
   `get_cf(primary, id.key_bytes())` to fetch the full row, push it into
   `Page.items` until `limit` is reached.
7. If a `limit + 1`-th matching key exists, stash it as `Page.next_cursor`
   so the next call seeks straight to the right row without re-walking.

Because `rslot_key` is the bitwise-NOT of `slot_key`, step 3 yields
newest-first naturally without any `Reverse` iterator. That property is
exercised by the property test `prop_rslot_orders_desc` in `keys.rs`.

The cursor is an opaque raw RocksDB key. The REST layer base64url-encodes
it into an ASCII token before shipping it to clients and decodes it on the
way back, but nothing about the token's internal structure is part of the
API contract — the indexer is free to change key layout between schema
versions, and old cursors become meaningless on reset (which is fine
because we also wipe the DB on schema bumps). This deliberately rules
out "deep link to page 137" style URLs; the explorer gets cheap `Next`
clicks and a client-side `Prev` stack in exchange for cursors that cannot
skip around arbitrarily.

Every `Db::iter_*` method therefore has the same shape:

```rust
fn iter_thing_by_foo(&self, foo: &Foo, after: Option<&[u8]>, limit: usize)
    -> Result<Page<Thing>>;
```

and `limit` is always bounded by `HARD_MAX_PAGE_SIZE` (100) at the REST
entry point, so even a malicious caller cannot ask for arbitrarily long
scans. Internal callers (chart handlers, CSV export) bypass that cap by
calling the DB directly in batches of 100.

### 2.5 Idempotency and compaction hygiene

- **Block / endorsement / transfer / sc-event / denunciation writes** batch
  the primary row and every matching secondary-index row into one
  `WriteBatch`, so an index can never diverge from its primary.
- **Operation writes** compute the `(addr, rslot, op_id)` from
  `op.inclusions.first()`. That first inclusion is maintained as the earliest
  sighting by the ingest worker, so re-applying a block that re-includes an
  already-stored op keeps the original ordering intact.
- **SC events and transfers** are bucketed per slot and fully cleared before
  being rewritten whenever a slot's final execution output arrives. The
  `clear_*_for_slot` helpers delete both the primary rows and every matching
  secondary-index entry in a single batch so nothing lingers.
- **Endorsements and denunciations** are first-write-wins: the
  `write_endorsement` and `write_denunciation` helpers detect a pre-existing
  row and only patch in missing fields (e.g. `included_block_id`), which
  keeps the "first sighting" timestamp stable across restarts and replays.

--------------------------------------------------------------------------------

## 3. Sync and backfill system

The indexer has two orthogonal ways to pick up data:

- The **node gRPC streams** (§3.1) — the primary, always-on source. They
  deliver CANDIDATE and FINAL frames as the node produces them.
- The **peer backfill** (§3.3) — a pull-mode catch-up mechanism that talks
  to other indexers to plug historical gaps that the local indexer never
  received (e.g. it wasn't running, or a stream was temporarily disabled).

Both feed a **single writer** — the ingest worker — so ordering invariants are
identical regardless of provenance.

### 3.1 Node-stream subscribers

`src/grpc.rs` spawns one Tokio task per enabled stream in `config.streams`:

- `NewFilledBlocksServer` → `Event::Block(FilledBlock)`
- `NewSlotExecutionOutputsServer` → `Event::Exec(SlotExecutionOutput)`
- `NewTransfersInfoServer` → `Event::Transfers(NewTransfersInfoServerResponse)`

`NewSlotABICallStacks` is intentionally **not** subscribed to — the
indexer does not store or surface ABI call-stack data. Emitter / caller
addresses and the owning operation / async message / deferred call are
already reconstructible from the SC-event context and the transfer
rows, which is enough for every user-facing query the explorer needs.

Every task shares a common template: connect, subscribe, push each message
into the ingest `mpsc::Sender` as the matching `Event` variant, and on error
back off exponentially (respecting `UNIMPLEMENTED` as a signal that the node
doesn't expose the stream). A task for a stream whose `[streams]` flag is
`false` is never spawned, so the ingest worker simply never receives those
events — and `SlotCompleteness::is_complete` ignores the matching bit (§1.5).

### 3.2 Ingest state machine

`src/ingest.rs` runs a single async loop that pulls `Event`s and dispatches:

```rust
pub enum Event {
    Block(Box<FilledBlock>),
    Exec(Box<SlotExecutionOutput>),
    Transfers(Box<NewTransfersInfoServerResponse>),
    PeerPatch(Box<indexer::v1::FinalSlotResponse>),
    Tick,
}
```

Each handler reads (or initialises) the `SlotState`, mutates what it can, and
writes back. Key invariants:

- **First-final-wins trail hash** (§7.1 of the spec): once a slot is `Final`,
  a second FINAL frame with a different `execution_trail_hash` is logged and
  discarded.
- **Never downgrade**: `SlotStatus::Final` can't move back to `Candidate`;
  `BlockStatus::Final` / `BlockStatus::Discarded` can't move back to
  `SeenCandidate`.
- **Re-runs are no-ops**: every write is either idempotent (`write_block`,
  `write_transfer`) or clear-then-rewrite (SC events and transfers when the
  slot finalises). Running the full stream history twice produces the same
  final DB state.
- **Parent-gap discovery (§8.4)**: `ensure_parent_stub` is called from
  `handle_block`, from the FINAL branch of `handle_exec`, and from
  `apply_peer_patch`. It inserts an `Unknown` `SlotState` for the previous
  slot in the same thread *if and only if* that slot has no row yet. The
  backfill scanner (§3.4) will then pick it up naturally.

`PeerPatch` events go through `peer::patch::apply_peer_patch` instead of the
stream handlers, but they end up touching the exact same `SlotState` and
share the same finality bookkeeping (the `apply_finality_to_blocks` helper is
duplicated in both code paths and kept deliberately symmetric).

### 3.2.1 Async pool

Every FINAL `SlotExecutionOutput` goes through
`apply_async_pool_changes`:

- `SET` — lift the embedded `AsyncMessage` into a new `StoredAsyncMsg`
  row (sender / destination / handler / coins / fee / validity window /
  payload / trigger / `can_be_executed`) with `state = Pending`.
  `first_seen_ts_ms` is captured here and preserved across every
  subsequent upsert.
- `UPDATE` — field-by-field merge over whatever row is in the DB (or a
  synthetic stub if we joined the stream mid-life). The proto's
  `SetOrKeep*` semantics are honoured: only `Set` branches touch
  storage, `Keep` / unset fields survive. Sender/destination changes
  also rewrite the `idx_async_by_sender` / `idx_async_by_dest`
  entries in the same `WriteBatch`.
- `DELETE` — mark terminal. The state transitions to `Consumed` unless
  a transfer has already promoted the row to `Executed` or `Cancelled`
  earlier in the same ingest pass (the transfer stream carries the
  ground-truth reason the message left the pool).

Once the slot's transfer list has been persisted,
`reconcile_async_from_transfers` walks the rows we just wrote looking
for `async_msg_id`-tagged transfers:

- `CoinOrigin::AsyncMsgCoins` → `Executed`,
- `CoinOrigin::AsyncMsgCancel` → `Cancelled`.

It also mints minimal rows for ids we saw on transfers but never saw
in the async-pool stream, so `/v1/async/{id}` and the "source" column
on the transfers table always resolve. Terminal states are sticky —
we never downgrade.

### 3.2.2 Deferred calls

The node's `state_changes` proto does not expose a
`deferred_calls_changes` field, so the lifecycle is driven entirely
from `reconcile_deferred_from_transfers`, which runs right after the
transfer list is durable. For every transfer carrying a
`deferred_call_id`:

- `DeferredCallRegister` — mark `Registered`; capture `registered_slot`
  and `sender` (= caller of the registration op).
- `DeferredCallCoins` — the handler ran and paid the target. Mark
  `Executed` and capture `target_slot` + `target_address`.
- `DeferredCallFail` — handler trapped. Mark `Failed`.
- `DeferredCallCancel` — user cancel or expired registration. Mark
  `Cancelled`.
- `DeferredCallStorageRefund` — ancillary refund row; we only touch
  `last_slot`/`last_updated_ts_ms`.

Terminal states (`Executed` / `Failed` / `Cancelled`) are sticky.
`idx_deferred_by_sender` / `idx_deferred_by_target` are rewritten on
every upsert, including address changes. Because the node can emit
the REGISTER and the EXECUTE in different slots, the upsert carefully
merges: the second write preserves the sender/registered_slot from
the first. The coin-amount field keeps the largest value seen across
rows — the register row books the full amount while later rows are
partial refunds.

### 3.3 Peer protocol

The peer surface is defined in `massa-indexer/proto/indexer/v1/peer.proto`.
Three RPCs:

- `GetHealth(HealthRequest) → HealthResponse` — cheap identity + liveness
  probe. Returns peer id, network name, build version, highest-known FINAL
  slot, and wall-clock timestamp.
- `GetFinalSlot(FinalSlotRequest) → FinalSlotResponse` — per-slot fetch.
  Caller passes `(period, thread)` plus a `FinalSlotParts` bitmask
  (`block`, `exec_output`, `transfers`). The peer answers with the
  parts it has; if the slot isn't FINAL locally it returns
  `final_known = false`.
- `StreamFinalSlots(StreamFinalSlotsRequest) → stream FinalSlotResponse`
  — server-streaming bulk fetch over a `[from, to]` range, newest-first,
  capped at `stream_limit_cap` (default 512).

The payload shape:

- `FinalSlotResponse` ships stored rows as the very same `*Pb` messages
  defined in `storage.proto` (`StoredBlockPb`, `StoredOperationPb`,
  `StoredEndorsementPb`, `StoredScEventPb`, `StoredTransferPb`,
  `StoredAsyncMsgPb`, `StoredDeferredCallPb`). One schema covers both
  the on-disk layout and the wire format.
- `block`, `operations[]`, `endorsements[]`, `sc_events[]`, `transfers[]`
  are populated in local-write order (index-in-slot) so the receiver can
  replay them idempotently without reordering.
- Denunciations ride inside `StoredBlockPb.denunciations`. The receiver
  expands them into `cf_denunciation` on apply; they are not a separate
  part on the wire.
- `async_msgs[]` travels under the `exec_output` part. The peer service
  enumerates all rows whose `last_slot == (period, thread)` via the
  `idx_async_by_last_slot` CF, so the caller gets the full snapshot of
  the async lifecycle that happened at this slot.
- `deferred_calls[]` travels under the `transfers` part (deferred-call
  lifecycle is driven by transfer sightings). The peer service
  enumerates rows via `idx_deferred_by_last_slot`.
- `execution_trail_hash` and `final_block_id` are echoed at the envelope
  level for quick header-only reasoning. Peers that don't know the slot
  yet reply with `final_known = false` and empty part fields.

Peer server (`src/peer/service.rs`):
- Stateless beyond the `Db` handle and a few identity strings.
- `GetFinalSlot` looks up `cf_slot`; if the slot isn't `Final` locally it
  returns `final_known = false`. Otherwise
  `build_final_slot_response_from_state` walks the primary CFs to collect the
  parts the caller asked for.
- `StreamFinalSlots` iterates `iter_slots_desc(seed = to, limit)` on a
  blocking thread (`tokio::task::spawn_blocking`) because RocksDB iterators
  are synchronous; each FINAL slot is assembled and sent over an
  `mpsc::channel(16)` exposed as a `ReceiverStream`.

Peer client pool (`src/peer/client.rs`):
- `PeerPool` wraps `Vec<Arc<PeerHandle>>` and an `expected_network` guard.
- Each `PeerHandle` keeps a lazy `PeerClient<Channel>` (`tonic`) behind a
  `Mutex<Option<…>>`. `Endpoint` is configured with `connect_timeout=5s`,
  `timeout=10s`, and TCP keepalive. On *any* error the handle drops the
  cached client so the next call reconnects.
- `health()` caches the last `HealthResponse` for 30 s to avoid a round-trip
  per backfill RPC.
- `fetch_final_slot(period, thread, parts)` walks the peers in a cheap
  deterministic-ish shuffle (xorshift from the wall clock) and returns the
  first `final_known == true` response. Peers whose `health().network` does
  not match `expected_network` are dropped for the remainder of the call.

### 3.4 Backfill worker — one worker, one loop

`src/peer/backfill.rs`. The worker is idle-friendly and never runs unless
there is at least one peer configured. There is exactly **one** backfill
task per indexer; it does the entire job (gap detection, fetch, apply
trigger) in a single backwards sweep.

Config (`BackfillConfig`, projected from `config.peer.*` and
`config.streams`):

- `rate_limit` (default 50 ms) — pause inserted **only after a peer
  RPC**. The skip path (slot already complete-FINAL, or speculative)
  is **not** throttled; it walks at full RocksDB iteration speed.
- `wrap_pause` (default 30 s) — pause after a sweep reaches `(0, 0)`,
  before starting a fresh sweep from the new head.
- `idle_pause` (default 5 s) — pause when the indexer hasn't seen its
  first FINAL slot yet (we don't have a head to walk from).
- `parts: FinalSlotParts` — bitmask of stream parts the worker is
  allowed to request. Mirrors `expected_streams` in production.
- `expected_streams: StreamsExpected` — projects `[streams]`; governs
  when a slot counts as "complete" from the backfill's point of view
  (a disabled stream is treated as "not expected", so its bit doesn't
  need to be set for the slot to be considered covered).
- `range_periods` (default 16) / `range_limit` (default 512) /
  `range_sparse_threshold` (default 24) / `apply_pause` (default 2 ms)
  — bulk range path tunables, see below.

Main loop (`run_backfill`):

```text
loop:
    head = db.last_final_slot().period
    if head == 0 and !any_final_yet:
        sleep(idle_pause)
        continue
    'sweep: for period in (head .. 0).rev():
        for thread in (0 .. thread_count):
            walk_slot(db, pool, tx, period, thread, cfg)   # skip or 1 RPC
            if tx.is_closed(): break 'sweep
    sleep(wrap_pause)   # then resume from the new (possibly larger) head
```

Per-slot decision (`walk_slot`):

1. Read the local `SlotState`.
2. If `status == Final` and every expected-stream completeness bit is
   set → **skip**. This is the steady-state path once coverage is
   dense; it costs one RocksDB `get` and a couple of bool checks.
3. If `status == Candidate` → **skip**. The live ingest path owns
   settling candidate rows; back-filling them would race with the
   stream.
4. Otherwise compute `missing_parts(state, expected, requestable)`:
   - `Unknown` row → request every enabled part;
   - `Final` row with at least one stream-part bit still `false` →
     request just those, masked by `expected`.
5. Call `pool.fetch_final_slot(period, thread, parts)`:
   - `Ok(Some(resp))` → push `Event::PeerPatch(Box::new(resp))` into
     the ingest channel. That's the ONLY way peer data reaches the
     DB, which preserves the single-writer invariant.
   - `Ok(None)` → no peer has FINAL for this slot yet; we **write
     nothing** locally (no "tried, no source" bookkeeping) and move on.
     The next sweep retries.
   - `Err(e)` → peer error already logged in the pool; move on.
6. Sleep `rate_limit`. Skip paths (steps 2 / 3) do **not** consume the
   pacing budget.

Cost: once the chain is fully covered, a complete sweep is essentially
a sequential `cf_slot` iteration with no RPCs. At ~5 µs per RocksDB
`get`, the entire 145 M-slot mainnet history re-checks in roughly
twelve minutes — perfectly fine for a background worker on the
production hardware. While the cluster is still converging, the
`rate_limit` knob caps RPC pressure on peers.

**Bulk range path.** The walker actually scans `range_periods`-period
windows locally before issuing RPCs. A densely-missing window
(≥ `range_sparse_threshold` slots) is pulled with a single
`StreamFinalSlots` range call per logical peer instead of one
round-trip per slot, turning deep-history catch-up from ~10 slots/s
into hundreds per second. Only slots the local DB misses are applied;
everything else in the stream is discarded without a write. Each
apply sleeps `apply_pause` so bulk catch-up cannot starve the live
ingest channel it shares with the node stream. Sparse windows and
small bulk leftovers (e.g. slots only reachable through a
session-only peer — SyncSession cannot carry range streams) still go
per-slot. Windows nobody can supply cost one or two instantly-empty
streams per sweep instead of hundreds of throttled per-slot probes.
Rolling upgrades are safe in any order because `StreamFinalSlots` has
been served since the first peer-protocol release; only the client
side changed.

What this design replaces (folded into this one walker):

- a newest-first scanner that only revisited rows already in `cf_slot`
  and so could never reach genuine gaps;
- a dedicated history filler that walked `lowest_known_period - 1`
  downward and could outrun its data source, leaving permanent
  "swiss-cheese" coverage;
- a runtime AWS-DDB fallback inside the regular scanner. AWS is now a
  separate **one-shot importer** (`src/legacy/oneshot.rs`, see §9 of
  `ARCHITECTURE.md`) launched at startup if explicitly enabled. The
  backfill loop is peers-only.

### 3.5 Applying a peer patch

`src/peer/patch.rs` (`apply_peer_patch`). Runs on the single-writer (ingest)
thread. Invariants:

- **Local data has priority.** A peer patch never overwrites an existing
  `execution_trail_hash` or a FINAL block body we already have.
- **Completeness only moves forward.** Bits that are already `true` in the
  local `SlotCompleteness` cause the matching apply step to be skipped.
- **Idempotent.** Re-applying the same response is a cheap no-op.

The apply order is:

1. **Status / trail hash.**
   - If local status was `< Final`, promote to `Final`, populate `is_miss`,
     `execution_trail_hash`, and `final_block_id`/`candidate_block_ids` from
     the response. Update `meta.last_final_slot` if this is newer.
   - If local status was already `Final`, compare trail hashes. Mismatch →
     log a `"peer trail-hash mismatch; keeping local FINAL"` warning, touch
     `last_updated_ts_ms`, and return early without applying any parts.

2. **Block body** (only if the slot isn't a miss and
   `!completeness.block_body_stored`). `apply_block_part` decodes the
   `block` message (a `StoredBlockPb`) into `StoredBlock` and writes it
   only if we don't already have that block id. Then walks `operations[]`
   and `endorsements[]`: each op is written **only if it doesn't exist
   locally** — this keeps a locally-observed candidate status from being
   regressed by a peer's view. `write_endorsement` is first-write-wins on
   its own, so it's safe to replay. Finally walks
   `block.denunciations[]` and calls `db.write_denunciation` for each,
   reusing the `ingest::denunciation_{hash,slot,kind_label,target_address}`
   helpers so the row (and `idx_denunciation_by_addr` /
   `idx_denunciation_recent` entries) match what the live ingest would
   have produced.

3. **Exec output** (if the caller requested it and
   `!completeness.exec_output_final`). `apply_exec_part` clears the slot's
   SC-event set and rewrites it from the peer's list (the FINAL view is
   authoritative for a FINAL slot). `executed_op_ids` and `sc_event_count`
   are copied over if missing. Every `resp.async_msgs[]` row is decoded
   with `codec::async_msg_from_peer_pb` and passed to
   `db.write_async_msg`, which performs the upsert and maintains both
   the per-address indexes and the new `idx_async_by_last_slot`.

4. **Transfers** (if the response carries any transfers and
   `!completeness.transfers_stored`). `apply_transfers_part` clears the
   slot's transfers and rewrites them from the peer's list, keeping the
   secondary indexes in sync via `write_transfer`'s batched insert.
   Every `resp.deferred_calls[]` row is decoded with
   `codec::deferred_call_from_peer_pb` and written via
   `db.write_deferred_call`, which maintains
   `idx_deferred_by_{sender,target,last_slot}`. After the list is
   rewritten we re-run the extracted free functions
   `reconcile_async_from_transfers` and
   `reconcile_deferred_from_transfers` against the new transfer set —
   the same routines the live ingest path uses — so async and deferred
   lifecycles derived from transfer sightings converge on whatever
   state the live path would have produced.

5. **Finality propagation.** If we now have both `block_body_stored` and
   `exec_output_final`, walk `candidate_block_ids[]` and mark the winner
   `BlockStatus::Final` and every other candidate `BlockStatus::Discarded`
   (same logic as the node-stream path).

6. **Parent-gap cascade.** Call `ensure_parent_stub(prev_period, thread)`.
   If that slot doesn't have a row yet, insert an `Unknown` stub. The
   scanner will see it on the next pass and fire another `GetFinalSlot`.
   A chain of `N` missing slots heals in `O(N)` scanner cycles.

7. **SSE.** Broadcast `SlotSseEvent::SlotUpdated(state)` so `/v1/stream/slots`
   subscribers see the change as if it had come from the live node stream.

### 3.6 End-to-end liveness / catch-up sequence

The "happy path" for a freshly-restarted indexer with peers configured and
all four streams enabled:

1. Node streams reconnect. The gRPC tasks re-subscribe and start pushing
   `Event::Block` / `Event::Exec` / `Event::Transfers` for the current
   slot region.
2. On the first FINAL frame for a new slot `(p, t)`, the ingest worker
   writes the `SlotState` row, sets `completeness.exec_output_final = true`,
   updates `meta.last_final_slot`, and calls `ensure_parent_stub(p, t)`.
3. `ensure_parent_stub` inserts a blank `Unknown` row for `(p-1, t)` unless
   one already exists.
4. The backfill scanner — which walks every slot from `head` down to
   `(0, 0)` once per sweep — visits `(p-1, t)`, sees it as `Unknown`,
   and `missing_parts` requests every enabled part for it. The peer
   pool picks a peer (round-robin) and returns a `FinalSlotResponse`.
5. The response is shipped through the ingest channel as `Event::PeerPatch`
   and applied on the single-writer thread. That promotes the stub to
   `Final`, writes the block body / ops / events / transfers, and in turn
   calls `ensure_parent_stub(p-2, t)`.
6. The cascade continues one slot at a time per thread. Even without
   the parent-stub heuristic the next sweep would visit `(p-2, t)`,
   `(p-3, t)`, … in order anyway — the stubs just make the cascade
   visible to the live scanner sooner. Slots that no peer can supply
   (`final_known = false`) get nothing written locally and are simply
   retried on the next sweep, possibly against a different peer.

### 3.7 Observability

`/v1/backfill/status` returns `incomplete_slots` (computed by
`Db::count_incomplete_slots(&StreamsExpected)`, which uses the same
`SlotCompleteness::is_complete` the ingest logic uses) and `peers[]` with
`name`, `url`, last health snapshot, and last-seen timestamp.

Metrics (`/v1/metrics`):
- `massa_indexer_backfill_passes_total` — full scanner passes.
- `massa_indexer_backfill_rpcs_total` — peer RPCs issued by the scanner.
- `massa_indexer_backfill_slots_filled_total` — FINAL responses we
  successfully applied.
- `massa_indexer_backfill_range_streams_total` — bulk `StreamFinalSlots`
  range calls issued (subset of `rpcs_total`).
- `massa_indexer_ingest_{blocks,exec_outputs,transfers,peer_patches}_total`
  — one counter per event kind the ingest worker consumed.

### 3.8 Resilience guarantees

- **A stream disabled at runtime does not stall backfill.** `StreamsExpected`
  propagates through both `SlotCompleteness::is_complete` and
  `missing_parts`, so a slot whose only "missing" bit is for a disabled
  stream is considered complete and stops being requeued.
- **A stream disabled in the past and re-enabled later** is handled
  gracefully: because the backfill worker always requests only the bits that
  are *currently* enabled AND *currently* missing, flipping a stream back on
  causes every already-stored slot whose matching bit is still `false` to be
  picked up on the next scan.
- **Peer-network mismatches** fail closed at `GetHealth` time; those peers
  are skipped for the remainder of the backfill call. A separate warning is
  logged so operators notice quickly.
- **Peer unavailability is never fatal.** Every peer RPC is wrapped in a
  per-call timeout (10 s); on any error we drop the cached `PeerClient`
  channel so the next call reconnects.
- **Local corruption is preferred over peer corruption.** Trail-hash
  mismatches keep the local row; peer data never silently overwrites a
  field that has a valid local value.

## 4. Frontend posture: read-only

The explorer is strictly a viewer. It consumes the indexer's REST +
SSE surface and renders chain state; it does not build, sign or
broadcast operations, it has no wallet integration, and it holds no
keys. The whole production bundle stays around ≈ 300 kB gzipped with a
flat dependency tree (`react`, `react-router-dom`, `@tanstack/react-query`,
`dayjs`, `react-helmet-async`) — no `@massalabs/massa-web3`, no
`@massalabs/wallet-provider`, no Node-built-in polyfills.

Rationale:

- **Scope.** Reconstructing a fully-trustable history from the node's
  streams is already a non-trivial amount of moving parts (§3 of this
  doc). Mixing a signing surface into the same app doubles the attack
  surface without adding anything the wallets don't already do better
  — MassaStation, Bearby and the browser extensions all ship their own
  operation builders and speak `SendOperations` to their own node.
- **Bundle cost.** `massa-web3` + `wallet-provider` + the Node polyfills
  they require (`buffer`, `crypto`, `stream`, `events`, `util`) are
  roughly 2 MB of minified JS, much of it only useful to the ~2 % of
  users who actually want to sign something. Keeping the explorer
  read-only means every visitor pays only for the pages they open.
- **Decoupling.** A viewer that never calls `SendOperations` doesn't
  need a node URL at all — only an indexer URL — which simplifies the
  per-session endpoint failover story (§12.1 of the spec). Live state
  the explorer does need (balance, rolls, deferred credits, MNS
  resolution) is served by two thin proxy endpoints on the indexer
  side; see §5 below for the rationale.

Users who want to submit a transaction, buy/sell rolls, or call/deploy
a contract do so from their wallet against their own node. Once the
operation reaches the chain, the explorer picks it up through the
indexer's `NewOperations` stream and renders it on `/op/{id}` like any
other op.

## 5. Narrow, curated node RPC passthrough

The indexer is **not** a generic node-RPC proxy. There is no
`src/proxy.rs`, no `/v1/node/*` routes, no `SendOperations` /
`GetDatastoreEntries` / `GetStakers` / `GetSelectorDraws` forwarding,
no `/v1/draws`, `/v1/stakers/{addr}` or `/v1/cycles/{cycle}`. Anything
that would re-implement a generic node entrypoint stays out.

That said, three carefully scoped endpoints proxy a single live-state
call each, because the data they serve is purely live (not derivable
from any RocksDB row) and the explorer needs it on a single
round-trip from a static asset host:

| Endpoint                                  | Underlying node call              | Surfaces                                                                                                                              |
| ----------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /v1/addresses/{addr}/node_state`     | `QueryState` + `GetStatus`        | final/candidate balance (nMAS, decimal string), final/candidate/active rolls, deferred credits (final + candidate) by ascending slot. |
| `GET /v1/addresses/{addr}/bytecode`       | `QueryState(AddressBytecodeFinal)`| Raw WASM bytecode of an `AS…` smart-contract address, streamed verbatim with `Content-Type: application/wasm`. 404 on EOA / empty.    |
| `GET /v1/mns/resolve?name=<label>`        | `ExecuteReadOnlyCall` on MNS SC   | Resolves a `.massa` label to a Massa `AU…`/`AS…` address. 404 on miss.                                                                |

Concretely, the three helpers live in `src/grpc.rs`
(`fetch_address_state`, `fetch_address_bytecode`, `mns_resolve`); the
REST handlers in `src/rest.rs` are thin wrappers that call them on
demand. Together they add ~180 lines of glue.

Why these three and only these three:

- **`node_state`** — the explorer's address page wants live balance,
  active rolls and deferred credits next to historical activity. The
  alternative (point the browser at a node URL too) re-introduces the
  "two transports, two failure modes" problem the indexer-only frontend
  is designed to avoid. Proxying *just* this call lets the address page
  stay a single-origin React app.
- **`bytecode`** — for `AS…` addresses the explorer surfaces a
  "Download .wasm" button and runs an in-browser WASM analysis
  (sections, imports, exports, globals, data-segment strings, …) on
  the same payload (see §11 below). The bytecode is not derivable from
  any historical RocksDB row (it lives on the live ledger) and is too
  large to mirror in a history index, so we proxy a single
  `AddressBytecodeFinal` `QueryState` call on demand. EOA addresses
  are short-circuited to 404 before the node call is even made — no
  contract can live there by construction.
- **`mns_resolve`** — the search bar accepts `*.massa` labels. The MNS
  registry is a smart contract; we don't index its datastore (§16.7 of
  the spec), so the search handler issues a read-only `dnsResolve`
  call against the on-chain MNS contract on demand. The mainnet
  (`AS1q5hUf…AgLpUGT`) and buildnet (`AS12qKAV…rPdP1sj3G`) contract
  addresses are hard-coded; the call picks the right one based on the
  node's reported `chain_id`.

Why we still didn't bring back the old generic proxy:

- **Cache coherency.** The proxy carried a 1.5 s TTL LRU over the
  node's responses. Every cached entry had to be invalidated against
  the latest `final_cursor` / `candidate_cursor`, and every uncached
  entry still paid the extra hop. The two endpoints we kept skip the
  cache entirely — they call the node directly on every request, so
  the explorer's data is as fresh as the node itself.
- **Adapter complexity.** `QueryState`, `ExecuteReadOnlyCall` and
  friends take tagged-union requests with dozens of variants. Writing
  a faithful JSON↔proto adapter for the whole surface delivered no
  unique value over a light client talking to the same node. The two
  endpoints we kept hand-code only the variants they need
  (`AddressBalance{Final,Candidate}`, `AddressRolls*`,
  `AddressDeferredCredits*`, `CycleInfos`, and one `FunctionCall`).
- **Smaller attack + ops surface.** No `SendOperations` path means the
  indexer still cannot forward bytes *into* a node, only out of it.
  Both proxied endpoints are strict GETs that bottom out in read-only
  node RPCs.

Everything in the explorer that is not balance/rolls/deferred-credits,
MNS lookup or smart-contract bytecode is served from RocksDB, exactly
as before.

## 5.1 In-browser WASM analysis on the SC address page

The `bytecode` proxy is just one half of a deliberate split: the
explorer downloads the WASM **once** and does its analysis right in
the browser. The parser is hand-rolled in
`massa-explorer/src/lib/wasm.ts` (~430 LOC, zero dependencies) and
covers the full WASM MVP section table plus the parts of the
post-MVP spec that contracts in the wild actually emit:

- **Header.** `\0asm` magic + 32-bit version. Bad magic surfaces a
  warning instead of crashing — some test contracts ship a header
  followed by a concatenated trailer, and the "size + SHA-256" panel
  stays useful in that case.
- **Section walk.** Every section (well-known 1..12 plus any number
  of custom sections, id 0) is recorded with its id, name and body
  size. The "section table" panel renders this verbatim so the
  reviewer can spot anomalies (e.g. an oversized `code` section
  relative to total size).
- **Types, imports, exports, memory/table types, globals.** Each
  section is decoded against the official binary grammar; init
  expressions for globals are skipped through to the `0x0b` opcode
  with arity-aware advances for the common constants (`i32.const`,
  `i64.const`, `f32.const`, `f64.const`, `global.get`,
  `ref.null|ref.func`) and a "scan-until-`end`" fallback for anything
  exotic.
- **Custom `name` section.** Subsections 0 (module name), 1
  (functions), 4 (types) and 7 (globals) are pulled into a name map
  that's used to embellish imports/exports with their developer-
  facing identifiers.
- **Data segments.** Both the segment count and total byte payload
  are reported. Each segment's bytes are scanned for runs of ≥4
  printable ASCII characters, and the first 256 runs are surfaced as
  a "Data strings" panel — a fast way to spot embedded datastore
  prefixes, ABI keys, log templates, or third-party SDK strings.

Why this split:

- **The bytecode is too large for a history index.** A typical
  contract on mainnet is 10–80 KiB; mirroring every published
  contract's WASM into RocksDB would balloon the indexer for data
  the user only wants on the rare contract-detail page visit.
- **The analysis must not need a second backend.** No "wasm-disasm"
  service to deploy, no shared mutable state, no rate limits. Once
  the browser has the bytes, every tab is offline-capable.
- **Deterministic from the local bytes alone.** The SHA-256 the page
  prints is computed in-browser on the downloaded buffer, so the
  user can verify it matches what the "Download .wasm" button gives
  them — there is no opportunity for the explorer to substitute
  different bytes between analysis and download.

The companion REST endpoint is the only place that *talks* to the
node about bytecode; once the bytes are in the browser everything
else is offline.

## 5.2 EOA vs smart-contract address pages

Massa addresses come in two prefixes: `AU…` (externally-owned
account, derived from a keypair) and `AS…` (smart contract, created
by an `ExecuteSC` operation). The explorer's address page now adapts
its tab set to the prefix:

- **`AU…` (EOA)** — five tabs: Blocks produced, Operations sent,
  Operations targeting, Transfers, Deferred calls. Same layout as
  before this change.
- **`AS…` (smart contract)** — four tabs: Operations targeting,
  Transfers, Deferred calls, **Bytecode**. "Blocks produced" and
  "Operations sent" are hidden because a contract has no signing
  key, so those rows are structurally empty for it. The default
  landing tab is "Operations targeting", which is the first tab
  that's actually populated for a contract.

The split is a one-line predicate on the address prefix
(`addr.startsWith("AS")`); there is no protocol-level lookup. This
keeps the UI route stable across reloads — the tab set is decided
synchronously from the URL parameter — and avoids the surprise of
an empty "Blocks produced" tab on a contract.

## 6. Operational CLI commands

Besides `serve` (the default), `healthcheck`, `show-config` and `stats`
the binary ships five operational subcommands intended for operators
running an indexer in production: `verify`, `dump`, `peers`, `replay`
and `reindex-secondaries`. They live in `src/cli.rs` and are dispatched
from `src/main.rs`. Every command honours the global `--config PATH`
flag and exits non-zero on failure so it composes with Kubernetes jobs
or shell scripts.

Why build these commands at all:

- **`verify` and `reindex-secondaries` guard on-disk invariants.** The
  database stores every logical entity twice — once in a primary CF
  (e.g. `CF_BLOCK`, `CF_OP`, `CF_EVENT`, `CF_TRANSFER`,
  `CF_ENDORSEMENT`, `CF_ASYNC_MSG`, `CF_DEFERRED_CALL`) and once in
  one or more `IDX_*` CFs that give us cheap prefix scans by address,
  operation or slot (§2.3). A crash between the primary write and the
  index write, or a future code change that forgets an index, would
  otherwise silently hide rows from REST listings. `verify` is a
  read-only walk over every `IDX_*` CF: it parses the composite key
  and checks that the expected primary row exists. It exits 1 if any
  orphan is found and bounds the printed sample at `--max-issues`
  (default 100) while still reflecting the full count in the exit
  code. `reindex-secondaries` is the repair counterpart: with `--yes`
  it clears every `IDX_*` CF and re-plays the primary CFs through the
  normal `write_*` code path, which is the single authoritative place
  where indexes are derived. `SECONDARY_INDEX_CFS` in `src/db.rs` is
  the canonical list consulted by both commands, so adding a new
  index only requires updating that array and the corresponding
  `write_*` method.

- **`dump` is the last-resort RocksDB inspector.** When a bug report
  mentions a specific slot / operation / address, operators want to
  read the exact bytes the indexer stores without loading a second
  tool. `dump` emits NDJSON `{"key": <hex>, "value": <hex>}` for a
  chosen CF, with `--key` (point lookup), `--prefix` (filter),
  `--after` (cursor pagination mirroring the REST `next_cursor`) and
  `--limit` (default 100). `--cf list` prints every known CF name,
  which is also how we document the schema without duplicating the
  list in docs.

- **`peers` is a pre-flight check.** Peer gRPC has no authentication
  and binds to `127.0.0.1` by default (§16.6 of the spec). Before
  relying on a peer for backfill or replay, operators want a quick
  `name | url | status | network | latency` table. The command reads
  `[peer.peers.*]` from `indexer.toml`, calls `GetHealth` on every
  entry through the same per-peer client pool the running indexer
  would use, and exits 1 if any peer fails or the list is empty. It
  does not mutate the local DB.

- **`replay` bootstraps from peers without a node.** A fresh indexer
  with a slow/absent node but healthy peers should still be able to
  catch up. `replay --from P[:T] --to P[:T]` walks the slot range
  ascending, skipping slots that are already complete
  (`SlotCompleteness::is_complete`), fetches the rest via
  `PeerPool::fetch_final_slot` (§3.3 of the explanation) and funnels
  them through the *same* `apply_peer_patch` code path as live
  backfill. That means fork-trail conflicts keep the local copy,
  repeated runs are idempotent, secondary indexes are written
  atomically with the primary rows, and the SSE hub is fed via a
  dummy `SseHub` so the CLI doesn't leak events into a running
  server. `--no-block / --no-exec / --no-transfers` clear the
  matching `FinalSlotParts` flags for operators who only need a
  specific part of each slot; `--max-slots` (default 100 000) caps a
  single invocation so an operator typo can't schedule an infinite
  pull.

Implementation notes:

- **Read-only commands reuse the live code.** `verify` uses the same
  key codecs as the ingest/REST paths (`src/keys.rs`) and queries the
  primary CFs via `Db::raw_get`, so any change in layout breaks both
  tests and CLI at once.
- **Destructive commands require explicit confirmation.**
  `reindex-secondaries` needs `--yes`; without it the binary prints a
  message reminding the operator to stop any concurrent `serve` and
  exits 1. `replay` refuses to run with an empty peer list.
- **Tests mirror the production wiring.** `src/cli.rs::tests` covers
  the synchronous commands with in-memory `Db` instances and
  deliberate corruption (orphaned index entries, dropped primaries,
  missing CFs). `tests/cli_e2e.rs` boots an in-process "source"
  indexer listening on an ephemeral port, seeds it with a handful of
  final slots, and drives `cli::peers` / `cli::replay` against the
  live peer; the dead-peer and empty-peer-list paths assert we fail
  loudly instead of silently succeeding.
- **No new process model.** Every CLI command runs in the same
  `tokio::main` runtime as `serve`, reuses the same `Db::open`
  constructor (so `db.compression` / `db.write_buffer_size_mb` are
  honoured), and reuses the same `PeerPool` / `apply_peer_patch`
  stack. There is no second DB handle, no separate tracing setup,
  and no cached state that would drift out of sync with the server.
