# Massa Indexer V2 & Explorer — Specification

> **Guiding principle:** the simplest solution that meets the requirements.
> Prefer fewer moving parts, fewer stored variants, fewer code paths.
> Robustness comes from idempotency + doing less, not from elaborate
> machinery.

This document is the canonical reference for what the indexer and explorer
**are**. Everything described in §1–§16 is shipped in the tree and covered
by tests. §17 lists the outstanding work.

---

## 1. High-level goals

1. Replace the legacy Node.js explorer + AWS infra with a self-hosted Rust
   indexer that stores **all history forever** in local RocksDB.
2. Run **3 redundant indexers**, each pointing at its own Massa node. Any
   1–2 may die; the survivors keep serving and backfill recovering peers.
3. Handle Massa specifics correctly: per-thread parent graph, block
   misses, autonomous state changes (deferred calls, deferred credits,
   async messages, denunciations, rewards), speculative vs final
   execution, forks and re-execution.
4. Expose a **public REST + SSE API** usable by the explorer and by any
   other service.
5. Ship a **static React/Vite explorer** that is a strict superset of the
   legacy explorer's features and can be served from disk, DeWeb, or any
   CDN, with a mainnet/buildnet toggle.
6. No auth / TLS / rate-limiting inside the indexer — layered on via
   `nginx` in front.
7. Be **economic**: don't re-fetch or re-index what we already have and
   hasn't diverged.

---

## 2. Glossary & conventions

| Term              | Meaning                                                                                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Node**          | A Massa full node (from `massa/`) the indexer connects to. **Not bundled** — the operator points the indexer at any suitable node.                                |
| **Indexer**       | Rust process that streams from a node, stores normalized data in RocksDB, and exposes REST + peer gRPC.                                                           |
| **Peer-indexer**  | Another indexer of the trio, reachable over gRPC.                                                                                                                 |
| **Explorer**      | Static front-end (React + Vite) that consumes the indexer REST API. Runs and deploys separately.                                                                  |
| **Slot**          | `(period: u64, thread: u8 ∈ 0..32)`. Wall-clock: `genesis_timestamp + period · t0 + thread · (t0 / 32)`.                                                           |
| **Final**         | Slot has been declared final by some authoritative source (the local node's FINAL `SlotExecutionOutput`, or a peer's `FinalSlot` response). **First-final-wins.** |
| **Candidate**     | Latest speculative view of a slot. Ephemeral, last-writer-wins, never rolled back through a journal — simply overwritten by the next CANDIDATE event.             |
| **MAS / nMAS**    | 1 MAS = 10⁹ nMAS.                                                                                                                                                 |

**Serialization.** `prost` (protobuf) end-to-end: the same `*Pb` messages
defined in `massa-indexer/proto/indexer/v1/storage.proto` are both the
on-disk value format and the peer-protocol wire format. REST responses are
JSON (serde).

**Keys.** Raw fixed-length bytes. `Slot` keys are 9 bytes (`period_be(8) ‖
thread(1)`); ids (`BlockId`, `OperationId`, `EndorsementId`, `Address`) are
33 bytes (`category(1) ‖ 32-byte hash`) obtained by `bs58-check`-decoding
the Massa display string.

**Sort order.** "Newest-first by slot" is implemented by prepending
`rslot_key(p, t) = ¬slot_key(p, t)` so that forward lexicographic
iteration yields newest first.

**Single-network invariant.** A given indexer instance serves **exactly
one** network, pinned by `[general].network`. The indexer does not know
about other networks; its REST/gRPC responses stamp the configured
network, and any peer advertising a different network is dropped at
handshake time. Running mainnet and buildnet side by side means running
two independent processes with two independent RocksDB paths.

---

## 3. System topology

```
       ┌──────────────────────────────┐
       │  massa-explorer (static)     │   runs anywhere (DeWeb, CDN, disk)
       └──────────────┬───────────────┘
                      │ REST + SSE  (HTTP)
                      ▼
         [ optional nginx in front ]
                      │
  ┌───────────────────┴───────────────────────┐
  ▼                                           ▼
indexer A  ◀── peer gRPC (127.0.0.1+tunnel) ──▶  indexer B
  │                                           │
  │ gRPC to node                              │ gRPC to node
  ▼                                           ▼
node A (any reachable massa-node)           node B
```

- Indexers are **independent processes on independent hosts**; the node is
  **not bundled** with the indexer.
- Peer gRPC binds to **127.0.0.1**; cross-host peer traffic goes through
  operator-provided secure tunnels (SSH / WireGuard / nginx+mTLS). The
  indexer binary itself makes no transport-security assumptions.
- The explorer is a static build and deploys separately.

---

## 4. Repositories

Two independent git trees.

### 4.1 `massa-indexer/` (Rust)

**Single crate** (no workspace split — fewer moving parts). MSRV pinned to
`rustc 1.81.0`, matching `massa/rust-toolchain.toml`, so the vendored
`massa-proto` types stay compatible.

```
massa-indexer/
├── Cargo.toml
├── rust-toolchain.toml
├── Dockerfile                    # multi-stage, non-root, tini pid-1
├── build.rs                      # tonic-build over proto/
├── config/
│   ├── indexer.toml              # dev defaults
│   ├── indexer.mainnet.toml
│   ├── indexer.buildnet.toml
│   └── indexer.production.toml   # annotated production template
├── proto/
│   ├── massa/…                   # vendored from ../massa-proto
│   └── indexer/v1/{peer,storage}.proto
├── src/
│   ├── main.rs / lib.rs / server.rs / config.rs
│   ├── db.rs / codec.rs / keys.rs / ids.rs / model.rs
│   ├── grpc.rs                   # node-stream consumers
│   ├── ingest.rs                 # single-writer state machine
│   ├── peer/{service,client,patch,backfill}.rs
│   ├── rest.rs / sse.rs
│   ├── metrics.rs / openapi.rs
│   └── error.rs
└── tests/
    ├── peer_backfill.rs          # 12 multi-instance integration tests
    ├── backfill_worker.rs        # run_backfill end-to-end
    └── live_node.rs              # opt-in smoke against a running node
```

### 4.2 `massa-explorer/` (TypeScript)

Stack: **Vite 5 + React 18 + TS 5 + Tailwind 3 + react-router-dom 6 +
@tanstack/react-query + react-helmet-async + dayjs**.

```
massa-explorer/
├── package.json / vite.config.ts / tailwind.config.js
├── Dockerfile                    # nginx-unprivileged, runtime /config.js
├── public/
├── src/
│   ├── main.tsx / App.tsx / routes.tsx
│   ├── api/                      # failover client, SSE hook, typed fetch
│   ├── config/networks.ts        # bundled defaults per network
│   ├── components/               # SlotRef, SlotTimestamp, Paginator, …
│   ├── features/                 # home, block, slot, op, address, …
│   └── lib/                      # types (mirror model.rs), format helpers
└── tests/                        # vitest
```

---

## 5. What we index

### 5.1 Node streams consumed

| Stream                          | Node feature flag | Purpose                                                                                        |
| ------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------- |
| `NewFilledBlocksServer`         | default           | Live blocks (header + op bodies + embedded endorsements/denunciations).                        |
| `NewSlotExecutionOutputsServer` | default           | Per-slot execution outputs (CANDIDATE & FINAL), SC events, executed op ids, trail hash.        |
| `NewTransfersInfoServer`        | `execution-trace` | Per-slot transfer list (op transfers, ABI side-effects, rewards, slashes, storage costs) — FINAL only. |

ABI call stacks (`NewSlotABICallStacks`) are intentionally **not**
subscribed to: the indexer doesn't store or surface call-stack data at
all. The information we actually need (emitter/caller addresses, the
owning operation, the owning async message or deferred call) is already
present in the SC-event context and in the transfer rows, so we derive
emitters / callers / per-op groupings from those streams and skip the
extra (node-feature-gated, FINAL-only) payload.

The node must be built with `cargo build --release -p massa-node --features
execution-trace` and its `config.toml` must set `[grpc.public].enabled =
true` and `enable_broadcast = true`.

Streams reconnect automatically with exponential backoff (capped at 30 s).
`UNIMPLEMENTED` is treated as a signal that the node doesn't expose the
stream — the loop logs a single warning and keeps retrying.

**Per-stream toggles.** Each stream can be turned on or off independently
in `[streams]` (or via `INDEXER_STREAMS_*` env vars):

```toml
[streams]
filled_blocks          = true
slot_execution_outputs = true
transfers              = true
```

When a stream is disabled:

- `grpc.rs` never subscribes (zero bandwidth, no `UNIMPLEMENTED` churn).
- `SlotCompleteness::is_complete` treats the missing part as "not
  expected" — the slot is allowed to become complete on the remaining
  enabled streams.
- The backfill worker applies the same projection, so peers are never
  asked for a disabled part.

Turning a stream back on is a restart-safe operation; backfill fills the
retroactive gap from peers.

### 5.2 Narrow, curated node-RPC passthrough

The indexer is **not** a generic node-RPC proxy. The REST surface is
served from RocksDB; the indexer never exposes `SendOperations`,
`GetDatastoreEntries`, `GetStakers`, `GetSelectorDraws`, raw
`QueryState`, or any other node entrypoint that would re-implement what
already lives on a public node.

Two endpoints break that rule deliberately, because the data they serve
is purely live (not derivable from any historical row in RocksDB) and
the explorer needs it on a single round-trip from a static asset host:

| Endpoint                                  | Underlying node RPC               | Surfaces                                                                                                                       |
| ----------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `GET /v1/addresses/{addr}/node_state`     | `QueryState` + `GetStatus`        | final/candidate balance (nMAS, decimal string), final/candidate/active rolls, deferred credits (final + candidate) sorted asc. |
| `GET /v1/addresses/{addr}/bytecode`       | `QueryState(AddressBytecodeFinal)`| Raw WASM bytes of an `AS…` smart-contract address, streamed with `Content-Type: application/wasm`. 404 on EOA or empty entry.  |
| `GET /v1/mns/resolve?name=<label>`        | `ExecuteReadOnlyCall` on MNS SC   | Resolves a Massa Name Service label (with or without `.massa`) to a Massa `AU…`/`AS…` address. 404 on miss.                    |

These calls are pure one-shot reads issued straight to the local node
on demand — no slot-aware cache, no proto↔JSON adapter for inputs (the
client supplies a plain `addr` or `name`), and the response shape is a
small hand-written struct (or, for `bytecode`, the raw payload). They
cost roughly **180 lines** of glue total (`grpc::fetch_address_state`,
`grpc::mns_resolve`, `grpc::fetch_address_bytecode`), versus the ~550
lines of cache + adapter the old generic proxy required.

Why these three and only these three:

- **`node_state`** — the explorer's address page needs to show live
  balance / rolls / deferred credits next to historical activity.
  Forcing the browser to also know a node URL just to populate that
  panel would re-introduce the "two transports, two failure modes"
  problem the indexer-only frontend is designed to avoid (§12.1).
- **`bytecode`** — for `AS…` addresses the explorer surfaces a
  "download .wasm" button and runs an in-browser WASM analysis
  (sections, imports, exports, globals, data-segment strings, etc.)
  derived locally from the same payload. The raw bytecode is not
  derivable from any RocksDB row and is too large to keep in a
  history index, so we proxy it on demand.
- **`mns_resolve`** — the search bar accepts `*.massa` names. The MNS
  registry is a smart contract; we don't index its datastore (§16.7),
  so we issue a `dnsResolve` read-only call against the public registry
  on demand. Mainnet (`AS1q5hUf…AgLpUGT`) and buildnet
  (`AS12qKAV…rPdP1sj3G`) addresses are hard-coded; the call picks the
  right one based on the node's reported `chain_id`.

Everything else stays out: there is **no** `/v1/node/*` surface, no
`/v1/draws`, no `/v1/stakers/{addr}`, no `/v1/cycles/{cycle}`, no
datastore reads, no `SendOperations`. Wallets that need to submit
operations talk to a node directly.

---

## 6. Data model & RocksDB schema

One RocksDB instance per indexer. Opened via `Db::open` in `src/db.rs`
with:

- `create_if_missing = true`, `create_missing_column_families = true`
- **Default compression.** We don't set `compression_type`; upgrading the
  `rocksdb` crate picks up whatever the project settled on.
- `write_buffer_size = [db].write_buffer_size_mb × 1 MiB`
- `increase_parallelism(min(cpu, 8))`

All values are `prost`-encoded against the `*Pb` messages in
`proto/indexer/v1/storage.proto`. `src/codec.rs` owns the
`Stored* ⇄ *Pb` conversions and is unit-tested for round-trip equality.

### 6.1 Schema versioning

`cf_meta[row]` carries a `schema_version` byte (`MetaRowPb`). `Db::open`
writes the current `SCHEMA_VERSION` constant when the directory is empty
and refuses to mount a DB whose recorded version differs — the startup
error points operators at the reset procedure (§15).

This is a safety net, not a migration framework. Schema evolution is
**wipe-and-rebuild**: the indexer is a derived cache of node data and
re-ingestion converges at 32+ slots/s on commodity hardware.

### 6.2 Primary column families

Every `idx_*` CF below stores an **empty value**; the key *is* the data.

| CF                          | Key                                                   | Value / purpose                                                                                     |
| --------------------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `cf_meta`                   | well-known ascii (`row`, `last_final_slot`, `last_candidate_slot`) | `MetaRowPb` (schema version, network, build, chain params) + last-final/candidate slot rows. |
| `cf_slot`                   | `slot_key(p,t)` (9 B)                                 | `SlotStatePb` — status, miss flag, final/candidate block ids, trail hash, executed op ids, SC-event count, completeness bitmap, timestamps. |
| `cf_slot_candidate_block`   | `slot_key(9) ‖ block_id.key_bytes()(33)`              | Empty. Lets us enumerate every block ever seen at a slot.                                           |
| `cf_block`                  | `block_id.key_bytes()` (33 B)                         | `StoredBlockPb` (full archival): header, parents, op ids, endorsement ids, denunciations, raw b64 signed-header, status (`SeenCandidate` / `Final` / `Discarded`). |
| `cf_op`                     | `op_id.key_bytes()` (33 B)                            | `StoredOperationPb`: creator/target, kind, fee, expire, per-block `inclusions[]`, candidate+final exec status, kind-specific `details`, full raw signed-op b64. |
| `cf_endorsement`            | `endorsement_id.key_bytes()` (33 B)                   | `StoredEndorsementPb`: endorsed slot, index in block, creator, signature, including block id if known. |
| `cf_denunciation`           | SHA-256 of the canonical denunciation (32 B)          | `StoredDenunciationEntryPb`: kind, denounced address, full raw denunciation, including block.       |
| `cf_sc_event`               | `slot_key(9) ‖ index_in_slot_be(4)`                   | `StoredScEventPb`: slot, index, emitters, callers, optional op id, status, utf-8 data.              |
| `cf_transfer`               | `slot_key(9) ‖ index_in_slot_be(4)`                   | `StoredTransferPb`: slot, block id, op id (optional), from/to, amount, coin origin, status.         |
| `cf_async_msg`              | message id (33 B)                                     | `StoredAsyncMsgPb`: full async message (sender, dest, handler, coins, fee, validity window, data, trigger, state: `Pending` / `Executed` / `Cancelled` / `Consumed`). See §7.2.1. |
| `cf_deferred_call`          | call id (33 B)                                        | `StoredDeferredCallPb`: caller + target, coins, registered/target slots, state: `Registered` / `Executed` / `Failed` / `Cancelled`. Lifecycle is driven by transfers — see §7.2.2. |
| `cf_peer_state`             | peer name                                             | Peer observability (last seen, last health snapshot).                                               |

### 6.3 Secondary indexes

All use newest-first sort via the bitwise-negated slot key.

| Index CF                     | Key layout                                                                    | Notes                                               |
| ---------------------------- | ----------------------------------------------------------------------------- | --------------------------------------------------- |
| `idx_block_by_creator`       | `addr(33) ‖ rslot_key(9) ‖ block_id(33)`                                      |                                                     |
| `idx_op_by_creator`          | `addr(33) ‖ rslot_key(9) ‖ op_id(33)`                                         | `rslot` = first inclusion slot.                     |
| `idx_op_by_target`           | `addr(33) ‖ rslot_key(9) ‖ op_id(33)`                                         | Target-bearing ops only.                            |
| `idx_endorsement_by_creator` | `addr(33) ‖ rslot_key(9) ‖ endorsement_id(33)`                                |                                                     |
| `idx_transfer_by_addr`       | `addr(33) ‖ rslot_key(9) ‖ index_be(4) ‖ tag(1)`                              | `tag=1` sender, `tag=2` receiver.                   |
| `idx_transfer_by_op`         | `op_id(33) ‖ rslot_key(9) ‖ index_be(4)`                                      |                                                     |
| `idx_transfer_by_block`      | `block_id(33) ‖ rslot_key(9) ‖ index_be(4)`                                   |                                                     |
| `idx_denunciation_by_addr`   | `addr(33) ‖ rslot_key(9) ‖ hash(32)`                                          | Victim address only.                                |
| `idx_sc_event_by_emitter`    | `addr(33) ‖ rslot_key(9) ‖ index_be(4)`                                       | One row per emitter per event.                      |
| `idx_sc_event_by_caller`     | `addr(33) ‖ rslot_key(9) ‖ index_be(4)`                                       | One row per caller per event.                       |
| `idx_sc_event_by_op`         | `op_id(33) ‖ rslot_key(9) ‖ index_be(4)`                                      | Events attributed to an op.                         |
| `idx_async_by_sender`        | `sender(33) ‖ msg_id`                                                         |                                                     |
| `idx_async_by_dest`          | `dest(33) ‖ msg_id`                                                           |                                                     |
| `idx_async_by_last_slot`     | `rslot_key(9) ‖ msg_id`                                                       | Enumerate async msgs per slot (peer backfill §9).   |
| `idx_deferred_by_sender`     | `sender(33) ‖ call_id`                                                        |                                                     |
| `idx_deferred_by_target`     | `target(33) ‖ call_id`                                                        |                                                     |
| `idx_deferred_by_last_slot`  | `rslot_key(9) ‖ call_id`                                                      | Enumerate deferred calls per slot (peer backfill §9). |

The `idx_transfer_by_addr` tag byte keeps sender and receiver rows
distinct for self-transfers; the reader de-duplicates by
`(period, thread, index)` so the result list stays clean.

### 6.4 `SlotCompleteness`

A 4-bit bitmap on every slot, plus the `is_miss` flag:

- `block_body_stored` — from `NewFilledBlocksServer`.
- `exec_output_final` — from the FINAL `SlotExecutionOutput`.
- `exec_output_candidate` — latest CANDIDATE view (not required for
  completeness).
- `transfers_stored` — from `NewTransfersInfoServer`.

A slot is **complete** iff every **enabled** part (per `[streams]`) is
satisfied. `filled_blocks` is satisfied by `block_body_stored ∨ is_miss`;
the others by their corresponding bit. Disabled streams count as
satisfied. The backfill worker and the `is_complete` check both use this
projection.

---

## 7. Ingestion state machine

Single writer (`src/ingest.rs`). All RocksDB writes are funneled through
one `mpsc` queue so a slot transition is atomic. Every transition is
**idempotent**: re-applying the same event leaves the DB identical.

### 7.1 Event types

```rust
enum Event {
    Block(Box<FilledBlock>),                          // NewFilledBlocksServer
    Exec(Box<SlotExecutionOutput>),                   // NewSlotExecutionOutputs (CANDIDATE | FINAL)
    Transfers(Box<NewTransfersInfoServerResponse>),   // NewTransfersInfoServer   (FINAL)
    PeerPatch(Box<indexer::v1::FinalSlotResponse>),   // from the backfill worker (FINAL)
    Tick,                                             // periodic 5 s — SSE heartbeat, meta flush
}
```

### 7.2 Invariants

- **Never downgrade.** `SlotStatus::Final` never moves back to
  `Candidate`; `BlockStatus::Final` / `Discarded` never move back to
  `SeenCandidate`.
- **First-final-wins trail hash.** Once a slot is `Final`, a second FINAL
  frame with a different `execution_trail_hash` is logged and dropped.
  The conflict is counted in metrics but never applied.
- **Re-runs are no-ops.** Every write is either idempotent (`write_block`,
  `write_transfer`) or clear-then-rewrite (SC events / transfers on
  finalisation). Replaying the full stream history twice produces the
  same final DB state.
- **Parent-gap discovery.** `ensure_parent_stub` is called from
  `handle_block`, from the FINAL branch of `handle_exec`, and from
  `apply_peer_patch`. It inserts an `Unknown` `SlotState` for
  `(period-1, thread)` if and only if no row exists yet. The backfill
  scanner picks it up on the next pass.
- **Zero-value transfers dropped.** The SC runtime emits a `coin`
  transfer for every ABI call even when the caller attached 0 coins;
  indexing those would flood every view with empty rows. `is_zero_value`
  filters them at ingestion; the remaining transfers are densely
  renumbered starting at 0. Cross-slot stable identity is carried by
  `StoredTransfer.id`, not by `index_in_slot`.
- **Operation attribution.** ABI-level side-effects
  (`abi_transfer_coins`, storage costs, `abi_send_msg_coins`, …) arrive
  on the stream without an `operation_id`. The ingester walks each slot's
  transfer list top-to-bottom, remembers the most recent transfer with a
  node-assigned `operation_id`, and propagates it forward until a hard
  boundary (an `async_msg_id` / `deferred_call_id` / `denunciation_index`
  is set, or any reward origin — `block_reward`, `endorsement_reward`,
  `endorsed_reward`, `slash`, `deferred_credit` — clears it). This gives
  `/v1/operations/{id}/transfers` the full execution-trace view without
  data loss.
- **Block finality propagation.** The first FINAL `E_EXEC` for a slot
  rewrites every known candidate block at that slot to its terminal
  status: the winner (`slot.final_block_id`) → `Final`, every other
  candidate → `Discarded`. `GET /v1/blocks/{id}` and
  `GET /v1/addresses/{addr}/blocks` also self-heal lazily — if the
  stored row is still `SeenCandidate` but its parent slot is already
  `Final`, the handler rewrites it on read.

### 7.2.1 Async-pool ingestion (FINAL only)

For every FINAL `SlotExecutionOutput` we walk
`state_changes.async_pool_changes` and upsert `cf_async_msg` rows:

| Change   | Action on `StoredAsyncMsg`                                     |
|----------|----------------------------------------------------------------|
| `SET`    | Lift the embedded `AsyncMessage` into a fresh row, `state = Pending`. `first_seen_ts_ms` captured here. |
| `UPDATE` | Read the existing row (or mint a stub if we joined the stream mid-life), apply every `SetOrKeep*` field in place, refresh `last_slot`. Field-by-field merge, never `Keep` clobbers data. |
| `DELETE` | Mark terminal. The state becomes `Consumed` unless a transfer has already promoted it to `Executed` / `Cancelled`. |

The secondary indexes `idx_async_by_sender` / `idx_async_by_dest` are
rewritten on every write. If an `UPDATE` changes `sender` or
`destination`, the old index entry is deleted in the same `WriteBatch`.

Transfers for the same slot then refine the `Consumed` verdict:

| `CoinOrigin` on transfer with `async_msg_id` set | Resulting state |
|--------------------------------------------------|-----------------|
| `AsyncMsgCoins`                                  | `Executed`       |
| `AsyncMsgCancel`                                 | `Cancelled`      |

We never downgrade a terminal state. Messages observed only through
transfers (no prior `SET`) are also minted as minimal rows so
`/v1/async/{id}` and the `from` columns of the transfer table always
resolve.

### 7.2.2 Deferred-call ingestion

The node's `state_changes` proto does **not** expose a
`deferred_calls_changes` field, so we derive the lifecycle entirely
from the transfer stream. For each transfer carrying a
`deferred_call_id`:

| `CoinOrigin`                 | Action on `StoredDeferredCall`                              |
|------------------------------|-------------------------------------------------------------|
| `DeferredCallRegister`       | State → `Registered`; record `registered_slot`, `sender`.   |
| `DeferredCallCoins`          | State → `Executed`; record `target_slot`, `target_address`. |
| `DeferredCallFail`           | State → `Failed` (terminal).                                |
| `DeferredCallCancel`         | State → `Cancelled` (terminal).                             |
| `DeferredCallStorageRefund`  | Touch `last_slot` / `last_updated_ts_ms`; never changes state. |

Terminal states (`Executed` / `Failed` / `Cancelled`) are sticky — a
subsequent transfer in the same slot cannot downgrade them. Both
primary-key writes and the `idx_deferred_by_sender` /
`idx_deferred_by_target` writes go through the same upsert path as
async, so address changes tidy up the old index entries.

### 7.3 Crash recovery

On startup the indexer opens RocksDB, reads `last_final_slot` and
`last_candidate_slot` from `cf_meta`, reconnects every enabled stream,
and launches the backfill worker. The node re-sends from its current tip;
duplicates are harmless because transitions are idempotent and
`SlotCompleteness` short-circuits already-stored parts. **No reindex on
reboot.** The only thing we re-fetch is what the completeness bitmap says
is missing.

---

## 8. Finality, forks, and GC

### 8.1 First-final-wins

The first FINAL verdict for a slot — from either the local node or a peer
— is committed and never overwritten by a later FINAL with a different
`execution_trail_hash`. A disagreeing FINAL is logged (`WARN`) and
counted in metrics. Rationale: any FINAL source is already authoritative
per Massa's consensus finality; if two disagree, flipping state helps no
one, and operators can inspect the divergence log.

Partial data is acceptable: a slot can be marked FINAL before its
transfers arrive; those parts are filled later and tracked by
`SlotCompleteness`.

### 8.2 Block lifecycle

- `SeenCandidate` — body known, not yet promoted.
- `Final` — block referenced by the FINAL exec output at its slot.
- `Discarded` — at finalisation time, every non-winning candidate at the
  slot is demoted and eligible for GC.

### 8.3 Candidate rewinds — we don't do them

Speculative re-execution of a slot simply produces a new
`E_EXEC(CANDIDATE)` event; we overwrite the per-slot candidate view in
place. No journal, no reverse-walk, no compensating deletes. Async-pool
and deferred-call state are **FINAL-only**, so they never need
rewinding.

### 8.4 GC rules

- **Block GC** at FINAL time of slot S: every candidate block at S that
  isn't the winner is removed from `cf_block`, `idx_block_by_creator`,
  and `cf_slot_candidate_block`. A lightweight sweep also picks up
  orphaned `SeenCandidate` rows at slots now known FINAL.
- **Op GC**: an op is GC'd iff it is not in any live block **and** its
  `expire_period ≤ last_final_slot.period` **and** it was never executed.
- **Async / deferred**: archival — we keep tombstones with
  `deleted_at_slot` + success/fail markers instead of deleting rows.

---

## 9. Peer protocol & backfill

Every indexer exposes the `massa.indexer.v1.Peer` gRPC service
(`src/peer/service.rs`). When `[peer].enabled = false` no port is bound
and the backfill worker is skipped.

### 9.1 Three RPCs

```proto
service Peer {
  rpc GetHealth(HealthRequest) returns (HealthResponse);
  rpc GetFinalSlot(FinalSlotRequest) returns (FinalSlotResponse);
  rpc StreamFinalSlots(StreamFinalSlotsRequest) returns (stream FinalSlotResponse);
}
```

- `GetHealth` — cheap identity + liveness probe. Returns peer id,
  network, build version, highest-known FINAL slot.
- `GetFinalSlot(slot, parts)` — per-slot fetch. `FinalSlotParts` is a
  bitmask (`block`, `exec_output`, `transfers`) and the peer answers
  only with the parts it has. Missing FINAL → `final_known = false`.
- `StreamFinalSlots(from, to, parts)` — server-streaming bulk fetch over
  a range, newest-first, capped at `stream_limit_cap` (default 512).

`FinalSlotResponse` carries its per-part payloads as the very same
`StoredBlockPb` / `StoredOperationPb` / `StoredEndorsementPb` /
`StoredScEventPb` / `StoredTransferPb` / `StoredAsyncMsgPb` /
`StoredDeferredCallPb` messages that live in RocksDB. One schema covers
both surfaces. Per part:

- `block` — `StoredBlockPb` + the operations / endorsements referenced
  by the header. Denunciations travel inside `StoredBlockPb` and the
  receiver expands them into `cf_denunciation` rows on apply.
- `exec_output` — `executed_op_ids`, `sc_events`, **and** every
  `StoredAsyncMsg` whose `last_slot` equals the requested slot (peer
  service enumerates via `idx_async_by_last_slot`).
- `transfers` — `StoredTransferPb` rows **and** every
  `StoredDeferredCall` whose `last_slot` equals the requested slot (via
  `idx_deferred_by_last_slot`). Async and deferred rows are *also*
  re-derived from the transfer list on the receiver by re-running the
  same `reconcile_{async,deferred}_from_transfers` routines the live
  ingest path uses, so the backfilled state converges on whatever the
  live path would have produced.

### 9.2 Backfill worker (`src/peer/backfill.rs`) — one worker, one loop

Runs on its own task. Idle-friendly; never runs unless at least one peer
is configured.

The worker performs a single **backwards sweep** of the slot space, from
`last_final_slot.period` (set by the live ingest every time a slot
finalises) all the way to `(0, 0)`. For every `(period, thread)` it
visits:

- the row is FINAL and every expected stream-part bit is set ⇒ skip
  (one RocksDB `get` + a few bool checks; no RPC);
- `SlotStatus::Candidate` ⇒ skip (the live stream owns settling
  candidate rows);
- otherwise ⇒ ask peers (round-robin) for the missing parts, masked by
  the operator's `StreamsExpected` (a locally-disabled stream is never
  requested). On hit the response is shipped as `Event::PeerPatch`. **If
  no peer can supply the slot, write nothing and move on** — the next
  sweep retries. There is no per-slot "tried, no source" bookkeeping.

```
loop:
    head = db.last_final_slot().period   # may be 0 at startup
    if head == 0 and !genesis_ready:
        sleep(idle_pause)                # wait for the live stream
        continue
    for period in (head .. 0).rev():
        for thread in (0 .. thread_count):
            walk_slot(period, thread)    # skip or one RPC
    sleep(wrap_pause)                    # then resume from the new head
```

Pacing is a single knob: `rate_limit` is the inter-RPC sleep
(default 50 ms). The skip path is **not** throttled — once coverage is
dense a complete sweep collapses to a sequential `cf_slot` iteration
(~5 µs per `get`), so the whole 145 M-slot mainnet history re-checks in
roughly twelve minutes.

A miss (`is_miss = true`) is a perfectly valid FINAL state and is
treated as covered on every subsequent sweep. The peer protocol
distinguishes "no block in this slot" from "I don't know this slot"
(`final_known = false`); only the latter triggers a retry.

**What this design replaces.** Three earlier mechanisms collapsed into
this single walker:

- a newest-first scanner that only revisited rows already in `cf_slot`
  and so could never reach genuine gaps;
- a dedicated history filler that walked `lowest_known_period - 1`
  downward and could outrun its data source, leaving permanent
  "swiss-cheese" coverage;
- a runtime AWS-DDB fallback inside the regular scanner — AWS is now a
  separate one-shot importer (§9 in `explanation.md` and the
  ARCHITECTURE doc), not a per-RPC fallback in the live loop.

### 9.3 Applying a peer patch (`src/peer/patch.rs`)

Runs on the single-writer (ingest) thread. Invariants:

- **Local data has priority.** A peer patch never overwrites an existing
  `execution_trail_hash` or a FINAL block body we already have.
- **Completeness only moves forward.** Bits already `true` locally cause
  the matching apply step to be skipped.
- **Idempotent.** Re-applying the same response is a cheap no-op.

Order:

1. **Status / trail hash.** If local status is `< Final`, promote to
   `Final` and populate `is_miss` / `execution_trail_hash` /
   `final_block_id` from the response. If already `Final` with a
   different trail hash, log and return early (the parts are not applied).
2. **Block body** (if `!is_miss && !block_body_stored`). Decode the
   `StoredBlockPb`; write the block only if we don't already have that
   id. Walk `operations[]` / `endorsements[]` and write rows only for
   ids we don't already have. Expand `block.denunciations[]` into
   `cf_denunciation` rows (with `idx_denunciation_by_addr` /
   `idx_denunciation_recent` updated) so denunciations backfill even
   though they are not a standalone part on the wire.
3. **Exec output** (if requested and `!exec_output_final`). Clear the
   slot's SC-event set and rewrite it from the peer's list; copy
   `executed_op_ids` / `sc_event_count` if missing. Upsert every
   `async_msgs[]` row verbatim; `write_async_msg` maintains
   `idx_async_by_last_slot` and the per-addr indexes.
4. **Transfers** (if requested and `!transfers_stored`). Clear the slot's
   transfers and rewrite them from the peer's list. Upsert every
   `deferred_calls[]` row, then re-run
   `reconcile_async_from_transfers` and
   `reconcile_deferred_from_transfers` against the new transfer list so
   async / deferred lifecycles derived from transfer sightings match
   what the live ingest would have produced.
5. **Finality propagation** (if the slot is now fully stored). Walk
   `candidate_block_ids[]` and mark winner / losers.
6. **Parent-gap cascade.** `ensure_parent_stub(prev_period, thread)` —
   inserts an `Unknown` stub for the previous slot if needed. Chains of
   N missing slots heal in O(N) scanner cycles.
7. **SSE.** Broadcast `SlotSseEvent::SlotUpdated(state)` so live
   subscribers see the change.

### 9.4 Resilience guarantees

- Disabled streams do not stall backfill — `StreamsExpected` propagates
  through both `is_complete` and `missing_parts`.
- Re-enabling a stream mid-flight is automatic: already-stored slots
  whose matching bit is still false get picked up on the next scan.
- Peer-network mismatch fails closed at `GetHealth` time and logs a
  warning; those peers are skipped for the remainder of the call.
- Peer unavailability is never fatal. Every peer RPC has a 10 s timeout;
  on any error the cached `PeerClient` channel is dropped so the next
  call reconnects.

---

## 10. Public REST API

Served by `src/rest.rs` via `axum`. JSON responses, `GET` unless noted.

**Pagination.** List endpoints are strictly cursor-based: they accept
`?limit=N&cursor=T` (default `limit=25`, hard max `limit=100`; cursor
defaults to the start of the range). The envelope carries an opaque
`cursor_next: string | null` plus a convenience `has_more: bool`; to
fetch the next page the client echoes `cursor_next` back as `cursor`.
Cursors are base64url-encoded raw RocksDB keys (or a 4-byte index into
an in-memory list for a handful of endpoints like
`/block/:id/operations`), never interpreted by the client. Every
paginated handler is implemented as a single bounded scan that seeks
directly to `cursor` — deep pages cost the same as the first page,
which is what lets us serve a public API without being sensitive to a
crawler asking for page 10,000. The `has_more` flag is only ever
derived from `cursor_next` being non-null; callers should treat
`cursor_next` as the source of truth.

"Previous" is a pure client concern: the explorer's `usePaged` hook
keeps a small stack of cursors it has seen and pops on `Prev`. The
server is strictly forward-only.

### 10.1 Meta

```
GET  /v1/health
GET  /v1/ready
GET  /v1/status                   network, chain params, last final /
                                    candidate slot, peers health, build
GET  /v1/openapi.json
GET  /v1/metrics                  Prometheus exposition
GET  /v1/backfill/status          incomplete slot count, peer snapshots
```

### 10.2 Core lookups

```
GET  /v1/blocks                   newest first
GET  /v1/blocks/{id}
GET  /v1/blocks/{id}/operations
GET  /v1/blocks/{id}/endorsements
GET  /v1/blocks/{id}/denunciations
GET  /v1/blocks/{id}/transfers

GET  /v1/operations
GET  /v1/operations/recent
GET  /v1/operations/{id}
GET  /v1/operations/{id}/events
GET  /v1/operations/{id}/transfers

GET  /v1/endorsements/{id}
GET  /v1/denunciations
GET  /v1/denunciations/{hash}

GET  /v1/slots/{period}/{thread}
GET  /v1/slots/{period}/{thread}/events
GET  /v1/slots/{period}/{thread}/transfers
GET  /v1/slots/range
```

`/v1/blocks/{id}/transfers` and `/v1/slots/{p}/{t}/transfers` return the
union of transfers recorded directly under that block / slot **and**
every transfer attributed (§7.2) to any operation the block / slot
included. The union is sorted by `(period, thread, index_in_slot)` and
deduped on `StoredTransfer.id`.

### 10.3 Address-scoped

```
GET  /v1/addresses/{addr}/blocks
GET  /v1/addresses/{addr}/ops              as creator
GET  /v1/addresses/{addr}/received_ops     as target
GET  /v1/addresses/{addr}/endorsements
GET  /v1/addresses/{addr}/denunciations
GET  /v1/addresses/{addr}/transfers
GET  /v1/addresses/{addr}/events?role=emitter|caller
GET  /v1/addresses/{addr}/node_state       live balance, rolls, deferred
                                             credits — proxied from the
                                             local node, see §5.2.
GET  /v1/addresses/{addr}/bytecode         raw WASM bytecode of a
                                             smart-contract (AS…) address
                                             — proxied from the local
                                             node, see §5.2. EOAs and
                                             empty entries return 404.
```

`/v1/addresses/{addr}/node_state` returns a small JSON document with
`final_balance_nmas` / `candidate_balance_nmas` as decimal strings
(no precision loss on the JS side), `final_rolls` / `candidate_rolls` /
`active_rolls` as integers, and `deferred_credits_final` /
`deferred_credits_candidate` as `[{slot:{period,thread}, nmas}]` sorted
by slot ascending. Every other live-state surface (datastore, draws,
stakers, …) is out of scope; see §5.2 for the rationale.

`/v1/addresses/{addr}/bytecode` deliberately breaks the JSON-envelope
rule: it streams the raw WASM bytes back with
`Content-Type: application/wasm`, a `Content-Disposition: inline;
filename="<addr>.wasm"` hint, and `X-Content-Type-Options: nosniff`. The
explorer uses the same payload for both the "Download .wasm" button
(via a `URL.createObjectURL` blob) and the in-browser WASM analysis
(see §12.2 below). Returns 400 for malformed addresses, 404 for EOAs
(`AU…`) and for `AS…` addresses whose ledger row has no bytecode entry.

### 10.4 Async / deferred / search / MNS

```
GET  /v1/async                             paginated list of every known
                                             async message row (byte-sorted
                                             by id).
GET  /v1/async/{id}
GET  /v1/deferred                          paginated list of deferred calls.
GET  /v1/deferred/{id}
GET  /v1/addresses/{addr}/async?role=…     async messages the address is
                                             sender or destination of.
GET  /v1/addresses/{addr}/deferred?role=…  deferred calls the address is
                                             sender or target of.
GET  /v1/search?q=                         block / op / endorsement id,
                                             `(period, thread)`, address,
                                             or a `*.massa` / bare MNS
                                             label (resolved on demand,
                                             see below).
GET  /v1/mns/resolve?name=<label>          explicit MNS lookup — returns
                                             `{ name: "<label>.massa",
                                             address: "AU…" }` on hit,
                                             404 on miss.
```

The `search` classifier first matches block / operation / endorsement
ids by prefix, then `period[,/: ]thread` slot tuples, then `AU…` / `AS…`
addresses. If none of those match and the query is a DNS-friendly label
(or carries an explicit `.massa` suffix), the indexer issues a
read-only `dnsResolve` call against the on-chain MNS contract and, on
hit, returns the address envelope (with `mns_name` included so the
explorer can render an "aka <label>.massa" badge). On miss it returns
404; on transport failure to the node, 500.

### 10.5 Charts and exports

```
GET  /v1/charts/throughput?window=1h|24h|7d&bucket=1m|5m|1h
GET  /v1/charts/blocks_per_slot
GET  /v1/charts/finality_lag
GET  /v1/charts/active_addresses

GET  /v1/export/slots.csv
GET  /v1/export/addresses/{addr}/transfers.csv
GET  /v1/export/operations/{id}.json
```

Charts are computed from the indexer's primary CFs only. Live-node
panels that don't fit the two curated proxy endpoints from §5.2
(stakers count, selector draws, datastore reads, …) stay out of
scope — neither the indexer nor the explorer talks to a node for them.

Charts compute on demand from primary CFs with 60 s per-endpoint
memoization — no rollup CFs.

### 10.6 Response envelope + errors

Success:

```json
{"network": "mainnet", "cursor_next": "…", "data": …, "warnings": []}
```

Errors follow RFC 7807:

```json
{"type": "…", "title": "…", "status": 404, "detail": "…", "instance": "/v1/…"}
```

---

## 11. Live updates (SSE)

```
GET  /v1/stream/slots              slot status changes
GET  /v1/stream/blocks             new blocks
GET  /v1/stream/final              FINAL slot events only
GET  /v1/stream/operations         new ops in blocks
GET  /v1/stream/events             ?emitter=&caller=&op_id=
GET  /v1/stream/addresses/{addr}   anything concerning addr
```

Each stream sends a `: ping` every 20 s. Clients reconnect with
`Last-Event-ID`; a 5-minute in-memory ring buffer replays missed events.

---

## 12. Frontend

The explorer is a static Vite build and is **read-only**. It consumes
the indexer's REST + SSE surface and renders blocks, operations, slots,
addresses, async/deferred activity and charts. It ships no wallet
integration and never builds, signs or broadcasts operations — users
who want to submit transactions use a wallet (MassaStation, Bearby, …)
directly against their node.

### 12.1 Runtime config & failover

`window.__MASSA_EXPLORER_CONFIG__` is populated by `/config.js`. The
Docker entrypoint rewrites `config.js` from `MASSA_EXPLORER_*` env vars
at container start so operators can change defaults without a rebuild.
A Settings UI lets users edit per-network lists of indexer URLs and
persists them to `localStorage`. There is no node URL in the config: the
explorer is a pure indexer client.

Per-request endpoint selection (`src/api/client.ts`):

1. **Per-session shuffle.** On boot (and on network switch) we freeze a
   random permutation of each list.
2. **Sticky primary with failover.** On network error / 5xx / 429 /
   timeout / network-mismatch, rotate to the next endpoint and retry
   once per endpoint. A 30 s cooldown suppresses the penalised endpoint
   from being primary.
3. **Health probe.** A parallel fan-out `GET /v1/health` every 60 s only
   updates the "3/3 reachable" UI badge — never mutates the permutation.
4. **SSE streams** follow the same rules with a 30 s no-event threshold
   (the 20 s heartbeat keeps a healthy stream alive).

### 12.2 Routes

Home, `/search`, `/blocks` + `/blocks/:id`, `/operations` +
`/operations/:id`, `/slots/:period/:thread`, `/endorsements/:id`,
`/denunciations/:hash`, `/addresses/:addr`, `/async/:id`,
`/deferred/:id`, `/charts`, `/api` (OpenAPI inline), `/404`.

The address page (`/addresses/:addr`) is **kind-aware**: the tab row
shown to the user depends on whether the address prefix is `AU…`
(externally-owned account, i.e. a human-controlled keypair) or `AS…`
(smart contract created by an `ExecuteSC` operation).

- For `AU…` (EOA): **Blocks produced**, **Operations sent**,
  **Operations targeting**, **Transfers**, **Deferred calls**.
- For `AS…` (smart contract): **Operations targeting**, **Transfers**,
  **Deferred calls**, **Bytecode**. "Blocks produced" and "Operations
  sent" are hidden because a smart contract has no signing key, so
  those rows are structurally empty for it.

The default landing tab is "Blocks produced" for EOAs and "Operations
targeting" for contracts (the first tab that's actually populated for
the address kind).

Above the tabs sits a live-state panel populated from
`/v1/addresses/:addr/node_state` (§10.3): final + candidate balance,
final / candidate / active rolls, and deferred credits (one row per
scheduled `slot → nMAS` release). The panel auto-refreshes every 15 s.
When the page was reached through an MNS lookup
(`/search?q=<label>.massa`), the URL carries a `?via=<label>.massa`
query param and the panel displays an "aka `<label>.massa`" badge
next to the address.

The **Bytecode** tab (smart contracts only) fetches the raw WASM via
`/v1/addresses/:addr/bytecode` (§10.3) on first open and runs a
self-contained analysis right in the browser (no external library;
the parser lives in `massa-explorer/src/lib/wasm.ts`, ~430 lines):

- Byte size + SHA-256 fingerprint (computed via `crypto.subtle`).
- Section table — every WASM section with its byte size, plus the
  decoded name for `custom:*` sections.
- Type table — every function signature in the module.
- Imports — module, name, kind (func / table / memory / global) and
  resolved signature or limits.
- Exports — name, kind, declared index, and (when derivable) the
  function signature.
- Memory and table types with min/max limits; declared globals with
  mutability.
- Start function (if any) and module name (if a `name` custom section
  is present).
- Data segments — count, total bytes, plus up to 256 printable-ASCII
  strings extracted from segment payloads (useful for spotting
  embedded datastore prefixes, ABI keys, log templates, etc.).

The same payload also feeds a "Download .wasm" button via
`URL.createObjectURL`, so the user gets exactly the bytes the
analysis was run against. There is **no** server-side disassembly:
the explorer never sends the bytecode anywhere; the analysis is
deterministic from the local bytes alone.

The search bar accepts the usual ids and `(period, thread)` tuples
**and** MNS labels (`damip.massa`, or just `damip`). Hits resolve to
the registered address and redirect to the address page.

### 12.3 Live DAG

Home renders a 32-thread × rolling-time canvas with status colors
(green=final, blue=candidate, red=discarded, slate-grey filled dot=miss,
unknown=tiny dot). Parent links are drawn as bezier curves for the single
latest produced block (tracked by strict slot-timestamp monotonicity) and
fade out ~1 s after a newer block takes over. Same-thread parent links
clamp their upward "bow" to stay inside the producing thread's rail, so
adjacent threads never cross visually. Polling is 1 s; the redraw loop
pauses when `document.visibilityState === "hidden"`.

### 12.4 Detail-page auto-refresh

Slot polls every 3 s while non-final; block polls every 3 s while neither
final nor discarded; operation polls every 5 s until `final_exec_status`
is set. All three also invalidate on the SSE `slot_updated` event for the
matching slot.

### 12.5 Wall-clock conventions

All wall-clock timestamps are rendered in ISO-8601 with the viewer's
timezone offset (e.g. `2026-04-22T17:36:00+02:00`). `formatTsUtc()` is
available for UTC-explicit displays. The chain params
(`genesis_timestamp_ms`, `t0_ms`, `thread_count`) come from
`/v1/status.meta` and are cached for 1 h with mainnet fallbacks for
pre-hydration renders.

### 12.6 Read-only posture

The explorer ships no wallet integration, no `@massalabs/wallet-provider`
or `@massalabs/massa-web3` dependency, and no Node-built-in polyfills.
The whole bundle stays around ≈ 300 kB gzipped and can be served from
any CDN without a runtime API key. Users who want to submit operations
use a wallet (MassaStation, Bearby, a browser extension, …) directly
against their node; once an operation reaches the chain, the explorer
picks it up from the indexer's streams and renders it on `/op/{id}`
like any other op.

---

## 13. Deployment

### 13.1 Indexer (Docker)

`massa-indexer/Dockerfile` is multi-stage: a `rust:1.81.0-slim-bookworm`
builder (with `clang`, `cmake`, `protobuf-compiler`, and
`CXXFLAGS="-include cstdint"` — works around GCC 14 no longer
transitively including `<cstdint>` for `rust-librocksdb-sys`) and a
`debian:bookworm-slim` runtime with `tini` as pid-1 and a non-root user
(`uid=10001`). A built-in `HEALTHCHECK` calls the binary's `healthcheck`
subcommand.

Typical `docker run`:

```bash
docker run -d --name massa-indexer \
  --restart unless-stopped \
  -v /srv/indexer/data:/data \
  -v /srv/indexer/config/indexer.toml:/etc/massa-indexer/indexer.toml:ro \
  -p 127.0.0.1:8080:8080   `# REST, front with nginx for public access` \
  -p 127.0.0.1:9443:9443   `# peer gRPC, reach via SSH/WireGuard only` \
  -e RUST_LOG=info,massa_indexer=debug \
  massa-indexer:local
```

`docker-compose.yml` ships a two-service reference stack (indexer +
explorer) with healthchecks and env-var overrides.
`nginx/indexer.conf.sample` is a TLS-terminating reverse-proxy template
with HSTS, per-IP rate limits, SSE buffering disabled, and long-lived
stream timeouts.

### 13.2 Three-indexer local cluster

`./run_indexers.sh` spins up three indexers against a local node with
non-overlapping ports and RocksDB paths:

| Container   | REST  | Peer gRPC | RocksDB host path                   |
| ----------- | ----: | --------: | ----------------------------------- |
| `indexer-1` | 8081  |    9444   | `data/indexers/indexer-1/rocksdb/`  |
| `indexer-2` | 8082  |    9445   | `data/indexers/indexer-2/rocksdb/`  |
| `indexer-3` | 8083  |    9446   | `data/indexers/indexer-3/rocksdb/`  |

All three use `--network host` to reach the local `massa-node` at
`127.0.0.1:33037` and to peer with each other at `http://127.0.0.1:94XX`.

### 13.3 Frontend

`npm run build` produces `dist/` with relative paths (no SSR). Ship to
DeWeb (per-network `deweb_cli_config.*.json`), a static file server, or
a CDN. A minimal `nginxinc/nginx-unprivileged` Dockerfile is provided;
its entrypoint rewrites `/config.js` from env vars.

---

## 14. Configuration

`indexer.toml`:

```toml
[general]
network = "mainnet"                # or "buildnet"

[node]
grpc_url = "http://<node-host>:33037"
connect_timeout_ms = 5000
keepalive_ms = 15000

[db]
path = "/data/rocksdb"
write_buffer_size_mb = 128

[streams]
filled_blocks          = true
slot_execution_outputs = true
transfers              = true

[rest]
bind = "0.0.0.0:8080"
cors = ["*"]
sse_ring_buffer_size = 10000
sse_heartbeat_secs = 20
default_page_size = 25
max_page_size = 100                # hard ceiling — every paginated handler
                                   # caps at this regardless of the request.

[peer]
enabled = true
bind = "127.0.0.1:9443"
peer_id = "ix-eu-1"                # reported via GetHealth; defaults to $HOSTNAME
scan_interval_ms = 50              # inter-RPC sleep used by the unified
                                   # backfill walker. Skip paths (slot
                                   # already complete / speculative) are
                                   # NOT throttled.

[peer.peers]
a = { url = "http://127.0.0.1:19443" }
b = { url = "http://127.0.0.1:29443" }

# Optional one-shot AWS DynamoDB importer (§9 of ARCHITECTURE.md).
# Defaults to disabled — enable on at most one host in the cluster.
[legacy_ddb]
enabled                = false
region                 = "eu-west-3"
access_key_id          = ""        # delivered via the systemd EnvironmentFile
secret_access_key      = ""        #   so secrets never land in this TOML
blocks_table           = "BlocksMainnet"
operations_table       = "OperationsMainnet"
endorsements_table     = "EndorsementsMainnet"
# max_period          = 4_550_000   # inclusive upper bound; legacy storer
                                    # wrote nothing beyond this point
# min_period          = 0           # inclusive lower bound; useful for
                                    # bounded re-imports after a partial run
rate_limit_ms          = 50         # pause between per-slot DDB queries
connect_timeout_ms     = 5000
request_timeout_ms     = 15000
```

Env overrides follow `INDEXER_<section>_<key>` (e.g.
`INDEXER_NODE_GRPC_URL`, `INDEXER_LEGACY_DDB_ACCESS_KEY_ID`).
`config/indexer.{mainnet,buildnet,production}.toml` ship as starting
templates.

---

## 15. Schema evolution policy

The indexer is a **derived cache** of node data. When the on-disk shape
changes between releases, operators wipe `db.path` and let live stream +
peer backfill rebuild from scratch. There is no in-process migration.

`cf_meta[row]` stores a `schema_version` byte that `Db::open` validates
against the build's `SCHEMA_VERSION` constant. Mismatch → refuse to
start. This is a safety net, not a migration framework.

Upgrade procedure:

```
systemctl stop massa-indexer
rm -rf /var/lib/massa-indexer/rocksdb
systemctl start massa-indexer
```

Releases that bump `SCHEMA_VERSION` are called out in `CHANGELOG.md`.

---

## 16. Observability, CLI, testing, security

### 16.1 Observability

- `tracing` + `tracing-subscriber`. `RUST_LOG` controls level;
  `RUST_LOG_JSON=1` emits JSON lines (default in the Docker image).
- `/v1/health`, `/v1/ready`, `/v1/status`, `/v1/backfill/status`.
- **Prometheus at `/v1/metrics`** (hand-rolled, no `prometheus` crate
  dependency):
  * `massa_indexer_build_info{version, network}` (gauge 1)
  * `massa_indexer_uptime_seconds` (gauge)
  * `ingest_blocks_total`, `ingest_exec_outputs_total`,
    `ingest_transfers_total`, `ingest_peer_patches_total`,
    `ingest_events_dropped_total` (counters)
  * `slots_finalized_total`, `slots_missed_total` (counters)
  * `rest_requests_total`, `rest_errors_total` (axum middleware)
  * `sse_connections_open` (gauge), `sse_connections_total` (counter)
  * `backfill_passes_total`, `backfill_rpcs_total`,
    `backfill_slots_filled_total` (counters)

### 16.2 Global allocator

`tikv-jemallocator` is wired as `#[global_allocator]` on every non-MSVC
target (all production Linux targets) with the `background_threads`
feature so the purge thread runs off the ingest hot path. Reason:
RocksDB's write-heavy LSM workload is the pathological case for glibc
`ptmalloc`; jemalloc keeps RSS bounded and is the allocator the RocksDB
wiki explicitly recommends.

### 16.3 CLI subcommands

The binary is driven by `clap`. Every command honours the global
`--config PATH` flag (defaults to `config/indexer.toml`). All CLI
commands except `serve` exit with a non-zero status on failure so they
compose cleanly in shell scripts / Kubernetes jobs.

```
massa-indexer serve                                       (default)
massa-indexer healthcheck [--url URL] [--timeout-ms N]    probes /v1/health; exit 0/1
massa-indexer show-config                                 prints the resolved config as JSON
massa-indexer stats                                       per-CF row counts
massa-indexer verify [--max-issues N]                     secondary-index sanity check
massa-indexer dump --cf NAME [--key HEX] [--prefix HEX]
                   [--after HEX] [--limit N]              raw (key, value) dump
massa-indexer dump --cf list                              lists every known CF
massa-indexer peers [--timeout-ms N]                      GetHealth probe on every peer
massa-indexer replay --from P[:T] --to P[:T]
                     [--thread-count N] [--no-block]
                     [--no-exec] [--no-transfers]
                     [--max-slots N]                      pulls a slot range from peers
massa-indexer reindex-secondaries --yes                   rebuilds every IDX_* CF
massa-indexer --version
```

Operational commands:

- **`verify`** walks every secondary-index CF
  (`IDX_BLOCK_BY_CREATOR`, `IDX_OP_BY_CREATOR`, `IDX_OP_BY_ADDR`,
  `IDX_EVENT_BY_OP`, `IDX_EVENT_BY_ADDR`, `IDX_TRANSFER_BY_OP_SLOT`,
  `IDX_TRANSFER_BY_BLOCK_SLOT`, `IDX_TRANSFER_BY_ADDR_SLOT`,
  `IDX_ENDORSEMENT_BY_SLOT`, `IDX_ENDORSEMENT_BY_PRODUCER`,
  `IDX_DEFERRED_BY_ADDR_ID`, `IDX_ASYNC_BY_ADDR_ID`) and reports
  any row whose expected primary entry (e.g. `CF_BLOCK`,
  `CF_OP`, `CF_EVENT`, `CF_TRANSFER`, `CF_ENDORSEMENT`,
  `CF_DEFERRED_CALL`, `CF_ASYNC_MSG`) is missing or whose key fails
  to parse. Pure read-only. Exits 1 if any orphan is found; prints
  at most `--max-issues` details (default 100, `0` disables the cap).
  The exit code still reflects the full count.
- **`dump`** pretty-prints raw `(key, value)` pairs from a chosen CF as
  NDJSON `{"key": <hex>, "value": <hex>}`. Supports point lookup
  (`--key HEX`), prefix scan (`--prefix HEX`), cursor pagination
  (`--after HEX` using the `next_cursor` from the previous call) and
  a configurable page size (`--limit`, default 100). `--cf list`
  prints every known CF name.
- **`peers`** reads `[peer.peers.*]`, calls `GetHealth` on every
  entry via the existing per-peer client pool and renders a small
  table (`name | url | status | network | latency`). Exits 1 if at
  least one peer fails or the peer list is empty. `--timeout-ms` is
  informational — the pool already enforces its own 10 s
  `RPC_TIMEOUT`.
- **`replay`** fetches a closed slot range `[--from, --to]` from the
  configured peers and applies it locally without subscribing to any
  node stream. Slot bounds are `PERIOD[:THREAD]` (thread defaults to
  `0` on `--from` and `thread_count - 1` on `--to`); thread count is
  taken from `cf_meta` or overridden by `--thread-count`. Fetched
  slots go through the same `apply_peer_patch` path as live backfill,
  so fork-trail conflicts keep the local copy and repeated runs are
  idempotent. `--no-block`, `--no-exec`, `--no-transfers` clear the
  matching `FinalSlotParts` flags; `--max-slots` (default 100 000)
  bounds the scan. Exits 1 if the peer list is empty or any slot
  failed to apply.
- **`reindex-secondaries`** wipes every `IDX_*` CF and rebuilds it by
  iterating the corresponding primary CFs and re-running the normal
  `write_*` code path. Requires `--yes` because it writes to the data
  directory (and the caller must stop any concurrent `serve`
  beforehand). Prints a `cleared / replayed` report keyed by CF name.

### 16.4 Testing

- **Unit tests** — in `src/*.rs::tests` modules. Cover codec round-trip,
  key layout (+ proptest for `rslot` ordering), ID parsing, ingest
  state-machine transitions (first-final-wins, finality propagation,
  late-block status), backfill `missing_parts`, peer patch application,
  SSE ring buffer, REST handlers, OpenAPI coverage, metrics rendering.
- **Integration tests** —
  * `tests/peer_backfill.rs` — 12 scenarios with 2–3 in-process indexer
    instances (tempdir RocksDB, ephemeral `127.0.0.1:0` ports): basic
    backfill, `StreamFinalSlots` newest-first, fork-trail-hash conflict,
    partial parts, peer fail-over, network-mismatch rejection,
    parent-gap cascade, patch idempotency, cumulative parts fill,
    peer-unknown-slot no-op, stale-Unknown stub upgrade, and end-to-end
    backfill of denunciations + async messages + deferred calls with
    their secondary indexes.
  * `tests/backfill_worker.rs` — drives `run_backfill` end-to-end against
    an in-process peer; a consumer with no seeded data catches up within
    one scan cycle.
  * `tests/live_node.rs` — opt-in smoke against a running local node
    (`MASSA_INDEXER_LIVE_NODE=1`).
  * `tests/cli_e2e.rs` — drives `cli::peers` and `cli::replay` against
    an in-process peer indexer: live-peer health, dead-peer error,
    slot-range replay application, empty-peer-list refusal.
- **CLI unit tests** — `src/cli.rs::tests` covers `verify` (clean DB,
  orphan detection, issue cap), `dump` (point lookup, unknown CF,
  paginated scan), `reindex_secondaries` (rebuild from primaries, op
  target loss recovery, transfer-by-op index), `next_slot` wrap and
  `render_peers_table` formatting.
- **Frontend** — vitest on the failover client + format helpers;
  `tsc --noEmit` clean, `vite build` clean.

### 16.5 CI

`.github/workflows/ci.yml` runs on every push / PR:

1. **backend** — `cargo fmt --check`, `cargo clippy --all-targets -- -D
   warnings`, `cargo test --lib --tests` on rustc 1.81.0.
2. **frontend** — `tsc --noEmit`, `vitest run`, `vite build`, artifact
   upload of `dist/`.
3. **docker** — builds both production images with BuildKit + Actions
   cache; smoke-tests the indexer (`--version`, `healthcheck`) and the
   explorer (`/healthz`, `/config.js`).

### 16.6 Security posture

- REST + peer gRPC are **unauthenticated plaintext**. Peer gRPC binds to
  `127.0.0.1` by default — tunnel outward. Operators front REST with
  nginx for TLS + rate limits + CORS.
- The indexer never writes to the node. The node gRPC connection is
  read-only (stream subscriptions + one-shot `GetNodeConfig` at
  startup); there is no `SendOperations` / `QueryState` path.
- RocksDB files are local; no secrets stored; no encryption at rest.
- `indexer.toml` is provided at runtime, never baked into the image.
- Docker images run as a non-root user; the explorer image uses
  `nginxinc/nginx-unprivileged` on port 8080 with security headers
  (`X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`,
  `Permissions-Policy`, `server_tokens off`).
- `SECURITY.md` documents the threat model and disclosure process.

### 16.7 Out of scope

- Top-holders / richest addresses (requires full ledger indexing).
- Indexing the MNS registry. Name → address resolution is served on
  demand by `/v1/mns/resolve` (§10.4), which is a thin proxy over the
  MNS contract's `dnsResolve` read-only call (§5.2). We never persist
  the name → address mapping, which means the explorer always reflects
  the on-chain truth (including names re-registered after release)
  without storing any datastore state.
- Reverse MNS (address → registered names). Same rationale — out of
  scope for the indexer; explorers that want it can call the MNS
  contract's `getDomainsFromTarget` directly.
- Multi-tenant API keys / billing.
- Cross-chain bridging / oracle data.
- "State at slot" historical snapshots.

---

## 17. Outstanding work

Everything in §1–§16 is implemented and exercised by the test suite.
The items below are explicitly deferred:

- **Auto-generated TypeScript types from OpenAPI.** Today
  `src/lib/types.ts` is hand-authored to mirror `massa-indexer/src/model.rs`.

*End of spec.*
