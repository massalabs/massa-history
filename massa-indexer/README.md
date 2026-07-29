# massa-indexer

Rust service that streams from a Massa node's gRPC API, persists every slot,
block, operation, endorsement, denunciation, SC event, transfer, and async
message into a local RocksDB database, and exposes a REST + Server-Sent Events
API at `/v1/*`.

The authoritative specification of what gets stored, what the endpoints
return, and how V1 acceptance criteria are met lives in
[`../spec.md`](../spec.md). The OpenAPI document describing every live
endpoint is served at `/v1/openapi.json`.

---

## Build

Toolchain is pinned in [`rust-toolchain.toml`](./rust-toolchain.toml) to
`1.81.0`. `librocksdb-sys` bundles its own C++ sources and needs GCC/Clang
with `<cstdint>` explicitly included on Debian 13 / GCC 14+; the `CXXFLAGS`
below papers over that.

```bash
cd massa-indexer
CXXFLAGS="-include cstdint" cargo build --release
```

The release profile enables thin LTO, a single codegen unit, and debug-info
stripping (see [`Cargo.toml`](./Cargo.toml)).

---

## Run

### Locally

```bash
./target/release/massa-indexer serve --config config/indexer.toml
```

Subcommands:

| Command | Purpose |
| --- | --- |
| `serve`          | Run ingest + REST + peer gRPC (default). |
| `show-config`    | Print the resolved config as JSON and exit. Useful for verifying env overrides. |
| `stats`          | Print approximate row counts per RocksDB column family. |
| `healthcheck`    | Probe the local `/v1/health` endpoint; exits 0 if healthy, 1 otherwise. Used by the Docker HEALTHCHECK. |
| `--version`      | Print the crate version (`CARGO_PKG_VERSION`). |

### Docker

The shipped image is multi-stage, non-root (`uid=10001`), runs under `tini`,
has a built-in HEALTHCHECK, and logs in structured JSON by default.

```bash
# Build (context is the workspace root):
docker build -f massa-indexer/Dockerfile -t massa-indexer:local .

# Run:
docker run -d --name indexer \
  -p 127.0.0.1:8080:8080 \
  -p 0.0.0.0:9443:9443 \
  -v indexer_data:/data \
  -e INDEXER_NODE_GRPC_URL=http://host.docker.internal:33037 \
  -e INDEXER_GENERAL_NETWORK=mainnet \
  -e INDEXER_REST_CORS='https://explorer.example.com' \
  massa-indexer:local

docker exec indexer massa-indexer stats
```

---

## Configuration

Configuration is a TOML file with env overrides on top
(`INDEXER_<SECTION>_<KEY>`, uppercase, `_` separator). See
[`config/indexer.production.toml`](./config/indexer.production.toml) for an
annotated template and [`src/config.rs`](./src/config.rs) for the full list
of supported keys and env overrides.

The most frequently overridden variables in production:

| Env var | TOML key | Typical value |
| --- | --- | --- |
| `INDEXER_NODE_GRPC_URL` | `node.grpc_url` | `https://node.mycompany.com:33037` |
| `INDEXER_GENERAL_NETWORK` | `general.network` | `mainnet` / `buildnet` |
| `INDEXER_DB_PATH` | `db.path` | `/data/rocksdb` |
| `INDEXER_REST_BIND` | `rest.bind` | `0.0.0.0:8080` |
| `INDEXER_REST_CORS` | `rest.cors` | `https://explorer.example.com,https://explorer2.example.com` |
| `INDEXER_PEER_PEERS` | `peer.peers.*` | `east=https://east.mycompany.com:9443,west=https://west.mycompany.com:9443` |

Logging:

* `RUST_LOG` — standard `tracing-subscriber` filter (`info,massa_indexer=debug` by default).
* `RUST_LOG_JSON=1` — switch to JSON output (shipped as the default in Docker).

The indexer **rejects mismatched networks**: once the RocksDB volume is
initialized for `mainnet`, you can't re-launch it with
`INDEXER_GENERAL_NETWORK=buildnet`. That's intentional — it prevents silently
corrupting a mainnet archive by pointing it at the wrong node.

---

## Observability

* `GET /v1/health` — returns `200` with node/ingest/DB age if ingest progressed
  within a small window, `503` otherwise. Used by the Docker HEALTHCHECK and
  the `healthcheck` subcommand.
* `GET /v1/meta` — network, RocksDB path, build version, last slots seen.
* `GET /v1/stats` — row counts per CF (same data as `massa-indexer stats`).
* `GET /v1/openapi.json` — machine-readable description of every route.
* `tracing` spans are emitted on the gRPC consumer, the ingest worker, the
  peer-backfill loop, and every REST handler.

Prometheus metrics are exposed at `GET /v1/metrics` (simple text format).
Counters cover ingest (blocks/exec/transfers/peer-patches/drops), REST/SSE
traffic, backfill activity, and slot-state progression.

---

## Operations cheat sheet

**Start:**
`docker compose up -d indexer` or `systemctl start massa-indexer`.

**Stop / graceful shutdown:**
SIGTERM (Docker stop, `kill -TERM`, systemd `stop`) is honored. The process
drains the ingest worker for 5 seconds before exiting.

**Backups:**
RocksDB lives at `$db.path` (`/data/rocksdb` in the image). A hot copy is safe
if made via `ldb backup` or a file-system snapshot. Restoring is as simple as
putting the files back into the volume; the indexer resumes from the last
meta row.

**Peers and backfill:**
When two+ indexers are configured as mutual peers, each runs a unified
backfill walker that scans `last_final_slot` → `(0, 0)` repeatedly,
asking peers for any missing or incomplete-FINAL slot. The pacing is
controlled by a single knob: `[peer] scan_interval_ms`. Peers
authenticate by network name; a mismatched peer is dropped.

**Stream toggles:**
Each gRPC subscription is independently enabled/disabled via `[streams]`.
The default config turns `filled_blocks`, `slot_execution_outputs`, and
`transfers` on. Backfill and slot-completeness ignore parts for disabled
streams so a flipped switch never leaves slots eternally "in progress".
The indexer intentionally does **not** subscribe to `NewSlotABICallStacks`
— emitter / caller addresses and the owning op / async / deferred-call
grouping are derived from SC-event context and the transfer rows, which
is enough for every user-facing query without the extra FINAL-only
(execution-trace-gated) payload.

**DB schema upgrades:**
The indexer does not attempt on-disk migrations. Since the indexer is a
derived cache (the node is the source of truth), the expected workflow is to
wipe `db.path` between incompatible releases and let the streams + peer
backfill rebuild state. Snapshot first if you care about retaining the
archive.

---

## Security posture

* The indexer listens plain-HTTP — always front with TLS (see
  [`../nginx/indexer.conf.sample`](../nginx/indexer.conf.sample)).
* CORS defaults to `*` so that a browser at any origin can consume the
  public data; restrict to your explorer's origin in production.
* There is **no authentication**: every endpoint is public-read. If you need
  to hide an indexer, use network-level ACLs (VPC, firewall).
* `/v1/export/*` endpoints are bandwidth-intensive; rate-limit them at the
  reverse proxy (2 req/s per IP is a good default; the sample nginx config
  does this).
* The peer gRPC port (`:9443`) is public so sibling indexers can reach it. It
  is read-only and authenticates sessions by the declared network + schema
  version, but it does disclose the full archive. If that's unacceptable,
  disable the peer layer with `INDEXER_PEER_ENABLED=false`.
* The binary runs as non-root inside the Docker image (`uid=10001`). If you
  run it outside Docker, create a dedicated user and give it exclusive
  ownership of the RocksDB directory.

---

## Testing

```bash
CXXFLAGS="-include cstdint" cargo test --lib --tests
```

Expect 64 tests passing: 54 unit tests (config, db, ingest, proxy, REST
handlers, SSE, OpenAPI), 8 peer-backfill integration tests, 1 backfill-worker
test, 1 live-node smoke test. Doctests in the auto-generated `google.api.rs`
proto bindings are intentionally skipped.
