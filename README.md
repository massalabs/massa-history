# massa-history

**Self-hostable Massa blockchain indexer and explorer** (Indexer V2).

This is the production stack that powers a full historical view of Massa
mainnet: a Rust indexer that streams a node’s public gRPC API into a local
RocksDB, plus a React explorer that consumes the indexer’s REST + SSE API.

| Component | Path | Purpose |
| --- | --- | --- |
| **Indexer** | [`massa-indexer/`](./massa-indexer/) | Streams filled blocks / execution / transfers from a Massa node, persists them in RocksDB, serves `/v1/*` (REST + SSE) and optional peer gRPC on `:9443`. |
| **Explorer** | [`massa-explorer/`](./massa-explorer/) | React / TypeScript SPA (Vite) that talks to the indexer API. |
| **Protos** | [`massa-proto/`](./massa-proto/) | Vendored `.proto` files (node public API + indexer peer RPC). |
| **Nginx samples** | [`nginx/`](./nginx/) | Reference reverse-proxy / TLS / rate-limit configs. |

**Docs in this repo**

| Doc | What it is |
| --- | --- |
| [`spec.md`](./spec.md) | Authoritative functional contract (API, schema, completeness, peers, legacy import). |
| [`explanation.md`](./explanation.md) | Design rationale — why each choice was made. |
| [`ARCHITECTURE.md`](./ARCHITECTURE.md) | Physical 3-host deployment: hardware, systemd, storage, peering, day-2 ops. |
| [`SECURITY.md`](./SECURITY.md) | Threat model and hardening checklist. |

OpenAPI for the live REST surface is served by every indexer at
`/v1/openapi.json` and rendered in the explorer under **API**.

> **Not in this repository (by design)**  
> Massa node source/binaries, RocksDB data, logs, AWS credentials, and node
> private keys. Those stay on the deployment hosts. The Massa node is an
> external dependency — build/run it from
> [`massalabs/massa`](https://github.com/massalabs/massa) (with the
> `execution-info` feature) next to the indexer.

---

## Architecture (overview)

Each production host is **fully self-contained** — no shared DB, no leader:

```
            ┌───────────────────────────────────────────────────┐
            │                  one physical host                │
            │                                                   │
   :80 ─────►   nginx  ────► (static dist) Massa Explorer SPA   │
            │     │                                             │
            │     └──► /v1/* (REST + SSE) ───►  massa-indexer   │
            │                                       │           │
            │                                       │ gRPC      │
            │                                       ▼           │
            │                                  massa-node       │
            │                               (execution-info)    │
            └───────────────────────────────────────────────────┘
```

**Data plane**

1. **Live ingest** — indexer subscribes to the node’s public gRPC streams
   (`NewFilledBlocks`, `NewSlotExecutionOutputs`, `NewTransfersInfo`, …).
2. **RocksDB** — fixed-length binary keys, protobuf values, secondary indexes
   (by address / op / block / transfer). Lives under `/data/indexer/rocksdb`
   on production hosts (never in git).
3. **REST + SSE** — `/v1/*` for queries; `/v1/slots/stream` (and friends) for live UI.
4. **Peer sync** — optional indexer↔indexer gRPC on `:9443`. Each box walks
   slots head→genesis and asks peers to fill gaps. Peers are free; they do
   **not** replace the local node.
5. **Legacy one-shot (optional)** — a single host may run a DynamoDB importer
   once to backfill pre-indexer history. Credentials come from a host-local
   systemd `EnvironmentFile`, never from this repo. After completion, disable
   it (`INDEXER_LEGACY_DDB_ENABLED=false`).

**Current deployment topology** (details in [`ARCHITECTURE.md`](./ARCHITECTURE.md)):

| Host | Where | Notes |
| --- | --- | --- |
| `indexer1` | LAN `192.168.0.19` | Node + indexer + explorer; was the AWS legacy importer host (now disabled). |
| `indexer2` | LAN `192.168.0.29` | Node + indexer + explorer; peers with indexer1 on LAN. |
| `indexer3` | Off-site `86.205.18.20` (SSH `-p 2222`) | Same stack; WAN peer ports not open yet — runs standalone until firewall work lands. |

Public gateway (operator-owned): TLS +
[`https://massa-ai.freeboxos.fr/explorer/`](https://massa-ai.freeboxos.fr/explorer/)
proxies `/explorer/v1/*` to a LAN indexer.

---

## Quickstart (Docker Compose)

Point the stack at an already-running Massa node (node keys stay on the host;
the indexer only needs **read-only** public gRPC, default `:33037`):

```bash
docker compose build

INDEXER_NODE_GRPC_URL=http://host.docker.internal:33037 \
INDEXER_NETWORK=mainnet \
MASSA_EXPLORER_MAINNET_ENDPOINTS=http://localhost:8080 \
  docker compose up -d

curl -s http://127.0.0.1:8080/v1/health | jq
# Explorer: http://127.0.0.1:8081/
```

```bash
docker compose down          # keeps the RocksDB volume
docker compose down -v       # also wipes RocksDB — destructive
```

For TLS / rate limits, front the indexer with
[`nginx/indexer.conf.sample`](./nginx/indexer.conf.sample).

---

## From source

**Requirements:** Rust 1.81.0 (see `massa-indexer/rust-toolchain.toml`),
Node.js 20+, a Massa node with public gRPC (`:33037`) built with
`execution-info`.

```bash
# Indexer
cd massa-indexer
CXXFLAGS="-include cstdint" cargo build --release
./target/release/massa-indexer serve --config config/indexer.toml

# Explorer
cd massa-explorer
npm ci
npm run dev          # http://localhost:5173
# production:
npm run build && npm run preview
```

Production config templates live under `massa-indexer/config/`
(`indexer.production.toml`, `indexer.mainnet.toml`, …). Override secrets and
host-specific values with `INDEXER_*` environment variables (see
`massa-indexer/src/config.rs`) — especially AWS legacy credentials.

---

## Deployment (production hosts)

Full recipe: [`ARCHITECTURE.md`](./ARCHITECTURE.md) (§5 provisioning, §6
upgrades, §8 peers, §9 legacy import). Short version:

### Layout per host

| Path | Contents |
| --- | --- |
| `/home/damip/massahistory/` | This git checkout (source + explorer `dist/`). |
| `/home/damip/massahistory/massa/` | Local Massa node checkout (**not in git**). |
| `/data/indexer/rocksdb/` | Indexer database (**not in git** — terabytes). |
| `/data/indexer/indexer.toml` | Host-local indexer config. |
| `/etc/default/massa-indexer-legacy` | Optional AWS creds (indexer1 only; root-readable). |
| systemd | `massa-node`, `massa-indexer`, `nginx` |

### Install / upgrade indexer binary (no DB wipe)

```bash
# on a build host
cd massa-indexer && CXXFLAGS="-include cstdint" cargo build --release

# copy binary only — never rsync rocksdb
scp target/release/massa-indexer user@host:/tmp/massa-indexer.new
ssh user@host 'sudo install -m 0755 /tmp/massa-indexer.new /usr/local/bin/massa-indexer
               && sudo systemctl restart massa-indexer'
```

### Explorer static files

```bash
cd massa-explorer && npm ci && npm run build
# LAN nginx root (example):
rsync -az --delete --exclude config.js dist/ user@host:/home/damip/massahistory/massa-explorer/dist/
# Public /explorer/ subpath build:
npx vite build --base=/explorer/ --outDir=dist-explorer
rsync -az --delete --exclude config.js dist-explorer/ user@gateway:/var/www/massa-explorer/dist/
```

### Peer mesh

On each host, `[peer] enabled = true` and `[peer.peers.*]` URLs pointing at
siblings’ `:9443`. Today indexer1 ↔ indexer2 on the LAN; indexer3 joins when
its public `:9443` (and related node ports) are reachable.

### Ports (typical)

| Port | Service |
| --- | --- |
| 31244 | massa-node consensus (P2P) |
| 31245 | massa-node bootstrap |
| 33035 | massa-node public JSON-RPC |
| 33036 | massa-node public REST v2 |
| 33037 | massa-node public gRPC (indexer → node) |
| 31248 | massa-node metrics |
| 8080 | indexer REST (usually only via nginx `:80`) |
| 9443 | indexer peer gRPC |
| 80 / 443 | nginx (explorer + `/v1/` proxy) |

Do **not** expose private node APIs (`33034`, `33038`).

### Safety rules

* **Never delete `/data/indexer/rocksdb`** unless you intentionally want a
  full resync (hours–days on mainnet).
* **Never commit** AWS keys, node `node_privkey.key`, or host `indexer.toml`
  with secrets — use systemd env files.
* Rolling upgrades: restart **one** host at a time; peers cover gaps.

---

## Tests & CI

```bash
cd massa-indexer && CXXFLAGS="-include cstdint" cargo test --lib --tests
cd massa-explorer && npm run lint && npm test && npm run build
```

GitHub Actions (`.github/workflows/ci.yml`) runs fmt/clippy/tests and builds
the Docker images on every push.

---

## Repository layout

```
massa-history/
├── massa-indexer/        Rust indexer (binary + lib + tests)
├── massa-explorer/       React SPA
├── massa-proto/          Vendored .proto files
├── nginx/                Sample reverse-proxy configs
├── spec.md               Functional specification
├── explanation.md        Design rationale
├── ARCHITECTURE.md       Physical deployment playbook
├── SECURITY.md           Threat model / hardening
├── docker-compose.yml    Reference two-service compose
└── .github/workflows/    CI
```

---

## License

Dual-licensed under MIT OR Apache-2.0. See [`LICENSE-MIT`](./LICENSE-MIT)
and [`LICENSE-APACHE`](./LICENSE-APACHE).
