# ARCHITECTURE.md — Massa Indexer V2 production deployment

This document describes the physical deployment of the Massa Indexer V2 +
explorer stack. It covers:

1. The three existing instances (`indexer1`, `indexer2`, `indexer3`).
2. The hardware bill of materials.
3. The exact per-host layout: storage, services, networking, hardening.
4. A step-by-step provisioning recipe so an extra box can be brought up
   the same way.
5. Day-2 operations (logs, restarts, upgrades, troubleshooting).
6. Disposable indexer DB / failure recovery.
7. What is intentionally not in this deployment.
8. Indexer-to-indexer peer sync (gap-fill on restarts).
9. Legacy block-storer (DynamoDB) one-shot importer.

The companion documents are:

- `spec.md` — full functional spec of the indexer + explorer.
- `explanation.md` — why each design decision was made.
- `run_node.sh` / `run_indexers.sh` — older laptop-dev launchers; the
  production hosts use systemd directly (see below) and these scripts
  remain useful only on a developer workstation.

---

## 1. Topology

Each host is **fully self-contained**. There is no shared storage, no
shared database, no leader/follower coordination. A box runs:

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
            │                                   (with           │
            │                                  execution-info)  │
            │                                                   │
            └───────────────────────────────────────────────────┘
```

Hosts are independent and **horizontally redundant**: lose one, the other
keeps serving. Adding a third host is purely additive — see §5.

Current live hosts:

| Host       | Address                         | Access                         | Role                           |
|------------|---------------------------------|--------------------------------|--------------------------------|
| `indexer1` | `192.168.0.19` (LAN)            | LAN only                       | full node + indexer + explorer |
| `indexer2` | `192.168.0.29` (LAN)            | LAN only                       | full node + indexer + explorer |
| `indexer3` | `86.205.18.20` (off-site)       | SSH `damip@… -p 2222`          | full node + indexer + explorer |

`indexer1` and `indexer2` share a LAN and can peer with each other today.
`indexer3` was moved off-site; public peer/API ports are not yet open, so
cross-site indexer peer sync (`:9443`) and LAN→unit3 reachability are
**pending** operator firewall work. Until then each site runs independently
from its own node; no data is deleted while connectivity is restored.

On the LAN, each box is reachable as `http://<ip>/` from a browser.

A separate **public-gateway** box (`192.168.0.44`, reachable from the
internet as `massa-ai.freeboxos.fr` on SSH port `2222`) terminates TLS
and reverse-proxies the explorer at
[`https://massa-ai.freeboxos.fr/explorer/`](https://massa-ai.freeboxos.fr/explorer/),
forwarding `/explorer/v1/*` requests to one of the three indexers. The
gateway is owned by the user and intentionally outside this repository
— this doc only notes its existence so operators know where the
public-facing URL lives. See §5.10 / §5.11 for how the explorer build
targets that subpath.

All three indexer boxes are configured identically, are independent,
and any one of them can be lost without affecting the others. The
recipe in §5 is exactly what was applied to each.

The three indexers also peer with each other over an internal indexer-to-indexer
gRPC protocol on port `:9443` to backfill any per-slot gaps each one might
develop while it's restarted. Peer sync is **purely complementary** — every
indexer is still authoritative for what its own node delivered live, and
each one can run on its own with peer mode disabled. See §8 for the full
peer-sync setup.

`indexer1` additionally has read-only AWS credentials for the **legacy
block-storer** DynamoDB tables and is the host that runs the **one-shot
importer** (§9): a background task that walks the legacy archive
exhaustively head → genesis exactly once, ships every recoverable row
into its local RocksDB, then exits. `indexer2` and `indexer3` learn
the same data through normal peer sync. AWS is **not** consulted from
the regular backfill loop on any host — peers are free, AWS reads are
not, and the dataset is meant to be imported once.

---

## 2. Hardware (per host)

Each host is a **Minisforum MS-01** mini-workstation, plus 2× 4 TB NVMe
SSDs added to the empty slots:

| Component   | Item                                            | Qty | Notes                                       |
|-------------|-------------------------------------------------|----:|---------------------------------------------|
| Mini-PC     | Minisforum MS-01 (i9-13900H, 32 GB DDR5, 1 TB) |   1 | comes with OEM 1 TB OS drive + 32 GB RAM    |
| Storage     | Crucial P310 4 TB Gen4 NVMe TLC                |   2 | populates the 2 free M.2 slots              |
| Network     | 2× 10 GbE SFP+, 2× 2.5 GbE RJ45, WiFi 6 (built-in MS-01) | — | wire whichever fits your switch         |

Total cost ≈ €1.9–2.4k per host depending on SSD pricing of the day.

Why this hardware:

- **i9-13900H** (14C / 20T, 5.4 GHz boost) easily handles a node + indexer
  workload. 6 P-cores keep RocksDB compactions snappy.
- **32 GB DDR5** is the minimum we want; the indexer uses ~4–10 GB
  RocksDB cache and the node ~1–4 GB. Headroom for OS/page cache.
- **TLC NAND** (P310) — RocksDB compactions write a lot. QLC works but
  has worse sustained-write performance. The P310 has 1200 TBW / 5-yr
  warranty.
- **Two 4 TB drives** are striped (RAID 0) into 8 TB to give the indexer
  room to grow and to double random IOPS — see §3.

---

## 3. Per-host layout

### 3.1 Storage

```
nvme0n1   1 TB Kingston OEM      ──►  /          (ext4, OS + Massa node)
nvme1n1 ─┐
         ├─ md0  RAID0  256 KB   ──►  /data      (ext4, 7.3 TB usable)
nvme2n1 ─┘
```

| Path                              | What lives there                          |
|-----------------------------------|-------------------------------------------|
| `/`                               | Ubuntu, source repo, **massa-node** binary, node bootstrap state (~few GB) |
| `/data/indexer/rocksdb/`          | the indexer's column families (the bulk of the data)                       |
| `/data/indexer/logs/indexer.log`  | indexer log (also tee'd to journald)                                       |
| `/data/indexer/indexer.toml`      | indexer config (see §3.3)                                                  |
| `/home/damip/massahistory/`       | source tree (rsync'd from dev machine)                                     |

Why this split:

- The OS drive carries the node's relatively small bootstrap state and
  benefits from being insulated from the indexer's heavy compaction
  load.
- The striped pair carries the indexer because it's the I/O hot path:
  block bodies, ops, transfers, async messages, deferred calls, plus all
  secondary indexes.

Why **RAID 0** specifically:

- Doubles read throughput and random IOPS for RocksDB.
- No redundancy on `/data` is acceptable: the only durable data is the
  ledger, which can be **resynced from peers** if a drive ever fails.
  Using RAID 1 would halve usable capacity for protection we don't need.
- Avoid ZFS / btrfs CoW: RocksDB does its own write amplification, and
  a CoW filesystem on top would double writes for no benefit.

### 3.2 Services and listening ports

| Service          | Process                                                               | Port(s)                          |
|------------------|-----------------------------------------------------------------------|----------------------------------|
| `massa-node`     | `/home/damip/massahistory/massa/target/release/massa-node`            | `:33037` gRPC pub, `:33035` P2P, `:31248` metrics |
| `massa-indexer`  | `/usr/local/bin/massa-indexer` (binary; build at `/home/damip/massahistory/massa-indexer/target/release/`) | `:8080` REST + SSE               |
| `nginx`          | system package                                                        | `:80` (serves explorer dist + reverse-proxies `/v1/*` to `:8080`) |

The node is built **with the `execution-info` cargo feature**. This is
mandatory: it's what enables the `NewTransfersInfoServer` gRPC stream
that the indexer subscribes to for the transfers index.

The node identity (`config/node_privkey.key`) is **regenerated per host**
so each box appears as a distinct peer on the Massa P2P network. Do not
copy this file across machines.

### 3.3 Configuration files

Indexer config — `/data/indexer/indexer.toml`:

```toml
[general]
network = "mainnet"

[node]
grpc_url = "http://127.0.0.1:33037"
connect_timeout_ms = 5000
keepalive_ms = 15000

[db]
path = "/data/indexer/rocksdb"
compression = "lz4"
write_buffer_size_mb = 256

[rest]
bind = "0.0.0.0:8080"
cors = ["*"]
sse_ring_buffer_size = 10000
sse_heartbeat_secs = 20
default_page_size = 25
max_page_size = 100

[streams]
filled_blocks          = true
slot_execution_outputs = true
transfers              = true

# Indexer-to-indexer peer sync — see §8. The unified backfill
# scanner walks every slot from `last_final_slot` down to (0,0)
# and asks peers for any missing or incomplete-FINAL slot.
# `scan_interval_ms` is the inter-RPC sleep; skip paths (slot
# already complete) are free.
[peer]
enabled  = true
bind     = "0.0.0.0:9443"
peer_id  = "indexer1"
scan_interval_ms = 50

[peer.peers.indexer2]
url = "http://192.168.0.29:9443"
# indexer3 is off-site (86.205.18.20). Re-enable once :9443 is reachable:
# [peer.peers.indexer3]
# url = "http://86.205.18.20:9443"

# Legacy block-storer (DynamoDB) one-shot importer — see §9.
# Credentials and the `enabled` flag come from the systemd
# EnvironmentFile at /etc/default/massa-indexer-legacy on the
# credentialled host only (typically just indexer1).
#
# When `enabled = true`, a single background task walks the AWS
# legacy tables head → genesis once, then exits. It is NOT consulted
# by the regular peer-backfill scanner — that one is peers-only.
[legacy_ddb]
max_period    = 4550000     # safe upper bound (legacy storer's last write)
rate_limit_ms = 50          # ≈ 20 RPS to AWS, well under provisioned capacity
```

Explorer runtime config — `/home/damip/massahistory/massa-explorer/dist/config.js`:

```js
window.__MASSA_EXPLORER_CONFIG__ = {
  defaultNetwork: "mainnet",
  endpoints: {
    mainnet:  ["http://<this-host-ip>"],
    buildnet: ["http://<this-host-ip>"],
  },
};
```

Same-origin (the SPA hits `/v1/*`, nginx proxies to the local indexer),
so SSE and REST work without any CORS headache.

### 3.4 systemd units

Three units per host. All three have **`Restart=always`** so any
crash, panic, OOM kill or `kill -9` is auto-recovered within ~10 s.

`/etc/systemd/system/massa-node.service`:

```
[Unit]
Description=Massa Node (with execution-info)
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=600
StartLimitBurst=20

[Service]
Type=simple
User=damip
Group=damip
WorkingDirectory=/home/damip/massahistory/massa/massa-node
ExecStart=/home/damip/massahistory/massa/target/release/massa-node -p ${MASSA_PASSWORD} -a
Environment=MASSA_PASSWORD=massahist-prod
Restart=always
RestartSec=10
TimeoutStopSec=60
KillMode=mixed
StandardOutput=append:/home/damip/massahistory/massa/massa-node/logs.txt
StandardError=append:/home/damip/massahistory/massa/massa-node/logs.txt
LimitNOFILE=1048576
LimitNPROC=infinity
OOMScoreAdjust=-500

[Install]
WantedBy=multi-user.target
```

`/etc/systemd/system/massa-indexer.service`:

```
[Unit]
Description=Massa Indexer V2
After=network-online.target massa-node.service
Wants=network-online.target
RequiresMountsFor=/data
StartLimitIntervalSec=600
StartLimitBurst=20

[Service]
Type=simple
User=damip
Group=damip
WorkingDirectory=/data/indexer
ExecStart=/usr/local/bin/massa-indexer serve --config /data/indexer/indexer.toml
Restart=always
RestartSec=10
TimeoutStopSec=60
KillMode=mixed
StandardOutput=append:/data/indexer/logs/indexer.log
StandardError=append:/data/indexer/logs/indexer.log
Environment=RUST_LOG=info,massa_indexer=info
LimitNOFILE=1048576
LimitNPROC=infinity
OOMScoreAdjust=-300

[Install]
WantedBy=multi-user.target
```

**Deploy workflow**: build locally, then `scp` the binary to a
neutral path (e.g. `/tmp/`) and `sudo install` it into
`/usr/local/bin/massa-indexer`. Don't write to the build-tree path
on a host whose service is running — the kernel returns `ETXTBSY`
and silently leaves the host on the previous binary, which is a
particularly nasty source of "I deployed but nothing changed"
incidents. Verify the running PID's exe with
`sudo sha256sum /proc/$(systemctl show massa-indexer -p MainPID --value)/exe`
after each restart.

`/etc/systemd/system/nginx.service.d/override.conf` — bumps Ubuntu's
default `Restart=on-failure` to `always`:

```
[Service]
Restart=always
RestartSec=5
```

Why these knobs matter:

- `RequiresMountsFor=/data` on the indexer prevents it from silently
  creating RocksDB on `/` if the RAID didn't come up.
- `nofail,x-systemd.device-timeout=30` in `/etc/fstab` for `/data`
  ensures the box still boots if the array is degraded.
- `OOMScoreAdjust=-500` (node) / `-300` (indexer) tells the kernel OOM
  killer to prefer almost any other userspace process before touching
  these.
- `TimeoutStopSec=60` lets RocksDB finish flushing on a clean shutdown
  before `SIGKILL`.
- `KillMode=mixed` ensures the whole cgroup is reaped — no orphan
  threads.

### 3.5 nginx config

`/etc/nginx/sites-available/massa-explorer`:

```
upstream massa_indexer {
    server 127.0.0.1:8080;
    keepalive 32;
}

server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    root /home/damip/massahistory/massa-explorer/dist;
    index index.html;

    location / {
        try_files $uri $uri/ /index.html;          # SPA fallback
    }

    # SSE streams: long-lived, no buffering
    location ~* ^/v1/(slots/stream|stream/.*) {
        proxy_pass            http://massa_indexer;
        proxy_http_version    1.1;
        proxy_buffering       off;
        proxy_cache           off;
        proxy_read_timeout    24h;
        proxy_send_timeout    24h;
        chunked_transfer_encoding on;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    # Everything else under /v1/
    location /v1/ {
        proxy_pass http://massa_indexer;
        proxy_http_version 1.1;
        proxy_set_header Host              $host;
        proxy_set_header X-Real-IP         $remote_addr;
        proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_connect_timeout  5s;
        proxy_read_timeout     60s;
        proxy_send_timeout     60s;
    }
}
```

A reference TLS-terminating variant with rate limits is in
`nginx/indexer.conf.sample` if you ever expose this on the public
internet.

### 3.6 Kernel and limits

`/etc/sysctl.d/99-massa-indexer.conf`:

```
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
fs.file-max = 2097152
fs.aio-max-nr = 1048576
```

`/etc/security/limits.d/99-massa.conf`:

```
damip soft nofile 1048576
damip hard nofile 1048576
damip soft nproc  unlimited
damip hard nproc  unlimited
root  soft nofile 1048576
root  hard nofile 1048576
```

`/etc/systemd/system.conf.d/99-massa.conf`:

```
[Manager]
DefaultLimitNOFILE=1048576
```

NVMe scheduler: kernel default (`none`) is what we want.

### 3.7 Crash and reboot recovery — what is guaranteed

Verified on both hosts by `kill -9` and full `systemctl reboot`:

- `kill -9 massa-node`     → systemd respawns within ~10 s.
- `kill -9 massa-indexer`  → systemd respawns within ~10 s.
- `kill -9 nginx`          → systemd respawns within ~6 s.
- Power cycle / `reboot`   → all three come back **automatically**:
  - mdadm reassembles `md0` from the persisted UUID.
  - `/etc/fstab` mounts `/data` (with `nofail` so a degraded RAID never
    blocks boot).
  - systemd starts node, then indexer (after `/data` is mounted), then
    nginx.
  - The indexer reopens its existing RocksDB and resumes ingestion from
    the last final slot it had written.

Indexer DB is durable on its own; **no manual intervention is required**
after a clean or unclean restart.

---

## 4. Networking notes

- Ethernet is preferred. The MS-01's WiFi 6 radio works for setup but
  caps out around 30 MB/s; gigabit ethernet sustains ~115 MB/s.
- WiFi can be left **enabled but not autoconnected** (`nmcli con modify
  "<ssid>" connection.autoconnect no` and `nmcli con down "<ssid>"`) so
  it stays a fallback we can flip on with one command, without the
  driver actually associating at boot.
- The default NetworkManager `metric=100` for ethernet vs `600` for
  WiFi means ethernet automatically wins as the default route the
  moment a cable is plugged in.

---

## 5. Provisioning a new host (recipe)

The whole setup below is exactly what was applied to `indexer1`,
`indexer2` and `indexer3`. It's idempotent: re-running it on an
already-provisioned host won't break anything, but the kill-test at the
end will obviously restart services.

End-to-end wall-clock on the MS-01 hardware: ~20 min (mostly the cargo
build + the rsync from the dev machine).

### 5.1 Prerequisites

- Ubuntu 26.04 LTS (Server or Desktop, doesn't matter — the box runs
  headless once configured).
- A user named **`damip`** with passwordless sudo (matches the existing
  hosts; if you choose a different name, search-and-replace
  `damip` → `<your-user>` in every snippet below and in the systemd
  units).
- Two empty 4 TB NVMe drives at `/dev/nvme1n1` and `/dev/nvme2n1`. The
  OS lives on `/dev/nvme0n1`.
- An SSH key authorized for `damip@<new-host>` from the dev machine
  that holds the source tree (`/home/damip/massahistory`).

### 5.2 Variables

```bash
NEW_HOST=192.168.0.<X>          # the new box's IP
DEV_HOST=/home/damip/massahistory  # path to the source tree on dev machine
```

### 5.3 OS packages

```bash
ssh damip@$NEW_HOST '
  set -e
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get update -qq
  sudo apt-get install -y -qq \
    mdadm e2fsprogs util-linux nvme-cli smartmontools \
    build-essential pkg-config libssl-dev libclang-dev clang llvm-dev cmake \
    protobuf-compiler libprotobuf-dev \
    git curl ca-certificates xz-utils \
    jq htop iotop sysstat \
    nginx
'
```

### 5.4 Storage: RAID 0 → ext4 → /data

```bash
ssh damip@$NEW_HOST '
  set -e
  sudo mdadm --zero-superblock /dev/nvme1n1 /dev/nvme2n1 2>/dev/null || true
  sudo mdadm --create --verbose --run /dev/md0 \
    --level=0 --chunk=256 --raid-devices=2 /dev/nvme1n1 /dev/nvme2n1
  sleep 2

  sudo mkdir -p /etc/mdadm
  sudo bash -c "mdadm --detail --scan > /etc/mdadm/mdadm.conf"
  sudo update-initramfs -u

  sudo mkfs.ext4 -F -L data \
    -E stride=64,stripe-width=128,lazy_itable_init=0,lazy_journal_init=0 \
    -m 0 /dev/md0

  UUID=$(sudo blkid -s UUID -o value /dev/md0)
  sudo cp -a /etc/fstab /etc/fstab.bak
  echo "UUID=$UUID  /data  ext4  defaults,noatime,nodiratime,nofail,x-systemd.device-timeout=30  0 2" \
    | sudo tee -a /etc/fstab
  sudo mkdir -p /data
  sudo mount /data
  sudo chown damip:damip /data
  mkdir -p /data/indexer/rocksdb /data/indexer/logs
'
```

### 5.5 Kernel/userspace tunings

```bash
ssh damip@$NEW_HOST '
  set -e
  sudo tee /etc/sysctl.d/99-massa-indexer.conf >/dev/null <<EOF
vm.swappiness = 1
vm.dirty_ratio = 10
vm.dirty_background_ratio = 5
fs.file-max = 2097152
fs.aio-max-nr = 1048576
EOF
  sudo sysctl --system >/dev/null

  sudo tee /etc/security/limits.d/99-massa.conf >/dev/null <<EOF
damip soft nofile 1048576
damip hard nofile 1048576
damip soft nproc  unlimited
damip hard nproc  unlimited
root  soft nofile 1048576
root  hard nofile 1048576
EOF
  sudo mkdir -p /etc/systemd/system.conf.d /etc/systemd/user.conf.d
  printf "[Manager]\nDefaultLimitNOFILE=1048576\n" \
    | sudo tee /etc/systemd/system.conf.d/99-massa.conf >/dev/null
  printf "[Manager]\nDefaultLimitNOFILE=1048576\n" \
    | sudo tee /etc/systemd/user.conf.d/99-massa.conf >/dev/null
'
```

### 5.6 Toolchains

```bash
ssh damip@$NEW_HOST '
  set -e
  if ! command -v rustup >/dev/null 2>&1; then
    curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --default-toolchain 1.81.0 --profile minimal
  fi
  . "$HOME/.cargo/env"
  rustup toolchain install 1.81.0 --profile minimal -c rustfmt -c clippy

  if ! command -v node >/dev/null 2>&1 || \
     [ "$(node --version | cut -d. -f1 | tr -d v)" -lt 22 ]; then
    curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nodejs
  fi
'
```

### 5.7 Push the source tree

From the dev machine:

```bash
rsync -aHP --delete \
  --exclude='target/' --exclude='node_modules/' --exclude='data/' \
  --exclude='dist/' --exclude='.git/objects/pack/*.pack' \
  --exclude='/block-explorer-old/' --exclude='/massa-graph-old/' \
  $DEV_HOST/ \
  damip@$NEW_HOST:/home/damip/massahistory/
```

### 5.8 Regenerate node identity + write indexer config

```bash
ssh damip@$NEW_HOST '
  set -e
  NODE_DIR=/home/damip/massahistory/massa/massa-node/config
  if [ -f "$NODE_DIR/node_privkey.key" ]; then
    mv "$NODE_DIR/node_privkey.key" "$NODE_DIR/node_privkey.key.from-rsync.bak"
  fi

  cat > /data/indexer/indexer.toml <<TOML
[general]
network = "mainnet"

[node]
grpc_url = "http://127.0.0.1:33037"
connect_timeout_ms = 5000
keepalive_ms = 15000

[db]
path = "/data/indexer/rocksdb"
compression = "lz4"
write_buffer_size_mb = 256

[rest]
bind = "0.0.0.0:8080"
cors = ["*"]
sse_ring_buffer_size = 10000
sse_heartbeat_secs = 20
default_page_size = 25
max_page_size = 100

[streams]
filled_blocks          = true
slot_execution_outputs = true
transfers              = true

[peer]
enabled = false
TOML
'
```

### 5.9 Build

```bash
ssh damip@$NEW_HOST '
  set -e
  . "$HOME/.cargo/env"

  # node (3-4 minutes on this hardware)
  cd /home/damip/massahistory/massa
  CXXFLAGS="-include cstdint" cargo build --release \
    -p massa-node --features execution-info -j 16

  # indexer (~2 minutes)
  cd /home/damip/massahistory/massa-indexer
  CXXFLAGS="-include cstdint" cargo build --release -j 16

  # explorer dist (~1 second)
  cd /home/damip/massahistory/massa-explorer
  npm ci --no-audit --no-fund
  npm run build
'
```

### 5.10 Explorer runtime endpoint

On a LAN host the explorer talks to its own local nginx (which then
proxies `/v1/*` to the indexer on `127.0.0.1:8080`):

```bash
ssh damip@$NEW_HOST "
  sudo tee /home/damip/massahistory/massa-explorer/dist/config.js >/dev/null <<EOF
window.__MASSA_EXPLORER_CONFIG__ = {
  defaultNetwork: 'mainnet',
  endpoints: {
    mainnet:  ['http://$NEW_HOST'],
    buildnet: ['http://$NEW_HOST'],
  },
};
EOF
"
```

The build deployed to the public-gateway box at
`massa-ai.freeboxos.fr/explorer/` uses a slightly different `config.js`,
because the frontend is served under a `/explorer/` subpath and the
gateway's nginx forwards `/explorer/v1/*` upstream:

```js
// /var/www/massa-explorer/dist/config.js on the public-gateway box
window.__MASSA_EXPLORER_CONFIG__ = {
  defaultNetwork: 'mainnet',
  endpoints: { mainnet: ['/explorer'] },
};
```

The matching frontend bundle is built with `vite build
--base=/explorer/ --outDir=dist-explorer` so all asset paths in
`index.html` carry the `/explorer/` prefix. See §6.4.1 for the rollout
recipe.

### 5.11 nginx site

```bash
ssh damip@$NEW_HOST '
  set -e
  sudo rm -f /etc/nginx/sites-enabled/default
  sudo chmod a+rx /home/damip /home/damip/massahistory \
                  /home/damip/massahistory/massa-explorer

  sudo tee /etc/nginx/sites-available/massa-explorer >/dev/null <<NGINX
upstream massa_indexer {
    server 127.0.0.1:8080;
    keepalive 32;
}

server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    root /home/damip/massahistory/massa-explorer/dist;
    index index.html;

    location / { try_files \$uri \$uri/ /index.html; }

    location ~* ^/v1/(slots/stream|stream/.*) {
        proxy_pass            http://massa_indexer;
        proxy_http_version    1.1;
        proxy_buffering       off;
        proxy_cache           off;
        proxy_read_timeout    24h;
        proxy_send_timeout    24h;
        chunked_transfer_encoding on;
        proxy_set_header Host              \$host;
        proxy_set_header X-Real-IP         \$remote_addr;
        proxy_set_header X-Forwarded-For   \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    location /v1/ {
        proxy_pass http://massa_indexer;
        proxy_http_version 1.1;
        proxy_set_header Host              \$host;
        proxy_set_header X-Real-IP         \$remote_addr;
        proxy_set_header X-Forwarded-For   \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout  5s;
        proxy_read_timeout     60s;
        proxy_send_timeout     60s;
    }
}
NGINX

  sudo ln -sf /etc/nginx/sites-available/massa-explorer \
              /etc/nginx/sites-enabled/massa-explorer
  sudo nginx -t

  sudo mkdir -p /etc/systemd/system/nginx.service.d
  sudo tee /etc/systemd/system/nginx.service.d/override.conf >/dev/null <<EOF
[Service]
Restart=always
RestartSec=5
EOF
  sudo systemctl daemon-reload
  sudo systemctl restart nginx
'
```

### 5.12 systemd units

```bash
ssh damip@$NEW_HOST '
  set -e
  sudo tee /etc/systemd/system/massa-node.service >/dev/null <<UNIT
[Unit]
Description=Massa Node (with execution-info)
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=600
StartLimitBurst=20

[Service]
Type=simple
User=damip
Group=damip
WorkingDirectory=/home/damip/massahistory/massa/massa-node
ExecStart=/home/damip/massahistory/massa/target/release/massa-node -p \${MASSA_PASSWORD} -a
Environment=MASSA_PASSWORD=massahist-prod
Restart=always
RestartSec=10
TimeoutStopSec=60
KillMode=mixed
StandardOutput=append:/home/damip/massahistory/massa/massa-node/logs.txt
StandardError=append:/home/damip/massahistory/massa/massa-node/logs.txt
LimitNOFILE=1048576
LimitNPROC=infinity
OOMScoreAdjust=-500

[Install]
WantedBy=multi-user.target
UNIT

  sudo tee /etc/systemd/system/massa-indexer.service >/dev/null <<UNIT
[Unit]
Description=Massa Indexer V2
After=network-online.target massa-node.service
Wants=network-online.target
RequiresMountsFor=/data
StartLimitIntervalSec=600
StartLimitBurst=20

[Service]
Type=simple
User=damip
Group=damip
WorkingDirectory=/data/indexer
ExecStart=/usr/local/bin/massa-indexer serve --config /data/indexer/indexer.toml
Restart=always
RestartSec=10
TimeoutStopSec=60
KillMode=mixed
StandardOutput=append:/data/indexer/logs/indexer.log
StandardError=append:/data/indexer/logs/indexer.log
Environment=RUST_LOG=info,massa_indexer=info
LimitNOFILE=1048576
LimitNPROC=infinity
OOMScoreAdjust=-300

[Install]
WantedBy=multi-user.target
UNIT

  # The unit points at /usr/local/bin/massa-indexer; install it from
  # the build tree (or from a deploy package) so the path exists
  # before the service starts.
  sudo install -m 0755 -o root -g root \
    /home/damip/massahistory/massa-indexer/target/release/massa-indexer \
    /usr/local/bin/massa-indexer

  sudo systemctl daemon-reload
  sudo systemctl enable --now massa-node.service massa-indexer.service nginx.service
'
```

### 5.13 Verify

```bash
ssh damip@$NEW_HOST '
  systemctl is-active massa-node massa-indexer nginx
  curl -s http://127.0.0.1/v1/health
'
```

…then open `http://$NEW_HOST/` in a browser.

The first node bootstrap takes 5–15 minutes on a 1 GbE link (more on
WiFi). The indexer will sit in a polite "connection refused" retry loop
during that time, then start ingesting the moment gRPC port `:33037`
comes up.

### 5.14 Smoke-test crash + reboot recovery

```bash
ssh damip@$NEW_HOST '
  for svc in massa-node massa-indexer nginx; do
    PID=$(systemctl show -p MainPID --value "$svc")
    sudo kill -9 "$PID"
  done
  sleep 15
  systemctl is-active massa-node massa-indexer nginx   # all should be active again
'
```

Optional full reboot test:

```bash
ssh damip@$NEW_HOST 'sudo systemctl reboot'
# wait ~2 min, then:
ssh damip@$NEW_HOST 'systemctl is-active massa-node massa-indexer nginx'
```

---

## 6. Day-2 operations

### 6.1 Tail logs

```bash
journalctl -u massa-node -f
journalctl -u massa-indexer -f
tail -f /home/damip/massahistory/massa/massa-node/logs.txt   # raw rust tracing
tail -f /data/indexer/logs/indexer.log
```

### 6.2 Health probes

```bash
curl http://<host>/v1/health
curl http://<host>/v1/status | jq .data.last_final_slot
```

### 6.3 Restart something

```bash
sudo systemctl restart massa-indexer
sudo systemctl restart massa-node       # also picks up rebuilt binary
sudo systemctl restart nginx            # picks up nginx site changes
```

### 6.4 Rebuild from updated source

When iterating on the indexer alone (the most common case), build on
the dev machine and **ship the binary directly** instead of doing a
full source sync + on-host cargo build. This avoids the `ETXTBSY` trap
where `scp` over a running binary silently fails:

```bash
# on dev machine:
cd /home/damip/massahistory/massa-indexer
CXXFLAGS="-include cstdint" cargo build --release

# roll out one indexer at a time so peer sync can fill the restart gap
for h in 192.168.0.19 192.168.0.29; do  # add indexer3 WAN IP once :9443 is open
  echo "--- $h ---"
  scp target/release/massa-indexer damip@$h:/tmp/massa-indexer.new
  ssh damip@$h '
    sudo install -m 0755 /tmp/massa-indexer.new /usr/local/bin/massa-indexer &&
    sudo systemctl restart massa-indexer
  '
  # give the just-restarted indexer ~30 s to come back and start
  # accepting peer fills again, then move on
  for i in $(seq 1 30); do
    out=$(curl -fsS --max-time 3 "http://$h/v1/health" 2>/dev/null || true)
    [[ "$out" == *'"ok"'* ]] && break
    sleep 1
  done
done
```

The systemd unit's `ExecStart=/usr/local/bin/massa-indexer` (set in
§5.12) is the only path that matters at runtime; the binary in
`/home/damip/massahistory/massa-indexer/target/release/` is just a
deploy artifact. **Never** point the unit at the build tree — `scp`
over a running ELF returns `ETXTBSY` and the old binary keeps running,
masked by a successful-looking deploy.

For a full-stack rebuild (node + indexer + explorer) on a single host:

```bash
# on dev machine:
rsync -aHP --delete \
  --exclude='target/' --exclude='node_modules/' --exclude='data/' \
  --exclude='dist/' --exclude='.git/objects/pack/*.pack' \
  --exclude='/block-explorer-old/' --exclude='/massa-graph-old/' \
  $DEV_HOST/  damip@$HOST:/home/damip/massahistory/

# on the host:
. ~/.cargo/env
cd /home/damip/massahistory/massa
CXXFLAGS="-include cstdint" cargo build --release -p massa-node --features execution-info -j 16
cd /home/damip/massahistory/massa-indexer
CXXFLAGS="-include cstdint" cargo build --release -j 16
sudo install -m 0755 target/release/massa-indexer /usr/local/bin/massa-indexer
cd /home/damip/massahistory/massa-explorer
npm ci --no-audit --no-fund && npm run build
sudo systemctl restart massa-node massa-indexer nginx
```

### 6.4.1 Public gateway (TLS / `/explorer` subpath)

The `dist-explorer/` build (Vite `--base=/explorer/`) is also pushed
to the public-gateway box that serves
`https://massa-ai.freeboxos.fr/explorer/`. That host is reachable only
via its public SSH on a non-default port:

```bash
# on dev machine, after `vite build --base=/explorer/ --outDir=dist-explorer`:
rsync -az --delete \
  --exclude config.js --exclude robots.txt \
  dist-explorer/ \
  root@massa-ai.freeboxos.fr:/var/www/massa-explorer/dist/ \
  -e 'ssh -p 2222'
```

The two excluded files (`config.js`, `robots.txt`) are gateway-local
runtime configuration; if you ever clobber them, recreate them from the
samples kept on the gateway under `/var/www/massa-explorer/extras/`.
The gateway's nginx config forwards `/explorer/v1/*` to one of the
three LAN indexers and serves the static dist under `/explorer/`.

### 6.5 Wipe + resync the indexer DB

The indexer is **disposable**: you can blow away `/data/indexer/rocksdb/`
and it will rebuild from the node + peers.

```bash
sudo systemctl stop massa-indexer
rm -rf /data/indexer/rocksdb/*
sudo systemctl start massa-indexer
```

### 6.6 Kill WiFi (we keep ethernet preferred)

```bash
sudo nmcli con modify "<ssid>" connection.autoconnect no
sudo nmcli con down   "<ssid>"
nmcli radio wifi      # confirm: still 'enabled' (radio on, just not associated)
```

### 6.7 RAID health check

```bash
cat /proc/mdstat
sudo mdadm --detail /dev/md0
sudo smartctl -a /dev/nvme1n1 | grep -E "Percentage|Available Spare|Critical"
sudo smartctl -a /dev/nvme2n1 | grep -E "Percentage|Available Spare|Critical"
```

### 6.8 Disk failure procedure

If one of the 4 TB drives fails, the RAID 0 is gone. Procedure:

1. Replace the failed drive.
2. Re-run §5.4 (mdadm create + fstab + mkdir layout). The fstab entry
   already has the same mountpoint; just update the UUID line.
3. Re-create `/data/indexer/indexer.toml` (§5.8 last block).
4. Restart `massa-indexer` — it will re-create RocksDB and resync from
   the local node + peers.

The node and explorer are unaffected.

---

## 7. What is intentionally **not** here

A few things you'd usually expect in a production HOWTO that we skipped
on purpose:

- **No TLS on the LAN boxes.** Each indexer host listens plain-HTTP
  on `:80`. TLS is terminated upstream — on the public-gateway box
  (`192.168.0.44` / `massa-ai.freeboxos.fr`) for the public `/explorer`
  URL, and not at all for direct LAN access.
- **No authentication.** The REST API is read-only; same caveat as above.
- **No load balancer.** The three hosts are independent; the explorer's
  endpoint failover (`endpoints[]` in `config.js`) is the only thing
  that ties them together.
- **No backup of `/data/indexer/`.** Disposable by design (§6.5).
- **No staking wallets.** Each `massa-node` is a follower-only full
  node; `config/staking_wallets/` is intentionally empty so the node
  doesn't try to produce blocks.
- **No on-host MNS / name-system datastore.** The indexer resolves
  `*.massa` names on demand via a read-only call against the MNS
  smart contract (`/v1/mns/resolve` and `/v1/search?q=<label>.massa`,
  see `spec.md` §5.2 / §10.4). Nothing about the registry is
  persisted; the explorer always reflects the on-chain truth.
- **No bytecode mirror in RocksDB.** Smart-contract bytecode is
  fetched on demand from the local node by the third curated proxy
  endpoint (`/v1/addresses/:addr/bytecode`, `spec.md` §5.2 / §10.3)
  and analysed entirely in the browser (`massa-explorer/src/lib/wasm.ts`
  — section table, imports, exports, globals, data-segment strings,
  SHA-256). The indexer never stores nor disassembles bytecode; the
  one round-trip is what makes the "Download .wasm" button and the
  in-page WASM analyser tab work, and nothing else.

---

## 8. Peer sync between indexers

Every indexer briefly stops ingesting from its own node when it
restarts, and during that ~30 s window the network keeps producing
slots. The Massa node's gRPC streams use `tokio::sync::broadcast`,
which doesn't replay missed messages, so each restart leaves a
per-host gap. Mutual indexer-to-indexer sync (`spec.md` §8) closes
those gaps by pulling the missing per-slot rows from the other live
indexers.

### 8.1 Wire shape

A small `Peer` gRPC service in the indexer (`peer.proto`) exposes
three RPCs:

- `GetHealth` — returns peer id, network name, build version,
  on-disk schema version, and the peer's `last_final_slot`. Used at
  handshake time to drop wrong-network or incompatible peers.
- `GetFinalSlot(period, thread, parts)` — fetch one final slot. The
  caller asks only for the parts (`block`, `exec_output`, `transfers`)
  it's missing locally. The peer ships pre-encoded `Stored*Pb` rows
  so the response can be written into RocksDB with zero re-encoding.
- `StreamFinalSlots(from, to, parts)` — server-streaming bulk-backfill
  variant for the same data, capped per call.

Each `FinalSlotResponse` includes:

- The block + its operations + its endorsements (when `parts.block`).
- `executed_op_ids`, SC events, and async-pool rows whose
  `last_slot == this slot` (when `parts.exec_output`).
- Transfers + deferred-call rows whose `last_slot == this slot`
  (when `parts.transfers`).
- `execution_trail_hash` and `final_block_id` for cross-checks.

Apply path (`peer/patch.rs`) honours three invariants:

1. local data has priority — peer patches never overwrite an existing
   FINAL block or `execution_trail_hash`; mismatches are logged and
   the conflicting parts skipped;
2. completeness only moves forward — parts already marked present in
   `SlotCompleteness` are not re-applied (cheap repeat);
3. idempotency — replaying the same patch twice yields the same RocksDB
   state.

Crucially, `apply_peer_patch` reuses the same `db.write_block /
write_transfers / write_endorsements / …` paths as the live ingest, so
**all secondary indexes** (`idx_block_by_creator`, `idx_op_by_creator`,
`idx_op_by_target`, `idx_endorsement_by_creator`, `idx_transfer_by_addr`,
`idx_transfer_by_op`, `idx_transfer_by_block`, `idx_async_by_*`,
`idx_deferred_by_*`, `idx_sc_event_by_*`, `idx_denunciation_*`) are
populated identically whether the row arrived from the live stream or
from a peer.

### 8.2 Backfill scanner — unified backwards walker

A single periodic worker (`peer/backfill.rs::run_backfill`)
implements the entire backfill story. The contract:

- start from `last_final_slot.period` (set by the live ingest path
  every time a slot transitions to FINAL);
- for each `(period, thread)` in that period and the 31 sibling
  threads:
  - if the local row is FINAL on every operator-enabled stream →
    skip (single RocksDB get + a few bool checks; no RPC);
  - if the row is `Candidate` → skip (live stream is still
    settling it);
  - else issue an RPC to peers (round-robin) for the missing
    parts; on hit, ship the response as `Event::PeerPatch`;
  - if no peer can supply the slot, **write nothing** and move on
    (case (a) of the design — no per-slot "tried, no source"
    bookkeeping);
- after each RPC, sleep `rate_limit` (default 50 ms);
- when the worker reaches `(0, 0)`, sleep `wrap_pause` (default
  30 s) and start the next sweep from the new head.

A complete sweep over 145 M mainnet slots is essentially a
sequential RocksDB iteration once coverage is dense — about 12
minutes on the production hardware. The sleep only fires on RPC
paths, so the steady-state cost is dominated by `read_slot` calls
which are ≈5 µs each.

**Bulk range path.** The walker scans 16-period windows (512 slots)
locally first. A densely-missing window (≥ 24 slots) is pulled with
one `StreamFinalSlots` range call per logical peer — hundreds of
slots per second instead of one slot per round-trip. Only locally
missing slots are applied; each apply sleeps `apply_pause` (2 ms) so
bulk catch-up cannot starve the live ingest channel. Sparse windows
and small leftovers (e.g. slots only reachable via a session-only
peer) still use the per-slot path; windows nobody can supply cost a
couple of instantly-empty streams per sweep. Because
`StreamFinalSlots` predates this client change, rolling upgrades are
safe in any order: old servers already serve the stream, old clients
keep fetching per-slot.

**What this design replaces.** Three older mechanisms were folded
into this single walker:

- a newest-first scanner that only re-visited slots already in
  `cf_slot` and could never reach genuine gaps;
- a dedicated history filler that walked
  `lowest_known_period - 1` downward and could outrun the data
  source, leaving permanent "swiss-cheese" gaps;
- the legacy DDB fallback in the regular scanner — AWS is now a
  one-shot importer (§9), not a runtime fallback.

The current worker is `O(slot_count)` per sweep regardless of how
much new data the live stream produced; concurrency with live
ingest is handled by `apply_*_patch`'s first-final-wins idempotency
guarantees.

### 8.3 Configuration (in `/data/indexer/indexer.toml`)

Each host binds on `0.0.0.0:9443` and lists the other two as peers.
Verbatim, on `indexer1`:

```toml
[peer]
enabled  = true
bind     = "0.0.0.0:9443"
peer_id  = "indexer1"
# Sleep between successive peer RPCs by the backfill scanner.
# Skip paths (slot already complete-FINAL or speculative) are
# free; the rate limit only applies on RPC-issuing iterations.
scan_interval_ms  = 50

[peer.peers.indexer2]
url = "http://192.168.0.29:9443"

# indexer3 is off-site — enable once WAN :9443 is reachable:
# [peer.peers.indexer3]
# url = "http://86.205.18.20:9443"
```

> **Migration note (May 2026):** `batch_size` and
> `max_rpcs_per_pass` from the previous schema are gone — the new
> scanner doesn't batch and doesn't cap per-pass RPCs. The TOML
> parser ignores unknown fields, so leaving them in the file is
> harmless during the rolling restart but they should be removed
> on the next config bump.

…symmetrically on `indexer2` (peers: indexer1 + indexer3) and
`indexer3` (peers: indexer1 + indexer2). To add a fourth host you'd
list the new IP under each existing host's `[peer.peers.*]` and add a
`[peer]` block for the new host listing all three existing IPs.

Switching peer mode on/off requires only `systemctl restart
massa-indexer` — never restart the node (bootstrap is hazardous,
node DB is durable on its own).

### 8.4 Operational notes

- ufw is `inactive` on the production boxes; the LAN is trusted.
  When/if these go on the public internet, port `:9443` should be
  firewalled to the LAN side or wrapped in TLS / WireGuard.
- The peer protocol carries an on-disk `schema_version`; when bumped,
  peers running an older schema are dropped at handshake time and the
  operator must roll the rebuild.
- Useful Prometheus counters at `http://<host>/v1/metrics`:
  - `massa_indexer_backfill_passes_total`     — scan passes completed.
  - `massa_indexer_backfill_rpcs_total`        — gap-slots targeted.
  - `massa_indexer_backfill_slots_filled_total` — RPCs that returned
    `final_known = true`. Should equal `…rpcs_total` once the chain
    is fully synced; the lag is the not-yet-filled gap. (With the bulk
    range path one stream RPC can fill hundreds of slots, so
    `filled_total` may legitimately exceed `rpcs_total` during
    catch-up.)
  - `massa_indexer_backfill_range_streams_total` — bulk
    `StreamFinalSlots` range calls issued by the walker.
  - `massa_indexer_ingest_peer_patches_total`  — patches applied.
- `journalctl -u massa-indexer | grep -E "peer|backfill"` is the
  fastest way to see who connected to whom and how many slots were
  filled in the last minute.

---

## 9. Legacy block-storer (DynamoDB) one-shot importer

A live Massa node can only ship slots that arrived on its gRPC
broadcast streams since the moment it last bootstrapped. Anything
older than that is invisible to the indexer's regular ingest path.
The deep mainnet history before any of these boxes was first turned
on lives in the legacy "block-storer" project's AWS DynamoDB tables;
this section documents how we pull that archive into the cluster
exactly once.

### 9.1 Why a one-shot, not a fallback

A previous iteration of this feature wired AWS DDB into the regular
peer-backfill scanner as a last-resort source. It worked, but had
two structural problems:

- **Cost.** DDB reads are billable. Every time the regular scanner
  asked AWS for a slot the cluster already had, the operator paid
  for nothing. Even with rate limits and skip-if-already-have
  guards, the steady-state cost was non-zero.
- **Lifetime.** The legacy archive does not grow — the storer was
  decommissioned around period 4.55 M and nothing has been written
  since. The whole import is bounded; once one indexer in the
  cluster has it, the others learn it through normal peer sync for
  free, forever.

So the design is now:

- The regular **peer-backfill scanner** (§8 / §10 below) is
  peers-only. It walks every slot from `last_final_slot` → `(0, 0)`
  and asks peers for missing or incomplete-FINAL slots. It never
  consults AWS, regardless of credentials.
- A separate, **one-shot importer** (this section) is opted into
  via `[legacy_ddb] enabled = true`. It runs at startup as a
  background task, walks the legacy archive head → genesis exactly
  once, ships every recoverable row into RocksDB, then exits.
  Operators flip `enabled = false` after the run completes.

### 9.2 Precedence (lowest)

Three sources can produce a final-slot row in `cf_slot`:

1. **Local node, live stream.** Always wins. The only source that
   produces SC events, async-pool rows, deferred calls, slot trail
   hashes, miss attribution and the precise per-stack-frame
   `CoinOrigin` for every transfer.
2. **Indexer peers (§8).** A `FinalSlotResponse` from a peer
   carries the same per-slot data its origin live-stream produced —
   i.e. it can refill everything live-stream can.
3. **Legacy DDB (one-shot importer).** Carries blocks, ops,
   endorsements, denunciations, `executed_op_ids` plus transfers
   reconstructible from `OperationsMainnet` (top-level Type=Transfer
   ops AND every sub-transfer row). Lowest priority, additive only.

`peer::patch::apply_legacy_patch` enforces the precedence at
write-time:

- never overwrites an existing FINAL block or
  `execution_trail_hash`;
- never marks `exec_output_final` or `transfers_stored` —
  legacy can't supply SC events, async-pool, deferred calls, or
  the precise reward / slash origin, so we leave those bits unset
  and let a live-stream / peer ship contribute them later;
- only sets `block_body_stored = true` so the imported block isn't
  re-fetched on subsequent runs.

### 9.3 The unified backfill scanner (peer side)

Lives in `src/peer/backfill.rs`. One worker, one loop. The contract:

- Read `last_final_slot.period` from `cf_meta`. Idle until it's set
  (waits for the live stream's first FINAL).
- Walk `(period, thread)` from `last_final_slot.period` down to
  `(0, 0)`, then sleep `wrap_pause` and start the next sweep from
  the new head.
- For every slot:
  - if it's complete-FINAL on every operator-enabled stream → skip
    (cheapest path: a single RocksDB get + a few bool checks);
  - if it's `Candidate` → skip (live stream is still settling it);
  - else ask peers (round-robin) for the missing parts.
- If no peer can supply the slot, **write nothing and move on** —
  the next sweep retries naturally. This is case (a) of the design:
  we do not record per-slot "tried, no source" metadata that would
  need eventual cleanup.

A complete sweep over 145 M slots is roughly a sequential RocksDB
iteration — about 12 minutes on the production hardware once
coverage is dense. The `rate_limit` (50 ms by default) only fires on
slots that issue an RPC; skip paths are free. Net effect: the worker
spends most of its time confirming the cluster is converged, then
fans out a small burst of RPCs whenever a new gap is found.

### 9.4 The one-shot AWS importer

Lives in `src/legacy/oneshot.rs`. Spawned at indexer startup iff
`config.legacy_ddb.enabled = true` AND the AWS client construction
succeeds.

Algorithm:

1. Block until the indexer has a `last_final_slot`. (No live head
   means no business importing legacy data — we'd risk
   non-deterministic head selection.)
2. Compute the start period: `cfg.max_period.unwrap_or(head)`.
3. Walk `(period, thread)` from start down to `(0, 0)`:
   - read the local row;
   - **skip** if FINAL with `is_miss = true`, OR FINAL with
     `transfers_stored = true` (the *only* completeness flag
     that's both written together with the actual transfer batch
     AND set by live ingest / peer patch but never by
     `apply_legacy_patch`). Specifically we do NOT skip on:
     - `block_body_stored = true`: an earlier version of this
       importer shipped the block body without ABI sub-transfers,
       so a slot can be `block_body_stored=true` but missing every
       transfer that should ride with it.
     - `exec_output_final = true`: the live ingest's transfers
       handler is a SEPARATE pipeline from the exec-output handler.
       A slot can therefore end up `exec_output_final=true` AND
       `transfers_stored=false` if the indexer joined the live
       stream a few slots after the exec_output for that slot was
       emitted (the node does not re-emit transfers on resume).
       Those slots are exactly the ones the legacy importer is
       supposed to fill; skipping them would leave them empty
       forever.
     Cost is bounded: as soon as a peer's `apply_peer_patch` (or
     live ingest) flips `transfers_stored` to true, the importer
     stops hitting AWS for that slot.
   - else call `LegacySource::fetch_slot(period, thread)`. On hit,
     ship `Event::LegacyPatch` (the ingest worker applies it via
     `apply_legacy_patch`). On miss, write nothing.
4. Sleep `cfg.rate_limit` between successive AWS RPCs. Errors get
   an extra 1-second backoff to avoid hammering a degraded
   endpoint.
5. When the worker reaches `(0, 0)`, log `IMPORT COMPLETE` and
   exit. The operator flips `enabled = false` and restarts so the
   importer doesn't re-launch on subsequent boots.

The importer is **non-blocking by construction**. It runs in its
own tokio task and shares only the ingest `EventTx` channel with
the live-stream / peer workers. Live ingest keeps making forward
progress on the head while the importer fills the historical tail
from the bottom.

Resume guarantee: because skips are pure (no DDB call issued),
restarting the importer mid-run is cheap. Roughly: a fully-imported
range costs one `read_slot` per slot, no AWS traffic.

### 9.5 What we recover from DDB (per slot)

For each `(period, thread)` the importer issues:

1. `Query` against `BlocksMainnet.PointInTimeAndHashIndex` — at
   most one row per slot (the legacy storer kept the FINAL block
   only). The row's `Raw` is the full `BlockWrapper` proto; we
   decode `StoredBlock`, embedded `StoredEndorsement[]` and
   `StoredDenunciation[]` from it.
2. Optional `GetItem` against `BlocksMainnet[Hash]` — only if the
   GSI projection happens to be missing `Raw` (rare; legacy
   default projection includes it).
3. `BatchGetItem` against `OperationsMainnet[Hash, …]` — fanned
   out from `block.operation_ids`. Returns top-level operation
   rows with full `Raw` for `OperationWrapper` decoding.
4. `Query` against `OperationsMainnet.PointInTimeAndHashIndex` —
   returns *every* row for the slot, including sub-transfer rows
   (Hash ending in `_<n>`). The projection covers `Type`, `Status`,
   `Amount`, `Fee`, `CreatorAddress`, `OtherAddress`,
   `OriginalOperationID`, `SlotAndAscIndex` — enough to build a
   `StoredTransfer` without a second round-trip.

Sub-transfer reconstruction (the new bit). Each row with a `_` in
its hash represents one coin movement that happened during slot
execution:

- **Op-driven** (`OriginalOperationID` set): an ABI sub-transfer
  emitted by a CallSC, an explicit fee transfer, an internal coin
  movement. Decoded with `origin = AbiTransferCoins` (the umbrella
  bucket — legacy didn't store the precise enum).
- **Slot-bound** (`SlotAndAscIndex` set, `OriginalOperationID = 0`):
  block reward, endorsement reward, slash transfer, async-msg coin
  movement. Decoded with `origin = BlockReward` (most common
  bucket).

Live-stream / peer ships override these heuristics later if richer
data arrives. The `transfers_stored` completeness bit stays unset
on legacy-filled slots precisely so this can happen.

#### Anti-duplicate invariant: synth vs sub-transfer row

The legacy importer can produce a `StoredTransfer` for the same
economic event from two distinct sources:

1. A **synthesised** transfer for every successfully-executed
   `Type=Transaction` top-level op. ID = `<op_id>:0`,
   `origin = OpTransactionCoins`. This exists because mainnet
   Type=Transaction ops have **no** `<op_id>_N` companion in
   `OperationsMainnet` — the top-level row IS the transfer record.
2. A **sub-transfer row** with `Hash = <op_id>_N`, decoded into a
   transfer with ID = `<op_id>_N`, `origin = AbiTransferCoins`. This
   is what we use for CallSC coin movements, ABI sub-transfers,
   fee transfers, etc.

To prevent double-counting if the legacy storer ever emitted both
shapes for the same op (i.e. a top-level row AND a `<op_id>_N` row),
`fetch_slot` walks the slot's `OperationsMainnet` rows once before
synthesising, builds the set of `OriginalOperationID` values
referenced by `_N` rows, and skips synthesis for any top-level op
whose id is in that set. The matching unit tests in
`legacy::source::tests` cover the three relevant shapes:

- `_N` row with a real `OriginalOperationID` ⇒ suppress synth.
- `_N` row with `OriginalOperationID = "0"` ⇒ slot-bound, do NOT
  suppress (the synth and the row describe different events).
- Top-level row (no `_`) ⇒ never suppresses anything.

A live spot-check of mainnet DDB confirmed Type=Transaction ops
**never** carry a `_N` companion (sample size 30, hit rate 0%), so
in practice the guard never triggers — but the invariant remains
machine-checked.

The downstream `apply_legacy_patch` writes every shipped transfer
unconditionally (same `(period, thread, index_in_slot)` key
overwrites in place). This is **deliberate**: re-importing the
same slot with a newer decoder version (e.g. one that derives
`operation_id` from the `<op_id>_N` hash prefix when the DDB
column was left blank) UPGRADES the existing row without an
explicit "wipe and re-fill" step. Live and legacy IDs use disjoint
shapes (`<op>:N` vs `<op>_N`), so the upgrade only ever fires
between successive legacy passes — once `transfers_stored` flips
to `true` because of a peer or live ship, the importer skips the
slot altogether.

#### Sub-transfer parent-op derivation

Mainnet DDB rows for `<op_id>_N` sub-transfers commonly leave the
`OriginalOperationID` and `SlotAndAscIndex` columns blank. The
decoder therefore parses the parent op id out of the
`Hash` itself (`<op_id>_<n>` ⇒ split on the first `_`) and, when
that succeeds, classifies the row as op-driven (origin
`AbiTransferCoins`) and stores the parent in
`StoredTransfer.operation_id`. This is what makes
`/v1/operations/<op_id>/transfers` return the legacy sub-transfers
even on the deeply-historical part of the chain. Slot-bound rows
(no `_` in the hash) keep their original heuristic of
`origin = BlockReward` (best-fit umbrella bucket).

### 9.6 What we don't fill from legacy

| Field on `StoredBlock`/`StoredOperation`/`SlotState`  | Why missing       |
|-------------------------------------------------------|-------------------|
| Slot `execution_trail_hash`                           | not in legacy    |
| SC events / async-pool / deferred-call rows           | not in legacy    |
| `CoinOrigin` for op-driven sub-transfers              | collapsed to `AbiTransferCoins` (umbrella) — legacy can't distinguish `AbiCallCoins` etc. |
| `CoinOrigin` for slot-bound sub-transfers             | collapsed to `BlockReward` — legacy can't distinguish reward vs slash vs async-msg |
| `transfers_stored = true`                             | left unset on purpose so peer ship can fill it |
| `exec_output_final = true`                            | left unset on purpose so peer ship can fill it |
| Synthesised transfer of a `Type=Transaction` op       | done via `legacy_op_to_transfer` (id `<op_id>:0`, origin `OpTransactionCoins`) — fee + storage refunds NOT separately materialised |
| Per-op fee transfer for `RollBuy` / `RollSell` / `CallSC` / `ExecuteSC` | not stored as a sub-transfer row in legacy; the top-level row carries the amount but not a separate fee-transfer record |

These are not bugs. They are explicit "leave room for a later peer
patch" decisions enforced at the storage layer.

### 9.7 Configuration

**a) The `[legacy_ddb]` section** in `/data/indexer/indexer.toml`:

```toml
[legacy_ddb]
# Inclusive upper bound. Slots with `period > max_period` are
# skipped — the legacy storer's last write was around period
# 4.55M, so anything more recent is guaranteed to miss. Saves AWS
# RPCs for nothing.
max_period    = 4550000

# Pause between successive per-slot DDB queries. 50 ms ≈ 20 RPS
# at 32 threads/period ≈ 0.6 periods/sec — well under the table's
# provisioned read capacity.
rate_limit_ms = 50
```

The `enabled / region / access_key_id / secret_access_key /
session_token` fields are intentionally **absent from the TOML**
so the secret never lands in a file we'd accidentally rsync. They
come from a systemd `EnvironmentFile=` instead — see (b).

**b) systemd drop-in** (only on the host running the import —
`indexer1` today):
`/etc/systemd/system/massa-indexer.service.d/legacy.conf`:

```
[Service]
EnvironmentFile=/etc/default/massa-indexer-legacy
```

`/etc/default/massa-indexer-legacy` (root-only readable):

```
INDEXER_LEGACY_DDB_ENABLED=true
INDEXER_LEGACY_DDB_REGION=eu-west-3
INDEXER_LEGACY_DDB_ACCESS_KEY_ID=AKIAxxxxxxxxxxxxxxxx
INDEXER_LEGACY_DDB_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

`config::apply_env` reads each `INDEXER_LEGACY_DDB_*` and folds it
into the `[legacy_ddb]` struct before validation runs, so the
file on disk stays credential-free.

### 9.8 Cluster topology

| Host       | DDB credentials | `[legacy_ddb] enabled` | Source of legacy slots |
|------------|-----------------|------------------------|------------------------|
| `indexer1` | yes (AWS, host env only) | **false** after one-shot finished | direct from AWS (done) |
| `indexer2` | no                       | false                             | peer sync from indexer1|
| `indexer3` | no                       | false                             | peer sync when WAN up  |

A single AWS-credentialled box is the design choice — running the
importer on all three would triple the AWS bill for no gain. Once
`indexer1` finishes its sweep, `indexer2` and `indexer3` pull the
same data via the peer-backfill scanner at LAN speed — about
35–50 slots/sec/box, free.

After the run completes (the importer logs `IMPORT COMPLETE`), the
operator should flip `enabled = false` on `indexer1` and restart
the indexer service. Subsequent boots won't re-launch the importer;
the imported rows stay put forever.

### 9.9 Operations

**Run the importer on a new host.** Drop the credentials env file
in place, install the systemd drop-in, add the `[legacy_ddb]` block
to `indexer.toml`, then `systemctl restart massa-indexer`. **Do
not** restart `massa-node` — bootstrap is hazardous and unrelated.

**Stop the importer cleanly.** `systemctl stop massa-indexer`.
Mid-run termination is safe: the worker drops its `EventTx` half
the moment the channel closes, and the `apply_legacy_patch` is
idempotent so half-applied slots are simply re-applied on the
next run.

**Permanently disable.** Set `INDEXER_LEGACY_DDB_ENABLED=false` (or
remove the env file entirely) and restart. The worker isn't
spawned. Existing legacy-sourced rows in RocksDB are untouched.

**Useful Prometheus counters at `http://<host>/v1/metrics`:**

- `massa_indexer_legacy_ddb_rpcs_total` — queries issued to AWS
  (sum across all RPCs: BlocksMainnet, OperationsMainnet, etc.).
- `massa_indexer_legacy_ddb_slots_filled_total` — slots imported
  successfully (one increment per `LegacyPatch` shipped).
- `massa_indexer_legacy_ddb_errors_total` — non-zero is a sign of
  AWS throttling, expired credentials, or DDB transient outages.
- `massa_indexer_ingest_legacy_patches_total` — patches applied
  via the legacy code path on this host. Should equal
  `…slots_filled_total` minus discards from
  `apply_legacy_patch`'s precedence checks.

**Cost ceiling.** With the default `rate_limit_ms = 50`, the
importer issues at most 5 DDB RPCs per slot × 20 slots/sec = 100
RPCs/sec sustained. Across the full mainnet history (≈ 145 M
slots) that's ≈ 7-8 days of continuous reads — bounded, finite,
and well inside the table's provisioned capacity.

### 9.10 Recovering from a transient AWS outage

The importer **does not retry** a slot that errored — it logs a
`AWS one-shot: fetch errored` line, increments
`massa_indexer_legacy_ddb_errors_total`, and walks on. This keeps
the worker from getting stuck on a single bad slot, but it also
means a sustained DDB outage leaves a contiguous range of
historical slots un-imported. The peer scanner can't fill them
either: indexer2 / indexer3 only ever see what indexer1 has shipped,
and indexer1 has nothing.

The recovery procedure is a **bounded re-walk** of just the affected
range. Use the `min_period` / `max_period` knobs:

1. **Find the gap.** Grep the importer log for the error band:

   ```bash
   ssh damip@indexer1 "
     grep 'AWS one-shot: fetch errored' /data/indexer/logs/indexer.log \
       | sed -E 's/\x1B\[[0-9;]*[a-zA-Z]//g' \
       | grep -oE 'period=[0-9]+' | grep -oE '[0-9]+' \
       | sort -un | awk '
           NR==1 {prev=\$1; start=\$1; next}
           \$1-prev>10 {printf \"%d–%d (%d)\\n\", start, prev, prev-start+1; start=\$1}
           {prev=\$1}
           END {printf \"%d–%d (%d)\\n\", start, prev, prev-start+1}
         '"
   ```

   The output is the list of contiguous error ranges. Pick the one
   you want to retry (typically: all of them).

2. **Note where the deep walk currently is.** Grep the last
   `current_period=…` line in the log; write that down — you'll need
   it to resume the deep walk after the targeted re-import.

3. **Patch `indexer.toml` on indexer1 only** to the retry bounds:

   ```toml
   [legacy_ddb]
   max_period = <upper bound of the error band>
   min_period = <lower bound of the error band>
   ```

   Both are inclusive. The importer will walk
   `max_period → min_period` and exit cleanly the moment it crosses
   `min_period`.

4. **Restart indexer1**: `sudo systemctl restart massa-indexer`. The
   importer logs `AWS one-shot importer starting … min_period=Some(L)
   max_period=Some(U)` and proceeds. At 50 ms per slot × 32 threads
   that's ≈ 1.6 s per period, so a 1000-period gap takes ≈ 25 min.

5. **Wait for `AWS one-shot importer COMPLETE`** in the log.

6. **Reset the config to resume the deep walk** by patching back to
   `min_period = 0` and `max_period = <noted position − a few periods
   of safety margin>`. Restart indexer1 again. The deep walk picks
   up where it left off (with the safety margin of repeated periods
   getting quick-skipped on the next sweep).

7. **Peer propagation is automatic.** Indexer2 and indexer3 pull the
   newly-imported slots via their backfill scanner at the next pass
   over that range — empirically within a couple of minutes for a
   gap of a thousand periods, since the scanner is already nearby.
   No restart of indexer2 / indexer3 is needed.

**Cost.** A targeted re-walk is bounded to just the affected
periods — typically ≪ $1 of DDB charges. Far cheaper than letting
the importer restart from head and re-walking every already-imported
slot.
