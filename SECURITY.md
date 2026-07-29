# Security posture

This document covers the threat model for a production deployment of
`massa-indexer` + `massa-explorer` and the hardening steps that the code,
Dockerfiles, and sample nginx configuration already take on your behalf.
Read the operational sections of
[`massa-indexer/README.md`](./massa-indexer/README.md) and
[`massa-explorer/README.md`](./massa-explorer/README.md) alongside this file.

## Threat model

| Actor | Access | Concern | Mitigation |
| --- | --- | --- | --- |
| Anonymous internet user | `https://indexer.example.com/v1/*` | Resource exhaustion via expensive endpoints (exports, full-range charts), scraping | nginx per-IP rate limits on `/v1/*` (30 req/s) and `/v1/export/*` (2 req/s); every RocksDB read is pre-authorized to a hard page-size cap of 500. |
| Anonymous internet user | `https://explorer.example.com/` | XSS via indexer content, drive-by trackers | No third-party analytics. `X-Content-Type-Options: nosniff`, `X-Frame-Options: SAMEORIGIN`, `Referrer-Policy: strict-origin-when-cross-origin`, `Permissions-Policy: geolocation=(), microphone=(), camera=()` set by nginx. React renders all indexer data as text, never as HTML. |
| Sibling indexer (peer) | `grpc://indexer-east:9443` | Hostile peer poisoning the local DB | Peer responses are verified against the local slot hash before being applied. A peer that declares a different `network` is dropped. Forks are recorded, not merged. |
| Operator of a compromised node | gRPC stream | Bad data injected through the node's public API | Indexer stores everything the node reports; it never trusts beyond the node. If you don't trust your node, you can't trust the indexer that indexes it. |
| Local user on the indexer host | Shell access | Read of RocksDB, exfiltration of logs | Indexer runs as `uid=10001` in the Docker image. Give the user exclusive access to `/data/rocksdb` via file-system permissions; treat logs as sensitive (they contain addresses). |

## Non-goals

* **Authentication / authorization.** All REST endpoints are public. If you
  need to restrict access, front the indexer with a gateway that enforces
  mTLS, an API key, or network-level ACLs.
* **Secret management.** The indexer stores no secrets. Keep the node's
  private keys out of the indexer's container entirely.
* **Client-side rate limits.** The explorer assumes an operator-controlled
  network path to the indexer; it doesn't try to throttle browser traffic.

## Default hardening applied

### Backend Docker image
* Multi-stage build drops the full Rust toolchain.
* Runs as a dedicated `uid=10001` non-root user.
* `tini` as pid-1 so SIGTERM / SIGINT propagate and Ctrl-C works in local dev.
* HEALTHCHECK uses the binary's built-in `healthcheck` subcommand — no extra
  packages (curl/wget/bash/jq) installed.
* Default logs are structured JSON (`RUST_LOG_JSON=1`) and rotated by the
  docker daemon.
* Release binary is stripped and built with thin LTO.

### Frontend Docker image
* Served by `nginxinc/nginx-unprivileged:1.27-alpine` running as the
  `nginx` user on port 8080 (no CAP_NET_BIND_SERVICE required).
* Static assets have immutable 1-year cache; HTML and `config.js` are
  `no-store`.
* `server_tokens off;`, locked-down methods (`GET`, `HEAD`, `OPTIONS`).

### Reverse proxy (sample config)
* HSTS enabled (`max-age=63072000; includeSubDomains`).
* Per-IP rate limits split between normal reads (30 req/s) and bulk
  exports (2 req/s).
* Long-lived SSE streams have buffering disabled and a 24-hour read timeout,
  short-circuiting Nginx's default idle-close behavior.

## Reporting a vulnerability

Open a private issue on the repository or email the maintainer listed in
`Cargo.toml`. Please include a reproduction and, if possible, a patch. We
aim to acknowledge within 48 hours and ship a fix within 14 days for
reproducible high/critical issues.
