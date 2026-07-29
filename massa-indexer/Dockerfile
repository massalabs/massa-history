# syntax=docker/dockerfile:1.6
#
# Production Dockerfile for `massa-indexer` (multi-stage, non-root, healthcheck).
#
# Build context MUST be the workspace root (the directory containing both
# `massa-indexer/` and `massa-proto/`). `massa-indexer/build.rs` compiles
# protos from `../massa-proto/proto`, so we need both trees available.
#
# Usage:
#   docker build -f massa-indexer/Dockerfile -t massa-indexer:local .
#
# The image exposes:
#   * 8080  -> REST + SSE
#   * 9443  -> peer gRPC
# and reads `/etc/massa-indexer/indexer.toml` + uses `/data` as RocksDB home.
#
# A bind-mount of an `indexer.toml` into `/etc/massa-indexer/indexer.toml:ro`
# and a named (or bind) volume at `/data` is the expected operator workflow.

# ---- builder -----------------------------------------------------------------
# Pinned to match massa-indexer/rust-toolchain.toml (1.81.0).
FROM rust:1.81.0-slim-bookworm AS build

# C++ deps: rust-librocksdb-sys + some transitive C++ code bundle their own
# sources and need a full C/C++ toolchain + clang (for bindgen). GCC 14+ no
# longer transitively includes <cstdint>; the `-include cstdint` flag below
# papers over that exactly like run_node.sh does.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        build-essential \
        clang \
        cmake \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ENV CXXFLAGS="-include cstdint"

WORKDIR /src

# Copy just the two trees we need. We do NOT copy the massa/ node clone; the
# indexer doesn't need it. The trailing `massa-proto/` context is fed as a
# sibling directory so `../massa-proto/proto` resolves correctly from inside
# `massa-indexer/`.
COPY massa-proto/  /src/massa-proto/
COPY massa-indexer/ /src/massa-indexer/

WORKDIR /src/massa-indexer

# `--locked` would be nicer but we don't ship a Cargo.lock in the crate;
# release builds ride the version pins in Cargo.toml which are already
# tight.
RUN cargo build --release --bin massa-indexer \
    && strip /src/massa-indexer/target/release/massa-indexer || true

# ---- runtime -----------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# `tini` as pid-1 so SIGTERM / SIGINT reach the indexer cleanly; `ca-certificates`
# for TLS gRPC to mainnet nodes; `tzdata` so the image logs in the container's
# timezone rather than UTC-only. `curl` is NOT shipped -- the binary exposes a
# built-in `healthcheck` subcommand, which avoids a second ~8MB install.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

# Non-root user. UID/GID 10001 matches the generic "untrusted service" range
# used by most of our fleet. `/data` is chowned so the bind-mount receives
# correct ownership on first boot (docker respects the volume's chmod).
RUN groupadd --system --gid 10001 massa \
    && useradd --system --uid 10001 --gid massa --home /data --shell /sbin/nologin massa \
    && mkdir -p /data /etc/massa-indexer \
    && chown -R massa:massa /data

COPY --from=build /src/massa-indexer/target/release/massa-indexer \
     /usr/local/bin/massa-indexer

# Shipped as a template; operators overwrite via bind-mount.
COPY --chown=root:root massa-indexer/config/indexer.toml \
     /etc/massa-indexer/indexer.toml

VOLUME ["/data"]
EXPOSE 8080 9443

# Structured JSON logs are usually what k8s/Loki/Cloudwatch want; flip with
# `-e RUST_LOG_JSON=0` if you prefer pretty output locally.
ENV RUST_LOG=info,massa_indexer=info \
    RUST_LOG_JSON=1 \
    RUST_BACKTRACE=1 \
    TZ=UTC

USER massa:massa

# Liveness probe: hit the built-in REST /v1/health. No network tools needed.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/local/bin/massa-indexer", "healthcheck", \
         "--config", "/etc/massa-indexer/indexer.toml"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/massa-indexer"]
CMD ["serve", "--config", "/etc/massa-indexer/indexer.toml"]
