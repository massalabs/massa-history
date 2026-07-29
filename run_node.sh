#!/usr/bin/env bash
#
# run_node.sh — build and run a local massa-node (detached) configured to feed the indexer.
#
# Behavior:
#   - Kills any previously running massa-node (uses a PID file first, then
#     falls back to matching the binary path).
#   - Builds release with `--features execution-info` if the binary is missing
#     (or if MASSA_FORCE_REBUILD=1).
#   - Starts the node as a fully detached background process (nohup + setsid),
#     with stdout+stderr written to massa/massa-node/logs.txt (gitignored).
#   - Writes the child's PID to massa/massa-node/run_node.pid (gitignored).
#   - Returns immediately so you can keep working.
#
# Tail the logs with:   tail -f massa/massa-node/logs.txt
# Stop the node with:   ./run_node.sh stop
# Check status with:    ./run_node.sh status
#
# Nothing inside massa/ is modified except gitignored paths. This workspace's
# own .gitignore also excludes massa/ entirely.

set -euo pipefail

# -- paths --
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASSA_DIR="${SCRIPT_DIR}/massa"
NODE_DIR="${MASSA_DIR}/massa-node"
BIN_PATH="${MASSA_DIR}/target/release/massa-node"
PID_FILE="${NODE_DIR}/run_node.pid"
LOG_FILE="${NODE_DIR}/logs.txt"

# -- tunables --
: "${MASSA_PASSWORD:=massahist-dev}"          # wallet password for staking keys
: "${MASSA_FEATURES:=execution-info}"         # cargo --features
# `execution-info` is a strict superset of `execution-trace`: it additionally
# enables `NewTransfersInfoServer`, which emits the full `ExecTransferInfo`
# (coin origin, transfer id, per-transfer operation / async-message /
# deferred-call id, and the typed transfer value: Rolls | Coins |
# DeferredCredits). This is what the indexer subscribes to for the transfers
# index; the older `NewSlotTransfers` carries far less information and is
# not used anymore.
: "${MASSA_BUILD_JOBS:=$(nproc 2>/dev/null || echo 4)}"
: "${MASSA_ACCEPT_CHARTER:=1}"                # 1 => pass -a
: "${MASSA_FORCE_REBUILD:=0}"                 # 1 => rebuild even if binary exists

cmd="${1:-start}"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

is_pid_running() {
  local pid=$1
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid_file() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null | tr -d '[:space:]')"
  [[ -n "$pid" ]] || return 1
  printf '%s' "$pid"
}

find_massa_node_pids() {
  # Match processes whose comm is exactly 'massa-node' (so this script,
  # shells that mention the path, or 'cargo build' are never matched).
  pgrep -x massa-node 2>/dev/null || true
}

stop_node() {
  local stopped=0

  local pid=""
  pid="$(read_pid_file || true)"
  if is_pid_running "${pid:-}"; then
    echo "[run_node] stopping pid ${pid} (from PID file)"
    kill "$pid" 2>/dev/null || true
    stopped=1
  fi

  # also sweep any stragglers matching the binary path
  local others
  others="$(find_massa_node_pids || true)"
  if [[ -n "$others" ]]; then
    echo "[run_node] stopping stragglers: $others"
    # shellcheck disable=SC2086
    kill $others 2>/dev/null || true
    stopped=1
  fi

  if (( stopped )); then
    # wait up to 15s for graceful shutdown
    for _ in $(seq 1 30); do
      sleep 0.5
      local still
      still="$(find_massa_node_pids || true)"
      [[ -z "$still" ]] && break
    done
    local still
    still="$(find_massa_node_pids || true)"
    if [[ -n "$still" ]]; then
      echo "[run_node] SIGKILL stragglers: $still"
      # shellcheck disable=SC2086
      kill -9 $still 2>/dev/null || true
    fi
  fi

  rm -f "$PID_FILE"
}

status_node() {
  local pid=""
  pid="$(read_pid_file || true)"
  if is_pid_running "${pid:-}"; then
    echo "[run_node] running (pid=${pid})"
    ps -o pid=,etime=,cmd= -p "$pid" 2>/dev/null || true
    return 0
  fi
  local others
  others="$(find_massa_node_pids || true)"
  if [[ -n "$others" ]]; then
    echo "[run_node] running (untracked pid(s)=${others})"
    return 0
  fi
  echo "[run_node] not running"
  return 1
}

build_if_needed() {
  local need_build=0
  if [[ ! -x "$BIN_PATH" ]]; then
    need_build=1
    echo "[run_node] massa-node binary missing, will build."
  elif [[ "$MASSA_FORCE_REBUILD" == "1" ]]; then
    need_build=1
    echo "[run_node] MASSA_FORCE_REBUILD=1, rebuilding."
  fi

  if (( need_build )); then
    echo "[run_node] cargo build --release -p massa-node --features '${MASSA_FEATURES}' (jobs=${MASSA_BUILD_JOBS})"
    echo "[run_node] this can take 10-30 minutes the first time."
    # GCC >= 14 tightened its standard headers: <cstdint> is no longer pulled
    # in transitively. The bundled rocksdb sources (rust-librocksdb-sys) and a
    # few other C++ deps rely on the old behavior and fail with
    # "'uint64_t' has not been declared". Force-include <cstdint> into every
    # C++ translation unit. (Do NOT set BINDGEN_EXTRA_CLANG_ARGS for this:
    # bindgen runs clang in C mode, where <cstdint> as a header name is
    # unresolved.)
    (
      cd "$MASSA_DIR"
      CXXFLAGS="${CXXFLAGS:-} -include cstdint" \
        cargo build --release -p massa-node --features "$MASSA_FEATURES" -j "$MASSA_BUILD_JOBS"
    )
  fi

  if [[ ! -x "$BIN_PATH" ]]; then
    echo "[run_node] build did not produce $BIN_PATH" >&2
    exit 2
  fi
}

start_node() {
  if [[ ! -d "$NODE_DIR" ]]; then
    echo "[run_node] error: $NODE_DIR not found" >&2
    exit 1
  fi

  # refuse to start twice
  local pid=""
  pid="$(read_pid_file || true)"
  if is_pid_running "${pid:-}"; then
    echo "[run_node] already running (pid=${pid}); 'stop' first if you want to restart."
    exit 0
  fi

  # sweep any untracked leftover instances
  local others
  others="$(find_massa_node_pids || true)"
  if [[ -n "$others" ]]; then
    echo "[run_node] sweeping untracked massa-node pids: $others"
    stop_node
  fi

  build_if_needed

  # sanity: broadcast flag in overlay
  if ! grep -q '^[[:space:]]*enable_broadcast[[:space:]]*=[[:space:]]*true' \
      "${NODE_DIR}/config/config.toml" 2>/dev/null; then
    echo "[run_node] warning: ${NODE_DIR}/config/config.toml does not set enable_broadcast = true"
    echo "           streams required by the indexer will be silent."
  fi

  # rotate log
  if [[ -f "$LOG_FILE" ]]; then
    mv "$LOG_FILE" "${LOG_FILE}.prev" 2>/dev/null || true
  fi

  local extra=()
  if [[ "$MASSA_ACCEPT_CHARTER" == "1" ]]; then
    extra+=(-a)
  fi

  echo "[run_node] starting detached massa-node"
  echo "[run_node] cwd:         $NODE_DIR"
  echo "[run_node] binary:      $BIN_PATH"
  echo "[run_node] log file:    $LOG_FILE"
  echo "[run_node] pid file:    $PID_FILE"
  echo "[run_node] public gRPC: http://127.0.0.1:33037"

  # Fully detach: new session (setsid), stdin from /dev/null, stdout+stderr
  # to the log file. Truncate/create the log before exec so the file exists
  # immediately for tail -f.
  : > "$LOG_FILE"
  (
    cd "$NODE_DIR"
    # nohup guards against SIGHUP, setsid makes a new session so the node
    # is not tied to any TTY/controlling terminal.
    exec </dev/null
    nohup setsid "$BIN_PATH" -p "$MASSA_PASSWORD" "${extra[@]}" \
      >>"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    disown || true
  )

  # tiny sanity check: the PID we wrote should still be alive a moment later.
  sleep 1
  local spawned
  spawned="$(read_pid_file || true)"
  if ! is_pid_running "${spawned:-}"; then
    echo "[run_node] error: node exited immediately. Last log lines:" >&2
    tail -n 50 "$LOG_FILE" >&2 || true
    rm -f "$PID_FILE"
    exit 3
  fi

  echo "[run_node] started (pid=${spawned}). tail -f '$LOG_FILE' to watch."
}

# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------

case "$cmd" in
  start) start_node ;;
  stop)  stop_node; echo "[run_node] stopped." ;;
  restart) stop_node; start_node ;;
  status) status_node ;;
  *)
    echo "usage: $0 [start|stop|restart|status]" >&2
    exit 64
    ;;
esac
