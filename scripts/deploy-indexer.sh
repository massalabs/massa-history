#!/usr/bin/env bash
# Detached, SSH-drop-proof deploy for one indexer host.
#
# Usage (on the indexer host itself):
#   nohup /home/damip/massahistory/scripts/deploy-indexer.sh > /tmp/deploy.log 2>&1 &
#
# The script survives SSH/VPN disconnections because it is started with
# nohup and re-execs itself under setsid. Progress and result land in
# /tmp/deploy.log and /tmp/deploy.status ("RUNNING" / "OK <sha>" / "FAIL <step>").

set -u
REPO=/home/damip/massahistory
STATUS=/tmp/deploy.status

if [ "${DEPLOY_DETACHED:-}" != "1" ]; then
    DEPLOY_DETACHED=1 setsid "$0" "$@" < /dev/null &
    echo "detached as pid $!"
    exit 0
fi

step() { echo "[$(date -u +%H:%M:%S)] $*"; }
fail() { echo "FAIL $1" > "$STATUS"; step "FAILED at: $1"; exit 1; }

echo "RUNNING" > "$STATUS"
source "$HOME/.cargo/env" 2>/dev/null || true
export PATH="$HOME/.cargo/bin:$PATH"

step "fetching main"
cd "$REPO" || fail cd
git fetch origin main || fail fetch
git reset --hard origin/main || fail reset
SHA=$(git rev-parse --short HEAD)

step "building $SHA"
cd "$REPO/massa-indexer" || fail cd2
cargo build --release || fail build

step "installing"
sudo install -m 755 target/release/massa-indexer /usr/local/bin/massa-indexer || fail install

step "restarting service"
sudo systemctl restart massa-indexer || fail restart

step "waiting for health"
ok=0
for i in $(seq 1 60); do
    if curl -sf --max-time 4 http://127.0.0.1:8080/v1/metrics > /tmp/deploy-metrics.txt; then
        ok=1
        break
    fi
    sleep 2
done
[ "$ok" = 1 ] || fail health

step "verifying"
systemctl is-active --quiet massa-indexer || fail active
grep -E "^massa_indexer_(ingest_events_dropped|slots_finalized)_total " /tmp/deploy-metrics.txt

echo "OK $SHA" > "$STATUS"
step "deploy complete: $SHA"
