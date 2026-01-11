#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p data logs data/logs

# ----------------------------
# Config (override via env)
# ----------------------------
: "${MOCK_DB:=./data/mockchain.db}"
: "${MOCK_RPC:=127.0.0.1:18080}"
: "${MOCK_ADDR:=5000}"
: "${MOCK_TICK:=1s}"
: "${MOCK_DET:=false}"
: "${MOCK_SEED:=1}"
: "${MOCK_BACKFILL_SEC:=86400}"   # -1 disables warmup
: "${MOCK_GAP_SEC:=0}"            # 0 => default=3*tickSec (implemented in main.go)

: "${KAFKA_BROKERS:=127.0.0.1:9092}"
: "${KAFKA_TOPIC:=mockchain.blocks}"

: "${FETCH_BACKFILL_SEC:=86400}" # -1 disables cold start backfill
: "${FETCH_PAGE:=200}"
: "${FETCH_POLL_HEAD:=2s}"
: "${FETCH_IDLE_SLEEP:=300ms}"
: "${FETCH_CKPT:=./data/fetcher.ckpt}"

: "${RPC_BASE:=http://$MOCK_RPC}"
: "${NO_BUILD:=false}"

# ----------------------------
# Helpers
# ----------------------------
log() { echo "[$(date '+%F %T')] $*"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
}

wait_http_ok() {
  local url="$1"
  local timeout_sec="${2:-30}"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    local now
    now="$(date +%s)"
    if (( now - start_ts >= timeout_sec )); then
      echo "timeout waiting for $url" >&2
      return 1
    fi
    sleep 0.2
  done
}

kill_group() {
  local pids=("$@")
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}

# ----------------------------
# Preconditions
# ----------------------------
need_cmd go
need_cmd curl

# Optional: Kafka CLIs if you want to auto-check topic existence later
# need_cmd kafka-topics.sh

# ----------------------------
# Start processes
# ----------------------------
PIDS=()

cleanup() {
  log "shutting down..."
  kill_group "${PIDS[@]}"
  # give them a moment
  sleep 0.3
  kill -KILL "${PIDS[@]}" >/dev/null 2>&1 || true
  log "bye"
}
trap cleanup EXIT INT TERM

if [ "$NO_BUILD" != "true" ]; then
  log "building binaries (optional, but speeds restarts)"
  go build -o ./bin/mockchain ./cmd/mockchain
  go build -o ./bin/fetcher  ./cmd/fetcher
fi

log "starting mockchain..."
MOCK_LOG="./data/logs/mockchain.$(date '+%Y%m%d_%H%M%S').log"
./bin/mockchain \
  -db "$MOCK_DB" \
  -rpc "$MOCK_RPC" \
  -addr "$MOCK_ADDR" \
  -tick "$MOCK_TICK" \
  -det="$MOCK_DET" \
  -seed "$MOCK_SEED" \
  -backfill-sec "$MOCK_BACKFILL_SEC" \
  -gap-sec "$MOCK_GAP_SEC" \
  >"$MOCK_LOG" 2>&1 &
PIDS+=("$!")
log "mockchain pid=${PIDS[-1]} log=$MOCK_LOG"

log "waiting for mockchain rpc..."
wait_http_ok "$RPC_BASE/head" 30
# also ensure /chain/head is reachable if you added it
wait_http_ok "$RPC_BASE/chain/head" 30 || true
log "mockchain rpc ready: $RPC_BASE"

log "starting fetcher..."
FETCH_LOG="./data/logs/fetcher.$(date '+%Y%m%d_%H%M%S').log"
./bin/fetcher \
  -rpc "$RPC_BASE" \
  -brokers "$KAFKA_BROKERS" \
  -topic "$KAFKA_TOPIC" \
  -backfill-sec "$FETCH_BACKFILL_SEC" \
  -page "$FETCH_PAGE" \
  -poll-head "$FETCH_POLL_HEAD" \
  -idle-sleep "$FETCH_IDLE_SLEEP" \
  -ckpt "$FETCH_CKPT" \
  >"$FETCH_LOG" 2>&1 &
PIDS+=("$!")
log "fetcher pid=${PIDS[-1]} log=$FETCH_LOG"

log "all services started."
log "tail logs:"
log "  tail -f $MOCK_LOG"
log "  tail -f $FETCH_LOG"

# Wait forever until one exits (fail-fast)
wait -n "${PIDS[@]}"
log "one process exited; stopping all."
exit 1
