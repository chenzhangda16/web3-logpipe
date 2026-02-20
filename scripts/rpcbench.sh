#!/usr/bin/env bash
# chmod +x scripts/rpcbench.sh
# ./scripts/rpcbench.sh start
# ./scripts/rpcbench.sh status
# ./scripts/rpcbench.sh logs
# ./scripts/rpcbench.sh stop
# ./scripts/rpcbench.sh restart

set -euo pipefail

# ----------------------------
# paths
# ----------------------------
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PID_DIR="./data/pids"
LOG_DIR="./logs"
PID_FILE="$PID_DIR/rpcbench.pids"

mkdir -p "$PID_DIR" "$LOG_DIR" ./bin

# ----------------------------
# config (override via env)
# ----------------------------
export NO_PROXY="localhost,127.0.0.1,::1"
export no_proxy="$NO_PROXY"

: "${MOCK_DB:=./data/mockchain.db}"
: "${MOCK_RPC:=127.0.0.1:18080}"
: "${MOCK_ADDR:=5000}"
: "${MOCK_TICK:=1s}"
: "${MOCK_DET:=false}"
: "${MOCK_SEED:=1}"
: "${MOCK_BACKFILL_SEC:=86400}"
: "${MOCK_GAP_SEC:=0}"

: "${RPC_BASE:=http://$MOCK_RPC}"

# ----------------------------
# rpcbench config (override via env)
# ----------------------------
: "${BENCH_C:=8}"                # -c
: "${BENCH_PAGE:=200}"           # -page
: "${BENCH_FROM:=1}"             # -from
: "${BENCH_N:=1000}"             # -n (-1 infinite)
: "${BENCH_REPORT:=1s}"          # -report
: "${BENCH_TIMEOUT:=10s}"        # -timeout
: "${BENCH_WARMUP:=2s}"          # -warmup

: "${NO_BUILD:=false}"

# ----------------------------
# helpers
# ----------------------------
log() { echo "[$(date '+%F %T')] [rpcbench] $*"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

read_pids() {
  [[ -f "$PID_FILE" ]] && cat "$PID_FILE"
}

append_pid() {
  mkdir -p "$(dirname "$PID_FILE")"
  echo "$1" >> "$PID_FILE"
}

stop_by_pidfile() {
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(read_pids || true)

  if [[ ${#pids[@]} -eq 0 ]]; then
    log "no pidfile or empty pidfile: $PID_FILE"
    return 0
  fi

  log "stopping ${#pids[@]} process(es) from pidfile..."
  for pid in "${pids[@]}"; do
    if is_pid_alive "$pid"; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

  # wait up to 2s
  local deadline=$(( $(date +%s) + 2 ))
  while true; do
    local any_alive=false
    for pid in "${pids[@]}"; do
      if is_pid_alive "$pid"; then
        any_alive=true
        break
      fi
    done
    $any_alive || break
    [[ $(date +%s) -ge $deadline ]] && break
    sleep 0.1
  done

  # force kill remaining
  for pid in "${pids[@]}"; do
    if is_pid_alive "$pid"; then
      log "force killing pid=$pid"
      kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
  done

  rm -f "$PID_FILE"
  log "stopped."
}

wait_http_ok() {
  local url="$1"
  local timeout_sec="${2:-30}"
  local start_ts
  start_ts="$(date +%s)"

  while true; do
    if curl -fsS --noproxy '*' "$url" >/dev/null 2>&1; then
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

start_with_dual_logs() {
  # usage: start_with_dual_logs <outvar> <prefix> <hist_log_path> -- <command...>
  local __outvar="$1"; shift
  local prefix="$1"; shift
  local hist_log="$1"; shift

  [[ "${1:-}" == "--" ]] && shift || true

  local latest_log="$LOG_DIR/${prefix}.latest.log"
  : >"$latest_log"

  stdbuf -oL -eL "$@" > >(tee -a "$latest_log" "$hist_log") 2>&1 &
  local pid=$!
  printf -v "$__outvar" '%s' "$pid"
}

build_bins() {
  if [[ "$NO_BUILD" == "true" ]]; then
    log "NO_BUILD=true, skip build"
    return 0
  fi
  need_cmd go
  log "building binaries: mockchain, rpcbench"
  go build -o ./bin/mockchain ./cmd/mockchain
  go build -o ./bin/rpcbench  ./cmd/rpcbench
}

cleanup_start() {
  log "start interrupted/failed, cleaning up..."
  stop_by_pidfile || true
}

status() {
  log "==================== rpcbench status ===================="

  if [[ ! -f "$PID_FILE" ]]; then
    log "components: stopped (no pidfile)"
  else
    local pids=()
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && pids+=("$pid")
    done < "$PID_FILE"

    local alive=0
    for pid in "${pids[@]}"; do
      is_pid_alive "$pid" && alive=$((alive+1))
    done

    log "components: pidfile=$PID_FILE total=${#pids[@]} alive=$alive"
    if [[ ${#pids[@]} -gt 0 ]]; then
      printf "  pids:\n"
      for pid in "${pids[@]}"; do
        if is_pid_alive "$pid"; then
          echo "    $pid alive"
        else
          echo "    $pid dead"
        fi
      done
    fi
  fi

  if curl -fsS --noproxy '*' "$RPC_BASE/chain/head" >/dev/null 2>&1; then
    log "rpc: OK  $RPC_BASE/chain/head"
  else
    log "rpc: BAD $RPC_BASE/chain/head"
  fi

  log "logs:"
  log "  mockchain latest: $LOG_DIR/mockchain.latest.log"
  log "  rpcbench  latest: $LOG_DIR/rpcbench.latest.log"

  log "========================================================="
}

logs() {
  local m b
  m="$LOG_DIR/mockchain.latest.log"
  b="$LOG_DIR/rpcbench.latest.log"

  log "tail -F latest logs (Ctrl+C to stop tailing)"
  tail -n 200 -F "$m" "$b"
}

start() {
  if [[ -f "$PID_FILE" ]]; then
    log "pidfile exists, checking liveness..."
    local alive=false
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if is_pid_alive "$pid"; then
        log "found alive pid=$pid, refusing to start"
        alive=true
        break
      fi
    done < "$PID_FILE"

    if [[ "$alive" == "true" ]]; then
      log "service appears to be running"
      log "use: ./scripts/rpcbench.sh status OR stop"
      exit 1
    else
      log "stale pidfile detected (all pids dead), cleaning up"
      rm -f "$PID_FILE"
    fi
  fi

  : > "$PID_FILE"
  trap 'cleanup_start; exit 1' ERR INT TERM

  need_cmd curl
  build_bins

  local ts
  ts="$(date '+%Y%m%d_%H%M%S')"

  # 1) mockchain
  log "starting mockchain..."
  local mock_log="$LOG_DIR/mockchain.$ts.log"
  local pid_mock=""
  start_with_dual_logs pid_mock mockchain "$mock_log" -- \
    ./bin/mockchain \
      -db "$MOCK_DB" \
      -rpc "$MOCK_RPC" \
      -addr "$MOCK_ADDR" \
      -tick "$MOCK_TICK" \
      -det="$MOCK_DET" \
      -seed "$MOCK_SEED" \
      -backfill-sec "$MOCK_BACKFILL_SEC" \
      -gap-sec "$MOCK_GAP_SEC"
  append_pid "$pid_mock"
  log "mockchain pid=$pid_mock log=$mock_log latest=$LOG_DIR/mockchain.latest.log"

  log "waiting for mockchain rpc..."
  wait_http_ok "$RPC_BASE/chain/head" 60
  log "mockchain rpc ready: $RPC_BASE"

  # 2) rpcbench
  log "starting rpcbench..."
  local bench_log="$LOG_DIR/rpcbench.$ts.log"
  local pid_bench=""

  start_with_dual_logs pid_bench rpcbench "$bench_log" -- \
    ./bin/rpcbench \
      -rpc "$RPC_BASE" \
      -c "$BENCH_C" \
      -page "$BENCH_PAGE" \
      -from "$BENCH_FROM" \
      -n "$BENCH_N" \
      -report "$BENCH_REPORT" \
      -timeout "$BENCH_TIMEOUT" \
      -warmup "$BENCH_WARMUP"

  append_pid "$pid_bench"
  log "rpcbench pid=$pid_bench log=$bench_log latest=$LOG_DIR/rpcbench.latest.log"

  trap - ERR INT TERM

  log "started. pidfile=$PID_FILE"
  log "env summary:"
  log "  RPC_BASE=$RPC_BASE"
  log "  BENCH_MODE=$BENCH_MODE"
  log "  BENCH_CONC=$BENCH_CONC"
  log "  BENCH_PAGE=$BENCH_PAGE"
  log "  BENCH_FROM=$BENCH_FROM"
  log "  BENCH_SECONDS=$BENCH_SECONDS"
  log "use:"
  log "  ./scripts/rpcbench.sh status"
  log "  ./scripts/rpcbench.sh logs"
  log "  ./scripts/rpcbench.sh stop"
}

stop() { stop_by_pidfile; }

restart() {
  stop || true
  start
}

usage() {
  cat <<EOF
Usage: ./scripts/rpcbench.sh <command>

commands:
  start     start mockchain + rpcbench in background (pidfile)
  stop      stop processes from pidfile
  restart   stop then start
  status    show pid status + rpc probe
  logs      tail -f latest logs

env overrides:
  MOCK_DB, MOCK_RPC, MOCK_ADDR, MOCK_TICK, MOCK_DET, MOCK_SEED, MOCK_BACKFILL_SEC, MOCK_GAP_SEC, RPC_BASE
  BENCH_MODE, BENCH_CONC, BENCH_PAGE, BENCH_FROM, BENCH_SECONDS, BENCH_QPS_LIMIT
  NO_BUILD=true  (skip go build)
EOF
}

cmd="${1:-}"
case "$cmd" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  logs) logs ;;
  *) usage; exit 1 ;;
esac
