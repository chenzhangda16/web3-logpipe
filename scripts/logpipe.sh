#!/usr/bin/env bash
#chmod +x scripts/logpipe.sh
#./scripts/logpipe.sh start
#./scripts/logpipe.sh status
#./scripts/logpipe.sh logs
#./scripts/logpipe.sh stop

set -euo pipefail

# ----------------------------
# paths
# ----------------------------
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PID_DIR="./data/pids"
LOG_DIR="./data/logs"
PID_FILE="$PID_DIR/logpipe.pids"

mkdir -p "$PID_DIR" "$LOG_DIR" ./bin

# ----------------------------
# config (override via env)
# ----------------------------
export NO_PROXY="localhost,127.0.0.1,::1"
export no_proxy="$NO_PROXY"

# PG bootstrap (db/user) + set PG_DSN

export PG_DSN="postgres://${PG_DB_USER:-web3}:${PG_DB_PASS:-web3}@${PG_HOST:-127.0.0.1}:${PG_PORT:-5432}/${PG_DB_NAME:-web3log}?sslmode=disable"

: "${MOCK_DB:=./data/mockchain.db}"
: "${MOCK_RPC:=127.0.0.1:18080}"
: "${MOCK_ADDR:=5000}"
: "${MOCK_TICK:=1s}"
: "${MOCK_DET:=false}"
: "${MOCK_SEED:=1}"
: "${MOCK_BACKFILL_SEC:=86400}"
: "${MOCK_GAP_SEC:=0}"

: "${KAFKA_BROKERS:=127.0.0.1:9092}"
: "${KAFKA_TOPIC:=mockchain.blocks}"

: "${FETCH_BACKFILL_SEC:=86400}"
: "${FETCH_PAGE:=200}"
: "${FETCH_POLL_HEAD:=2s}"
: "${FETCH_IDLE_SLEEP:=300ms}"
: "${FETCH_CKPT:=./data/fetcher.ckpt}"
: "${RPC_BASE:=http://$MOCK_RPC}"

: "${PROC_GROUP:=logpipe-processor}"
: "${PROC_SPOOL:=./data/spool.wal}"
: "${PROC_DECODE_WORKER:=4}"
: "${PROC_DECODE_QUEUE:=8192}"
: "${PROC_CKPT:=./data/processor.ckpt}"

: "${OUT_TOPIC:=logpipe.out}"
: "${WRITER_GROUP:=logpipe.writer}"

: "${NO_BUILD:=false}"

# ----------------------------
# helpers
# ----------------------------
log() { echo "[$(date '+%F %T')] [logpipe] $*"; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
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

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

probe_tcp() {
  local host="$1" port="$2"
  (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
}

kafka_host() { echo "${KAFKA_BROKERS%%:*}"; }
kafka_port() { echo "${KAFKA_BROKERS##*:}"; }

kafka_is_up() { probe_tcp "$(kafka_host)" "$(kafka_port)"; }

pg_is_up() {
  have_cmd pg_isready || return 2
  pg_isready -h "${PG_HOST:-127.0.0.1}" -p "${PG_PORT:-5432}" >/dev/null 2>&1
}

read_file_1st_line() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  head -n 1 "$f" 2>/dev/null | tr -d '\r' || true
}

read_pids() {
  if [[ -f "$PID_FILE" ]]; then
    cat "$PID_FILE"
  fi
}

write_pids() {
  : > "$PID_FILE"
  for pid in "$@"; do
    echo "$pid" >> "$PID_FILE"
  done
}

append_pid() {
  mkdir -p "$(dirname "$PID_FILE")"
  echo "$1" >> "$PID_FILE"
}

cleanup_start() {
  # used by trap during start(); best-effort cleanup
  log "start interrupted/failed, cleaning up..."
  stop_by_pidfile || true
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

status() {
  log "==================== logpipe status ===================="

  # ---------------- go components (pidfile) ----------------
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

  # ---------------- rpc probe ----------------
  if curl -fsS --noproxy '*' "$RPC_BASE/chain/head" >/dev/null 2>&1; then
    log "rpc: OK  $RPC_BASE/chain/head"
  else
    log "rpc: BAD $RPC_BASE/chain/head"
  fi

  # ---------------- kafka status ----------------
  local kafka_pid_file="$PID_DIR/kafka.pid"
  local kafka_tail_pid_file="$PID_DIR/kafka_tail.pid"
  local kafka_latest="$LOG_DIR/kafka.latest.log"

  if kafka_is_up; then
    log "kafka: OK  $KAFKA_BROKERS"
  else
    log "kafka: BAD $KAFKA_BROKERS"
  fi

  local kpid ktpid
  kpid="$(read_file_1st_line "$kafka_pid_file" || true)"
  ktpid="$(read_file_1st_line "$kafka_tail_pid_file" || true)"
  if [[ -n "$kpid" ]]; then
    if is_pid_alive "$kpid"; then
      log "kafka pid: $kpid alive ($kafka_pid_file)"
    else
      log "kafka pid: $kpid dead  ($kafka_pid_file)"
    fi
  else
    log "kafka pid: (none) ($kafka_pid_file)"
  fi
  if [[ -n "$ktpid" ]]; then
    if is_pid_alive "$ktpid"; then
      log "kafka tail: $ktpid alive ($kafka_tail_pid_file)"
    else
      log "kafka tail: $ktpid dead  ($kafka_tail_pid_file)"
    fi
  else
    log "kafka tail: (none) ($kafka_tail_pid_file)"
  fi
  log "kafka log: $kafka_latest"

  local kafka_data_dir="$ROOT_DIR/data/kafka/logs"
  log "kafka data dir: $kafka_data_dir"


  # ---------------- postgres status ----------------
  local pgdata="${PGDATA:-$ROOT_DIR/data/pg/data}"
  local postmaster_pid_file="$pgdata/postmaster.pid"
  local pg_tail_pid_file="$PID_DIR/postgres_tail.pid"
  local pg_latest="$LOG_DIR/postgres.latest.log"

  if pg_is_up; then
    log "postgres: OK  ${PG_HOST:-127.0.0.1}:${PG_PORT:-5432}"
  else
    if have_cmd pg_isready; then
      log "postgres: BAD ${PG_HOST:-127.0.0.1}:${PG_PORT:-5432}"
    else
      log "postgres: (unknown, pg_isready not installed)"
    fi
  fi

  local pgpid pgtpid
  pgpid="$(read_file_1st_line "$postmaster_pid_file" || true)"
  pgtpid="$(read_file_1st_line "$pg_tail_pid_file" || true)"

  if [[ -n "$pgpid" ]]; then
    if is_pid_alive "$pgpid"; then
      log "postgres pid: $pgpid alive ($postmaster_pid_file)"
    else
      log "postgres pid: $pgpid dead  ($postmaster_pid_file)"
    fi
  else
    log "postgres pid: (none) ($postmaster_pid_file)"
  fi

  if [[ -n "$pgtpid" ]]; then
    if is_pid_alive "$pgtpid"; then
      log "pg tail: $pgtpid alive ($pg_tail_pid_file)"
    else
      log "pg tail: $pgtpid dead  ($pg_tail_pid_file)"
    fi
  else
    log "pg tail: (none) ($pg_tail_pid_file)"
  fi

  log "postgres log: $pg_latest"

  # ---------------- component latest logs ----------------
  log "component logs:"
  log "  mockchain:  $LOG_DIR/mockchain.latest.log"
  log "  fetcher:    $LOG_DIR/fetcher.latest.log"
  log "  processor:  $LOG_DIR/processor.latest.log"
  log "  writer:     $LOG_DIR/writer.latest.log"

  log "========================================================="
}

logs() {
  local m f p w
  m="$LOG_DIR/mockchain.latest.log"
  f="$LOG_DIR/fetcher.latest.log"
  p="$LOG_DIR/processor.latest.log"
  w="$LOG_DIR/writer.latest.log"

  log "tail -F latest logs (Ctrl+C to stop tailing)"
  tail -n 200 -F "$m" "$f" "$p" "$w"
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

  # write pid to caller's variable (no subshell)
  printf -v "$__outvar" '%s' "$pid"
}

build_bins() {
  if [[ "$NO_BUILD" == "true" ]]; then
    log "NO_BUILD=true, skip build"
    return 0
  fi
  need_cmd go
  log "building binaries"
  go build -o ./bin/mockchain  ./cmd/mockchain
  go build -o ./bin/fetcher    ./cmd/fetcher
  go build -o ./bin/processor  ./cmd/processor
  go build -o ./bin/writer     ./cmd/writer
}

start() {
  # tolerate stale pidfile (all pids dead)
  if [[ -f "$PID_FILE" ]]; then
    log "pidfile exists, checking liveness..."
    local alive=false
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" >/dev/null 2>&1; then
        log "found alive pid=$pid, refusing to start"
        alive=true
        break
      fi
    done < "$PID_FILE"

    if [[ "$alive" == "true" ]]; then
      log "service appears to be running"
      log "use: ./scripts/logpipe.sh status OR stop"
      exit 1
    else
      log "stale pidfile detected (all pids dead), cleaning up"
      rm -f "$PID_FILE"
    fi
  fi

  # --- transactional start record ---
  : > "$PID_FILE"

  # If anything fails during start (ERR) or user interrupts (INT) or killed (TERM),
  # auto cleanup whatever has already been started.
  trap 'cleanup_start; exit 1' ERR INT TERM

  source ./scripts/ensure_pg.sh
  source ./scripts/ensure_kafka.sh

  need_cmd curl
  build_bins

  local ts
  ts="$(date '+%Y%m%d_%H%M%S')"

  # ----------------------
  # 1) mockchain
  # ----------------------
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

  # ----------------------
  # 2) fetcher
  # ----------------------
  log "starting fetcher..."
  local fetch_log="$LOG_DIR/fetcher.$ts.log"
  local pid_fetch=""
  start_with_dual_logs pid_fetch fetcher "$fetch_log" -- \
    ./bin/fetcher \
      -rpc "$RPC_BASE" \
      -brokers "$KAFKA_BROKERS" \
      -topic "$KAFKA_TOPIC" \
      -backfill-sec "$FETCH_BACKFILL_SEC" \
      -page "$FETCH_PAGE" \
      -poll-head "$FETCH_POLL_HEAD" \
      -idle-sleep "$FETCH_IDLE_SLEEP" \
      -ckpt "$FETCH_CKPT"
  append_pid "$pid_fetch"
  log "fetcher pid=$pid_fetch log=$fetch_log latest=$LOG_DIR/fetcher.latest.log"

  # ----------------------
  # 3) processor
  # ----------------------
  log "starting processor..."
  local proc_log="$LOG_DIR/processor.$ts.log"
  local pid_proc=""
  start_with_dual_logs pid_proc processor "$proc_log" -- \
    ./bin/processor \
      -brokers "$KAFKA_BROKERS" \
      -group "$PROC_GROUP" \
      -topic "$KAFKA_TOPIC" \
      -spool "$PROC_SPOOL" \
      -decode-worker "$PROC_DECODE_WORKER" \
      -decode-queue "$PROC_DECODE_QUEUE" \
      -ckpt "$PROC_CKPT"
  append_pid "$pid_proc"
  log "processor pid=$pid_proc log=$proc_log latest=$LOG_DIR/processor.latest.log"

  # ----------------------
  # 4) writer
  # ----------------------
  log "starting writer..."
  local writer_log="$LOG_DIR/writer.$ts.log"
  local pid_writer=""
  start_with_dual_logs pid_writer writer "$writer_log" -- \
    ./bin/writer \
      -brokers "$KAFKA_BROKERS" \
      -topic "$OUT_TOPIC" \
      -group "$WRITER_GROUP"
  append_pid "$pid_writer"
  log "writer pid=$pid_writer log=$writer_log latest=$LOG_DIR/writer.latest.log"

  # start succeeded -> disable the start-only trap
  trap - ERR INT TERM

  log "started. pidfile=$PID_FILE"
  log "env summary:"
  log "  RPC_BASE=$RPC_BASE"
  log "  KAFKA_BROKERS=$KAFKA_BROKERS"
  log "  KAFKA_TOPIC(in)=$KAFKA_TOPIC"
  log "  OUT_TOPIC(out)=$OUT_TOPIC"
  log "  PG_DSN=$PG_DSN"
  log "use:"
  log "  ./scripts/logpipe.sh status"
  log "  ./scripts/logpipe.sh logs"
  log "  ./scripts/logpipe.sh stop"
}

stop() {
  stop_by_pidfile
}

restart() {
  stop || true
  start
}

down() {
  stop_by_pidfile || true
  ./scripts/stop_kafka.sh || true
  ./scripts/stop_pg.sh || true
  log "down: all components + infra stopped."
}

usage() {
  cat <<EOF
Usage: ./scripts/logpipe.sh <command>

commands:
  start     start mockchain+fetcher+processor+writer in background (pidfile)
  stop      stop processes from pidfile (no pkill blast radius)
  restart   stop then start
  status    show pid status + rpc probe
  logs      tail -f latest logs for all components
  down      stop components + stop kafka/postgres (project-managed infra)

tips:
  - If you manually started processes and pidfile is missing, use ./scripts/kill_all.sh as a last resort.
EOF
}

cmd="${1:-}"
case "$cmd" in
  start) start ;;
  stop) stop ;;
  restart) restart ;;
  status) status ;;
  down) down ;;
  logs) logs ;;
  *) usage; exit 1 ;;
esac
