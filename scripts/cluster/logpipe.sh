#!/usr/bin/env bash
#chmod +x scripts/local/logpipe.sh
#./scripts/local/logpipe.sh start
#./scripts/local/logpipe.sh status
#./scripts/local/logpipe.sh logs
#./scripts/local/logpipe.sh stop

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

wait_ready_fifo() {
  local fifo="$1"
  local timeout_sec="${2:-30}"

  [[ -p "$fifo" ]] || { echo "ready fifo not found: $fifo" >&2; return 1; }

  # 阻塞读一行；用 timeout 防止永等（非忙等）
  if command -v timeout >/dev/null 2>&1; then
    timeout "${timeout_sec}s" bash -c "read -r _ < '$fifo'"
  else
    # 没有 timeout 就退化（仍阻塞，但你自己Ctrl+C）
    read -r _ < "$fifo"
  fi
}

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
  pg_isready -h "${PG_HOST:-192.168.1.50}" -p "${PG_PORT:-55432}" >/dev/null 2>&1
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

  local kafka_data_dir="$ROOT_DIR/data/kafka/logs/local"
  log "kafka data dir: $kafka_data_dir"


  # ---------------- postgres status ----------------
  local pgdata="${PGDATA:-$ROOT_DIR/data/pg/data}"
  local postmaster_pid_file="$pgdata/postmaster.pid"
  local pg_tail_pid_file="$PID_DIR/postgres_tail.pid"
  local pg_latest="$LOG_DIR/postgres.latest.log"

  if pg_is_up; then
    log "postgres: OK  ${PG_HOST:-192.168.1.50}:${PG_PORT:-55432}"
  else
    if have_cmd pg_isready; then
      log "postgres: BAD ${PG_HOST:-192.168.1.50}:${PG_PORT:-55432}"
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
      log "use: ./scripts/local/logpipe.sh status OR stop"
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

  source ./scripts/local/ensure_pg.sh
  source ./scripts/local/ensure_kafka.sh

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
    # 2) writer
    # ----------------------
    log "starting writer..."
    local writer_log="$LOG_DIR/writer.$ts.log"
    local pid_writer=""
    local writer_fifo="$READY_DIR/writer.ready.fifo"
    rm -f "$writer_fifo"
    mkfifo "$writer_fifo"

    start_with_dual_logs pid_writer writer "$writer_log" -- \
      ./bin/writer \
        -brokers "$KAFKA_BROKERS" \
        -topic "$KAFKA_OUT_TOPIC" \
        -group "$WRITER_GROUP" \
        -ready-fifo "$writer_fifo"
    append_pid "$pid_writer"
    log "writer pid=$pid_writer log=$writer_log latest=$LOG_DIR/writer.latest.log"

    log "waiting for writer ready..."
    wait_ready_fifo "$writer_fifo" 60
    log "writer ready"

    # ----------------------
    # 3) processor
    # ----------------------
    log "starting processor..."
    local proc_log="$LOG_DIR/processor.$ts.log"
    local pid_proc=""
    local proc_fifo="$READY_DIR/processor.ready.fifo"
    rm -f "$proc_fifo"
    mkfifo "$proc_fifo"

    start_with_dual_logs pid_proc processor "$proc_log" -- \
      ./bin/processor \
        -brokers "$KAFKA_BROKERS" \
        -group "$PROC_GROUP" \
        -topic "$KAFKA_IN_TOPIC" \
        -spool "$PROC_SPOOL" \
        -decode-worker "$PROC_DECODE_WORKER" \
        -decode-queue "$PROC_DECODE_QUEUE" \
        -ckpt "$PROC_CKPT" \
        -ready-fifo "$proc_fifo"
    append_pid "$pid_proc"
    log "processor pid=$pid_proc log=$proc_log latest=$LOG_DIR/processor.latest.log"

    log "waiting for processor ready..."
    wait_ready_fifo "$proc_fifo" 60
    log "processor ready"

    # ----------------------
    # 4) fetcher
    # ----------------------
    log "starting fetcher..."
    local fetch_log="$LOG_DIR/fetcher.$ts.log"
    local pid_fetch=""
    start_with_dual_logs pid_fetch fetcher "$fetch_log" -- \
      ./bin/fetcher \
        -rpc "$RPC_BASE" \
        -rpc-concurrency "$RPC_CONCURRENCY" \
        -brokers "$KAFKA_BROKERS" \
        -topic "$KAFKA_IN_TOPIC" \
        -backfill-sec "$FETCH_BACKFILL_SEC" \
        -page "$FETCH_PAGE" \
        -poll-head "$FETCH_POLL_HEAD" \
        -ckpt-path "$FETCH_CKPT" \
        -ckpt-tick "$CKPT_TICK" \
        -perf-mode "$PERF_MODE"
    append_pid "$pid_fetch"
    log "fetcher pid=$pid_fetch log=$fetch_log latest=$LOG_DIR/fetcher.latest.log"

  # start succeeded -> disable the start-only trap
  trap - ERR INT TERM

  log "started. pidfile=$PID_FILE"
  log "env summary:"
  log "  RPC_BASE=$RPC_BASE"
  log "  KAFKA_BROKERS=$KAFKA_BROKERS"
  log "  KAFKA_IN_TOPIC=$KAFKA_IN_TOPIC"
  log "  KAFKA_OUT_TOPIC=$KAFKA_OUT_TOPIC"
  log "  PG_DSN=$PG_DSN"
  log "use:"
  log "  ./scripts/local/logpipe.sh status"
  log "  ./scripts/local/logpipe.sh logs"
  log "  ./scripts/local/logpipe.sh stop"
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
Usage: ./scripts/local/logpipe.sh <command>

commands:
  start     start mockchain+fetcher+processor+writer in background (pidfile)
  stop      stop processes from pidfile (no pkill blast radius)
  restart   stop then start
  status    show pid status + rpc probe
  logs      tail -f latest logs for all components
  down      stop components + stop kafka/postgres (project-managed infra)

tips:
  - If you manually started processes and pidfile is missing, use ./scripts/local/kill_all.sh as a last resort.
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
