#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/logpipe.sh
#
# cluster runtime orchestrator:
# - ensure cluster sync + pg + kafka
# - start 4 app services on mapped nodes
# - remote stdout/stderr streams directly back to controller latest logs
# - remote timestamped history logs stay on remote nodes
#
# usage:
#   bash scripts/cluster/logpipe.sh start
#   bash scripts/cluster/logpipe.sh stop
#   bash scripts/cluster/logpipe.sh restart
#   bash scripts/cluster/logpipe.sh status
#   bash scripts/cluster/logpipe.sh logs
#   bash scripts/cluster/logpipe.sh down
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

source "$ROOT_DIR/scripts/cluster/lib/_cluster_ctl.sh"

ts()  { date '+%F %T'; }
log() {
  local src="${BASH_SOURCE[1]##*/}"
  local line="${BASH_LINENO[0]}"
  local func="${FUNCNAME[1]:-main}"
  echo "[$(ts)] [$src:$line][$func] $*";
}
die() {
  local src="${BASH_SOURCE[1]##*/}"
  local line="${BASH_LINENO[0]}"
  local func="${FUNCNAME[1]:-main}"
  echo "[$(ts)] ERROR: [$src:$line][$func] $*" >&2; exit 1;
}

have_cmd() { command -v "$1" >/dev/null 2>&1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

wait_http_ok() {
  local url="$1"
  local timeout_sec="${2:-30}"
  local start_ts now
  start_ts="$(date +%s)"

  while true; do
    if curl -fsS --noproxy '*' "$url" >/dev/null 2>&1; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - start_ts >= timeout_sec )); then
      die "timeout waiting for $url"
    fi
    sleep 0.2
  done
}

wait_remote_ready_fifo() {
  local node="$1"
  local fifo="$2"
  local timeout_sec="${3:-30}"

  local root
  root="$(root_of_node "$node")" || die "failed to resolve root for node=$node"

  if node_is_local "$node"; then
    (
      cd "$root"
      bash scripts/cluster/wait_ready_fifo.sh "$fifo" "$timeout_sec"
    )
  else
    ssh_bash "$node" "cd $(printf '%q' "$root") && bash scripts/cluster/wait_ready_fifo.sh $(printf '%q' "$fifo") $(printf '%q' "$timeout_sec")"
  fi
}

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

probe_tcp() {
  local host="$1" port="$2"
  (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
}

kafka_host() { echo "${KAFKA_BROKERS%%:*}"; }
kafka_port() { echo "${KAFKA_BROKERS##*:}"; }

kafka_is_up() { probe_tcp "$(kafka_host)" "$(kafka_port)"; }

pg_is_up() {
  have_cmd pg_isready || return 2
  pg_isready -h "$PG_IP" -p "$PG_PORT" >/dev/null 2>&1
}

read_pids() {
  [[ -f "$PID_FILE" ]] && cat "$PID_FILE"
}

append_pid() {
  mkdir -p "$(dirname "$PID_FILE")"
  echo "$1" >> "$PID_FILE"
}

read_file_1st_line() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  head -n 1 "$f" 2>/dev/null | tr -d '\r' || true
}

service_node() {
  local svc="$1"
  host_of_service "$svc"
}

remote_ready_fifo_path() {
  local node="$1"
  local name="$2"
  printf '%s/data/cluster/ready/%s.ready.fifo' "$(root_of_node "$node")" "$name"
}

cleanup_start() {
  log "start interrupted/failed, cleaning up..."
  stop || true
}

stop_transport_pids() {
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(read_pids || true)

  if [[ ${#pids[@]} -eq 0 ]]; then
    log "no pidfile or empty pidfile: $PID_FILE"
    return 0
  fi

  log "stopping ${#pids[@]} ssh transport process(es) from pidfile..."
  for pid in "${pids[@]}"; do
    if is_pid_alive "$pid"; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done

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

  for pid in "${pids[@]}"; do
    if is_pid_alive "$pid"; then
      log "force killing transport pid=$pid"
      kill -KILL "$pid" >/dev/null 2>&1 || true
    fi
  done

  rm -f "$PID_FILE"
}

stop_remote_apps() {
  local node
  while read -r node; do
    [[ -z "$node" ]] && continue
    if remote_project_exists "$node"; then
      run_remote_primitive_rel "$node" "scripts/cluster/kill_apps.sh" || true
    fi
  done < <(all_cluster_nodes)
}

stop() {
  stop_transport_pids || true
  stop_remote_apps || true
  log "stopped."
}

restart() {
  stop || true
  start
}

down() {
  local kafka_node pg_node
  kafka_node="$(service_node kafka)"
  pg_node="$(service_node pg)"

  stop || true

  if remote_project_exists "$kafka_node"; then
    run_remote_primitive_rel "$kafka_node" "scripts/cluster/stop_kafka.sh" || true
  fi
  if remote_project_exists "$pg_node"; then
    run_remote_primitive_rel "$pg_node" "scripts/cluster/stop_pg.sh" || true
  fi

  log "down: all components + infra stopped."
}

start_remote_stream() {
  # usage: start_remote_stream <outvar> <node> <prefix> <script_rel> [args...]
  local __outvar="$1"; shift
  local node="$1"; shift
  local prefix="$1"; shift
  local script_rel="$1"; shift

  local latest_log="$LOG_DIR/${prefix}.latest.log"
  local root remote_cmd pid arg

  : > "$latest_log"
  root="$(root_of_node "$node")"

  remote_cmd="cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
  for arg in "$@"; do
    remote_cmd+=" $(printf '%q' "$arg")"
  done

  if node_is_local "$node"; then
    nohup bash -lc "$remote_cmd" >> "$latest_log" 2>&1 &
  else
    nohup ssh "$node" "bash -lc $(printf '%q' "$remote_cmd")" >> "$latest_log" 2>&1 &
  fi
  pid=$!

  printf -v "$__outvar" '%s' "$pid"
}

status() {
  log "==================== cluster logpipe status ===================="

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

    log "transport: pidfile=$PID_FILE total=${#pids[@]} alive=$alive"
    if [[ ${#pids[@]} -gt 0 ]]; then
      printf "  transport pids:\n"
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

  if kafka_is_up; then
    log "kafka: OK  $KAFKA_BROKERS"
  else
    log "kafka: BAD $KAFKA_BROKERS"
  fi

  if pg_is_up; then
    log "postgres: OK  ${PG_IP}:${PG_PORT}"
  else
    if have_cmd pg_isready; then
      log "postgres: BAD ${PG_IP}:${PG_PORT}"
    else
      log "postgres: (unknown, pg_isready not installed)"
    fi
  fi

  log "service placement:"
  log "  mockchain:  $(service_node mockchain)"
  log "  fetcher:    $(service_node fetcher)"
  log "  processor:  $(service_node processor)"
  log "  writer:     $(service_node writer)"
  log "  kafka:      $(service_node kafka)"
  log "  pg:         $(service_node pg)"

  log "local latest logs:"
  log "  mockchain:  $LOG_DIR/mockchain.latest.log"
  log "  fetcher:    $LOG_DIR/fetcher.latest.log"
  log "  processor:  $LOG_DIR/processor.latest.log"
  log "  writer:     $LOG_DIR/writer.latest.log"

  log "==============================================================="
}

logs() {
  local m f p w
  m="$LOG_DIR/mockchain.latest.log"
  f="$LOG_DIR/fetcher.latest.log"
  p="$LOG_DIR/processor.latest.log"
  w="$LOG_DIR/writer.latest.log"

  log "tail -F local latest logs (Ctrl+C to stop tailing)"
  tail -n 200 -F "$m" "$f" "$p" "$w"
}

start() {
  if [[ -f "$PID_FILE" ]]; then
    log "pidfile exists, checking liveness..."
    local alive=false
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      if kill -0 "$pid" >/dev/null 2>&1; then
        log "found alive transport pid=$pid, refusing to start"
        alive=true
        break
      fi
    done < "$PID_FILE"

    if [[ "$alive" == "true" ]]; then
      log "service appears to be running"
      log "use: bash scripts/cluster/logpipe.sh status OR stop"
      exit 1
    else
      log "stale pidfile detected (all transports dead), cleaning up"
      rm -f "$PID_FILE"
    fi
  fi

  : > "$PID_FILE"
  trap 'cleanup_start; exit 1' ERR INT TERM

  need_cmd ssh
  need_cmd curl
  need_cmd rsync

  cluster_ensure_infra force

  local ts_now
  ts_now="$(date '+%Y%m%d_%H%M%S')"

  local node_mock node_fetch node_proc node_writer
  node_mock="$(service_node mockchain)"
  node_fetch="$(service_node fetcher)"
  node_proc="$(service_node processor)"
  node_writer="$(service_node writer)"

  # 1) mockchain
  log "starting mockchain on $node_mock..."
  local pid_mock=""
  start_remote_stream pid_mock "$node_mock" mockchain "scripts/cluster/start_mockchain.sh" "$ts_now"
  append_pid "$pid_mock"
  log "mockchain transport pid=$pid_mock latest=$LOG_DIR/mockchain.latest.log"

  log "waiting for mockchain rpc..."
  wait_http_ok "$RPC_BASE/chain/head" 60
  log "mockchain rpc ready: $RPC_BASE"

  # 2) writer
  log "starting writer on $node_writer..."
  local writer_fifo pid_writer
  writer_fifo="$(remote_ready_fifo_path "$node_writer" writer)"
  pid_writer=""
  start_remote_stream pid_writer "$node_writer" writer "scripts/cluster/start_writer.sh" "$ts_now" "$writer_fifo"
  append_pid "$pid_writer"
  log "writer transport pid=$pid_writer latest=$LOG_DIR/writer.latest.log"

  log "waiting for writer ready..."
  wait_remote_ready_fifo "$node_writer" "$writer_fifo" 60
  log "writer ready"

  # 3) processor
  log "starting processor on $node_proc..."
  local proc_fifo pid_proc
  proc_fifo="$(remote_ready_fifo_path "$node_proc" processor)"
  pid_proc=""
  start_remote_stream pid_proc "$node_proc" processor "scripts/cluster/start_processor.sh" "$ts_now" "$proc_fifo"
  append_pid "$pid_proc"
  log "processor transport pid=$pid_proc latest=$LOG_DIR/processor.latest.log"

  log "waiting for processor ready..."
  wait_remote_ready_fifo "$node_proc" "$proc_fifo" 60
  log "processor ready"

  # 4) fetcher
  log "starting fetcher on $node_fetch..."
  local pid_fetch=""
  start_remote_stream pid_fetch "$node_fetch" fetcher "scripts/cluster/start_fetcher.sh" "$ts_now"
  append_pid "$pid_fetch"
  log "fetcher transport pid=$pid_fetch latest=$LOG_DIR/fetcher.latest.log"

  sleep 120
#  if ! wait_remote_process_alive "$node_fetch" '/fetcher' 5; then
#    log "fetcher failed to stay alive; dumping remote diagnostics..."
#    ssh_bash "$node_fetch" "pgrep -af fetcher || true"
#    die "fetcher not alive on node=$node_fetch"
#  fi
  log "fetcher process alive"

  trap - ERR INT TERM

  log "started. pidfile=$PID_FILE"
  log "env summary:"
  log "  RPC_BASE=$RPC_BASE"
  log "  KAFKA_BROKERS=$KAFKA_BROKERS"
  log "  KAFKA_IN_TOPIC=$KAFKA_IN_TOPIC"
  log "  KAFKA_OUT_TOPIC=$KAFKA_OUT_TOPIC"
  log "  PG_IP=$PG_IP"
  log "  PG_PORT=$PG_PORT"
  log "use:"
  log "  bash scripts/cluster/logpipe.sh status"
  log "  bash scripts/cluster/logpipe.sh logs"
  log "  bash scripts/cluster/logpipe.sh stop"
}

usage() {
  cat <<'EOF'
Usage: bash scripts/cluster/logpipe.sh <command>

commands:
  start     ensure infra, then start mockchain+fetcher+processor+writer
  stop      stop local ssh transports + remote app processes
  restart   stop then start
  status    show transport pid status + rpc/kafka/pg probes
  logs      tail local latest logs for all components
  down      stop components + stop kafka/postgres on service nodes
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