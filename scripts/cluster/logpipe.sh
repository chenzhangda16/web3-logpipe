#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/logpipe.sh
#
# cluster runtime orchestrator:
# - ensure cluster sync + pg + kafka
# - start 4 app services on mapped nodes
# - runtime logs stay on remote nodes
# - remote latest/history logs are observed separately via scripts/cluster/logtail.sh
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
  echo "[$(ts)] [$src:$line][$func] $*"
}
die() {
  local src="${BASH_SOURCE[1]##*/}"
  local line="${BASH_LINENO[0]}"
  local func="${FUNCNAME[1]:-main}"
  echo "[$(ts)] ERROR: [$src:$line][$func] $*" >&2
  exit 1
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

service_node() {
  local svc="$1"
  host_of_service "$svc"
}

remote_ready_fifo_path() {
  local node="$1"
  local name="$2"
  printf '%s/data/cluster/ready/%s.ready.fifo' "$(root_of_node "$node")" "$name"
}

remote_log_dir() {
  local node="$1"
  log_dir_of_node "$node"
}

cleanup_start() {
  log "start interrupted/failed, cleaning up..."
  stop || true
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
  kill_all_remote_services || die "failed to kill remote services"
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

start_remote_service() {
  # usage: start_remote_service <node> <script_rel> [args...]
  local node="$1"
  local script_rel="$2"
  shift 2 || true

  local root cmd arg
  root="$(root_of_node "$node")"

  remote_cmd="cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
    for arg in "$@"; do
      remote_cmd+=" $(printf '%q' "$arg")"
    done

    if node_is_local "$node"; then
      nohup bash -lc "$remote_cmd" >/dev/null 2>&1 &
    else
      nohup ssh "$node" "bash -lc $(printf '%q' "$remote_cmd")" >/dev/null 2>&1 &
    fi
}

wait_remote_process_match() {
  local node="$1"
  local pattern="$2"
  local timeout_sec="${3:-10}"
  local start_ts now
  start_ts="$(date +%s)"

  while true; do
    if ssh_bash "$node" "pgrep -af $(printf '%q' "$pattern") >/dev/null 2>&1"; then
      return 0
    fi
    now="$(date +%s)"
    if (( now - start_ts >= timeout_sec )); then
      return 1
    fi
    sleep 0.2
  done
}

status() {
  log "==================== cluster logpipe status ===================="

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

  log "remote latest logs:"
  log "  mockchain:  $(remote_log_dir "$(service_node mockchain)")/mockchain.latest.log"
  log "  fetcher:    $(remote_log_dir "$(service_node fetcher)")/fetcher.latest.log"
  log "  processor:  $(remote_log_dir "$(service_node processor)")/processor.latest.log"
  log "  writer:     $(remote_log_dir "$(service_node writer)")/writer.latest.log"

  log "log viewing: use bash scripts/cluster/logtail.sh ..."
  log "==============================================================="
}

logs() {
  die "local latest logs are no longer maintained here; use: bash scripts/cluster/logtail.sh start|status|stop"
}

start() {
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

  kill_all_remote_services || die "failed to kill remote services"

  # 1) mockchain
  log "starting mockchain on $node_mock..."
  start_remote_service "$node_mock" "scripts/cluster/start_mockchain.sh" "$ts_now"
  log "mockchain launched; remote latest=$(remote_log_dir "$node_mock")/mockchain.latest.log"

  log "waiting for mockchain rpc..."
  wait_http_ok "$RPC_BASE/chain/head" 60
  log "mockchain rpc ready: $RPC_BASE"

  # 2) writer
  log "starting writer on $node_writer..."
  local writer_fifo
  writer_fifo="$(remote_ready_fifo_path "$node_writer" writer)"
  start_remote_service "$node_writer" "scripts/cluster/start_writer.sh" "$ts_now" "$writer_fifo"
  log "writer launched; remote latest=$(remote_log_dir "$node_writer")/writer.latest.log"

  log "waiting for writer ready..."
  wait_remote_ready_fifo "$node_writer" "$writer_fifo" 60
  log "writer ready"

  # 3) processor
  log "starting processor on $node_proc..."
  local proc_fifo
  proc_fifo="$(remote_ready_fifo_path "$node_proc" processor)"
  start_remote_service "$node_proc" "scripts/cluster/start_processor.sh" "$ts_now" "$proc_fifo"
  log "processor launched; remote latest=$(remote_log_dir "$node_proc")/processor.latest.log"

  log "waiting for processor ready..."
  wait_remote_ready_fifo "$node_proc" "$proc_fifo" 60
  log "processor ready"

  # 4) fetcher
  log "starting fetcher on $node_fetch..."
  start_remote_service "$node_fetch" "scripts/cluster/start_fetcher.sh" "$ts_now"
  log "fetcher launched; remote latest=$(remote_log_dir "$node_fetch")/fetcher.latest.log"

  if ! wait_remote_process_match "$node_fetch" '(^|/)fetcher([[:space:]]|$)' 10; then
    log "fetcher did not appear in remote process table; dumping diagnostics..."
    ssh_bash "$node_fetch" "pgrep -af fetcher || true"
    die "fetcher not alive on node=$node_fetch"
  fi
  log "fetcher process alive"

  trap - ERR INT TERM

  log "started."
  log "env summary:"
  log "  RPC_BASE=$RPC_BASE"
  log "  KAFKA_BROKERS=$KAFKA_BROKERS"
  log "  KAFKA_IN_TOPIC=$KAFKA_IN_TOPIC"
  log "  KAFKA_OUT_TOPIC=$KAFKA_OUT_TOPIC"
  log "  PG_IP=$PG_IP"
  log "  PG_PORT=$PG_PORT"
  log "use:"
  log "  bash scripts/cluster/logpipe.sh status"
  log "  bash scripts/cluster/logtail.sh start"
  log "  bash scripts/cluster/logpipe.sh stop"
}

usage() {
  cat <<'EOF'
Usage: bash scripts/cluster/logpipe.sh <command>

commands:
  start     ensure infra, then start mockchain+fetcher+processor+writer
  stop      stop remote app processes
  restart   stop then start
  status    show rpc/kafka/pg probes + service placement + remote latest paths
  logs      deprecated here; use scripts/cluster/logtail.sh instead
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
