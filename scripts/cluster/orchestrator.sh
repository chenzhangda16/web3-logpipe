#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# cluster/orchestrator.sh
#
# goals:
# - sync repo/scripts/bin to cluster nodes via rsync (no github/vpn coupling)
# - ssh execute remote cluster scripts
# - start/stop remote log mirror tails onto main machine
#
# usage examples:
#   bash scripts/cluster/orchestrator.sh sync all
#   bash scripts/cluster/orchestrator.sh sync m2
#   bash scripts/cluster/orchestrator.sh run kafka
#   bash scripts/cluster/orchestrator.sh run pg
#   bash scripts/cluster/orchestrator.sh logs-start kafka
#   bash scripts/cluster/orchestrator.sh logs-start pg
#   bash scripts/cluster/orchestrator.sh logs-start logs/cluster/server.log
#   bash scripts/cluster/orchestrator.sh logs-start data/kafka/logs/server.log
#   bash scripts/cluster/orchestrator.sh logs-stop  data/kafka/logs/server.log
#   bash scripts/cluster/orchestrator.sh exec m2 'pwd && ls'
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

clog() { echo "[$(date '+%F %T')] [cluster] $*"; }
die()  { echo "[$(date '+%F %T')] [cluster] ERROR: $*" >&2; exit 1; }

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

require_cmds() {
  local x
  for x in ssh rsync nohup; do
    have_cmd "$x" || die "required command not found: $x"
  done
}

# ------------------------------------------------------------------------------
# env var helpers
# ------------------------------------------------------------------------------

getvar() {
  local name="$1"
  printf '%s' "${!name:-}"
}

# current ssh config already uses node names directly:
# Host main / pc127 / m2 / pixel
host_alias_of_node() {
  local node="$1"
  printf '%s' "$node"
}

root_of_node() {
  local node="$1"
  local var="ROOT_${node^^}"
  local val
  val="$(getvar "$var")"
  [[ -n "$val" ]] || die "env var not found: $var"
  printf '%s' "$val"
}

bin_dir_of_node() {
  local node="$1"
  local var="BIN_DIR_${node^^}"
  local val
  val="$(getvar "$var" || true)"
  if [[ -n "$val" ]]; then
    printf '%s' "$val"
  else
    printf '%s/bin' "$(root_of_node "$node")"
  fi
}

log_dir_of_node() {
  local node="$1"
  local var="LOG_DIR_${node^^}"
  local val
  val="$(getvar "$var" || true)"
  if [[ -n "$val" ]]; then
    printf '%s' "$val"
  else
    printf '%s/logs' "$(root_of_node "$node")"
  fi
}

node_of_service() {
  local svc="$1"
  local var="HOST_${svc^^}"
  local val
  val="$(getvar "$var")"
  [[ -n "$val" ]] || die "service mapping not found: $var"
  printf '%s' "$val"
}

# ------------------------------------------------------------------------------
# node sets
# ------------------------------------------------------------------------------

controller_node() {
  printf '%s' main
}

deploy_nodes() {
  printf '%s\n' pc127 m2 pixel
}

# ------------------------------------------------------------------------------
# ssh / rsync wrappers
# ------------------------------------------------------------------------------

ssh_node() {
  local node="$1"
  shift
  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "$@"
}

ssh_bash() {
  local node="$1"
  local cmd="$2"
  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "bash -lc $(printf '%q' "$cmd")"
}

ensure_remote_root() {
  local node="$1"
  local root
  root="$(root_of_node "$node")"
  clog "ensure remote root: node=$node root=$root"
  ssh_bash "$node" "mkdir -p $(printf '%q' "$root")"
}

sync_node() {
  local node="$1"
  local host root
  host="$(host_alias_of_node "$node")"
  root="$(root_of_node "$node")"

  ensure_remote_root "$node"

  clog "sync repo: node=$node host=$host root=$root"

  rsync -az --delete \
    --exclude '.git' \
    --exclude 'data' \
    --exclude 'logs' \
    --exclude 'tmp' \
    --exclude '.idea' \
    --exclude '.vscode' \
    --exclude 'node_modules' \
    --exclude 'vendor' \
    "$ROOT_DIR/" "$host:$root/"
}

managed_nodes() {
  local node
  while read -r node; do
    [[ -z "$node" ]] && continue
    sync_node "$node"
  done < <(deploy_nodes)
}

# ------------------------------------------------------------------------------
# remote script execution
# ------------------------------------------------------------------------------

run_remote_script() {
  local node="$1"
  local script_rel="$2"

  local root
  root="$(root_of_node "$node")"

  clog "run remote script: node=$node script=$script_rel"
  ssh_bash "$node" "cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
}

run_service_ensure() {
  local svc="$1"
  local node
  node="$(node_of_service "$svc")"

  case "$svc" in
    kafka)
      run_remote_script "$node" "scripts/cluster/ensure_kafka.sh"
      ;;
    pg)
      run_remote_script "$node" "scripts/cluster/ensure_pg.sh"
      ;;
    *)
      die "unsupported ensure service: $svc"
      ;;
  esac
}

# ------------------------------------------------------------------------------
# log mirror helpers
# ------------------------------------------------------------------------------

TAIL_PID_DIR="$ROOT_DIR/tmp/cluster-tail/pids"
TAIL_ERR_DIR="$ROOT_DIR/tmp/cluster-tail/stderr"

mkdir -p "$TAIL_PID_DIR" "$TAIL_ERR_DIR"

normalize_relpath() {
  local p="$1"
  [[ -n "$p" ]] || die "empty path"
  [[ "$p" != /* ]] || die "absolute path is not allowed: $p"

  while [[ "$p" == ./* ]]; do
    p="${p#./}"
  done

  [[ -n "$p" ]] || die "invalid relative path"
  printf '%s' "$p"
}

abs_to_rel_under_root() {
  local abs="$1"
  local root="$2"

  case "$abs" in
    "$root")
      printf '.'
      ;;
    "$root"/*)
      printf '%s' "${abs#"$root"/}"
      ;;
    *)
      die "path is not under root: abs=$abs root=$root"
      ;;
  esac
}

log_rel_of_service() {
  local svc="$1"
  local node root logdir logdir_rel

  node="$(node_of_service "$svc")"
  root="$(root_of_node "$node")"
  logdir="$(log_dir_of_node "$node")"
  logdir_rel="$(abs_to_rel_under_root "$logdir" "$root")"

  case "$svc" in
    kafka)
      printf '%s/kafka.latest.log' "$logdir_rel"
      ;;
    pg)
      printf '%s/postgres.latest.log' "$logdir_rel"
      ;;
    *)
      die "unsupported log service: $svc"
      ;;
  esac
}

spec_to_relpath() {
  local spec="$1"
  case "$spec" in
    kafka|pg)
      log_rel_of_service "$spec"
      ;;
    *)
      normalize_relpath "$spec"
      ;;
  esac
}

remote_abs_of_rel() {
  local node="$1"
  local rel="$2"
  printf '%s/%s' "$(root_of_node "$node")" "$rel"
}

local_abs_of_rel() {
  local rel="$1"
  printf '%s/%s' "$ROOT_DIR" "$rel"
}

pid_file_of_rel() {
  local rel="$1"
  local key
  key="$(printf '%s' "$rel" | sed 's#[^A-Za-z0-9._-]#_#g')"
  printf '%s/%s.pid' "$TAIL_PID_DIR" "$key"
}

stderr_file_of_rel() {
  local rel="$1"
  local key
  key="$(printf '%s' "$rel" | sed 's#[^A-Za-z0-9._-]#_#g')"
  printf '%s/%s.err.log' "$TAIL_ERR_DIR" "$key"
}

remote_file_exists() {
  local node="$1"
  local remote_abs="$2"
  ssh_bash "$node" "[[ -f $(printf '%q' "$remote_abs") ]]"
}

is_kafka_cluster_log_name() {
  local name="$1"
  case "$name" in
    controller.log|kafka-authorizer.log|kafka-request.log|kafkaServer-gc.log|log-cleaner.log|server.log|state-change.log)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

service_of_relpath() {
  local rel="$1"
  local base

  case "$rel" in
    data/kafka/*)
      printf 'kafka'
      return 0
      ;;
    logs/cluster/*)
      base="$(basename "$rel")"
      if is_kafka_cluster_log_name "$base"; then
        printf 'kafka'
      else
        printf 'pg'
      fi
      return 0
      ;;
    *)
      printf 'pg'
      return 0
      ;;
  esac
}

node_of_relpath() {
  local rel="$1"
  local svc

  svc="$(service_of_relpath "$rel")"
  node_of_service "$svc"
}

logs_start() {
  local spec="$1"
  local rel node host remote_abs local_abs pid_file err_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"
  err_file="$(stderr_file_of_rel "$rel")"

  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      clog "logs-start already running: rel=$rel pid=$pid"
      return 0
    fi
    rm -f "$pid_file"
  fi

  node="$(node_of_relpath "$rel")"
  host="$(host_alias_of_node "$node")"
  remote_abs="$(remote_abs_of_rel "$node" "$rel")"

  if ! remote_file_exists "$node" "$remote_abs"; then
    clog "logs-start skip: remote file not found: node=$node rel=$rel"
    return 0
  fi

  local_abs="$(local_abs_of_rel "$rel")"
  mkdir -p "$(dirname "$local_abs")"

  clog "logs-start: node=$node host=$host rel=$rel -> local=$local_abs"

  nohup ssh "$host" "tail -F $(printf '%q' "$remote_abs")" \
    >> "$local_abs" 2>> "$err_file" &
  pid=$!

  echo "$pid" > "$pid_file"

  clog "logs-start ok: rel=$rel pid=$pid"
}

logs_stop() {
  local spec="$1"
  local rel pid_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"

  if [[ ! -f "$pid_file" ]]; then
    clog "logs-stop skip: no pid file for rel=$rel"
    return 0
  fi

  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$pid_file"
    clog "logs-stop skip: empty pid file for rel=$rel"
    return 0
  fi

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    clog "logs-stop sent: rel=$rel pid=$pid"
  else
    clog "logs-stop cleanup stale pid: rel=$rel pid=$pid"
  fi

  rm -f "$pid_file"
}

# ------------------------------------------------------------------------------
# generic remote exec
# ------------------------------------------------------------------------------

exec_node() {
  local node="$1"
  shift
  local cmd="$*"
  [[ -n "$cmd" ]] || die "exec requires command"
  clog "exec: node=$node cmd=$cmd"
  ssh_bash "$node" "$cmd"
}

# ------------------------------------------------------------------------------
# high-level helpers
# ------------------------------------------------------------------------------

sync_and_run_service() {
  local svc="$1"
  local node
  node="$(node_of_service "$svc")"
  sync_node "$node"
  run_service_ensure "$svc"
}

usage() {
  cat <<'EOF'
Usage:
  bash scripts/cluster/orchestrator.sh sync all
  bash scripts/cluster/orchestrator.sh sync <node>
  bash scripts/cluster/orchestrator.sh run <service>
  bash scripts/cluster/orchestrator.sh sync-run <service>

  # log mirror
  bash scripts/cluster/orchestrator.sh logs-start <service-or-relpath>
  bash scripts/cluster/orchestrator.sh logs-stop  <service-or-relpath>
  bash scripts/cluster/orchestrator.sh logs       <service-or-relpath>   # alias of logs-start

  bash scripts/cluster/orchestrator.sh exec <node> '<cmd>'

Nodes:
  main | pc127 | m2 | pixel

Services:
  kafka | pg

Examples:
  bash scripts/cluster/orchestrator.sh sync all
  bash scripts/cluster/orchestrator.sh sync m2
  bash scripts/cluster/orchestrator.sh run kafka
  bash scripts/cluster/orchestrator.sh sync-run pg

  # shorthand
  bash scripts/cluster/orchestrator.sh logs-start kafka
  bash scripts/cluster/orchestrator.sh logs-stop  kafka

  # fixed relative-path mirror
  bash scripts/cluster/orchestrator.sh logs-start logs/cluster/server.log
  bash scripts/cluster/orchestrator.sh logs-stop  logs/cluster/server.log

  bash scripts/cluster/orchestrator.sh logs-start data/kafka/logs/server.log
  bash scripts/cluster/orchestrator.sh logs-stop  data/kafka/logs/server.log

  bash scripts/cluster/orchestrator.sh exec m2 'cd ~/workspace/web3-logpipe && ls -la'
EOF
}

main() {
  require_cmds

  local action="${1:-}"
  local target="${2:-}"

  case "$action" in
    sync)
      case "$target" in
        all) managed_nodes ;;
        pc127|m2|pixel) sync_node "$target" ;;
        *) die "sync target must be one of: all|pc127|m2|pixel" ;;
      esac
      ;;
    run)
      case "$target" in
        kafka|pg) run_service_ensure "$target" ;;
        *) die "run target must be one of: kafka|pg" ;;
      esac
      ;;
    sync-run)
      case "$target" in
        kafka|pg) sync_and_run_service "$target" ;;
        *) die "sync-run target must be one of: kafka|pg" ;;
      esac
      ;;
    logs|logs-start)
      [[ -n "$target" ]] || die "logs-start requires <service-or-relpath>"
      logs_start "$target"
      ;;
    logs-stop)
      [[ -n "$target" ]] || die "logs-stop requires <service-or-relpath>"
      logs_stop "$target"
      ;;
    exec)
      shift 2 || true
      case "$target" in
        pc127|m2|pixel) exec_node "$target" "$@" ;;
        *) die "exec target must be one of: pc127|m2|pixel" ;;
      esac
      ;;
    ""|-h|--help|help)
      usage
      ;;
    *)
      die "unknown action: $action"
      ;;
  esac
}

main "$@"