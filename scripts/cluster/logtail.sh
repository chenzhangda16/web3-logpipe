#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/logtail.sh
#
# runtime-only log tail helper:
# - start remote ssh tail -F and mirror into local files
# - stop mirrored tails by pidfile
#
# usage:
#   bash scripts/cluster/logtail.sh start kafka
#   bash scripts/cluster/logtail.sh start pg
#   bash scripts/cluster/logtail.sh start logs/cluster/server.log
#   bash scripts/cluster/logtail.sh stop  kafka
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

source "$ROOT_DIR/scripts/cluster/lib/_cluster_ctl.sh"

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [logtail] $*"; }
die() { echo "[$(ts)] [logtail] ERROR: $*" >&2; exit 1; }

require_cmds() {
  command -v nohup >/dev/null 2>&1 || die "required command not found: nohup"
  cluster_ctl_require_cmds || exit 1
}

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

  node="$(host_of_service "$svc")"
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
  printf '%s/%s.pid' "$PID_DIR" "$key"
}

stderr_file_of_rel() {
  local rel="$1"
  local key
  key="$(printf '%s' "$rel" | sed 's#[^A-Za-z0-9._-]#_#g')"
  printf '%s/%s.err.log' "$ERR_DIR" "$key"
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
  host_of_service "$svc"
}

start_tail() {
  local spec="$1"
  local rel node remote_abs local_abs pid_file err_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"
  err_file="$(stderr_file_of_rel "$rel")"

  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      log "start skip: already running rel=$rel pid=$pid"
      return 0
    fi
    rm -f "$pid_file"
  fi

  node="$(node_of_relpath "$rel")"
  remote_abs="$(remote_abs_of_rel "$node" "$rel")"

  if ! remote_file_exists "$node" "$remote_abs"; then
    log "start skip: remote file not found node=$node rel=$rel"
    return 0
  fi

  local_abs="$(local_abs_of_rel "$rel")"
  mkdir -p "$(dirname "$local_abs")"

  log "start: node=$node rel=$rel -> local=$local_abs"

  nohup ssh "$node" "tail -F $(printf '%q' "$remote_abs")" \
    >> "$local_abs" 2>> "$err_file" &
  pid=$!

  echo "$pid" > "$pid_file"
  log "start ok: rel=$rel pid=$pid"
}

stop_tail() {
  local spec="$1"
  local rel pid_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"

  if [[ ! -f "$pid_file" ]]; then
    log "stop skip: no pid file for rel=$rel"
    return 0
  fi

  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$pid_file"
    log "stop skip: empty pid file for rel=$rel"
    return 0
  fi

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    log "stop sent: rel=$rel pid=$pid"
  else
    log "stop cleanup stale pid: rel=$rel pid=$pid"
  fi

  rm -f "$pid_file"
}

usage() {
  cat <<'EOF'
Usage:
  bash scripts/cluster/logtail.sh start <service-or-relpath>
  bash scripts/cluster/logtail.sh stop  <service-or-relpath>

Services:
  kafka | pg

Examples:
  bash scripts/cluster/logtail.sh start kafka
  bash scripts/cluster/logtail.sh stop  kafka

  bash scripts/cluster/logtail.sh start logs/cluster/server.log
  bash scripts/cluster/logtail.sh stop  logs/cluster/server.log

  bash scripts/cluster/logtail.sh start data/kafka/logs/server.log
  bash scripts/cluster/logtail.sh stop  data/kafka/logs/server.log
EOF
}

main() {
  require_cmds

  local action="${1:-}"
  local target="${2:-}"

  case "$action" in
    start)
      [[ -n "$target" ]] || die "start requires <service-or-relpath>"
      start_tail "$target"
      ;;
    stop)
      [[ -n "$target" ]] || die "stop requires <service-or-relpath>"
      stop_tail "$target"
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