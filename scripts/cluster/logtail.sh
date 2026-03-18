#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/logtail.sh
#
# runtime-only log mirror helper:
# - mirror remote component.latest.log into local files
# - resolve latest symlink on every (re)connect
# - auto reconnect when ssh/tail exits
# - stop mirrored tails by pidfile
#
# usage:
#   bash scripts/cluster/logtail.sh start mockchain
#   bash scripts/cluster/logtail.sh start kafka
#   bash scripts/cluster/logtail.sh start logs/cluster/server.log
#   bash scripts/cluster/logtail.sh status mockchain
#   bash scripts/cluster/logtail.sh stop  mockchain
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

latest_rel_of_service() {
  local svc="$1"
  local node root logdir logdir_rel

  case "$svc" in
    kafka)
      node="$(host_of_service kafka)"
      root="$(root_of_node "$node")"
      logdir="$(log_dir_of_node "$node")"
      logdir_rel="$(abs_to_rel_under_root "$logdir" "$root")"
      printf '%s/kafka.latest.log' "$logdir_rel"
      ;;
    pg)
      node="$(host_of_service pg)"
      root="$(root_of_node "$node")"
      logdir="$(log_dir_of_node "$node")"
      logdir_rel="$(abs_to_rel_under_root "$logdir" "$root")"
      printf '%s/postgres.latest.log' "$logdir_rel"
      ;;
    mockchain|fetcher|processor|writer)
      node="$(host_of_service "$svc")"
      root="$(root_of_node "$node")"
      logdir="$(log_dir_of_node "$node")"
      logdir_rel="$(abs_to_rel_under_root "$logdir" "$root")"
      printf '%s/%s.latest.log' "$logdir_rel" "$svc"
      ;;
    *)
      die "unsupported log service: $svc"
      ;;
  esac
}

spec_to_relpath() {
  local spec="$1"
  case "$spec" in
    kafka|pg|mockchain|fetcher|processor|writer)
      latest_rel_of_service "$spec"
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

state_file_of_rel() {
  local rel="$1"
  local key
  key="$(printf '%s' "$rel" | sed 's#[^A-Za-z0-9._-]#_#g')"
  printf '%s/%s.state' "$PID_DIR" "$key"
}

remote_latest_exists() {
  local node="$1"
  local remote_abs="$2"
  ssh_bash "$node" "[[ -e $(printf '%q' "$remote_abs") || -L $(printf '%q' "$remote_abs") ]]"
}

service_of_relpath() {
  local rel="$1"
  local base

  case "$rel" in
    */mockchain.latest.log|*/mockchain.*.log)
      printf 'mockchain'
      return 0
      ;;
    */fetcher.latest.log|*/fetcher.*.log)
      printf 'fetcher'
      return 0
      ;;
    */processor.latest.log|*/processor.*.log)
      printf 'processor'
      return 0
      ;;
    */writer.latest.log|*/writer.*.log)
      printf 'writer'
      return 0
      ;;
    data/kafka/*)
      printf 'kafka'
      return 0
      ;;
    logs/cluster/kafka.latest.log|logs/cluster/postgres.latest.log)
      base="$(basename "$rel")"
      if [[ "$base" == "kafka.latest.log" ]]; then
        printf 'kafka'
      else
        printf 'pg'
      fi
      return 0
      ;;
    logs/cluster/*)
      base="$(basename "$rel")"
      case "$base" in
        controller.log|kafka-authorizer.log|kafka-request.log|kafkaServer-gc.log|log-cleaner.log|server.log|state-change.log)
          printf 'kafka'
          ;;
        *)
          printf 'pg'
          ;;
      esac
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
  local rel node remote_abs local_abs pid_file err_file state_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"
  err_file="$(stderr_file_of_rel "$rel")"
  state_file="$(state_file_of_rel "$rel")"

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

  if ! remote_latest_exists "$node" "$remote_abs"; then
    log "start skip: remote file/link not found node=$node rel=$rel"
    return 0
  fi

  local_abs="$(local_abs_of_rel "$rel")"
  mkdir -p "$(dirname "$local_abs")" "$(dirname "$err_file")" "$(dirname "$state_file")"
  : > "$state_file"

  log "start: node=$node rel=$rel -> local=$local_abs"

  nohup bash -c scripts/cluster/logtail_supervise.sh \
   --node "$node" \
   --remote-latest "$remote_abs" \
   --local-log "$local_abs" \
   --state-file "$err_file" \
   --err-file "$state_file" </dev/null >/dev/null 2>&1 &
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

status_tail() {
  local spec="$1"
  local rel pid_file state_file pid

  rel="$(spec_to_relpath "$spec")"
  pid_file="$(pid_file_of_rel "$rel")"
  state_file="$(state_file_of_rel "$rel")"

  if [[ ! -f "$pid_file" ]]; then
    log "status: rel=$rel running=no"
    return 0
  fi

  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    log "status: rel=$rel running=yes pid=$pid"
  else
    log "status: rel=$rel running=no stale_pid=${pid:-<empty>}"
  fi

  if [[ -f "$state_file" ]]; then
    tail -n 5 "$state_file"
  fi
}

usage() {
  cat <<'EOF2'
Usage:
  bash scripts/cluster/logtail.sh start  <service-or-relpath>
  bash scripts/cluster/logtail.sh status <service-or-relpath>
  bash scripts/cluster/logtail.sh stop   <service-or-relpath>

Services:
  mockchain | fetcher | processor | writer | kafka | pg

Examples:
  bash scripts/cluster/logtail.sh start mockchain
  bash scripts/cluster/logtail.sh status mockchain
  bash scripts/cluster/logtail.sh stop  mockchain

  bash scripts/cluster/logtail.sh start logs/cluster/server.log
  bash scripts/cluster/logtail.sh stop  logs/cluster/server.log
EOF2
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
    status)
      [[ -n "$target" ]] || die "status requires <service-or-relpath>"
      status_tail "$target"
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
