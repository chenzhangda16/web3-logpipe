#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/logtail.sh
#
# runtime-only log mirror helper:
# - mirror remote *.latest.log into local files
# - auto reconnect when ssh/tail exits
# - manage local tail supervisors by pidfile
# - provide optional interactive shell for ad-hoc tail management
#
# usage:
#   bash scripts/cluster/logtail.sh start mockchain
#   bash scripts/cluster/logtail.sh start all
#   bash scripts/cluster/logtail.sh stop  kafka
#   bash scripts/cluster/logtail.sh stop  all
#   bash scripts/cluster/logtail.sh status all
#   bash scripts/cluster/logtail.sh interactive --start-all
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

source "$ROOT_DIR/scripts/cluster/lib/_cluster_ctl.sh"

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [logtail] $*"; }
die() { echo "[$(ts)] [logtail] ERROR: $*" >&2; exit 1; }

DEFAULT_TAIL_SERVICES=(mockchain fetcher processor writer)

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

key_of_rel() {
  local rel="$1"
  printf '%s' "$rel" | sed 's#[^A-Za-z0-9._-]#_#g'
}

pid_file_of_rel() {
  local rel="$1"
  printf '%s/%s.pid' "$PID_DIR" "$(key_of_rel "$rel")"
}

stderr_file_of_rel() {
  local rel="$1"
  printf '%s/%s.err.log' "$ERR_DIR" "$(key_of_rel "$rel")"
}

state_file_of_rel() {
  local rel="$1"
  printf '%s/%s.state' "$PID_DIR" "$(key_of_rel "$rel")"
}

remote_latest_exists() {
  local node="$1"
  local remote_abs="$2"
  ssh_bash "$node" "[[ -e $(printf '%q' "$remote_abs") ]]"
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

all_known_tail_targets() {
  printf '%s\n' "${DEFAULT_TAIL_SERVICES[@]}"
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
    log "start skip: remote file not found node=$node rel=$rel"
    return 0
  fi

  local_abs="$(local_abs_of_rel "$rel")"
  mkdir -p "$(dirname "$local_abs")" "$(dirname "$err_file")" "$(dirname "$state_file")"
  : > "$state_file"

  log "start: node=$node rel=$rel -> local=$local_abs"

  nohup bash "$ROOT_DIR/scripts/cluster/logtail_supervise.sh" \
      --node "$node" \
      --remote-latest "$remote_abs" \
      --local-log "$local_abs" \
      --state-file "$state_file" \
      --err-file "$err_file" \
      </dev/null >/dev/null 2>&1 &
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

start_all() {
  local spec
  while read -r spec; do
    [[ -n "$spec" ]] || continue
    start_tail "$spec"
  done < <(all_known_tail_targets)
}

stop_all() {
  local spec
  while read -r spec; do
    [[ -n "$spec" ]] || continue
    stop_tail "$spec"
  done < <(all_known_tail_targets)
}

status_all() {
  local spec
  while read -r spec; do
    [[ -n "$spec" ]] || continue
    status_tail "$spec"
  done < <(all_known_tail_targets)
}

interactive_help() {
  cat <<'EOF'
Interactive commands:
  help
  quit
  exit

  tail <target>
  untail <target>
  start <target>
  stop <target>

  status
  status all
  status <target>

Examples:
  tail kafka
  tail pg
  tail logs/cluster/server.log
  untail kafka
  status
  status writer
EOF
}

interactive_loop() {
  local line cmd arg1 rest

  log "interactive mode started"
  log "type 'help' for commands"

  while true; do
    printf '> '
    IFS= read -r line || {
      printf '\n'
      break
    }

    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -n "$line" ]] || continue

    cmd="${line%%[[:space:]]*}"
    if [[ "$cmd" == "$line" ]]; then
      arg1=""
      rest=""
    else
      rest="${line#"$cmd"}"
      rest="${rest#"${rest%%[![:space:]]*}"}"
      arg1="$rest"
    fi

    case "$cmd" in
      help)
        interactive_help
        ;;
      quit|exit)
        break
        ;;
      tail|start)
        [[ -n "$arg1" ]] || {
          log "interactive: '$cmd' requires <target>"
          continue
        }
        start_tail "$arg1"
        ;;
      untail|stop)
        [[ -n "$arg1" ]] || {
          log "interactive: '$cmd' requires <target>"
          continue
        }
        stop_tail "$arg1"
        ;;
      status)
        if [[ -z "$arg1" || "$arg1" == "all" ]]; then
          status_all
        else
          status_tail "$arg1"
        fi
        ;;
      *)
        log "interactive: unknown command: $cmd"
        ;;
    esac
  done

  log "interactive mode exit"
}

usage() {
  cat <<'EOF2'
Usage:
  bash scripts/cluster/logtail.sh start       <service-or-relpath>
  bash scripts/cluster/logtail.sh stop        <service-or-relpath>
  bash scripts/cluster/logtail.sh status      <service-or-relpath>

  bash scripts/cluster/logtail.sh start       all
  bash scripts/cluster/logtail.sh stop        all
  bash scripts/cluster/logtail.sh status      all

  bash scripts/cluster/logtail.sh interactive [--start-all]

Services:
  mockchain | fetcher | processor | writer | kafka | pg

Examples:
  bash scripts/cluster/logtail.sh start mockchain
  bash scripts/cluster/logtail.sh stop  mockchain
  bash scripts/cluster/logtail.sh status all

  bash scripts/cluster/logtail.sh start logs/cluster/server.log
  bash scripts/cluster/logtail.sh stop  logs/cluster/server.log

  bash scripts/cluster/logtail.sh interactive --start-all
EOF2
}

main() {
  require_cmds

  local action="${1:-}"
  local target="${2:-}"

  case "$action" in
    start)
      [[ -n "$target" ]] || die "start requires <service-or-relpath|all>"
      if [[ "$target" == "all" ]]; then
        start_all
      else
        start_tail "$target"
      fi
      ;;
    status)
      [[ -n "$target" ]] || die "status requires <service-or-relpath|all>"
      if [[ "$target" == "all" ]]; then
        status_all
      else
        status_tail "$target"
      fi
      ;;
    stop)
      [[ -n "$target" ]] || die "stop requires <service-or-relpath|all>"
      if [[ "$target" == "all" ]]; then
        stop_all
      else
        stop_tail "$target"
      fi
      ;;
    interactive)
      if [[ "${2:-}" == "--start-all" ]]; then
        start_all
      elif [[ -n "${2:-}" ]]; then
        die "interactive only supports optional --start-all"
      fi
      interactive_loop
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