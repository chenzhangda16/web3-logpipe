#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/stop_pg.sh
#
# run on target node
# - stop only project-owned postgres cluster under PGDATA
# - stop postgres log tailer pid if present
# - never scan/kill arbitrary system postgres
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()   { date '+%F %T'; }
log()  { echo "[$(ts)] [stop_pg] $*"; }
warn() { echo "[$(ts)] [stop_pg] WARN: $*" >&2; }

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

pid_alive() {
  kill -0 "$1" >/dev/null 2>&1
}

kill_pid_soft_hard() {
  local pid="$1"
  [[ -z "$pid" ]] && return 0

  if ! pid_alive "$pid"; then
    return 0
  fi

  kill -TERM "$pid" >/dev/null 2>&1 || true
  sleep 0.5
  pid_alive "$pid" && kill -KILL "$pid" >/dev/null 2>&1 || true
}

stop_tailer() {
  local tail_pid_file="$PID_DIR/postgres_tail.pid"

  if [[ -f "$tail_pid_file" ]]; then
    local tpid
    tpid="$(cat "$tail_pid_file" 2>/dev/null || true)"
    [[ -n "$tpid" ]] && kill_pid_soft_hard "$tpid"
    rm -f "$tail_pid_file" >/dev/null 2>&1 || true
    log "stopped postgres tailer"
  fi
}

stop_pg_cluster() {
  if [[ ! -d "$PGDATA" ]]; then
    log "PGDATA missing: $PGDATA"
    return 0
  fi

  if have_cmd pg_ctl; then
    if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
      log "pg_ctl stop -D $PGDATA -m immediate"
      pg_ctl -D "$PGDATA" stop -m immediate >/dev/null 2>&1 || true
    fi
  fi

  if [[ -f "$PGDATA/postmaster.pid" ]]; then
    local pid
    pid="$(head -n 1 "$PGDATA/postmaster.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && pid_alive "$pid"; then
      log "stopping postgres pid=$pid PGDATA=$PGDATA"
      kill_pid_soft_hard "$pid"
    else
      warn "stale postmaster.pid detected"
    fi
    rm -f "$PGDATA/postmaster.pid" 2>/dev/null || true
  else
    log "no postmaster.pid under PGDATA=$PGDATA"
  fi
}

main() {
  stop_tailer
  stop_pg_cluster
  log "done"
}

main "$@"