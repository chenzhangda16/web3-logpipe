#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${PID_DIR:-$ROOT_DIR/data/pids}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/data/logs}"

PGDATA="${PGDATA:-$ROOT_DIR/data/pg/data}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"

TAIL_PID_FILE="${TAIL_PID_FILE:-$PID_DIR/postgres_tail.pid}"

ts() { date '+%H:%M:%S'; }
log() { echo "[$(ts)] [stop_pg] $*"; }
warn() { echo "[$(ts)] [stop_pg] WARN: $*" >&2; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }
pid_alive() { kill -0 "$1" >/dev/null 2>&1; }

stop_tailer() {
  if [[ ! -f "$TAIL_PID_FILE" ]]; then
    return 0
  fi
  local tpid
  tpid="$(cat "$TAIL_PID_FILE" 2>/dev/null || true)"
  if [[ -n "$tpid" ]] && pid_alive "$tpid"; then
    log "Stopping postgres log tailer pid=$tpid"
    kill "$tpid" >/dev/null 2>&1 || true
    sleep 0.1
    pid_alive "$tpid" && kill -KILL "$tpid" >/dev/null 2>&1 || true
  fi
  rm -f "$TAIL_PID_FILE" >/dev/null 2>&1 || true
}

main() {
  stop_tailer

  if [[ ! -f "$PGDATA/postmaster.pid" ]]; then
    log "No postmaster.pid under $PGDATA (already stopped?)"
    return 0
  fi

  have_cmd pg_ctl || { warn "pg_ctl not found; cannot stop postgres cleanly"; return 1; }

  log "Stopping postgres via pg_ctl (PGDATA=$PGDATA)"
  # fast tries smart/fast shutdown; you can change -m fast to -m smart if you want long waits.
  pg_ctl -D "$PGDATA" -m fast stop >/dev/null 2>&1 || true

  # best-effort wait
  local pid
  pid="$(head -n 1 "$PGDATA/postmaster.pid" 2>/dev/null || true)"
  for _ in $(seq 1 50); do
    [[ -z "$pid" ]] && break
    pid_alive "$pid" || break
    sleep 0.1
  done

  log "Stopped postgres."
  log "Hint: logs under $LOG_DIR/postgres.*.log"
}

main "$@"
