#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${PID_DIR:-$ROOT_DIR/data/pids}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/data/logs}"

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka_2.13-3.8.0}"
KAFKA_PID_FILE="${KAFKA_PID_FILE:-$PID_DIR/kafka.pid}"
KAFKA_TAIL_PID_FILE="${KAFKA_TAIL_PID_FILE:-$PID_DIR/kafka_tail.pid}"
KAFKA_SERVER_STOP="${KAFKA_SERVER_STOP:-$KAFKA_HOME/bin/kafka-server-stop.sh}"

ts() { date '+%H:%M:%S'; }
log() { echo "[$(ts)] [stop_kafka] $*"; }
warn() { echo "[$(ts)] [stop_kafka] WARN: $*" >&2; }

pid_alive() { kill -0 "$1" >/dev/null 2>&1; }

stop_pid_graceful_then_kill() {
  local pid="$1"
  local timeout="${2:-10}"

  pid_alive "$pid" || return 0

  log "Sending SIGTERM to pid=$pid"
  kill "$pid" >/dev/null 2>&1 || true

  local start now
  start="$(date +%s)"
  while pid_alive "$pid"; do
    now="$(date +%s)"
    (( now - start >= timeout )) && break
    sleep 0.2
  done

  if pid_alive "$pid"; then
    warn "Still alive after ${timeout}s, sending SIGKILL pid=$pid"
    kill -KILL "$pid" >/dev/null 2>&1 || true
  fi
}

stop_tailer() {
  if [[ -f "$KAFKA_TAIL_PID_FILE" ]]; then
    local tpid
    tpid="$(cat "$KAFKA_TAIL_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$tpid" ]] && pid_alive "$tpid"; then
      log "Stopping kafka log tailer pid=$tpid"
      stop_pid_graceful_then_kill "$tpid" 2
    fi
    rm -f "$KAFKA_TAIL_PID_FILE" >/dev/null 2>&1 || true
  fi
}

main() {
  stop_tailer

  if [[ ! -f "$KAFKA_PID_FILE" ]]; then
    log "No kafka pidfile: $KAFKA_PID_FILE (already stopped?)"
    return 0
  fi

  local pid
  pid="$(cat "$KAFKA_PID_FILE" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    warn "Empty pidfile: $KAFKA_PID_FILE"
    rm -f "$KAFKA_PID_FILE" || true
    return 0
  fi

  if ! pid_alive "$pid"; then
    log "Kafka pid=$pid not alive; cleaning pidfile"
    rm -f "$KAFKA_PID_FILE" || true
    return 0
  fi

  if [[ -x "$KAFKA_SERVER_STOP" ]]; then
    log "Stopping Kafka via kafka-server-stop.sh (pid=$pid)"
    "$KAFKA_SERVER_STOP" >/dev/null 2>&1 || true
    sleep 0.5
  else
    warn "kafka-server-stop.sh not found/executable: $KAFKA_SERVER_STOP (will signal pid)"
  fi

  stop_pid_graceful_then_kill "$pid" 10

  rm -f "$KAFKA_PID_FILE" || true
  log "Stopped Kafka."
  log "Hint: logs under $LOG_DIR/kafka.*.log (latest=$LOG_DIR/kafka.latest.log)"
}

main "$@"
