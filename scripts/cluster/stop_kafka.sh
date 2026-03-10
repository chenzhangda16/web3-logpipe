#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/stop_kafka.sh
#
# run on target node
# - stop project-owned kafka process
# - stop kafka log tailer pid if present
# - fallback scan for kafka java / wrapper processes
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [stop_kafka] $*"; }

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
  sleep 0.2
  pid_alive "$pid" && kill -KILL "$pid" >/dev/null 2>&1 || true
}

stop_by_pidfiles() {
  if [[ -f "${KAFKA_TAIL_PID_FILE:-}" ]]; then
    local tpid
    tpid="$(cat "$KAFKA_TAIL_PID_FILE" 2>/dev/null || true)"
    [[ -n "$tpid" ]] && kill_pid_soft_hard "$tpid"
    rm -f "$KAFKA_TAIL_PID_FILE" || true
    log "stopped kafka tailer by pidfile"
  fi

  if [[ -f "${KAFKA_PID_FILE:-}" ]]; then
    local kpid
    kpid="$(cat "$KAFKA_PID_FILE" 2>/dev/null || true)"
    [[ -n "$kpid" ]] && kill_pid_soft_hard "$kpid"
    rm -f "$KAFKA_PID_FILE" || true
    log "stopped kafka by pidfile"
  fi
}

stop_by_fallback_scan() {
  pkill -TERM -f 'kafka\.Kafka|KafkaRaftServer|QuorumController' 2>/dev/null || true
  sleep 0.5
  pkill -KILL -f 'kafka\.Kafka|KafkaRaftServer|QuorumController' 2>/dev/null || true

  pkill -TERM -f 'kafka-server-start\.sh' 2>/dev/null || true
  sleep 0.2
  pkill -KILL -f 'kafka-server-start\.sh' 2>/dev/null || true

  log "fallback kafka scan done"
}

main() {
  stop_by_pidfiles
  stop_by_fallback_scan
  log "done"
}

main "$@"