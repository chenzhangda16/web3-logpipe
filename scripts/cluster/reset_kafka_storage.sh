#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/reset_kafka_storage.sh
#
# run on target node
# - remove only project-owned kafka storage dir
# - clean project kafka logs under LOG_DIR
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [reset_kafka_storage] $*"; }

main() {
  if [[ -n "${KAFKA_PROJECT_DIR:-}" && -d "$KAFKA_PROJECT_DIR" ]]; then
    log "removing KAFKA_PROJECT_DIR=$KAFKA_PROJECT_DIR"
    rm -rf "$KAFKA_PROJECT_DIR"
  else
    log "KAFKA_PROJECT_DIR missing; skip"
  fi

  mkdir -p "${LOG_DIR:-$ROOT_DIR/logs}" 2>/dev/null || true
  rm -f "$LOG_DIR"/kafka.*.log "$LOG_DIR"/kafka.latest.log 2>/dev/null || true
  log "cleaned kafka logs under LOG_DIR=$LOG_DIR"
  log "done"
}

main "$@"