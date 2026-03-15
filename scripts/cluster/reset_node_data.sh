#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/reset_node_data.sh
#
# run on target node
# usage:
#   bash scripts/cluster/reset_node_data.sh 1
#   bash scripts/cluster/reset_node_data.sh 2
#
# mode:
#   1 -> wipe $DATA_DIR except selected persistent paths
#   2 -> nuke entire $DATA_DIR
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [reset_node_data] $*"; }
die() { echo "[$(ts)] [reset_node_data] ERROR: $*" >&2; exit 1; }

main() {
  local mode="${1:-}"

  case "$mode" in
    2)
      log "FULL_RESET=2, nuking $DATA_DIR"
      rm -rf "$DATA_DIR"
      mkdir -p "$DATA_DIR"
      ;;
    1)
      log "FULL_RESET=1, wiping $DATA_DIR except persistent paths"
      mkdir -p "$DATA_DIR"
      find "$DATA_DIR" -mindepth 1 -maxdepth 1 \
        ! -path "$MOCK_DB" \
        ! -path "$PID_DIR" \
        ! -path "$ERR_DIR" \
        -exec rm -rf {} +
      ;;
    *)
      die "usage: $0 <1|2>"
      ;;
  esac

  log "done"
}

main "$@"