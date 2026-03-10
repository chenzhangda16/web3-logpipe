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
#   1 -> wipe $ROOT_DIR/data except $ROOT_DIR/data/mockchain.db
#   2 -> nuke entire $ROOT_DIR/data
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [reset_node_data] $*"; }
die() { echo "[$(ts)] [reset_node_data] ERROR: $*" >&2; exit 1; }

safe_root_data() {
  local p="$ROOT_DIR/data"
  [[ -n "$ROOT_DIR" ]] || die "ROOT_DIR empty"
  [[ "$ROOT_DIR" != "/" ]] || die "ROOT_DIR must not be /"
  [[ "$p" == "$ROOT_DIR/data" ]] || die "unexpected data path: $p"
  printf '%s' "$p"
}

main() {
  local mode="${1:-}"
  local data_dir
  data_dir="$(safe_root_data)"

  case "$mode" in
    2)
      log "FULL_RESET=2, nuking $data_dir"
      rm -rf "$data_dir"
      mkdir -p "$data_dir"
      ;;
    1)
      log "FULL_RESET=1, wiping $data_dir except mockchain.db"
      mkdir -p "$data_dir"
      find "$data_dir" -mindepth 1 -maxdepth 1 \
        ! -path "$data_dir/mockchain.db" \
        -exec rm -rf {} +
      ;;
    *)
      die "usage: $0 <1|2>"
      ;;
  esac

  log "done"
}

main "$@"