#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster
source "$ROOT_DIR/scripts/cluster/lib/_component_stream.sh"

ts()  { date '+%F %T'; }
die() { echo "[$(ts)] [start_mockchain] ERROR: $*" >&2; exit 1; }

main() {
  local stamp="${1:-}"
  [[ -n "$stamp" ]] || die "usage: $0 <stamp>"

  cluster_component_stream_run mockchain "$stamp" "" -- \
    "$MOCKCHAIN_BIN_DIR/mockchain" \
      -db "$MOCK_DB" \
      -rpc "$MOCK_RPC" \
      -addr "$MOCK_ADDR" \
      -tick "$MOCK_TICK" \
      -det="$MOCK_DET" \
      -seed "$MOCK_SEED" \
      -backfill-sec "$MOCK_BACKFILL_SEC" \
      -gap-sec "$MOCK_GAP_SEC"
}

main "$@"