#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster
source "$ROOT_DIR/scripts/cluster/lib/_component_stream.sh"

ts()  { date '+%F %T'; }
die() { echo "[$(ts)] [start_fetcher] ERROR: $*" >&2; exit 1; }

main() {
  local stamp="${1:-}"
  [[ -n "$stamp" ]] || die "usage: $0 <stamp>"

  cluster_component_stream_run fetcher "$stamp" "" -- \
    "$FETCHER_BIN_DIR/fetcher" \
      -rpc "$RPC_BASE" \
      -rpc-concurrency "$RPC_CONCURRENCY" \
      -brokers "$KAFKA_BROKERS" \
      -topic "$KAFKA_IN_TOPIC" \
      -backfill-sec "$FETCH_BACKFILL_SEC" \
      -page "$FETCH_PAGE" \
      -poll-head "$FETCH_POLL_HEAD" \
      -ckpt-path "$FETCH_CKPT" \
      -ckpt-tick "$CKPT_TICK" \
      -perf-mode "$PERF_MODE"
}

main "$@"