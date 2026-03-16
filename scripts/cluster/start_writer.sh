#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster
source "$ROOT_DIR/scripts/cluster/lib/_component_stream.sh"

ts()  { date '+%F %T'; }
die() { echo "[$(ts)] [start_writer] ERROR: $*" >&2; exit 1; }

main() {
  local stamp="${1:-}"
  local ready_fifo="${2:-}"

  [[ -n "$stamp" ]] || die "usage: $0 <stamp> <ready_fifo>"
  [[ -n "$ready_fifo" ]] || die "usage: $0 <stamp> <ready_fifo>"

  cluster_component_stream_run writer "$stamp" "$ready_fifo" -- \
    "$WRITER_CLUSTER_BIN_DIR/writer" \
      -brokers "$KAFKA_BROKERS" \
      -topic "$KAFKA_OUT_TOPIC" \
      -group "$WRITER_GROUP" \
      -ready-fifo "$ready_fifo"
}

main "$@"