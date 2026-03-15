#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster
source "$ROOT_DIR/scripts/cluster/lib/_component_stream.sh"

ts()  { date '+%F %T'; }
die() { echo "[$(ts)] [start_processor] ERROR: $*" >&2; exit 1; }

main() {
  local stamp="${1:-}"
  local ready_fifo="${2:-}"

  [[ -n "$stamp" ]] || die "usage: $0 <stamp> <ready_fifo>"
  [[ -n "$ready_fifo" ]] || die "usage: $0 <stamp> <ready_fifo>"

  cluster_component_stream_run processor "$stamp" "$ready_fifo" -- \
    "$PROCESSOR_BIN_DIR/processor" \
      -brokers "$KAFKA_BROKERS" \
      -group "$PROC_GROUP" \
      -topic "$KAFKA_IN_TOPIC" \
      -spool "$PROC_SPOOL" \
      -decode-worker "$PROC_DECODE_WORKER" \
      -decode-queue "$PROC_DECODE_QUEUE" \
      -ckpt "$PROC_CKPT" \
      -ready-fifo "$ready_fifo"
}

main "$@"