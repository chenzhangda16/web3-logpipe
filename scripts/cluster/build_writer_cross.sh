#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [build_writer_cross] $*"; }
die() { echo "[$(ts)] [build_writer_cross] ERROR: $*" >&2; exit 1; }

main() {
  command -v go >/dev/null 2>&1 || die "go not found"

  local out_dir="$ROOT_DIR/bin/out"
  mkdir -p "$out_dir"

  log "building writer for android/arm64..."
  CGO_ENABLED=0 GOOS=android GOARCH=arm64 \
    go build -o "$out_dir/writer.android.arm64" ./cmd/writer

  log "building writer for linux/arm64..."
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
    go build -o "$out_dir/writer.linux.arm64" ./cmd/writer

  log "done:"
  log "  $out_dir/writer.android.arm64"
  log "  $out_dir/writer.linux.arm64"
}

main "$@"