#!/usr/bin/env bash
set -euo pipefail

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [wait_ready_fifo] $*"; }
die() { echo "[$(ts)] [wait_ready_fifo] ERROR: $*" >&2; exit 1; }

main() {
  local fifo="${1:-}"
  local timeout_sec="${2:-30}"

  [[ -n "$fifo" ]] || die "usage: wait_ready_fifo.sh <fifo> [timeout_sec]"
  [[ "$timeout_sec" =~ ^[0-9]+$ ]] || die "timeout_sec must be integer: got=$timeout_sec"

  local start now
  start="$(date +%s)"

  log "waiting fifo create: fifo=$fifo timeout=${timeout_sec}s"
  while [[ ! -p "$fifo" ]]; do
    now="$(date +%s)"
    if (( now - start >= timeout_sec )); then
      die "fifo not created before timeout: fifo=$fifo timeout=${timeout_sec}s"
    fi
    sleep 0.2
  done

  log "fifo created: fifo=$fifo"
  log "opening fifo reader: fifo=$fifo"

  exec 3<"$fifo" || die "failed to open fifo for reading: fifo=$fifo"

  now="$(date +%s)"
  if (( now - start >= timeout_sec )); then
    die "timeout before waiting ready signal: fifo=$fifo timeout=${timeout_sec}s"
  fi

  local remain
  remain=$(( timeout_sec - (now - start) ))

  if command -v timeout >/dev/null 2>&1; then
    timeout "${remain}s" bash -c 'IFS= read -r _ <&3' bash 3<&3 || \
      die "timeout waiting ready signal: fifo=$fifo timeout=${timeout_sec}s"
  else
    # 没有 timeout 命令时退化处理
    IFS= read -r _ <&3 || die "fifo read failed: fifo=$fifo"
  fi

  log "ready signal received: fifo=$fifo"
}

main "$@"