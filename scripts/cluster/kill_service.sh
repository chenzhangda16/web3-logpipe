#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [kill_service] $*"; }
die() { echo "[$(ts)] [kill_service] ERROR: $*" >&2; exit 1; }

main() {
  local svc="${1:-}"
  local remote_bin="${2:-}"

  [[ -n "$svc" ]] || die "usage: kill_service.sh <SERVICE> <REMOTE_BIN>"
  [[ -n "$remote_bin" ]] || die "usage: kill_service.sh <SERVICE> <REMOTE_BIN>"

  local pattern="^${remote_bin}( |$)"

  log "svc=$svc remote_bin=$remote_bin"
  log "svc=$svc pattern=$pattern"

  local matched
  matched="$(pgrep -af -- "$pattern" || true)"
  if [[ -z "$matched" ]]; then
    log "svc=$svc no process matched"
    return 0
  fi

  log "svc=$svc matched:"
  while IFS= read -r line; do
    [[ -n "$line" ]] && log "  $line"
  done <<< "$matched"

  pkill -f -- "$pattern" 2>/dev/null || true
  sleep 1

  matched="$(pgrep -af -- "$pattern" || true)"
  if [[ -n "$matched" ]]; then
    log "svc=$svc still alive after soft kill; escalate -9"
    while IFS= read -r line; do
      [[ -n "$line" ]] && log "  $line"
    done <<< "$matched"
    pkill -9 -f -- "$pattern" 2>/dev/null || true
  fi
}

main "$@"