#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/kill_apps.sh
#
# run on target node
# - kill project app processes by APP_KILL_RE
# - only concerns app layer, not kafka/pg infra
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [kill_apps] $*"; }

main() {
  if [[ -z "${APP_KILL_RE:-}" ]]; then
    log "APP_KILL_RE empty; skip"
    return 0
  fi

  log "pkill -9 -f $APP_KILL_RE"
  pkill -9 -f "$APP_KILL_RE" 2>/dev/null || true
  log "done"
}

main "$@"