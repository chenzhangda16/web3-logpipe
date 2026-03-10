#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/drop_pg_db.sh
#
# run on target node
# - drop only business DB PG_DB from reachable postgres at PG_HOST:PG_PORT
# - does not stop cluster
# ------------------------------------------------------------------------------

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
  bootstrap cluster
fi

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [drop_pg_db] $*"; }

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

main() {
  if ! have_cmd psql; then
    log "psql not found; skip"
    return 0
  fi

  log "dropping database PG_DB=$PG_DB on ${PG_HOST}:${PG_PORT}"

  psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "$PG_PORT" -d postgres <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname='${PG_DB}'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "${PG_DB}";
SQL

  log "done"
}

main "$@"