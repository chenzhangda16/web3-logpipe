#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# ensure_pg.sh (Project-managed Postgres, pg_ctl only)
# - Ensure postgres is reachable; if not, initdb (if needed) + pg_ctl start
# - Ensure role/db exist (using current user as superuser created by initdb)
# - Export PG_DSN
#
# Env (override):
#   PG_HOST=127.0.0.1
#   PG_PORT=5432
#   PG_DB=web3log
#   PG_USER=web3
#   PG_PASS=web3
#
#   PGDATA=<repo>/data/pg/data
#   PG_LOG_DIR=<repo>/data/logs
#   PG_INITDB_AUTH=trust
# ------------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${PID_DIR:-$ROOT_DIR/data/pids}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/data/logs}"
mkdir -p "$PID_DIR" "$LOG_DIR"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-web3log}"
PG_USER="${PG_USER:-web3}"
PG_PASS="${PG_PASS:-web3}"

PGDATA="${PGDATA:-$ROOT_DIR/data/pg/data}"
PG_LOG_DIR="${PG_LOG_DIR:-$LOG_DIR}"
PG_INITDB_AUTH="${PG_INITDB_AUTH:-trust}"

# explicit TCP
export PGHOST="$PG_HOST"
export PGPORT="$PG_PORT"

ts() { date '+%H:%M:%S'; }
pglog() { echo "[$(date '+%F %T')] [ensure_pg] $*"; }
warn() { echo "[$(date '+%F %T')] [ensure_pg] WARN: $*" >&2; }
die() { echo "[$(date '+%F %T')] [ensure_pg] ERROR: $*" >&2; exit 1; }

have_cmd() { command -v "$1" >/dev/null 2>&1; }

pg_is_up() {
  pg_isready -h "$PG_HOST" -p "$PG_PORT" >/dev/null 2>&1
}

pg_wait_up() {
  local timeout="${1:-30}"
  local start now
  start="$(date +%s)"
  while true; do
    pg_is_up && return 0
    now="$(date +%s)"
    (( now - start >= timeout )) && return 1
    sleep 0.2
  done
}

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

ensure_inited() {
  have_cmd initdb || die "initdb not found. Install postgresql (server utilities)."
  mkdir -p "$PGDATA" "$PG_LOG_DIR"
  if [[ -s "$PGDATA/PG_VERSION" ]]; then
    return 0
  fi
  pglog "Initializing PGDATA at $PGDATA (auth=$PG_INITDB_AUTH)"
  initdb -D "$PGDATA" -A "$PG_INITDB_AUTH" >/dev/null
}

start_pg() {
  have_cmd pg_ctl || die "pg_ctl not found. Install postgresql (server utilities)."

  ensure_inited

  local ts_full latest hist tail_pid_file
  ts_full="$(date '+%Y%m%d_%H%M%S')"
  latest="$PG_LOG_DIR/postgres.latest.log"
  hist="$PG_LOG_DIR/postgres.$ts_full.log"
  tail_pid_file="$PID_DIR/postgres_tail.pid"

  # If already running (postmaster.pid exists and PID alive), skip start
  if [[ -f "$PGDATA/postmaster.pid" ]]; then
    local pid
    pid="$(head -n 1 "$PGDATA/postmaster.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && is_pid_alive "$pid"; then
      pglog "Postgres already running pid=$pid (PGDATA=$PGDATA)"
      # still ensure tailer is sane (so hist capture works for this run)
      :
    else
      rm -f "$PGDATA/postmaster.pid" 2>/dev/null || true
    fi
  fi

  # kill old tailer if any (avoid multiple tailers writing to new hist)
  if [[ -f "$tail_pid_file" ]]; then
    local tpid
    tpid="$(cat "$tail_pid_file" 2>/dev/null || true)"
    if [[ -n "$tpid" ]] && is_pid_alive "$tpid"; then
      pglog "Stopping old postgres log tailer pid=$tpid"
      kill "$tpid" >/dev/null 2>&1 || true
      # small wait
      sleep 0.1
      is_pid_alive "$tpid" && kill -KILL "$tpid" >/dev/null 2>&1 || true
    fi
    rm -f "$tail_pid_file" >/dev/null 2>&1 || true
  fi

  # (Re)create logs: latest is the live rolling log; hist is archival for this start
  : >"$latest"
  : >"$hist"

  # Start postgres; it writes logs to latest via -l
  # Note: -k $PGDATA sets unix socket dir, harmless even if we use TCP explicitly.
  pglog "Starting postgres via pg_ctl (host=$PG_HOST port=$PG_PORT pgdata=$PGDATA)"
  pglog "Postgres logs: latest=$latest hist=$hist"

  if ! pg_is_up; then
    pg_ctl -D "$PGDATA" -o "-h $PG_HOST -p $PG_PORT -k $PGDATA" -l "$latest" start >/dev/null
  fi

  # Wait until reachable before starting the tailer (avoid tailing a file that never gets written)
  pg_wait_up 30 || die "Postgres failed to start. Check latest log: $latest"

  # Tail latest into hist for archival, in real-time.
  # Use -n 0 so hist only contains logs after this start.
  # Use -F to survive log truncation / recreation.
  nohup tail -n 0 -F "$latest" >> "$hist" 2>&1 &
  echo "$!" > "$tail_pid_file"
  pglog "Postgres log tailer started pid=$(cat "$tail_pid_file") (latest -> hist)"
}

psql_super() {
  # In project-managed cluster, the initdb-created superuser is the current OS user.
  # We connect to 'postgres' maintenance DB for role/db creation.
  psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "$PG_PORT" -d postgres "$@"
}

role_exists() {
  local role="$1"
  psql_super -tAc "SELECT 1 FROM pg_roles WHERE rolname='${role}'" | grep -qx "1"
}

db_exists() {
  local db="$1"
  psql_super -tAc "SELECT 1 FROM pg_database WHERE datname='${db}'" | grep -qx "1"
}

ensure_role() {
  role_exists "$PG_USER" && return 0
  pglog "Creating role: $PG_USER"
  psql_super -c "DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='${PG_USER}') THEN
    CREATE ROLE ${PG_USER} WITH LOGIN PASSWORD '${PG_PASS}';
  END IF;
END
\$\$;" >/dev/null
}

ensure_db() {
  db_exists "$PG_DB" && return 0
  pglog "Creating database: $PG_DB (owner=$PG_USER)"
  psql_super -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};" >/dev/null
}

ensure_privileges() {
  pglog "Ensuring privileges..."
  psql_super -c "ALTER DATABASE ${PG_DB} OWNER TO ${PG_USER};" >/dev/null
  psql_super -c "GRANT CONNECT ON DATABASE ${PG_DB} TO ${PG_USER};" >/dev/null
}

export_dsn() {
  if [[ -n "${PG_DSN:-}" ]]; then
    pglog "PG_DSN already set; keep existing."
    return 0
  fi
  export PG_DSN="postgres://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${PG_DB}?sslmode=disable"
  pglog "Exported PG_DSN=$PG_DSN"
}

# ----------------------------- main -------------------------------------------
have_cmd pg_isready || die "pg_isready not found. Install postgresql-client."
have_cmd psql || die "psql not found. Install postgresql-client."

pglog "Checking Postgres: ${PG_HOST}:${PG_PORT}"
if ! pg_is_up; then
  pglog "Postgres not reachable; attempting to start (pg_ctl)..."
  start_pg
fi

pg_wait_up 30 || die "Postgres still not reachable at ${PG_HOST}:${PG_PORT}. Check logs under: $PG_LOG_DIR"

pglog "Postgres reachable"
ensure_role
ensure_db
ensure_privileges
export_dsn
pglog "OK"
