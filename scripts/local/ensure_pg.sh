#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# ensure_pg.sh (Project-managed Postgres, pg_ctl only)
#
# Goals:
# - Ensure project-managed postgres is reachable; if not, initdb (if needed) + pg_ctl start
# - Ensure role/db exist
# - Export PG_DSN
# - Avoid colliding with system postgres (default port uses 55432, not 5432)
# - Refuse to operate if connected cluster's data_directory != expected PGDATA
# ------------------------------------------------------------------------------
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap local

pglog() { echo "[$(date '+%F %T')] [ensure_pg] $*"; }
warn()  { echo "[$(date '+%F %T')] [ensure_pg] WARN: $*" >&2; }
die()   { echo "[$(date '+%F %T')] [ensure_pg] ERROR: $*" >&2; exit 1; }

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

canonical_path() {
  local p="$1"
  if [[ -d "$p" ]]; then
    (cd "$p" && pwd)
  else
    local dir base
    dir="$(dirname "$p")"
    base="$(basename "$p")"
    if [[ -d "$dir" ]]; then
      echo "$(cd "$dir" && pwd)/$base"
    else
      echo "$p"
    fi
  fi
}

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

pg_is_up() {
  pg_isready -h "$PG_IP" -p "$PG_PORT" >/dev/null 2>&1
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

psql_as() {
  local db_user="$1"
  shift
  PGPASSWORD="${PGPASSWORD:-}" psql -v ON_ERROR_STOP=1 -h "$PG_IP" -p "$PG_PORT" -U "$db_user" -d postgres "$@"
}

psql_super() {
  psql_as "$PG_SUPERUSER" "$@"
}

ensure_runtime_config() {
  local conf="$PGDATA/postgresql.conf"
  [[ -f "$conf" ]] || die "postgresql.conf not found: $conf"

  sed -i \
    -e "/^[[:space:]]*listen_addresses[[:space:]]*=/d" \
    -e "/^[[:space:]]*port[[:space:]]*=/d" \
    -e "/^[[:space:]]*unix_socket_directories[[:space:]]*=/d" \
    "$conf"

  {
    echo ""
    echo "# added by scripts/local/ensure_pg.sh"
    echo "listen_addresses = '*'"
    echo "port = $PG_PORT"
    echo "unix_socket_directories = '$PGDATA'"
  } >> "$conf"
}

ensure_pg_hba() {
  local hba="$PGDATA/pg_hba.conf"
  [[ -f "$hba" ]] || die "pg_hba.conf not found: $hba"

  if ! grep -Fqx "host    all    all    127.0.0.1/32      trust" "$hba"; then
    {
      echo ""
      echo "# added by scripts/local/ensure_pg.sh"
      echo "host    all    all    127.0.0.1/32      trust"
      echo "host    all    all    192.168.1.0/24    trust"
      echo "host    all    all    ::1/128           trust"
    } >> "$hba"
  fi
}

ensure_inited() {
  have_cmd initdb || die "initdb not found. Install postgresql server utilities."
  mkdir -p "$PGDATA" "$LOG_DIR"

  if [[ -s "$PGDATA/PG_VERSION" ]]; then
    return 0
  fi

  pglog "Initializing PGDATA at $PGDATA (auth=$PG_INITDB_AUTH superuser=$PG_SUPERUSER)"
  initdb -D "$PGDATA" -A "$PG_INITDB_AUTH" -U "$PG_SUPERUSER" >/dev/null

  local conf="$PGDATA/postgresql.conf"
  if [[ -f "$conf" ]]; then
    {
      echo ""
      echo "# added by scripts/local/ensure_pg.sh"
      echo "listen_addresses = '$PG_IP'"
      echo "port = $PG_PORT"
      echo "unix_socket_directories = '$PGDATA'"
    } >> "$conf"
  fi
}

ensure_expected_cluster() {
  local actual expected
  actual="$(psql_super -Atqc "SHOW data_directory;" 2>/dev/null || true)"
  [[ -n "$actual" ]] || die "Cannot determine connected postgres data_directory at ${PG_IP}:${PG_PORT}"

  actual="$(canonical_path "$actual")"
  expected="$(canonical_path "$PGDATA")"

  if [[ "$actual" != "$expected" ]]; then
    die "Connected postgres data_directory=$actual, expected PGDATA=$expected. Wrong cluster is occupying ${PG_IP}:${PG_PORT}"
  fi
}

start_pg() {
  have_cmd pg_ctl || die "pg_ctl not found. Install postgresql server utilities."

  ensure_inited
  ensure_runtime_config
  ensure_pg_hba

  local ts_full latest hist tail_pid_file
  ts_full="$(date '+%Y%m%d_%H%M%S')"
  latest="$LOG_DIR/postgres.latest.log"
  hist="$LOG_DIR/postgres.$ts_full.log"
  tail_pid_file="$PID_DIR/postgres_tail.pid"

  if [[ -f "$PGDATA/postmaster.pid" ]]; then
    local pid
    pid="$(head -n 1 "$PGDATA/postmaster.pid" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && is_pid_alive "$pid"; then
      pglog "Postgres already running pid=$pid (PGDATA=$PGDATA)"
      return 0
    else
      warn "Removing stale postmaster.pid in $PGDATA"
      rm -f "$PGDATA/postmaster.pid" 2>/dev/null || true
    fi
  fi

  if [[ -f "$tail_pid_file" ]]; then
    local tpid
    tpid="$(cat "$tail_pid_file" 2>/dev/null || true)"
    if [[ -n "$tpid" ]] && is_pid_alive "$tpid"; then
      pglog "Stopping old postgres log tailer pid=$tpid"
      kill "$tpid" >/dev/null 2>&1 || true
      sleep 0.1
      is_pid_alive "$tpid" && kill -KILL "$tpid" >/dev/null 2>&1 || true
    fi
    rm -f "$tail_pid_file" >/dev/null 2>&1 || true
  fi

  : > "$latest"
  : > "$hist"

  pglog "Starting postgres via pg_ctl (host=$PG_IP port=$PG_PORT pgdata=$PGDATA superuser=$PG_SUPERUSER)"
  pglog "Postgres logs: latest=$latest hist=$hist"

  if ! pg_is_up; then
    pg_ctl -D "$PGDATA" -o "-p $PG_PORT -k $PGDATA" -l "$latest" start >/dev/null
  fi

  pg_wait_up 30 || die "Postgres failed to start. Check latest log: $latest"
  pg_ctl -D "$PGDATA" reload >/dev/null 2>&1 || true

  nohup tail -n 0 -F "$latest" >> "$hist" 2>&1 &
  echo "$!" > "$tail_pid_file"
  pglog "Postgres log tailer started pid=$(cat "$tail_pid_file") (latest -> hist)"
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
  export PG_DSN="postgres://${PG_USER}:${PG_PASS}@${PG_IP}:${PG_PORT}/${PG_DB}?sslmode=disable"
  pglog "Exported PG_DSN=$PG_DSN"
}

# ----------------------------- main -------------------------------------------
have_cmd pg_isready || die "pg_isready not found. Install postgresql-client."
have_cmd psql      || die "psql not found. Install postgresql-client."

pglog "Checking Postgres: ${PG_IP}:${PG_PORT} (expected PGDATA=$PGDATA)"

if pg_is_up; then
  pglog "Postgres reachable; verifying cluster identity..."
  ensure_expected_cluster
else
  pglog "Postgres not reachable; attempting to start project-managed cluster (pg_ctl)..."
  start_pg
  ensure_expected_cluster
fi

pg_wait_up 30 || die "Postgres still not reachable at ${PG_IP}:${PG_PORT}. Check logs under: $LOG_DIR"

pglog "Postgres reachable and cluster verified"
ensure_role
ensure_db
ensure_privileges
export_dsn
pglog "OK"