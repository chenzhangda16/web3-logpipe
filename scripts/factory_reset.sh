#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# factory_reset.sh
# - Brutal, deterministic reset for local dev.
# - Only touches project-owned data directories + business DB.
# - Then re-runs ensure_pg.sh + ensure_kafka.sh to re-bootstrap infra.
#
# It assumes ensure_kafka.sh pins log.dirs into:
#   $ROOT_DIR/data/kafka/logs  (project-owned)
# and project config:
#   $ROOT_DIR/data/kafka/server.properties
# ------------------------------------------------------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# -------- app proc patterns (your preferred "simple brutal") -------------------
APP_KILL_RE='/bin/(mockchain|fetcher|processor|writer)\b'

# ----------------------------- PG defaults ------------------------------------
PG_DSN="${PG_DSN:-postgres://web3:web3@127.0.0.1:5432/web3log?sslmode=disable}"
PG_ADMIN_DSN="${PG_ADMIN_DSN:-postgres://chenchangda@127.0.0.1:5432/postgres?sslmode=disable}"
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB_NAME="${PG_DB_NAME:-web3log}"
PG_DB_OWNER="${PG_DB_OWNER:-web3}"

# ----------------------------- Kafka defaults (align ensure_kafka.sh) ----------
KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
PID_DIR="${PID_DIR:-$ROOT_DIR/data/pids}"
LOG_DIR="${LOG_DIR:-$ROOT_DIR/data/logs}"

KAFKA_HOME="${KAFKA_HOME:-/opt/kafka_2.13-3.8.0}"
KAFKA_SERVER_START="${KAFKA_SERVER_START:-$KAFKA_HOME/bin/kafka-server-start.sh}"

KAFKA_PROJECT_DIR="${KAFKA_PROJECT_DIR:-$ROOT_DIR/data/kafka}"
KAFKA_PROJECT_LOG_DIR="${KAFKA_PROJECT_LOG_DIR:-$KAFKA_PROJECT_DIR/logs}"
KAFKA_PROJECT_CONFIG="${KAFKA_PROJECT_CONFIG:-$KAFKA_PROJECT_DIR/server.properties}"

KAFKA_PID_FILE="${KAFKA_PID_FILE:-$PID_DIR/kafka.pid}"
KAFKA_TAIL_PID_FILE="${KAFKA_TAIL_PID_FILE:-$PID_DIR/kafka_tail.pid}"

mkdir -p "$PID_DIR" "$LOG_DIR"

ts() { date '+%F %T'; }
log() { echo "[$(ts)] [factory_reset] $*"; }

pid_alive() { kill -0 "$1" >/dev/null 2>&1; }

kill_pid_soft_hard() {
  local pid="$1"
  [[ -z "$pid" ]] && return 0
  if ! pid_alive "$pid"; then
    return 0
  fi
  kill -TERM "$pid" >/dev/null 2>&1 || true
  sleep 0.2
  pid_alive "$pid" && kill -KILL "$pid" >/dev/null 2>&1 || true
}

kafka_stop_by_pidfile() {
  # stop tailer first (avoid holding file handles)
  if [[ -f "$KAFKA_TAIL_PID_FILE" ]]; then
    local tpid
    tpid="$(cat "$KAFKA_TAIL_PID_FILE" 2>/dev/null || true)"
    [[ -n "$tpid" ]] && kill_pid_soft_hard "$tpid"
    rm -f "$KAFKA_TAIL_PID_FILE" || true
  fi

  if [[ -f "$KAFKA_PID_FILE" ]]; then
    local kpid
    kpid="$(cat "$KAFKA_PID_FILE" 2>/dev/null || true)"
    [[ -n "$kpid" ]] && kill_pid_soft_hard "$kpid"
    rm -f "$KAFKA_PID_FILE" || true
  fi
}

kafka_stop_fallback() {
  # Fallback: match Kafka Java processes (version-dependent class names)
  pkill -TERM -f 'kafka\.Kafka|KafkaRaftServer|QuorumController' 2>/dev/null || true
  sleep 0.5
  pkill -KILL -f 'kafka\.Kafka|KafkaRaftServer|QuorumController' 2>/dev/null || true

  # Also kill kafka-server-start.sh wrapper if any
  pkill -TERM -f 'kafka-server-start\.sh' 2>/dev/null || true
  sleep 0.2
  pkill -KILL -f 'kafka-server-start\.sh' 2>/dev/null || true
}

rm_kafka_project_storage() {
  # Only delete project-owned kafka storage/logs.
  if [[ -d "$KAFKA_PROJECT_DIR" ]]; then
    log "Removing Kafka project dir: $KAFKA_PROJECT_DIR"
    rm -rf "$KAFKA_PROJECT_DIR"
  fi

  # Ensure no stale logs in shared log dir
  rm -f "$LOG_DIR"/kafka.*.log "$LOG_DIR"/kafka.latest.log 2>/dev/null || true
}

pg_reset_business_db() {
  if ! command -v psql >/dev/null 2>&1; then
    log "psql not found; skipping PG reset."
    return 0
  fi

  log "Dropping Postgres DB: ${PG_DB_NAME}"
    psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -p "$PG_PORT" -d postgres <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname='${PG_DB_NAME}'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "${PG_DB_NAME}";
SQL
}

main() {
  log "Killing app processes..."
  pkill -9 -f "$APP_KILL_RE" 2>/dev/null || true

  log "Stopping Kafka (pidfile first)..."
  kafka_stop_by_pidfile
  log "Stopping Kafka (fallback scan)..."
  kafka_stop_fallback

  log "Reset Kafka project-owned storage..."
  rm_kafka_project_storage

  log "Reset Postgres business DB..."
  pg_reset_business_db

  log "Re-bootstrap Postgres (ensure_pg.sh)..."
  "$ROOT_DIR/scripts/ensure_pg.sh"

  log "Re-bootstrap Kafka (ensure_kafka.sh)..."
  "$ROOT_DIR/scripts/ensure_kafka.sh"

  case "${FULL_RESET:-0}" in
    2)
      log "FULL_RESET=2, nuking entire $ROOT_DIR/data"
      rm -rf "$ROOT_DIR/data"
      mkdir -p "$ROOT_DIR/data"
      ;;
    1)
      log "FULL_RESET=1, wiping $ROOT_DIR/data except mockchain.db"

      # 先确保 data 存在
      mkdir -p "$ROOT_DIR/data"

      # 删除 data 下除 mockchain.db 之外的所有内容
      find "$ROOT_DIR/data" -mindepth 1 -maxdepth 1 \
        ! -path "$ROOT_DIR/data/mockchain.db" \
        -exec rm -rf {} +

      ;;
    *)
      log "FULL_RESET disabled; skipping full data wipe"
      ;;
  esac

  log "Done."
}

main "$@"
#FULL_RESET=1 ./scripts/factory_reset.sh