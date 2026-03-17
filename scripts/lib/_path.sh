#!/usr/bin/env bash
set -euo pipefail

get_root_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

load_base_path_stack() {
  local mode="${1:?mode required}"  # local | cluster

  ROOT_DIR="$(get_root_dir)"

  SCRIPTS_DIR="$ROOT_DIR/scripts"
  ENV_DIR="$SCRIPTS_DIR/env"
  CUR_MODE_SCRIPTS="$SCRIPTS_DIR/${mode}"

  DATA_DIR="$ROOT_DIR/data/${mode}"
  LOG_DIR="$ROOT_DIR/logs/${mode}"
  BIN_DIR="$ROOT_DIR/bin/${mode}"

  PID_DIR="$DATA_DIR/pids"
  ERR_DIR="$DATA_DIR/stderr"
  READY_DIR="$DATA_DIR/ready"

  MOCK_DB="$DATA_DIR/mockchain.db"
  FETCH_CKPT="$DATA_DIR/fetcher.ckpt"
  PROC_CKPT="$DATA_DIR/processor.ckpt"
  PROC_SPOOL="$DATA_DIR/spool.wal"

  PGDATA="$DATA_DIR/pg/data"

  KAFKA_PROJECT_DIR="$DATA_DIR/kafka"
  KAFKA_PROJECT_LOG_DIR="$KAFKA_PROJECT_DIR/logs"
  KAFKA_PROJECT_CONFIG="$KAFKA_PROJECT_DIR/server.properties"

  PID_FILE="$PID_DIR/logpipe.pids"
  KAFKA_PID_FILE="$PID_DIR/kafka.pid"
  KAFKA_TAIL_PID_FILE="$PID_DIR/kafka_tail.pid"
  if [[ "$mode" == local ]]; then
    APP_KILL_RE="${APP_KILL_RE:-/bin/local/(mockchain|fetcher|processor|writer)\b}"
  fi

  export ROOT_DIR SCRIPTS_DIR ENV_DIR CUR_MODE_SCRIPTS
  export DATA_DIR LOG_DIR BIN_DIR
  export PID_DIR ERR_DIR READY_DIR
  export MOCK_DB FETCH_CKPT PROC_CKPT PROC_SPOOL
  export PGDATA
  export KAFKA_PROJECT_DIR KAFKA_PROJECT_LOG_DIR KAFKA_PROJECT_CONFIG
  export PID_FILE KAFKA_PID_FILE KAFKA_TAIL_PID_FILE APP_KILL_RE
}

ensure_base_path_stack() {
  : "${ROOT_DIR:?ROOT_DIR not set}"
  : "${DATA_DIR:?DATA_DIR not set}"
  : "${LOG_DIR:?LOG_DIR not set}"
  : "${BIN_DIR:?BIN_DIR not set}"
  : "${PID_DIR:?PID_DIR not set}"
  : "${ERR_DIR:?ERR_DIR not set}"
  : "${READY_DIR:?READY_DIR not set}"
  : "${KAFKA_PROJECT_DIR:?KAFKA_PROJECT_DIR not set}"
  : "${KAFKA_PROJECT_LOG_DIR:?KAFKA_PROJECT_LOG_DIR not set}"

  mkdir -p \
    "$DATA_DIR" \
    "$LOG_DIR" \
    "$BIN_DIR" \
    "$PID_DIR" \
    "$ERR_DIR" \
    "$READY_DIR" \
    "$KAFKA_PROJECT_DIR" \
    "$KAFKA_PROJECT_LOG_DIR"
}