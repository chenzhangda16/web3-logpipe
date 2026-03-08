#!/usr/bin/env bash
set -euo pipefail

get_root_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

load_path_stack() {
  local mode="${1:?mode required}"  # local | cluster
  ROOT_DIR="$(get_root_dir)"

  SCRIPTS_DIR="$ROOT_DIR/scripts"
  ENV_DIR="$SCRIPTS_DIR/env"

  DATA_DIR="$ROOT_DIR/data"
  LOG_DIR="$ROOT_DIR/logs/${mode}"
  BIN_DIR="$ROOT_DIR/bin"

  PID_DIR="$DATA_DIR/pids"
  READY_DIR="$DATA_DIR/ready"

  MOCK_DB="$DATA_DIR/mockchain.db"
  FETCH_CKPT="$DATA_DIR/fetcher.ckpt"
  PROC_CKPT="$DATA_DIR/processor.ckpt"
  PROC_SPOOL="$DATA_DIR/spool.wal"

  PGDATA="$DATA_DIR/pg/data"
  PG_LOG_DIR="$LOG_DIR"

  KAFKA_PROJECT_DIR="$DATA_DIR/kafka"
  KAFKA_PROJECT_LOG_DIR="$KAFKA_PROJECT_DIR/logs/${mode}"
  KAFKA_PROJECT_CONFIG="$KAFKA_PROJECT_DIR/server.properties"

  PID_FILE="$PID_DIR/logpipe.pids"
  KAFKA_PID_FILE="$PID_DIR/kafka.pid"
  KAFKA_TAIL_PID_FILE="$PID_DIR/kafka_tail.pid"

  mkdir -p \
    "$PID_DIR" \
    "$LOG_DIR" \
    "$BIN_DIR" \
    "$READY_DIR" \
    "$KAFKA_PROJECT_DIR" \
    "$KAFKA_PROJECT_LOG_DIR"

  export ROOT_DIR SCRIPTS_DIR ENV_DIR
  export DATA_DIR LOG_DIR BIN_DIR
  export PID_DIR READY_DIR
  export MOCK_DB FETCH_CKPT PROC_CKPT PROC_SPOOL
  export PGDATA PG_LOG_DIR
  export KAFKA_PROJECT_DIR KAFKA_PROJECT_LOG_DIR KAFKA_PROJECT_CONFIG
  export PID_FILE KAFKA_PID_FILE KAFKA_TAIL_PID_FILE
}