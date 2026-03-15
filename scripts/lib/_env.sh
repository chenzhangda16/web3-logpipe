#!/usr/bin/env bash
set -euo pipefail

load_env_stack() {
  local mode="${1:?mode required}"  # local | cluster

  local common_env="$ENV_DIR/common.env"
  local mode_env="$ENV_DIR/${mode}.env"

  [[ -f "$common_env" ]] || {
    echo "missing env file: $common_env" >&2
    return 1
  }
  [[ -f "$mode_env" ]] || {
    echo "missing env file: $mode_env" >&2
    return 1
  }

  set -a
  source "$common_env"
  source "$mode_env"
  set +a

  # -------------------- compatibility / derived env --------------------

  # postgres canonical -> compatibility
  PG_DB_OWNER="${PG_DB_OWNER:-$PG_USER}" # unused
  PG_SUPERUSER="${PG_SUPERUSER:-$(id -un)}"

  PG_DSN="${PG_DSN:-postgres://${PG_USER}:${PG_PASS}@${PG_IP}:${PG_PORT}/${PG_DB}?sslmode=disable}"
  PG_ADMIN_DSN="${PG_ADMIN_DSN:-postgres://${PG_SUPERUSER}@${PG_IP}:${PG_PORT}/postgres?sslmode=disable}" # unused

  # mock / rpc
  MOCK_RPC="${MOCK_RPC_IP}:${MOCK_RPC_PORT}"
  RPC_BASE="${RPC_BASE:-http://${MOCK_RPC}}"

  # kafka
  KAFKA_BROKERS="${KAFKA_BROKERS:-${KAFKA_IP}:${KAFKA_PORT}}"

  KAFKA_SERVER_START="${KAFKA_SERVER_START:-$KAFKA_HOME/bin/kafka-server-start.sh}"
  KAFKA_STORAGE="${KAFKA_STORAGE:-$KAFKA_HOME/bin/kafka-storage.sh}"
  KAFKA_TOPICS_SH="${KAFKA_TOPICS_SH:-kafka-topics.sh}"
  KAFKA_CONFIG="${KAFKA_CONFIG:-$KAFKA_HOME/config/kraft/server.properties}"

  # kill pattern for factory_reset
  APP_KILL_RE="${APP_KILL_RE:-/bin/(mockchain|fetcher|processor|writer)\b}"

  # proxy compatibility
  no_proxy="${no_proxy:-$NO_PROXY}"

  export PG_USER PG_DB_OWNER PG_SUPERUSER
  export PG_DSN PG_ADMIN_DSN
  export MOCK_RPC RPC_BASE
  export KAFKA_BROKERS KAFKA_SERVER_START KAFKA_STORAGE KAFKA_TOPICS_SH KAFKA_CONFIG
  export APP_KILL_RE
  export no_proxy
}