#!/usr/bin/env bash
set -euo pipefail

load_raw_env_stack() {
  local mode="${1:?mode required}"

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
}

require_assoc_array() {
  local name="${1:?name required}"
  declare -p "$name" >/dev/null 2>&1 || {
    echo "missing assoc array: $name" >&2
    return 1
  }
}

load_topology_stack() {
  local mode="${1:?mode required}"

  if [[ "$mode" == local ]]; then
    # local 模式可以直接给出本机 canonical service vars
    : "${HOST_IP:=127.0.0.1}"

    export MOCKCHAIN_HOST=127.0.0.1
    export MOCKCHAIN_IP=127.0.0.1
    export FETCHER_HOST=127.0.0.1
    export FETCHER_IP=127.0.0.1
    export PROCESSOR_HOST=127.0.0.1
    export PROCESSOR_IP=127.0.0.1
    export WRITER_HOST=127.0.0.1
    export WRITER_IP=127.0.0.1
    export KAFKA_HOST=127.0.0.1
    export KAFKA_IP=127.0.0.1
    export PG_HOST=127.0.0.1
    export PG_IP=127.0.0.1

    # local 模式下远端 root/bin/log 直接映射到本项目
    export ROOT_LOCAL="$ROOT_DIR"
    export BIN_DIR_LOCAL="$BIN_DIR"
    export LOG_DIR_LOCAL="$LOG_DIR"

    return 0
  fi

  require_assoc_array NODE_IP
  require_assoc_array NODE_HOST
  require_assoc_array NODE_ARCH
  require_assoc_array NODE_ROOT
  require_assoc_array SERVICE_NODE
  require_assoc_array SERVICE_PORT

  local node svc root arch

  for node in "${!NODE_ROOT[@]}"; do
    root="${NODE_ROOT[$node]}"
    arch="${NODE_ARCH[$node]}"

    printf -v "ROOT_${node}" '%s' "$root"
    printf -v "BIN_DIR_${node}" '%s/bin/%s' "$root" "$arch"
    printf -v "LOG_DIR_${node}" '%s/logs' "$root"

    export "ROOT_${node}" "BIN_DIR_${node}" "LOG_DIR_${node}"
  done

  for svc in "${!SERVICE_NODE[@]}"; do
    node="${SERVICE_NODE[$svc]}"
    root="${NODE_ROOT[$node]}"
    arch="${NODE_ARCH[$node]}"

    printf -v "${svc}_NODE" '%s' "$node"
    printf -v "${svc}_HOST" '%s' "${NODE_HOST[$node]}"
    printf -v "${svc}_IP" '%s' "${NODE_IP[$node]}"
    printf -v "${svc}_ROOT" '%s' "$root"
    printf -v "${svc}_BIN_DIR" '%s/bin/%s' "$root" "$arch"
    printf -v "${svc}_LOG_DIR" '%s/logs' "$root"

    export \
      "${svc}_NODE" \
      "${svc}_HOST" \
      "${svc}_IP" \
      "${svc}_ROOT" \
      "${svc}_BIN_DIR" \
      "${svc}_LOG_DIR"
  done

  for svc in "${!SERVICE_PORT[@]}"; do
    printf -v "${svc}_PORT" '%s' "${SERVICE_PORT[$svc]}"
    export "${svc}_PORT"
  done

  # compatibility aliases
  export MOCK_RPC_HOST="$MOCKCHAIN_HOST"
  export MOCK_RPC_IP="$MOCKCHAIN_IP"
}

load_runtime_env_stack() {
  # ---------- mock rpc alias ----------
  : "${MOCKCHAIN_HOST:?MOCKCHAIN_HOST not set}"
  : "${MOCKCHAIN_IP:?MOCKCHAIN_IP not set}"
  : "${MOCK_RPC_PORT:?MOCK_RPC_PORT not set}"

  export MOCK_RPC_HOST="${MOCK_RPC_HOST:-$MOCKCHAIN_HOST}"
  export MOCK_RPC_IP="${MOCK_RPC_IP:-$MOCKCHAIN_IP}"
  export MOCK_RPC="${MOCK_RPC_IP}:${MOCK_RPC_PORT}"
  export RPC_BASE="${RPC_BASE:-http://${MOCK_RPC}}"

  # ---------- postgres ----------
  : "${PG_IP:?PG_IP not set}"
  : "${PG_PORT:?PG_PORT not set}"
  : "${PG_USER:?PG_USER not set}"
  : "${PG_PASS:?PG_PASS not set}"
  : "${PG_DB:?PG_DB not set}"

  export PG_DB_OWNER="${PG_DB_OWNER:-$PG_USER}"
  export PG_SUPERUSER="${PG_SUPERUSER:-$(id -un)}"
  export PG_DSN="${PG_DSN:-postgres://${PG_USER}:${PG_PASS}@${PG_IP}:${PG_PORT}/${PG_DB}?sslmode=disable}"
  export PG_ADMIN_DSN="${PG_ADMIN_DSN:-postgres://${PG_SUPERUSER}@${PG_IP}:${PG_PORT}/postgres?sslmode=disable}"

  # ---------- kafka ----------
  : "${KAFKA_IP:?KAFKA_IP not set}"
  : "${KAFKA_PORT:?KAFKA_PORT not set}"

  export KAFKA_BROKERS="${KAFKA_BROKERS:-${KAFKA_IP}:${KAFKA_PORT}}"

  # ---------- misc ----------
  export no_proxy="${no_proxy:-$NO_PROXY}"
}