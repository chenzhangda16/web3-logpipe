#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf '%s\n' "error: do not execute this file directly." >&2
  printf '%s\n' "use: source ${BASH_SOURCE[0]}" >&2
  exit 1
fi

if [[ -n "${__WEB3_LOGPIPE_BOOTSTRAP_LIB_SOURCED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
__WEB3_LOGPIPE_BOOTSTRAP_LIB_SOURCED=1

bootstrap() {
  local mode="${1:?usage: bootstrap <mode>}"

  if [[ -n "${BOOTSTRAP_MODE:-}" && "${BOOTSTRAP_MODE}" != "$mode" ]]; then
    printf '%s\n' "bootstrap mode conflict: existing=${BOOTSTRAP_MODE} requested=$mode" >&2
    return 1
  fi
  export BOOTSTRAP_MODE="$mode"

  local lib_dir
  lib_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

  source "$lib_dir/_path.sh"
  load_base_path_stack "$mode"

  source "$SCRIPTS_DIR/lib/_env.sh"
  load_raw_env_stack "$mode"
  load_topology_stack "$mode"
  load_runtime_env_stack

  ensure_base_path_stack
  cd "$ROOT_DIR"
}