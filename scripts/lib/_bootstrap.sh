#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf '%s\n' "error: do not execute this file directly." >&2
  printf '%s\n' "use: source ${BASH_SOURCE[0]}" >&2
  exit 1
fi

# Guard: source library body only once per shell/process.
if [[ -n "${__WEB3_LOGPIPE_BOOTSTRAP_LIB_SOURCED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
__WEB3_LOGPIPE_BOOTSTRAP_LIB_SOURCED=1

bootstrap() {
  local mode="${1:?usage: bootstrap <mode>}"

  # mode 一旦确定，不允许同一 shell 混用
  if [[ -n "${BOOTSTRAP_MODE:-}" && "${BOOTSTRAP_MODE}" != "$mode" ]]; then
    printf '%s\n' "bootstrap mode conflict: existing=${BOOTSTRAP_MODE} requested=$mode" >&2
    return 1
  fi

  export BOOTSTRAP_MODE="$mode"

  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_path.sh"
  load_path_stack "$mode"
  ensure_path_stack

  source "$SCRIPTS_DIR/lib/_env.sh"

  # 环境按 mode 做一次性幂等；路径与目录则每次都修复
  local env_flag_var="__WEB3_LOGPIPE_ENV_LOADED_${mode^^}"
  if [[ -z "${!env_flag_var:-}" ]]; then
    load_env_stack "$mode"
    printf -v "$env_flag_var" '%s' 1
    export "$env_flag_var"
  fi

  cd "$ROOT_DIR"
}