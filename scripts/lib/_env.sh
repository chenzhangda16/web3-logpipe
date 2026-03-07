#!/usr/bin/env bash
set -euo pipefail

_root_dir_from_env_sh() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
}

load_env_stack() {
  local mode="${1:?mode required}"  # local | cluster
  local root
  root="$(_root_dir_from_env_sh)"

  local common_env="$root/scripts/env/common.env"
  local mode_env="$root/scripts/env/${mode}.env"

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