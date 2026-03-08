#!/usr/bin/env bash
set -euo pipefail

bootstrap() {
  local mode="${1:-local}"

  source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_path.sh"
  load_path_stack "$mode"

  source "$SCRIPTS_DIR/lib/_env.sh"
  load_env_stack "$mode"

  cd "$ROOT_DIR"
}