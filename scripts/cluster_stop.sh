#!/usr/bin/env bash
set -euo pipefail

stop_console_host() {
  local pid_file="./logs/cluster/console.pid"
  local pid

  [[ -f "$pid_file" ]] || return 0

  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -z "$pid" ]]; then
    rm -f "$pid_file"
    return 0
  fi

  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi

  rm -f "$pid_file"
}

MODE=cluster
mkdir -p ./logs/$MODE
TS=$(date '+%Y%m%d_%H%M%S')

: > ./logs/$MODE/reset.latest.log

#./scripts/$MODE/logview.sh stop >/dev/null 2>&1 || true
./scripts/$MODE/logtail.sh stop all >/dev/null 2>&1 || true

FULL_RESET=1 ./scripts/$MODE/factory_reset.sh \
  2>&1 | tee -a "./logs/$MODE/reset.latest.log" "./logs/$MODE/reset.$TS.log"

stop_console_host || true
