#!/usr/bin/env bash
set -euo pipefail

MODE=cluster
mkdir -p "./logs/$MODE"

console_pid_file="./logs/$MODE/console.pid"
echo "$$" > "$console_pid_file"
trap 'rm -f "$console_pid_file"' EXIT

TS="$(date '+%Y%m%d_%H%M%S')"

: > "./logs/$MODE/run.latest.log"
: > "./logs/$MODE/mockchain.latest.log"
: > "./logs/$MODE/fetcher.latest.log"
: > "./logs/$MODE/writer.latest.log"

./scripts/$MODE/logtail.sh stop all >/dev/null 2>&1 || true

nohup "./scripts/$MODE/logpipe.sh" start \
  2>&1 | tee -a "./logs/$MODE/run.latest.log" "./logs/$MODE/run.start.$TS.log" &

"./scripts/$MODE/logtail.sh" interactive --start-all \
  2>&1 | tee -a "./logs/$MODE/run.latest.log" "./logs/$MODE/run.console.$TS.log"