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
# processor.latest.log is owned/rotated by processor itself.
# DO NOT truncate it here, otherwise previous latest content cannot be copied
# into the timestamped history log during processor startup.
: > "./logs/$MODE/writer.latest.log"

# 停旧
./scripts/$MODE/logview.sh stop >/dev/null 2>&1 || true
./scripts/$MODE/logtail.sh stop all >/dev/null 2>&1 || true

# 起核心系统
nohup "./scripts/$MODE/logpipe.sh" start \
  2>&1 | tee -a "./logs/$MODE/run.latest.log" "./logs/$MODE/run.start.$TS.log" &

# logview (experimental; keep current foreground console unchanged)
#sleep 1
nohup "./scripts/$MODE/logview.sh" start --mode split >/dev/null 2>&1 &

# 主控制台
"./scripts/$MODE/logtail.sh" interactive --start-all \
  2>&1 | tee -a "./logs/$MODE/run.latest.log" "./logs/$MODE/run.console.$TS.log"
