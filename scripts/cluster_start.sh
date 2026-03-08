#!/usr/bin/env bash
set -euo pipefail

MODE=cluster

mkdir -p ./logs/$MODE && TS=$(date '+%Y%m%d_%H%M%S') && : > ./logs/$MODE/run.latest.log && ./scripts/$MODE/logpipe.sh start 2>&1 | tee -a ./logs/$MODE/run.latest.log > ./logs/$MODE/run.$TS.log