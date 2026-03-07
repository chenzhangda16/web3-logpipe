#!/usr/bin/env bash
set -euo pipefail

mkdir -p ./logs/local && TS=$(date '+%Y%m%d_%H%M%S') && : > ./logs/local/run.latest.log && ./scripts/local/logpipe.sh start 2>&1 | tee -a ./logs/local/run.latest.log > ./logs/local/run.$TS.log