#!/usr/bin/env bash
set -euo pipefail

MODE=cluster

mkdir -p ./logs/$MODE && TS=$(date '+%Y%m%d_%H%M%S') && : > ./logs/$MODE/reset.latest.log && FULL_RESET=1 ./scripts/$MODE/factory_reset.sh 2>&1 | tee -a ./logs/$MODE/reset.latest.log > ./logs/$MODE/reset.$TS.log