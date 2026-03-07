#!/usr/bin/env bash
set -euo pipefail

mkdir -p ./logs/local && TS=$(date '+%Y%m%d_%H%M%S') && : > ./logs/local/reset.latest.log && FULL_RESET=1 ./scripts/local/factory_reset.sh 2>&1 | tee -a ./logs/local/reset.latest.log > ./logs/local/reset.$TS.log