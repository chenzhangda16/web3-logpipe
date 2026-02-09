#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"$ROOT_DIR/scripts/stop_kafka.sh" || true
"$ROOT_DIR/scripts/stop_pg.sh" || true
