#!/usr/bin/env bash
set -euo pipefail

mode=""
service=""
fifo=""
fetcher_fifo=""
processor_fifo=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      shift
      mode="${1:-}"
      ;;
    --service)
      shift
      service="${1:-}"
      ;;
    --fifo)
      shift
      fifo="${1:-}"
      ;;
    --fetcher-fifo)
      shift
      fetcher_fifo="${1:-}"
      ;;
    --processor-fifo)
      shift
      processor_fifo="${1:-}"
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 1
      ;;
  esac
  shift || true
done

case "$mode" in
  single)
    printf '[viewer single] service=%s fifo=%s\n' "$service" "$fifo"
    exec stdbuf -oL cat "$fifo"
    ;;
  merge)
    printf '[viewer merge] fetcher_fifo=%s processor_fifo=%s\n' "$fetcher_fifo" "$processor_fifo"
    while true; do
      sleep 3600
    done
    ;;
  *)
    echo "invalid or missing --mode" >&2
    exit 1
    ;;
esac