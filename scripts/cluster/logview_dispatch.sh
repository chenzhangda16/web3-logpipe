#!/usr/bin/env bash
set -euo pipefail
trap '' PIPE

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

open_fifo_writers() {
  exec 31>"$(logview_fifo_path fetcher.frame)"
  exec 32>"$(logview_fifo_path processor.frame)"
}

write_fifo_line() {
  local fifo="${1:?fifo required}"
  local line="${2:?line required}"

  case "$fifo" in
    fetcher.frame)   printf '%s\n' "$line" >&31 || true ;;
    processor.frame) printf '%s\n' "$line" >&32 || true ;;
    *) return 1 ;;
  esac
}

route_line() {
  local line="${1:-}"
  [[ -n "$line" ]] || return 0
  [[ "$line" == "==>"*"<==" ]] && return 0

  local ver svc kind json
  IFS=$'\t' read -r ver svc kind json _ <<< "$line"

  [[ "$ver" == "BENCHv1" ]] || return 0
  [[ "$kind" == "frame" ]] || return 0

  case "$svc" in
    fetcher)   write_fifo_line "fetcher.frame" "$line" ;;
    processor) write_fifo_line "processor.frame" "$line" ;;
    *) return 0 ;;
  esac
}