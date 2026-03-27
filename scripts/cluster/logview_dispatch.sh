#!/usr/bin/env bash
set -euo pipefail
cleanup() {
  trap - EXIT TERM INT PIPE
  exec 31>&- 32>&- || true
}

trap '' PIPE
trap cleanup EXIT TERM INT

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

  local ver svc json _
  IFS=$'\t' read -r ver svc json _ <<< "$line"

  [[ "$ver" == "BENCHv1" ]] || return 0

  case "$svc" in
    fetcher)   write_fifo_line "fetcher.frame" "$json" ;;
    processor) write_fifo_line "processor.frame" "$json" ;;
    *) return 0 ;;
  esac
}

main() {
  open_fifo_writers

  files=()
  for svc in "${SERVICES[@]}"; do
    files+=("./logs/cluster/$svc.latest.log")
  done

  while IFS= read -r line; do
    route_line "$line"
  done < <(tail -n0 -F -q "${files[@]}")
}

main "$@"