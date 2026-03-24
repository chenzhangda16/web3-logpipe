#!/usr/bin/env bash
set -euo pipefail
trap '' PIPE

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

open_fifo_writers() {
  exec 31>"$(logview_fifo_path fetcher.flow)"
  exec 32>"$(logview_fifo_path fetcher.core)"
  exec 33>"$(logview_fifo_path fetcher.wire)"
  exec 34>"$(logview_fifo_path processor.flow)"
  exec 35>"$(logview_fifo_path processor.core)"
  exec 36>"$(logview_fifo_path processor.wire)"
  exec 37>"$(logview_fifo_path processor.wins)"
}

write_fifo_line() {
  local fifo="${1:?fifo required}"
  local line="${2:?line required}"

  case "$fifo" in
    fetcher.flow)    printf '%s\n' "$line" >&31 || true ;;
    fetcher.core)    printf '%s\n' "$line" >&32 || true ;;
    fetcher.wire)    printf '%s\n' "$line" >&33 || true ;;
    processor.flow)  printf '%s\n' "$line" >&34 || true ;;
    processor.core)  printf '%s\n' "$line" >&35 || true ;;
    processor.wire)  printf '%s\n' "$line" >&36 || true ;;
    *) return 1 ;;
  esac
}

route_line() {
  local line="${1:-}"
  [[ -n "$line" ]] || return 0

  # 过滤 tail 多文件头
  [[ "$line" == "==>"*"<==" ]] && return 0

  local ver svc kind json
  IFS=$'\t' read -r ver svc kind json _ <<< "$line"

  [[ "$ver" == "BENCHv1" ]] || return 0
  [[ -n "${svc:-}" && -n "${kind:-}" && -n "${json:-}" ]] || return 0

  case "$svc:$kind" in
    fetcher:flow)
      write_fifo_line "fetcher.flow" "$line"
      ;;
    fetcher:core)
      write_fifo_line "fetcher.core" "$line"
      ;;
    fetcher:wire)
      write_fifo_line "fetcher.wire" "$line"
      ;;

    processor:flow)
      write_fifo_line "processor.flow" "$line"
      write_fifo_line "processor.wins" "$line"
      ;;
    processor:core)
      write_fifo_line "processor.core" "$line"
      ;;
    processor:wire)
      write_fifo_line "processor.wire" "$line"
      ;;
  esac
}