#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

mode=""
service=""
fifo=""
fetcher_fifo=""
processor_fifo=""

viewer_pid=""
resume_after_int=0

confirm_exit() {
  while true; do
    printf '\n[viewer] Ctrl+C detected. Exit viewer? [y/n]: ' > /dev/tty
    local ans=""
    IFS= read -r ans < /dev/tty || ans=""

    case "$ans" in
      y|Y)
        exit 130
        ;;
      n|N)
        printf '[viewer] continue\n' > /dev/tty
        resume_after_int=1
        return 0
        ;;
      *)
        printf '[viewer] please input y or n, then press Enter.\n' > /dev/tty
        ;;
    esac
  done
}

viewer_schema_for_service() {
  local service="${1:?service required}"
  case "$service" in
    processor)
      printf 'proc\n'
      ;;
    fetcher)
      printf 'fetch\n'
      ;;
    *)
      echo "unknown service for schema mapping: $service" >&2
      return 1
      ;;
  esac
}

run_single() {
  local service="${1:?service required}"
  local fifo="${2:?fifo required}"
  local bin schema sample

  bin="$(logview_bin_path)"
  schema="$(viewer_schema_for_service "$service")"
  sample="$(logview_sample_path "$schema")"

  [[ -x "$bin" ]] || {
    echo "[viewer] binary not found or not executable: $bin" >&2
    exit 1
  }

  printf '[viewer single] service=%s schema=%s fifo=%s bin=%s\n' \
    "$service" "$schema" "$fifo" "$bin"

  trap 'confirm_exit' INT

  while true; do
    resume_after_int=0

    "$bin" --fifo "$fifo" --schema "$schema" --sample "$sample" &
    viewer_pid=$!

    set +e
    wait "$viewer_pid"
    rc=$?
    set -e

    # Ctrl+C 后用户选择 n，则 viewer 已退出，需要重启
    if [[ "$resume_after_int" -eq 1 ]]; then
      continue
    fi

    # 正常退出，短暂等待后重连
    if [[ "$rc" -eq 0 ]]; then
      sleep 0.1
      continue
    fi

    # 被信号打死（比如 Ctrl+C）
    if [[ "$rc" -eq 130 || "$rc" -eq 131 || "$rc" -eq 143 ]]; then
      exit "$rc"
    fi

    printf '[viewer] process exited rc=%s, retrying...\n' "$rc" >&2
    sleep 0.2
  done
}

run_merge() {
  local fetcher_fifo="${1:?fetcher_fifo required}"
  local processor_fifo="${2:?processor_fifo required}"

  printf '[viewer merge] fetcher_fifo=%s processor_fifo=%s\n' "$fetcher_fifo" "$processor_fifo"

  trap 'confirm_exit' INT

  while true; do
    resume_after_int=0

    sleep 3600 &
    viewer_pid=$!

    set +e
    wait "$viewer_pid"
    rc=$?
    set -e

    if [[ "$resume_after_int" -eq 1 ]]; then
      continue
    fi

    if [[ "$rc" -eq 130 || "$rc" -eq 131 || "$rc" -eq 143 ]]; then
      exit "$rc"
    fi
  done
}

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
    run_single "$service" "$fifo"
    ;;
  merge)
    run_merge "$fetcher_fifo" "$processor_fifo"
    ;;
  *)
    echo "invalid or missing --mode" >&2
    exit 1
    ;;
esac