#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf '%s\n' "error: do not execute this file directly." >&2
  printf '%s\n' "use: source ${BASH_SOURCE[0]}" >&2
  exit 1
fi

if [[ -n "${__WEB3_LOGPIPE_COMPONENT_STREAM_LIB_SOURCED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
__WEB3_LOGPIPE_COMPONENT_STREAM_LIB_SOURCED=1

cstream_ts()  { date '+%F %T'; }
cstream_log() { echo "[$(cstream_ts)] [component_stream] $*"; }
cstream_die() { echo "[$(cstream_ts)] [component_stream] ERROR: $*" >&2; return 1; }

cstream_prepare_latest_and_rotate_prev() {
  local component="$1"
  local new_stamp="$2"

  [[ -n "$component" ]] || {
    cstream_die "component is empty"
    return 1
  }
  [[ -n "$new_stamp" ]] || {
    cstream_die "new_stamp is empty"
    return 1
  }
  [[ -n "${LOG_DIR:-}" ]] || {
    cstream_die "LOG_DIR is empty or unset"
    return 1
  }

  mkdir -p "$LOG_DIR"

  local latest="$LOG_DIR/${component}.latest.log"
  local stamp_file="$LOG_DIR/${component}.latest.stamp"
  local prev_stamp=""
  local prev_hist=""

  if [[ -f "$stamp_file" ]]; then
    prev_stamp="$(tr -d '\r\n' < "$stamp_file" || true)"
  fi

  if [[ -n "$prev_stamp" && -f "$latest" && -s "$latest" ]]; then
    prev_hist="$LOG_DIR/${component}.${prev_stamp}.log"

    if [[ -e "$prev_hist" ]]; then
      cstream_die "previous hist already exists, refusing to overwrite: $prev_hist"
      return 1
    fi

    cp -- "$latest" "$prev_hist"
  fi

  : > "$latest"
  printf '%s\n' "$new_stamp" > "$stamp_file"
}

cluster_component_stream_run() {
  local component="$1"
  local stamp="$2"
  local ready_fifo="${3:-}"

  shift 3 || true
  [[ "${1:-}" == "--" ]] || {
    cstream_die "usage: cluster_component_stream_run <component> <stamp> <ready_fifo-or-empty> -- <cmd> [args...]"
    return 1
  }
  shift

  [[ -n "$component" ]] || {
    cstream_die "component is empty"
    return 1
  }
  [[ -n "$stamp" ]] || {
    cstream_die "stamp is empty"
    return 1
  }
  (($# > 0)) || {
    cstream_die "command is empty"
    return 1
  }
  [[ -n "${LOG_DIR:-}" ]] || {
    cstream_die "LOG_DIR is empty or unset"
    return 1
  }

  cstream_prepare_latest_and_rotate_prev "$component" "$stamp"

  local latest="$LOG_DIR/${component}.latest.log"
  local stamp_file="$LOG_DIR/${component}.latest.stamp"

  if [[ -n "$ready_fifo" ]]; then
    mkdir -p "$(dirname "$ready_fifo")"
    rm -f "$ready_fifo"
    mkfifo "$ready_fifo"
    cstream_log "component=$component latest=$latest stamp_file=$stamp_file ready_fifo=$ready_fifo"
  else
    cstream_log "component=$component latest=$latest stamp_file=$stamp_file"
  fi

  exec >> "$latest" 2>&1
  exec stdbuf -oL -eL "$@"
}