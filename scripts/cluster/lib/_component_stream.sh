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

  mkdir -p "$LOG_DIR"

  local hist="$LOG_DIR/${component}.${stamp}.log"

  if [[ -n "$ready_fifo" ]]; then
    mkdir -p "$(dirname "$ready_fifo")"
    rm -f "$ready_fifo"
    mkfifo "$ready_fifo"
    cstream_log "component=$component hist=$hist ready_fifo=$ready_fifo"
  else
    cstream_log "component=$component hist=$hist"
  fi

  stdbuf -oL -eL "$@" 2>&1 | tee -a "$hist" >/dev/null
}