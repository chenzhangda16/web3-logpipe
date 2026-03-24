#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

start_dispatcher() {
  ensure_logview_dirs

  local pid_file log_file
  pid_file="$(logview_pid_file)"
  log_file="$(logview_dispatch_log)"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    rm -f "$pid_file"
  fi

  nohup "./scripts/cluster/logview_dispatch.sh" \
    >>"$log_file" 2>&1 &
  echo "$!" > "$pid_file"
}

stop_dispatcher() {
  local pid_file pid
  pid_file="$(logview_pid_file)"
  [[ -f "$pid_file" ]] || return 0

  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
}

start_all() {
  ensure_logview_dirs
  create_logview_fifos
  "./scripts/cluster/logview_tmux.sh" start
  start_dispatcher
}

stop_all() {
  stop_dispatcher || true
  "./scripts/cluster/logview_tmux.sh" stop || true
  remove_logview_fifos || true
}

status_all() {
  local session pid_file pid
  session="$(logview_session_name)"
  pid_file="$(logview_pid_file)"

  echo "tmux_session=$session"
  if tmux has-session -t "$session" 2>/dev/null; then
    echo "tmux=up"
  else
    echo "tmux=down"
  fi

  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "dispatcher=up pid=$pid"
    else
      echo "dispatcher=stale"
    fi
  else
    echo "dispatcher=down"
  fi
}

case "${1:-}" in
  start) start_all ;;
  stop) stop_all ;;
  restart) stop_all; start_all ;;
  attach) "./scripts/cluster/logview_tmux.sh" attach ;;
  status) status_all ;;
  *)
    echo "usage: $0 {start|stop|restart|attach|status}" >&2
    exit 1
    ;;
esac
