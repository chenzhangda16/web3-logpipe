#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

SESSION="$(logview_session_name)"

pane_cmd() {
  local name="${1:?name required}"
  local fifo
  fifo="$(logview_fifo_path "$name")"
  printf "stdbuf -oL cat '%s'" "$fifo"
}

start_tmux() {
  create_logview_fifos

  tmux has-session -t "$SESSION" 2>/dev/null && return 0

  tmux new-session -d -s "$SESSION" "$(pane_cmd fetcher.flow)"
  tmux split-window -h -t "$SESSION:0" "$(pane_cmd fetcher.core)"
  tmux split-window -v -t "$SESSION:0.0" "$(pane_cmd processor.flow)"
  tmux split-window -v -t "$SESSION:0.1" "$(pane_cmd processor.core)"
  tmux split-window -v -t "$SESSION:0.2" "$(pane_cmd processor.wins)"
  tmux split-window -v -t "$SESSION:0.3" "$(pane_cmd writer.flow)"

  tmux select-layout -t "$SESSION:0" tiled
}

attach_tmux() {
  tmux attach-session -t "$SESSION"
}

stop_tmux() {
  tmux has-session -t "$SESSION" 2>/dev/null || return 0
  tmux kill-session -t "$SESSION"
}

case "${1:-}" in
  start) start_tmux ;;
  attach) attach_tmux ;;
  stop) stop_tmux ;;
  *)
    echo "usage: $0 {start|attach|stop}" >&2
    exit 1
    ;;
esac