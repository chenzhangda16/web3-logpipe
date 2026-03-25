#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)/cluster/lib/_logview.sh"

readonly LOGVIEW_FETCHER_SESSION="logview-fetcher"
readonly LOGVIEW_PROCESSOR_SESSION="logview-processor"
readonly LOGVIEW_MERGE_SESSION="logview-merge"

readonly LOGVIEW_WINDOW="viewer"

usage() {
  cat >&2 <<'EOF'
usage:
  ./scripts/cluster/logview.sh start --mode split
  ./scripts/cluster/logview.sh start --mode merge
  ./scripts/cluster/logview.sh stop
  ./scripts/cluster/logview.sh status
  ./scripts/cluster/logview.sh attach --target fetcher
  ./scripts/cluster/logview.sh attach --target processor
  ./scripts/cluster/logview.sh attach --target merge
EOF
  exit 1
}

logview_run_log() {
  echo "$(logview_mode_root)/logview.latest.log"
}

tmux_has_session() {
  local session="${1:?session required}"
  tmux has-session -t "$session" 2>/dev/null
}

tmux_kill_session_if_exists() {
  local session="${1:?session required}"
  tmux_has_session "$session" || return 0
  tmux kill-session -t "$session"
}

viewer_script_path() {
  echo "./scripts/cluster/logview_viewer.sh"
}

viewer_cmd_single() {
  local service="${1:?service required}"
  local fifo
  fifo="$(logview_fifo_path "$service.frame")"

  printf "%q --mode single --service %q --fifo %q" \
    "$(viewer_script_path)" \
    "$service" \
    "$fifo"
}

viewer_cmd_merge() {
  local fetcher_fifo processor_fifo
  fetcher_fifo="$(logview_fifo_path "fetcher.frame")"
  processor_fifo="$(logview_fifo_path "processor.frame")"

  printf "%q --mode merge --fetcher-fifo %q --processor-fifo %q" \
    "$(viewer_script_path)" \
    "$fetcher_fifo" \
    "$processor_fifo"
}

ensure_tmux_session_single() {
  local session="${1:?session required}"
  local service="${2:?service required}"

  if tmux_has_session "$session"; then
    return 0
  fi

  tmux new-session -d -s "$session" -n "$LOGVIEW_WINDOW" "$(viewer_cmd_single "$service")"
}

ensure_tmux_session_merge() {
  local session="${1:?session required}"

  if tmux_has_session "$session"; then
    return 0
  fi

  tmux new-session -d -s "$session" -n "$LOGVIEW_WINDOW" "$(viewer_cmd_merge)"
}

start_split() {
  ensure_logview_dirs
  create_logview_fifos

  ensure_tmux_session_single "$LOGVIEW_FETCHER_SESSION" "fetcher"
  ensure_tmux_session_single "$LOGVIEW_PROCESSOR_SESSION" "processor"
}

start_merge() {
  ensure_logview_dirs
  create_logview_fifos

  ensure_tmux_session_merge "$LOGVIEW_MERGE_SESSION"
}

stop_all() {
  tmux_kill_session_if_exists "$LOGVIEW_FETCHER_SESSION" || true
  tmux_kill_session_if_exists "$LOGVIEW_PROCESSOR_SESSION" || true
  tmux_kill_session_if_exists "$LOGVIEW_MERGE_SESSION" || true
}

status_one() {
  local session="${1:?session required}"
  if tmux_has_session "$session"; then
    echo "up   $session"
  else
    echo "down $session"
  fi
}

status_all() {
  status_one "$LOGVIEW_FETCHER_SESSION"
  status_one "$LOGVIEW_PROCESSOR_SESSION"
  status_one "$LOGVIEW_MERGE_SESSION"
}

attach_target() {
  local target="${1:?target required}"

  case "$target" in
    fetcher)
      exec tmux attach-session -t "$LOGVIEW_FETCHER_SESSION"
      ;;
    processor)
      exec tmux attach-session -t "$LOGVIEW_PROCESSOR_SESSION"
      ;;
    merge)
      exec tmux attach-session -t "$LOGVIEW_MERGE_SESSION"
      ;;
    *)
      echo "unknown attach target: $target" >&2
      return 1
      ;;
  esac
}

parse_start_mode() {
  local mode=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --mode)
        shift
        [[ $# -gt 0 ]] || {
          echo "missing value for --mode" >&2
          return 1
        }
        mode="$1"
        ;;
      *)
        echo "unknown start arg: $1" >&2
        return 1
        ;;
    esac
    shift
  done

  [[ -n "$mode" ]] || {
    echo "start requires --mode {split|merge}" >&2
    return 1
  }

  case "$mode" in
    split|merge)
      printf '%s\n' "$mode"
      ;;
    *)
      echo "invalid mode: $mode" >&2
      return 1
      ;;
  esac
}

parse_attach_target() {
  local target=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --target)
        shift
        [[ $# -gt 0 ]] || {
          echo "missing value for --target" >&2
          return 1
        }
        target="$1"
        ;;
      *)
        echo "unknown attach arg: $1" >&2
        return 1
        ;;
    esac
    shift
  done

  [[ -n "$target" ]] || {
    echo "attach requires --target {fetcher|processor|merge}" >&2
    return 1
  }

  case "$target" in
    fetcher|processor|merge)
      printf '%s\n' "$target"
      ;;
    *)
      echo "invalid attach target: $target" >&2
      return 1
      ;;
  esac
}

main() {
  [[ $# -gt 0 ]] || usage

  local cmd="$1"
  shift || true

  case "$cmd" in
    start)
      local mode
      mode="$(parse_start_mode "$@")"

      case "$mode" in
        split)
          start_split
          ;;
        merge)
          start_merge
          ;;
      esac
      ;;
    stop)
      stop_all
      ;;
    status)
      status_all
      ;;
    attach)
      local target
      target="$(parse_attach_target "$@")"
      attach_target "$target"
      ;;
    *)
      usage
      ;;
  esac
}

main "$@"