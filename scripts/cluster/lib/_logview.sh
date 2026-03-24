#!/usr/bin/env bash
set -euo pipefail

logview_mode_root() {
  echo "./logs/cluster/logview"
}

logview_tmp_root() {
  echo "./tmp/logview"
}

logview_pid_file() {
  echo "$(logview_mode_root)/dispatcher.pid"
}

logview_session_name() {
  echo "logview-cluster"
}

logview_fifo_path() {
  local name="${1:?fifo name required}"
  echo "$(logview_tmp_root)/$name.fifo"
}

ensure_logview_dirs() {
  mkdir -p "$(logview_mode_root)"
  mkdir -p "$(logview_tmp_root)"
}

create_logview_fifos() {
  ensure_logview_dirs
  local names=(
    fetcher.flow
    fetcher.core
    processor.flow
    processor.core
    processor.wins
    writer.flow
  )
  local n p
  for n in "${names[@]}"; do
    p="$(logview_fifo_path "$n")"
    [[ -p "$p" ]] || mkfifo "$p"
  done
}

remove_logview_fifos() {
  local root
  root="$(logview_tmp_root)"
  [[ -d "$root" ]] || return 0
  find "$root" -maxdepth 1 -type p -name '*.fifo' -delete || true
}

logview_dispatch_log() {
  echo "$(logview_mode_root)/dispatch.latest.log"
}