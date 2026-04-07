#!/usr/bin/env bash
set -euo pipefail

SERVICES=(fetcher processor)

get_root_dir() {
  cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd
}

logview_bin_path() {
  printf '%s/bin/cluster/logview' "$(get_root_dir)"
}

logview_log_key() {
  local service="${1:-}"
  if [[ -n "$service" ]]; then
    printf 'logview.%s' "$service"
  else
    printf 'logview'
  fi
}

logview_latest_log() {
  local service="${1:-}"
  printf '%s/%s.latest.log' "$(logview_log_root)" "$(logview_log_key "$service")"
}

logview_latest_stamp() {
  local service="${1:-}"
  printf '%s/%s.latest.stamp' "$(logview_log_root)" "$(logview_log_key "$service")"
}

logview_history_log() {
  local stamp="${1:?stamp required}"
  local service="${2:-}"
  printf '%s/%s.%s.log' "$(logview_log_root)" "$(logview_log_key "$service")" "$stamp"
}

prepare_logview_logs() {
  local service="${1:-}"
  local latest_log latest_stamp old_stamp hist_log new_stamp
  latest_log="$(logview_latest_log "$service")"
  latest_stamp="$(logview_latest_stamp "$service")"

  ensure_logview_dirs

  if [[ -s "$latest_log" && -f "$latest_stamp" ]]; then
    old_stamp="$(cat "$latest_stamp" 2>/dev/null || true)"
    if [[ -n "$old_stamp" ]]; then
      hist_log="$(logview_history_log "$old_stamp" "$service")"
      cp "$latest_log" "$hist_log"
    fi
  fi

  new_stamp="$(date '+%Y%m%d_%H%M%S')"
  printf '%s\n' "$new_stamp" > "$latest_stamp"
  : > "$latest_log"
}

build_logview_binary() {
  local root bin log_file
  root="$(get_root_dir)"
  bin="$(logview_bin_path)"
  log_file="$(logview_latest_log)"

  prepare_logview_logs ""
  mkdir -p "$(dirname "$bin")"

  printf '[logview] building %s -> %s\n' "$root/cmd/logview" "$bin" >>"$log_file"

  (
    cd "$root"
    go build -o "$bin" ./cmd/logview
  ) >>"$log_file" 2>&1
}

ensure_logview_binary() {
  local bin
  bin="$(logview_bin_path)"

  build_logview_binary

  [[ -x "$bin" ]] || {
    echo "[logview] build finished but binary not executable: $bin" >&2
    return 1
  }
}

logview_mode_root() {
  printf '%s/data/cluster/pids' "$(get_root_dir)"
}

logview_tmp_root() {
  printf '%s/tmp/logview' "$(get_root_dir)"
}

logview_log_root() {
  printf '%s/logs/cluster' "$(get_root_dir)"
}

logview_sample_root() {
  printf '%s/config/logview' "$(get_root_dir)"
}

logview_sample_path() {
  local schema="${1:?schema required}"
  printf '%s/%s.sample.json' "$(logview_sample_root)" "$schema"
}

logview_dispatch_pid_file() {
  printf '%s/dispatcher.pid' "$(logview_mode_root)"
}

logview_dispatch_script_path() {
  printf '%s/scripts/cluster/logview_dispatch.sh' "$(get_root_dir)"
}

logview_viewer_script_path() {
  printf '%s/scripts/cluster/logview_viewer.sh' "$(get_root_dir)"
}

logview_dispatch_latest_log() {
  printf '%s/dispatch.latest.log' "$(logview_log_root)"
}

logview_dispatch_latest_stamp() {
  printf '%s/dispatch.latest.stamp' "$(logview_log_root)"
}

logview_dispatch_history_log() {
  local stamp="${1:?stamp required}"
  printf '%s/dispatch.%s.log' "$(logview_log_root)" "$stamp"
}

logview_fifo_path() {
  local name="${1:?fifo name required}"
  printf '%s/%s.fifo' "$(logview_tmp_root)" "$name"
}

ensure_logview_dirs() {
  mkdir -p "$(logview_mode_root)"
  mkdir -p "$(logview_tmp_root)"
  mkdir -p "$(logview_log_root)"
  mkdir -p "$(logview_sample_root)"
}

prepare_logview_dispatch_logs() {
  local latest_log latest_stamp old_stamp hist_log new_stamp
  latest_log="$(logview_dispatch_latest_log)"
  latest_stamp="$(logview_dispatch_latest_stamp)"

  ensure_logview_dirs

  if [[ -s "$latest_log" && -f "$latest_stamp" ]]; then
    old_stamp="$(cat "$latest_stamp" 2>/dev/null || true)"
    if [[ -n "$old_stamp" ]]; then
      hist_log="$(logview_dispatch_history_log "$old_stamp")"
      cp "$latest_log" "$hist_log"
    fi
  fi

  new_stamp="$(date '+%Y%m%d_%H%M%S')"
  printf '%s\n' "$new_stamp" > "$latest_stamp"
  : > "$latest_log"
}

create_logview_fifos() {
  ensure_logview_dirs

  local names=(
    fetcher.frame
    processor.frame
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