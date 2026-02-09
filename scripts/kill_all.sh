#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="$ROOT_DIR/data/logs"
PID_DIR="$ROOT_DIR/data/pids"

say() { echo "[kill_all] $*"; }

# Graceful then force kill for a list of PIDs
kill_pids() {
  local label="$1"; shift
  local pids=("$@")
  local alive=()

  # filter alive
  for p in "${pids[@]}"; do
    [[ -n "$p" ]] || continue
    kill -0 "$p" >/dev/null 2>&1 && alive+=("$p") || true
  done

  [[ ${#alive[@]} -eq 0 ]] && return 0

  say "TERM $label: ${alive[*]}"
  kill "${alive[@]}" >/dev/null 2>&1 || true

  # wait a bit
  local deadline=$(( $(date +%s) + 2 ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local any=false
    for p in "${alive[@]}"; do
      if kill -0 "$p" >/dev/null 2>&1; then
        any=true; break
      fi
    done
    $any || return 0
    sleep 0.1
  done

  # force
  local still=()
  for p in "${alive[@]}"; do
    kill -0 "$p" >/dev/null 2>&1 && still+=("$p") || true
  done
  [[ ${#still[@]} -eq 0 ]] && return 0
  say "KILL $label: ${still[*]}"
  kill -KILL "${still[@]}" >/dev/null 2>&1 || true
}

pids_by_pattern() {
  # print pids (space-separated) matching a regex over full cmdline
  local pat="$1"
  pgrep -f -- "$pat" 2>/dev/null | tr '\n' ' ' || true
}

say "repo=$ROOT_DIR"
say "killing web3-logpipe-related processes (tight scope)..."

# 0) Kill any running logpipe.sh itself (the orchestrator) under this repo
#    (handles the 'subshell stuck' case: start never reached write_pids)
lp_pids="$(pids_by_pattern "$ROOT_DIR/scripts/logpipe\.sh")"
# also catch "bash ./scripts/logpipe.sh start" launched from repo
lp2_pids="$(pids_by_pattern "bash .*${ROOT_DIR}/scripts/logpipe\.sh")"
kill_pids "logpipe.sh" $lp_pids $lp2_pids

# 1) Kill tee processes that are dual-writing logs under this repo
#    (matches: tee -a <latest> <hist> where latest/hist are in LOG_DIR)
tee_pids="$(pids_by_pattern "tee -a ${LOG_DIR}/.*\.latest\.log ${LOG_DIR}/.*\.log")"
kill_pids "tee log-dup" $tee_pids

# 2) Kill tail-based log tailers if you use them (e.g., tail -n 0 -F latest >> hist)
tail_pids="$(pids_by_pattern "tail .* -F ${LOG_DIR}/.*\.latest\.log")"
kill_pids "tail log-tailer" $tail_pids

# 3) Kill the 4 binaries under this repo path (avoid killing other projects' binaries)
mock_pids="$(pids_by_pattern "${ROOT_DIR}/bin/mockchain")"
fetch_pids="$(pids_by_pattern "${ROOT_DIR}/bin/fetcher")"
proc_pids="$(pids_by_pattern "${ROOT_DIR}/bin/processor")"
writ_pids="$(pids_by_pattern "${ROOT_DIR}/bin/writer")"

kill_pids "mockchain" $mock_pids
kill_pids "fetcher"   $fetch_pids
kill_pids "processor" $proc_pids
kill_pids "writer"    $writ_pids

# 4) Cleanup pidfiles (best-effort; safe even if absent)
rm -f "$PID_DIR/logpipe.pids" "$PID_DIR/kafka.pid" 2>/dev/null || true

say "done."
