#!/usr/bin/env bash
set -euo pipefail

ts() { date '+%F %T'; }
log() { printf '[%s] [logtail_supervise] %s\n' "$(ts)" "$*"; }

die() {
  log "ERROR: $*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
usage:
  logtail_supervise.sh \
    --node <node> \
    --remote-latest <remote_latest_log_path> \
    --local-log <local_log_path> \
    --state-file <state_file_path> \
    --err-file <err_file_path> \
    [--retry-sec <seconds>]

example:
  bash scripts/cluster/logtail_supervise.sh \
    --node m2 \
    --remote-latest /path/to/logs/mockchain.latest.log \
    --local-log /path/to/local/mockchain.latest.log \
    --state-file /path/to/local/mockchain.state \
    --err-file /path/to/local/mockchain.err.log
EOF
  exit 2
}

NODE=""
REMOTE_LATEST=""
LOCAL_LOG=""
STATE_FILE=""
ERR_FILE=""
RETRY_SEC=2

while (($# > 0)); do
  case "$1" in
    --node)
      NODE="${2:-}"
      shift 2
      ;;
    --remote-latest)
      REMOTE_LATEST="${2:-}"
      shift 2
      ;;
    --local-log)
      LOCAL_LOG="${2:-}"
      shift 2
      ;;
    --state-file)
      STATE_FILE="${2:-}"
      shift 2
      ;;
    --err-file)
      ERR_FILE="${2:-}"
      shift 2
      ;;
    --retry-sec)
      RETRY_SEC="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      die "unknown arg: $1"
      ;;
  esac
done

[[ -n "$NODE" ]] || usage
[[ -n "$REMOTE_LATEST" ]] || usage
[[ -n "$LOCAL_LOG" ]] || usage
[[ -n "$STATE_FILE" ]] || usage
[[ -n "$ERR_FILE" ]] || usage
[[ "$RETRY_SEC" =~ ^[0-9]+$ ]] || die "retry-sec must be integer: $RETRY_SEC"

mkdir -p "$(dirname "$LOCAL_LOG")"
mkdir -p "$(dirname "$STATE_FILE")"
mkdir -p "$(dirname "$ERR_FILE")"

touch "$LOCAL_LOG" "$ERR_FILE" "$STATE_FILE"

write_state() {
  printf '[%s] %s\n' "$(ts)" "$*" >> "$STATE_FILE"
}

ssh_opts=(
  -o BatchMode=yes
  -o ServerAliveInterval=20
  -o ServerAliveCountMax=3
  -o TCPKeepAlive=yes
  -o LogLevel=ERROR
)

remote_tail_latest() {
  ssh "${ssh_opts[@]}" "$NODE" "tail -F \"$REMOTE_LATEST\""
}

write_state "supervisor start: node=$NODE remote_latest=$REMOTE_LATEST local_log=$LOCAL_LOG"

while true; do
  write_state "attach: node=$NODE remote_latest=$REMOTE_LATEST"

  if remote_tail_latest >>"$LOCAL_LOG" 2>>"$ERR_FILE"; then
    write_state "tail exited cleanly: remote_latest=$REMOTE_LATEST"
  else
    rc=$?
    write_state "tail exited: rc=$rc remote_latest=$REMOTE_LATEST"
  fi

  sleep "$RETRY_SEC"
done