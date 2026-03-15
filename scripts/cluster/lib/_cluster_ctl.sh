#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  printf '%s\n' "error: do not execute this file directly." >&2
  printf '%s\n' "use: source ${BASH_SOURCE[0]}" >&2
  exit 1
fi

# Guard: source library body only once per shell/process.
if [[ -n "${__WEB3_LOGPIPE_CLUSTER_CTL_LIB_SOURCED:-}" ]]; then
  return 0 2>/dev/null || exit 0
fi
__WEB3_LOGPIPE_CLUSTER_CTL_LIB_SOURCED=1

cluster_ctl_ts()  { date '+%F %T'; }
cluster_ctl_log() { echo "[$(cluster_ctl_ts)] [cluster_ctl] $*"; }
cluster_ctl_die() { echo "[$(cluster_ctl_ts)] [cluster_ctl] ERROR: $*" >&2; return 1; }

cluster_ctl_have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

cluster_ctl_require_cmds() {
  local x
  for x in ssh rsync; do
    cluster_ctl_have_cmd "$x" || {
      cluster_ctl_die "required command not found: $x"
      return 1
    }
  done
}

cluster_ctl_getvar() {
  local name="$1"
  printf '%s' "${!name:-}"
}

# ----------------------------------------------------------------------
# topology helpers
# ----------------------------------------------------------------------

controller_node() {
  printf '%s' main
}

deploy_nodes() {
  # 暂时保留硬编码；后续再收敛到 cluster.env
  printf '%s\n' pc127 m2 pixel
}

all_cluster_nodes() {
  printf '%s\n' "$(controller_node)"
  deploy_nodes
}

host_alias_of_node() {
  local node="$1"
  printf '%s' "$node"
}

root_of_node() {
  local node="$1"
  local var="ROOT_${node^^}"
  local val
  val="$(cluster_ctl_getvar "$var")"
  [[ -n "$val" ]] || {
    cluster_ctl_die "env var not found: $var"
    return 1
  }
  printf '%s' "$val"
}

bin_dir_of_node() {
  local node="$1"
  local var="BIN_DIR_${node^^}"
  local val
  val="$(cluster_ctl_getvar "$var")"
  if [[ -n "$val" ]]; then
    printf '%s' "$val"
  else
    printf '%s/bin' "$(root_of_node "$node")"
  fi
}

log_dir_of_node() {
  local node="$1"
  local var="LOG_DIR_${node^^}"
  local val
  val="$(cluster_ctl_getvar "$var")"
  if [[ -n "$val" ]]; then
    printf '%s' "$val"
  else
    printf '%s/logs' "$(root_of_node "$node")"
  fi
}

node_of_service() {
  local svc="$1"
  local var="${svc^^}_HOST"
  local val
  val="$(cluster_ctl_getvar "$var")"
  [[ -n "$val" ]] || {
    cluster_ctl_die "service mapping not found: $var"
    return 1
  }
  printf '%s' "$val"
}

# ----------------------------------------------------------------------
# ssh / rsync wrappers
# ----------------------------------------------------------------------

ssh_node() {
  local node="$1"
  shift

  if node_is_local "$node"; then
    "$@"
    return
  fi

  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "$@"
}

ssh_bash() {
  local node="$1"
  local cmd="$2"

  if node_is_local "$node"; then
    run_local_bash "$cmd"
    return
  fi

  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "bash -lc $(printf '%q' "$cmd")" < /dev/null
}

remote_project_exists() {
  local node="$1"
  local root
  root="$(root_of_node "$node")" || return 1

  if node_is_local "$node"; then
    [[ -d "$root" ]]
    return
  fi

  ssh_bash "$node" "[[ -d $(printf '%q' "$root") ]]"
}

ensure_remote_root() {
  local node="$1"
  local root
  root="$(root_of_node "$node")" || return 1

  cluster_ctl_log "ensure root: node=$node root=$root"

  if node_is_local "$node"; then
    mkdir -p "$root"
    return 0
  fi

  ssh_bash "$node" "mkdir -p $(printf '%q' "$root")"
}

sync_node() {
  local node="$1"
  local host root rc

  host="$(host_alias_of_node "$node")" || return 1
  root="$(root_of_node "$node")" || return 1

  # 关键：把隐藏字符打出来
  printf '[%s] [sync_node] host=<%q>\n' "$(ts)" "$host" >&2
  printf '[%s] [sync_node] root=<%q>\n' "$(ts)" "$root" >&2

  ensure_remote_root "$node" || return 1

  cluster_ctl_log "sync repo: node=$node host=$host root=$root"

  if ! rsync -az --delete \
    --exclude '.git' \
    --exclude 'data' \
    --exclude 'logs' \
    --exclude 'tmp' \
    --exclude '.idea' \
    --exclude '.vscode' \
    --exclude 'node_modules' \
    --exclude 'vendor' \
    "$ROOT_DIR/" "${host}:${root%/}/" < /dev/null
  then
    rc=$?
    printf '[%s] [sync_node] rsync failed rc=%s host=<%q> root=<%q>\n' "$(ts)" "$rc" "$host" "$root" >&2
    return "$rc"
  fi
}

sync_all_nodes() {
  local node
  while read -r node; do
    [[ -z "$node" ]] && continue
    sync_node "$node"
  done < <(deploy_nodes)
}

run_remote_script_rel() {
  local node="$1"
  local script_rel="$2"
  local root

  root="$(root_of_node "$node")" || return 1

  cluster_ctl_log "run remote script: node=$node script=$script_rel"
  ssh_bash "$node" "cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
}

run_remote_script_rel() {
  local node="$1"
  local script_rel="$2"
  local root

  root="$(root_of_node "$node")" || return 1

  cluster_ctl_log "run script: node=$node script=$script_rel"

  if node_is_local "$node"; then
    cd "$root" && bash "$script_rel"
    return
  fi

  ssh_bash "$node" "cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
}

run_remote_primitive_rel() {
  local node="$1"
  local script_rel="$2"
  shift 2 || true

  local root cmd arg
  root="$(root_of_node "$node")" || return 1

  cmd="cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
  for arg in "$@"; do
    cmd+=" $(printf '%q' "$arg")"
  done

  cluster_ctl_log "run remote primitive: node=$node script=$script_rel args=($*)"
  ssh_bash "$node" "$cmd"
}

# ----------------------------------------------------------------------
# cluster sync gate
# ----------------------------------------------------------------------

cluster_sync_done_file() {
  printf '%s/.cluster_sync_all.done' "$DATA_DIR"
}

cluster_sync_mark_done() {
  local f
  f="$(cluster_sync_done_file)"
  mkdir -p "$(dirname "$f")"
  : > "$f"
}

cluster_sync_is_done_on_disk() {
  local f
  f="$(cluster_sync_done_file)"
  [[ -f "$f" ]]
}

cluster_sync_is_done_in_shell() {
  [[ -n "${__WEB3_LOGPIPE_CLUSTER_SYNC_DONE:-}" ]]
}

cluster_sync_mark_done_in_shell() {
  export __WEB3_LOGPIPE_CLUSTER_SYNC_DONE=1
}

cluster_sync_once() {
  if cluster_sync_is_done_in_shell; then
    cluster_ctl_log "cluster sync already done in this shell once; skip"
    return 0
  fi

  cluster_ctl_require_cmds || return 1
  sync_all_nodes || return 1

  cluster_sync_mark_done
  cluster_sync_mark_done_in_shell
  cluster_ctl_log "cluster sync done"
}

cluster_sync_ensure() {
  local force="${1:-}"

  if cluster_sync_is_done_in_shell; then
    cluster_ctl_log "ensure cluster sync already done in this shell; skip"
    return 0
  fi

  if [[ "$force" != "force" ]] && cluster_sync_is_done_on_disk; then
    cluster_sync_mark_done_in_shell
    cluster_ctl_log "cluster sync already done on disk; skip"
    return 0
  fi

  if [[ "$force" == "force" ]]; then
    cluster_ctl_log "cluster sync forced; syncing all nodes now"
  else
    cluster_ctl_log "cluster sync done-file missing; syncing all nodes now"
  fi

  cluster_sync_once
}

# ------------------------------------------------------------------------------
# infra ensure bundle
# ------------------------------------------------------------------------------

cluster_ensure_pg() {
  local node
  node="$(node_of_service pg)" || return 1
  run_remote_script_rel "$node" "scripts/cluster/ensure_pg.sh"
}

cluster_ensure_kafka() {
  local node
  node="$(node_of_service kafka)" || return 1
  run_remote_script_rel "$node" "scripts/cluster/ensure_kafka.sh"
}

cluster_ensure_infra() {
  local force="${1:-}"

  cluster_sync_ensure "$force" || return 1

  # 先 pg 后 kafka，保持现有时序
  cluster_ensure_pg || return 1
  cluster_ensure_kafka || return 1

  cluster_ctl_log "cluster infra ensured"
}

# ------------------------------------------------------------------------------
# infra ensure bundle
# ------------------------------------------------------------------------------

deploy_file_to_node() {
  local local_file="$1"
  local node="$2"
  local remote_file="$3"

  [[ -f "$local_file" ]] || {
    cluster_ctl_die "local file not found: $local_file"
    return 1
  }

  cluster_ctl_log "deploy file: $local_file -> $node:$remote_file"

  if node_is_local "$node"; then
    mkdir -p "$(dirname "$remote_file")" || return 1
    cp -f "$local_file" "$remote_file" || return 1
    return 0
  fi

  local host remote_dir
  host="$(host_alias_of_node "$node")"
  remote_dir="$(dirname "$remote_file")"

  ssh_bash "$node" "mkdir -p $(printf '%q' "$remote_dir")" || return 1
  rsync -az "$local_file" "$host:$remote_file" || return 1
}

writer_remote_platform() {
  # 可在 cluster.env 覆盖：
  #   WRITER_REMOTE_PLATFORM=linux_arm64
  #   WRITER_REMOTE_PLATFORM=android_arm64
  printf '%s' "${WRITER_REMOTE_PLATFORM:-android_arm64}"
}

writer_cross_artifact() {
  local platform
  platform="$(writer_remote_platform)"

  case "$platform" in
    android_arm64)
      printf '%s/bin/out/writer.android.arm64' "$ROOT_DIR"
      ;;
    linux_arm64)
      printf '%s/bin/out/writer.linux.arm64' "$ROOT_DIR"
      ;;
    *)
      cluster_ctl_die "unsupported WRITER_REMOTE_PLATFORM=$platform"
      return 1
      ;;
  esac
}

deploy_writer_binary() {
  local node artifact remote_bin
  node="$(node_of_service writer)" || return 1
  artifact="$(writer_cross_artifact)" || return 1
  remote_bin="$(bin_dir_of_node "$node")/writer"

  [[ -f "$artifact" ]] || {
    cluster_ctl_die "writer artifact not found: $artifact"
    return 1
  }

  deploy_file_to_node "$artifact" "$node" "$remote_bin" || return 1
  ssh_bash "$node" "chmod +x $(printf '%q' "$remote_bin")" || return 1

  cluster_ctl_log "writer binary deployed: node=$node artifact=$artifact remote=$remote_bin"
}

node_is_local() {
  local node="$1"
  [[ "$node" == "$(controller_node)" ]]
}

run_local_bash() {
  local cmd="$1"
  bash -lc "$cmd"
}