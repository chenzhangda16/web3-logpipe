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

cluster_ctl_ts() { date '+%F %T'; }

cluster_ctl_log() {
  local src="${BASH_SOURCE[1]##*/}"
  local line="${BASH_LINENO[0]}"
  local func="${FUNCNAME[1]:-main}"
  echo "[$(cluster_ctl_ts)] [$src:$line][$func] $*"
}

cluster_ctl_die() {
  local src="${BASH_SOURCE[1]##*/}"
  local line="${BASH_LINENO[0]}"
  local func="${FUNCNAME[1]:-main}"
  echo "[$(cluster_ctl_ts)] ERROR [$src:$line][$func] $*" >&2
  return 1
}

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

node_is_local() {
  local node="$1"
  [[ "${node}" == "$(controller_node)" ]]
}

run_local_bash() {
  local cmd="$1"
  bash -lc "$cmd"
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

log_dir_of_node() {
  local node="$1"
  local var="LOG_DIR_${node^^}"
  local val
  val="$(cluster_ctl_getvar "$var")"
  if [[ -n "$val" ]]; then
    printf '%s' "$val"
  else
    printf '%s/logs/cluster' "$(root_of_node "$node")"
  fi
}

host_of_service() {
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
  ssh "$node" "$@"
}

ssh_bash() {
  local node="$1"
  local cmd="$2"

  if node_is_local "$node"; then
    run_local_bash "$cmd"
    return
  fi

  local host
  ssh "$node" "bash -lc $(printf '%q' "$cmd")" < /dev/null
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

  root="$(root_of_node "$node")" || return 1

  ensure_remote_root "$node" || return 1

  cluster_ctl_log "sync repo: node=$node root=$root"

  if ! rsync -az --delete \
    --exclude '.git' \
    --exclude 'bin' \
    --exclude 'data' \
    --exclude 'logs' \
    --exclude 'tmp' \
    --exclude '.idea' \
    --exclude '.vscode' \
    --exclude 'node_modules' \
    --exclude 'vendor' \
    "$ROOT_DIR/" "${node}:${root%/}/" < /dev/null
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
  node="$(host_of_service pg)" || {
    cluster_ctl_die "failed to resolve host of service: pg"
    return 1
  }

  run_remote_script_rel "$node" "scripts/cluster/ensure_pg.sh" || {
    cluster_ctl_die "failed to ensure pg on node=$node"
    return 1
  }
}

cluster_ensure_kafka() {
  local node
  node="$(host_of_service kafka)" || {
    cluster_ctl_die "failed to resolve host of service: kafka"
    return 1
  }

  run_remote_script_rel "$node" "scripts/cluster/ensure_kafka.sh" || {
    cluster_ctl_die "failed to ensure kafka on node=$node"
    return 1
  }
}

go_env_of_arch() {
  local arch="$1"

  case "$arch" in
    linux_amd64)
      GOOS_OUT=linux
      GOARCH_OUT=amd64
      ;;
    linux_arm64)
      GOOS_OUT=linux
      GOARCH_OUT=arm64
      ;;
    android_arm64)
      GOOS_OUT=android
      GOARCH_OUT=arm64
      ;;
    *)
      cluster_ctl_die "unsupported service arch: arch=$arch"
      return 1
      ;;
  esac
}

deploy_file_to_node() {
  local local_file="$1"
  local node="$2"
  local remote_file="$3"

  [[ -f "$local_file" ]] || {
    cluster_ctl_die "local file not found: local_file=$local_file node=$node remote_file=$remote_file"
    return 1
  }

  cluster_ctl_log "deploy file: $local_file -> $node:$remote_file"

  if node_is_local "$node"; then
    if [[ "$local_file" == "$remote_file" ]]; then
      cluster_ctl_log "skip local deploy for same file: node=$node file=$local_file"
      return 0
    fi

    mkdir -p "$(dirname "$remote_file")" || {
      cluster_ctl_die "failed to mkdir local remote dir: node=$node remote_file=$remote_file"
      return 1
    }
    cp -f "$local_file" "$remote_file" || {
      cluster_ctl_die "failed to copy local file: local_file=$local_file remote_file=$remote_file"
      return 1
    }
    return 0
  fi

  local remote_dir
  remote_dir="$(dirname "$remote_file")"

  ssh_bash "$node" "mkdir -p $(printf '%q' "$remote_dir")" || {
    cluster_ctl_die "failed to ensure remote dir: node=$node remote_dir=$remote_dir"
    return 1
  }
  rsync -az "$local_file" "$node:$remote_file" || {
    cluster_ctl_die "failed to rsync file: local_file=$local_file node=$node remote_file=$remote_file"
    return 1
  }
}

ensure_local_service_binaries() {
  local svc svc_lc arch local_bin_dir out goos goarch

  : "${ROOT_DIR:?ROOT_DIR not set}"
  : "${COMPILE_SERVICE_SET:?COMPILE_SERVICE_SET not set}"

  if [[ "${NO_BUILD:-false}" == "true" ]]; then
    cluster_ctl_log "NO_BUILD=true; skip local service binary build"
    return 0
  fi

  for svc in "${COMPILE_SERVICE_SET[@]}"; do
    svc_lc="${svc,,}"

    arch="$(cluster_ctl_getvar "${svc}_ARCH")"
    [[ -n "$arch" ]] || {
      cluster_ctl_die "missing env var: ${svc}_ARCH svc=$svc"
      return 1
    }

    local_bin_dir="$(cluster_ctl_getvar "${svc}_LOCAL_BIN_DIR")"
    [[ -n "$local_bin_dir" ]] || {
      cluster_ctl_die "missing env var: ${svc}_LOCAL_BIN_DIR svc=$svc"
      return 1
    }

    out="$local_bin_dir/$svc_lc"

    mkdir -p "$local_bin_dir" || {
      cluster_ctl_die "failed to ensure local bin dir: svc=$svc dir=$local_bin_dir"
      return 1
    }

    go_env_of_arch "$arch" || {
      cluster_ctl_die "failed to resolve go env: svc=$svc arch=$arch"
      return 1
    }
    goos="$GOOS_OUT"
    goarch="$GOARCH_OUT"

    cluster_ctl_log "build service: svc=$svc arch=$arch goos=$goos goarch=$goarch out=$out"

    GOOS="$goos" GOARCH="$goarch" \
      go build -o "$out" "./cmd/$svc_lc" || {
        cluster_ctl_die "failed to build service: svc=$svc arch=$arch out=$out"
        return 1
      }

    chmod +x "$out" || {
      cluster_ctl_die "failed to chmod built binary: svc=$svc out=$out"
      return 1
    }
  done

  cluster_ctl_log "all local service binaries ensured"
}

ensure_cluster_service_binaries() {
  local svc svc_lc node local_bin_dir cluster_bin_dir local_file remote_file

  : "${COMPILE_SERVICE_SET:?COMPILE_SERVICE_SET not set}"

  for svc in "${COMPILE_SERVICE_SET[@]}"; do
    svc_lc="${svc,,}"

    node="$(cluster_ctl_getvar "${svc}_NODE")"
    [[ -n "$node" ]] || {
      cluster_ctl_die "missing env var: ${svc}_NODE svc=$svc"
      return 1
    }

    local_bin_dir="$(cluster_ctl_getvar "${svc}_LOCAL_BIN_DIR")"
    [[ -n "$local_bin_dir" ]] || {
      cluster_ctl_die "missing env var: ${svc}_LOCAL_BIN_DIR svc=$svc"
      return 1
    }

    cluster_bin_dir="$(cluster_ctl_getvar "${svc}_CLUSTER_BIN_DIR")"
    [[ -n "$cluster_bin_dir" ]] || {
      cluster_ctl_die "missing env var: ${svc}_CLUSTER_BIN_DIR svc=$svc"
      return 1
    }

    local_file="$local_bin_dir/$svc_lc"
    remote_file="$cluster_bin_dir/$svc_lc"

    [[ -f "$local_file" ]] || {
      cluster_ctl_die "local artifact not found: svc=$svc local_file=$local_file"
      return 1
    }

    cluster_ctl_log "deploy service: svc=$svc node=$node local=$local_file remote=$remote_file"

    deploy_file_to_node "$local_file" "$node" "$remote_file" || {
      cluster_ctl_die "failed to deploy service binary: svc=$svc node=$node local=$local_file remote=$remote_file"
      return 1
    }
    ssh_bash "$node" "chmod +x $(printf '%q' "$remote_file")" || {
      cluster_ctl_die "failed to chmod remote binary: svc=$svc node=$node remote=$remote_file"
      return 1
    }
  done

  cluster_ctl_log "all cluster service binaries ensured"
}

ensure_service_binaries() {
  ensure_local_service_binaries || {
    cluster_ctl_die "failed to ensure local service binaries"
    return 1
  }
  ensure_cluster_service_binaries || {
    cluster_ctl_die "failed to ensure cluster service binaries"
    return 1
  }
}

cluster_ensure_infra() {
  local force="${1:-}"

  cluster_sync_ensure "$force" || {
    cluster_ctl_die "failed to ensure cluster sync: force=$force"
    return 1
  }

  if [[ "$force" == "force" ]]; then
    cluster_ctl_log "force mode: ensure service binaries now"
    ensure_service_binaries || {
      cluster_ctl_die "failed to ensure service binaries in force mode"
      return 1
    }
  else
    cluster_ctl_log "non-force mode: skip ensure service binaries"
  fi

  # 先 pg 后 kafka，保持现有时序
  cluster_ensure_pg || {
    cluster_ctl_die "failed to ensure pg infra"
    return 1
  }
  cluster_ensure_kafka || {
    cluster_ctl_die "failed to ensure kafka infra"
    return 1
  }

  cluster_ctl_log "cluster infra ensured"
}

# ------------------------------------------------------------------------------
# service kill
# ------------------------------------------------------------------------------

kill_remote_service() {
  local svc="$1"
  local svc_lc node cluster_bin_dir remote_bin

  [[ -n "$svc" ]] || {
    cluster_ctl_die "service name is empty"
    return 1
  }

  svc_lc="${svc,,}"

  node="$(cluster_ctl_getvar "${svc}_NODE")"
  [[ -n "$node" ]] || {
    cluster_ctl_die "missing env var: ${svc}_NODE svc=$svc"
    return 1
  }

  cluster_bin_dir="$(cluster_ctl_getvar "${svc}_CLUSTER_BIN_DIR")"
  [[ -n "$cluster_bin_dir" ]] || {
    cluster_ctl_die "missing env var: ${svc}_CLUSTER_BIN_DIR svc=$svc"
    return 1
  }

  remote_bin="$cluster_bin_dir/$svc_lc"

  if remote_project_exists "$node"; then
    cluster_ctl_log "kill service: svc=$svc node=$node remote_bin=$remote_bin"
    run_remote_primitive_rel "$node" "scripts/cluster/kill_service.sh" "$svc" "$remote_bin" || {
      cluster_ctl_die "failed to kill service: svc=$svc node=$node remote_bin=$remote_bin"
      return 1
    }
  else
    cluster_ctl_log "skip kill service: svc=$svc node=$node project root missing"
  fi
}

kill_all_remote_services() {
  local svc

  : "${COMPILE_SERVICE_SET:?COMPILE_SERVICE_SET not set}"

  for svc in "${COMPILE_SERVICE_SET[@]}"; do
    kill_remote_service "$svc" || return 1
  done
}