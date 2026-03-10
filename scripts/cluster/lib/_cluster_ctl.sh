#!/usr/bin/env bash

# ------------------------------------------------------------------------------
# scripts/cluster/lib/_cluster_ctl.sh
#
# cluster controller helpers
# - topology helpers
# - ssh wrappers
# - remote script execution
#
# expected:
# - caller already sourced scripts/lib/_bootstrap.sh and executed:
#     bootstrap cluster
# ------------------------------------------------------------------------------

CLUSTER_CTL_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

cctl_ts()  { date '+%F %T'; }
cctl_log() { echo "[$(cctl_ts)] [cluster-ctl] $*"; }
cctl_die() { echo "[$(cctl_ts)] [cluster-ctl] ERROR: $*" >&2; exit 1; }

cctl_have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

cctl_require_cmds() {
  local x
  for x in ssh; do
    cctl_have_cmd "$x" || cctl_die "required command not found: $x"
  done
}

cctl_getvar() {
  local name="$1"
  printf '%s' "${!name:-}"
}

controller_node() {
  printf '%s' main
}

deploy_nodes() {
  printf '%s\n' pc127 m2 pixel
}

reset_nodes() {
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
  val="$(cctl_getvar "$var")"
  [[ -n "$val" ]] || cctl_die "env var not found: $var"
  printf '%s' "$val"
}

node_of_service() {
  local svc="$1"
  local var="HOST_${svc^^}"
  local val
  val="$(cctl_getvar "$var")"
  [[ -n "$val" ]] || cctl_die "service mapping not found: $var"
  printf '%s' "$val"
}

ssh_node() {
  local node="$1"
  shift
  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "$@"
}

ssh_bash() {
  local node="$1"
  local cmd="$2"
  local host
  host="$(host_alias_of_node "$node")"
  ssh "$host" "bash -lc $(printf '%q' "$cmd")"
}

remote_project_exists() {
  local node="$1"
  local root
  root="$(root_of_node "$node")"
  ssh_bash "$node" "[[ -d $(printf '%q' "$root") ]]"
}

run_remote_script_rel() {
  local node="$1"
  local script_rel="$2"
  local root
  root="$(root_of_node "$node")"

  cctl_log "run remote script: node=$node script=$script_rel"
  ssh_bash "$node" "cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
}