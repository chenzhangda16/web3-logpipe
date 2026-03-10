#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/factory_reset.sh
#
# cluster-aware brutal reset:
# - kill app processes on all nodes
# - stop kafka on kafka node
# - for FULL_RESET=1/2 also stop pg on pg node
# - FULL_RESET=0:
#     reset kafka project storage + drop PG business DB
# - FULL_RESET=1/2:
#     wipe each node's $ROOT_DIR/data
# - finally re-bootstrap pg + kafka
#
# FULL_RESET modes:
#   0 / unset : no full data wipe
#   1         : wipe each node's $ROOT_DIR/data except mockchain.db
#   2         : wipe each node's $ROOT_DIR/data entirely
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

source "$ROOT_DIR/scripts/cluster/lib/_cluster_ctl.sh"

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [factory_reset] $*"; }
die() { echo "[$(ts)] [factory_reset] ERROR: $*" >&2; exit 1; }

run_remote_cluster_primitive() {
  local node="$1"
  local script_rel="$2"
  shift 2 || true

  local root cmd
  root="$(root_of_node "$node")"

  cmd="cd $(printf '%q' "$root") && bash $(printf '%q' "$script_rel")"
  if (($# > 0)); then
    local arg
    for arg in "$@"; do
      cmd+=" $(printf '%q' "$arg")"
    done
  fi

  log "run remote primitive: node=$node script=$script_rel args=($*)"
  ssh_bash "$node" "$cmd"
}

main() {
  cctl_require_cmds

  local kafka_node pg_node full_reset node
  kafka_node="$(node_of_service kafka)"
  pg_node="$(node_of_service pg)"
  full_reset="${FULL_RESET:-0}"

  log "cluster factory reset begin"
  log "kafka node=$kafka_node pg node=$pg_node FULL_RESET=$full_reset"

  log "phase 1: kill app processes on all nodes"
  while read -r node; do
    [[ -z "$node" ]] && continue
    if remote_project_exists "$node"; then
      run_remote_cluster_primitive "$node" "scripts/cluster/kill_apps.sh"
    else
      log "skip node=$node, project root missing"
    fi
  done < <(reset_nodes)

  log "phase 2: stop infra"
  if remote_project_exists "$kafka_node"; then
    run_remote_cluster_primitive "$kafka_node" "scripts/cluster/stop_kafka.sh"
  else
    log "skip kafka stop: node=$kafka_node project root missing"
  fi

  case "$full_reset" in
    1|2)
      if remote_project_exists "$pg_node"; then
        run_remote_cluster_primitive "$pg_node" "scripts/cluster/stop_pg.sh"
      else
        log "skip pg stop: node=$pg_node project root missing"
      fi
      ;;
    *)
      :
      ;;
  esac

  log "phase 3: reset data"
  case "$full_reset" in
    2|1)
      while read -r node; do
        [[ -z "$node" ]] && continue
        if remote_project_exists "$node"; then
          run_remote_cluster_primitive "$node" "scripts/cluster/reset_node_data.sh" "$full_reset"
        else
          log "skip node=$node, project root missing"
        fi
      done < <(reset_nodes)
      ;;
    0|"")
      if remote_project_exists "$kafka_node"; then
        run_remote_cluster_primitive "$kafka_node" "scripts/cluster/reset_kafka_storage.sh"
      else
        log "skip kafka storage reset: node=$kafka_node project root missing"
      fi

      if remote_project_exists "$pg_node"; then
        run_remote_cluster_primitive "$pg_node" "scripts/cluster/drop_pg_db.sh"
      else
        log "skip pg db drop: node=$pg_node project root missing"
      fi
      ;;
    *)
      die "FULL_RESET must be one of: 0|1|2"
      ;;
  esac

  log "phase 4: re-bootstrap infra"
  if remote_project_exists "$pg_node"; then
    run_remote_script_rel "$pg_node" "scripts/cluster/ensure_pg.sh"
  else
    log "skip ensure_pg: node=$pg_node project root missing"
  fi

  if remote_project_exists "$kafka_node"; then
    run_remote_script_rel "$kafka_node" "scripts/cluster/ensure_kafka.sh"
  else
    log "skip ensure_kafka: node=$kafka_node project root missing"
  fi

  log "Done."
}

main "$@"

# examples:
#   bash scripts/cluster/factory_reset.sh
#   FULL_RESET=1 bash scripts/cluster/factory_reset.sh
#   FULL_RESET=2 bash scripts/cluster/factory_reset.sh