#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# scripts/cluster/factory_reset.sh
#
# cluster-aware brutal reset:
# - ensure cluster repo sync exists
# - kill app processes on all nodes
# - stop kafka on kafka node
# - for FULL_RESET=1/2 also stop pg on pg node
# - FULL_RESET=0:
#     reset kafka project storage + drop PG business DB
# - FULL_RESET=1/2:
#     wipe each node's $DATA_DIR
# - finally re-bootstrap infra as a bundle
#
# FULL_RESET modes:
#   0 / unset : no full data wipe
#   1         : wipe each node's $DATA_DIR except selected persistent paths
#   2         : wipe each node's $DATA_DIR entirely
# ------------------------------------------------------------------------------

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/lib/_bootstrap.sh"
bootstrap cluster

source "$ROOT_DIR/scripts/cluster/lib/_cluster_ctl.sh"
cluster_sync_ensure

ts()  { date '+%F %T'; }
log() { echo "[$(ts)] [factory_reset] $*"; }
die() { echo "[$(ts)] [factory_reset] ERROR: $*" >&2; exit 1; }

main() {
  cluster_ctl_require_cmds || exit 1

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
      run_remote_primitive_rel "$node" "scripts/cluster/kill_apps.sh"
    else
      log "skip node=$node, project root missing"
    fi
  done < <(all_cluster_nodes)

  log "phase 2: stop infra"
  if remote_project_exists "$kafka_node"; then
    run_remote_primitive_rel "$kafka_node" "scripts/cluster/stop_kafka.sh"
  else
    log "skip kafka stop: node=$kafka_node project root missing"
  fi

  case "$full_reset" in
    1|2)
      if remote_project_exists "$pg_node"; then
        run_remote_primitive_rel "$pg_node" "scripts/cluster/stop_pg.sh"
      else
        log "skip pg stop: node=$pg_node project root missing"
      fi
      ;;
    0|"")
      :
      ;;
    *)
      die "FULL_RESET must be one of: 0|1|2"
      ;;
  esac

  log "phase 3: reset data"
  case "$full_reset" in
    2|1)
      while read -r node; do
        [[ -z "$node" ]] && continue
        if remote_project_exists "$node"; then
          run_remote_primitive_rel "$node" "scripts/cluster/reset_node_data.sh" "$full_reset"
        else
          log "skip node=$node, project root missing"
        fi
      done < <(all_cluster_nodes)
      ;;
    0|"")
      if remote_project_exists "$kafka_node"; then
        run_remote_primitive_rel "$kafka_node" "scripts/cluster/reset_kafka_storage.sh"
      else
        log "skip kafka storage reset: node=$kafka_node project root missing"
      fi

      if remote_project_exists "$pg_node"; then
        run_remote_primitive_rel "$pg_node" "scripts/cluster/drop_pg_db.sh"
      else
        log "skip pg db drop: node=$pg_node project root missing"
      fi
      ;;
  esac

  log "phase 4: re-bootstrap infra"
  cluster_ensure_infra || die "cluster_ensure_infra failed"

  log "Done."
}

main "$@"

# examples:
#   bash scripts/cluster/factory_reset.sh
#   FULL_RESET=1 bash scripts/cluster/factory_reset.sh
#   FULL_RESET=2 bash scripts/cluster/factory_reset.sh