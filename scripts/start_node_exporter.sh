#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Start node_exporter on selected allocated nodes.
#
# Required environment variables:
# - CASE_DIR
# - BROKER_COUNT
# - NODE_EXPORTER_HOME
#
# Optional environment variables:
# - NODE_EXPORTER_PORT       (default: 9100)
# - NODE_EXPORTER_NODE_SCOPE (default: all)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
require_nonempty "NODE_EXPORTER_HOME" "${NODE_EXPORTER_HOME:-}"

require_command srun
require_file "$NODE_EXPORTER_HOME/node_exporter"

NODE_EXPORTER_PORT="${NODE_EXPORTER_PORT:-9100}"

split_nodes "$BROKER_COUNT"
select_node_exporter_nodes

NODE_EXPORTER_LOG_DIR="$CASE_DIR/logs/node_exporter"
NODE_EXPORTER_RUNTIME_DIR="$CASE_DIR/runtime/node_exporter"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$NODE_EXPORTER_LOG_DIR"
ensure_dir "$NODE_EXPORTER_RUNTIME_DIR"

start_one_node_exporter() {
    local node_name="${1:?node required}"
    local log_file="${2:?log file required}"
    local pid_file="${3:?pid file required}"
    local srun_pid_file="${4:?srun pid file required}"

    srun --overlap --nodes=1 --ntasks=1 -w "$node_name" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}
        echo \$\$ > '${pid_file}'
        exec '${NODE_EXPORTER_HOME}/node_exporter' \
            --web.listen-address=':${NODE_EXPORTER_PORT}'
    " > "$log_file" 2>&1 &
    echo $! > "$srun_pid_file"
}

for node in "${NODE_EXPORTER_NODES[@]}"; do
    ensure_dir "$NODE_EXPORTER_LOG_DIR/$node"
    ensure_dir "$NODE_EXPORTER_RUNTIME_DIR/$node"

    start_one_node_exporter \
        "$node" \
        "$NODE_EXPORTER_LOG_DIR/$node/node_exporter.log" \
        "$NODE_EXPORTER_RUNTIME_DIR/$node/node_exporter.pid" \
        "$NODE_EXPORTER_RUNTIME_DIR/$node/node_exporter.srun.pid"
done

sleep 1
for node in "${NODE_EXPORTER_NODES[@]}"; do
    node_address="$(resolve_node_service_address "$node")"
    wait_for_tcp_endpoint "$node_address" "$NODE_EXPORTER_PORT" 30 "node_exporter on $node"
done
log_info "node_exporter started on nodes: $(join_by , "${NODE_EXPORTER_NODES[@]}")"
