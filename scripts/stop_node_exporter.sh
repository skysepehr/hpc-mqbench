#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Stop node_exporter on selected allocated nodes.
#
# start_node_exporter.sh writes one PID file per broker node. We prefer that PID
# for a clean stop and use pkill only when explicitly allowed.
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"

require_command srun
validate_broker_count "$BROKER_COUNT"

ALLOW_NODE_EXPORTER_PKILL="${ALLOW_NODE_EXPORTER_PKILL:-0}"
STOP_NODE_EXPORTER_TIMEOUT_SEC="${STOP_NODE_EXPORTER_TIMEOUT_SEC:-30}"

split_nodes "$BROKER_COUNT"
select_node_exporter_nodes

NODE_EXPORTER_RUNTIME_DIR="$CASE_DIR/runtime/node_exporter"
NODE_EXPORTER_LOG_DIR="$CASE_DIR/logs/node_exporter"
ensure_dir "$NODE_EXPORTER_LOG_DIR"

stop_one_node_exporter() {
    local node_name="${1:?node required}"
    local pid_file="${2:?pid file required}"
    local log_file="${3:?log file required}"
    local remote_stop_script

    ensure_dir "$(dirname "$log_file")"

    remote_stop_script="
        set -euo pipefail

        if [[ -f '${pid_file}' ]]; then
            PID=\$(cat '${pid_file}' || true)
            if [[ -n \"\${PID:-}\" ]]; then
                kill \"\$PID\" || true
                for _ in 1 2 3 4 5 6 7 8 9 10; do
                    if ! kill -0 \"\$PID\" >/dev/null 2>&1; then
                        break
                    fi
                    sleep 1
                done
                if kill -0 \"\$PID\" >/dev/null 2>&1; then
                    kill -KILL \"\$PID\" || true
                fi
            fi
            rm -f '${pid_file}'
        fi

        if [[ '${ALLOW_NODE_EXPORTER_PKILL}' == '1' ]]; then
            pkill -f 'node_exporter' || true
        fi
    "

    if command -v timeout >/dev/null 2>&1; then
        timeout "$STOP_NODE_EXPORTER_TIMEOUT_SEC" \
            srun --overlap --nodes=1 --ntasks=1 -w "$node_name" bash -lc "$remote_stop_script" \
            >"$log_file" 2>&1 || log_warn "Timed out or failed while stopping node_exporter on ${node_name}; see $log_file" &
    else
        srun --overlap --nodes=1 --ntasks=1 -w "$node_name" bash -lc "$remote_stop_script" \
            >"$log_file" 2>&1 || log_warn "Failed while stopping node_exporter on ${node_name}; see $log_file" &
    fi
}

for node in "${NODE_EXPORTER_NODES[@]}"; do
    stop_one_node_exporter \
        "$node" \
        "$NODE_EXPORTER_RUNTIME_DIR/$node/node_exporter.pid" \
        "$NODE_EXPORTER_LOG_DIR/$node/node_exporter-stop.log"
done

wait
log_info "node_exporter stop sequence completed"
