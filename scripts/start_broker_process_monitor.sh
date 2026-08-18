#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
require_nonempty "KAFKA_RAM_ROOT" "${KAFKA_RAM_ROOT:-}"
require_command srun
require_command python3

split_nodes "$BROKER_COUNT"
MONITOR_DIR="$CASE_DIR/runtime/broker-process-monitor"
DATA_DIR="$CASE_DIR/monitoring"
LOG_DIR="$CASE_DIR/logs/broker-process-monitor"
INTERVAL_SEC="${BROKER_PROCESS_MONITOR_INTERVAL_SEC:-1}"
INTERFACE="${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$MONITOR_DIR"
ensure_dir "$DATA_DIR"
ensure_dir "$LOG_DIR"

for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
    node="${BROKER_NODES[$i]}"
    node_id=$((i + 1))
    broker_pid_file="$CASE_DIR/runtime/brokers/pids/broker-${node_id}.pid"
    output_file="$DATA_DIR/broker_process_${node}.csv"
    stop_file="$MONITOR_DIR/stop-${node_id}"
    srun_pid_file="$MONITOR_DIR/srun-${node_id}.pid"
    log_file="$LOG_DIR/${node}.log"
    rm -f "$stop_file"

    srun --overlap --cpu-bind=none --nodes=1 --ntasks=1 -w "$node" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}
        cd '${PROJECT_ROOT}'
        export PYTHONPATH='${PROJECT_ROOT}':\${PYTHONPATH:-}
        python3 -m src.benchmark.broker_process_monitor \
            --pid-file '${broker_pid_file}' \
            --output '${output_file}' \
            --stop-file '${stop_file}' \
            --tmpfs-path '${KAFKA_RAM_ROOT}' \
            --interface '${INTERFACE}' \
            --interval-sec '${INTERVAL_SEC}'
    " >"$log_file" 2>&1 &
    echo "$!" > "$srun_pid_file"
done

log_info "Kafka process/ib0/tmpfs monitor started"
