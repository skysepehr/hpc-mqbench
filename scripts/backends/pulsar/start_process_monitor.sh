#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
pulsar_require_case
pulsar_load_nodes

RUNTIME_DIR="$(pulsar_runtime_dir)"
MONITOR_DIR="$RUNTIME_DIR/process-monitor"
DATA_DIR="$CASE_DIR/monitoring"
LOG_DIR="$CASE_DIR/logs/pulsar-process-monitor"
INTERVAL_SEC="${BACKEND_PROCESS_MONITOR_INTERVAL_SEC:-${BROKER_PROCESS_MONITOR_INTERVAL_SEC:-1}}"
INTERFACE="${BENCHMARK_NETWORK_INTERFACE:-${KAFKA_HPC_NETWORK_INTERFACE:-ib0}}"
PID_FILE="$RUNTIME_DIR/pids/pulsar.pid"
STOP_FILE="$MONITOR_DIR/stop"
SRUN_PID_FILE="$MONITOR_DIR/srun.pid"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"
ensure_dir "$MONITOR_DIR"
ensure_dir "$DATA_DIR"
ensure_dir "$LOG_DIR"
rm -f "$STOP_FILE"

srun --overlap --cpu-bind=none --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}
    cd '${PROJECT_ROOT}'
    export PYTHONPATH='${PROJECT_ROOT}':\${PYTHONPATH:-}
    python3 -m src.benchmark.broker_process_monitor \
        --pid-file '${PID_FILE}' \
        --output '${DATA_DIR}/pulsar_process_${PULSAR_NODE}.csv' \
        --stop-file '${STOP_FILE}' \
        --tmpfs-path '$(pulsar_ram_root)' \
        --interface '${INTERFACE}' \
        --interval-sec '${INTERVAL_SEC}'
" >"$LOG_DIR/${PULSAR_NODE}.log" 2>&1 &
echo "$!" > "$SRUN_PID_FILE"
log_info "Pulsar process/interface/tmpfs monitor started"
