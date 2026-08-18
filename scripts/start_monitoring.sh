#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Start monitoring services for one benchmark case.
#
# Responsibilities:
# - determine the active backend service nodes
# - generate a Prometheus scrape configuration dynamically
# - include JMX exporter targets for all active brokers
# - optionally include node_exporter targets for selected allocated nodes
# - optionally include kafka_exporter on the monitoring node
# - start Prometheus on the monitoring node
#
# Assumptions:
# - scripts/common.sh is available
# - split_nodes "$BROKER_COUNT" can be called
# - Prometheus is installed and PROMETHEUS_HOME is set
#
# Required environment variables:
# - CASE_DIR
# - BROKER_COUNT
# - PROMETHEUS_HOME
#
# Optional environment variables:
# - PROMETHEUS_PORT        (default: 9090)
# - PROMETHEUS_SCRAPE_INTERVAL_SEC (default: 15)
# - PROMETHEUS_READY_TIMEOUT_SEC (default: 120)
# - JMX_EXPORTER_PORT_BASE (default: 7101)
# - NODE_EXPORTER_PORT     (default: 9100)
# - ENABLE_NODE_EXPORTER   (default: 1)
# - NODE_EXPORTER_NODE_SCOPE (default: all)
# - ENABLE_KAFKA_EXPORTER  (default: 0)
# - KAFKA_EXPORTER_PORT    (default: 9308)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
SERVICE_NODE_COUNT="${SERVICE_NODE_COUNT:-${BROKER_COUNT:-}}"
require_nonempty "SERVICE_NODE_COUNT" "$SERVICE_NODE_COUNT"
export SERVICE_NODE_COUNT
require_nonempty "PROMETHEUS_HOME" "${PROMETHEUS_HOME:-}"

require_command srun
require_file "$PROMETHEUS_HOME/prometheus"

if [[ "${BACKEND_ID:-kafka}" == "kafka" ]]; then
    validate_broker_count "${BROKER_COUNT:-$SERVICE_NODE_COUNT}"
fi

PROMETHEUS_PORT="${PROMETHEUS_PORT:-9090}"
PROMETHEUS_SCRAPE_INTERVAL_SEC="${PROMETHEUS_SCRAPE_INTERVAL_SEC:-15}"
# Starting a new overlapping Slurm step can be delayed even when its allocated
# node is healthy. Allow that scheduler delay before declaring Prometheus dead.
PROMETHEUS_READY_TIMEOUT_SEC="${PROMETHEUS_READY_TIMEOUT_SEC:-120}"
JMX_EXPORTER_PORT_BASE="${JMX_EXPORTER_PORT_BASE:-${KAFKA_JMX_PORT_BASE:-7101}}"
NODE_EXPORTER_PORT="${NODE_EXPORTER_PORT:-9100}"
ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
ENABLE_KAFKA_EXPORTER="${ENABLE_KAFKA_EXPORTER:-0}"
KAFKA_EXPORTER_PORT="${KAFKA_EXPORTER_PORT:-9308}"

if [[ ! "$PROMETHEUS_SCRAPE_INTERVAL_SEC" =~ ^[1-9][0-9]*$ ]]; then
    die "PROMETHEUS_SCRAPE_INTERVAL_SEC must be a positive integer, got: $PROMETHEUS_SCRAPE_INTERVAL_SEC"
fi
if [[ ! "$PROMETHEUS_READY_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]]; then
    die "PROMETHEUS_READY_TIMEOUT_SEC must be a positive integer, got: $PROMETHEUS_READY_TIMEOUT_SEC"
fi

# Rebuild node-role assignment from current Slurm allocation.
split_nodes "$SERVICE_NODE_COUNT"
if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
    select_node_exporter_nodes
fi
MONITORING_ADDRESS="$(resolve_node_service_address "$MONITORING_NODE")"

MONITORING_RUNTIME_DIR="$CASE_DIR/runtime/monitoring"
MONITORING_LOG_DIR="$CASE_DIR/logs/monitoring"
PROMETHEUS_DATA_DIR="$MONITORING_RUNTIME_DIR/prometheus-data"
PROMETHEUS_CONFIG_FILE="$MONITORING_RUNTIME_DIR/prometheus.yml"
PROMETHEUS_LOG_FILE="$MONITORING_LOG_DIR/prometheus.log"
PROMETHEUS_PID_FILE="$MONITORING_RUNTIME_DIR/prometheus.pid"
PROMETHEUS_SRUN_PID_FILE="$MONITORING_RUNTIME_DIR/prometheus.srun.pid"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$MONITORING_RUNTIME_DIR"
ensure_dir "$MONITORING_LOG_DIR"
ensure_dir "$PROMETHEUS_DATA_DIR"

log_info "Preparing Prometheus config for monitoring node: $MONITORING_NODE"
log_info "Monitoring service address: $MONITORING_ADDRESS"
log_info "Backend service nodes to scrape: $(join_by , "${SERVICE_NODES[@]}")"
log_info "Prometheus scrape interval: ${PROMETHEUS_SCRAPE_INTERVAL_SEC}s"
if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
    log_info "node_exporter nodes to scrape: $(join_by , "${NODE_EXPORTER_NODES[@]}")"
fi

# -----------------------------------------------------------------------------
# Build the Prometheus scrape config dynamically.
#
# We generate scrape jobs for:
# - kafka_jmx: Kafka/JVM metrics through JMX exporter
# - node_exporter: host metrics for each broker node
# - kafka_exporter: optional Kafka cluster and consumer-group metrics
# -----------------------------------------------------------------------------
{
    echo "global:"
    echo "  scrape_interval: ${PROMETHEUS_SCRAPE_INTERVAL_SEC}s"
    echo ""
    echo "scrape_configs:"

    if [[ "${BACKEND_ID:-kafka}" == "kafka" ]]; then
        # Kafka JMX exporter targets
        echo "  - job_name: 'kafka_jmx'"
        echo "    static_configs:"
        for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
            NODE_NAME="${BROKER_NODES[$i]}"
            NODE_ADDRESS="$(resolve_node_service_address "$NODE_NAME")"
            JMX_PORT=$((JMX_EXPORTER_PORT_BASE + i))
            NODE_ROLE="$(benchmark_node_role_label "$NODE_NAME")"
            echo "      - targets: ['${NODE_ADDRESS}:${JMX_PORT}']"
            echo "        labels:"
            echo "          node: '${NODE_NAME}'"
            echo "          role: '${NODE_ROLE}'"
            echo "          exporter: 'jmx'"
        done
    elif [[ "$BACKEND_ID" == "pulsar" ]]; then
        # Pulsar exposes Prometheus metrics directly on the admin port.
        echo "  - job_name: 'pulsar'"
        echo "    metrics_path: '/metrics/'"
        echo "    static_configs:"
        for NODE_NAME in "${SERVICE_NODES[@]}"; do
            NODE_ADDRESS="$(resolve_node_service_address "$NODE_NAME")"
            NODE_ROLE="$(benchmark_node_role_label "$NODE_NAME")"
            echo "      - targets: ['${NODE_ADDRESS}:8080']"
            echo "        labels:"
            echo "          node: '${NODE_NAME}'"
            echo "          role: '${NODE_ROLE}'"
            echo "          exporter: 'pulsar_native'"
        done
    else
        die "No Prometheus scrape definition for backend: $BACKEND_ID"
    fi

    # Optional node_exporter targets
    if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
        echo "  - job_name: 'node_exporter'"
        echo "    static_configs:"
        for NODE_NAME in "${NODE_EXPORTER_NODES[@]}"; do
            NODE_ADDRESS="$(resolve_node_service_address "$NODE_NAME")"
            NODE_ROLE="$(benchmark_node_role_label "$NODE_NAME")"
            echo "      - targets: ['${NODE_ADDRESS}:${NODE_EXPORTER_PORT}']"
            echo "        labels:"
            echo "          node: '${NODE_NAME}'"
            echo "          role: '${NODE_ROLE}'"
            echo "          exporter: 'node'"
        done
    fi

    # Optional kafka_exporter target on the monitoring node
    if [[ "${BACKEND_ID:-kafka}" == "kafka" && "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        echo "  - job_name: 'kafka_exporter'"
        echo "    static_configs:"
        echo "      - targets: ['${MONITORING_ADDRESS}:${KAFKA_EXPORTER_PORT}']"
        echo "        labels:"
        echo "          node: '${MONITORING_NODE}'"
        echo "          role: 'monitoring'"
        echo "          exporter: 'kafka'"
    fi
} > "$PROMETHEUS_CONFIG_FILE"

log_info "Prometheus config written to: $PROMETHEUS_CONFIG_FILE"

# -----------------------------------------------------------------------------
# Start Prometheus on the monitoring node.
#
# We use nohup so it keeps running after the srun shell exits.
# The PID is saved into a file so stop_monitoring.sh can stop it later.
# -----------------------------------------------------------------------------
log_info "Starting Prometheus on node: $MONITORING_NODE"

srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}

    echo \$\$ > '${PROMETHEUS_PID_FILE}'
    exec '${PROMETHEUS_HOME}/prometheus' \
        --config.file='${PROMETHEUS_CONFIG_FILE}' \
        --storage.tsdb.path='${PROMETHEUS_DATA_DIR}' \
        --web.listen-address='0.0.0.0:${PROMETHEUS_PORT}'
" > "$PROMETHEUS_LOG_FILE" 2>&1 &

echo $! > "$PROMETHEUS_SRUN_PID_FILE"
sleep 1

echo "${MONITORING_ADDRESS}:${PROMETHEUS_PORT}" > "$MONITORING_RUNTIME_DIR/prometheus_endpoint.txt"

wait_for_tcp_endpoint "$MONITORING_ADDRESS" "$PROMETHEUS_PORT" "$PROMETHEUS_READY_TIMEOUT_SEC" "Prometheus on $MONITORING_NODE"
log_info "Prometheus started"
log_info "Prometheus endpoint: http://${MONITORING_ADDRESS}:${PROMETHEUS_PORT}"
