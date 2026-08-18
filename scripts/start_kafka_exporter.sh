#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Start kafka_exporter on the monitoring node for one benchmark case.
#
# kafka_exporter is a Prometheus metrics exporter. It is not the Kafka broker.
# It connects to the active Kafka bootstrap servers and exposes topic, broker,
# and consumer-group metrics on the monitoring node.
#
# Required environment variables:
# - CASE_DIR
# - BROKER_COUNT
#
# Optional environment variables:
# - KAFKA_EXPORTER_HOME (default: tools/kafka_exporter-current)
# - KAFKA_EXPORTER_PORT (default: 9308)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
require_command srun

validate_broker_count "$BROKER_COUNT"
split_nodes "$BROKER_COUNT"
MONITORING_ADDRESS="$(resolve_node_service_address "$MONITORING_NODE")"

KAFKA_EXPORTER_HOME="${KAFKA_EXPORTER_HOME:-$PROJECT_ROOT/tools/kafka_exporter-current}"
KAFKA_EXPORTER_PORT="${KAFKA_EXPORTER_PORT:-9308}"

require_file "$KAFKA_EXPORTER_HOME/kafka_exporter"

RUNTIME_DIR="$CASE_DIR/runtime"
BOOTSTRAP_FILE="$RUNTIME_DIR/bootstrap_servers.txt"
require_file "$BOOTSTRAP_FILE"

BOOTSTRAP_SERVERS="$(<"$BOOTSTRAP_FILE")"
require_nonempty "BOOTSTRAP_SERVERS" "$BOOTSTRAP_SERVERS"

EXPORTER_RUNTIME_DIR="$CASE_DIR/runtime/kafka_exporter"
EXPORTER_LOG_DIR="$CASE_DIR/logs/kafka_exporter"
EXPORTER_PID_FILE="$EXPORTER_RUNTIME_DIR/kafka_exporter.pid"
EXPORTER_SRUN_PID_FILE="$EXPORTER_RUNTIME_DIR/kafka_exporter.srun.pid"
EXPORTER_LOG_FILE="$EXPORTER_LOG_DIR/kafka_exporter.log"
EXPORTER_ENDPOINT_FILE="$EXPORTER_RUNTIME_DIR/kafka_exporter_endpoint.txt"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$EXPORTER_RUNTIME_DIR"
ensure_dir "$EXPORTER_LOG_DIR"

KAFKA_SERVER_ARGS=""
IFS=',' read -r -a KAFKA_SERVERS <<< "$BOOTSTRAP_SERVERS"
for server in "${KAFKA_SERVERS[@]}"; do
    server="${server//[[:space:]]/}"
    [[ -n "$server" ]] || continue
    KAFKA_SERVER_ARGS+=" --kafka.server=$(printf '%q' "$server")"
done

if [[ -z "$KAFKA_SERVER_ARGS" ]]; then
    die "No Kafka bootstrap servers available for kafka_exporter"
fi

log_info "Starting kafka_exporter on monitoring node: $MONITORING_NODE"
log_info "kafka_exporter service address: $MONITORING_ADDRESS"
log_info "kafka_exporter bootstrap servers: $BOOTSTRAP_SERVERS"

srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}

    echo \$\$ > '${EXPORTER_PID_FILE}'
    exec '${KAFKA_EXPORTER_HOME}/kafka_exporter' \
        --web.listen-address='0.0.0.0:${KAFKA_EXPORTER_PORT}' \
        ${KAFKA_SERVER_ARGS}
" > "$EXPORTER_LOG_FILE" 2>&1 &

echo $! > "$EXPORTER_SRUN_PID_FILE"
sleep 1

echo "${MONITORING_ADDRESS}:${KAFKA_EXPORTER_PORT}" > "$EXPORTER_ENDPOINT_FILE"

wait_for_tcp_endpoint "$MONITORING_ADDRESS" "$KAFKA_EXPORTER_PORT" 30 "kafka_exporter on $MONITORING_NODE"
log_info "kafka_exporter started"
log_info "kafka_exporter endpoint: http://${MONITORING_ADDRESS}:${KAFKA_EXPORTER_PORT}/metrics"
