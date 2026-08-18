#!/usr/bin/env bash

PULSAR_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$PULSAR_SCRIPT_DIR/../../.." && pwd)}"
PULSAR_HOME="${PULSAR_HOME:-$PROJECT_ROOT/.local/pulsar-current}"
export PULSAR_HOME

# shellcheck source=../../common.sh
source "$PROJECT_ROOT/scripts/common.sh"

pulsar_require_case() {
    require_nonempty "CASE_DIR" "${CASE_DIR:-}"
    require_nonempty "CONFIG_PATH" "${CONFIG_PATH:-}"
    require_nonempty "SERVICE_NODE_COUNT" "${SERVICE_NODE_COUNT:-1}"
    require_nonempty "PULSAR_HOME" "${PULSAR_HOME:-}"
    require_file "$PULSAR_HOME/bin/pulsar"
}

pulsar_load_nodes() {
    split_nodes "${SERVICE_NODE_COUNT:-1}"
    PULSAR_NODE="${SERVICE_NODES[0]}"
    PULSAR_ADDRESS="$(resolve_node_service_address "$PULSAR_NODE")"
}

pulsar_runtime_dir() {
    printf '%s\n' "$CASE_DIR/runtime/pulsar"
}

pulsar_ram_root() {
    printf '%s\n' "${PULSAR_RAM_ROOT:-${KAFKA_RAM_ROOT:-/dev/shm/messaging-benchmark}/pulsar}"
}
