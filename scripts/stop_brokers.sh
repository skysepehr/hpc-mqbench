#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Stop Kafka brokers for one benchmark case.
#
# This script assumes:
# - it runs inside an active Slurm allocation
# - scripts/common.sh is available
# - split_nodes "$BROKER_COUNT" can be called
#
# Responsibilities:
# - stop Kafka broker processes on active broker nodes
# - optionally remove RAM-backed Kafka log directories
#
# Required environment variables:
# - CASE_DIR
# - KAFKA_HOME
# - BROKER_COUNT
#
# Optional environment variables:
# - CLEAN_BROKER_RAM_DIRS (default: 0)
# - ALLOW_KAFKA_BROKER_PKILL (default: 0, set 1 only on dedicated nodes)
# - STOP_BROKER_TIMEOUT_SEC (default: 30)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"

require_command srun
require_file "$KAFKA_HOME/bin/kafka-server-stop.sh"

validate_broker_count "$BROKER_COUNT"

CLEAN_BROKER_RAM_DIRS="${CLEAN_BROKER_RAM_DIRS:-1}"
ALLOW_KAFKA_BROKER_PKILL="${ALLOW_KAFKA_BROKER_PKILL:-0}"
STOP_BROKER_TIMEOUT_SEC="${STOP_BROKER_TIMEOUT_SEC:-30}"
ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
KAFKA_RAM_ROOT="${KAFKA_RAM_ROOT:-/dev/shm/kafka-simple-${USER:-user}-${SLURM_JOB_ID:-manual}}"

# Rebuild broker node list from the Slurm allocation.
split_nodes "$BROKER_COUNT"

BROKER_RUNTIME_DIR="$CASE_DIR/runtime/brokers"
BROKER_LOG_DIR="$CASE_DIR/logs/brokers"
ensure_dir "$BROKER_LOG_DIR"

log_info "Stopping Kafka brokers on: $(join_by , "${BROKER_NODES[@]}")"

broker_ram_data_dir() {
    local node_id="${1:?node_id is required}"
    if [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
        printf '%s/broker-%s/kafka-logs\n' "$KAFKA_RAM_ROOT" "$node_id"
    else
        printf '/dev/shm/kafka-logs-%s\n' "$node_id"
    fi
}

broker_ram_metadata_dir() {
    local node_id="${1:?node_id is required}"
    if [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
        printf '%s/broker-%s/metadata-log\n' "$KAFKA_RAM_ROOT" "$node_id"
    else
        printf '/dev/shm/kafka-metadata-%s\n' "$node_id"
    fi
}

stop_one_broker() {
    local node_id="${1:?node_id is required}"
    local node_name="${2:?node_name is required}"
    local stop_log_file="${3:?stop_log_file is required}"
    local broker_pid_file="${4:?broker_pid_file is required}"
    local broker_data_dir="${5:?broker data dir is required}"
    local broker_metadata_dir="${6:?broker metadata dir is required}"
    local remote_stop_script

    log_info "Stopping broker node.id=${node_id} on ${node_name}"

    remote_stop_script="
        set -euo pipefail

        # Prefer the exact PID recorded when this benchmark started the broker.
        if [[ -f '${broker_pid_file}' ]]; then
            PID=\$(cat '${broker_pid_file}' || true)
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
            rm -f '${broker_pid_file}'
        elif [[ -x '${KAFKA_HOME}/bin/kafka-server-stop.sh' ]]; then
            # Fall back to Kafka's stop script only when no exact PID was saved.
            '${KAFKA_HOME}/bin/kafka-server-stop.sh' || true
        fi

        # Optional broad fallback for dedicated broker nodes only.
        if [[ '${ALLOW_KAFKA_BROKER_PKILL}' == '1' ]]; then
            pkill -f 'kafka.Kafka' || true
        fi

        # Optional cleanup of RAM-backed broker log directory.
        if [[ '${CLEAN_BROKER_RAM_DIRS}' == '1' ]]; then
            rm -rf '${broker_data_dir}' '${broker_metadata_dir}' || true
        fi
    "

    if command -v timeout >/dev/null 2>&1; then
        timeout "$STOP_BROKER_TIMEOUT_SEC" \
            srun --overlap --nodes=1 --ntasks=1 -w "$node_name" bash -lc "$remote_stop_script" \
            >"$stop_log_file" 2>&1 || log_warn "Timed out or failed while stopping broker node.id=${node_id}; see $stop_log_file" &
    else
        srun --overlap --nodes=1 --ntasks=1 -w "$node_name" bash -lc "$remote_stop_script" \
            >"$stop_log_file" 2>&1 || log_warn "Failed while stopping broker node.id=${node_id}; see $stop_log_file" &
    fi
}

for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
    NODE_NAME="${BROKER_NODES[$i]}"
    NODE_ID=$((i + 1))
    NODE_LOG_DIR="$BROKER_LOG_DIR/${NODE_NAME}"
    STOP_LOG_FILE="$NODE_LOG_DIR/kafka-stop.log"
    BROKER_PID_FILE="$BROKER_RUNTIME_DIR/pids/broker-${NODE_ID}.pid"
    BROKER_DATA_PATH="$(broker_ram_data_dir "$NODE_ID")"
    BROKER_METADATA_PATH="$(broker_ram_metadata_dir "$NODE_ID")"

    ensure_dir "$NODE_LOG_DIR"
    stop_one_broker "$NODE_ID" "$NODE_NAME" "$STOP_LOG_FILE" "$BROKER_PID_FILE" "$BROKER_DATA_PATH" "$BROKER_METADATA_PATH"
done

# Wait for all stop commands to complete.
wait

log_info "Kafka broker stop sequence completed"

if [[ "$CLEAN_BROKER_RAM_DIRS" == "1" && "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
    for NODE_NAME in "${ALL_NODES[@]}"; do
        CLEAN_LOG_FILE="$BROKER_LOG_DIR/ram-root-cleanup-${NODE_NAME}.log"
        log_info "Removing RAM-backed runtime root on ${NODE_NAME}: $KAFKA_RAM_ROOT"
        srun --overlap --nodes=1 --ntasks=1 -w "$NODE_NAME" bash -lc "
            set -euo pipefail
            rm -rf '${KAFKA_RAM_ROOT}' || true
        " >"$CLEAN_LOG_FILE" 2>&1 || \
            log_warn "Could not remove RAM runtime root on ${NODE_NAME}; see $CLEAN_LOG_FILE"
    done
fi
