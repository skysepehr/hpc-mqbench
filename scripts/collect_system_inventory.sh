#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Collect same-allocation system capability data before the backend starts.
#
# Required environment variables:
# - CASE_DIR
# - SERVICE_NODE_COUNT (BROKER_COUNT remains a compatibility alias)
#
# Optional environment variables:
# - ENABLE_SYSTEM_INVENTORY       (default: 1)
# - ENABLE_SYSTEM_IPERF           (default: 1)
# - ENABLE_SYSTEM_RAM_PROBE       (default: 1)
# - SYSTEM_IPERF_SECONDS          (default: 10)
# - SYSTEM_IPERF_PARALLEL         (default: 4)
# - SYSTEM_IPERF_PORT_BASE        (default: 5201)
# - SYSTEM_IPERF_READY_SLEEP_SEC   (default: 8)
# - SYSTEM_IPERF_CONNECT_RETRIES   (default: 5)
# - SYSTEM_IPERF_RETRY_DELAY_SEC   (default: 2)
# - SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC (default: 30)
# - SYSTEM_RAM_PROBE_MB           (default: 512)
# - SYSTEM_RAM_PROBE_TIMEOUT_SEC  (default: 60)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
SERVICE_NODE_COUNT="${SERVICE_NODE_COUNT:-${BROKER_COUNT:-}}"
require_nonempty "SERVICE_NODE_COUNT" "$SERVICE_NODE_COUNT"
require_command python3
require_command srun

ENABLE_SYSTEM_INVENTORY="${ENABLE_SYSTEM_INVENTORY:-1}"
ENABLE_SYSTEM_IPERF="${ENABLE_SYSTEM_IPERF:-1}"
ENABLE_SYSTEM_RAM_PROBE="${ENABLE_SYSTEM_RAM_PROBE:-1}"
SYSTEM_IPERF_SECONDS="${SYSTEM_IPERF_SECONDS:-10}"
SYSTEM_IPERF_PARALLEL="${SYSTEM_IPERF_PARALLEL:-4}"
SYSTEM_IPERF_PORT_BASE="${SYSTEM_IPERF_PORT_BASE:-5201}"
SYSTEM_IPERF_READY_SLEEP_SEC="${SYSTEM_IPERF_READY_SLEEP_SEC:-8}"
SYSTEM_IPERF_CONNECT_RETRIES="${SYSTEM_IPERF_CONNECT_RETRIES:-5}"
SYSTEM_IPERF_RETRY_DELAY_SEC="${SYSTEM_IPERF_RETRY_DELAY_SEC:-2}"
SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC="${SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC:-30}"
SYSTEM_RAM_PROBE_MB="${SYSTEM_RAM_PROBE_MB:-512}"
SYSTEM_RAM_PROBE_TIMEOUT_SEC="${SYSTEM_RAM_PROBE_TIMEOUT_SEC:-60}"
BENCHMARK_NETWORK_INTERFACE="${BENCHMARK_NETWORK_INTERFACE:-${KAFKA_HPC_NETWORK_INTERFACE:-ib0}}"

if [[ "$ENABLE_SYSTEM_INVENTORY" != "1" ]]; then
    log_info "System inventory disabled"
    exit 0
fi

split_nodes "$SERVICE_NODE_COUNT"

SYSTEM_RUNTIME_DIR="$CASE_DIR/runtime/system_inventory"
SYSTEM_NODE_DIR="$SYSTEM_RUNTIME_DIR/nodes"
SYSTEM_IPERF_DIR="$SYSTEM_RUNTIME_DIR/iperf"
SYSTEM_IPERF_RAW_DIR="$SYSTEM_RUNTIME_DIR/iperf_raw"
SYSTEM_LOG_DIR="$CASE_DIR/logs/system_inventory"
SYSTEM_INVENTORY_FILE="$SYSTEM_RUNTIME_DIR/system_inventory.json"
SYSTEM_DATA_COPY="$CASE_DIR/data/system_inventory.json"
SERVICE_MAP_FILE="$CASE_DIR/runtime/node_service_addresses.tsv"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$SYSTEM_NODE_DIR"
ensure_dir "$SYSTEM_IPERF_DIR"
ensure_dir "$SYSTEM_IPERF_RAW_DIR"
ensure_dir "$SYSTEM_LOG_DIR"
ensure_dir "$CASE_DIR/data"

write_node_service_address_map "$SERVICE_MAP_FILE" "${ALL_NODES[@]}"

node_service_address() {
    local node="${1:?node required}"
    awk -F '\t' -v node="$node" '$1 == node {print $2; found=1; exit} END {if (!found) exit 1}' "$SERVICE_MAP_FILE"
}

write_failed_node_record() {
    local output_file="${1:?output file required}"
    local node="${2:?node required}"
    local role="${3:?role required}"
    local service_address="${4:?service address required}"
    local reason="${5:?reason required}"

    python3 - "$output_file" "$node" "$role" "$service_address" "$reason" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

output, node, role, service_address, reason = sys.argv[1:]
payload = {
    "node": node,
    "role": role,
    "service_address": service_address,
    "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    "status": "failed",
    "reason": reason,
}
Path(output).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY
}

RAM_PROBE_BIN=""
compile_ram_probe() {
    if [[ "$ENABLE_SYSTEM_RAM_PROBE" != "1" ]]; then
        log_info "RAM bandwidth probe disabled"
        return 0
    fi
    if ! command -v gcc >/dev/null 2>&1; then
        log_warn "gcc is not available; RAM bandwidth probe will be marked skipped"
        return 0
    fi

    local source_file="$PROJECT_ROOT/tools/stream_probe.c"
    local output_file="$SYSTEM_RUNTIME_DIR/stream_probe"
    local compile_log="$SYSTEM_LOG_DIR/stream_probe_compile.log"
    if gcc -O3 -std=c11 "$source_file" -o "$output_file" >"$compile_log" 2>&1; then
        RAM_PROBE_BIN="$output_file"
        log_info "RAM bandwidth probe compiled: $RAM_PROBE_BIN"
    else
        RAM_PROBE_BIN=""
        log_warn "RAM bandwidth probe compile failed; see $compile_log"
    fi
}

collect_one_node() {
    local node="${1:?node required}"
    local role service_address output_file log_file
    role="$(benchmark_node_role_label "$node")"
    service_address="$(node_service_address "$node" || printf '%s' "$node")"
    output_file="$SYSTEM_NODE_DIR/${node}.json"
    log_file="$SYSTEM_LOG_DIR/${node}.log"

    local q_node q_role q_address q_interface q_probe q_output q_ram_mb q_ram_timeout
    printf -v q_node '%q' "$node"
    printf -v q_role '%q' "$role"
    printf -v q_address '%q' "$service_address"
    printf -v q_interface '%q' "$BENCHMARK_NETWORK_INTERFACE"
    printf -v q_probe '%q' "$RAM_PROBE_BIN"
    printf -v q_output '%q' "$output_file"
    printf -v q_ram_mb '%q' "$SYSTEM_RAM_PROBE_MB"
    printf -v q_ram_timeout '%q' "$SYSTEM_RAM_PROBE_TIMEOUT_SEC"

    log_info "Collecting system facts on $node ($role)"
    if ! srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}
        python3 -m src.benchmark.system_inventory collect-node \
            --node ${q_node} \
            --role ${q_role} \
            --service-address ${q_address} \
            --interface ${q_interface} \
            --ram-probe ${q_probe} \
            --ram-array-mb ${q_ram_mb} \
            --ram-timeout-sec ${q_ram_timeout} \
            --output ${q_output}
    " >"$log_file" 2>&1; then
        write_failed_node_record "$output_file" "$node" "$role" "$service_address" "node inventory command failed; see logs/system_inventory/${node}.log"
        log_warn "System inventory failed on $node; see $log_file"
    fi
}

first_node_for_role() {
    local desired="${1:?role required}"
    local node role
    for node in "${ALL_NODES[@]}"; do
        role="$(benchmark_node_role_label "$node")"
        if [[ "$role" == "$desired" ]]; then
            printf '%s\n' "$node"
            return 0
        fi
    done
    return 1
}

first_producer_node() {
    local node role
    for node in "${ALL_NODES[@]}"; do
        role="$(benchmark_node_role_label "$node")"
        case "$role" in
            producer_controller|producer)
                printf '%s\n' "$node"
                return 0
                ;;
        esac
    done
    return 1
}

normalize_iperf() {
    local raw_input="${1:?raw input required}"
    local output_file="${2:?output file required}"
    local label="${3:?label required}"
    local source_node="${4:?source node required}"
    local target_node="${5:?target node required}"
    local target_address="${6:?target address required}"
    local port="${7:?port required}"
    local status="${8:?status required}"
    local reason="${9:-}"

    python3 -m src.benchmark.system_inventory normalize-iperf \
        --raw-input "$raw_input" \
        --output "$output_file" \
        --label "$label" \
        --source-node "$source_node" \
        --target-node "$target_node" \
        --target-address "$target_address" \
        --port "$port" \
        --seconds "$SYSTEM_IPERF_SECONDS" \
        --parallel "$SYSTEM_IPERF_PARALLEL" \
        --status "$status" \
        --reason "$reason"
}

run_iperf_path() {
    local label="${1:?label required}"
    local source_node="${2:?source node required}"
    local target_node="${3:?target node required}"
    local index="${4:?index required}"
    local target_address port raw_output output_file server_log client_log server_pid client_status server_timeout attempt

    output_file="$SYSTEM_IPERF_DIR/${label}.json"
    raw_output="$SYSTEM_IPERF_RAW_DIR/${label}.raw.json"
    server_log="$SYSTEM_LOG_DIR/${label}.server.log"
    client_log="$SYSTEM_LOG_DIR/${label}.client.log"
    target_address="$(node_service_address "$target_node" || printf '%s' "$target_node")"
    port=$((SYSTEM_IPERF_PORT_BASE + index))

    if [[ "$ENABLE_SYSTEM_IPERF" != "1" ]]; then
        normalize_iperf "$raw_output" "$output_file" "$label" "$source_node" "$target_node" "$target_address" "$port" "skipped" "iperf3 capability tests disabled"
        return 0
    fi
    if ! command -v iperf3 >/dev/null 2>&1; then
        normalize_iperf "$raw_output" "$output_file" "$label" "$source_node" "$target_node" "$target_address" "$port" "skipped" "iperf3 is not available on the batch node"
        return 0
    fi

    local q_target_address q_port q_seconds q_parallel
    printf -v q_target_address '%q' "$target_address"
    printf -v q_port '%q' "$port"
    printf -v q_seconds '%q' "$SYSTEM_IPERF_SECONDS"
    printf -v q_parallel '%q' "$SYSTEM_IPERF_PARALLEL"
    server_timeout=$((
        SYSTEM_IPERF_READY_SLEEP_SEC
        + SYSTEM_IPERF_SECONDS
        + SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC
        + SYSTEM_IPERF_CONNECT_RETRIES * SYSTEM_IPERF_RETRY_DELAY_SEC
    ))

    log_info "Running iperf3 path $label: $source_node -> $target_node ($target_address:$port)"
    srun --overlap --nodes=1 --ntasks=1 -w "$target_node" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}
        exec timeout ${server_timeout} iperf3 -s -p ${q_port} --bind ${q_target_address}
    " >"$server_log" 2>&1 &
    server_pid=$!

    sleep "$SYSTEM_IPERF_READY_SLEEP_SEC"

    if ! kill -0 "$server_pid" >/dev/null 2>&1; then
        wait "$server_pid" >/dev/null 2>&1 || true
        normalize_iperf "$raw_output" "$output_file" "$label" "$source_node" "$target_node" "$target_address" "$port" "failed" "iperf3 server exited before the client started; see logs/system_inventory/${label}.server.log"
        return 0
    fi

    client_status=1
    for ((attempt = 1; attempt <= SYSTEM_IPERF_CONNECT_RETRIES; attempt++)); do
        client_status=0
        srun --overlap --nodes=1 --ntasks=1 -w "$source_node" bash -lc "
            set -euo pipefail
            ${HPC_NODE_ENV_SNIPPET}
            iperf3 -c ${q_target_address} -p ${q_port} -t ${q_seconds} -P ${q_parallel} -J
        " >"$raw_output" 2>"$client_log" || client_status=$?
        if ((client_status == 0)); then
            break
        fi
        if ((attempt == SYSTEM_IPERF_CONNECT_RETRIES)) \
            || ! grep -q "unable to connect to server" "$raw_output"; then
            break
        fi
        log_warn "iperf3 path $label refused connection; retrying ($attempt/$SYSTEM_IPERF_CONNECT_RETRIES)"
        sleep "$SYSTEM_IPERF_RETRY_DELAY_SEC"
    done

    if kill -0 "$server_pid" >/dev/null 2>&1; then
        kill "$server_pid" >/dev/null 2>&1 || true
    fi
    wait "$server_pid" >/dev/null 2>&1 || true

    if (( client_status == 0 )); then
        normalize_iperf "$raw_output" "$output_file" "$label" "$source_node" "$target_node" "$target_address" "$port" "completed" ""
    else
        normalize_iperf "$raw_output" "$output_file" "$label" "$source_node" "$target_node" "$target_address" "$port" "failed" "iperf3 client failed; see logs/system_inventory/${label}.client.log"
    fi
}

compile_ram_probe

for node in "${ALL_NODES[@]}"; do
    collect_one_node "$node"
done

BROKER_NODE="${BROKER_NODES[0]}"
PRODUCER_NODE="$(first_producer_node || true)"
CONSUMER_NODE="$(first_node_for_role consumer || true)"

if [[ -n "$PRODUCER_NODE" ]]; then
    run_iperf_path "producer_to_broker" "$PRODUCER_NODE" "$BROKER_NODE" 0
else
    log_warn "No producer node found for iperf3 producer_to_broker path"
fi

if [[ -n "$CONSUMER_NODE" ]]; then
    run_iperf_path "broker_to_consumer" "$BROKER_NODE" "$CONSUMER_NODE" 1
else
    log_warn "No consumer node found for iperf3 broker_to_consumer path"
fi

SETTINGS_JSON="$(python3 - <<PY
import json
print(json.dumps({
    "interface": "${BENCHMARK_NETWORK_INTERFACE}",
    "iperf_seconds": int("${SYSTEM_IPERF_SECONDS}"),
    "iperf_parallel": int("${SYSTEM_IPERF_PARALLEL}"),
    "iperf_ready_sleep_sec": int("${SYSTEM_IPERF_READY_SLEEP_SEC}"),
    "iperf_connect_retries": int("${SYSTEM_IPERF_CONNECT_RETRIES}"),
    "iperf_retry_delay_sec": int("${SYSTEM_IPERF_RETRY_DELAY_SEC}"),
    "iperf_server_timeout_extra_sec": int("${SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC}"),
    "ram_probe_mb": int("${SYSTEM_RAM_PROBE_MB}"),
    "ram_probe_enabled": "${ENABLE_SYSTEM_RAM_PROBE}" == "1",
    "iperf_enabled": "${ENABLE_SYSTEM_IPERF}" == "1",
    "ram_root": "${KAFKA_RAM_ROOT:-}",
}, sort_keys=True))
PY
)"

python3 -m src.benchmark.system_inventory merge \
    --nodes-dir "$SYSTEM_NODE_DIR" \
    --iperf-dir "$SYSTEM_IPERF_DIR" \
    --output "$SYSTEM_INVENTORY_FILE" \
    --settings-json "$SETTINGS_JSON"

cp -f "$SYSTEM_INVENTORY_FILE" "$SYSTEM_DATA_COPY"
log_info "System inventory written to: $SYSTEM_INVENTORY_FILE"
