#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Run one benchmark case.
#
# This script launches the Python MPI benchmark on BENCHMARK_NODES only.
#
# Responsibilities:
# - validate required environment variables
# - compute total MPI ranks
# - read bootstrap servers
# - create an MPI hostfile from benchmark nodes
# - launch the Python benchmark with mpirun restricted to benchmark nodes
#
# Required environment variables:
# - CONFIG_PATH
# - CASE_ID
# - CASE_NAME
# - CASE_DIR
# - PRODUCER_RANKS
# - CONSUMER_RANKS
# - BENCHMARK_NODES (bash array exported by caller shell context)
#
# Runtime files required:
# - CASE_DIR/runtime/bootstrap_servers.txt
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
# shellcheck source=./python_env_common.sh
source "$SCRIPT_DIR/python_env_common.sh"

require_nonempty "CONFIG_PATH" "${CONFIG_PATH:-}"
require_nonempty "CASE_ID" "${CASE_ID:-}"
require_nonempty "CASE_NAME" "${CASE_NAME:-}"
require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "PRODUCER_RANKS" "${PRODUCER_RANKS:-}"
require_nonempty "CONSUMER_RANKS" "${CONSUMER_RANKS:-}"

require_file "$CONFIG_PATH"
require_command mpirun
require_command python3

BOOTSTRAP_SERVERS_FILE="$CASE_DIR/runtime/bootstrap_servers.txt"
require_file "$BOOTSTRAP_SERVERS_FILE"

BOOTSTRAP_SERVERS="$(<"$BOOTSTRAP_SERVERS_FILE")"
require_nonempty "BOOTSTRAP_SERVERS" "$BOOTSTRAP_SERVERS"

# Rebuild node roles from current allocation so this script does not depend on
# bash arrays being exported across processes.
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
split_nodes "$BROKER_COUNT"

if [[ ${#BENCHMARK_NODES[@]} -eq 0 ]]; then
    die "No benchmark nodes available for MPI run"
fi

# Rank layout:
# - rank 0 = controller
# - producer ranks = PRODUCER_RANKS
# - consumer ranks = CONSUMER_RANKS
TOTAL_MPI_RANKS=$((1 + PRODUCER_RANKS + CONSUMER_RANKS))

if (( TOTAL_MPI_RANKS <= 1 )); then
    die "Invalid total MPI ranks: $TOTAL_MPI_RANKS"
fi

BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-packed}"
case "$BENCHMARK_NODE_LAYOUT" in
    packed|role_split) ;;
    *) die "BENCHMARK_NODE_LAYOUT must be packed or role_split, got: $BENCHMARK_NODE_LAYOUT" ;;
esac

CASE_LOG_DIR="$CASE_DIR/logs"
RUNTIME_DIR="$CASE_DIR/runtime"
HOSTFILE="$RUNTIME_DIR/mpi_hosts.txt"
ALLOCATION_HOSTFILE="$RUNTIME_DIR/mpi_allocation_hosts.txt"
RANKFILE="$RUNTIME_DIR/mpi_rankfile.txt"
MPI_RANKFILE_LAUNCH_DIR="${MPI_RANKFILE_LAUNCH_DIR:-$PROJECT_ROOT/.local/mpi_rankfiles}"
MPI_RANKFILE_LAUNCH_TOKEN="${SLURM_JOB_ID:-$$}"
MPI_RANKFILE_LAUNCH_PATH="$MPI_RANKFILE_LAUNCH_DIR/rankfile_${MPI_RANKFILE_LAUNCH_TOKEN}.txt"
MPI_RANKFILE_ARG="$MPI_RANKFILE_LAUNCH_PATH"
MPI_LOG_FILE="$CASE_LOG_DIR/mpi-run.log"
MPI_PLACEMENT_MODE="${MPI_PLACEMENT_MODE:-rankfile}"

ensure_dir "$CASE_LOG_DIR"
ensure_dir "$RUNTIME_DIR"
ensure_dir "$MPI_RANKFILE_LAUNCH_DIR"

validate_nonnegative_integer_value() {
    local name="${1:?name required}"
    local value="${2:-}"
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        die "$name must be a non-negative integer, got: ${value:-<empty>}"
    fi
}

validate_positive_integer_value() {
    local name="${1:?name required}"
    local value="${2:-}"
    validate_nonnegative_integer_value "$name" "$value"
    if (( value <= 0 )); then
        die "$name must be a positive integer, got: $value"
    fi
}

ceil_div_value() {
    local numerator="${1:?numerator required}"
    local denominator="${2:?denominator required}"
    if (( denominator <= 0 )); then
        die "Cannot divide by zero while building MPI rankfile"
    fi
    printf '%s\n' $(( (numerator + denominator - 1) / denominator ))
}

append_rank_block() {
    local role_name="${1:?role name required}"
    local start_rank="${2:?start rank required}"
    local rank_count="${3:?rank count required}"
    local slots_per_node="${4:?slots per node required}"
    shift 4
    local -a role_nodes=("$@")
    local slot_offset="${APPEND_RANK_SLOT_OFFSET:-0}"

    if (( rank_count == 0 )); then
        return
    fi
    if (( ${#role_nodes[@]} == 0 )); then
        die "No nodes available for $role_name ranks"
    fi

    local emitted=0
    local node slot rank_id
    for node in "${role_nodes[@]}"; do
        for ((slot = 0; slot < slots_per_node; slot++)); do
            if (( emitted >= rank_count )); then
                return
            fi
            rank_id=$(( start_rank + emitted ))
            echo "rank $rank_id=$node slot=$((slot + slot_offset))" >> "$RANKFILE"
            emitted=$((emitted + 1))
        done
    done

    if (( emitted < rank_count )); then
        die "Could not place all $role_name ranks; placed $emitted of $rank_count"
    fi
}

# Build a hostfile for diagnostics and a rankfile for the actual launch.
#
# On GWDG's OpenMPI 5 stack, using a hostfile as a subset filter inside a
# larger Slurm allocation can crash prterun when broker/monitoring nodes are
# excluded from the benchmark hostfile. A rankfile keeps the full allocation
# visible to OpenMPI while placing only benchmark ranks on benchmark nodes.
: > "$HOSTFILE"

: > "$RANKFILE"

if [[ "$BENCHMARK_NODE_LAYOUT" == "role_split" ]]; then
    CONTROLLER_NODE_COUNT="${SLURM_CONTROLLER_NODES:-0}"
    PRODUCER_NODE_COUNT="${SLURM_PRODUCER_NODES:-$(( PRODUCER_RANKS > 0 ? 1 : 0 ))}"
    CONSUMER_NODE_COUNT="${SLURM_CONSUMER_NODES:-$(( CONSUMER_RANKS > 0 ? 1 : 0 ))}"

    validate_nonnegative_integer_value "SLURM_CONTROLLER_NODES" "$CONTROLLER_NODE_COUNT"
    validate_nonnegative_integer_value "SLURM_PRODUCER_NODES" "$PRODUCER_NODE_COUNT"
    validate_nonnegative_integer_value "SLURM_CONSUMER_NODES" "$CONSUMER_NODE_COUNT"

    if (( PRODUCER_RANKS > 0 && PRODUCER_NODE_COUNT == 0 )); then
        die "Producer ranks require at least one producer benchmark node"
    fi
    if (( CONSUMER_RANKS > 0 && CONSUMER_NODE_COUNT == 0 )); then
        die "Consumer ranks require at least one consumer benchmark node"
    fi
    if (( CONTROLLER_NODE_COUNT == 0 && PRODUCER_NODE_COUNT == 0 )); then
        die "Colocating rank 0 requires at least one producer benchmark node"
    fi

    EXPECTED_BENCHMARK_NODES=$(( CONTROLLER_NODE_COUNT + PRODUCER_NODE_COUNT + CONSUMER_NODE_COUNT ))
    if (( ${#BENCHMARK_NODES[@]} != EXPECTED_BENCHMARK_NODES )); then
        die "BENCHMARK_NODE_LAYOUT=role_split expected $EXPECTED_BENCHMARK_NODES benchmark nodes, got ${#BENCHMARK_NODES[@]}"
    fi

    if (( CONTROLLER_NODE_COUNT == 0 )); then
        PRODUCER_NODE_START=0
        CONSUMER_NODE_START="$PRODUCER_NODE_COUNT"
        PRODUCER_NODES=("${BENCHMARK_NODES[@]:PRODUCER_NODE_START:PRODUCER_NODE_COUNT}")
        CONSUMER_NODES=("${BENCHMARK_NODES[@]:CONSUMER_NODE_START:CONSUMER_NODE_COUNT}")
        CONTROLLER_NODES=("${PRODUCER_NODES[0]}")
        CONTROLLER_COLOCATED_WITH_PRODUCER=1
    else
        PRODUCER_NODE_START="$CONTROLLER_NODE_COUNT"
        CONSUMER_NODE_START=$(( CONTROLLER_NODE_COUNT + PRODUCER_NODE_COUNT ))
        CONTROLLER_NODES=("${BENCHMARK_NODES[@]:0:CONTROLLER_NODE_COUNT}")
        PRODUCER_NODES=("${BENCHMARK_NODES[@]:PRODUCER_NODE_START:PRODUCER_NODE_COUNT}")
        CONSUMER_NODES=("${BENCHMARK_NODES[@]:CONSUMER_NODE_START:CONSUMER_NODE_COUNT}")
        CONTROLLER_COLOCATED_WITH_PRODUCER=0
    fi

    CONTROLLER_SLOTS_PER_NODE=1
    if (( CONTROLLER_NODE_COUNT > 0 )); then
        CONTROLLER_SLOTS_PER_NODE="$(ceil_div_value 1 "$CONTROLLER_NODE_COUNT")"
    fi
    PRODUCER_SLOTS_PER_NODE=1
    CONSUMER_SLOTS_PER_NODE=1
    if (( PRODUCER_RANKS > 0 )); then
        PRODUCER_SLOTS_PER_NODE="$(ceil_div_value "$PRODUCER_RANKS" "$PRODUCER_NODE_COUNT")"
    fi
    if (( CONSUMER_RANKS > 0 )); then
        CONSUMER_SLOTS_PER_NODE="$(ceil_div_value "$CONSUMER_RANKS" "$CONSUMER_NODE_COUNT")"
    fi

    for node in "${CONTROLLER_NODES[@]}"; do
        if (( CONTROLLER_COLOCATED_WITH_PRODUCER == 0 )); then
            echo "$node slots=$CONTROLLER_SLOTS_PER_NODE" >> "$HOSTFILE"
        fi
    done
    for node in "${PRODUCER_NODES[@]}"; do
        producer_host_slots="$PRODUCER_SLOTS_PER_NODE"
        if (( CONTROLLER_COLOCATED_WITH_PRODUCER == 1 )); then
            producer_host_slots=$((producer_host_slots + 1))
        fi
        echo "$node slots=$producer_host_slots" >> "$HOSTFILE"
    done
    for node in "${CONSUMER_NODES[@]}"; do
        echo "$node slots=$CONSUMER_SLOTS_PER_NODE" >> "$HOSTFILE"
    done

    APPEND_RANK_SLOT_OFFSET=0 append_rank_block "controller" 0 1 "$CONTROLLER_SLOTS_PER_NODE" "${CONTROLLER_NODES[@]}"
    if (( CONTROLLER_COLOCATED_WITH_PRODUCER == 1 )); then
        APPEND_RANK_SLOT_OFFSET=1 append_rank_block "producer" 1 "$PRODUCER_RANKS" "$PRODUCER_SLOTS_PER_NODE" "${PRODUCER_NODES[@]}"
    else
        APPEND_RANK_SLOT_OFFSET=0 append_rank_block "producer" 1 "$PRODUCER_RANKS" "$PRODUCER_SLOTS_PER_NODE" "${PRODUCER_NODES[@]}"
    fi
    append_rank_block "consumer" "$((1 + PRODUCER_RANKS))" "$CONSUMER_RANKS" "$CONSUMER_SLOTS_PER_NODE" "${CONSUMER_NODES[@]}"
else
    SLOTS_PER_BENCHMARK_NODE=$(( (TOTAL_MPI_RANKS + ${#BENCHMARK_NODES[@]} - 1) / ${#BENCHMARK_NODES[@]} ))
    for node in "${BENCHMARK_NODES[@]}"; do
        echo "$node slots=$SLOTS_PER_BENCHMARK_NODE" >> "$HOSTFILE"
    done

    rank=0
    for node in "${BENCHMARK_NODES[@]}"; do
        for ((slot = 0; slot < SLOTS_PER_BENCHMARK_NODE; slot++)); do
            if (( rank >= TOTAL_MPI_RANKS )); then
                break 2
            fi
            echo "rank $rank=$node slot=$slot" >> "$RANKFILE"
            rank=$((rank + 1))
        done
    done
fi

# Keep every Slurm node visible for the hostfile fallback while assigning
# workload slots only to the benchmark nodes. This avoids treating a subset
# hostfile as a replacement allocation on affected OpenMPI/PRRTE builds.
: > "$ALLOCATION_HOSTFILE"
for node in "${ALL_NODES[@]}"; do
    host_entry="$(awk -v host="$node" '$1 == host { print; exit }' "$HOSTFILE")"
    if [[ -n "$host_entry" ]]; then
        printf '%s\n' "$host_entry" >> "$ALLOCATION_HOSTFILE"
    else
        printf '%s slots=0\n' "$node" >> "$ALLOCATION_HOSTFILE"
    fi
done

# PRRTE's rankfile qualifier rejects some long campaign paths even though the
# file is valid. Keep the complete case-local rankfile for provenance and use a
# short, job-scoped copy solely as the launcher argument.
cp "$RANKFILE" "$MPI_RANKFILE_LAUNCH_PATH"
if command -v realpath >/dev/null 2>&1; then
    if relative_rankfile="$(realpath --relative-to="$PROJECT_ROOT" "$MPI_RANKFILE_LAUNCH_PATH" 2>/dev/null)"; then
        MPI_RANKFILE_ARG="$relative_rankfile"
    fi
fi

write_node_role_metadata() {
    local output_file="$RUNTIME_DIR/node_roles.json"
    local service_map_file="$RUNTIME_DIR/node_service_addresses.tsv"
    local controller_nodes_csv=""
    local producer_nodes_csv=""
    local consumer_nodes_csv=""

    if declare -p CONTROLLER_NODES >/dev/null 2>&1; then
        controller_nodes_csv="$(join_by , "${CONTROLLER_NODES[@]}")"
    fi
    if declare -p PRODUCER_NODES >/dev/null 2>&1; then
        producer_nodes_csv="$(join_by , "${PRODUCER_NODES[@]}")"
    fi
    if declare -p CONSUMER_NODES >/dev/null 2>&1; then
        consumer_nodes_csv="$(join_by , "${CONSUMER_NODES[@]}")"
    fi

    python3 - \
        "$output_file" \
        "$service_map_file" \
        "$RANKFILE" \
        "$(join_by , "${ALL_NODES[@]}")" \
        "$(join_by , "${BROKER_NODES[@]}")" \
        "$MONITORING_NODE" \
        "$controller_nodes_csv" \
        "$producer_nodes_csv" \
        "$consumer_nodes_csv" \
        "$BENCHMARK_NODE_LAYOUT" \
        "${CONTROLLER_COLOCATED_WITH_PRODUCER:-0}" \
        "$PRODUCER_RANKS" \
        "$CONSUMER_RANKS" \
        "${KAFKA_JMX_PORT_BASE:-7101}" \
        "${NODE_EXPORTER_PORT:-9100}" \
        "${PROMETHEUS_PORT:-9090}" \
        "${KAFKA_EXPORTER_PORT:-9308}" \
        "${ENABLE_NODE_EXPORTER:-1}" \
        "${ENABLE_KAFKA_EXPORTER:-1}" \
        "${NODE_EXPORTER_NODE_SCOPE:-all}" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path


(
    output_file,
    service_map_file,
    rankfile,
    all_nodes_csv,
    broker_nodes_csv,
    monitoring_node,
    controller_nodes_csv,
    producer_nodes_csv,
    consumer_nodes_csv,
    layout,
    controller_colocated,
    producer_ranks,
    consumer_ranks,
    jmx_port_base,
    node_exporter_port,
    prometheus_port,
    kafka_exporter_port,
    enable_node_exporter,
    enable_kafka_exporter,
    node_exporter_scope,
) = sys.argv[1:]


def csv_nodes(value: str) -> list[str]:
    return [item for item in value.split(",") if item]


def read_service_addresses(path: str) -> dict[str, str]:
    addresses: dict[str, str] = {}
    file_path = Path(path)
    if not file_path.is_file():
        return addresses
    for line in file_path.read_text(encoding="utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0]:
            addresses[parts[0]] = parts[1]
    return addresses


def rank_role(rank: int) -> str:
    if rank == 0:
        return "controller"
    producer_count = int(producer_ranks)
    if 1 <= rank <= producer_count:
        return "producer"
    if 1 + producer_count <= rank <= producer_count + int(consumer_ranks):
        return "consumer"
    return "unknown"


def read_ranks(path: str) -> dict[str, list[dict[str, int | str]]]:
    by_node: dict[str, list[dict[str, int | str]]] = {}
    file_path = Path(path)
    if not file_path.is_file():
        return by_node
    pattern = re.compile(r"^rank\s+(\d+)=(\S+)\s+slot=(\d+)")
    for line in file_path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        rank = int(match.group(1))
        node = match.group(2)
        slot = int(match.group(3))
        by_node.setdefault(node, []).append(
            {"rank": rank, "slot": slot, "role": rank_role(rank)}
        )
    for ranks in by_node.values():
        ranks.sort(key=lambda item: int(item["rank"]))
    return by_node


all_nodes = csv_nodes(all_nodes_csv)
broker_nodes = csv_nodes(broker_nodes_csv)
controller_nodes = csv_nodes(controller_nodes_csv)
producer_nodes = csv_nodes(producer_nodes_csv)
consumer_nodes = csv_nodes(consumer_nodes_csv)
service_addresses = read_service_addresses(service_map_file)
ranks_by_node = read_ranks(rankfile)


def node_roles(node: str) -> list[str]:
    roles: list[str] = []
    if node in broker_nodes:
        roles.append("broker")
    if node == monitoring_node:
        roles.append("monitoring")
    if node in controller_nodes:
        roles.append("controller")
    if node in producer_nodes:
        roles.append("producer")
    if node in consumer_nodes:
        roles.append("consumer")
    if not roles and node in all_nodes:
        roles.append("benchmark")
    return roles or ["unknown"]


def primary_role(roles: list[str]) -> str:
    if "broker" in roles:
        return "broker"
    if "monitoring" in roles:
        return "monitoring"
    if "controller" in roles and "producer" in roles:
        return "producer_controller"
    for role in ("controller", "producer", "consumer", "benchmark"):
        if role in roles:
            return role
    return roles[0] if roles else "unknown"


def node_exporter_enabled(node: str, roles: list[str]) -> bool:
    if enable_node_exporter != "1":
        return False
    scope = node_exporter_scope
    if scope == "all":
        return True
    if scope in {"broker", "broker_only"}:
        return "broker" in roles
    if scope == "broker_and_benchmark":
        return "broker" in roles or any(
            role in roles for role in ("controller", "producer", "consumer", "benchmark")
        )
    if scope in {"benchmark", "benchmark_only"}:
        return any(
            role in roles for role in ("controller", "producer", "consumer", "benchmark")
        )
    return False


nodes = []
for node in all_nodes:
    address = service_addresses.get(node, node)
    roles = node_roles(node)
    exporters = []
    if "broker" in roles:
        broker_index = broker_nodes.index(node)
        exporters.append(
            {
                "name": "jmx",
                "job": "kafka_jmx",
                "endpoint": f"{address}:{int(jmx_port_base) + broker_index}",
            }
        )
    if node_exporter_enabled(node, roles):
        exporters.append(
            {
                "name": "node",
                "job": "node_exporter",
                "endpoint": f"{address}:{node_exporter_port}",
            }
        )
    if node == monitoring_node:
        exporters.append(
            {
                "name": "prometheus",
                "job": "prometheus",
                "endpoint": f"{address}:{prometheus_port}",
            }
        )
        if enable_kafka_exporter == "1":
            exporters.append(
                {
                    "name": "kafka",
                    "job": "kafka_exporter",
                    "endpoint": f"{address}:{kafka_exporter_port}",
                }
            )

    nodes.append(
        {
            "node": node,
            "service_address": address,
            "roles": roles,
            "primary_role": primary_role(roles),
            "mpi_ranks": ranks_by_node.get(node, []),
            "exporters": exporters,
        }
    )

rank_roles = [
    {"rank": rank, "role": rank_role(rank)}
    for rank in range(0, 1 + int(producer_ranks) + int(consumer_ranks))
]

payload = {
    "format": "node_roles.v1",
    "layout": layout,
    "controller_colocated_with_producer": controller_colocated == "1",
    "producer_ranks": int(producer_ranks),
    "consumer_ranks": int(consumer_ranks),
    "nodes": nodes,
    "rank_roles": rank_roles,
}

Path(output_file).write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY

    log_info "Node role metadata written to: $output_file"
}

write_node_role_metadata

log_info "Launching benchmark case"
log_info "Case ID: $CASE_ID"
log_info "Case name: $CASE_NAME"
log_info "Config path: $CONFIG_PATH"
log_info "Bootstrap servers: $BOOTSTRAP_SERVERS"
log_info "Benchmark nodes: $(join_by , "${BENCHMARK_NODES[@]}")"
log_info "Total MPI ranks: $TOTAL_MPI_RANKS"
log_info "Benchmark node layout: $BENCHMARK_NODE_LAYOUT"
if [[ "$BENCHMARK_NODE_LAYOUT" == "role_split" ]]; then
    log_info "Controller nodes: $(join_by , "${CONTROLLER_NODES[@]}")"
    log_info "Producer nodes: $(join_by , "${PRODUCER_NODES[@]}")"
    log_info "Consumer nodes: $(join_by , "${CONSUMER_NODES[@]}")"
    if (( CONTROLLER_COLOCATED_WITH_PRODUCER == 1 )); then
        log_info "MPI rank 0 is colocated on the first producer node"
    fi
    log_info "Role slots per node: controller=$CONTROLLER_SLOTS_PER_NODE producer=$PRODUCER_SLOTS_PER_NODE consumer=$CONSUMER_SLOTS_PER_NODE"
else
    log_info "MPI slots per benchmark node: $SLOTS_PER_BENCHMARK_NODE"
fi
log_info "MPI hostfile: $HOSTFILE"
log_info "MPI allocation hostfile: $ALLOCATION_HOSTFILE"
log_info "MPI rankfile: $RANKFILE"
log_info "MPI rankfile launcher copy: $MPI_RANKFILE_LAUNCH_PATH"
log_info "MPI rankfile launch argument: $MPI_RANKFILE_ARG"
log_info "MPI placement mode: $MPI_PLACEMENT_MODE"

# Launch from the repository root so `python3 -m src.benchmark.main` can resolve
# src/models plus optional repo-local Python packages. On HPC, a system or module
# install can still take precedence by setting KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1.
export_kafka_hpc_python_env "$PROJECT_ROOT"
cd "$PROJECT_ROOT"

MPI_ENV_ARGS=()
for env_name in \
    KAFKA_RAM_ROOT \
    TMPDIR \
    TMP \
    TEMP \
    XDG_CACHE_HOME \
    KAFKA_CLIENT_RAM_DIR; do
    if [[ -n "${!env_name:-}" ]]; then
        MPI_ENV_ARGS+=("-x" "$env_name")
    fi
done

# OpenMPI 5.0.6 / PRRTE 3.0.7 builds can reject the documented rankfile file
# qualifier. The explicit fallback preserves the full Slurm allocation and
# maps ranks only to hosts with positive slot counts.
MPI_PLACEMENT_ARGS=()
case "$MPI_PLACEMENT_MODE" in
    rankfile)
        MPI_PLACEMENT_ARGS=(--map-by "rankfile:file=$MPI_RANKFILE_ARG")
        ;;
    allocation_hostfile_slots)
        MPI_PLACEMENT_ARGS=(--hostfile "$ALLOCATION_HOSTFILE" --map-by slot)
        ;;
    *)
        die "MPI_PLACEMENT_MODE must be rankfile or allocation_hostfile_slots, got: $MPI_PLACEMENT_MODE"
        ;;
esac

mpirun \
    "${MPI_ENV_ARGS[@]}" \
    "${MPI_PLACEMENT_ARGS[@]}" \
    -np "$TOTAL_MPI_RANKS" \
    python3 -m src.benchmark.main \
    --config "$CONFIG_PATH" \
    --case-id "$CASE_ID" \
    --case-name "$CASE_NAME" \
    --output-dir "$CASE_DIR" \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --report-mode "${BENCHMARK_REPORT_MODE:-full}" \
    2>&1 | tee "$MPI_LOG_FILE"

log_info "Benchmark case finished"
log_info "MPI log written to: $MPI_LOG_FILE"
