#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Common shell helpers for the Kafka HPC benchmark framework.
#
# This file is meant to be sourced by other shell scripts, for example:
#   source ./scripts/common.sh
#
# Responsibilities:
# - common logging helpers
# - error helpers
# - path setup helpers
# - Slurm node parsing helpers
# - simple validation helpers
# -----------------------------------------------------------------------------

# Print an informational message with timestamp.
log_info() {
    local msg="${1:-}"
    printf '[INFO] [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg"
}

# Print a warning message with timestamp.
log_warn() {
    local msg="${1:-}"
    printf '[WARN] [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" >&2
}

# Print an error message with timestamp.
log_error() {
    local msg="${1:-}"
    printf '[ERROR] [%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$msg" >&2
}

# Print an error and exit immediately.
die() {
    local msg="${1:-Unknown error}"
    log_error "$msg"
    exit 1
}

# Ensure a directory exists.
ensure_dir() {
    local dir="${1:?directory path is required}"
    mkdir -p "$dir"
}

# Ensure a required command exists in PATH.
require_command() {
    local cmd="${1:?command name is required}"
    command -v "$cmd" >/dev/null 2>&1 || die "Required command not found: $cmd"
}

# Validate that a file exists.
require_file() {
    local file="${1:?file path is required}"
    [[ -f "$file" ]] || die "Required file not found: $file"
}

# Validate that a variable is non-empty.
require_nonempty() {
    local name="${1:?variable name is required}"
    local value="${2:-}"
    [[ -n "$value" ]] || die "Required value is empty: $name"
}

# Wait until a TCP endpoint accepts connections.
wait_for_tcp_endpoint() {
    local host="${1:?host is required}"
    local port="${2:?port is required}"
    local timeout_sec="${3:-30}"
    local label="${4:-${host}:${port}}"
    local elapsed=0

    require_command python3

    while (( elapsed < timeout_sec )); do
        if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
with socket.create_connection((host, port), timeout=1.0):
    pass
PY
        then
            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    die "Timed out waiting for ${label} at ${host}:${port}"
}

# Return the absolute path of a file or directory.
abspath() {
    local path="${1:?path is required}"
    python3 - <<PY
from pathlib import Path
print(Path("$path").resolve())
PY
}

# -----------------------------------------------------------------------------
# Slurm / node helpers
# -----------------------------------------------------------------------------

# Expand the node list from Slurm into one hostname per line.
#
# Example:
#   get_allocated_nodes
#
# Requires SLURM_JOB_NODELIST to be set.
get_allocated_nodes() {
    require_nonempty "SLURM_JOB_NODELIST" "${SLURM_JOB_NODELIST:-}"
    scontrol show hostnames "$SLURM_JOB_NODELIST"
}

# Read allocated nodes into a bash array.
#
# Example:
#   mapfile -t ALL_NODES < <(get_allocated_nodes)
load_allocated_nodes_into_array() {
    get_allocated_nodes
}

# Split allocated nodes into:
# - backend service nodes
# - one monitoring node
# - remaining benchmark nodes
#
# Usage:
#   split_nodes "$BROKER_COUNT"
#
# Output variables created globally:
#   ALL_NODES
#   BROKER_NODES
#   MONITORING_NODE
#   BENCHMARK_NODES
split_nodes() {
    local service_count="${1:?service_count is required}"

    mapfile -t ALL_NODES < <(get_allocated_nodes)

    local total_nodes="${#ALL_NODES[@]}"
    local minimum_needed=$(( service_count + 1 + 1 ))
    # minimum_needed means:
    #   service_count backend service nodes
    #   1 monitoring node
    #   at least 1 benchmark node

    if (( total_nodes < minimum_needed )); then
        die "Not enough allocated nodes. Need at least $minimum_needed, got $total_nodes"
    fi

    SERVICE_NODES=()
    for ((i=0; i<service_count; i++)); do
        SERVICE_NODES+=("${ALL_NODES[$i]}")
    done
    # Compatibility alias used by the existing Kafka lifecycle scripts.
    BROKER_NODES=("${SERVICE_NODES[@]}")

    MONITORING_NODE="${ALL_NODES[$service_count]}"

    BENCHMARK_NODES=()
    for ((i=service_count+1; i<total_nodes; i++)); do
        BENCHMARK_NODES+=("${ALL_NODES[$i]}")
    done
}

# Select which allocated nodes should run node_exporter.
#
# NODE_EXPORTER_NODE_SCOPE controls the target set:
# - all: broker, monitoring, and benchmark nodes
# - broker_and_benchmark: broker plus benchmark nodes
# - broker: broker nodes only
# - benchmark: benchmark nodes only
#
# The default is all so reports can show whether producer/consumer/controller
# nodes are network or CPU bottlenecks, not only the broker nodes.
select_node_exporter_nodes() {
    local scope="${NODE_EXPORTER_NODE_SCOPE:-all}"
    NODE_EXPORTER_NODES=()

    case "$scope" in
        all)
            NODE_EXPORTER_NODES=("${ALL_NODES[@]}")
            ;;
        broker_and_benchmark)
            NODE_EXPORTER_NODES=("${BROKER_NODES[@]}" "${BENCHMARK_NODES[@]}")
            ;;
        broker|broker_only)
            NODE_EXPORTER_NODES=("${BROKER_NODES[@]}")
            ;;
        benchmark|benchmark_only)
            NODE_EXPORTER_NODES=("${BENCHMARK_NODES[@]}")
            ;;
        *)
            die "Unsupported NODE_EXPORTER_NODE_SCOPE: $scope"
            ;;
    esac

    if (( ${#NODE_EXPORTER_NODES[@]} == 0 )); then
        die "NODE_EXPORTER_NODE_SCOPE=$scope selected no nodes"
    fi
}

# Join array values with a delimiter.
#
# Example:
#   joined="$(join_by , "${BROKER_NODES[@]}")"
join_by() {
    local delimiter="${1:?delimiter is required}"
    shift
    local first="${1:-}"
    shift || true
    printf '%s' "$first"
    for item in "$@"; do
        printf '%s%s' "$delimiter" "$item"
    done
}

node_in_array() {
    local needle="${1:?node is required}"
    shift || true

    local item
    for item in "$@"; do
        if [[ "$item" == "$needle" ]]; then
            return 0
        fi
    done
    return 1
}

benchmark_node_role_label() {
    local node="${1:?node is required}"

    if node_in_array "$node" "${BROKER_NODES[@]}"; then
        if [[ "${BACKEND_ID:-kafka}" == "kafka" ]]; then
            printf 'broker\n'
        else
            printf '%s_service\n' "${BACKEND_ID}"
        fi
        return 0
    fi

    if [[ "$node" == "${MONITORING_NODE:-}" ]]; then
        printf 'monitoring\n'
        return 0
    fi

    if [[ "${BENCHMARK_NODE_LAYOUT:-role_split}" != "role_split" ]]; then
        if node_in_array "$node" "${BENCHMARK_NODES[@]}"; then
            printf 'benchmark\n'
            return 0
        fi
        printf 'unknown\n'
        return 0
    fi

    local producer_ranks="${PRODUCER_RANKS:-0}"
    local consumer_ranks="${CONSUMER_RANKS:-0}"
    local controller_node_count="${SLURM_CONTROLLER_NODES:-0}"
    local producer_node_count="${SLURM_PRODUCER_NODES:-$(( producer_ranks > 0 ? 1 : 0 ))}"
    local consumer_node_count="${SLURM_CONSUMER_NODES:-$(( consumer_ranks > 0 ? 1 : 0 ))}"
    local producer_node_start consumer_node_start
    local -a controller_nodes producer_nodes consumer_nodes

    if (( controller_node_count == 0 )); then
        producer_node_start=0
        consumer_node_start="$producer_node_count"
        producer_nodes=("${BENCHMARK_NODES[@]:producer_node_start:producer_node_count}")
        consumer_nodes=("${BENCHMARK_NODES[@]:consumer_node_start:consumer_node_count}")
        controller_nodes=()
        if (( ${#producer_nodes[@]} > 0 )); then
            controller_nodes=("${producer_nodes[0]}")
        fi
    else
        producer_node_start="$controller_node_count"
        consumer_node_start=$(( controller_node_count + producer_node_count ))
        controller_nodes=("${BENCHMARK_NODES[@]:0:controller_node_count}")
        producer_nodes=("${BENCHMARK_NODES[@]:producer_node_start:producer_node_count}")
        consumer_nodes=("${BENCHMARK_NODES[@]:consumer_node_start:consumer_node_count}")
    fi

    if node_in_array "$node" "${controller_nodes[@]}" && node_in_array "$node" "${producer_nodes[@]}"; then
        printf 'producer_controller\n'
    elif node_in_array "$node" "${controller_nodes[@]}"; then
        printf 'controller\n'
    elif node_in_array "$node" "${producer_nodes[@]}"; then
        printf 'producer\n'
    elif node_in_array "$node" "${consumer_nodes[@]}"; then
        printf 'consumer\n'
    elif node_in_array "$node" "${BENCHMARK_NODES[@]}"; then
        printf 'benchmark\n'
    else
        printf 'unknown\n'
    fi
}

# Resolve the address backend and monitoring clients should use for a Slurm node.
#
# Slurm still needs the scheduler hostname for srun placement, but benchmark
# clients should use the data-path interface. On GWDG Emmy P2/P3 this is usually
# ib0 with a 10.246.x.x IP address. The BENCHMARK_* names are preferred; the
# historical KAFKA_HPC_* names remain accepted.
resolve_node_service_address() {
    local node="${1:?node is required}"
    local interface="${2:-${BENCHMARK_NETWORK_INTERFACE:-${KAFKA_HPC_NETWORK_INTERFACE:-ib0}}}"
    local require_fabric="${BENCHMARK_REQUIRE_FABRIC:-${KAFKA_HPC_REQUIRE_FABRIC:-1}}"
    local quoted_interface address

    case "$interface" in
        ""|host|hostname|none|default)
            printf '%s\n' "$node"
            return 0
            ;;
    esac

    require_command srun
    printf -v quoted_interface '%q' "$interface"

    address="$(
        srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc \
            "iface=${quoted_interface}; ip -4 -o addr show dev \"\$iface\" 2>/dev/null | while read -r _ _ _ cidr _; do printf '%s\n' \"\${cidr%%/*}\"; break; done" \
            2>/dev/null || true
    )"

    if [[ -z "$address" ]]; then
        if [[ "$require_fabric" == "1" ]]; then
            die "Could not resolve interface ${interface} on node ${node}. Set BENCHMARK_NETWORK_INTERFACE=hostname or BENCHMARK_REQUIRE_FABRIC=0 only if you intentionally want the slower hostname/LAN path."
        fi

        log_warn "Could not resolve interface ${interface} on node ${node}; falling back to hostname because BENCHMARK_REQUIRE_FABRIC=0"
        printf '%s\n' "$node"
        return 0
    fi

    printf '%s\n' "$address"
}

write_node_service_address_map() {
    local output_file="${1:?output file is required}"
    shift

    local node address
    : > "$output_file"
    for node in "$@"; do
        address="$(resolve_node_service_address "$node")"
        printf '%s\t%s\n' "$node" "$address" >> "$output_file"
    done
}

# Build a comma-separated bootstrap servers string from broker nodes.
#
# Example output:
#   192.0.2.10:9092,192.0.2.11:9092
build_bootstrap_servers() {
    local port="${1:-9092}"

    if [[ ${#BROKER_NODES[@]} -eq 0 ]]; then
        die "BROKER_NODES is empty. Did you call split_nodes?"
    fi

    local servers=()
    local node address
    for node in "${BROKER_NODES[@]}"; do
        address="$(resolve_node_service_address "$node")"
        servers+=("${address}:${port}")
    done

    join_by "," "${servers[@]}"
}

# -----------------------------------------------------------------------------
# Benchmark / config helpers
# -----------------------------------------------------------------------------

# Validate supported broker count.
validate_broker_count() {
    local broker_count="${1:?broker_count is required}"
    case "$broker_count" in
        1) ;;
        *) die "Unsupported broker count: $broker_count. V1 supports only one broker." ;;
    esac
}

# Validate replication factor against broker count.
validate_replication_factor() {
    local replication_factor="${1:?replication_factor is required}"
    local broker_count="${2:?broker_count is required}"

    if (( replication_factor > broker_count )); then
        die "replication_factor ($replication_factor) cannot exceed broker_count ($broker_count)"
    fi
}

# Create a per-case directory layout.
#
# Input:
#   CASE_DIR
#
# Output directories created:
#   data/       machine-readable JSON, snapshots, and validation files
#   reports/    Markdown reports and report graph assets
#   logs/       command, MPI, broker, and monitoring logs
#   monitoring/ raw/csv/graph monitoring bundles when enabled
#   runtime/    temporary files needed while the case is running
prepare_case_directories() {
    local case_dir="${1:?case_dir is required}"

    ensure_dir "$case_dir"
    ensure_dir "$case_dir/data"
    ensure_dir "$case_dir/logs"
    ensure_dir "$case_dir/reports"
    ensure_dir "$case_dir/monitoring"
    ensure_dir "$case_dir/runtime"
}

# Write a small environment snapshot for debugging.
write_env_snapshot() {
    local output_file="${1:?output file is required}"

    {
        echo "HOSTNAME=$(hostname)"
        echo "PWD=$(pwd)"
        echo "DATE=$(date '+%Y-%m-%d %H:%M:%S')"
        echo "SLURM_JOB_ID=${SLURM_JOB_ID:-}"
        echo "SLURM_JOB_NODELIST=${SLURM_JOB_NODELIST:-}"
        echo "SLURM_NNODES=${SLURM_NNODES:-}"
        echo "ENABLE_MONITORING=${ENABLE_MONITORING:-}"
        echo "ENABLE_NODE_EXPORTER=${ENABLE_NODE_EXPORTER:-}"
        echo "NODE_EXPORTER_NODE_SCOPE=${NODE_EXPORTER_NODE_SCOPE:-}"
        echo "ENABLE_KAFKA_EXPORTER=${ENABLE_KAFKA_EXPORTER:-}"
        echo "ENABLE_SYSTEM_INVENTORY=${ENABLE_SYSTEM_INVENTORY:-}"
        echo "ENABLE_RAM_BACKED_RUNTIME=${ENABLE_RAM_BACKED_RUNTIME:-}"
        echo "KAFKA_RAM_ROOT=${KAFKA_RAM_ROOT:-}"
        echo "KAFKA_RAM_TMPDIR=${KAFKA_RAM_TMPDIR:-}"
        echo "KAFKA_RAM_CACHE_HOME=${KAFKA_RAM_CACHE_HOME:-}"
        echo "TMPDIR=${TMPDIR:-}"
        echo "XDG_CACHE_HOME=${XDG_CACHE_HOME:-}"
        echo "KAFKA_CLIENT_RAM_DIR=${KAFKA_CLIENT_RAM_DIR:-}"
        echo "BENCHMARK_NODE_LAYOUT=${BENCHMARK_NODE_LAYOUT:-}"
        echo "SLURM_BENCHMARK_NODES=${SLURM_BENCHMARK_NODES:-}"
        echo "SLURM_CONTROLLER_NODES=${SLURM_CONTROLLER_NODES:-}"
        echo "SLURM_PRODUCER_NODES=${SLURM_PRODUCER_NODES:-}"
        echo "SLURM_CONSUMER_NODES=${SLURM_CONSUMER_NODES:-}"
        echo "BACKEND_ID=${BACKEND_ID:-kafka}"
        echo "HPC_MODULES=${HPC_MODULES:-}"
        echo "SLURM_HPC_MODULES=${SLURM_HPC_MODULES:-}"
        echo "KAFKA_HPC_MODULES=${KAFKA_HPC_MODULES:-}"
        echo "PULSAR_HPC_MODULES=${PULSAR_HPC_MODULES:-}"
        echo "PULSAR_JAVA_HOME=${PULSAR_JAVA_HOME:-}"
        echo "KAFKA_HOME=${KAFKA_HOME:-}"
        echo "PULSAR_HOME=${PULSAR_HOME:-}"
        echo "PULSAR_PROFILE_ID=${PULSAR_PROFILE_ID:-}"
        echo "PULSAR_PROFILE_SHA256=${PULSAR_PROFILE_SHA256:-}"
        echo "PULSAR_PRODUCT_VERSION=${PULSAR_PRODUCT_VERSION:-}"
        echo "PULSAR_MEM=${PULSAR_MEM:-}"
        echo "PULSAR_RAM_ROOT=${PULSAR_RAM_ROOT:-}"
        echo "BENCHMARK_NETWORK_INTERFACE=${BENCHMARK_NETWORK_INTERFACE:-}"
        echo "BENCHMARK_REQUIRE_FABRIC=${BENCHMARK_REQUIRE_FABRIC:-}"
        echo "KAFKA_HPC_NETWORK_INTERFACE=${KAFKA_HPC_NETWORK_INTERFACE:-}"
        echo "KAFKA_HPC_REQUIRE_FABRIC=${KAFKA_HPC_REQUIRE_FABRIC:-}"
        echo "PROMETHEUS_HOME=${PROMETHEUS_HOME:-}"
    } > "$output_file"
}

# Build a shell snippet for commands launched through srun on compute nodes.
#
# Slurm can execute helper commands in a lean environment that does not include
# the modules loaded by the batch shell. Service launchers use this prelude so
# backend services, Prometheus, and exporters see the same repo-local environment and
# compute-node module stack as the main job.
build_hpc_node_env_snippet() {
    local project_root="${1:?project root is required}"
    local quoted_root quoted_env quoted_modules quoted_python_env

    printf -v quoted_root '%q' "$project_root"
    printf -v quoted_env '%q' "$project_root/.local/hpc_env.sh"
    printf -v quoted_modules '%q' "$project_root/scripts/hpc_modules.sh"
    printf -v quoted_python_env '%q' "$project_root/scripts/python_env_common.sh"

    cat <<EOF
cd ${quoted_root}
if [[ -f ${quoted_env} ]]; then
    source ${quoted_env}
fi
if [[ -n "\${SLURM_HPC_MODULES:-}" ]]; then
    export HPC_MODULES="\${SLURM_HPC_MODULES}"
fi
source ${quoted_modules}
select_backend_hpc_modules "\${BACKEND_ID:-kafka}"
load_hpc_modules
source ${quoted_python_env}
export_benchmark_hpc_python_env ${quoted_root}
if [[ -n "\${KAFKA_RAM_ROOT:-}" ]]; then
    export TMPDIR="\${KAFKA_RAM_TMPDIR:-\${KAFKA_RAM_ROOT}/tmp}"
    export TMP="\${TMPDIR}"
    export TEMP="\${TMPDIR}"
    export XDG_CACHE_HOME="\${KAFKA_RAM_CACHE_HOME:-\${KAFKA_RAM_ROOT}/cache}"
    export KAFKA_CLIENT_RAM_DIR="\${KAFKA_CLIENT_RAM_DIR:-\${KAFKA_RAM_ROOT}/clients}"
    mkdir -p "\${TMPDIR}" "\${XDG_CACHE_HOME}" "\${KAFKA_CLIENT_RAM_DIR}" 2>/dev/null || true
fi
EOF
}
