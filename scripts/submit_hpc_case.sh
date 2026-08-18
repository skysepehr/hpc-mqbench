#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Submit one MPI benchmark config to Slurm with the V1 four-node role layout.
#
# Default layout:
# - broker node
# - monitoring node
# - producer node, also hosting MPI rank 0
# - consumer node
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT"

CONFIG_PATH="${1:-configs/one_broker_mpi_simultaneous.json}"
REQUESTED_BACKEND_ID="$(python3 - "$CONFIG_PATH" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
print(str(payload.get("backend_id", "kafka")).strip() or "kafka")
PY
)"
if [[ "$REQUESTED_BACKEND_ID" == "pulsar" \
    && -z "${SLURM_HPC_MODULES:-}" \
    && -n "${PULSAR_HPC_MODULES:-}" ]]; then
    export SLURM_HPC_MODULES="$PULSAR_HPC_MODULES"
    export HPC_MODULES="$PULSAR_HPC_MODULES"
fi
export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"
export ENABLE_MONITORING="${ENABLE_MONITORING:-1}"
export ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
export ENABLE_KAFKA_EXPORTER="${ENABLE_KAFKA_EXPORTER:-1}"
export ENABLE_SYSTEM_INVENTORY="${ENABLE_SYSTEM_INVENTORY:-1}"
export ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
export CLEAN_BROKER_RAM_DIRS="${CLEAN_BROKER_RAM_DIRS:-1}"
RUN_PREFLIGHT="${RUN_PREFLIGHT:-1}"
RUN_COMPUTE_PREFLIGHT="${RUN_COMPUTE_PREFLIGHT:-0}"
DRY_RUN="${DRY_RUN:-0}"

validate_positive_integer() {
    local name="${1:?name required}"
    local value="${2:-}"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '[submit-hpc] ERROR: %s must be a positive integer, got: %s\n' \
            "$name" "${value:-<empty>}" >&2
        exit 1
    fi
}

validate_nonnegative_integer() {
    local name="${1:?name required}"
    local value="${2:-}"
    if [[ ! "$value" =~ ^[0-9]+$ ]]; then
        printf '[submit-hpc] ERROR: %s must be a non-negative integer, got: %s\n' \
            "$name" "${value:-<empty>}" >&2
        exit 1
    fi
}

ceil_div() {
    local numerator="${1:?numerator required}"
    local denominator="${2:?denominator required}"
    if (( denominator <= 0 )); then
        printf '[submit-hpc] ERROR: denominator must be positive for ceil_div\n' >&2
        exit 1
    fi
    printf '%s\n' $(( (numerator + denominator - 1) / denominator ))
}

max_int() {
    local max_value=0
    local value
    for value in "$@"; do
        if (( value > max_value )); then
            max_value="$value"
        fi
    done
    printf '%s\n' "$max_value"
}

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi
select_backend_hpc_modules "$REQUESTED_BACKEND_ID"
if [[ -n "${HPC_MODULES:-}" ]]; then
    export SLURM_HPC_MODULES="$HPC_MODULES"
fi
if [[ "$REQUESTED_BACKEND_ID" == "pulsar" ]]; then
    export PULSAR_HOME="${PULSAR_HOME:-$PROJECT_ROOT/.local/pulsar-current}"
fi

if [[ "$DRY_RUN" != "1" ]]; then
    load_hpc_modules
    mkdir -p "$PROJECT_ROOT/logs" "$PROJECT_ROOT/results"
fi

CONFIG_INFO="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$CONFIG_PATH" <<'PY'
import shlex
import sys
from pathlib import Path

from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config

path = Path(sys.argv[1])
config = load_benchmark_config(path)
if config.mode != "single":
    raise SystemExit("The current runner supports only mode='single'")
plan = get_backend(config.backend_id).resource_plan(config)

print(f"BACKEND_ID={shlex.quote(config.backend_id)}")
print(f"MODE={shlex.quote(config.mode)}")
print(f"SERVICE_NODE_COUNT={plan.service_nodes}")
# Compatibility alias retained for existing Kafka submission tooling.
print(f"BROKER_COUNT_MAX={plan.service_nodes}")
print(f"MAX_PRODUCER_RANKS={config.producer_ranks}")
print(f"MAX_CONSUMER_RANKS={config.consumer_ranks}")
print(f"MAX_MPI_RANKS={config.total_mpi_ranks}")
print("ESTIMATED_CASES=1")
PY
)"
eval "$CONFIG_INFO"

if [[ "$BACKEND_ID" != "kafka" ]]; then
    ENABLE_KAFKA_EXPORTER=0
fi

BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
case "$BENCHMARK_NODE_LAYOUT" in
    packed|role_split) ;;
    *)
        printf '[submit-hpc] ERROR: BENCHMARK_NODE_LAYOUT must be packed or role_split, got: %s\n' \
            "$BENCHMARK_NODE_LAYOUT" >&2
        exit 1
        ;;
esac
export BENCHMARK_NODE_LAYOUT

if [[ "$BENCHMARK_NODE_LAYOUT" == "role_split" ]]; then
    if [[ -n "${SLURM_NODES:-}" || -n "${SLURM_BENCHMARK_NODES:-}" ]]; then
        printf '[submit-hpc] ERROR: role_split uses SLURM_CONTROLLER_NODES, SLURM_PRODUCER_NODES, and SLURM_CONSUMER_NODES instead of SLURM_NODES or SLURM_BENCHMARK_NODES\n' >&2
        exit 1
    fi

    SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
    SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-$(( MAX_PRODUCER_RANKS > 0 ? 1 : 0 ))}"
    SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-$(( MAX_CONSUMER_RANKS > 0 ? 1 : 0 ))}"

    validate_nonnegative_integer "SLURM_CONTROLLER_NODES" "$SLURM_CONTROLLER_NODES"
    validate_nonnegative_integer "SLURM_PRODUCER_NODES" "$SLURM_PRODUCER_NODES"
    validate_nonnegative_integer "SLURM_CONSUMER_NODES" "$SLURM_CONSUMER_NODES"

    if (( MAX_PRODUCER_RANKS > 0 && SLURM_PRODUCER_NODES == 0 )); then
        printf '[submit-hpc] ERROR: producer ranks require at least one SLURM_PRODUCER_NODES node\n' >&2
        exit 1
    fi
    if (( MAX_CONSUMER_RANKS > 0 && SLURM_CONSUMER_NODES == 0 )); then
        printf '[submit-hpc] ERROR: consumer ranks require at least one SLURM_CONSUMER_NODES node\n' >&2
        exit 1
    fi
    if (( SLURM_CONTROLLER_NODES == 0 && SLURM_PRODUCER_NODES == 0 )); then
        printf '[submit-hpc] ERROR: colocated MPI controller requires a producer node\n' >&2
        exit 1
    fi

    BENCHMARK_NODE_COUNT=$(( SLURM_CONTROLLER_NODES + SLURM_PRODUCER_NODES + SLURM_CONSUMER_NODES ))
    NODE_COUNT=$(( SERVICE_NODE_COUNT + 1 + BENCHMARK_NODE_COUNT ))

    controller_tasks_per_node=0
    if (( SLURM_CONTROLLER_NODES > 0 )); then
        controller_tasks_per_node="$(ceil_div 1 "$SLURM_CONTROLLER_NODES")"
    fi

    producer_tasks_per_node=0
    consumer_tasks_per_node=0
    if (( MAX_PRODUCER_RANKS > 0 )); then
        producer_tasks_per_node="$(ceil_div "$MAX_PRODUCER_RANKS" "$SLURM_PRODUCER_NODES")"
        if (( SLURM_CONTROLLER_NODES == 0 )); then
            producer_tasks_per_node=$(( producer_tasks_per_node + 1 ))
        fi
    fi
    if (( MAX_CONSUMER_RANKS > 0 )); then
        consumer_tasks_per_node="$(ceil_div "$MAX_CONSUMER_RANKS" "$SLURM_CONSUMER_NODES")"
    fi
    DEFAULT_NTASKS_PER_NODE="$(max_int "$controller_tasks_per_node" "$producer_tasks_per_node" "$consumer_tasks_per_node")"

    export SLURM_CONTROLLER_NODES
    export SLURM_PRODUCER_NODES
    export SLURM_CONSUMER_NODES
else
    if [[ -n "${SLURM_CONTROLLER_NODES:-}" || -n "${SLURM_PRODUCER_NODES:-}" || -n "${SLURM_CONSUMER_NODES:-}" ]]; then
        printf '[submit-hpc] ERROR: SLURM_CONTROLLER_NODES, SLURM_PRODUCER_NODES, and SLURM_CONSUMER_NODES require BENCHMARK_NODE_LAYOUT=role_split\n' >&2
        exit 1
    fi

    NODE_COUNT="${SLURM_NODES:-$(( SERVICE_NODE_COUNT + 2 ))}"
    validate_positive_integer "SLURM_NODES" "$NODE_COUNT"
    BENCHMARK_NODE_COUNT=$(( NODE_COUNT - SERVICE_NODE_COUNT - 1 ))
    DEFAULT_NTASKS_PER_NODE="$(ceil_div "$MAX_MPI_RANKS" "$BENCHMARK_NODE_COUNT")"
fi

SLURM_NTASKS_PER_NODE="${SLURM_NTASKS_PER_NODE:-$DEFAULT_NTASKS_PER_NODE}"
JOB_NAME="${SLURM_JOB_NAME:-${BACKEND_ID}-messaging-benchmark}"
SLURM_TIME="${SLURM_TIME:-00:30:00}"

if [[ "$RUN_PREFLIGHT" == "1" && "$DRY_RUN" != "1" ]]; then
    ./scripts/backend_lifecycle.sh "$BACKEND_ID" preflight "$CONFIG_PATH"
fi

if [[ "$RUN_COMPUTE_PREFLIGHT" == "1" ]]; then
    if [[ -n "${SLURM_JOB_ID:-}" ]]; then
        srun --nodes=1 --ntasks=1 bash -lc \
            'cd "$1" && ./scripts/hpc_prepare_compute_python.sh && ./scripts/hpc_compute_preflight.sh "$2"' \
            bash "$PROJECT_ROOT" "$CONFIG_PATH"
    else
        printf '[submit-hpc] ERROR: RUN_COMPUTE_PREFLIGHT=1 requires an existing Slurm allocation.\n' >&2
        exit 1
    fi
fi

sbatch_args=(
    --nodes="$NODE_COUNT"
    --ntasks-per-node="$SLURM_NTASKS_PER_NODE"
    --job-name="$JOB_NAME"
    --time="$SLURM_TIME"
    --output="$PROJECT_ROOT/logs/slurm-%j.out"
)

if [[ "${SLURM_EXCLUSIVE:-0}" == "1" ]]; then
    sbatch_args+=(--exclusive)
fi
if [[ -n "${SLURM_DEPENDENCY:-}" ]]; then
    sbatch_args+=(--dependency="$SLURM_DEPENDENCY")
fi

sbatch_export_entries=(
    "PROJECT_ROOT=$PROJECT_ROOT"
    "BACKEND_ID=$BACKEND_ID"
    "AUTO_LOAD_GWDG_MODULES=$AUTO_LOAD_GWDG_MODULES"
    "ENABLE_MONITORING=$ENABLE_MONITORING"
    "ENABLE_NODE_EXPORTER=$ENABLE_NODE_EXPORTER"
    "ENABLE_KAFKA_EXPORTER=$ENABLE_KAFKA_EXPORTER"
    "ENABLE_SYSTEM_INVENTORY=$ENABLE_SYSTEM_INVENTORY"
    "ENABLE_BROKER_PROCESS_MONITOR=${ENABLE_BROKER_PROCESS_MONITOR:-1}"
    "ENABLE_RAM_BACKED_RUNTIME=$ENABLE_RAM_BACKED_RUNTIME"
    "BENCHMARK_NODE_LAYOUT=$BENCHMARK_NODE_LAYOUT"
)

for export_name in \
    ALLOW_KAFKA_BROKER_PKILL \
    CLEAN_BROKER_RAM_DIRS \
    CLEAN_STALE_KAFKA_RAM_ROOTS \
    DELETE_TOPIC_FIRST \
    ENABLE_EGRESS_PREFILL \
    FORMAT_STORAGE \
    KAFKA_RAM_CACHE_HOME \
    KAFKA_RAM_ROOT \
    KAFKA_RAM_TMPDIR \
    KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH \
    KAFKA_HOME \
    KAFKA_HPC_NETWORK_INTERFACE \
    KAFKA_HPC_REQUIRE_FABRIC \
    KAFKA_NUM_IO_THREADS \
    KAFKA_NUM_NETWORK_THREADS \
    KAFKA_QUEUED_MAX_REQUESTS \
    KAFKA_SOCKET_RECEIVE_BUFFER_BYTES \
    KAFKA_SOCKET_SEND_BUFFER_BYTES \
    BENCHMARK_NETWORK_INTERFACE \
    BENCHMARK_REQUIRE_FABRIC \
    MPI_PLACEMENT_MODE \
    KAFKA_HPC_MODULES \
    PULSAR_HPC_MODULES \
    PULSAR_JAVA_HOME \
    PULSAR_HOME \
    PULSAR_RAM_ROOT \
    PULSAR_READY_TIMEOUT_SEC \
    SYSTEM_IPERF_PARALLEL \
    SYSTEM_IPERF_PORT_BASE \
    SYSTEM_IPERF_READY_SLEEP_SEC \
    SYSTEM_IPERF_SECONDS \
    SYSTEM_IPERF_SERVER_TIMEOUT_EXTRA_SEC \
    SYSTEM_RAM_PROBE_MB \
    SYSTEM_RAM_PROBE_TIMEOUT_SEC \
    ENABLE_SYSTEM_IPERF \
    ENABLE_SYSTEM_RAM_PROBE \
    MONITORING_GRACE_SEC \
    MONITORING_QUERY_TIMEOUT_SEC \
    MONITORING_RANGE_STEP_SEC \
    JMX_EXPORTER_CONFIG \
    JMX_EXPORTER_JAR \
    KAFKA_EXPORTER_HOME \
    NODE_EXPORTER_HOME \
    NODE_EXPORTER_NODE_SCOPE \
    PROMETHEUS_HOME \
    PROMETHEUS_READY_TIMEOUT_SEC \
    PROMETHEUS_SCRAPE_INTERVAL_SEC \
    SLURM_CONTROLLER_NODES \
    SLURM_CONSUMER_NODES \
    SLURM_HPC_MODULES \
    SLURM_PRODUCER_NODES \
    SOURCE_HPC_ENV_FILE; do
    if [[ ${!export_name+x} ]]; then
        sbatch_export_entries+=("${export_name}=${!export_name}")
    fi
done

for export_name in \
    CASE_DIR_OVERRIDE \
    CASE_ID_OVERRIDE \
    CASE_NAME_OVERRIDE \
    BROKER_PROCESS_MONITOR_INTERVAL_SEC; do
    if [[ ${!export_name+x} ]]; then
        sbatch_export_entries+=("${export_name}=${!export_name}")
    fi
done

SBATCH_EXPORT="$(IFS=,; printf '%s' "${sbatch_export_entries[*]}")"
sbatch_args+=(--export="$SBATCH_EXPORT")

if [[ -n "${SLURM_PARTITION:-}" ]]; then
    sbatch_args+=(--partition="$SLURM_PARTITION")
fi
if [[ -n "${SLURM_ACCOUNT:-}" ]]; then
    sbatch_args+=(--account="$SLURM_ACCOUNT")
fi
if [[ -n "${SLURM_QOS:-}" ]]; then
    sbatch_args+=(--qos="$SLURM_QOS")
fi
if [[ -n "${SLURM_CONSTRAINT:-}" ]]; then
    sbatch_args+=(--constraint="$SLURM_CONSTRAINT")
fi

printf '[submit-hpc] Config: %s\n' "$CONFIG_PATH"
printf '[submit-hpc] Backend: %s, service nodes: %s\n' "$BACKEND_ID" "$SERVICE_NODE_COUNT"
printf '[submit-hpc] Mode: %s, estimated cases: %s\n' "${MODE:-unknown}" "${ESTIMATED_CASES:-unknown}"
printf '[submit-hpc] Nodes: %s\n' "$NODE_COUNT"
printf '[submit-hpc] Max MPI ranks: %s, benchmark nodes: %s, ntasks-per-node: %s\n' \
    "$MAX_MPI_RANKS" "$BENCHMARK_NODE_COUNT" "$SLURM_NTASKS_PER_NODE"
printf '[submit-hpc] Benchmark node layout: %s\n' "$BENCHMARK_NODE_LAYOUT"
if [[ "$BENCHMARK_NODE_LAYOUT" == "role_split" ]]; then
    printf '[submit-hpc] Role split nodes: controller=%s producer=%s consumer=%s\n' \
        "$SLURM_CONTROLLER_NODES" "$SLURM_PRODUCER_NODES" "$SLURM_CONSUMER_NODES"
    if [[ "$SLURM_CONTROLLER_NODES" == "0" ]]; then
        printf '[submit-hpc] MPI rank 0: colocated on first producer node\n'
    fi
fi
printf '[submit-hpc] Monitoring: ENABLE_MONITORING=%s ENABLE_NODE_EXPORTER=%s ENABLE_KAFKA_EXPORTER=%s\n' \
    "$ENABLE_MONITORING" "$ENABLE_NODE_EXPORTER" "$ENABLE_KAFKA_EXPORTER"
printf '[submit-hpc] System inventory: ENABLE_SYSTEM_INVENTORY=%s ENABLE_RAM_BACKED_RUNTIME=%s\n' \
    "$ENABLE_SYSTEM_INVENTORY" "$ENABLE_RAM_BACKED_RUNTIME"
printf '[submit-hpc] Backend network interface: %s\n' "${BENCHMARK_NETWORK_INTERFACE:-${KAFKA_HPC_NETWORK_INTERFACE:-ib0}}"
printf '[submit-hpc] Require fabric interface: %s\n' "${BENCHMARK_REQUIRE_FABRIC:-${KAFKA_HPC_REQUIRE_FABRIC:-1}}"

if [[ "$DRY_RUN" == "1" ]]; then
    printf '[submit-hpc] DRY_RUN=1, not submitting. Command would be:\n'
    printf '  sbatch'
    printf ' %q' "${sbatch_args[@]}" "$PROJECT_ROOT/run_all.sh" "$CONFIG_PATH"
    printf '\n'
    exit 0
fi

sbatch "${sbatch_args[@]}" "$PROJECT_ROOT/run_all.sh" "$CONFIG_PATH"
