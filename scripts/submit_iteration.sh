#!/usr/bin/env bash
set -euo pipefail

# Submit one ordinary benchmark iteration as a single Slurm allocation.
# The same four nodes run, in order:
# 1. ingress-only
# 2. egress-only
# 3. simultaneous

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT"

ITERATION_NAME="${1:-ordinary_no_compression}"
DRY_RUN="${DRY_RUN:-0}"
RUN_PREFLIGHT="${RUN_PREFLIGHT:-1}"

sanitize_name() {
    local value="${1:?name required}"
    value="$(printf '%s' "$value" | tr -c 'A-Za-z0-9_.-' '-')"
    printf '%s\n' "${value:0:80}"
}

validate_positive_integer() {
    local name="${1:?name required}"
    local value="${2:-}"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '[submit-iteration] ERROR: %s must be a positive integer, got: %s\n' \
            "$name" "${value:-<empty>}" >&2
        exit 1
    fi
}

ceil_div() {
    local numerator="${1:?numerator required}"
    local denominator="${2:?denominator required}"
    if (( denominator <= 0 )); then
        printf '[submit-iteration] ERROR: denominator must be positive for ceil_div\n' >&2
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

ITERATION_NAME="$(sanitize_name "$ITERATION_NAME")"
JOB_NAME="${SLURM_JOB_NAME:-kafka-simple-${ITERATION_NAME}}"
JOB_NAME="$(sanitize_name "$JOB_NAME")"

configs=(
    "configs/one_broker_mpi_ingress_only.json"
    "configs/one_broker_mpi_egress_only.json"
    "configs/one_broker_mpi_simultaneous.json"
)

labels=(
    "ingress-only"
    "egress-only"
    "simultaneous"
)

for config_path in "${configs[@]}"; do
    if [[ ! -f "$config_path" ]]; then
        printf '[submit-iteration] ERROR: missing config: %s\n' "$config_path" >&2
        exit 1
    fi
done

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

if [[ "$DRY_RUN" != "1" ]]; then
    load_hpc_modules
    mkdir -p "$PROJECT_ROOT/logs" "$PROJECT_ROOT/results"
fi

CONFIG_INFO="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "${configs[@]}" <<'PY'
import shlex
import sys
from pathlib import Path

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from src.benchmark.config_loader import load_config

configs = []
for raw_path in sys.argv[1:]:
    path = Path(raw_path)
    raw = load_config(path)
    config = BenchmarkConfig(**benchmark_config_input_dict(raw))
    if config.mode != "single":
        raise SystemExit(f"{path}: kafka-simple-benchmark V1 supports only mode='single'")
    if config.broker_count != 1:
        raise SystemExit(f"{path}: kafka-simple-benchmark V1 supports only broker_count=1")
    configs.append((path, config))

print("MODE=single")
print(f"BROKER_COUNT_MAX={max(config.broker_count for _, config in configs)}")
print(f"MAX_PRODUCER_RANKS={max(config.producer_ranks for _, config in configs)}")
print(f"MAX_CONSUMER_RANKS={max(config.consumer_ranks for _, config in configs)}")
print(f"MAX_MPI_RANKS={max(config.total_mpi_ranks for _, config in configs)}")
print(f"ESTIMATED_CASES={len(configs)}")
print("SCENARIOS=" + shlex.quote(",".join(config.scenario for _, config in configs)))
PY
)"
eval "$CONFIG_INFO"

BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
if [[ "$BENCHMARK_NODE_LAYOUT" != "role_split" ]]; then
    printf '[submit-iteration] ERROR: same-node iteration requires BENCHMARK_NODE_LAYOUT=role_split\n' >&2
    exit 1
fi
export BENCHMARK_NODE_LAYOUT

SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"

validate_positive_integer "SLURM_PRODUCER_NODES" "$SLURM_PRODUCER_NODES"
validate_positive_integer "SLURM_CONSUMER_NODES" "$SLURM_CONSUMER_NODES"
if [[ ! "$SLURM_CONTROLLER_NODES" =~ ^[0-9]+$ ]]; then
    printf '[submit-iteration] ERROR: SLURM_CONTROLLER_NODES must be a non-negative integer, got: %s\n' \
        "$SLURM_CONTROLLER_NODES" >&2
    exit 1
fi

BENCHMARK_NODE_COUNT=$(( SLURM_CONTROLLER_NODES + SLURM_PRODUCER_NODES + SLURM_CONSUMER_NODES ))
NODE_COUNT=$(( BROKER_COUNT_MAX + 1 + BENCHMARK_NODE_COUNT ))

controller_tasks_per_node=0
if (( SLURM_CONTROLLER_NODES > 0 )); then
    controller_tasks_per_node="$(ceil_div 1 "$SLURM_CONTROLLER_NODES")"
fi

producer_tasks_per_node="$(ceil_div "$MAX_PRODUCER_RANKS" "$SLURM_PRODUCER_NODES")"
if (( SLURM_CONTROLLER_NODES == 0 )); then
    producer_tasks_per_node=$(( producer_tasks_per_node + 1 ))
fi
consumer_tasks_per_node="$(ceil_div "$MAX_CONSUMER_RANKS" "$SLURM_CONSUMER_NODES")"

DEFAULT_NTASKS_PER_NODE="$(max_int "$controller_tasks_per_node" "$producer_tasks_per_node" "$consumer_tasks_per_node")"
SLURM_NTASKS_PER_NODE="${SLURM_NTASKS_PER_NODE:-$DEFAULT_NTASKS_PER_NODE}"
SLURM_TIME="${SLURM_TIME:-02:00:00}"

export SLURM_CONTROLLER_NODES
export SLURM_PRODUCER_NODES
export SLURM_CONSUMER_NODES

if [[ "$RUN_PREFLIGHT" == "1" && "$DRY_RUN" != "1" ]]; then
    for config_path in "${configs[@]}"; do
        ./scripts/hpc_preflight.sh "$config_path"
    done
fi

sbatch_args=(
    --nodes="$NODE_COUNT"
    --ntasks-per-node="$SLURM_NTASKS_PER_NODE"
    --job-name="$JOB_NAME"
    --time="$SLURM_TIME"
    --output="$PROJECT_ROOT/logs/slurm-%j.out"
)

sbatch_export_entries=(
    "PROJECT_ROOT=$PROJECT_ROOT"
    "AUTO_LOAD_GWDG_MODULES=${AUTO_LOAD_GWDG_MODULES:-1}"
    "ENABLE_MONITORING=${ENABLE_MONITORING:-1}"
    "ENABLE_NODE_EXPORTER=${ENABLE_NODE_EXPORTER:-1}"
    "ENABLE_KAFKA_EXPORTER=${ENABLE_KAFKA_EXPORTER:-1}"
    "ENABLE_SYSTEM_INVENTORY=${ENABLE_SYSTEM_INVENTORY:-1}"
    "ENABLE_RAM_BACKED_RUNTIME=${ENABLE_RAM_BACKED_RUNTIME:-1}"
    "BENCHMARK_NODE_LAYOUT=$BENCHMARK_NODE_LAYOUT"
    "SLURM_CONTROLLER_NODES=$SLURM_CONTROLLER_NODES"
    "SLURM_PRODUCER_NODES=$SLURM_PRODUCER_NODES"
    "SLURM_CONSUMER_NODES=$SLURM_CONSUMER_NODES"
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
    KAFKA_HPC_NETWORK_INTERFACE \
    KAFKA_HPC_REQUIRE_FABRIC \
    KAFKA_NUM_IO_THREADS \
    KAFKA_NUM_NETWORK_THREADS \
    KAFKA_QUEUED_MAX_REQUESTS \
    KAFKA_SOCKET_RECEIVE_BUFFER_BYTES \
    KAFKA_SOCKET_SEND_BUFFER_BYTES \
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
    NODE_EXPORTER_NODE_SCOPE \
    PROMETHEUS_READY_TIMEOUT_SEC \
    PROMETHEUS_SCRAPE_INTERVAL_SEC \
    SLURM_HPC_MODULES \
    SOURCE_HPC_ENV_FILE; do
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

printf '[submit-iteration] Iteration: %s\n' "$ITERATION_NAME"
printf '[submit-iteration] DRY_RUN=%s\n' "$DRY_RUN"
printf '[submit-iteration] Same allocation: yes, one Slurm job runs all scenarios on the same nodes\n'
printf '[submit-iteration] Order: ingress-only -> egress-only -> simultaneous\n'
printf '[submit-iteration] Result layout: results/iteration_<slurm_job_id>_%s/{case_<slurm_job_id>_01_ingress_only,case_<slurm_job_id>_02_egress_only,case_<slurm_job_id>_03_simultaneous}\n' \
    "$ITERATION_NAME"
printf '[submit-iteration] Configs:\n'
for index in "${!configs[@]}"; do
    printf '  %s. %s: %s\n' "$(( index + 1 ))" "${labels[$index]}" "${configs[$index]}"
done
printf '[submit-hpc] Nodes: %s\n' "$NODE_COUNT"
printf '[submit-hpc] Max MPI ranks: %s, benchmark nodes: %s, ntasks-per-node: %s\n' \
    "$MAX_MPI_RANKS" "$BENCHMARK_NODE_COUNT" "$SLURM_NTASKS_PER_NODE"
printf '[submit-hpc] Benchmark node layout: %s\n' "$BENCHMARK_NODE_LAYOUT"
printf '[submit-hpc] Role split nodes: controller=%s producer=%s consumer=%s\n' \
    "$SLURM_CONTROLLER_NODES" "$SLURM_PRODUCER_NODES" "$SLURM_CONSUMER_NODES"
if [[ "$SLURM_CONTROLLER_NODES" == "0" ]]; then
    printf '[submit-hpc] MPI rank 0: colocated on first producer node\n'
fi

if [[ "$DRY_RUN" == "1" ]]; then
    if [[ "$NODE_COUNT" != "4" ]]; then
        printf '[submit-iteration] ERROR: dry-run did not resolve to 4 nodes.\n' >&2
        exit 1
    fi
    if [[ "$SLURM_CONTROLLER_NODES" != "0" || "$SLURM_PRODUCER_NODES" != "1" || "$SLURM_CONSUMER_NODES" != "1" ]]; then
        printf '[submit-iteration] ERROR: dry-run did not use the expected role-split placement.\n' >&2
        exit 1
    fi

    printf '[submit-iteration] DRY_RUN=1, not submitting. Command would be:\n'
    printf '  sbatch'
    printf ' %q' "${sbatch_args[@]}" "$PROJECT_ROOT/scripts/run_iteration.sh" "$ITERATION_NAME" "${configs[@]}"
    printf '\n'
    exit 0
fi

sbatch "${sbatch_args[@]}" "$PROJECT_ROOT/scripts/run_iteration.sh" "$ITERATION_NAME" "${configs[@]}"
