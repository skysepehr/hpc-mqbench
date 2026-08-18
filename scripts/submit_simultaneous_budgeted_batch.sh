#!/usr/bin/env bash
set -euo pipefail

# Submit one simultaneous-only sweep batch as a single Slurm allocation.
# Dry-run is the validation path for code changes:
#   DRY_RUN=1 ./scripts/submit_simultaneous_budgeted_batch.sh configs/sweeps/.../batch_001.csv

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT"

BATCH_CSV="${1:-}"
if [[ -z "$BATCH_CSV" ]]; then
    printf '[submit-sweep] ERROR: usage: %s <batch.csv>\n' "$0" >&2
    exit 1
fi
if [[ ! -f "$BATCH_CSV" ]]; then
    printf '[submit-sweep] ERROR: batch CSV not found: %s\n' "$BATCH_CSV" >&2
    exit 1
fi

DRY_RUN="${DRY_RUN:-0}"
RUN_PREFLIGHT="${RUN_PREFLIGHT:-1}"
SWEEP_ID="${SWEEP_ID:-simultaneous_budgeted}"
SWEEP_RUN_ID="${SWEEP_RUN_ID:-}"
SWEEP_MAX_CASES="${SWEEP_MAX_CASES:-50}"
SWEEP_STOP_MARGIN_MIN="${SWEEP_STOP_MARGIN_MIN:-15}"
SWEEP_REPORT_MODE="${SWEEP_REPORT_MODE:-light}"
BENCHMARK_REPORT_MODE="${BENCHMARK_REPORT_MODE:-$SWEEP_REPORT_MODE}"
SLURM_EXCLUSIVE="${SLURM_EXCLUSIVE:-1}"
SLURM_DEPENDENCY="${SLURM_DEPENDENCY:-}"

sanitize_name() {
    local value="${1:?name required}"
    value="$(printf '%s' "$value" | tr -c 'A-Za-z0-9_.-' '-')"
    printf '%s\n' "${value:0:80}"
}

validate_positive_integer() {
    local name="${1:?name required}"
    local value="${2:-}"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        printf '[submit-sweep] ERROR: %s must be a positive integer, got: %s\n' \
            "$name" "${value:-<empty>}" >&2
        exit 1
    fi
}

ceil_div() {
    local numerator="${1:?numerator required}"
    local denominator="${2:?denominator required}"
    if (( denominator <= 0 )); then
        printf '[submit-sweep] ERROR: denominator must be positive for ceil_div\n' >&2
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

validate_positive_integer "SWEEP_MAX_CASES" "$SWEEP_MAX_CASES"
validate_positive_integer "SWEEP_STOP_MARGIN_MIN" "$SWEEP_STOP_MARGIN_MIN"
if [[ "$SLURM_EXCLUSIVE" != "0" && "$SLURM_EXCLUSIVE" != "1" ]]; then
    printf '[submit-sweep] ERROR: SLURM_EXCLUSIVE must be 0 or 1, got: %s\n' \
        "$SLURM_EXCLUSIVE" >&2
    exit 1
fi

case "$BENCHMARK_REPORT_MODE" in
    full|light|machine) ;;
    *)
        printf '[submit-sweep] ERROR: BENCHMARK_REPORT_MODE must be full, light, or machine, got: %s\n' "$BENCHMARK_REPORT_MODE" >&2
        exit 1
        ;;
esac

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

if [[ -n "${SLURM_HPC_MODULES:-}" ]]; then
    export HPC_MODULES="$SLURM_HPC_MODULES"
fi

if [[ "$DRY_RUN" != "1" ]]; then
    load_hpc_modules
    mkdir -p "$PROJECT_ROOT/logs" "$PROJECT_ROOT/results"
fi

CONFIG_INFO="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$BATCH_CSV" <<'PY'
import csv
import shlex
import sys
from pathlib import Path

from src.benchmark.broker_profile import resolve_profile_for_config
from src.benchmark.config_loader import load_benchmark_config
from src.benchmark.qualification import COMMON_QUALIFICATION_POLICY_ID

project = Path.cwd().resolve()
batch = Path(sys.argv[1])
rows = list(csv.DictReader(batch.open("r", encoding="utf-8", newline="")))
if not rows:
    raise SystemExit(f"{batch}: no configs found")
if len(rows) > 50:
    raise SystemExit(f"{batch}: sweep batches must contain at most 50 configs, got {len(rows)}")

configs = []
declared_profiles = set()
reproducible_modes = set()
for row in rows:
    config_path = Path(row.get("config_path") or "")
    if not config_path.is_absolute():
        config_path = project / config_path
    config = load_benchmark_config(config_path)
    if config.mode != "single":
        raise SystemExit(f"{config_path}: sweep supports only mode='single'")
    if config.scenario != "simultaneous":
        raise SystemExit(f"{config_path}: sweep supports only scenario='simultaneous'")
    if config.broker_count != 1:
        raise SystemExit(f"{config_path}: sweep supports only broker_count=1")
    if config.replication_factor != 1:
        raise SystemExit(f"{config_path}: sweep supports only replication_factor=1")
    if config.acks != "1":
        raise SystemExit(f"{config_path}: sweep supports only acks='1'")
    if config.compression_type != "none":
        raise SystemExit(f"{config_path}: sweep supports only compression_type='none'")
    extra = config.extra if isinstance(config.extra, dict) else {}
    campaign_metadata = extra.get("campaign_metadata", {})
    if not isinstance(campaign_metadata, dict):
        campaign_metadata = {}
    measurement_contract_id = str(
        campaign_metadata.get("measurement_contract_id", "")
    )
    reproducible = measurement_contract_id == "measurement.messaging.reproducible.v1"
    reproducible_modes.add(reproducible)
    if reproducible:
        if (config.warmup_sec, config.duration_sec, config.drain_timeout_sec) != (15, 30, 60):
            raise SystemExit(
                f"{config_path}: reproducible Kafka timing must be 15/30/60 seconds"
            )
        if config.qualification_policy_id != COMMON_QUALIFICATION_POLICY_ID:
            raise SystemExit(
                f"{config_path}: reproducible Kafka requires the common qualification policy"
            )
        if not config.record_envelope_enabled:
            raise SystemExit(
                f"{config_path}: reproducible Kafka requires the record envelope"
            )
    profile = resolve_profile_for_config(
        config,
        config_path=config_path,
        project_root=project,
    )
    declared_profiles.add(
        None if profile is None else (profile.profile_id, profile.sha256)
    )
    configs.append((row.get("config_id", config_path.stem), config_path, config))

if len(reproducible_modes) != 1:
    raise SystemExit(
        "A batch cannot mix historical and reproducible measurement contracts"
    )
restart_per_case = reproducible_modes == {True}

if len(declared_profiles) > 1 and not restart_per_case:
    rendered = sorted(
        "legacy/no-profile" if item is None else item[0]
        for item in declared_profiles
    )
    raise SystemExit(
        "A historical broker-once batch cannot mix broker profiles: "
        + ", ".join(rendered)
    )

print("MODE=single")
print(f"BROKER_COUNT_MAX={max(config.broker_count for _, _, config in configs)}")
print(f"MAX_PRODUCER_RANKS={max(config.producer_ranks for _, _, config in configs)}")
print(f"MAX_CONSUMER_RANKS={max(config.consumer_ranks for _, _, config in configs)}")
print(f"MAX_MPI_RANKS={max(config.total_mpi_ranks for _, _, config in configs)}")
print(f"ESTIMATED_CASES={len(configs)}")
print(f"KAFKA_RESTART_PER_CASE={1 if restart_per_case else 0}")
print("CONFIG_IDS=" + shlex.quote(",".join(config_id for config_id, _, _ in configs)))
print("CONFIG_PATHS=" + shlex.quote(",".join(str(path.relative_to(project)) if path.is_relative_to(project) else str(path) for _, path, _ in configs)))
PY
)"
eval "$CONFIG_INFO"

BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
if [[ "$BENCHMARK_NODE_LAYOUT" != "role_split" ]]; then
    printf '[submit-sweep] ERROR: sweep batches require BENCHMARK_NODE_LAYOUT=role_split\n' >&2
    exit 1
fi
export BENCHMARK_NODE_LAYOUT

SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"

validate_positive_integer "SLURM_PRODUCER_NODES" "$SLURM_PRODUCER_NODES"
validate_positive_integer "SLURM_CONSUMER_NODES" "$SLURM_CONSUMER_NODES"
if [[ ! "$SLURM_CONTROLLER_NODES" =~ ^[0-9]+$ ]]; then
    printf '[submit-sweep] ERROR: SLURM_CONTROLLER_NODES must be a non-negative integer, got: %s\n' \
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
JOB_NAME="$(sanitize_name "${SLURM_JOB_NAME:-kafka-sweep-sim}")"

export SLURM_CONTROLLER_NODES
export SLURM_PRODUCER_NODES
export SLURM_CONSUMER_NODES

if [[ "$RUN_PREFLIGHT" == "1" && "$DRY_RUN" != "1" ]]; then
    IFS=, read -r -a preflight_configs <<< "$CONFIG_PATHS"
    for config_path in "${preflight_configs[@]}"; do
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
if [[ "$SLURM_EXCLUSIVE" == "1" ]]; then
    sbatch_args+=(--exclusive)
fi
if [[ -n "$SLURM_DEPENDENCY" ]]; then
    sbatch_args+=(--dependency="$SLURM_DEPENDENCY")
fi

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
    "SWEEP_ID=$SWEEP_ID"
    "SWEEP_MAX_CASES=$SWEEP_MAX_CASES"
    "SWEEP_STOP_MARGIN_MIN=$SWEEP_STOP_MARGIN_MIN"
    "SWEEP_RESUME=${SWEEP_RESUME:-0}"
    "BENCHMARK_REPORT_MODE=$BENCHMARK_REPORT_MODE"
    "SKIP_MONITORING_GRAPHS=${SKIP_MONITORING_GRAPHS:-1}"
    "KAFKA_RESTART_PER_CASE=$KAFKA_RESTART_PER_CASE"
    "ENABLE_BROKER_PROCESS_MONITOR=${ENABLE_BROKER_PROCESS_MONITOR:-$KAFKA_RESTART_PER_CASE}"
    "BROKER_PROCESS_MONITOR_INTERVAL_SEC=${BROKER_PROCESS_MONITOR_INTERVAL_SEC:-1}"
)

if [[ -n "$SWEEP_RUN_ID" ]]; then
    sbatch_export_entries+=("SWEEP_RUN_ID=$SWEEP_RUN_ID")
fi

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
    SWEEP_BATCH_DIR_OVERRIDE \
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

printf '[submit-sweep] Batch: %s\n' "$BATCH_CSV"
printf '[submit-sweep] Configs: %s\n' "$ESTIMATED_CASES"
printf '[submit-sweep] Config IDs: %s\n' "$CONFIG_IDS"
printf '[submit-sweep] Scenario: simultaneous only\n'
printf '[submit-sweep] Nodes: %s\n' "$NODE_COUNT"
printf '[submit-sweep] Exclusive allocation: %s\n' "$SLURM_EXCLUSIVE"
printf '[submit-sweep] Dependency: %s\n' "${SLURM_DEPENDENCY:-none}"
printf '[submit-sweep] Max MPI ranks: %s, benchmark nodes: %s, ntasks-per-node: %s\n' \
    "$MAX_MPI_RANKS" "$BENCHMARK_NODE_COUNT" "$SLURM_NTASKS_PER_NODE"
printf '[submit-sweep] Benchmark node layout: %s\n' "$BENCHMARK_NODE_LAYOUT"
printf '[submit-sweep] Role split nodes: controller=%s producer=%s consumer=%s\n' \
    "$SLURM_CONTROLLER_NODES" "$SLURM_PRODUCER_NODES" "$SLURM_CONSUMER_NODES"
if [[ "$SLURM_CONTROLLER_NODES" == "0" ]]; then
    printf '[submit-sweep] MPI rank 0: colocated on first producer node\n'
fi
printf '[submit-sweep] Report mode: %s\n' "$BENCHMARK_REPORT_MODE"
printf '[submit-sweep] Kafka lifecycle: %s\n' "$([[ "$KAFKA_RESTART_PER_CASE" == "1" ]] && printf 'restart-and-clean-per-case' || printf 'historical-broker-once')"
printf '[submit-sweep] Time guard: stop when less than %s minutes remain\n' "$SWEEP_STOP_MARGIN_MIN"

if [[ "$DRY_RUN" == "1" ]]; then
    if [[ "$NODE_COUNT" != "4" ]]; then
        printf '[submit-sweep] ERROR: dry-run did not resolve to 4 nodes.\n' >&2
        exit 1
    fi
    if [[ "$SLURM_CONTROLLER_NODES" != "0" || "$SLURM_PRODUCER_NODES" != "1" || "$SLURM_CONSUMER_NODES" != "1" ]]; then
        printf '[submit-sweep] ERROR: dry-run did not use expected controller+producer and consumer placement.\n' >&2
        exit 1
    fi

    printf '[submit-sweep] DRY_RUN=1, not submitting. Command would be:\n'
    printf '  sbatch'
    printf ' %q' "${sbatch_args[@]}" "$PROJECT_ROOT/scripts/run_simultaneous_budgeted_batch.sh" "$BATCH_CSV"
    printf '\n'
    exit 0
fi

sbatch "${sbatch_args[@]}" "$PROJECT_ROOT/scripts/run_simultaneous_budgeted_batch.sh" "$BATCH_CSV"
