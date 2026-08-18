#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PROJECT_ROOT
cd "$PROJECT_ROOT"

BACKEND_ID="${1:-}"
MANIFEST="${2:-}"
DRY_RUN="${DRY_RUN:-0}"
RUN_PREFLIGHT="${RUN_PREFLIGHT:-1}"
BACKEND_BATCH_RUN_ID="${BACKEND_BATCH_RUN_ID:-batch_$(date +%Y%m%d_%H%M%S)}"
BACKEND_BATCH_MAX_CASES="${BACKEND_BATCH_MAX_CASES:-50}"
BACKEND_BATCH_STOP_MARGIN_MIN="${BACKEND_BATCH_STOP_MARGIN_MIN:-15}"
BACKEND_BATCH_CASE_OVERHEAD_SEC="${BACKEND_BATCH_CASE_OVERHEAD_SEC:-60}"
BACKEND_BATCH_STARTUP_OVERHEAD_SEC="${BACKEND_BATCH_STARTUP_OVERHEAD_SEC:-180}"
BACKEND_BATCH_ALLOW_PROFILE_CHANGES="${BACKEND_BATCH_ALLOW_PROFILE_CHANGES:-0}"
BENCHMARK_REPORT_MODE="${BENCHMARK_REPORT_MODE:-light}"

usage() {
    printf 'Usage: %s BACKEND MANIFEST.csv\n' "$0" >&2
    exit 2
}

[[ -n "$BACKEND_ID" && -f "$MANIFEST" ]] || usage
for pair in \
    "BACKEND_BATCH_MAX_CASES:$BACKEND_BATCH_MAX_CASES" \
    "BACKEND_BATCH_STOP_MARGIN_MIN:$BACKEND_BATCH_STOP_MARGIN_MIN" \
    "BACKEND_BATCH_CASE_OVERHEAD_SEC:$BACKEND_BATCH_CASE_OVERHEAD_SEC" \
    "BACKEND_BATCH_STARTUP_OVERHEAD_SEC:$BACKEND_BATCH_STARTUP_OVERHEAD_SEC"; do
    name="${pair%%:*}"
    value="${pair#*:}"
    if [[ ! "$value" =~ ^[0-9]+$ ]] || [[ "$name" == "BACKEND_BATCH_MAX_CASES" && "$value" == "0" ]]; then
        printf '[submit-batch] ERROR: %s has invalid value %s\n' "$name" "$value" >&2
        exit 1
    fi
done
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" != "0" \
    && "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" != "1" ]]; then
    printf '[submit-batch] ERROR: BACKEND_BATCH_ALLOW_PROFILE_CHANGES must be 0 or 1\n' >&2
    exit 1
fi

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"
# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"
if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" \
    && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi
select_backend_hpc_modules "$BACKEND_ID"
if [[ "$DRY_RUN" != "1" ]]; then
    load_hpc_modules
fi
export_benchmark_hpc_python_env "$PROJECT_ROOT"

planner_args=(
    "$BACKEND_ID"
    "$MANIFEST"
    --project-root "$PROJECT_ROOT"
    --format shell
)
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" == "1" ]]; then
    planner_args+=(--allow-profile-changes)
fi
PLAN_SHELL="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -B \
        "$PROJECT_ROOT/scripts/plan_backend_batch.py" "${planner_args[@]}"
)"
eval "$PLAN_SHELL"

if (( ESTIMATED_CASES > BACKEND_BATCH_MAX_CASES )); then
    printf '[submit-batch] ERROR: manifest has %s cases; limit is %s\n' \
        "$ESTIMATED_CASES" "$BACKEND_BATCH_MAX_CASES" >&2
    exit 1
fi
if [[ "$NODE_COUNT" != "4" ]]; then
    printf '[submit-batch] ERROR: current batch contract requires four nodes, got %s\n' \
        "$NODE_COUNT" >&2
    exit 1
fi

SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"
producer_tasks=$(( MAX_PRODUCER_RANKS + (SLURM_CONTROLLER_NODES == 0 ? 1 : 0) ))
SLURM_NTASKS_PER_NODE="${SLURM_NTASKS_PER_NODE:-$(( producer_tasks > MAX_CONSUMER_RANKS ? producer_tasks : MAX_CONSUMER_RANKS ))}"
SLURM_TIME="${SLURM_TIME:-02:00:00}"

slurm_time_seconds() {
    local raw="${1:?time required}" days=0 hours minutes seconds
    if [[ "$raw" == *-* ]]; then
        days="${raw%%-*}"
        raw="${raw#*-}"
    fi
    IFS=: read -r hours minutes seconds <<< "$raw"
    if [[ -z "${seconds:-}" ]]; then
        seconds="$minutes"
        minutes="$hours"
        hours=0
    fi
    for value in "$days" "$hours" "$minutes" "$seconds"; do
        [[ "$value" =~ ^[0-9]+$ ]] || return 1
    done
    printf '%s\n' $(( days * 86400 + hours * 3600 + minutes * 60 + seconds ))
}

wall_time_sec="$(slurm_time_seconds "$SLURM_TIME")" || {
    printf '[submit-batch] ERROR: unsupported SLURM_TIME: %s\n' "$SLURM_TIME" >&2
    exit 1
}
required_time_sec=$((
    TOTAL_CASE_BUDGET_SEC
    + ESTIMATED_CASES * BACKEND_BATCH_CASE_OVERHEAD_SEC
    + BACKEND_BATCH_STARTUP_OVERHEAD_SEC
    + BACKEND_BATCH_STOP_MARGIN_MIN * 60
))
if (( required_time_sec > wall_time_sec )); then
    printf '[submit-batch] ERROR: conservative batch budget is %ss, exceeding %ss wall time\n' \
        "$required_time_sec" "$wall_time_sec" >&2
    exit 1
fi

planner_args=(
    "$BACKEND_ID"
    "$MANIFEST"
    --project-root "$PROJECT_ROOT"
    --format tsv
)
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" == "1" ]]; then
    planner_args+=(--allow-profile-changes)
fi
first_config="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -B \
        "$PROJECT_ROOT/scripts/plan_backend_batch.py" "${planner_args[@]}" \
        | awk -F '\t' 'NR==2 {print $7}'
)"
if [[ "$RUN_PREFLIGHT" == "1" && "$DRY_RUN" != "1" ]]; then
    "$PROJECT_ROOT/scripts/backend_lifecycle.sh" "$BACKEND_ID" preflight "$first_config"
fi

if [[ "$DRY_RUN" != "1" ]]; then
    mkdir -p "$PROJECT_ROOT/logs" "$PROJECT_ROOT/results/runs/$BACKEND_ID"
fi
job_name="${SLURM_JOB_NAME:-${BACKEND_ID}-batch-${BACKEND_BATCH_RUN_ID}}"
job_name="$(printf '%s' "$job_name" | tr -c 'A-Za-z0-9_.-' '-' | cut -c1-80)"
sbatch_args=(
    --exclusive
    --nodes="$NODE_COUNT"
    --ntasks-per-node="$SLURM_NTASKS_PER_NODE"
    --job-name="$job_name"
    --time="$SLURM_TIME"
    --output="$PROJECT_ROOT/logs/slurm-%j.out"
)
[[ -n "${SLURM_DEPENDENCY:-}" ]] && sbatch_args+=(--dependency="$SLURM_DEPENDENCY")

sbatch_export_entries=(
    "PROJECT_ROOT=$PROJECT_ROOT"
    "BACKEND_ID=$BACKEND_ID"
    "BACKEND_BATCH_RUN_ID=$BACKEND_BATCH_RUN_ID"
    "BACKEND_BATCH_MAX_CASES=$BACKEND_BATCH_MAX_CASES"
    "BACKEND_BATCH_STOP_MARGIN_MIN=$BACKEND_BATCH_STOP_MARGIN_MIN"
    "BACKEND_BATCH_CASE_OVERHEAD_SEC=$BACKEND_BATCH_CASE_OVERHEAD_SEC"
    "BACKEND_BATCH_ALLOW_PROFILE_CHANGES=$BACKEND_BATCH_ALLOW_PROFILE_CHANGES"
    "BACKEND_BATCH_REPAIR_ID=${BACKEND_BATCH_REPAIR_ID:-}"
    "BENCHMARK_REPORT_MODE=$BENCHMARK_REPORT_MODE"
    "SKIP_MONITORING_GRAPHS=${SKIP_MONITORING_GRAPHS:-1}"
    "AUTO_LOAD_GWDG_MODULES=${AUTO_LOAD_GWDG_MODULES:-1}"
    "ENABLE_MONITORING=${ENABLE_MONITORING:-1}"
    "ENABLE_NODE_EXPORTER=${ENABLE_NODE_EXPORTER:-1}"
    "ENABLE_KAFKA_EXPORTER=$([[ "$BACKEND_ID" == kafka ]] && printf 1 || printf 0)"
    "ENABLE_SYSTEM_INVENTORY=${ENABLE_SYSTEM_INVENTORY:-1}"
    "ENABLE_BROKER_PROCESS_MONITOR=${ENABLE_BROKER_PROCESS_MONITOR:-1}"
    "ENABLE_RAM_BACKED_RUNTIME=${ENABLE_RAM_BACKED_RUNTIME:-1}"
    "BENCHMARK_NODE_LAYOUT=role_split"
    "SLURM_CONTROLLER_NODES=$SLURM_CONTROLLER_NODES"
    "SLURM_PRODUCER_NODES=$SLURM_PRODUCER_NODES"
    "SLURM_CONSUMER_NODES=$SLURM_CONSUMER_NODES"
)
for export_name in \
    BACKEND_BATCH_MAX_FAILURES \
    BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT \
    BACKEND_PROCESS_MONITOR_INTERVAL_SEC \
    BENCHMARK_NETWORK_INTERFACE \
    BENCHMARK_REQUIRE_FABRIC \
    CLEAN_BROKER_RAM_DIRS \
    CLEAN_STALE_KAFKA_RAM_ROOTS \
    ENABLE_SYSTEM_IPERF \
    ENABLE_SYSTEM_RAM_PROBE \
    KAFKA_HPC_MODULES \
    KAFKA_HPC_NETWORK_INTERFACE \
    KAFKA_HPC_REQUIRE_FABRIC \
    KAFKA_RAM_CACHE_HOME \
    KAFKA_RAM_ROOT \
    KAFKA_RAM_TMPDIR \
    MONITORING_GRACE_SEC \
    MONITORING_QUERY_TIMEOUT_SEC \
    MONITORING_RANGE_STEP_SEC \
    NODE_EXPORTER_NODE_SCOPE \
    PROMETHEUS_READY_TIMEOUT_SEC \
    PROMETHEUS_SCRAPE_INTERVAL_SEC \
    PULSAR_HPC_MODULES \
    PULSAR_HOME \
    PULSAR_JAVA_HOME \
    PULSAR_RAM_ROOT \
    PULSAR_READY_TIMEOUT_SEC \
    SLURM_HPC_MODULES \
    SOURCE_HPC_ENV_FILE \
    SYSTEM_IPERF_PARALLEL \
    SYSTEM_IPERF_SECONDS \
    SYSTEM_RAM_PROBE_MB; do
    if [[ ${!export_name+x} ]]; then
        sbatch_export_entries+=("${export_name}=${!export_name}")
    fi
done
SBATCH_EXPORT="$(IFS=,; printf '%s' "${sbatch_export_entries[*]}")"
sbatch_args+=(--export="$SBATCH_EXPORT")

[[ -n "${SLURM_PARTITION:-}" ]] && sbatch_args+=(--partition="$SLURM_PARTITION")
[[ -n "${SLURM_ACCOUNT:-}" ]] && sbatch_args+=(--account="$SLURM_ACCOUNT")
[[ -n "${SLURM_QOS:-}" ]] && sbatch_args+=(--qos="$SLURM_QOS")
[[ -n "${SLURM_CONSTRAINT:-}" ]] && sbatch_args+=(--constraint="$SLURM_CONSTRAINT")

printf '[submit-batch] Backend: %s\n' "$BACKEND_ID"
printf '[submit-batch] Manifest: %s\n' "$MANIFEST"
printf '[submit-batch] Cases: %s in one exclusive job\n' "$ESTIMATED_CASES"
printf '[submit-batch] Profiles: %s (%s distinct)\n' \
    "$BATCH_PROFILE_ID" "$BATCH_PROFILE_COUNT"
printf '[submit-batch] Nodes: %s; ntasks-per-node: %s\n' "$NODE_COUNT" "$SLURM_NTASKS_PER_NODE"
printf '[submit-batch] Timing budget: %ss of %ss\n' "$required_time_sec" "$wall_time_sec"
printf '[submit-batch] Run ID: %s\n' "$BACKEND_BATCH_RUN_ID"

if [[ "$DRY_RUN" == "1" ]]; then
    printf '[submit-batch] DRY_RUN=1; command would be:\n  sbatch'
    printf ' %q' "${sbatch_args[@]}" \
        "$PROJECT_ROOT/scripts/run_backend_batch.sh" "$BACKEND_ID" "$MANIFEST"
    printf '\n'
    exit 0
fi

sbatch "${sbatch_args[@]}" \
    "$PROJECT_ROOT/scripts/run_backend_batch.sh" "$BACKEND_ID" "$MANIFEST"
