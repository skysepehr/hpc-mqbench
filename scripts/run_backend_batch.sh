#!/usr/bin/env bash
set -euo pipefail

# Run several configurations sequentially in one exclusive Slurm allocation.
# Node placement, system inventory, Prometheus, and node_exporter are shared.
# Pulsar itself and its BookKeeper data root are restarted per case because
# deleting a topic does not immediately reclaim its ledger files. The default
# contract requires one immutable profile; tuning campaigns may explicitly opt
# into independently verified profile changes between fresh service starts.

unset SLURM_EXCLUSIVE

if [[ -n "${PROJECT_ROOT:-}" && -f "$PROJECT_ROOT/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" && -f "$SLURM_SUBMIT_DIR/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$SLURM_SUBMIT_DIR" && pwd)"
else
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
export PROJECT_ROOT
cd "$PROJECT_ROOT"
SCRIPT_DIR="$PROJECT_ROOT/scripts"

# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
# shellcheck source=./hpc_modules.sh
source "$SCRIPT_DIR/hpc_modules.sh"
# shellcheck source=./python_env_common.sh
source "$SCRIPT_DIR/python_env_common.sh"

BACKEND_ID="${1:-}"
MANIFEST="${2:-}"
[[ -n "$BACKEND_ID" && -f "$MANIFEST" ]] || \
    die "Usage: scripts/run_backend_batch.sh BACKEND MANIFEST.csv"
if [[ "$BACKEND_ID" != "pulsar" ]]; then
    die "The reusable backend batch runner currently supports pulsar; Kafka retains its verified sweep runner"
fi

if [[ -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi
select_backend_hpc_modules "$BACKEND_ID"
load_hpc_modules
export_benchmark_hpc_python_env "$PROJECT_ROOT"

for command_name in python3 java srun scontrol mpirun; do
    require_command "$command_name"
done
PULSAR_HOME="${PULSAR_HOME:-$PROJECT_ROOT/.local/pulsar-current}"
export PULSAR_HOME
require_file "$PULSAR_HOME/bin/pulsar"
require_file "$PULSAR_HOME/bin/pulsar-admin"

export BACKEND_ID
export ENABLE_MONITORING="${ENABLE_MONITORING:-1}"
export ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
export ENABLE_KAFKA_EXPORTER=0
export ENABLE_SYSTEM_INVENTORY="${ENABLE_SYSTEM_INVENTORY:-1}"
export ENABLE_BROKER_PROCESS_MONITOR="${ENABLE_BROKER_PROCESS_MONITOR:-1}"
export ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
export CLEAN_BROKER_RAM_DIRS="${CLEAN_BROKER_RAM_DIRS:-1}"
export CLEAN_STALE_KAFKA_RAM_ROOTS="${CLEAN_STALE_KAFKA_RAM_ROOTS:-0}"
export BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
export SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
export SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
export SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"
export BENCHMARK_REPORT_MODE="${BENCHMARK_REPORT_MODE:-light}"
export SKIP_MONITORING_GRAPHS="${SKIP_MONITORING_GRAPHS:-1}"
export KAFKA_HPC_NETWORK_INTERFACE="${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
export KAFKA_HPC_REQUIRE_FABRIC="${KAFKA_HPC_REQUIRE_FABRIC:-1}"
export BENCHMARK_NETWORK_INTERFACE="${BENCHMARK_NETWORK_INTERFACE:-$KAFKA_HPC_NETWORK_INTERFACE}"
export BENCHMARK_REQUIRE_FABRIC="${BENCHMARK_REQUIRE_FABRIC:-$KAFKA_HPC_REQUIRE_FABRIC}"

BACKEND_BATCH_RUN_ID="${BACKEND_BATCH_RUN_ID:-batch_${SLURM_JOB_ID:-manual_$(date +%Y%m%d_%H%M%S)}}"
BACKEND_BATCH_MAX_CASES="${BACKEND_BATCH_MAX_CASES:-50}"
BACKEND_BATCH_STOP_MARGIN_MIN="${BACKEND_BATCH_STOP_MARGIN_MIN:-15}"
BACKEND_BATCH_CASE_OVERHEAD_SEC="${BACKEND_BATCH_CASE_OVERHEAD_SEC:-60}"
BACKEND_BATCH_MAX_FAILURES="${BACKEND_BATCH_MAX_FAILURES:-1}"
BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT="${BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT:-25}"
BACKEND_BATCH_ALLOW_PROFILE_CHANGES="${BACKEND_BATCH_ALLOW_PROFILE_CHANGES:-0}"
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" != "0" \
    && "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" != "1" ]]; then
    die "BACKEND_BATCH_ALLOW_PROFILE_CHANGES must be 0 or 1"
fi
RUN_ROOT="$PROJECT_ROOT/results/runs/$BACKEND_ID/$BACKEND_BATCH_RUN_ID"
BATCH_ID="$(basename "$MANIFEST" .csv)"
JOB_TOKEN="${SLURM_JOB_ID:-manual}"
BATCH_DIR="$RUN_ROOT/batches/${BATCH_ID}_job_${JOB_TOKEN}"
SERVICE_CASE_DIR="$BATCH_DIR/service_case"
PLAN_TSV="$BATCH_DIR/batch_plan.tsv"
STATUS_FILE="$BATCH_DIR/batch_cases.tsv"
SUMMARY_FILE="$BATCH_DIR/batch_summary.json"
NODES_FILE="$BATCH_DIR/nodes.txt"
mkdir -p "$BATCH_DIR" "$SERVICE_CASE_DIR"

planner_args=(
    "$BACKEND_ID"
    "$MANIFEST"
    --project-root "$PROJECT_ROOT"
    --format tsv
)
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" == "1" ]]; then
    planner_args+=(--allow-profile-changes)
fi
PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -B \
    "$SCRIPT_DIR/plan_backend_batch.py" "${planner_args[@]}" > "$PLAN_TSV"

mapfile -t PLAN_LINES < "$PLAN_TSV"
case_count=$(( ${#PLAN_LINES[@]} - 1 ))
(( case_count > 0 )) || die "Batch plan contains no cases"
(( case_count <= BACKEND_BATCH_MAX_CASES )) || \
    die "Batch has $case_count cases, exceeding limit $BACKEND_BATCH_MAX_CASES"

if [[ -n "${SLURM_JOB_NODELIST:-}" ]]; then
    scontrol show hostnames "$SLURM_JOB_NODELIST" > "$NODES_FILE"
else
    hostname > "$NODES_FILE"
fi

printf '%s\n' \
    'sequence	status	stage	case_id	config_id	config_path	case_dir	started_unix	finished_unix	reason' \
    > "$STATUS_FILE"

read_config_values() {
    local config_path="${1:?config path required}"
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$config_path" <<'PY'
import shlex
import sys
from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config

config = load_benchmark_config(sys.argv[1])
pairs = get_backend(config.backend_id).runtime_environment(config)
for key, value in pairs.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
}

load_case_exports() {
    local config_path="${1:?config path required}"
    unset PULSAR_SERVICE_COUNT PULSAR_TOPIC_NAME PULSAR_SUBSCRIPTION_NAME \
        PULSAR_PROFILE_ID PULSAR_PROFILE_SHA256 PULSAR_PRODUCT_VERSION PULSAR_MEM
    eval "$(read_config_values "$config_path")"
    export CONFIG_PATH="$(abspath "$config_path")"
    export BACKEND_ID SERVICE_NODE_COUNT BROKER_COUNT PARTITIONS \
        REPLICATION_FACTOR TOPIC_NAME SCENARIO PRODUCER_RANKS CONSUMER_RANKS
    export PULSAR_SERVICE_COUNT PULSAR_TOPIC_NAME PULSAR_SUBSCRIPTION_NAME \
        PULSAR_PROFILE_ID PULSAR_PROFILE_SHA256 PULSAR_PRODUCT_VERSION PULSAR_MEM
}

configure_ram_backed_runtime() {
    [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]] || return 0
    local user_token="${USER:-user}"
    export KAFKA_RAM_ROOT="${KAFKA_RAM_ROOT:-/dev/shm/messaging-batch-${user_token}-${JOB_TOKEN}}"
    export TMPDIR="${KAFKA_RAM_TMPDIR:-$KAFKA_RAM_ROOT/tmp}"
    export TMP="$TMPDIR"
    export TEMP="$TMPDIR"
    export XDG_CACHE_HOME="${KAFKA_RAM_CACHE_HOME:-$KAFKA_RAM_ROOT/cache}"
    export KAFKA_CLIENT_RAM_DIR="${KAFKA_CLIENT_RAM_DIR:-$KAFKA_RAM_ROOT/clients}"
    export PULSAR_RAM_ROOT="${PULSAR_RAM_ROOT:-$KAFKA_RAM_ROOT/pulsar}"
}

prepare_ram_backed_runtime_dirs() {
    [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]] || return 0
    local q_root q_tmp q_cache q_clients node
    printf -v q_root '%q' "$KAFKA_RAM_ROOT"
    printf -v q_tmp '%q' "$TMPDIR"
    printf -v q_cache '%q' "$XDG_CACHE_HOME"
    printf -v q_clients '%q' "$KAFKA_CLIENT_RAM_DIR"
    for node in "${ALL_NODES[@]}"; do
        srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc "
            rm -rf ${q_root}
            mkdir -p ${q_root} ${q_tmp} ${q_cache} ${q_clients}
            chmod 700 ${q_root} ${q_tmp} ${q_cache} ${q_clients} || true
        " >"$SERVICE_CASE_DIR/logs/ram-runtime-${node}.log" 2>&1
    done
}

cleanup_all_ram_runtime_dirs() {
    [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]] || return 0
    local q_root node
    printf -v q_root '%q' "$KAFKA_RAM_ROOT"
    for node in "${ALL_NODES[@]}"; do
        srun --overlap --nodes=1 --ntasks=1 -w "$node" \
            bash -lc "rm -rf ${q_root}" || \
            log_warn "Could not remove shared RAM runtime root on $node"
    done
}

reset_backend_ram_runtime() {
    [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]] || return 0
    local q_root reset_log used_percent free_percent
    printf -v q_root '%q' "$PULSAR_RAM_ROOT"
    reset_log="$CASE_DIR/logs/pulsar/post-case-storage-reset.log"
    ensure_dir "$(dirname "$reset_log")"
    srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
        set -euo pipefail
        rm -rf ${q_root}
        mkdir -p ${q_root}
        chmod 700 ${q_root} || true
        df -P /dev/shm
    " >"$reset_log" 2>&1
    used_percent="$(
        srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" \
            bash -lc "df -P /dev/shm | awk 'NR == 2 {gsub(/%/, \"\", \$5); print \$5}'" \
            2>>"$reset_log" | tail -n 1
    )"
    [[ "$used_percent" =~ ^[0-9]+$ ]] || {
        log_error "Could not determine tmpfs use after Pulsar reset; see $reset_log"
        return 1
    }
    free_percent=$((100 - used_percent))
    printf 'post_reset_used_percent=%s\npost_reset_free_percent=%s\n' \
        "$used_percent" "$free_percent" >> "$reset_log"
    if (( free_percent < BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT )); then
        log_error "Pulsar tmpfs has only ${free_percent}% free after reset; see $reset_log"
        return 1
    fi
    log_info "Pulsar per-case storage reset completed; tmpfs free=${free_percent}%"
}

record_event() {
    local name="${1:?event name required}" label="${2:?event label required}"
    local event_unix="${3:-}"
    [[ -n "$event_unix" ]] || event_unix="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    python3 - "$CASE_DIR/runtime/benchmark_events.json" "$name" "$label" "$event_unix" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
name, label, raw_time = sys.argv[2:]
timestamp = float(raw_time)
payload = {}
if path.is_file():
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = {}
events = payload.get("events", []) if isinstance(payload, dict) else []
events.append({
    "name": name,
    "label": label,
    "source": "slurm-batch",
    "unix": timestamp,
    "iso": dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).isoformat(),
})
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps({"format": "benchmark_events.v1", "events": sorted(events, key=lambda item: item["unix"])}, indent=2, sort_keys=True), encoding="utf-8")
PY
}

write_windows() {
    cat > "$CASE_DIR/runtime/benchmark_window.json" <<EOF
{
  "start_unix": ${BENCHMARK_START_UNIX},
  "end_unix": ${BENCHMARK_END_UNIX}
}
EOF
    cat > "$CASE_DIR/runtime/monitoring_window.json" <<EOF
{
  "start_unix": ${MONITORING_WINDOW_START_UNIX},
  "end_unix": ${BENCHMARK_END_UNIX}
}
EOF
}

append_status() {
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" \
        >> "$STATUS_FILE"
}

write_summary() {
    python3 - "$BACKEND_ID" "$BACKEND_BATCH_RUN_ID" "$BATCH_DIR" \
        "$STATUS_FILE" "$SUMMARY_FILE" <<'PY'
import csv
import json
import sys
import time
from pathlib import Path

backend_id, run_id, batch_dir, status_name, output_name = sys.argv[1:]
latest = {}
with Path(status_name).open("r", encoding="utf-8", newline="") as handle:
    for row in csv.DictReader(handle, delimiter="\t"):
        latest[row["case_id"]] = row
rows = sorted(latest.values(), key=lambda row: int(row["sequence"]))
payload = {
    "format": "messaging-benchmark.backend-batch-summary.v1",
    "backend_id": backend_id,
    "run_id": run_id,
    "batch_dir": str(Path(batch_dir).name),
    "slurm_job_id": __import__("os").environ.get("SLURM_JOB_ID", ""),
    "case_count": len(rows),
    "completed_count": sum(row["status"] == "completed" for row in rows),
    "failed_count": sum(row["status"].startswith("failed") for row in rows),
    "skipped_count": sum(row["status"].startswith("skipped") for row in rows),
    "finished_unix": int(time.time()),
    "cases": rows,
}
Path(output_name).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

slurm_deadline_unix() {
    [[ -n "${SLURM_JOB_ID:-}" ]] || return 1
    local end_time
    end_time="$(scontrol show job "$SLURM_JOB_ID" -o 2>/dev/null \
        | tr ' ' '\n' | awk -F= '$1=="EndTime" {print $2; exit}' || true)"
    [[ -n "$end_time" && "$end_time" != Unknown && "$end_time" != N/A ]] || return 1
    date -d "$end_time" +%s
}

time_guard_allows_start() {
    local case_budget_sec="${1:?case budget required}"
    [[ -n "${BATCH_DEADLINE_UNIX:-}" ]] || return 0
    local remaining required
    remaining=$(( BATCH_DEADLINE_UNIX - $(date +%s) ))
    required=$((
        case_budget_sec
        + BACKEND_BATCH_CASE_OVERHEAD_SEC
        + BACKEND_BATCH_STOP_MARGIN_MIN * 60
    ))
    if (( remaining < required )); then
        log_warn "Batch time guard: remaining=${remaining}s required=${required}s"
        return 1
    fi
}

copy_service_artifacts() {
    local source_runtime="$SERVICE_CASE_DIR/runtime"
    if [[ -d "$source_runtime/system_inventory" ]]; then
        cp -a "$source_runtime/system_inventory" "$CASE_DIR/runtime/"
        cp -f "$source_runtime/system_inventory/system_inventory.json" \
            "$CASE_DIR/data/system_inventory.json"
    fi
    if [[ -f "$source_runtime/monitoring/prometheus_endpoint.txt" ]]; then
        mkdir -p "$CASE_DIR/runtime/monitoring"
        cp -f "$source_runtime/monitoring/prometheus_endpoint.txt" \
            "$CASE_DIR/runtime/monitoring/prometheus_endpoint.txt"
    fi
}

FIRST_CONFIG="$(awk -F '\t' 'NR==2 {print $7}' "$PLAN_TSV")"
load_case_exports "$FIRST_CONFIG"
BATCH_PROFILE_ID="$PULSAR_PROFILE_ID"
BATCH_PROFILE_SHA256="$PULSAR_PROFILE_SHA256"
configure_ram_backed_runtime

export CASE_ID="service_${JOB_TOKEN}"
export CASE_NAME="${BATCH_ID}_services"
export CASE_DIR="$SERVICE_CASE_DIR"
prepare_case_directories "$CASE_DIR"
write_env_snapshot "$CASE_DIR/runtime/env_snapshot.txt"
"$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" snapshot "$CONFIG_PATH"
split_nodes "$SERVICE_NODE_COUNT"
PULSAR_NODE="${SERVICE_NODES[0]}"
export PULSAR_NODE
prepare_ram_backed_runtime_dirs

MONITORING_STARTED=0
BACKEND_RUNNING=0

stop_backend_for_case() {
    [[ "$BACKEND_RUNNING" == "1" ]] || return 0
    local previous_clean="${CLEAN_BROKER_RAM_DIRS:-1}" stop_status=0
    export CLEAN_BROKER_RAM_DIRS=0
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" stop "$CONFIG_PATH" \
        || stop_status=$?
    export CLEAN_BROKER_RAM_DIRS="$previous_clean"
    BACKEND_RUNNING=0
    return "$stop_status"
}

cleanup_services() {
    local exit_code=$?
    if [[ "$BACKEND_RUNNING" == "1" ]]; then
        stop_backend_for_case || true
    fi
    if [[ "$MONITORING_STARTED" == "1" ]]; then
        export CASE_DIR="$SERVICE_CASE_DIR"
        "$SCRIPT_DIR/stop_monitoring.sh" || true
        [[ "$ENABLE_NODE_EXPORTER" == "1" ]] && "$SCRIPT_DIR/stop_node_exporter.sh" || true
    fi
    cleanup_all_ram_runtime_dirs || true
    write_summary || true
    trap - EXIT
    exit "$exit_code"
}
trap cleanup_services EXIT

log_info "Backend batch run: $BACKEND_BATCH_RUN_ID"
log_info "Cases: $case_count; nodes: $(paste -sd, "$NODES_FILE")"
if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" == "1" ]]; then
    log_info "Immutable profiles may change only between fresh per-case service starts"
else
    log_info "Immutable profile: $BATCH_PROFILE_ID ($BATCH_PROFILE_SHA256)"
fi

if BATCH_DEADLINE_UNIX="$(slurm_deadline_unix)"; then
    export BATCH_DEADLINE_UNIX
else
    BATCH_DEADLINE_UNIX=""
fi

if [[ "$ENABLE_SYSTEM_INVENTORY" == "1" ]]; then
    bash "$SCRIPT_DIR/collect_system_inventory.sh" || \
        log_warn "System capability inventory collection failed"
fi
if [[ "$ENABLE_MONITORING" == "1" ]]; then
    MONITORING_STARTED=1
    [[ "$ENABLE_NODE_EXPORTER" == "1" ]] && "$SCRIPT_DIR/start_node_exporter.sh"
    "$SCRIPT_DIR/start_monitoring.sh"
fi

failed_count=0
skip_remaining=0
for plan_line in "${PLAN_LINES[@]:1}"; do
    IFS=$'\t' read -r sequence block order stage case_id config_id config_path \
        profile_id profile_sha256 producer_ranks consumer_ranks warmup_sec \
        duration_sec drain_timeout_sec case_budget_sec topic_name <<< "$plan_line"
    if [[ -n "${BACKEND_BATCH_REPAIR_ID:-}" ]]; then
        case_dir="$RUN_ROOT/repairs/$BACKEND_BATCH_REPAIR_ID/$stage/$case_id"
    else
        case_dir="$RUN_ROOT/$stage/$case_id"
    fi

    if [[ "$skip_remaining" == "1" ]]; then
        append_status "$sequence" skipped_after_failure "$stage" "$case_id" \
            "$config_id" "$config_path" "$case_dir" "" "$(date +%s)" \
            "earlier case failed"
        continue
    fi
    if ! time_guard_allows_start "$case_budget_sec"; then
        append_status "$sequence" skipped_time_guard "$stage" "$case_id" \
            "$config_id" "$config_path" "$case_dir" "" "$(date +%s)" \
            "Slurm wall-time guard"
        skip_remaining=1
        continue
    fi

    load_case_exports "$config_path"
    if [[ "$BACKEND_BATCH_ALLOW_PROFILE_CHANGES" != "1" \
        && ( "$PULSAR_PROFILE_ID" != "$BATCH_PROFILE_ID" \
        || "$PULSAR_PROFILE_SHA256" != "$BATCH_PROFILE_SHA256" ) ]]; then
        die "Profile drift in $case_id: $PULSAR_PROFILE_ID ($PULSAR_PROFILE_SHA256)"
    fi

    export CASE_ID="$case_id" CASE_NAME="$config_id" CASE_DIR="$case_dir"
    prepare_case_directories "$CASE_DIR"
    write_env_snapshot "$CASE_DIR/runtime/env_snapshot.txt"
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" snapshot "$CONFIG_PATH"
    copy_service_artifacts
    started_unix="$(date +%s)"
    append_status "$sequence" running "$stage" "$case_id" "$config_id" \
        "$config_path" "$case_dir" "$started_unix" "" ""
    log_info "Starting batch case ${sequence}/${case_count}: $case_id"

    start_status=0
    BACKEND_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export BACKEND_START_UNIX KAFKA_START_UNIX="$BACKEND_START_UNIX"
    record_event backend_start_requested "Fresh Pulsar service start requested" \
        "$BACKEND_START_UNIX"
    BACKEND_RUNNING=1
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" start "$CONFIG_PATH" \
        || start_status=$?
    if (( start_status == 0 )); then
        "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" wait-ready "$CONFIG_PATH" \
            || start_status=$?
    fi
    if (( start_status != 0 )); then
        stop_status=0
        reset_status=0
        stop_backend_for_case || stop_status=$?
        (( stop_status != 0 )) || reset_backend_ram_runtime || reset_status=$?
        failed_count=$((failed_count + 1))
        reason="start=$start_status stop=$stop_status reset=$reset_status"
        append_status "$sequence" failed "$stage" "$case_id" "$config_id" \
            "$config_path" "$case_dir" "$started_unix" "$(date +%s)" "$reason"
        log_error "Pulsar service failed before case $case_id ($reason)"
        if (( failed_count >= BACKEND_BATCH_MAX_FAILURES )); then
            skip_remaining=1
        fi
        continue
    fi
    record_event backend_ready "Fresh Pulsar service is ready"

    MONITORING_WINDOW_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export MONITORING_WINDOW_START_UNIX
    record_event stream_create_start "Pulsar stream creation starts"
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" create-stream "$CONFIG_PATH"
    record_event stream_ready "Pulsar stream ready"
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" prefill "$CONFIG_PATH"
    if [[ "$ENABLE_BROKER_PROCESS_MONITOR" == "1" ]]; then
        BACKEND_MONITOR_COMPONENT=process \
            "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" start-monitoring "$CONFIG_PATH"
    fi

    BENCHMARK_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export BENCHMARK_START_UNIX
    record_event mpi_launch "MPI benchmark launch" "$BENCHMARK_START_UNIX"
    run_status=0
    "$SCRIPT_DIR/run_case.sh" </dev/null || run_status=$?
    BENCHMARK_END_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export BENCHMARK_END_UNIX
    record_event benchmark_window_end "Benchmark window ends" "$BENCHMARK_END_UNIX"
    write_windows

    health_status=0
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" check-health "$CONFIG_PATH" \
        || health_status=$?
    if [[ "$ENABLE_BROKER_PROCESS_MONITOR" == "1" ]]; then
        BACKEND_MONITOR_COMPONENT=process \
            "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" stop-monitoring "$CONFIG_PATH" \
            || true
    fi
    if ! "$SCRIPT_DIR/collect_results.sh"; then
        (( run_status != 0 )) || run_status=1
    fi

    cleanup_status=0
    record_event stream_cleanup_start "Pulsar stream cleanup starts"
    BACKEND_STREAM_REQUIRE_TMPFS_RECOVERY=0 \
        "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" delete-stream "$CONFIG_PATH" \
        || cleanup_status=$?
    record_event stream_cleanup_end "Pulsar stream cleanup ends"

    stop_status=0
    reset_status=0
    record_event backend_stop_start "Pulsar per-case service stop starts"
    stop_backend_for_case || stop_status=$?
    if (( stop_status == 0 )); then
        reset_backend_ram_runtime || reset_status=$?
    fi
    record_event backend_stop_end "Pulsar per-case service stop ends"

    if (( run_status == 0 && health_status == 0 && cleanup_status == 0 \
        && stop_status == 0 && reset_status == 0 )); then
        append_status "$sequence" completed "$stage" "$case_id" "$config_id" \
            "$config_path" "$case_dir" "$started_unix" "$(date +%s)" ""
        log_info "Completed batch case: $case_id"
    else
        failed_count=$((failed_count + 1))
        reason="run=$run_status health=$health_status cleanup=$cleanup_status stop=$stop_status reset=$reset_status"
        append_status "$sequence" "failed" "$stage" "$case_id" \
            "$config_id" "$config_path" "$case_dir" "$started_unix" \
            "$(date +%s)" "$reason"
        log_error "Batch case failed: $case_id ($reason)"
        if (( failed_count >= BACKEND_BATCH_MAX_FAILURES )); then
            skip_remaining=1
        fi
    fi
done

write_summary
if (( failed_count > 0 || skip_remaining > 0 )); then
    exit 1
fi
log_info "Backend batch completed: $BATCH_DIR"
