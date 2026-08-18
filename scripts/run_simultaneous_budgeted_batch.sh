#!/usr/bin/env bash
set -euo pipefail

# Slurm entrypoint for one budgeted simultaneous-only sweep batch.
# Cases run sequentially inside one allocation. Historical campaigns retain a
# batch-scoped broker; the reproducible contract restarts Kafka and clears its
# RAM-backed broker storage for every case.

# Some Slurm releases export SLURM_EXCLUSIVE=1 for an exclusive sbatch
# allocation. Nested srun commands parse that environment variable as an
# option value and reject it. The allocation remains exclusive after removing
# this inherited client-side option.
unset SLURM_EXCLUSIVE

if [[ -n "${PROJECT_ROOT:-}" && -f "$PROJECT_ROOT/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" && -f "$SLURM_SUBMIT_DIR/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$SLURM_SUBMIT_DIR" && pwd)"
else
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
cd "$PROJECT_ROOT"
SCRIPT_DIR="$PROJECT_ROOT/scripts"

# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
# shellcheck source=./hpc_modules.sh
source "$SCRIPT_DIR/hpc_modules.sh"
# shellcheck source=./python_env_common.sh
source "$SCRIPT_DIR/python_env_common.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

if [[ -n "${SLURM_HPC_MODULES:-}" ]]; then
    export HPC_MODULES="$SLURM_HPC_MODULES"
fi

export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"
load_hpc_modules
export_kafka_hpc_python_env "$PROJECT_ROOT"

if [[ $# -lt 1 ]]; then
    die "Usage: sbatch scripts/run_simultaneous_budgeted_batch.sh <batch.csv>"
fi

BATCH_CSV="$(abspath "$1")"
require_file "$BATCH_CSV"

require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
for command_name in python3 java srun scontrol mpirun; do
    require_command "$command_name"
done
require_file "$KAFKA_HOME/bin/kafka-server-start.sh"
require_file "$KAFKA_HOME/bin/kafka-storage.sh"
require_file "$KAFKA_HOME/bin/kafka-topics.sh"
require_file "$KAFKA_HOME/bin/kafka-broker-api-versions.sh"

export PROJECT_ROOT
export KAFKA_HOME
export KAFKA_LISTENER_PORT="${KAFKA_LISTENER_PORT:-9092}"
export KAFKA_CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"
export KAFKA_HPC_NETWORK_INTERFACE="${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
export KAFKA_HPC_REQUIRE_FABRIC="${KAFKA_HPC_REQUIRE_FABRIC:-1}"
export KAFKA_JMX_PORT_BASE="${KAFKA_JMX_PORT_BASE:-7101}"
export JMX_EXPORTER_PORT_BASE="${JMX_EXPORTER_PORT_BASE:-$KAFKA_JMX_PORT_BASE}"
export FORMAT_STORAGE="${FORMAT_STORAGE:-1}"
export ENABLE_MONITORING="${ENABLE_MONITORING:-1}"
export ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
export ENABLE_KAFKA_EXPORTER="${ENABLE_KAFKA_EXPORTER:-1}"
export ENABLE_SYSTEM_INVENTORY="${ENABLE_SYSTEM_INVENTORY:-1}"
export ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
export ENABLE_EGRESS_PREFILL="${ENABLE_EGRESS_PREFILL:-0}"
export CLEAN_BROKER_RAM_DIRS="${CLEAN_BROKER_RAM_DIRS:-1}"
export CLEAN_STALE_KAFKA_RAM_ROOTS="${CLEAN_STALE_KAFKA_RAM_ROOTS:-0}"
export DELETE_TOPIC_FIRST="${DELETE_TOPIC_FIRST:-1}"
export BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
export SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
export SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
export SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"
export BENCHMARK_REPORT_MODE="${BENCHMARK_REPORT_MODE:-light}"
export SKIP_MONITORING_GRAPHS="${SKIP_MONITORING_GRAPHS:-1}"
export KAFKA_RESTART_PER_CASE="${KAFKA_RESTART_PER_CASE:-0}"
export ENABLE_BROKER_PROCESS_MONITOR="${ENABLE_BROKER_PROCESS_MONITOR:-0}"
export BROKER_PROCESS_MONITOR_INTERVAL_SEC="${BROKER_PROCESS_MONITOR_INTERVAL_SEC:-1}"
if [[ "$KAFKA_RESTART_PER_CASE" != "0" && "$KAFKA_RESTART_PER_CASE" != "1" ]]; then
    die "KAFKA_RESTART_PER_CASE must be 0 or 1"
fi
if [[ "$KAFKA_RESTART_PER_CASE" == "1" && "$ENABLE_RAM_BACKED_RUNTIME" != "1" ]]; then
    die "The reproducible Kafka contract requires RAM-backed broker storage"
fi

export JMX_EXPORTER_JAR="${JMX_EXPORTER_JAR:-$PROJECT_ROOT/tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar}"
export JMX_EXPORTER_CONFIG="${JMX_EXPORTER_CONFIG:-$PROJECT_ROOT/monitoring/jmx_exporter_config.yml}"
export PROMETHEUS_HOME="${PROMETHEUS_HOME:-}"
export NODE_EXPORTER_HOME="${NODE_EXPORTER_HOME:-}"
export KAFKA_EXPORTER_HOME="${KAFKA_EXPORTER_HOME:-$PROJECT_ROOT/tools/kafka_exporter-current}"
export KAFKA_EXPORTER_PORT="${KAFKA_EXPORTER_PORT:-9308}"

require_file "$JMX_EXPORTER_JAR"
require_file "$JMX_EXPORTER_CONFIG"
if [[ "$ENABLE_MONITORING" == "1" ]]; then
    require_nonempty "PROMETHEUS_HOME" "$PROMETHEUS_HOME"
    require_file "$PROMETHEUS_HOME/prometheus"
    if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
        require_nonempty "NODE_EXPORTER_HOME" "$NODE_EXPORTER_HOME"
        require_file "$NODE_EXPORTER_HOME/node_exporter"
    fi
    if [[ "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        require_file "$KAFKA_EXPORTER_HOME/kafka_exporter"
    fi
fi

SWEEP_ID="${SWEEP_ID:-simultaneous_budgeted}"
if [[ -n "${SWEEP_RUN_ID:-}" ]]; then
    RUN_ID="$SWEEP_RUN_ID"
else
    RUN_ID="run_${SLURM_JOB_ID:-manual_$(date +%Y%m%d_%H%M%S)}"
fi
BATCH_ID="$(basename "$BATCH_CSV" .csv)"
JOB_TOKEN="${SLURM_JOB_ID:-manual}"
BATCH_DIR="${SWEEP_BATCH_DIR_OVERRIDE:-$PROJECT_ROOT/results/sweeps/$SWEEP_ID/$RUN_ID/${BATCH_ID}_job_${JOB_TOKEN}}"
SERVICE_CASE_DIR="$BATCH_DIR/service_case"
CASES_DIR="$BATCH_DIR/cases"
STATUS_FILE="$BATCH_DIR/sweep_cases.tsv"
BATCH_SUMMARY_FILE="$BATCH_DIR/batch_summary.json"
MANIFEST_TSV="$BATCH_DIR/batch_manifest.tsv"
NODES_FILE="$BATCH_DIR/nodes.txt"

mkdir -p "$BATCH_DIR" "$CASES_DIR" "$SERVICE_CASE_DIR"

normalize_batch_manifest() {
    python3 - "$BATCH_CSV" "$MANIFEST_TSV" <<'PY'
import csv
import sys
from pathlib import Path

from src.benchmark.config_loader import load_benchmark_config

project = Path.cwd().resolve()
batch = Path(sys.argv[1])
output = Path(sys.argv[2])
rows = list(csv.DictReader(batch.open("r", encoding="utf-8", newline="")))
if not rows:
    raise SystemExit(f"{batch}: no configs found")

fields = [
    "config_id",
    "config_path",
    "scenario",
    "topic_name",
    "partitions",
    "producer_ranks",
    "consumer_ranks",
    "batch_size",
    "linger_ms",
    "payload_size_bytes",
]

with output.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
    writer.writeheader()
    for index, row in enumerate(rows, start=1):
        raw_path = Path(row.get("config_path") or "")
        config_path = raw_path if raw_path.is_absolute() else project / raw_path
        config = load_benchmark_config(config_path)
        if config.scenario != "simultaneous":
            raise SystemExit(f"{config_path}: sweep runner requires scenario=simultaneous")
        if config.broker_count != 1 or config.replication_factor != 1:
            raise SystemExit(f"{config_path}: sweep runner requires one broker and rf=1")
        if config.acks != "1" or config.compression_type != "none":
            raise SystemExit(f"{config_path}: sweep runner requires acks=1 and compression=none")
        cfg_id = row.get("config_id") or f"cfg_{index:03d}"
        writer.writerow(
            {
                "config_id": cfg_id,
                "config_path": str(config_path.relative_to(project) if config_path.is_relative_to(project) else config_path),
                "scenario": config.scenario,
                "topic_name": config.topic_name,
                "partitions": config.partitions,
                "producer_ranks": config.producer_ranks,
                "consumer_ranks": config.consumer_ranks,
                "batch_size": config.batch_size,
                "linger_ms": config.linger_ms,
                "payload_size_bytes": config.payload_size_bytes,
            }
        )
PY
}

read_config_values() {
    local config_path="${1:?config path is required}"
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$config_path" <<'PY'
import shlex
import sys
from pathlib import Path

from src.benchmark.config_loader import load_benchmark_config

config = load_benchmark_config(Path(sys.argv[1]))
if config.mode != "single":
    raise SystemExit("kafka-simple-benchmark V1 supports only mode='single'")

pairs = {
    "MODE": config.mode,
    "SCENARIO": config.scenario,
    "BROKER_COUNT": str(config.broker_count),
    "PARTITIONS": str(config.partitions),
    "REPLICATION_FACTOR": str(config.replication_factor),
    "TOPIC_NAME": config.topic_name,
    "PRODUCER_RANKS": str(config.producer_ranks),
    "CONSUMER_RANKS": str(config.consumer_ranks),
    "CASE_WARMUP_SEC": str(config.warmup_sec),
    "CASE_MEASUREMENT_SEC": str(config.duration_sec),
    "CASE_DRAIN_TIMEOUT_SEC": str(config.drain_timeout_sec),
}
extra = config.extra if isinstance(config.extra, dict) else {}
payload_size_bytes = int(config.payload_size_bytes)
batch_size_bytes = int(config.batch_size)
message_limit = extra.get("kafka_message_max_bytes")
if message_limit is not None:
    message_limit = int(message_limit)
elif max(payload_size_bytes, batch_size_bytes) >= 1_000_000:
    message_limit = max(payload_size_bytes, batch_size_bytes) + 1_048_576
if message_limit is not None:
    if message_limit <= 0:
        raise ValueError("extra.kafka_message_max_bytes must be greater than 0")
    pairs.update(
        {
            "KAFKA_MESSAGE_MAX_BYTES": str(message_limit),
            "KAFKA_RECEIVE_MESSAGE_MAX_BYTES": str(int(extra.get("kafka_receive_message_max_bytes", max(100_000_000, message_limit + 1_048_576)))),
            "KAFKA_SOCKET_REQUEST_MAX_BYTES": str(int(extra.get("kafka_socket_request_max_bytes", max(104_857_600, message_limit + 16_777_216)))),
            "KAFKA_REPLICA_FETCH_MAX_BYTES": str(int(extra.get("kafka_replica_fetch_max_bytes", message_limit))),
            "KAFKA_TOPIC_MAX_MESSAGE_BYTES": str(int(extra.get("kafka_topic_max_message_bytes", message_limit))),
            "KAFKA_LOG_SEGMENT_BYTES": str(int(extra.get("kafka_log_segment_bytes", min(2_147_483_647, max(1_073_741_824, message_limit + 67_108_864))))),
        }
    )
if extra.get("kafka_heap_opts") is not None:
    pairs["KAFKA_HEAP_OPTS"] = str(extra["kafka_heap_opts"])
extra_export_map = {
    "kafka_num_network_threads": "KAFKA_NUM_NETWORK_THREADS",
    "kafka_num_io_threads": "KAFKA_NUM_IO_THREADS",
    "kafka_socket_send_buffer_bytes": "KAFKA_SOCKET_SEND_BUFFER_BYTES",
    "kafka_socket_receive_buffer_bytes": "KAFKA_SOCKET_RECEIVE_BUFFER_BYTES",
    "kafka_queued_max_requests": "KAFKA_QUEUED_MAX_REQUESTS",
}
for extra_key, env_name in extra_export_map.items():
    if extra.get(extra_key) is not None:
        pairs[env_name] = str(extra[extra_key])
for key, value in pairs.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
}

load_case_exports() {
    local config_path="${1:?config path is required}"
    unset KAFKA_MESSAGE_MAX_BYTES KAFKA_RECEIVE_MESSAGE_MAX_BYTES KAFKA_SOCKET_REQUEST_MAX_BYTES \
        KAFKA_REPLICA_FETCH_MAX_BYTES KAFKA_TOPIC_MAX_MESSAGE_BYTES KAFKA_LOG_SEGMENT_BYTES KAFKA_HEAP_OPTS
    eval "$(read_config_values "$config_path")"
    eval "$(
        PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
            python3 "$SCRIPT_DIR/render_broker_profile_env.py" \
            --config "$config_path" \
            --project-root "$PROJECT_ROOT"
    )"
    validate_broker_count "$BROKER_COUNT"
    validate_replication_factor "$REPLICATION_FACTOR" "$BROKER_COUNT"
    export CONFIG_PATH="$config_path"
    export BROKER_COUNT PARTITIONS REPLICATION_FACTOR TOPIC_NAME SCENARIO PRODUCER_RANKS CONSUMER_RANKS
    for optional_name in \
        KAFKA_MESSAGE_MAX_BYTES KAFKA_RECEIVE_MESSAGE_MAX_BYTES KAFKA_SOCKET_REQUEST_MAX_BYTES \
        KAFKA_REPLICA_FETCH_MAX_BYTES KAFKA_TOPIC_MAX_MESSAGE_BYTES KAFKA_LOG_SEGMENT_BYTES KAFKA_HEAP_OPTS \
        KAFKA_NUM_NETWORK_THREADS KAFKA_NUM_IO_THREADS KAFKA_SOCKET_SEND_BUFFER_BYTES \
        KAFKA_SOCKET_RECEIVE_BUFFER_BYTES KAFKA_QUEUED_MAX_REQUESTS; do
        if [[ -n "${!optional_name:-}" ]]; then
            export "$optional_name"
        fi
    done
    for profile_name in BROKER_PROFILE_ID BROKER_PROFILE_SHA256 BROKER_PROFILE_MANIFEST_PATH; do
        if [[ -n "${!profile_name:-}" ]]; then
            export "$profile_name"
        fi
    done
}

configure_ram_backed_runtime() {
    if [[ "${ENABLE_RAM_BACKED_RUNTIME:-1}" != "1" ]]; then
        return 0
    fi
    local job_token="${SLURM_JOB_ID:-manual-$$}"
    local user_token="${USER:-user}"
    export KAFKA_RAM_ROOT="${KAFKA_RAM_ROOT:-/dev/shm/kafka-simple-${user_token}-${job_token}}"
    export TMPDIR="${KAFKA_RAM_TMPDIR:-$KAFKA_RAM_ROOT/tmp}"
    export TMP="$TMPDIR"
    export TEMP="$TMPDIR"
    export XDG_CACHE_HOME="${KAFKA_RAM_CACHE_HOME:-$KAFKA_RAM_ROOT/cache}"
    export KAFKA_CLIENT_RAM_DIR="${KAFKA_CLIENT_RAM_DIR:-$KAFKA_RAM_ROOT/clients}"
    mkdir -p "$TMPDIR" "$XDG_CACHE_HOME" "$KAFKA_CLIENT_RAM_DIR" 2>/dev/null || true
    log_info "RAM-backed runtime root: $KAFKA_RAM_ROOT"
}

prepare_ram_backed_runtime_dirs() {
    if [[ "${ENABLE_RAM_BACKED_RUNTIME:-1}" != "1" ]]; then
        return 0
    fi
    require_nonempty "KAFKA_RAM_ROOT" "${KAFKA_RAM_ROOT:-}"
    local q_root q_tmp q_cache q_clients q_stale_pattern node
    printf -v q_root '%q' "$KAFKA_RAM_ROOT"
    printf -v q_tmp '%q' "$TMPDIR"
    printf -v q_cache '%q' "$XDG_CACHE_HOME"
    printf -v q_clients '%q' "$KAFKA_CLIENT_RAM_DIR"
    printf -v q_stale_pattern '%q' "kafka-simple-${USER:-user}-*"
    for node in "${ALL_NODES[@]}"; do
        log_info "Preparing RAM-backed runtime directories on $node"
        srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc "
            set -euo pipefail
            if [[ '${CLEAN_STALE_KAFKA_RAM_ROOTS}' == '1' ]]; then
                find /dev/shm -mindepth 1 -maxdepth 1 -type d -name ${q_stale_pattern} ! -path ${q_root} -exec rm -rf {} + 2>/dev/null || true
            fi
            rm -rf ${q_root}
            mkdir -p ${q_root} ${q_tmp} ${q_cache} ${q_clients}
            chmod 700 ${q_root} ${q_tmp} ${q_cache} ${q_clients} || true
            df -h ${q_root} || true
        " > "$SERVICE_CASE_DIR/logs/ram-runtime-${node}.log" 2>&1 || \
            log_warn "Could not prepare RAM runtime directories on $node"
    done
}

record_benchmark_event() {
    local event_name="${1:?event name is required}"
    local event_label="${2:?event label is required}"
    local event_source="${3:-slurm}"
    local event_unix="${4:-}"
    local event_file="$CASE_DIR/runtime/benchmark_events.json"
    if [[ -z "$event_unix" ]]; then
        event_unix="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    fi
    python3 - "$event_file" "$event_name" "$event_label" "$event_source" "$event_unix" <<'PY'
import datetime as dt
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
name, label, source, raw_unix = sys.argv[2:]
unix_time = float(raw_unix)
path.parent.mkdir(parents=True, exist_ok=True)
payload = {}
if path.is_file():
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = {}
events = payload.get("events", []) if isinstance(payload, dict) else []
if not isinstance(events, list):
    events = []
events.append({"name": name, "label": label, "source": source, "unix": unix_time, "iso": dt.datetime.fromtimestamp(unix_time, tz=dt.timezone.utc).isoformat()})
payload = {"format": "benchmark_events.v1", "events": sorted(events, key=lambda item: float(item.get("unix", 0.0)))}
path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY
}

write_benchmark_window() {
    cat > "$CASE_DIR/runtime/benchmark_window.json" <<EOF
{
  "start_unix": ${BENCHMARK_START_UNIX},
  "end_unix": ${BENCHMARK_END_UNIX}
}
EOF
}

write_monitoring_window() {
    local start_unix="${MONITORING_WINDOW_START_UNIX:-${BENCHMARK_START_UNIX:-}}"
    cat > "$CASE_DIR/runtime/monitoring_window.json" <<EOF
{
  "start_unix": ${start_unix},
  "end_unix": ${BENCHMARK_END_UNIX}
}
EOF
}

append_status() {
    local config_id="${1:?config id required}"
    local status="${2:?status required}"
    local case_id="${3:?case id required}"
    local case_name="${4:?case name required}"
    local config_path="${5:?config path required}"
    local case_dir="${6:?case dir required}"
    local started="${7:-}"
    local finished="${8:-}"
    local reason="${9:-}"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$config_id" "$status" "$case_id" "$case_name" "$config_path" "$case_dir" \
        "$started" "$finished" "$SCENARIO" "$TOPIC_NAME" "$PARTITIONS" "$PRODUCER_RANKS" \
        "$CONSUMER_RANKS" "${BATCH_SIZE_FOR_STATUS:-}" "${LINGER_MS_FOR_STATUS:-}" \
        "${PAYLOAD_SIZE_FOR_STATUS:-}" "$reason" >> "$STATUS_FILE"
}

write_batch_summary() {
    python3 - "$BATCH_DIR" "$STATUS_FILE" "$BATCH_SUMMARY_FILE" <<'PY'
import csv
import json
import sys
import time
from pathlib import Path

batch_dir = Path(sys.argv[1])
status_path = Path(sys.argv[2])
output_path = Path(sys.argv[3])
rows = []
if status_path.is_file():
    with status_path.open("r", encoding="utf-8", newline="") as handle:
        latest = {}
        for row in csv.DictReader(handle, delimiter="\t"):
            config_id = str(row.get("config_id") or "").strip()
            if not config_id:
                continue
            latest[config_id] = {
                key: value.rstrip("\r") if isinstance(value, str) else value
                for key, value in row.items()
            }
        rows = list(latest.values())
rows.sort(key=lambda row: row.get("config_id", ""))
output_path.write_text(
    json.dumps(
        {
            "format": "kafka_simple_simultaneous_budgeted_batch.v1",
            "batch_dir": str(batch_dir),
            "case_count": len(rows),
            "completed_count": sum(1 for row in rows if row.get("status") == "completed"),
            "failed_count": sum(1 for row in rows if str(row.get("status", "")).startswith("failed")),
            "skipped_count": sum(1 for row in rows if str(row.get("status", "")).startswith("skipped")),
            "finished_unix": int(time.time()),
            "cases": rows,
        },
        indent=2,
        sort_keys=True,
    ),
    encoding="utf-8",
)
PY
}

slurm_deadline_unix() {
    if [[ -z "${SLURM_JOB_ID:-}" ]]; then
        return 1
    fi
    local end_time
    end_time="$(scontrol show job "$SLURM_JOB_ID" -o 2>/dev/null | tr ' ' '\n' | awk -F= '$1=="EndTime"{print $2; exit}' || true)"
    if [[ -z "$end_time" || "$end_time" == "Unknown" || "$end_time" == "N/A" ]]; then
        return 1
    fi
    date -d "$end_time" +%s 2>/dev/null || return 1
}

time_guard_allows_start() {
    local duration_sec="${1:-30}"
    local deadline="${SWEEP_DEADLINE_UNIX:-}"
    if [[ -z "$deadline" ]]; then
        return 0
    fi
    local now remaining required
    now="$(date +%s)"
    remaining=$(( deadline - now ))
    required=$(( SWEEP_STOP_MARGIN_MIN * 60 + duration_sec + ${SWEEP_CASE_OVERHEAD_SEC:-90} ))
    if (( remaining < required )); then
        log_warn "Time guard stopping batch: remaining=${remaining}s required=${required}s"
        return 1
    fi
    return 0
}

topic_exists_for_cleanup() {
    local bootstrap_servers="${1:?bootstrap servers required}"
    local topic_name="${2:?topic name required}"
    local log_file="${3:?log file required}"
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$bootstrap_servers" \
        --describe \
        --topic "$topic_name" \
        >"$log_file" 2>&1
}

wait_for_topic_deleted_for_cleanup() {
    local bootstrap_servers="${1:?bootstrap servers required}"
    local topic_name="${2:?topic name required}"
    local log_dir="${3:?log dir required}"
    local timeout_sec="${SWEEP_TOPIC_CLEANUP_TIMEOUT_SEC:-120}"
    local poll_sec="${SWEEP_TOPIC_CLEANUP_POLL_SEC:-5}"
    local deadline exists_log
    exists_log="$log_dir/post_case_topic_exists.log"
    deadline=$((SECONDS + timeout_sec))
    while (( SECONDS < deadline )); do
        if ! topic_exists_for_cleanup "$bootstrap_servers" "$topic_name" "$exists_log"; then
            return 0
        fi
        sleep "$poll_sec"
    done
    log_warn "Topic '$topic_name' still exists after ${timeout_sec}s; see $exists_log"
    return 1
}

wait_for_broker_ram_free_after_cleanup() {
    if [[ "${ENABLE_RAM_BACKED_RUNTIME:-1}" != "1" ]]; then
        return 0
    fi
    if [[ "${SWEEP_CHECK_RAM_FREE_AFTER_TOPIC_DELETE:-1}" != "1" ]]; then
        return 0
    fi
    local threshold_percent="${SWEEP_MIN_RAM_FREE_PERCENT:-20}"
    local timeout_sec="${SWEEP_RAM_FREE_TIMEOUT_SEC:-120}"
    local poll_sec="${SWEEP_TOPIC_CLEANUP_POLL_SEC:-5}"
    local log_file="${1:?log file required}"
    local broker_node="${BROKER_NODES[0]:-}"
    if [[ -z "$broker_node" ]]; then
        return 0
    fi
    local deadline used_percent free_percent q_root
    printf -v q_root '%q' "$KAFKA_RAM_ROOT"
    deadline=$((SECONDS + timeout_sec))
    while (( SECONDS < deadline )); do
        used_percent="$(srun --overlap --nodes=1 --ntasks=1 -w "$broker_node" bash -lc "df -P ${q_root} 2>/dev/null | tail -n 1 | tr -s ' ' | cut -d ' ' -f 5 | tr -d '%'" 2>>"$log_file" | tail -n 1 || true)"
        if [[ "$used_percent" =~ ^[0-9]+$ ]]; then
            free_percent=$((100 - used_percent))
            printf '[%s] broker=%s ram_root=%s used_percent=%s free_percent=%s threshold=%s\n' "$(date -Is)" "$broker_node" "$KAFKA_RAM_ROOT" "$used_percent" "$free_percent" "$threshold_percent" >> "$log_file"
            if (( free_percent >= threshold_percent )); then
                return 0
            fi
        else
            printf '[%s] broker=%s ram_root=%s used_percent_unavailable=%s\n' "$(date -Is)" "$broker_node" "$KAFKA_RAM_ROOT" "$used_percent" >> "$log_file"
        fi
        sleep "$poll_sec"
    done
    log_warn "Broker RAM-backed runtime free space stayed below ${threshold_percent}% after topic cleanup; see $log_file"
    return 1
}

cleanup_case_topic_after_run() {
    if [[ "${SWEEP_DELETE_TOPIC_AFTER_CASE:-1}" != "1" ]]; then
        return 0
    fi
    local topic_name="${TOPIC_NAME:?TOPIC_NAME required}"
    local bootstrap_file="$CASE_DIR/runtime/bootstrap_servers.txt"
    local log_dir="$CASE_DIR/logs/topics"
    local delete_log="$log_dir/post_case_delete_topic.log"
    local ram_log="$log_dir/post_case_ram_free.log"
    require_file "$bootstrap_file"
    ensure_dir "$log_dir"
    local bootstrap_servers
    bootstrap_servers="$(<"$bootstrap_file")"
    require_nonempty "BOOTSTRAP_SERVERS" "$bootstrap_servers"

    local previous_case_dir="$CASE_DIR"
    record_benchmark_event "topic_cleanup_start" "Benchmark topic cleanup starts"
    log_info "Deleting sweep topic after measured case: $topic_name"
    if "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$bootstrap_servers" \
        --delete \
        --topic "$topic_name" \
        >"$delete_log" 2>&1; then
        wait_for_topic_deleted_for_cleanup "$bootstrap_servers" "$topic_name" "$log_dir" || return 1
    else
        if topic_exists_for_cleanup "$bootstrap_servers" "$topic_name" "$log_dir/post_case_topic_exists_after_delete_error.log"; then
            log_warn "Failed to delete topic '$topic_name'; see $delete_log"
            return 1
        fi
    fi
    export CASE_DIR="$SERVICE_CASE_DIR"
    if ! "$SCRIPT_DIR/wait_for_brokers.sh"; then
        export CASE_DIR="$previous_case_dir"
        return 1
    fi
    export CASE_DIR="$previous_case_dir"
    wait_for_broker_ram_free_after_cleanup "$ram_log" || return 1
    record_benchmark_event "topic_cleanup_end" "Benchmark topic cleanup ends"
}

reset_case_broker_storage() {
    local log_dir="$CASE_DIR/logs/brokers"
    local node node_id storage_path q_storage
    ensure_dir "$log_dir"
    for ((node_id=1; node_id<=${#BROKER_NODES[@]}; node_id++)); do
        node="${BROKER_NODES[$((node_id - 1))]}"
        if [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
            storage_path="$KAFKA_RAM_ROOT/broker-${node_id}"
        else
            storage_path="/dev/shm/kafka-broker-${USER:-user}-${SLURM_JOB_ID:-manual}-${node_id}"
        fi
        printf -v q_storage '%q' "$storage_path"
        log_info "Resetting Kafka broker storage on $node: $storage_path"
        srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc "
            set -euo pipefail
            rm -rf ${q_storage}
            if [[ -e ${q_storage} ]]; then
                printf 'storage path still exists after cleanup: %s\\n' ${q_storage} >&2
                exit 1
            fi
            df -P /dev/shm || true
        " >>"$log_dir/storage-reset-${node}.log" 2>&1
    done
}

start_case_kafka() {
    reset_case_broker_storage
    unset KAFKA_CLUSTER_ID
    KAFKA_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export KAFKA_START_UNIX
    record_benchmark_event "kafka_start_requested" "Kafka broker start requested"
    BROKER_RUNNING=1
    "$SCRIPT_DIR/start_brokers.sh"
    require_file "$CASE_DIR/runtime/bootstrap_servers.txt"
    "$SCRIPT_DIR/wait_for_brokers.sh"
    record_benchmark_event "kafka_ready" "Kafka broker ready"
    if [[ "$ENABLE_MONITORING" == "1" && "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        "$SCRIPT_DIR/start_kafka_exporter.sh"
        KAFKA_EXPORTER_RUNNING=1
    fi
}

start_case_broker_process_monitor() {
    if [[ "$ENABLE_BROKER_PROCESS_MONITOR" != "1" ]]; then
        return 0
    fi
    BROKER_PROCESS_MONITOR_RUNNING=1
    if ! "$SCRIPT_DIR/start_broker_process_monitor.sh"; then
        stop_case_broker_process_monitor || true
        return 1
    fi
}

stop_case_broker_process_monitor() {
    if [[ "${BROKER_PROCESS_MONITOR_RUNNING:-0}" != "1" ]]; then
        return 0
    fi
    "$SCRIPT_DIR/stop_broker_process_monitor.sh"
    BROKER_PROCESS_MONITOR_RUNNING=0
}

stop_case_kafka() {
    local status=0
    stop_case_broker_process_monitor || status=1
    if [[ "${KAFKA_EXPORTER_RUNNING:-0}" == "1" ]]; then
        "$SCRIPT_DIR/stop_kafka_exporter.sh" || status=1
        KAFKA_EXPORTER_RUNNING=0
    fi
    if [[ "${BROKER_RUNNING:-0}" == "1" ]]; then
        record_benchmark_event "kafka_stop_requested" "Kafka broker stop requested"
        CLEAN_BROKER_RAM_DIRS=0 "$SCRIPT_DIR/stop_brokers.sh" || status=1
        BROKER_RUNNING=0
        reset_case_broker_storage || status=1
        record_benchmark_event "kafka_storage_clean" "Kafka broker storage removed"
    fi
    return "$status"
}

run_post_batch_analysis() {
    if [[ -n "${SWEEP_BATCH_DIR_OVERRIDE:-}" ]]; then
        log_info "Deferring campaign-specific analysis for overridden batch output"
        return 0
    fi
    local run_root="$PROJECT_ROOT/results/sweeps/$SWEEP_ID/$RUN_ID"
    python3 "$SCRIPT_DIR/aggregate_simultaneous_budgeted_results.py" --run-dir "$run_root"
    python3 "$SCRIPT_DIR/analyze_simultaneous_budgeted_results.py" --run-dir "$run_root"
}

cleanup_services() {
    local exit_code="${1:-0}"
    log_info "Stopping sweep services"
    stop_case_broker_process_monitor || log_warn "Kafka process monitor cleanup failed"
    if [[ "$KAFKA_RESTART_PER_CASE" == "1" ]]; then
        stop_case_kafka || log_warn "per-case Kafka cleanup failed"
    fi
    export CASE_DIR="$SERVICE_CASE_DIR"
    if [[ "$ENABLE_MONITORING" == "1" ]]; then
        if [[ "$KAFKA_RESTART_PER_CASE" != "1" && "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
            "$SCRIPT_DIR/stop_kafka_exporter.sh" || log_warn "stop_kafka_exporter.sh failed"
        fi
        "$SCRIPT_DIR/stop_monitoring.sh" || log_warn "stop_monitoring.sh failed"
        if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
            "$SCRIPT_DIR/stop_node_exporter.sh" || log_warn "stop_node_exporter.sh failed"
        fi
    fi
    if [[ "$KAFKA_RESTART_PER_CASE" != "1" ]]; then
        "$SCRIPT_DIR/stop_brokers.sh" || log_warn "stop_brokers.sh failed"
    fi
    write_batch_summary || true
    run_post_batch_analysis || true
    if [[ "$exit_code" == "0" ]]; then
        log_info "Sweep cleanup completed successfully"
    else
        log_warn "Sweep cleanup completed after failure"
    fi
}

cleanup() {
    local exit_code=$?
    if [[ "${SERVICES_STARTED:-0}" == "1" ]]; then
        cleanup_services "$exit_code"
    fi
    exit "$exit_code"
}
trap cleanup EXIT

normalize_batch_manifest

if [[ -n "${SLURM_JOB_NODELIST:-}" ]]; then
    scontrol show hostnames "$SLURM_JOB_NODELIST" > "$NODES_FILE"
else
    hostname > "$NODES_FILE"
fi

{
    printf 'config_id\tstatus\tcase_id\tcase_name\tconfig_path\tcase_dir\tstarted_unix\tfinished_unix\tscenario\ttopic_name\tpartitions\tproducer_ranks\tconsumer_ranks\tbatch_size\tlinger_ms\tpayload_size_bytes\treason\n'
} > "$STATUS_FILE"

PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$MANIFEST_TSV" "$PROJECT_ROOT" <<'PY'
import csv
import os
import sys
from pathlib import Path

from src.benchmark.broker_profile import resolve_profile_for_config
from src.benchmark.config_loader import load_benchmark_config
from src.benchmark.qualification import COMMON_QUALIFICATION_POLICY_ID

manifest_path = Path(sys.argv[1])
project_root = Path(sys.argv[2])
declared_profiles = set()
reproducible_modes = set()
with manifest_path.open("r", encoding="utf-8", newline="") as handle:
    for row in csv.DictReader(handle, delimiter="\t"):
        config_path = Path(row["config_path"])
        config = load_benchmark_config(config_path)
        profile = resolve_profile_for_config(
            config,
            config_path=config_path,
            project_root=project_root,
        )
        declared_profiles.add(
            None if profile is None else (profile.profile_id, profile.sha256)
        )
        extra = config.extra if isinstance(config.extra, dict) else {}
        metadata = extra.get("campaign_metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        reproducible = (
            metadata.get("measurement_contract_id")
            == "measurement.messaging.reproducible.v1"
        )
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

restart_per_case = os.environ.get("KAFKA_RESTART_PER_CASE", "0") == "1"
if len(reproducible_modes) != 1 or restart_per_case != (reproducible_modes == {True}):
    raise SystemExit("KAFKA_RESTART_PER_CASE does not match the batch measurement contract")
if len(declared_profiles) > 1 and not restart_per_case:
    rendered = sorted("legacy/no-profile" if item is None else item[0] for item in declared_profiles)
    raise SystemExit(
        "A historical batch starts Kafka once and cannot mix broker profiles: "
        + ", ".join(rendered)
    )
PY

FIRST_CONFIG="$(python3 - "$MANIFEST_TSV" <<'PY'
import csv
import sys
from pathlib import Path
with Path(sys.argv[1]).open("r", encoding="utf-8", newline="") as handle:
    row = next(csv.DictReader(handle, delimiter="\t"))
    print(row["config_path"])
PY
)"

load_case_exports "$FIRST_CONFIG"
configure_ram_backed_runtime
export CASE_ID="service_${JOB_TOKEN}"
export CASE_NAME="${BATCH_ID}_services"
export CASE_DIR="$SERVICE_CASE_DIR"
prepare_case_directories "$CASE_DIR"
write_env_snapshot "$CASE_DIR/runtime/env_snapshot.txt"
split_nodes "$BROKER_COUNT"
prepare_ram_backed_runtime_dirs

log_info "Sweep run: $RUN_ID"
log_info "Batch: $BATCH_ID"
log_info "Batch directory: $BATCH_DIR"
log_info "Allocated nodes: $(join_by , "${ALL_NODES[@]}")"
log_info "Broker nodes: $(join_by , "${BROKER_NODES[@]}")"
log_info "Monitoring node: ${MONITORING_NODE}"
log_info "Benchmark nodes: $(join_by , "${BENCHMARK_NODES[@]}")"

if SWEEP_DEADLINE_UNIX="$(slurm_deadline_unix)"; then
    export SWEEP_DEADLINE_UNIX
    log_info "Slurm deadline unix: $SWEEP_DEADLINE_UNIX"
else
    SWEEP_DEADLINE_UNIX=""
    export SWEEP_DEADLINE_UNIX
    log_warn "Could not determine Slurm deadline; time guard will use best effort only"
fi

if [[ "$ENABLE_SYSTEM_INVENTORY" == "1" ]]; then
    log_info "Collecting system capability inventory once before sweep Kafka starts"
    bash "$SCRIPT_DIR/collect_system_inventory.sh" || log_warn "System capability inventory collection failed"
fi

SERVICES_STARTED=1
export SERVICES_STARTED
BROKER_RUNNING=0
KAFKA_EXPORTER_RUNNING=0
BROKER_PROCESS_MONITOR_RUNNING=0

if [[ "$ENABLE_MONITORING" == "1" ]]; then
    if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
        log_info "Starting node_exporter"
        "$SCRIPT_DIR/start_node_exporter.sh"
    fi
    log_info "Starting Prometheus"
    "$SCRIPT_DIR/start_monitoring.sh"
    if [[ -f "$CASE_DIR/runtime/monitoring/prometheus_endpoint.txt" ]]; then
        PROMETHEUS_ENDPOINT="$(<"$CASE_DIR/runtime/monitoring/prometheus_endpoint.txt")"
        export PROMETHEUS_ENDPOINT
    fi
fi

if [[ "$KAFKA_RESTART_PER_CASE" != "1" ]]; then
    log_info "Starting Kafka broker once for historical sweep batch"
    KAFKA_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export KAFKA_START_UNIX
    record_benchmark_event "kafka_start_requested" "Kafka broker start requested" "slurm" "$KAFKA_START_UNIX"
    BROKER_RUNNING=1
    "$SCRIPT_DIR/start_brokers.sh"
    require_file "$CASE_DIR/runtime/bootstrap_servers.txt"
    "$SCRIPT_DIR/wait_for_brokers.sh"
    record_benchmark_event "kafka_ready" "Kafka broker ready"

    if [[ "$ENABLE_MONITORING" == "1" && "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        log_info "Starting kafka_exporter"
        "$SCRIPT_DIR/start_kafka_exporter.sh"
        KAFKA_EXPORTER_RUNNING=1
    fi
fi

case_counter=0
skip_remaining_status=""
skip_remaining_reason=""
mapfile -t SWEEP_MANIFEST_LINES < "$MANIFEST_TSV"
for manifest_line in "${SWEEP_MANIFEST_LINES[@]}"; do
    IFS=$'	' read -r config_id config_path scenario topic_name partitions producer_ranks consumer_ranks batch_size linger_ms payload_size_bytes <<< "$manifest_line"
    if [[ -z "${config_id:-}" || "$config_id" == "config_id" ]]; then
        continue
    fi
    case_counter=$((case_counter + 1))
    if (( case_counter > SWEEP_MAX_CASES )) && [[ -z "$skip_remaining_status" ]]; then
        log_warn "SWEEP_MAX_CASES=$SWEEP_MAX_CASES reached; marking remaining cases as skipped_max_cases"
        skip_remaining_status="skipped_max_cases"
        skip_remaining_reason="SWEEP_MAX_CASES reached"
    fi

    BATCH_SIZE_FOR_STATUS="$batch_size"
    LINGER_MS_FOR_STATUS="$linger_ms"
    PAYLOAD_SIZE_FOR_STATUS="$payload_size_bytes"

    case_id="$config_id"
    case_name="$config_id"
    case_dir="$CASES_DIR/$config_id"

    if [[ -n "$skip_remaining_status" ]]; then
        load_case_exports "$config_path"
        append_status "$config_id" "$skip_remaining_status" "$case_id" "$case_name" "$config_path" "$case_dir" "" "$(date +%s)" "$skip_remaining_reason"
        continue
    fi

    load_case_exports "$config_path"
    case_runtime_sec=$((
        ${CASE_WARMUP_SEC:-0}
        + ${CASE_MEASUREMENT_SEC:-30}
        + ${CASE_DRAIN_TIMEOUT_SEC:-0}
    ))
    if ! time_guard_allows_start "$case_runtime_sec"; then
        append_status "$config_id" "skipped_time_guard" "$case_id" "$case_name" "$config_path" "$case_dir" "" "$(date +%s)" "Slurm wall-time guard"
        skip_remaining_status="skipped_time_guard"
        skip_remaining_reason="Slurm wall-time guard"
        continue
    fi

    if [[ "${SWEEP_RESUME:-0}" == "1" && -f "$case_dir/final_report.json" ]]; then
        if python3 - "$case_dir/final_report.json" <<'PY'
import json
import sys
from pathlib import Path
path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
raise SystemExit(0 if data.get("case", {}).get("status") == "completed" else 1)
PY
        then
            append_status "$config_id" "skipped_resume_completed" "$case_id" "$case_name" "$config_path" "$case_dir" "" "$(date +%s)" "existing completed final_report.json"
            continue
        fi
    fi

    log_info "Starting sweep case $case_counter: $config_id ($config_path)"
    export CASE_ID="$case_id"
    export CASE_NAME="$case_name"
    export CASE_DIR="$case_dir"
    prepare_case_directories "$CASE_DIR"
    write_env_snapshot "$CASE_DIR/runtime/env_snapshot.txt"
    "$SCRIPT_DIR/backend_lifecycle.sh" kafka snapshot "$CONFIG_PATH"
    if [[ "$KAFKA_RESTART_PER_CASE" != "1" ]]; then
        cp -f "$SERVICE_CASE_DIR/runtime/bootstrap_servers.txt" "$CASE_DIR/runtime/bootstrap_servers.txt"
    fi
    if [[ -f "$SERVICE_CASE_DIR/runtime/system_inventory/system_inventory.json" ]]; then
        mkdir -p "$CASE_DIR/runtime/system_inventory" "$CASE_DIR/data"
        cp -f "$SERVICE_CASE_DIR/runtime/system_inventory/system_inventory.json" "$CASE_DIR/runtime/system_inventory/system_inventory.json"
        cp -f "$SERVICE_CASE_DIR/runtime/system_inventory/system_inventory.json" "$CASE_DIR/data/system_inventory.json"
    fi
    if [[ -n "${PROMETHEUS_ENDPOINT:-}" ]]; then
        mkdir -p "$CASE_DIR/runtime/monitoring"
        printf '%s\n' "$PROMETHEUS_ENDPOINT" > "$CASE_DIR/runtime/monitoring/prometheus_endpoint.txt"
    fi

    started_unix="$(date +%s)"
    append_status "$config_id" "running" "$case_id" "$case_name" "$config_path" "$case_dir" "$started_unix" "" ""

    if [[ "$KAFKA_RESTART_PER_CASE" == "1" ]]; then
        log_info "Starting fresh Kafka broker for reproducible case $config_id"
        start_case_kafka
        start_case_broker_process_monitor
    fi

    record_benchmark_event "topic_create_start" "Benchmark topic creation starts"
    "$SCRIPT_DIR/create_topics.sh"
    record_benchmark_event "topic_ready" "Benchmark topic ready"

    MONITORING_WINDOW_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    BENCHMARK_START_UNIX="$MONITORING_WINDOW_START_UNIX"
    export MONITORING_WINDOW_START_UNIX BENCHMARK_START_UNIX
    record_benchmark_event "mpi_launch" "MPI benchmark launch" "slurm" "$BENCHMARK_START_UNIX"

    if "$SCRIPT_DIR/run_case.sh" </dev/null; then
        BENCHMARK_END_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
        export BENCHMARK_END_UNIX
        record_benchmark_event "benchmark_window_end" "Benchmark window ends" "slurm" "$BENCHMARK_END_UNIX"
        write_benchmark_window
        write_monitoring_window
        health_status=0
        if "$SCRIPT_DIR/backend_lifecycle.sh" kafka check-health "$CONFIG_PATH"; then
            record_benchmark_event "backend_health_passed" \
                "Kafka remained healthy through the workload"
        else
            health_status=$?
            record_benchmark_event "backend_health_failed" \
                "Kafka failed its post-workload health check"
            log_error "Kafka failed its post-workload health check for $config_id"
        fi
        stop_case_broker_process_monitor || log_warn "broker process monitor stop failed for $config_id"
        "$SCRIPT_DIR/collect_results.sh" || log_warn "collect_results.sh failed for $config_id"
        if [[ "$KAFKA_RESTART_PER_CASE" == "1" ]]; then
            case_cleanup_command=stop_case_kafka
            case_cleanup_failure="post-case Kafka stop/storage cleanup failed"
        else
            case_cleanup_command=cleanup_case_topic_after_run
            case_cleanup_failure="post-case topic cleanup failed"
        fi
        if "$case_cleanup_command" && (( health_status == 0 )); then
            append_status "$config_id" "completed" "$case_id" "$case_name" "$config_path" "$case_dir" "$started_unix" "$(date +%s)" ""
        else
            if (( health_status != 0 )); then
                append_status "$config_id" "failed:backend_health" "$case_id" "$case_name" "$config_path" "$case_dir" "$started_unix" "$(date +%s)" "post-workload Kafka health check failed"
                die "post-workload Kafka health check failed for $config_id"
            fi
            append_status "$config_id" "failed:case_cleanup" "$case_id" "$case_name" "$config_path" "$case_dir" "$started_unix" "$(date +%s)" "$case_cleanup_failure"
            die "$case_cleanup_failure for $config_id"
        fi
    else
        run_status=$?
        BENCHMARK_END_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
        export BENCHMARK_END_UNIX
        record_benchmark_event "benchmark_window_end" "Benchmark window ends" "slurm" "$BENCHMARK_END_UNIX"
        write_benchmark_window
        write_monitoring_window
        stop_case_broker_process_monitor || true
        "$SCRIPT_DIR/collect_results.sh" || true
        if [[ "$KAFKA_RESTART_PER_CASE" == "1" ]]; then
            stop_case_kafka || log_warn "post-failure Kafka cleanup failed for $config_id"
        else
            cleanup_case_topic_after_run || log_warn "post-failure topic cleanup failed for $config_id"
        fi
        append_status "$config_id" "failed:${run_status}" "$case_id" "$case_name" "$config_path" "$case_dir" "$started_unix" "$(date +%s)" "run_case.sh failed"
        if (( ${SWEEP_MAX_FAILURES:-5} > 0 )); then
            failed_count="$(awk -F '\t' 'NR>1 && $2 ~ /^failed/ {count++} END {print count+0}' "$STATUS_FILE")"
            if (( failed_count >= ${SWEEP_MAX_FAILURES:-5} )); then
                die "Stopping sweep after $failed_count failures"
            fi
        fi
    fi
done

write_batch_summary
run_post_batch_analysis

log_info "Sweep batch completed: $BATCH_DIR"
