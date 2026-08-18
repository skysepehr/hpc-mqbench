#!/usr/bin/env bash
set -euo pipefail

#SBATCH --job-name=kafka-simple-mpi
#SBATCH --output=logs/slurm-%j.out
#SBATCH --time=01:00:00

# -----------------------------------------------------------------------------
# Slurm entrypoint for one MPI messaging benchmark case.
#
# The current adapters share one single-case MPI/Python benchmark shape:
# - one backend service node
# - one monitoring node
# - one producer benchmark node, also hosting MPI rank 0
# - one consumer benchmark node
# -----------------------------------------------------------------------------

# Some Slurm releases export SLURM_EXCLUSIVE=1 for an exclusive sbatch
# allocation. Nested srun commands parse that environment variable as an
# option value and reject it. The allocation is already exclusive, so remove
# only the inherited client-side option before launching job steps.
unset SLURM_EXCLUSIVE

if [[ -n "${PROJECT_ROOT:-}" && -f "$PROJECT_ROOT/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" && -f "$SLURM_SUBMIT_DIR/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$SLURM_SUBMIT_DIR" && pwd)"
else
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
cd "$PROJECT_ROOT"
SCRIPT_DIR="$PROJECT_ROOT/scripts"

# shellcheck source=./scripts/common.sh
source "$SCRIPT_DIR/common.sh"
# shellcheck source=./scripts/hpc_modules.sh
source "$SCRIPT_DIR/hpc_modules.sh"
# shellcheck source=./scripts/python_env_common.sh
source "$SCRIPT_DIR/python_env_common.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

if [[ $# -lt 1 ]]; then
    die "Usage: sbatch run_all.sh <path-to-single-config.json>"
fi

PREMODULE_CONFIG_PATH="$(abspath "$1")"
require_file "$PREMODULE_CONFIG_PATH"
REQUESTED_BACKEND_ID="${BACKEND_ID:-}"
if [[ -z "$REQUESTED_BACKEND_ID" ]]; then
    REQUESTED_BACKEND_ID="$(python3 - "$PREMODULE_CONFIG_PATH" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
print(str(payload.get("backend_id", "kafka")).strip() or "kafka")
PY
)"
fi
export BACKEND_ID="$REQUESTED_BACKEND_ID"

select_backend_hpc_modules "$REQUESTED_BACKEND_ID"

export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"
load_hpc_modules
export_benchmark_hpc_python_env "$PROJECT_ROOT"

INPUT_CONFIG_PATH="$(abspath "$1")"
require_file "$INPUT_CONFIG_PATH"

for command_name in python3 srun scontrol mpirun; do
    require_command "$command_name"
done

python3 - <<'PY' || die "python3 must be >= 3.10 after module loading"
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY

BACKEND_ID="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$INPUT_CONFIG_PATH" <<'PY'
import sys
from src.benchmark.config_loader import load_benchmark_config
print(load_benchmark_config(sys.argv[1]).backend_id)
PY
)"
require_nonempty "BACKEND_ID" "$BACKEND_ID"
"$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" validate \
    "$INPUT_CONFIG_PATH" >/dev/null

if [[ "$BACKEND_ID" == "kafka" ]]; then
    require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
    require_command java
    require_file "$KAFKA_HOME/bin/kafka-server-start.sh"
    require_file "$KAFKA_HOME/bin/kafka-storage.sh"
    require_file "$KAFKA_HOME/bin/kafka-topics.sh"
    require_file "$KAFKA_HOME/bin/kafka-broker-api-versions.sh"
    python3 -c 'import confluent_kafka' || \
        die "confluent_kafka is unavailable in the Slurm runtime"
elif [[ "$BACKEND_ID" == "pulsar" ]]; then
    PULSAR_HOME="${PULSAR_HOME:-$PROJECT_ROOT/.local/pulsar-current}"
    export PULSAR_HOME
    require_nonempty "PULSAR_HOME" "${PULSAR_HOME:-}"
    require_file "$PULSAR_HOME/bin/pulsar"
    require_file "$PULSAR_HOME/bin/pulsar-admin"
    python3 - <<'PY' || die "pulsar-client or its certifi dependency is unavailable in the Slurm runtime"
from pathlib import Path

import certifi
import pulsar

assert Path(certifi.where()).is_file()
assert pulsar.__version__ == "3.13.0"
client = pulsar.Client("pulsar://127.0.0.1:1", operation_timeout_seconds=1)
client.close()
PY
fi

ENABLE_MONITORING="${ENABLE_MONITORING:-1}"
ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
ENABLE_KAFKA_EXPORTER="${ENABLE_KAFKA_EXPORTER:-1}"
ENABLE_SYSTEM_INVENTORY="${ENABLE_SYSTEM_INVENTORY:-1}"
ENABLE_BROKER_PROCESS_MONITOR="${ENABLE_BROKER_PROCESS_MONITOR:-1}"
ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
ENABLE_EGRESS_PREFILL="${ENABLE_EGRESS_PREFILL:-1}"
CLEAN_BROKER_RAM_DIRS="${CLEAN_BROKER_RAM_DIRS:-1}"
CLEAN_STALE_KAFKA_RAM_ROOTS="${CLEAN_STALE_KAFKA_RAM_ROOTS:-0}"
DELETE_TOPIC_FIRST="${DELETE_TOPIC_FIRST:-1}"

JMX_EXPORTER_JAR="${JMX_EXPORTER_JAR:-$PROJECT_ROOT/tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar}"
JMX_EXPORTER_CONFIG="${JMX_EXPORTER_CONFIG:-$PROJECT_ROOT/monitoring/jmx_exporter_config.yml}"
PROMETHEUS_HOME="${PROMETHEUS_HOME:-}"
NODE_EXPORTER_HOME="${NODE_EXPORTER_HOME:-}"
KAFKA_EXPORTER_HOME="${KAFKA_EXPORTER_HOME:-$PROJECT_ROOT/tools/kafka_exporter-current}"
KAFKA_EXPORTER_PORT="${KAFKA_EXPORTER_PORT:-9308}"

if [[ "$BACKEND_ID" == "kafka" ]]; then
    require_file "$JMX_EXPORTER_JAR"
    require_file "$JMX_EXPORTER_CONFIG"
else
    ENABLE_KAFKA_EXPORTER=0
fi

if [[ "$ENABLE_MONITORING" == "1" ]]; then
    require_nonempty "PROMETHEUS_HOME" "$PROMETHEUS_HOME"
    require_file "$PROMETHEUS_HOME/prometheus"

    if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
        require_nonempty "NODE_EXPORTER_HOME" "$NODE_EXPORTER_HOME"
        require_file "$NODE_EXPORTER_HOME/node_exporter"
    fi

    if [[ "$BACKEND_ID" == "kafka" && "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        require_file "$KAFKA_EXPORTER_HOME/kafka_exporter"
    fi
fi

ensure_dir "$PROJECT_ROOT/logs"
ensure_dir "$PROJECT_ROOT/results"

export PROJECT_ROOT
export KAFKA_HOME="${KAFKA_HOME:-}"
export BACKEND_ID
export KAFKA_LISTENER_PORT="${KAFKA_LISTENER_PORT:-9092}"
export KAFKA_CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"
export KAFKA_HPC_NETWORK_INTERFACE="${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
export KAFKA_HPC_REQUIRE_FABRIC="${KAFKA_HPC_REQUIRE_FABRIC:-1}"
export BENCHMARK_NETWORK_INTERFACE="${BENCHMARK_NETWORK_INTERFACE:-$KAFKA_HPC_NETWORK_INTERFACE}"
export BENCHMARK_REQUIRE_FABRIC="${BENCHMARK_REQUIRE_FABRIC:-$KAFKA_HPC_REQUIRE_FABRIC}"
export KAFKA_JMX_PORT_BASE="${KAFKA_JMX_PORT_BASE:-7101}"
export JMX_EXPORTER_PORT_BASE="${JMX_EXPORTER_PORT_BASE:-$KAFKA_JMX_PORT_BASE}"
export FORMAT_STORAGE="${FORMAT_STORAGE:-1}"
export ENABLE_MONITORING
export ENABLE_NODE_EXPORTER
export ENABLE_KAFKA_EXPORTER
export ENABLE_SYSTEM_INVENTORY
export ENABLE_BROKER_PROCESS_MONITOR
export ENABLE_RAM_BACKED_RUNTIME
export ENABLE_EGRESS_PREFILL
export CLEAN_BROKER_RAM_DIRS
export CLEAN_STALE_KAFKA_RAM_ROOTS
export DELETE_TOPIC_FIRST
export JMX_EXPORTER_JAR
export JMX_EXPORTER_CONFIG
export PROMETHEUS_HOME
export NODE_EXPORTER_HOME
export KAFKA_EXPORTER_HOME
export KAFKA_EXPORTER_PORT
export BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"
export SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"
export SLURM_PRODUCER_NODES="${SLURM_PRODUCER_NODES:-1}"
export SLURM_CONSUMER_NODES="${SLURM_CONSUMER_NODES:-1}"

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
    if [[ "${BACKEND_ID:-kafka}" == "pulsar" ]]; then
        export PULSAR_RAM_ROOT="${PULSAR_RAM_ROOT:-$KAFKA_RAM_ROOT/pulsar}"
    fi

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
        " > "$CASE_DIR/logs/ram-runtime-${node}.log" 2>&1 || \
            log_warn "Could not prepare RAM runtime directories on $node; see logs/ram-runtime-${node}.log"
    done
}

CASE_RUNNING=0

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
if path.is_file():
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = {}
else:
    payload = {}
events = payload.get("events", []) if isinstance(payload, dict) else []
if not isinstance(events, list):
    events = []
events.append(
    {
        "name": name,
        "label": label,
        "source": source,
        "unix": unix_time,
        "iso": dt.datetime.fromtimestamp(unix_time, tz=dt.timezone.utc).isoformat(),
    }
)
payload = {"format": "benchmark_events.v1", "events": sorted(events, key=lambda item: float(item.get("unix", 0.0)))}
path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY
}

read_config_values() {
    local config_path="${1:?config path is required}"

    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$config_path" <<'PY'
import shlex
import sys
from pathlib import Path

from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config

config = load_benchmark_config(Path(sys.argv[1]))
if config.mode != "single":
    raise SystemExit("The current runner supports only mode='single'")

pairs = get_backend(config.backend_id).runtime_environment(config)

extra = config.extra if isinstance(config.extra, dict) else {}
message_limit = None
if config.backend_id == "kafka":
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
            "KAFKA_RECEIVE_MESSAGE_MAX_BYTES": str(
                int(extra.get("kafka_receive_message_max_bytes", max(100_000_000, message_limit + 1_048_576)))
            ),
            "KAFKA_SOCKET_REQUEST_MAX_BYTES": str(
                int(extra.get("kafka_socket_request_max_bytes", max(104_857_600, message_limit + 16_777_216)))
            ),
            "KAFKA_REPLICA_FETCH_MAX_BYTES": str(
                int(extra.get("kafka_replica_fetch_max_bytes", message_limit))
            ),
            "KAFKA_TOPIC_MAX_MESSAGE_BYTES": str(
                int(extra.get("kafka_topic_max_message_bytes", message_limit))
            ),
            "KAFKA_LOG_SEGMENT_BYTES": str(
                int(extra.get("kafka_log_segment_bytes", min(2_147_483_647, max(1_073_741_824, message_limit + 67_108_864))))
            ),
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
    if config.backend_id == "kafka" and extra.get(extra_key) is not None:
        pairs[env_name] = str(extra[extra_key])

for key, value in pairs.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
}

load_case_exports() {
    local config_path="${1:?config path is required}"

    unset \
        KAFKA_MESSAGE_MAX_BYTES \
        KAFKA_RECEIVE_MESSAGE_MAX_BYTES \
        KAFKA_SOCKET_REQUEST_MAX_BYTES \
        KAFKA_REPLICA_FETCH_MAX_BYTES \
        KAFKA_TOPIC_MAX_MESSAGE_BYTES \
        KAFKA_LOG_SEGMENT_BYTES \
        KAFKA_HEAP_OPTS

    eval "$(read_config_values "$config_path")"
    if [[ "$BACKEND_ID" == "kafka" ]]; then
        eval "$(
            PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
                python3 "$SCRIPT_DIR/render_broker_profile_env.py" \
                --config "$config_path" \
                --project-root "$PROJECT_ROOT"
        )"
        validate_broker_count "$BROKER_COUNT"
        validate_replication_factor "$REPLICATION_FACTOR" "$BROKER_COUNT"
    elif [[ "$BACKEND_ID" == "pulsar" ]]; then
        PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
            python3 -B "$SCRIPT_DIR/verify_pulsar_profile.py" \
            "$config_path" --project-root "$PROJECT_ROOT" >/dev/null
    fi

    export CONFIG_PATH="$config_path"
    export BACKEND_ID
    export SERVICE_NODE_COUNT
    export BROKER_COUNT
    export PARTITIONS
    export REPLICATION_FACTOR
    export TOPIC_NAME
    export SCENARIO
    export PRODUCER_RANKS
    export CONSUMER_RANKS

    for optional_name in \
        KAFKA_MESSAGE_MAX_BYTES \
        KAFKA_RECEIVE_MESSAGE_MAX_BYTES \
        KAFKA_SOCKET_REQUEST_MAX_BYTES \
        KAFKA_REPLICA_FETCH_MAX_BYTES \
        KAFKA_TOPIC_MAX_MESSAGE_BYTES \
        KAFKA_LOG_SEGMENT_BYTES \
        KAFKA_HEAP_OPTS \
        KAFKA_NUM_NETWORK_THREADS \
        KAFKA_NUM_IO_THREADS \
        KAFKA_SOCKET_SEND_BUFFER_BYTES \
        KAFKA_SOCKET_RECEIVE_BUFFER_BYTES \
        KAFKA_QUEUED_MAX_REQUESTS; do
        if [[ -n "${!optional_name:-}" ]]; then
            export "$optional_name"
        fi
    done
    for pulsar_name in \
        PULSAR_SERVICE_COUNT \
        PULSAR_TOPIC_NAME \
        PULSAR_SUBSCRIPTION_NAME \
        PULSAR_PROFILE_ID \
        PULSAR_PROFILE_SHA256 \
        PULSAR_PRODUCT_VERSION \
        PULSAR_MEM; do
        if [[ -n "${!pulsar_name:-}" ]]; then
            export "$pulsar_name"
        fi
    done
    for profile_name in \
        BROKER_PROFILE_ID \
        BROKER_PROFILE_SHA256 \
        BROKER_PROFILE_MANIFEST_PATH; do
        if [[ -n "${!profile_name:-}" ]]; then
            export "$profile_name"
        fi
    done
}

write_benchmark_window() {
    local window_file="$CASE_DIR/runtime/benchmark_window.json"
    require_nonempty "BENCHMARK_START_UNIX" "${BENCHMARK_START_UNIX:-}"
    require_nonempty "BENCHMARK_END_UNIX" "${BENCHMARK_END_UNIX:-}"

    cat > "$window_file" <<EOF
{
  "start_unix": ${BENCHMARK_START_UNIX},
  "end_unix": ${BENCHMARK_END_UNIX}
}
EOF
    log_info "Benchmark monitoring window written to: $window_file"
}

write_monitoring_window() {
    local window_file="$CASE_DIR/runtime/monitoring_window.json"
    local start_unix="${MONITORING_WINDOW_START_UNIX:-${BACKEND_START_UNIX:-${KAFKA_START_UNIX:-${BENCHMARK_START_UNIX:-}}}}"
    local end_unix="${BENCHMARK_END_UNIX:-}"
    require_nonempty "MONITORING_WINDOW_START_UNIX" "$start_unix"
    require_nonempty "BENCHMARK_END_UNIX" "$end_unix"

    cat > "$window_file" <<EOF
{
  "start_unix": ${start_unix},
  "end_unix": ${end_unix}
}
EOF
    log_info "Monitoring graph window written to: $window_file"
}

cleanup_case() {
    local exit_code="${1:-0}"

    log_info "Collecting and stopping services for case: ${CASE_ID:-unknown}"

    if [[ "${ENABLE_BROKER_PROCESS_MONITOR:-1}" == "1" ]]; then
        BACKEND_MONITOR_COMPONENT=process \
            "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" stop-monitoring \
            "$CONFIG_PATH" || log_warn "backend process monitoring stop failed"
    fi

    if [[ -n "${CASE_DIR:-}" && -d "$CASE_DIR" ]]; then
        "$SCRIPT_DIR/collect_results.sh" || log_warn "collect_results.sh failed"
    fi

    if [[ "$ENABLE_MONITORING" == "1" ]]; then
        "$SCRIPT_DIR/stop_monitoring.sh" || log_warn "stop_monitoring.sh failed"

        if [[ "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
            BACKEND_MONITOR_COMPONENT=metrics \
                "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" stop-monitoring \
                "$CONFIG_PATH" || log_warn "backend metrics exporter stop failed"
        fi

        if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
            "$SCRIPT_DIR/stop_node_exporter.sh" || log_warn "stop_node_exporter.sh failed"
        fi
    fi

    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" stop "$CONFIG_PATH" || \
        log_warn "backend stop failed"

    if [[ "$exit_code" == "0" ]]; then
        log_info "Case cleanup completed successfully"
    else
        log_warn "Case cleanup completed after failure"
    fi
}

cleanup() {
    local exit_code=$?

    if [[ "$CASE_RUNNING" == "1" ]]; then
        log_info "Entering cleanup handler with exit code: $exit_code"
        cleanup_case "$exit_code"
    fi

    exit "$exit_code"
}
trap cleanup EXIT

run_one_case() {
    local case_config_path="${1:?case config path is required}"
    local case_id="${2:?case id is required}"
    local case_name="${3:?case name is required}"
    local case_dir="${4:?case dir is required}"

    load_case_exports "$case_config_path"

    export CASE_ID="$case_id"
    export CASE_NAME="$case_name"
    export CASE_DIR="$case_dir"

    configure_ram_backed_runtime

    prepare_case_directories "$CASE_DIR"
    write_env_snapshot "$CASE_DIR/runtime/env_snapshot.txt"
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" snapshot "$CONFIG_PATH"

    split_nodes "$SERVICE_NODE_COUNT"
    prepare_ram_backed_runtime_dirs

    log_info "Project root: $PROJECT_ROOT"
    log_info "Config path: $CONFIG_PATH"
    log_info "Case ID: $CASE_ID"
    log_info "Case name: $CASE_NAME"
    log_info "Case directory: $CASE_DIR"
    log_info "Allocated nodes: $(join_by , "${ALL_NODES[@]}")"
    log_info "Backend service nodes: $(join_by , "${SERVICE_NODES[@]}")"
    log_info "Monitoring node: ${MONITORING_NODE}"
    log_info "Benchmark nodes: $(join_by , "${BENCHMARK_NODES[@]}")"

    CASE_RUNNING=1

    if [[ "$ENABLE_SYSTEM_INVENTORY" == "1" ]]; then
        log_info "Collecting system capability inventory before the backend starts"
        bash "$SCRIPT_DIR/collect_system_inventory.sh" || \
            log_warn "System capability inventory collection failed"
    fi

    if [[ "$ENABLE_MONITORING" == "1" ]]; then
        if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
            record_benchmark_event "node_exporter_start_requested" "node_exporter start requested"
            log_info "Starting node_exporter before the backend so startup pressure is visible"
            "$SCRIPT_DIR/start_node_exporter.sh"
            record_benchmark_event "node_exporter_ready" "node_exporter ready"
        fi

        record_benchmark_event "prometheus_start_requested" "Prometheus start requested"
        log_info "Starting Prometheus before the backend so startup appears in monitoring graphs"
        "$SCRIPT_DIR/start_monitoring.sh"
        MONITORING_WINDOW_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
        export MONITORING_WINDOW_START_UNIX
        record_benchmark_event "prometheus_ready" "Prometheus ready" "slurm" "$MONITORING_WINDOW_START_UNIX"
    fi

    log_info "Starting $BACKEND_ID backend"
    BACKEND_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export BACKEND_START_UNIX
    # Compatibility alias for historical Kafka monitoring consumers.
    KAFKA_START_UNIX="$BACKEND_START_UNIX"
    export KAFKA_START_UNIX
    if [[ "$BACKEND_ID" == "kafka" ]]; then
        record_benchmark_event "kafka_start_requested" "Kafka broker start requested" "slurm" "$BACKEND_START_UNIX"
    else
        record_benchmark_event "backend_start_requested" "$BACKEND_ID backend start requested" "slurm" "$BACKEND_START_UNIX"
    fi
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" start "$CONFIG_PATH"

    local bootstrap_servers_file="$CASE_DIR/runtime/bootstrap_servers.txt"
    require_file "$bootstrap_servers_file"

    local bootstrap_servers
    bootstrap_servers="$(<"$bootstrap_servers_file")"
    require_nonempty "BOOTSTRAP_SERVERS" "$bootstrap_servers"
    log_info "Bootstrap servers: $bootstrap_servers"

    log_info "Waiting for broker readiness"
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" wait-ready "$CONFIG_PATH"
    if [[ "$BACKEND_ID" == "kafka" ]]; then
        record_benchmark_event "kafka_ready" "Kafka broker ready"
    else
        record_benchmark_event "backend_ready" "$BACKEND_ID backend ready"
    fi

    if [[ "$ENABLE_BROKER_PROCESS_MONITOR" == "1" ]]; then
        BACKEND_MONITOR_COMPONENT=process \
            "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" start-monitoring \
            "$CONFIG_PATH"
    fi

    if [[ "$ENABLE_MONITORING" == "1" ]]; then
        if [[ "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
            record_benchmark_event "kafka_exporter_start_requested" "kafka_exporter start requested"
            log_info "Starting kafka_exporter on monitoring node"
            BACKEND_MONITOR_COMPONENT=metrics \
                "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" start-monitoring \
                "$CONFIG_PATH"
            record_benchmark_event "kafka_exporter_ready" "kafka_exporter ready"
        fi
    fi

    log_info "Creating benchmark stream"
    if [[ "$BACKEND_ID" == "kafka" ]]; then
        record_benchmark_event "topic_create_start" "Benchmark topic creation starts"
    else
        record_benchmark_event "stream_create_start" "Benchmark stream creation starts"
    fi
    "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" create-stream "$CONFIG_PATH"
    if [[ "$BACKEND_ID" == "kafka" ]]; then
        record_benchmark_event "topic_ready" "Benchmark topic ready"
    else
        record_benchmark_event "stream_ready" "Benchmark stream ready"
    fi

    log_info "Prefilling benchmark stream if needed"
    if [[ "${SCENARIO:-}" == "egress_only" ]]; then
        record_benchmark_event "egress_prefill_start" "Egress topic prefill starts"
        "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" prefill "$CONFIG_PATH"
        record_benchmark_event "egress_prefill_end" "Egress topic prefill ends"
    else
        "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" prefill "$CONFIG_PATH"
    fi

    log_info "Running MPI benchmark case"
    BENCHMARK_START_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
    export BENCHMARK_START_UNIX
    record_benchmark_event "mpi_launch" "MPI benchmark launch" "slurm" "$BENCHMARK_START_UNIX"
    if "$SCRIPT_DIR/run_case.sh"; then
        BENCHMARK_END_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
        export BENCHMARK_END_UNIX
        record_benchmark_event "benchmark_window_end" "Benchmark window ends" "slurm" "$BENCHMARK_END_UNIX"
        write_benchmark_window
        write_monitoring_window
    else
        run_status=$?
        BENCHMARK_END_UNIX="$(python3 -c 'import time; print(f"{time.time():.3f}")')"
        export BENCHMARK_END_UNIX
        record_benchmark_event "benchmark_window_end" "Benchmark window ends" "slurm" "$BENCHMARK_END_UNIX"
        write_benchmark_window
        write_monitoring_window
        return "$run_status"
    fi

    log_info "Checking $BACKEND_ID backend health after the workload"
    if "$SCRIPT_DIR/backend_lifecycle.sh" "$BACKEND_ID" check-health "$CONFIG_PATH"; then
        record_benchmark_event "backend_health_passed" \
            "$BACKEND_ID backend remained healthy through the workload"
    else
        local health_status=$?
        if [[ ! -s "$CASE_DIR/runtime/backend_health.json" ]]; then
            python3 -B "$PROJECT_ROOT/scripts/write_backend_health.py" \
                --output "$CASE_DIR/runtime/backend_health.json" \
                --backend-id "$BACKEND_ID" \
                --status failed \
                --check lifecycle_probe fail \
                "The backend health probe exited before writing structured evidence" \
                --failure "The post-workload backend health probe failed" \
                >/dev/null || true
        fi
        record_benchmark_event "backend_health_failed" \
            "$BACKEND_ID backend failed its post-workload health check"
        log_error "$BACKEND_ID backend failed its post-workload health check"
        log_error "The case artifacts will be retained, but the measurement is ineligible"
        return "$health_status"
    fi

    log_info "Benchmark case completed successfully"
    log_info "Outputs stored in: $CASE_DIR"

    cleanup_case 0
    CASE_RUNNING=0
}

JOB_ID="${SLURM_JOB_ID:-$(date +%Y%m%d_%H%M%S)}"
CASE_ID="${CASE_ID_OVERRIDE:-case_${JOB_ID}}"
CASE_NAME="${CASE_NAME_OVERRIDE:-$(basename "$INPUT_CONFIG_PATH" .json)}"
CASE_DIR="${CASE_DIR_OVERRIDE:-$PROJECT_ROOT/results/runs/$BACKEND_ID/$CASE_ID}"

run_one_case "$INPUT_CONFIG_PATH" "$CASE_ID" "$CASE_NAME" "$CASE_DIR"
