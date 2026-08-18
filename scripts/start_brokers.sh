#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Start Kafka brokers for one benchmark case.
#
# This script assumes:
# - it is called from within an active Slurm allocation
# - scripts/common.sh has already been sourced
# - split_nodes "$BROKER_COUNT" has already been called
#
# Responsibilities:
# - create per-broker runtime directories
# - generate one broker config per active broker node
# - build the KRaft controller quorum voters string
# - optionally format broker storage
# - start one broker per broker node
# - write bootstrap server information for later scripts
#
# Required environment variables:
# - CASE_DIR
# - KAFKA_HOME
# - BROKER_COUNT
#
# Optional environment variables:
# - KAFKA_LISTENER_PORT   (default: 9092)
# - KAFKA_CONTROLLER_PORT (default: 9093)
# - KAFKA_HPC_NETWORK_INTERFACE (default: ib0, use hostname to disable)
# - KAFKA_HPC_REQUIRE_FABRIC    (default: 1, fail if the interface is missing)
# - KAFKA_JMX_PORT_BASE   (default: 7101, Prometheus JMX exporter HTTP port)
# - KAFKA_HEAP_OPTS       (default: "-Xms1g -Xmx1g")
# - FORMAT_STORAGE        (default: "1")
# - KAFKA_CLUSTER_ID      (default: generated if missing)
# - INTERNAL_TOPIC_REPLICATION_FACTOR (default: min(3, BROKER_COUNT))
# - INTERNAL_TOPIC_MIN_ISR            (default: 1 for rf=1, otherwise 2)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"

require_command srun
require_command python3

validate_broker_count "$BROKER_COUNT"

JMX_EXPORTER_JAR="${JMX_EXPORTER_JAR:-$PROJECT_ROOT/tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar}"
JMX_EXPORTER_CONFIG="${JMX_EXPORTER_CONFIG:-$PROJECT_ROOT/monitoring/jmx_exporter_config.yml}"

require_nonempty "JMX_EXPORTER_JAR" "${JMX_EXPORTER_JAR:-}"
require_nonempty "JMX_EXPORTER_CONFIG" "${JMX_EXPORTER_CONFIG:-}"

require_file "$JMX_EXPORTER_JAR"
require_file "$JMX_EXPORTER_CONFIG"

KAFKA_LISTENER_PORT="${KAFKA_LISTENER_PORT:-9092}"
KAFKA_CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"
KAFKA_JMX_PORT_BASE="${KAFKA_JMX_PORT_BASE:-7101}"
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms1g -Xmx1g}"
KAFKA_NUM_NETWORK_THREADS="${KAFKA_NUM_NETWORK_THREADS:-3}"
KAFKA_NUM_IO_THREADS="${KAFKA_NUM_IO_THREADS:-8}"
KAFKA_SOCKET_SEND_BUFFER_BYTES="${KAFKA_SOCKET_SEND_BUFFER_BYTES:-102400}"
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES="${KAFKA_SOCKET_RECEIVE_BUFFER_BYTES:-102400}"
KAFKA_QUEUED_MAX_REQUESTS="${KAFKA_QUEUED_MAX_REQUESTS:-500}"
KAFKA_SOCKET_REQUEST_MAX_BYTES="${KAFKA_SOCKET_REQUEST_MAX_BYTES:-104857600}"
KAFKA_LOG_SEGMENT_BYTES="${KAFKA_LOG_SEGMENT_BYTES:-1073741824}"
ENABLE_RAM_BACKED_RUNTIME="${ENABLE_RAM_BACKED_RUNTIME:-1}"
KAFKA_RAM_ROOT="${KAFKA_RAM_ROOT:-/dev/shm/kafka-simple-${USER:-user}-${SLURM_JOB_ID:-manual}}"
FORMAT_STORAGE="${FORMAT_STORAGE:-1}"
INTERNAL_TOPIC_REPLICATION_FACTOR="${INTERNAL_TOPIC_REPLICATION_FACTOR:-}"
INTERNAL_TOPIC_MIN_ISR="${INTERNAL_TOPIC_MIN_ISR:-}"

if [[ -z "$INTERNAL_TOPIC_REPLICATION_FACTOR" ]]; then
    if (( BROKER_COUNT >= 3 )); then
        INTERNAL_TOPIC_REPLICATION_FACTOR=3
    else
        INTERNAL_TOPIC_REPLICATION_FACTOR="$BROKER_COUNT"
    fi
fi

if [[ ! "$INTERNAL_TOPIC_REPLICATION_FACTOR" =~ ^[1-9][0-9]*$ ]]; then
    die "INTERNAL_TOPIC_REPLICATION_FACTOR must be a positive integer"
fi

if [[ -z "$INTERNAL_TOPIC_MIN_ISR" ]]; then
    if (( INTERNAL_TOPIC_REPLICATION_FACTOR >= 3 )); then
        INTERNAL_TOPIC_MIN_ISR=2
    else
        INTERNAL_TOPIC_MIN_ISR=1
    fi
fi

if [[ ! "$INTERNAL_TOPIC_MIN_ISR" =~ ^[1-9][0-9]*$ ]]; then
    die "INTERNAL_TOPIC_MIN_ISR must be a positive integer"
fi

if (( INTERNAL_TOPIC_REPLICATION_FACTOR <= 0 )); then
    die "INTERNAL_TOPIC_REPLICATION_FACTOR must be greater than 0"
fi

if (( INTERNAL_TOPIC_REPLICATION_FACTOR > BROKER_COUNT )); then
    die "INTERNAL_TOPIC_REPLICATION_FACTOR cannot exceed BROKER_COUNT"
fi

if (( INTERNAL_TOPIC_MIN_ISR <= 0 || INTERNAL_TOPIC_MIN_ISR > INTERNAL_TOPIC_REPLICATION_FACTOR )); then
    die "INTERNAL_TOPIC_MIN_ISR must be between 1 and INTERNAL_TOPIC_REPLICATION_FACTOR"
fi

require_file "$KAFKA_HOME/bin/kafka-server-start.sh"
require_file "$KAFKA_HOME/bin/kafka-storage.sh"

# Ensure node-role variables exist.
split_nodes "$BROKER_COUNT"

RUNTIME_DIR="$CASE_DIR/runtime"
BROKER_RUNTIME_DIR="$RUNTIME_DIR/brokers"
BROKER_CONFIG_DIR="$BROKER_RUNTIME_DIR/configs"
BROKER_DATA_DIR="$BROKER_RUNTIME_DIR/data"
BROKER_PID_DIR="$BROKER_RUNTIME_DIR/pids"
BROKER_SRUN_PID_DIR="$BROKER_RUNTIME_DIR/srun-pids"
BROKER_LOG_DIR="$CASE_DIR/logs/brokers"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"
BROKER_SRUN_ARGS=()
if [[ -n "${BROKER_PROFILE_ID:-}" ]]; then
    BROKER_SRUN_ARGS+=(--overlap --cpu-bind=none)
fi

prepare_case_directories "$CASE_DIR"
ensure_dir "$BROKER_RUNTIME_DIR"
ensure_dir "$BROKER_CONFIG_DIR"
ensure_dir "$BROKER_DATA_DIR"
ensure_dir "$BROKER_PID_DIR"
ensure_dir "$BROKER_SRUN_PID_DIR"
ensure_dir "$BROKER_LOG_DIR"

if [[ -n "${BROKER_PROFILE_ID:-}" ]]; then
    require_nonempty "BROKER_PROFILE_SHA256" "${BROKER_PROFILE_SHA256:-}"
    require_file "${BROKER_PROFILE_MANIFEST_PATH:-}"
    cp "$BROKER_PROFILE_MANIFEST_PATH" "$RUNTIME_DIR/broker_profile_snapshot.json"
    printf '%s\n' "$BROKER_PROFILE_SHA256" > "$RUNTIME_DIR/broker_profile.sha256"
    log_info "Broker profile: ${BROKER_PROFILE_ID} (${BROKER_PROFILE_SHA256})"
fi

BROKER_SERVICE_ADDRESSES=()
for NODE_NAME in "${BROKER_NODES[@]}"; do
    BROKER_SERVICE_ADDRESSES+=("$(resolve_node_service_address "$NODE_NAME")")
done

log_info "Kafka data-path interface: ${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
log_info "Broker service addresses: $(join_by , "${BROKER_SERVICE_ADDRESSES[@]}")"

# -----------------------------------------------------------------------------
# Build the KRaft controller quorum voters string.
#
# Example:
#   1@192.0.2.10:9093,2@192.0.2.11:9093,3@192.0.2.12:9093
#
# In this first design, every broker is also a controller voter.
# -----------------------------------------------------------------------------
build_quorum_voters() {
    local voters=()
    local i node_id address

    for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
        node_id=$((i + 1))
        address="${BROKER_SERVICE_ADDRESSES[$i]}"
        voters+=("${node_id}@${address}:${KAFKA_CONTROLLER_PORT}")
    done

    join_by "," "${voters[@]}"
}

build_broker_bootstrap_servers() {
    local servers=()
    local address

    for address in "${BROKER_SERVICE_ADDRESSES[@]}"; do
        servers+=("${address}:${KAFKA_LISTENER_PORT}")
    done

    join_by "," "${servers[@]}"
}

KAFKA_QUORUM_VOTERS="$(build_quorum_voters)"
export KAFKA_QUORUM_VOTERS

# -----------------------------------------------------------------------------
# Determine or generate cluster ID for KRaft storage formatting.
#
# For one case, all brokers in the same cluster must use the same cluster ID.
# -----------------------------------------------------------------------------
if [[ -z "${KAFKA_CLUSTER_ID:-}" ]]; then
    KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
fi
export KAFKA_CLUSTER_ID

log_info "Kafka cluster ID: $KAFKA_CLUSTER_ID"
log_info "Kafka quorum voters: $KAFKA_QUORUM_VOTERS"
log_info "Kafka internal topic replication factor: $INTERNAL_TOPIC_REPLICATION_FACTOR"
log_info "Kafka internal topic min ISR: $INTERNAL_TOPIC_MIN_ISR"
log_info "Kafka socket request max bytes: $KAFKA_SOCKET_REQUEST_MAX_BYTES"
log_info "Kafka log segment bytes: $KAFKA_LOG_SEGMENT_BYTES"
log_info "Kafka network/io threads: network=$KAFKA_NUM_NETWORK_THREADS io=$KAFKA_NUM_IO_THREADS queued.max.requests=$KAFKA_QUEUED_MAX_REQUESTS"
log_info "Kafka socket buffers: send=$KAFKA_SOCKET_SEND_BUFFER_BYTES receive=$KAFKA_SOCKET_RECEIVE_BUFFER_BYTES"
log_info "Kafka RAM-backed runtime: ENABLE_RAM_BACKED_RUNTIME=$ENABLE_RAM_BACKED_RUNTIME KAFKA_RAM_ROOT=$KAFKA_RAM_ROOT"
if [[ -n "${KAFKA_MESSAGE_MAX_BYTES:-}" ]]; then
    log_info "Kafka message max bytes: $KAFKA_MESSAGE_MAX_BYTES"
fi
if [[ -n "${KAFKA_REPLICA_FETCH_MAX_BYTES:-}" ]]; then
    log_info "Kafka replica fetch max bytes: $KAFKA_REPLICA_FETCH_MAX_BYTES"
fi

# -----------------------------------------------------------------------------
# Write one broker config file per broker node.
#
# We use /dev/shm for log.dirs because the benchmark design uses RAM-backed
# Kafka storage.
# -----------------------------------------------------------------------------
broker_ram_data_dir() {
    local node_id="${1:?node_id is required}"
    if [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
        printf '%s/broker-%s/kafka-logs\n' "$KAFKA_RAM_ROOT" "$node_id"
    else
        printf '/dev/shm/kafka-logs-%s\n' "$node_id"
    fi
}

broker_ram_metadata_dir() {
    local node_id="${1:?node_id is required}"
    if [[ "$ENABLE_RAM_BACKED_RUNTIME" == "1" ]]; then
        printf '%s/broker-%s/metadata-log\n' "$KAFKA_RAM_ROOT" "$node_id"
    else
        printf '/dev/shm/kafka-metadata-%s\n' "$node_id"
    fi
}

write_broker_config() {
    local node_id="${1:?node_id is required}"
    local node_name="${2:?node_name is required}"
    local broker_address="${3:?broker service address is required}"
    local config_path="${4:?config_path is required}"
    local broker_data_dir="${5:?broker data dir is required}"
    local broker_metadata_dir="${6:?broker metadata dir is required}"

    cat > "$config_path" <<EOF
process.roles=broker,controller
node.id=${node_id}

controller.quorum.voters=${KAFKA_QUORUM_VOTERS}
controller.listener.names=CONTROLLER

listeners=PLAINTEXT://0.0.0.0:${KAFKA_LISTENER_PORT},CONTROLLER://0.0.0.0:${KAFKA_CONTROLLER_PORT}
advertised.listeners=PLAINTEXT://${broker_address}:${KAFKA_LISTENER_PORT}
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT

num.network.threads=${KAFKA_NUM_NETWORK_THREADS}
num.io.threads=${KAFKA_NUM_IO_THREADS}
socket.send.buffer.bytes=${KAFKA_SOCKET_SEND_BUFFER_BYTES}
socket.receive.buffer.bytes=${KAFKA_SOCKET_RECEIVE_BUFFER_BYTES}
socket.request.max.bytes=${KAFKA_SOCKET_REQUEST_MAX_BYTES}
queued.max.requests=${KAFKA_QUEUED_MAX_REQUESTS}

log.dirs=${broker_data_dir}
metadata.log.dir=${broker_metadata_dir}
num.partitions=3
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=${INTERNAL_TOPIC_REPLICATION_FACTOR}
transaction.state.log.replication.factor=${INTERNAL_TOPIC_REPLICATION_FACTOR}
transaction.state.log.min.isr=${INTERNAL_TOPIC_MIN_ISR}

log.retention.hours=1
log.segment.bytes=${KAFKA_LOG_SEGMENT_BYTES}
log.retention.check.interval.ms=300000

group.initial.rebalance.delay.ms=0
EOF

    if [[ -n "${KAFKA_MESSAGE_MAX_BYTES:-}" ]]; then
        cat >> "$config_path" <<EOF

message.max.bytes=${KAFKA_MESSAGE_MAX_BYTES}
EOF
    fi

    if [[ -n "${KAFKA_REPLICA_FETCH_MAX_BYTES:-}" ]]; then
        cat >> "$config_path" <<EOF
replica.fetch.max.bytes=${KAFKA_REPLICA_FETCH_MAX_BYTES}
EOF
    fi
}

# -----------------------------------------------------------------------------
# Start one broker on one node.
#
# We:
# - clear the old /dev/shm directory for this broker
# - optionally format storage
# - start Kafka in the background on that node
# - write stdout/stderr to a per-broker log file
# -----------------------------------------------------------------------------
start_one_broker() {
    local node_id="${1:?node_id is required}"
    local node_name="${2:?node_name is required}"
    local config_path="${3:?config_path is required}"
    local broker_log_file="${4:?broker_log_file is required}"
    local jmx_port="${5:?jmx_port is required}"
    local broker_pid_file="${6:?broker_pid_file is required}"
    local broker_srun_pid_file="${7:?broker srun pid file is required}"
    local broker_data_dir="${8:?broker data dir is required}"
    local broker_metadata_dir="${9:?broker metadata dir is required}"

    log_info "Starting broker node.id=${node_id} on ${node_name}"

    srun "${BROKER_SRUN_ARGS[@]}" --nodes=1 --ntasks=1 -w "$node_name" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}

        export KAFKA_RAM_ROOT='${KAFKA_RAM_ROOT}'
        export TMPDIR='${KAFKA_RAM_ROOT}/tmp'
        export TMP="\$TMPDIR"
        export TEMP="\$TMPDIR"
        export XDG_CACHE_HOME='${KAFKA_RAM_ROOT}/cache'
        mkdir -p "\$TMPDIR" "\$XDG_CACHE_HOME"

        export KAFKA_HEAP_OPTS='${KAFKA_HEAP_OPTS}'

        # Attach the Prometheus JMX exporter as a JVM agent. This exposes Kafka
        # metrics over HTTP on the per-broker port scraped by Prometheus.
        export KAFKA_OPTS='-Djava.io.tmpdir=${KAFKA_RAM_ROOT}/tmp -javaagent:${JMX_EXPORTER_JAR}=${jmx_port}:${JMX_EXPORTER_CONFIG} '\${KAFKA_OPTS:-}

        rm -rf '${broker_data_dir}' '${broker_metadata_dir}'
        mkdir -p '${broker_data_dir}' '${broker_metadata_dir}'

        if [[ '${FORMAT_STORAGE}' == '1' ]]; then
            '${KAFKA_HOME}/bin/kafka-storage.sh' format \
                --ignore-formatted \
                --cluster-id '${KAFKA_CLUSTER_ID}' \
                --config '${config_path}'
        fi

        echo \$\$ > '${broker_pid_file}'
        exec '${KAFKA_HOME}/bin/kafka-server-start.sh' '${config_path}'
    " > "$broker_log_file" 2>&1 &

    echo $! > "$broker_srun_pid_file"
}

# -----------------------------------------------------------------------------
# Write all broker configs and start all brokers.
# -----------------------------------------------------------------------------
for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
    NODE_NAME="${BROKER_NODES[$i]}"
    BROKER_ADDRESS="${BROKER_SERVICE_ADDRESSES[$i]}"
    NODE_ID=$((i + 1))
    CONFIG_PATH="$BROKER_CONFIG_DIR/server-${NODE_ID}.properties"
    BROKER_NODE_LOG_DIR="$BROKER_LOG_DIR/${NODE_NAME}"
    BROKER_LOG_FILE="$BROKER_NODE_LOG_DIR/kafka-server.log"
    BROKER_PID_FILE="$BROKER_PID_DIR/broker-${NODE_ID}.pid"
    BROKER_SRUN_PID_FILE="$BROKER_SRUN_PID_DIR/broker-${NODE_ID}.srun.pid"
    JMX_PORT=$((KAFKA_JMX_PORT_BASE + i))
    BROKER_DATA_PATH="$(broker_ram_data_dir "$NODE_ID")"
    BROKER_METADATA_PATH="$(broker_ram_metadata_dir "$NODE_ID")"

    ensure_dir "$BROKER_NODE_LOG_DIR"
    write_broker_config "$NODE_ID" "$NODE_NAME" "$BROKER_ADDRESS" "$CONFIG_PATH" "$BROKER_DATA_PATH" "$BROKER_METADATA_PATH"
    sha256sum "$CONFIG_PATH" > "$CONFIG_PATH.sha256"
done

if [[ -n "${BROKER_PROFILE_ID:-}" ]]; then
    mapfile -t GENERATED_SERVER_CONFIGS < <(
        find "$BROKER_CONFIG_DIR" -maxdepth 1 -type f -name 'server-*.properties' | sort
    )
    JAVA_VERSION_TEXT="$(java -version 2>&1 | head -n 1)"
    KAFKA_VERSION_TEXT="$("$KAFKA_HOME/bin/kafka-topics.sh" --version 2>&1 | head -n 1)"
    python3 "$SCRIPT_DIR/verify_broker_runtime_profile.py" \
        --profile "$BROKER_PROFILE_MANIFEST_PATH" \
        --server-properties "${GENERATED_SERVER_CONFIGS[@]}" \
        --heap-opts "$KAFKA_HEAP_OPTS" \
        --java-version "$JAVA_VERSION_TEXT" \
        --kafka-version "$KAFKA_VERSION_TEXT" \
        --output "$RUNTIME_DIR/broker_runtime_manifest.json"
fi

for ((i=0; i<${#BROKER_NODES[@]}; i++)); do
    NODE_NAME="${BROKER_NODES[$i]}"
    NODE_ID=$((i + 1))
    CONFIG_PATH="$BROKER_CONFIG_DIR/server-${NODE_ID}.properties"
    BROKER_NODE_LOG_DIR="$BROKER_LOG_DIR/${NODE_NAME}"
    BROKER_LOG_FILE="$BROKER_NODE_LOG_DIR/kafka-server.log"
    BROKER_PID_FILE="$BROKER_PID_DIR/broker-${NODE_ID}.pid"
    BROKER_SRUN_PID_FILE="$BROKER_SRUN_PID_DIR/broker-${NODE_ID}.srun.pid"
    JMX_PORT=$((KAFKA_JMX_PORT_BASE + i))
    BROKER_DATA_PATH="$(broker_ram_data_dir "$NODE_ID")"
    BROKER_METADATA_PATH="$(broker_ram_metadata_dir "$NODE_ID")"
    start_one_broker "$NODE_ID" "$NODE_NAME" "$CONFIG_PATH" "$BROKER_LOG_FILE" "$JMX_PORT" "$BROKER_PID_FILE" "$BROKER_SRUN_PID_FILE" "$BROKER_DATA_PATH" "$BROKER_METADATA_PATH"
done

# Give the srun service steps a moment to create PID files before readiness
# checks start. The long-lived srun processes continue in the background.
sleep 2

# -----------------------------------------------------------------------------
# Write runtime helper files for later scripts.
# -----------------------------------------------------------------------------
BOOTSTRAP_SERVERS="$(build_broker_bootstrap_servers)"
echo "$BOOTSTRAP_SERVERS" > "$RUNTIME_DIR/bootstrap_servers.txt"
build_bootstrap_servers "$KAFKA_LISTENER_PORT" > "$RUNTIME_DIR/bootstrap_servers_resolved_check.txt"
echo "$KAFKA_CLUSTER_ID" > "$RUNTIME_DIR/cluster_id.txt"
printf '%s\n' "${BROKER_NODES[@]}" > "$RUNTIME_DIR/broker_nodes.txt"
printf '%s\n' "${BROKER_SERVICE_ADDRESSES[@]}" > "$RUNTIME_DIR/broker_addresses.txt"
write_node_service_address_map "$RUNTIME_DIR/node_service_addresses.tsv" "${ALL_NODES[@]}"

log_info "Kafka brokers started"
log_info "Bootstrap servers: $BOOTSTRAP_SERVERS"
