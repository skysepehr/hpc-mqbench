#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Create Kafka topics for one benchmark case.
#
# This script assumes:
# - it runs inside an active Slurm allocation
# - scripts/common.sh is available
# - Kafka brokers have already been started
# - bootstrap_servers.txt has already been written by start_brokers.sh
#
# Responsibilities:
# - validate topic settings
# - read bootstrap servers from runtime files
# - delete/recreate the benchmark topic by default so partitions and replication
#   settings cannot be accidentally reused across campaign cases
# - create the benchmark topic if it does not exist when reuse is requested
# - optionally describe the topic after creation
#
# Required environment variables:
# - CASE_DIR
# - KAFKA_HOME
# - TOPIC_NAME
# - PARTITIONS
# - REPLICATION_FACTOR
# - BROKER_COUNT
#
# Optional environment variables:
# - DELETE_TOPIC_FIRST        (default: 1, set 0 only for deliberate topic reuse)
# - TOPIC_DELETE_TIMEOUT_SEC  (default: 60)
# - TOPIC_READY_TIMEOUT_SEC   (default: 60)
# - TOPIC_CONFIGS_JSON        (JSON object of extra topic configs; not used yet)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
require_nonempty "TOPIC_NAME" "${TOPIC_NAME:-}"
require_nonempty "PARTITIONS" "${PARTITIONS:-}"
require_nonempty "REPLICATION_FACTOR" "${REPLICATION_FACTOR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"

require_file "$KAFKA_HOME/bin/kafka-topics.sh"

validate_broker_count "$BROKER_COUNT"
validate_replication_factor "$REPLICATION_FACTOR" "$BROKER_COUNT"

if (( PARTITIONS <= 0 )); then
    die "PARTITIONS must be greater than 0"
fi

RUNTIME_DIR="$CASE_DIR/runtime"
BOOTSTRAP_FILE="$RUNTIME_DIR/bootstrap_servers.txt"

require_file "$BOOTSTRAP_FILE"

BOOTSTRAP_SERVERS="$(<"$BOOTSTRAP_FILE")"
require_nonempty "BOOTSTRAP_SERVERS" "$BOOTSTRAP_SERVERS"

TOPIC_LOG_DIR="$CASE_DIR/logs/topics"
ensure_dir "$TOPIC_LOG_DIR"

CREATE_LOG_FILE="$TOPIC_LOG_DIR/create_topic.log"
DELETE_LOG_FILE="$TOPIC_LOG_DIR/delete_topic.log"
DESCRIBE_LOG_FILE="$TOPIC_LOG_DIR/describe_topic.log"
EXISTS_LOG_FILE="$TOPIC_LOG_DIR/topic_exists.log"

DELETE_TOPIC_FIRST="${DELETE_TOPIC_FIRST:-1}"
TOPIC_DELETE_TIMEOUT_SEC="${TOPIC_DELETE_TIMEOUT_SEC:-60}"
TOPIC_READY_TIMEOUT_SEC="${TOPIC_READY_TIMEOUT_SEC:-60}"
TOPIC_MAX_MESSAGE_BYTES="${TOPIC_MAX_MESSAGE_BYTES:-${KAFKA_TOPIC_MAX_MESSAGE_BYTES:-}}"

if [[ "$DELETE_TOPIC_FIRST" != "0" && "$DELETE_TOPIC_FIRST" != "1" ]]; then
    die "DELETE_TOPIC_FIRST must be 0 or 1"
fi

if [[ ! "$TOPIC_DELETE_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]]; then
    die "TOPIC_DELETE_TIMEOUT_SEC must be a positive integer"
fi

if [[ ! "$TOPIC_READY_TIMEOUT_SEC" =~ ^[1-9][0-9]*$ ]]; then
    die "TOPIC_READY_TIMEOUT_SEC must be a positive integer"
fi

log_info "Creating topic '$TOPIC_NAME'"
log_info "Bootstrap servers: $BOOTSTRAP_SERVERS"
log_info "Partitions: $PARTITIONS"
log_info "Replication factor: $REPLICATION_FACTOR"
log_info "Delete topic first: $DELETE_TOPIC_FIRST"
if [[ -n "$TOPIC_MAX_MESSAGE_BYTES" ]]; then
    log_info "Topic max message bytes: $TOPIC_MAX_MESSAGE_BYTES"
fi

# -----------------------------------------------------------------------------
# Topic state helpers.
# -----------------------------------------------------------------------------
topic_exists() {
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        --describe \
        --topic "$TOPIC_NAME" \
        >"$EXISTS_LOG_FILE" 2>&1
}

wait_for_topic_deleted() {
    local deadline
    deadline=$((SECONDS + TOPIC_DELETE_TIMEOUT_SEC))

    while (( SECONDS < deadline )); do
        if ! topic_exists; then
            return 0
        fi
        sleep 2
    done

    die "Topic '$TOPIC_NAME' still exists after ${TOPIC_DELETE_TIMEOUT_SEC}s"
}

wait_for_topic_ready() {
    local deadline
    deadline=$((SECONDS + TOPIC_READY_TIMEOUT_SEC))

    while (( SECONDS < deadline )); do
        if topic_exists; then
            return 0
        fi
        sleep 2
    done

    die "Topic '$TOPIC_NAME' was not visible after ${TOPIC_READY_TIMEOUT_SEC}s"
}

# -----------------------------------------------------------------------------
# Delete old benchmark topic state unless the caller explicitly requests reuse.
#
# Campaign cases can sweep partitions and replication factor while using the same
# logical topic name. Recreating the topic for each benchmark case prevents stale
# topic metadata from invalidating later measurements.
# -----------------------------------------------------------------------------
if [[ "$DELETE_TOPIC_FIRST" == "1" ]]; then
    if topic_exists; then
        log_info "Deleting existing topic '$TOPIC_NAME' before benchmark case"
        "$KAFKA_HOME/bin/kafka-topics.sh" \
            --bootstrap-server "$BOOTSTRAP_SERVERS" \
            --delete \
            --topic "$TOPIC_NAME" \
            >"$DELETE_LOG_FILE" 2>&1 || {
                if topic_exists; then
                    die "Failed to delete existing topic '$TOPIC_NAME'. See: $DELETE_LOG_FILE"
                fi
            }
        wait_for_topic_deleted
    else
        log_info "Topic '$TOPIC_NAME' does not exist yet"
    fi
fi

# -----------------------------------------------------------------------------
# Create the topic.
#
# With DELETE_TOPIC_FIRST=1, creation must succeed against a clean namespace. With
# DELETE_TOPIC_FIRST=0, --if-not-exists keeps deliberate debug re-runs nonfatal.
# -----------------------------------------------------------------------------
CREATE_ARGS=(
    --bootstrap-server "$BOOTSTRAP_SERVERS"
    --create
    --topic "$TOPIC_NAME"
    --partitions "$PARTITIONS"
    --replication-factor "$REPLICATION_FACTOR"
)

if [[ "$DELETE_TOPIC_FIRST" != "1" ]]; then
    CREATE_ARGS+=(--if-not-exists)
fi

if [[ -n "$TOPIC_MAX_MESSAGE_BYTES" ]]; then
    if [[ ! "$TOPIC_MAX_MESSAGE_BYTES" =~ ^[1-9][0-9]*$ ]]; then
        die "TOPIC_MAX_MESSAGE_BYTES must be a positive integer"
    fi
    CREATE_ARGS+=(--config "max.message.bytes=$TOPIC_MAX_MESSAGE_BYTES")
fi

"$KAFKA_HOME/bin/kafka-topics.sh" "${CREATE_ARGS[@]}" >"$CREATE_LOG_FILE" 2>&1

log_info "Topic creation command finished"
wait_for_topic_ready

# -----------------------------------------------------------------------------
# Describe the topic and save the output.
#
# This is helpful for debugging and also useful later in result collection.
# -----------------------------------------------------------------------------
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server "$BOOTSTRAP_SERVERS" \
    --describe \
    --topic "$TOPIC_NAME" \
    >"$DESCRIBE_LOG_FILE" 2>&1

log_info "Topic description saved to: $DESCRIBE_LOG_FILE"
log_info "Topic '$TOPIC_NAME' is ready"
