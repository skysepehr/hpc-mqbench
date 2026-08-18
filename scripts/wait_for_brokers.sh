#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Wait until the Kafka cluster accepts admin/client requests.
#
# Broker JVMs are started asynchronously by start_brokers.sh. A fixed sleep can
# be either too short on a busy cluster or unnecessarily long on a fast one, so
# this script polls Kafka until the bootstrap endpoints respond or a timeout is
# reached.
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"

require_file "$KAFKA_HOME/bin/kafka-broker-api-versions.sh"

BROKER_READY_TIMEOUT_SEC="${BROKER_READY_TIMEOUT_SEC:-120}"
BROKER_READY_POLL_SEC="${BROKER_READY_POLL_SEC:-5}"

RUNTIME_DIR="$CASE_DIR/runtime"
BOOTSTRAP_FILE="$RUNTIME_DIR/bootstrap_servers.txt"
READY_LOG_DIR="$CASE_DIR/logs/readiness"
READY_LOG_FILE="$READY_LOG_DIR/broker-readiness.log"

require_file "$BOOTSTRAP_FILE"
ensure_dir "$READY_LOG_DIR"

BOOTSTRAP_SERVERS="$(<"$BOOTSTRAP_FILE")"
require_nonempty "BOOTSTRAP_SERVERS" "$BOOTSTRAP_SERVERS"

start_epoch="$(date +%s)"
deadline=$(( start_epoch + BROKER_READY_TIMEOUT_SEC ))

log_info "Waiting for Kafka brokers to become ready"
log_info "Bootstrap servers: $BOOTSTRAP_SERVERS"
log_info "Readiness timeout: ${BROKER_READY_TIMEOUT_SEC}s"

while true; do
    now="$(date +%s)"

    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server "$BOOTSTRAP_SERVERS" \
        >"$READY_LOG_FILE" 2>&1; then
        log_info "Kafka brokers are ready"
        exit 0
    fi

    if (( now >= deadline )); then
        log_error "Kafka brokers did not become ready within ${BROKER_READY_TIMEOUT_SEC}s"
        log_error "Last readiness output: $READY_LOG_FILE"
        exit 1
    fi

    sleep "$BROKER_READY_POLL_SEC"
done
