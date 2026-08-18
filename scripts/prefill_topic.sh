#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Prefill the benchmark topic for egress-only cases.
#
# This helper produces a real Kafka backlog before the MPI consumers start. It is
# skipped for non-egress scenarios and can be disabled with ENABLE_EGRESS_PREFILL=0.
#
# Required environment variables:
# - CONFIG_PATH
# - CASE_ID
# - CASE_DIR
# - SCENARIO
#
# Optional environment variables:
# - EGRESS_PREFILL_MESSAGES
# - ENABLE_EGRESS_PREFILL (default: 1)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
# shellcheck source=./python_env_common.sh
source "$SCRIPT_DIR/python_env_common.sh"

require_nonempty "CONFIG_PATH" "${CONFIG_PATH:-}"
require_nonempty "CASE_ID" "${CASE_ID:-}"
require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "SCENARIO" "${SCENARIO:-}"

if [[ "$SCENARIO" != "egress_only" ]]; then
    log_info "Topic prefill skipped for scenario: $SCENARIO"
    exit 0
fi

ENABLE_EGRESS_PREFILL="${ENABLE_EGRESS_PREFILL:-1}"
if [[ "$ENABLE_EGRESS_PREFILL" != "1" ]]; then
    log_info "Topic prefill disabled by ENABLE_EGRESS_PREFILL=$ENABLE_EGRESS_PREFILL"
    exit 0
fi

BOOTSTRAP_SERVERS_FILE="$CASE_DIR/runtime/bootstrap_servers.txt"
require_file "$BOOTSTRAP_SERVERS_FILE"

BOOTSTRAP_SERVERS="$(<"$BOOTSTRAP_SERVERS_FILE")"
require_nonempty "BOOTSTRAP_SERVERS" "$BOOTSTRAP_SERVERS"

PREFILL_LOG_DIR="$CASE_DIR/logs/topics"
PREFILL_RESULT_DIR="$CASE_DIR/${DATA_DIR_NAME:-data}"
PREFILL_LOG_FILE="$PREFILL_LOG_DIR/prefill_topic.log"
PREFILL_RESULT_FILE="$PREFILL_RESULT_DIR/egress_prefill_result.json"

ensure_dir "$PREFILL_LOG_DIR"
ensure_dir "$PREFILL_RESULT_DIR"

messages_args=()
if [[ -n "${EGRESS_PREFILL_MESSAGES:-}" ]]; then
    messages_args=(--messages "$EGRESS_PREFILL_MESSAGES")
fi

log_info "Prefilling topic for egress-only benchmark"
log_info "Prefill result file: $PREFILL_RESULT_FILE"

export_kafka_hpc_python_env "$PROJECT_ROOT"
cd "$PROJECT_ROOT"

python3 -m src.benchmark.topic_prefill \
    --config "$CONFIG_PATH" \
    --bootstrap-servers "$BOOTSTRAP_SERVERS" \
    --case-id "$CASE_ID" \
    --output-json "$PREFILL_RESULT_FILE" \
    "${messages_args[@]}" \
    2>&1 | tee "$PREFILL_LOG_FILE"

log_info "Topic prefill completed"
