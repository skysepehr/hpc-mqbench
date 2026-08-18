#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Stop kafka_exporter on the monitoring node for one benchmark case.
#
# Required environment variables:
# - CASE_DIR
# - BROKER_COUNT
#
# Optional environment variables:
# - ALLOW_KAFKA_EXPORTER_PKILL  (default: 0, set 1 only on dedicated nodes)
# - STOP_KAFKA_EXPORTER_TIMEOUT_SEC (default: 30)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
require_command srun

validate_broker_count "$BROKER_COUNT"
split_nodes "$BROKER_COUNT"

EXPORTER_RUNTIME_DIR="$CASE_DIR/runtime/kafka_exporter"
EXPORTER_LOG_DIR="$CASE_DIR/logs/kafka_exporter"
EXPORTER_PID_FILE="$EXPORTER_RUNTIME_DIR/kafka_exporter.pid"
EXPORTER_STOP_LOG="$EXPORTER_LOG_DIR/kafka_exporter-stop.log"
ALLOW_KAFKA_EXPORTER_PKILL="${ALLOW_KAFKA_EXPORTER_PKILL:-0}"
STOP_KAFKA_EXPORTER_TIMEOUT_SEC="${STOP_KAFKA_EXPORTER_TIMEOUT_SEC:-30}"

ensure_dir "$EXPORTER_LOG_DIR"

log_info "Stopping kafka_exporter on monitoring node: $MONITORING_NODE"

REMOTE_STOP_SCRIPT="
    set -euo pipefail

    if [[ -f '${EXPORTER_PID_FILE}' ]]; then
        PID=\$(cat '${EXPORTER_PID_FILE}' || true)
        if [[ -n \"\${PID:-}\" ]]; then
            kill \"\$PID\" || true
            for _ in 1 2 3 4 5 6 7 8 9 10; do
                if ! kill -0 \"\$PID\" >/dev/null 2>&1; then
                    break
                fi
                sleep 1
            done
            if kill -0 \"\$PID\" >/dev/null 2>&1; then
                kill -KILL \"\$PID\" || true
            fi
        fi
        rm -f '${EXPORTER_PID_FILE}'
    fi

    if [[ '${ALLOW_KAFKA_EXPORTER_PKILL}' == '1' ]]; then
        pkill -f 'kafka_exporter' || true
    fi
"

if command -v timeout >/dev/null 2>&1; then
    timeout "$STOP_KAFKA_EXPORTER_TIMEOUT_SEC" \
        srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "$REMOTE_STOP_SCRIPT" \
        >"$EXPORTER_STOP_LOG" 2>&1 || log_warn "Timed out or failed while stopping kafka_exporter; see $EXPORTER_STOP_LOG"
else
    srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "$REMOTE_STOP_SCRIPT" \
        >"$EXPORTER_STOP_LOG" 2>&1 || log_warn "Failed while stopping kafka_exporter; see $EXPORTER_STOP_LOG"
fi

log_info "kafka_exporter stop sequence completed"
