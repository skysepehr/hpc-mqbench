#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Stop monitoring services for one benchmark case.
#
# This first version stops Prometheus started by start_monitoring.sh.
#
# Responsibilities:
# - locate the monitoring node from the current Slurm allocation
# - read the Prometheus PID file if available
# - stop Prometheus cleanly
# - fall back to pkill if needed
#
# Required environment variables:
# - CASE_DIR
# - BROKER_COUNT
#
# Optional environment variables:
# - ALLOW_PROMETHEUS_PKILL (default: 0, set 1 only on dedicated nodes)
# - STOP_MONITORING_TIMEOUT_SEC (default: 30)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"

require_command srun
validate_broker_count "$BROKER_COUNT"

# Rebuild node-role assignment from current Slurm allocation.
split_nodes "$BROKER_COUNT"

MONITORING_RUNTIME_DIR="$CASE_DIR/runtime/monitoring"
MONITORING_LOG_DIR="$CASE_DIR/logs/monitoring"
PROMETHEUS_PID_FILE="$MONITORING_RUNTIME_DIR/prometheus.pid"
PROMETHEUS_STOP_LOG="$MONITORING_LOG_DIR/prometheus-stop.log"
ALLOW_PROMETHEUS_PKILL="${ALLOW_PROMETHEUS_PKILL:-0}"
STOP_MONITORING_TIMEOUT_SEC="${STOP_MONITORING_TIMEOUT_SEC:-30}"

ensure_dir "$MONITORING_LOG_DIR"

log_info "Stopping monitoring services on node: $MONITORING_NODE"

# -----------------------------------------------------------------------------
# Stop Prometheus.
#
# Preferred method:
# - read PID from the pid file
# - send SIGTERM
#
# Optional fallback:
# - pkill based on the prometheus process name only when explicitly allowed
# -----------------------------------------------------------------------------
REMOTE_STOP_SCRIPT="
    set -euo pipefail

    if [[ -f '${PROMETHEUS_PID_FILE}' ]]; then
        PID=\$(cat '${PROMETHEUS_PID_FILE}' || true)
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
        rm -f '${PROMETHEUS_PID_FILE}'
    fi

    if [[ '${ALLOW_PROMETHEUS_PKILL}' == '1' ]]; then
        pkill -f '/prometheus' || true
    fi
"

if command -v timeout >/dev/null 2>&1; then
    timeout "$STOP_MONITORING_TIMEOUT_SEC" \
        srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "$REMOTE_STOP_SCRIPT" \
        >"$PROMETHEUS_STOP_LOG" 2>&1 || log_warn "Timed out or failed while stopping Prometheus; see $PROMETHEUS_STOP_LOG"
else
    srun --overlap --nodes=1 --ntasks=1 -w "$MONITORING_NODE" bash -lc "$REMOTE_STOP_SCRIPT" \
        >"$PROMETHEUS_STOP_LOG" 2>&1 || log_warn "Failed while stopping Prometheus; see $PROMETHEUS_STOP_LOG"
fi

log_info "Monitoring stop sequence completed"
