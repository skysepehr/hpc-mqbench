#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
MONITOR_DIR="$CASE_DIR/runtime/broker-process-monitor"
if [[ ! -d "$MONITOR_DIR" ]]; then
    exit 0
fi

for pid_file in "$MONITOR_DIR"/srun-*.pid; do
    [[ -f "$pid_file" ]] || continue
    suffix="$(basename "$pid_file" .pid)"
    suffix="${suffix#srun-}"
    touch "$MONITOR_DIR/stop-${suffix}"
done

deadline=$((SECONDS + 15))
for pid_file in "$MONITOR_DIR"/srun-*.pid; do
    [[ -f "$pid_file" ]] || continue
    pid="$(<"$pid_file")"
    while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
    fi
done

log_info "Kafka process/ib0/tmpfs monitor stopped"
