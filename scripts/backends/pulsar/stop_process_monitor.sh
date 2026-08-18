#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
require_nonempty "CASE_DIR" "${CASE_DIR:-}"
MONITOR_DIR="$CASE_DIR/runtime/pulsar/process-monitor"
[[ -d "$MONITOR_DIR" ]] || exit 0
touch "$MONITOR_DIR/stop"
if [[ -f "$MONITOR_DIR/srun.pid" ]]; then
    pid="$(<"$MONITOR_DIR/srun.pid")"
    deadline=$((SECONDS + 15))
    while kill -0 "$pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 1; done
    kill "$pid" 2>/dev/null || true
fi
log_info "Pulsar process/interface/tmpfs monitor stopped"
