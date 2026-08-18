#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
require_nonempty "CASE_DIR" "${CASE_DIR:-}"
RUNTIME_DIR="$CASE_DIR/runtime/pulsar"
[[ -d "$RUNTIME_DIR" ]] || exit 0
pulsar_load_nodes

if [[ -f "$RUNTIME_DIR/service_nodes.txt" && -f "$RUNTIME_DIR/pids/pulsar.pid" ]]; then
    node="$(head -n 1 "$RUNTIME_DIR/service_nodes.txt")"
    remote_pid="$(<"$RUNTIME_DIR/pids/pulsar.pid")"
    srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc "
        kill -TERM '${remote_pid}' 2>/dev/null || true
        for _ in \$(seq 1 30); do
            kill -0 '${remote_pid}' 2>/dev/null || exit 0
            sleep 1
        done
        kill -KILL '${remote_pid}' 2>/dev/null || true
    " || true
fi
if [[ -f "$RUNTIME_DIR/pids/pulsar.srun.pid" ]]; then
    srun_pid="$(<"$RUNTIME_DIR/pids/pulsar.srun.pid")"
    deadline=$((SECONDS + 30))
    while kill -0 "$srun_pid" 2>/dev/null && (( SECONDS < deadline )); do sleep 1; done
    kill "$srun_pid" 2>/dev/null || true
fi

if [[ "${CLEAN_BROKER_RAM_DIRS:-1}" == "1" \
    && "${ENABLE_RAM_BACKED_RUNTIME:-1}" == "1" \
    && -n "${KAFKA_RAM_ROOT:-}" ]]; then
    printf -v quoted_ram_root '%q' "$KAFKA_RAM_ROOT"
    for node in "${ALL_NODES[@]}"; do
        srun --overlap --nodes=1 --ntasks=1 -w "$node" bash -lc \
            "rm -rf ${quoted_ram_root}" || \
            log_warn "Could not remove RAM runtime root on $node"
    done
fi
log_info "Pulsar standalone stopped"
