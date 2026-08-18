#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$PROJECT_ROOT"

BACKEND_ID="${1:-}"
ACTION="${2:-}"
CONFIG_PATH="${3:-${CONFIG_PATH:-}}"
COMPONENT="${BACKEND_MONITOR_COMPONENT:-all}"

usage() {
    cat >&2 <<'EOF'
Usage: scripts/backend_lifecycle.sh BACKEND ACTION [CONFIG]

Actions:
  validate, plan, preflight, start, wait-ready, check-health, create-stream,
  delete-stream, prefill, start-monitoring, stop-monitoring, snapshot, stop
EOF
    exit 2
}

[[ -n "$BACKEND_ID" && -n "$ACTION" ]] || usage

python_dispatch() {
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
        python3 -B -m src.benchmark.backends.lifecycle_cli "$@"
}

require_config() {
    if [[ -z "$CONFIG_PATH" || ! -f "$CONFIG_PATH" ]]; then
        printf '[backend] ERROR: configuration file not found: %s\n' \
            "${CONFIG_PATH:-<empty>}" >&2
        exit 1
    fi
}

resolve_and_run() {
    local internal_action="${1:?internal action required}"
    local script
    script="$(python_dispatch resolve "$BACKEND_ID" "$internal_action" \
        --project-root "$PROJECT_ROOT")"
    "$script"
}

require_config
python_dispatch validate "$BACKEND_ID" "$CONFIG_PATH" >/dev/null

case "$ACTION" in
    validate|plan)
        python_dispatch "$ACTION" "$BACKEND_ID" "$CONFIG_PATH"
        ;;
    preflight)
        script="$(python_dispatch resolve "$BACKEND_ID" preflight \
            --project-root "$PROJECT_ROOT")"
        "$script" "$CONFIG_PATH"
        ;;
    start|wait-ready|check-health|create-stream|delete-stream|prefill|stop)
        resolve_and_run "$ACTION"
        ;;
    start-monitoring)
        case "$COMPONENT" in
            process) resolve_and_run start-monitoring ;;
            metrics) resolve_and_run start-metrics-exporter ;;
            all)
                resolve_and_run start-monitoring
                resolve_and_run start-metrics-exporter
                ;;
            *) printf '[backend] ERROR: invalid monitor component: %s\n' "$COMPONENT" >&2; exit 2 ;;
        esac
        ;;
    stop-monitoring)
        case "$COMPONENT" in
            process) resolve_and_run stop-monitoring ;;
            metrics) resolve_and_run stop-metrics-exporter ;;
            all)
                resolve_and_run stop-monitoring
                resolve_and_run stop-metrics-exporter
                ;;
            *) printf '[backend] ERROR: invalid monitor component: %s\n' "$COMPONENT" >&2; exit 2 ;;
        esac
        ;;
    snapshot)
        : "${CASE_DIR:?CASE_DIR is required for snapshot}"
        python_dispatch snapshot "$BACKEND_ID" "$CONFIG_PATH" \
            "$CASE_DIR/runtime/backend_snapshot.json" >/dev/null
        ;;
    *)
        usage
        ;;
esac
