#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

pulsar_require_case
pulsar_load_nodes
require_command srun

RUNTIME_DIR="$(pulsar_runtime_dir)"
PID_FILE="$RUNTIME_DIR/pids/pulsar.pid"
SRUN_PID_FILE="$RUNTIME_DIR/pids/pulsar.srun.pid"
HEALTH_FILE="$CASE_DIR/runtime/backend_health.json"
HEALTH_TIMEOUT_SEC="${BACKEND_HEALTH_TIMEOUT_SEC:-15}"

process_status="fail"
process_detail="Pulsar PID file is missing"
if [[ -s "$PID_FILE" ]]; then
    remote_pid="$(<"$PID_FILE")"
    if timeout "$HEALTH_TIMEOUT_SEC" \
        srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" \
        bash -lc "kill -0 '$remote_pid'" >/dev/null 2>&1; then
        process_status="pass"
        process_detail="Pulsar process $remote_pid is alive on $PULSAR_NODE"
    else
        process_detail="Pulsar process $remote_pid is not alive on $PULSAR_NODE"
    fi
fi

launcher_status="fail"
launcher_detail="Pulsar srun PID file is missing"
if [[ -s "$SRUN_PID_FILE" ]]; then
    srun_pid="$(<"$SRUN_PID_FILE")"
    if kill -0 "$srun_pid" >/dev/null 2>&1; then
        launcher_status="pass"
        launcher_detail="Pulsar srun launcher $srun_pid is alive"
    else
        launcher_detail="Pulsar srun launcher $srun_pid has exited"
    fi
fi

admin_status="fail"
admin_detail="Pulsar admin health endpoint is unavailable"
if python3 - "$PULSAR_ADDRESS" "$HEALTH_TIMEOUT_SEC" <<'PY'
import sys
import urllib.request

host, timeout = sys.argv[1:]
url = f"http://{host}:8080/admin/v2/brokers/health"
with urllib.request.urlopen(url, timeout=float(timeout)) as response:
    if response.status != 200:
        raise SystemExit(1)
PY
then
    admin_status="pass"
    admin_detail="Pulsar admin health endpoint returned HTTP 200"
fi

failure_args=()
signature_args=()
[[ "$process_status" == "pass" ]] || failure_args+=(--failure "$process_detail")
[[ "$launcher_status" == "pass" ]] || failure_args+=(--failure "$launcher_detail")
[[ "$admin_status" == "pass" ]] || failure_args+=(--failure "$admin_detail")

oom_match="$(grep -R -n -m 1 -E \
    'OutOfMemoryError|OutOfDirectMemoryError' \
    "$CASE_DIR/logs/pulsar" 2>/dev/null || true)"
if [[ -n "$oom_match" ]]; then
    signature_args+=(--signature "$oom_match")
    failure_args+=(--failure "Pulsar log contains an out-of-memory failure")
fi

status="healthy"
if [[ "$process_status" != "pass" \
    || "$launcher_status" != "pass" \
    || "$admin_status" != "pass" \
    || -n "$oom_match" ]]; then
    status="failed"
fi

python3 -B "$PROJECT_ROOT/scripts/write_backend_health.py" \
    --output "$HEALTH_FILE" \
    --backend-id pulsar \
    --status "$status" \
    --check service_process "$process_status" "$process_detail" \
    --check slurm_service_step "$launcher_status" "$launcher_detail" \
    --check admin_endpoint "$admin_status" "$admin_detail" \
    "${failure_args[@]}" \
    "${signature_args[@]}" >/dev/null

if [[ "$status" != "healthy" ]]; then
    log_error "Pulsar failed its post-workload health check; see $HEALTH_FILE"
    exit 1
fi
log_info "Pulsar passed its post-workload health check"
