#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"
require_nonempty "KAFKA_HOME" "${KAFKA_HOME:-}"
require_nonempty "BROKER_COUNT" "${BROKER_COUNT:-}"
require_command srun
require_file "$KAFKA_HOME/bin/kafka-broker-api-versions.sh"

split_nodes "$BROKER_COUNT"
RUNTIME_DIR="$CASE_DIR/runtime"
PID_DIR="$RUNTIME_DIR/brokers/pids"
SRUN_PID_DIR="$RUNTIME_DIR/brokers/srun-pids"
HEALTH_FILE="$RUNTIME_DIR/backend_health.json"
HEALTH_TIMEOUT_SEC="${BACKEND_HEALTH_TIMEOUT_SEC:-15}"

alive_count=0
launcher_count=0
for ((i=0; i<BROKER_COUNT; i++)); do
    node="${BROKER_NODES[$i]}"
    node_id=$((i + 1))
    pid_file="$PID_DIR/broker-${node_id}.pid"
    if [[ -s "$pid_file" ]]; then
        remote_pid="$(<"$pid_file")"
        if timeout "$HEALTH_TIMEOUT_SEC" \
            srun --overlap --nodes=1 --ntasks=1 -w "$node" \
            bash -lc "kill -0 '$remote_pid'" >/dev/null 2>&1; then
            alive_count=$((alive_count + 1))
        fi
    fi
    srun_pid_file="$SRUN_PID_DIR/broker-${node_id}.srun.pid"
    if [[ -s "$srun_pid_file" ]]; then
        srun_pid="$(<"$srun_pid_file")"
        if kill -0 "$srun_pid" >/dev/null 2>&1; then
            launcher_count=$((launcher_count + 1))
        fi
    fi
done

process_status="fail"
process_detail="$alive_count/$BROKER_COUNT Kafka broker processes are alive"
[[ "$alive_count" == "$BROKER_COUNT" ]] && process_status="pass"

launcher_status="fail"
launcher_detail="$launcher_count/$BROKER_COUNT Kafka srun launchers are alive"
[[ "$launcher_count" == "$BROKER_COUNT" ]] && launcher_status="pass"

admin_status="fail"
admin_detail="Kafka admin request failed"
bootstrap_file="$RUNTIME_DIR/bootstrap_servers.txt"
if [[ -s "$bootstrap_file" ]]; then
    bootstrap_servers="$(<"$bootstrap_file")"
    if timeout "$HEALTH_TIMEOUT_SEC" \
        "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server "$bootstrap_servers" >/dev/null 2>&1; then
        admin_status="pass"
        admin_detail="Kafka admin request completed for $bootstrap_servers"
    fi
fi

failure_args=()
signature_args=()
[[ "$process_status" == "pass" ]] || failure_args+=(--failure "$process_detail")
[[ "$launcher_status" == "pass" ]] || failure_args+=(--failure "$launcher_detail")
[[ "$admin_status" == "pass" ]] || failure_args+=(--failure "$admin_detail")

oom_match="$(grep -R -n -m 1 -E \
    'OutOfMemoryError|OutOfDirectMemoryError' \
    "$CASE_DIR/logs/brokers" 2>/dev/null || true)"
if [[ -n "$oom_match" ]]; then
    signature_args+=(--signature "$oom_match")
    failure_args+=(--failure "Kafka broker log contains an out-of-memory failure")
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
    --backend-id kafka \
    --status "$status" \
    --check broker_processes "$process_status" "$process_detail" \
    --check slurm_service_steps "$launcher_status" "$launcher_detail" \
    --check admin_request "$admin_status" "$admin_detail" \
    "${failure_args[@]}" \
    "${signature_args[@]}" >/dev/null

if [[ "$status" != "healthy" ]]; then
    log_error "Kafka failed its post-workload health check; see $HEALTH_FILE"
    exit 1
fi
log_info "Kafka passed its post-workload health check"
