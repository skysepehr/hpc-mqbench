#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

pulsar_require_case
pulsar_load_nodes
require_nonempty "PULSAR_TOPIC_NAME" "${PULSAR_TOPIC_NAME:-}"

admin_url_file="$CASE_DIR/runtime/pulsar_admin_url.txt"
require_file "$admin_url_file"
admin_url="$(<"$admin_url_file")"
log_dir="$CASE_DIR/logs/pulsar"
delete_log="$log_dir/delete-stream.log"
verify_log="$log_dir/delete-stream-verify.log"
timeout_sec="${BACKEND_STREAM_DELETE_TIMEOUT_SEC:-120}"
poll_sec="${BACKEND_STREAM_DELETE_POLL_SEC:-2}"
min_free_percent="${BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT:-25}"
require_tmpfs_recovery="${BACKEND_STREAM_REQUIRE_TMPFS_RECOVERY:-1}"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$log_dir"
for value_name in timeout_sec poll_sec min_free_percent; do
    value="${!value_name}"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        die "$value_name must be a positive integer, got: $value"
    fi
done
if (( min_free_percent >= 100 )); then
    die "BACKEND_BATCH_MIN_TMPFS_FREE_PERCENT must be less than 100"
fi
if [[ "$require_tmpfs_recovery" != "0" && "$require_tmpfs_recovery" != "1" ]]; then
    die "BACKEND_STREAM_REQUIRE_TMPFS_RECOVERY must be 0 or 1"
fi

log_info "Deleting Pulsar partitioned topic after case: $PULSAR_TOPIC_NAME"
srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}
    '${PULSAR_HOME}/bin/pulsar-admin' --admin-url '${admin_url}' \
        topics delete-partitioned-topic --force '${PULSAR_TOPIC_NAME}'
" >"$delete_log" 2>&1

deadline=$((SECONDS + timeout_sec))
deleted=0
while (( SECONDS < deadline )); do
    if ! srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
        set -euo pipefail
        ${HPC_NODE_ENV_SNIPPET}
        '${PULSAR_HOME}/bin/pulsar-admin' --admin-url '${admin_url}' \
            topics get-partitioned-topic-metadata '${PULSAR_TOPIC_NAME}'
    " >"$verify_log" 2>&1; then
        deleted=1
        break
    fi
    if python3 - "$verify_log" <<'PY'
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
raise SystemExit(0 if int(payload.get("partitions", -1)) == 0 else 1)
PY
    then
        deleted=1
        break
    fi
    sleep "$poll_sec"
done

if [[ "$deleted" != "1" ]]; then
    log_error "Pulsar topic still exists after ${timeout_sec}s: $PULSAR_TOPIC_NAME"
    exit 1
fi

if [[ "$require_tmpfs_recovery" == "0" ]]; then
    log_info "Pulsar stream metadata deleted; storage recovery is deferred to the per-case service reset"
    exit 0
fi

ram_root="$(pulsar_ram_root)"
deadline=$((SECONDS + timeout_sec))
while (( SECONDS < deadline )); do
    used_percent="$(
        srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" \
            bash -lc "df -P '${ram_root}' | tail -n 1 | tr -s ' ' | cut -d ' ' -f 5 | tr -d '%'" \
            2>>"$verify_log" | tail -n 1 || true
    )"
    if [[ "$used_percent" =~ ^[0-9]+$ ]] \
        && (( 100 - used_percent >= min_free_percent )); then
        log_info "Pulsar stream deleted; tmpfs free=$((100 - used_percent))%"
        exit 0
    fi
    sleep "$poll_sec"
done

log_error "Pulsar tmpfs did not recover to ${min_free_percent}% free after stream deletion"
exit 1
