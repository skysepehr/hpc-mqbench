#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
pulsar_require_case
pulsar_load_nodes
require_nonempty "PULSAR_TOPIC_NAME" "${PULSAR_TOPIC_NAME:-}"
require_nonempty "PARTITIONS" "${PARTITIONS:-}"

admin_url="$(<"$CASE_DIR/runtime/pulsar_admin_url.txt")"
metadata_file="$(pulsar_runtime_dir)/topic_metadata.json"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"
ensure_dir "$CASE_DIR/logs/pulsar"
srun --overlap --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}
    '${PULSAR_HOME}/bin/pulsar-admin' --admin-url '${admin_url}' \
        topics delete-partitioned-topic --force '${PULSAR_TOPIC_NAME}' >/dev/null 2>&1 || true
    '${PULSAR_HOME}/bin/pulsar-admin' --admin-url '${admin_url}' \
        topics create-partitioned-topic --partitions '${PARTITIONS}' '${PULSAR_TOPIC_NAME}'
    '${PULSAR_HOME}/bin/pulsar-admin' --admin-url '${admin_url}' \
        topics get-partitioned-topic-metadata '${PULSAR_TOPIC_NAME}' > '${metadata_file}'
    cat '${metadata_file}'
" > "$CASE_DIR/logs/pulsar/create-stream.log" 2>&1
python3 - "$metadata_file" "$PARTITIONS" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
actual = int(metadata.get("partitions", -1))
expected = int(sys.argv[2])
if actual != expected:
    raise SystemExit(
        f"Pulsar topic partition mismatch: runtime={actual} expected={expected}"
    )
PY
log_info "Created Pulsar partitioned topic $PULSAR_TOPIC_NAME with $PARTITIONS partitions"
