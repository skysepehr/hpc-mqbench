#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"
pulsar_require_case
pulsar_load_nodes

timeout_sec="${PULSAR_READY_TIMEOUT_SEC:-120}"
wait_for_tcp_endpoint "$PULSAR_ADDRESS" 6650 "$timeout_sec" "Pulsar binary service"
wait_for_tcp_endpoint "$PULSAR_ADDRESS" 8080 "$timeout_sec" "Pulsar admin service"

python3 - "$PULSAR_ADDRESS" "$timeout_sec" <<'PY'
import sys
import time
import urllib.request

host, raw_timeout = sys.argv[1:]
deadline = time.monotonic() + int(raw_timeout)
url = f"http://{host}:8080/admin/v2/brokers/health"
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            if response.status == 200:
                raise SystemExit(0)
    except Exception:
        time.sleep(1)
raise SystemExit(f"Pulsar health endpoint did not become ready: {url}")
PY
log_info "Pulsar standalone is ready"
