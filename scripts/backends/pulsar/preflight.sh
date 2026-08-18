#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
CONFIG_PATH="${1:-${CONFIG_PATH:-}}"
PULSAR_HOME="${PULSAR_HOME:-$PROJECT_ROOT/.local/pulsar-current}"
export PULSAR_HOME

if [[ -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi
# shellcheck source=../../hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"
select_backend_hpc_modules pulsar
load_hpc_modules

[[ -f "$CONFIG_PATH" ]] || { printf '[pulsar-preflight] missing config: %s\n' "$CONFIG_PATH" >&2; exit 1; }
[[ -x "${PULSAR_HOME:-}/bin/pulsar" ]] || {
    printf '[pulsar-preflight] Pulsar is not installed; run ./scripts/install_local_pulsar.sh\n' >&2
    exit 1
}

java_major="$(java -version 2>&1 | awk -F'[\".]' '/version/ {print $2; exit}')"
if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 21 )); then
    printf '[pulsar-preflight] Pulsar 5.0.0-M1 requires Java 21 or newer; found Java %s. Run ./benchmark.sh prepare-hpc pulsar or set PULSAR_JAVA_HOME to a verified JDK 21 installation.\n' "${java_major:-unknown}" >&2
    exit 1
fi

PYTHONPATH="$PROJECT_ROOT/.local/python:$PROJECT_ROOT:${PYTHONPATH:-}" \
    python3 - <<'PY'
from pathlib import Path

import certifi
import pulsar

assert pulsar.__version__ == "3.13.0"
assert Path(certifi.where()).is_file()
client = pulsar.Client("pulsar://127.0.0.1:1", operation_timeout_seconds=1)
client.close()
PY
PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
    python3 -B "$PROJECT_ROOT/scripts/verify_pulsar_profile.py" \
    "$CONFIG_PATH" --project-root "$PROJECT_ROOT" >/dev/null

printf '[pulsar-preflight] Pulsar: %s\n' "$("$PULSAR_HOME/bin/pulsar" version | head -n 1)"
printf '[pulsar-preflight] Java major: %s\n' "$java_major"
printf '[pulsar-preflight] Python client: 3.13.0\n'
printf '[pulsar-preflight] Python client dependencies: verified\n'
printf '[pulsar-preflight] Configuration and immutable profile verified\n'
