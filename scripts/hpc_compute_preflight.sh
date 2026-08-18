#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Compute-node preflight for the Slurm benchmark runtime.
#
# Run this inside an interactive allocation or a tiny diagnostic Slurm job before
# submitting the first real pilot. It records the active Python/Java/MPI stack,
# verifies mpi4py and the selected backend client's import path, checks /dev/shm,
# and scans mpi4py's MPI extension for CUDA-linked runtime dependencies.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

CONFIG_PATH="${1:-configs/one_broker_mpi_simultaneous.json}"
OUTPUT_DIR="${HPC_COMPUTE_PREFLIGHT_DIR:-$PROJECT_ROOT/.local/hpc_compute_preflight}"
mkdir -p "$OUTPUT_DIR"

REQUESTED_BACKEND_ID="${BACKEND_ID:-}"
if [[ -z "$REQUESTED_BACKEND_ID" ]]; then
    REQUESTED_BACKEND_ID="$(python3 - "$CONFIG_PATH" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    payload = json.load(handle)
print(str(payload.get("backend_id", "kafka")).strip() or "kafka")
PY
)"
fi
export BACKEND_ID="$REQUESTED_BACKEND_ID"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

if [[ -n "${SLURM_HPC_MODULES:-}" ]]; then
    export HPC_MODULES="$SLURM_HPC_MODULES"
fi
export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"
select_backend_hpc_modules "$REQUESTED_BACKEND_ID"
load_hpc_modules

# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"
export_benchmark_hpc_python_env "$PROJECT_ROOT"

errors=0
warnings=0

pass() {
    printf '[hpc-compute-preflight] PASS: %s\n' "$*"
}

warn() {
    warnings=$((warnings + 1))
    printf '[hpc-compute-preflight] WARN: %s\n' "$*" >&2
}

fail() {
    errors=$((errors + 1))
    printf '[hpc-compute-preflight] FAIL: %s\n' "$*" >&2
}

check_command() {
    local command_name="${1:?command name required}"
    if command -v "$command_name" >/dev/null 2>&1; then
        pass "command available: $command_name ($(command -v "$command_name"))"
    else
        fail "command missing: $command_name"
    fi
}

check_file() {
    local file_path="${1:?file path required}"
    local label="${2:-file}"
    if [[ -f "$file_path" ]]; then
        pass "$label exists: $file_path"
    else
        fail "$label missing: $file_path"
    fi
}

ENV_FILE="$OUTPUT_DIR/env.txt"
IMPORTS_JSON="$OUTPUT_DIR/imports.json"
IMPORTS_LOG="$OUTPUT_DIR/imports.log"
LDD_FILE="$OUTPUT_DIR/mpi4py_ldd.txt"
CONFIG_INFO_FILE="$OUTPUT_DIR/config_info.txt"
MPI_SMOKE_LOG="$OUTPUT_DIR/mpirun_mpi4py_smoke.log"
REQUIRE_COMPUTE_MPI4PY="${REQUIRE_COMPUTE_MPI4PY:-1}"
BENCHMARK_NETWORK_INTERFACE="${BENCHMARK_NETWORK_INTERFACE:-${KAFKA_HPC_NETWORK_INTERFACE:-ib0}}"
BENCHMARK_REQUIRE_FABRIC="${BENCHMARK_REQUIRE_FABRIC:-${KAFKA_HPC_REQUIRE_FABRIC:-1}}"

{
    printf 'DATE=%s\n' "$(date '+%Y-%m-%d %H:%M:%S %z')"
    printf 'HOSTNAME=%s\n' "$(hostname)"
    printf 'PROJECT_ROOT=%s\n' "$PROJECT_ROOT"
    printf 'CONFIG_PATH=%s\n' "$CONFIG_PATH"
    printf 'BACKEND_ID=%s\n' "$REQUESTED_BACKEND_ID"
    printf 'SLURM_JOB_ID=%s\n' "${SLURM_JOB_ID:-}"
    printf 'SLURM_JOB_NODELIST=%s\n' "${SLURM_JOB_NODELIST:-}"
    printf 'SLURM_NNODES=%s\n' "${SLURM_NNODES:-}"
    printf 'HPC_MODULES=%s\n' "${HPC_MODULES:-}"
    printf 'BENCHMARK_NETWORK_INTERFACE=%s\n' "$BENCHMARK_NETWORK_INTERFACE"
    printf 'BENCHMARK_REQUIRE_FABRIC=%s\n' "$BENCHMARK_REQUIRE_FABRIC"
    printf 'PYTHONPATH=%s\n' "${PYTHONPATH:-}"
    printf 'LD_LIBRARY_PATH=%s\n' "${LD_LIBRARY_PATH:-}"
    printf '\n[python]\n'
    python3 --version 2>&1 || true
    command -v python3 || true
    printf '\n[java]\n'
    java -version 2>&1 || true
    command -v java || true
    printf '\n[mpirun]\n'
    command -v mpirun || true
    mpirun --version 2>&1 | head -20 || true
    printf '\n[mpicc]\n'
    command -v mpicc || true
    mpicc --showme:command 2>&1 || mpicc -show 2>&1 || true
    printf '\n[modules]\n'
    if type module >/dev/null 2>&1; then
        module list 2>&1 || true
    else
        printf 'module command unavailable\n'
    fi
    printf '\n[/dev/shm]\n'
    df -h /dev/shm 2>&1 || true
    touch /dev/shm/messaging_benchmark_preflight_write_test.$$ 2>&1 && rm -f /dev/shm/messaging_benchmark_preflight_write_test.$$ || true
    printf '\n[network]\n'
    ip -br addr 2>&1 || true
    ip route 2>&1 || true
} > "$ENV_FILE"

printf '[hpc-compute-preflight] Project root: %s\n' "$PROJECT_ROOT"
printf '[hpc-compute-preflight] Config: %s\n' "$CONFIG_PATH"
printf '[hpc-compute-preflight] Backend: %s\n' "$REQUESTED_BACKEND_ID"
printf '[hpc-compute-preflight] Diagnostics directory: %s\n' "$OUTPUT_DIR"
printf '[hpc-compute-preflight] Benchmark network interface: %s\n' "$BENCHMARK_NETWORK_INTERFACE"

check_file "$CONFIG_PATH" "benchmark config"
for command_name in python3 java mpirun mpicc; do
    check_command "$command_name"
done

if command -v ip >/dev/null 2>&1; then
    if [[ "$BENCHMARK_NETWORK_INTERFACE" == "hostname" || "$BENCHMARK_NETWORK_INTERFACE" == "host" || "$BENCHMARK_NETWORK_INTERFACE" == "none" ]]; then
        warn "benchmark network interface is set to hostname mode; this may use the slower LAN path"
    elif fabric_address="$(ip -4 -o addr show dev "$BENCHMARK_NETWORK_INTERFACE" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -n 1)" && [[ -n "$fabric_address" ]]; then
        pass "network interface $BENCHMARK_NETWORK_INTERFACE has address $fabric_address on $(hostname)"
    elif [[ "$BENCHMARK_REQUIRE_FABRIC" == "1" ]]; then
        fail "network interface $BENCHMARK_NETWORK_INTERFACE has no IPv4 address on $(hostname)"
    else
        warn "network interface $BENCHMARK_NETWORK_INTERFACE has no IPv4 address on $(hostname); benchmark may fall back to hostnames"
    fi
else
    warn "ip command unavailable; network interface check skipped"
fi

if python3 - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
then
    pass "python3 version is new enough: $(python3 --version 2>&1)"
else
    fail "python3 must be >= 3.10; current version is: $(python3 --version 2>&1 || printf unknown)"
fi

if python3 - "$IMPORTS_JSON" "$REQUESTED_BACKEND_ID" <<'PY' > "$IMPORTS_LOG" 2>&1
from __future__ import annotations

import json
import sys
from pathlib import Path

output = Path(sys.argv[1])
backend_id = sys.argv[2]

import mpi4py

module_path = Path(mpi4py.__file__).resolve()
extension_candidates = sorted(module_path.parent.glob("MPI*.so"))
extension_path = str(extension_candidates[0].resolve()) if extension_candidates else ""

info = {
    "backend_id": backend_id,
    "mpi4py_file": str(module_path),
    "mpi4py_version": mpi4py.__version__,
    "mpi4py_extension": extension_path,
}
if backend_id == "kafka":
    import confluent_kafka

    info.update(
        {
            "backend_client": "confluent_kafka",
            "backend_client_file": getattr(confluent_kafka, "__file__", ""),
            "backend_client_version": confluent_kafka.version(),
        }
    )
elif backend_id == "pulsar":
    import certifi
    import pulsar

    ca_bundle = Path(certifi.where()).resolve()
    if not ca_bundle.is_file():
        raise RuntimeError(f"certifi CA bundle is missing: {ca_bundle}")
    client = pulsar.Client(
        "pulsar://127.0.0.1:1",
        operation_timeout_seconds=1,
    )
    client.close()
    info.update(
        {
            "backend_client": "pulsar",
            "backend_client_file": getattr(pulsar, "__file__", ""),
            "backend_client_version": getattr(pulsar, "__version__", "unknown"),
            "certifi_file": getattr(certifi, "__file__", ""),
            "certifi_version": getattr(certifi, "__version__", "unknown"),
            "certifi_ca_bundle": str(ca_bundle),
        }
    )
else:
    raise RuntimeError(f"unsupported backend for compute preflight: {backend_id}")
output.write_text(json.dumps(info, indent=2, default=str) + "\n", encoding="utf-8")
print(json.dumps(info, indent=2, default=str))
PY
then
    pass "Python runtime imports mpi4py and the $REQUESTED_BACKEND_ID client"
else
    fail "Python runtime cannot import mpi4py and the $REQUESTED_BACKEND_ID client; inspect $IMPORTS_LOG"
fi

if command -v mpirun >/dev/null 2>&1; then
    if mpirun --oversubscribe -np 1 python3 - <<'PY' > "$MPI_SMOKE_LOG" 2>&1
from mpi4py import MPI

print(f"vendor={MPI.get_vendor()}")
print(MPI.Get_library_version())
PY
    then
        pass "mpirun mpi4py smoke passed; log written to $MPI_SMOKE_LOG"
    else
        fail "mpirun mpi4py smoke failed; inspect $MPI_SMOKE_LOG"
    fi
else
    warn "mpirun unavailable; mpi4py runtime smoke skipped"
fi

MPI4PY_FILE=""
if [[ -f "$IMPORTS_JSON" ]]; then
    MPI4PY_FILE="$(
        python3 - "$IMPORTS_JSON" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    print(json.loads(path.read_text(encoding="utf-8")).get("mpi4py_file", ""))
except Exception:
    print("")
PY
    )"
fi
if [[ -n "$MPI4PY_FILE" ]]; then
    COMPUTE_PYTHON_DIR="$(
        python3 - "$PROJECT_ROOT/.local/python-hpc" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).resolve())
PY
    )"
    MPI4PY_FILE_RESOLVED="$(
        python3 - "$MPI4PY_FILE" <<'PY'
import sys
from pathlib import Path

print(Path(sys.argv[1]).resolve())
PY
    )"
    case "$MPI4PY_FILE_RESOLVED" in
        "$COMPUTE_PYTHON_DIR"/*)
            pass "mpi4py imports from compute-node target: $MPI4PY_FILE"
            ;;
        *)
            if [[ "$REQUIRE_COMPUTE_MPI4PY" == "1" ]]; then
                fail "mpi4py is not importing from .local/python-hpc: $MPI4PY_FILE"
            else
                warn "mpi4py is not importing from .local/python-hpc: $MPI4PY_FILE"
            fi
            ;;
    esac
fi

MPI_EXTENSION=""
if [[ -f "$IMPORTS_JSON" ]]; then
    MPI_EXTENSION="$(
        python3 - "$IMPORTS_JSON" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    print(json.loads(path.read_text(encoding="utf-8")).get("mpi4py_extension", ""))
except Exception:
    print("")
PY
    )"
fi

if [[ -n "$MPI_EXTENSION" && -f "$MPI_EXTENSION" ]]; then
    if command -v ldd >/dev/null 2>&1; then
        ldd "$MPI_EXTENSION" > "$LDD_FILE" || true
        pass "mpi4py extension dependency scan written to $LDD_FILE"
        if grep -q 'libcuda\.so\.1.*not found' "$LDD_FILE"; then
            fail "mpi4py/OpenMPI requires missing libcuda.so.1 on this node"
        elif grep -q 'libcuda\.so\.1' "$LDD_FILE"; then
            warn "mpi4py/OpenMPI links libcuda.so.1; verify this is valid on CPU jobs"
        else
            pass "mpi4py dependency scan does not reference libcuda.so.1"
        fi
    else
        warn "ldd is unavailable; mpi4py dependency scan skipped"
    fi
else
    warn "mpi4py extension path was not available; dependency scan skipped"
fi

if [[ -d /dev/shm && -w /dev/shm ]]; then
    if touch /dev/shm/messaging_benchmark_preflight_write_test.$$ >/dev/null 2>&1; then
        rm -f /dev/shm/messaging_benchmark_preflight_write_test.$$
        pass "/dev/shm exists and is writable"
    else
        warn "/dev/shm exists but write test failed"
    fi
else
    warn "/dev/shm is missing or not writable"
fi

if CONFIG_INFO="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$CONFIG_PATH" <<'PY'
import shlex
import sys
from pathlib import Path

from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config

path = Path(sys.argv[1])
config = load_benchmark_config(path)
if config.mode != "single":
    raise SystemExit("The current runner supports only mode='single'")
plan = get_backend(config.backend_id).resource_plan(config)

print(f"BACKEND_ID={shlex.quote(config.backend_id)}")
print(f"MODE={shlex.quote(config.mode)}")
print("ESTIMATED_CASES=1")
print(f"SERVICE_NODE_COUNT={plan.service_nodes}")
print(f"PRODUCER_RANKS={config.producer_ranks}")
print(f"CONSUMER_RANKS={config.consumer_ranks}")
print(f"TOTAL_MPI_RANKS={config.total_mpi_ranks}")
PY
)"; then
    printf '%s\n' "$CONFIG_INFO" > "$CONFIG_INFO_FILE"
    eval "$CONFIG_INFO"
    pass "config validates as backend=${BACKEND_ID:-unknown}, mode=${MODE:-unknown}, estimated cases=${ESTIMATED_CASES:-unknown}"
else
    fail "config validation failed: $CONFIG_PATH"
fi

if [[ -n "${SLURM_JOB_NODELIST:-}" ]] && command -v scontrol >/dev/null 2>&1; then
    mapfile -t allocated_nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
    allocated_count="${#allocated_nodes[@]}"
    controller_nodes="${SLURM_CONTROLLER_NODES:-0}"
    producer_nodes="${SLURM_PRODUCER_NODES:-$(( PRODUCER_RANKS > 0 ? 1 : 0 ))}"
    consumer_nodes="${SLURM_CONSUMER_NODES:-$(( CONSUMER_RANKS > 0 ? 1 : 0 ))}"
    needed_count=$(( ${SERVICE_NODE_COUNT:-1} + 1 + controller_nodes + producer_nodes + consumer_nodes ))
    if (( allocated_count >= needed_count )); then
        pass "Slurm allocation has ${allocated_count} nodes; need at least ${needed_count}"
    else
        fail "Slurm allocation has ${allocated_count} nodes; need at least ${needed_count}"
    fi
else
    warn "not running inside a Slurm allocation; node-count check skipped"
fi

printf '[hpc-compute-preflight] Wrote diagnostics:\n'
printf '  %s\n' "$ENV_FILE"
printf '  %s\n' "$IMPORTS_JSON"
printf '  %s\n' "$LDD_FILE"
printf '[hpc-compute-preflight] Summary: %s error(s), %s warning(s)\n' "$errors" "$warnings"

if (( errors > 0 )); then
    exit 1
fi
