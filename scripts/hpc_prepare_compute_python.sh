#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Build compute-node Python packages that depend on the active MPI runtime.
#
# Run this inside a compute-node allocation after scripts/hpc_prepare_repo.sh has
# prepared the repo on the login node. The main target is mpi4py, which must be
# compiled against the same mpicc/OpenMPI runtime that Slurm will use for the
# benchmark ranks. Output goes to .local/python-hpc/ so it can override any
# login-node mpi4py without replacing shared packages in .local/python/.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

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
load_hpc_modules

# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"

MPI4PY_VERSION="${MPI4PY_VERSION:-4.1.1}"
TARGET_DIR="${LOCAL_PYTHON_HPC_DIR:-$PROJECT_ROOT/.local/python-hpc}"
SOURCE_ARCHIVE="${LOCAL_MPI4PY_ARCHIVE:-$PROJECT_ROOT/tools/archives/mpi4py-${MPI4PY_VERSION}.tar.gz}"
BUILD_LOG="${LOCAL_MPI4PY_BUILD_LOG:-$PROJECT_ROOT/.local/mpi4py-hpc-build.log}"

die() {
    printf '[hpc-compute-python] ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local command_name="${1:?command name required}"
    command -v "$command_name" >/dev/null 2>&1 || die "Required command not found: $command_name"
}

require_file() {
    local file_path="${1:?file path required}"
    [[ -f "$file_path" ]] || die "Required file not found: $file_path"
}

require_command python3
require_command mpicc
require_file "$SOURCE_ARCHIVE"

MPICC_COMMAND="${HPC_COMPUTE_MPICC:-$(command -v mpicc)}"
export MPICC="$MPICC_COMMAND"

mkdir -p "$TARGET_DIR" "$(dirname "$BUILD_LOG")"

printf '[hpc-compute-python] Project root: %s\n' "$PROJECT_ROOT"
printf '[hpc-compute-python] Host: %s\n' "$(hostname)"
printf '[hpc-compute-python] Python: %s\n' "$(python3 --version 2>&1)"
printf '[hpc-compute-python] mpicc: %s\n' "$MPICC_COMMAND"
if "$MPICC_COMMAND" --showme:command >/dev/null 2>&1; then
    printf '[hpc-compute-python] mpicc command: %s\n' "$("$MPICC_COMMAND" --showme:command 2>&1)"
elif "$MPICC_COMMAND" -show >/dev/null 2>&1; then
    printf '[hpc-compute-python] mpicc command: %s\n' "$("$MPICC_COMMAND" -show 2>&1)"
fi
printf '[hpc-compute-python] mpi4py source archive: %s\n' "$SOURCE_ARCHIVE"
printf '[hpc-compute-python] target directory: %s\n' "$TARGET_DIR"
printf '[hpc-compute-python] build log: %s\n' "$BUILD_LOG"

LOCAL_PYTHON_DEPS_DIR="$TARGET_DIR" \
LOCAL_MPI4PY_ARCHIVE="$SOURCE_ARCHIVE" \
LOCAL_MPI4PY_BUILD_LOG="$BUILD_LOG" \
MPICC="$MPICC_COMMAND" \
    "$PROJECT_ROOT/scripts/install_local_mpi4py.sh"

export_kafka_hpc_python_env "$PROJECT_ROOT"

IMPORT_INFO_FILE="$PROJECT_ROOT/.local/hpc_compute_python_imports.txt"
LDD_FILE="$PROJECT_ROOT/.local/hpc_compute_python_mpi4py_ldd.txt"
MPI_SMOKE_LOG="$PROJECT_ROOT/.local/hpc_compute_python_mpirun_smoke.log"

python3 - "$TARGET_DIR" "$IMPORT_INFO_FILE" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

target = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2])

import mpi4py

module_path = Path(mpi4py.__file__).resolve()
if target not in module_path.parents:
    raise SystemExit(f"mpi4py imported from {module_path}, expected under {target}")

extension_candidates = sorted(module_path.parent.glob("MPI*.so"))
if not extension_candidates:
    raise SystemExit(f"mpi4py extension was not found under {module_path.parent}")
extension_path = extension_candidates[0].resolve()

info = {
    "mpi4py_version": mpi4py.__version__,
    "mpi4py_module": str(module_path),
    "mpi4py_extension": str(extension_path),
}
output.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
print(json.dumps(info, indent=2))
PY

MPI_EXTENSION="$(
    python3 - "$IMPORT_INFO_FILE" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["mpi4py_extension"])
PY
)"

if command -v ldd >/dev/null 2>&1; then
    ldd "$MPI_EXTENSION" > "$LDD_FILE" || true
    printf '[hpc-compute-python] mpi4py extension ldd: %s\n' "$LDD_FILE"
    if grep -q 'libcuda\.so\.1.*not found' "$LDD_FILE"; then
        die "mpi4py extension still requires missing libcuda.so.1; inspect $LDD_FILE"
    fi
    if grep -q 'libcuda\.so\.1' "$LDD_FILE"; then
        printf '[hpc-compute-python] WARNING: mpi4py/OpenMPI links libcuda.so.1; verify this is valid on the chosen partition.\n' >&2
    fi
else
    printf '[hpc-compute-python] WARNING: ldd is not available; dependency scan skipped.\n' >&2
fi

if command -v mpirun >/dev/null 2>&1; then
    if mpirun --oversubscribe -np 1 python3 - <<'PY' > "$MPI_SMOKE_LOG" 2>&1
from mpi4py import MPI

print(f"vendor={MPI.get_vendor()}")
print(MPI.Get_library_version())
PY
    then
        printf '[hpc-compute-python] mpirun mpi4py smoke passed: %s\n' "$MPI_SMOKE_LOG"
    else
        tail -n 120 "$MPI_SMOKE_LOG" >&2 || true
        die "mpirun mpi4py smoke failed; inspect $MPI_SMOKE_LOG"
    fi
else
    printf '[hpc-compute-python] WARNING: mpirun is unavailable; MPI runtime smoke skipped.\n' >&2
fi

printf '[hpc-compute-python] Compute Python path is ready.\n'
printf '[hpc-compute-python] Next diagnostic command:\n'
printf '  ./scripts/hpc_compute_preflight.sh configs/one_broker_mpi_simultaneous.json\n'
