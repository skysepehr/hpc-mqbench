#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Install mpi4py from vendored wheel/source archives into a repo-local target.
#
# This script does not use sudo, does not download anything, and does not install
# global Python packages. If no compatible wheel is vendored, it builds mpi4py
# against the MPI compiler wrapper available on the current machine, usually
# mpicc. LOCAL_PYTHON_DEPS_DIR defaults to .local/python and can be set to
# .local/python-hpc for compute-node Slurm builds.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

MPI4PY_VERSION="${MPI4PY_VERSION:-4.1.1}"
ARCHIVE="${LOCAL_MPI4PY_ARCHIVE:-$PROJECT_ROOT/tools/archives/mpi4py-${MPI4PY_VERSION}.tar.gz}"
EXPECTED_SHA256="${LOCAL_MPI4PY_SHA256:-}"
CYTHON_VERSION="${CYTHON_VERSION:-3.2.4}"
CYTHON_WHEEL="${LOCAL_CYTHON_WHEEL:-$PROJECT_ROOT/tools/archives/cython-${CYTHON_VERSION}-py3-none-any.whl}"
CYTHON_SHA256="${LOCAL_CYTHON_SHA256:-732fc93bc33ae4b14f6afaca663b916c2fdd5dcbfad7114e17fb2434eeaea45c}"
TARGET_DIR="${LOCAL_PYTHON_DEPS_DIR:-$PROJECT_ROOT/.local/python}"
BUILD_LOG="${LOCAL_MPI4PY_BUILD_LOG:-$PROJECT_ROOT/.local/mpi4py-build.log}"
MPICC_COMMAND="${MPICC:-mpicc}"

# shellcheck source=./python_pip_common.sh
source "$PROJECT_ROOT/scripts/python_pip_common.sh"
PYTHON_TAG="$(python3 - <<'PY'
import sys
print(f"cp{sys.version_info.major}{sys.version_info.minor}")
PY
)"
WHEEL_CANDIDATE="${LOCAL_MPI4PY_WHEEL:-}"

if [[ -z "$WHEEL_CANDIDATE" ]]; then
    for candidate in "$PROJECT_ROOT"/tools/archives/mpi4py-"$MPI4PY_VERSION"-"$PYTHON_TAG"-"$PYTHON_TAG"-*.whl; do
        if [[ -f "$candidate" ]]; then
            WHEEL_CANDIDATE="$candidate"
            break
        fi
    done
fi

if [[ -z "$EXPECTED_SHA256" \
    && "$MPI4PY_VERSION" == "4.1.1" \
    && "$ARCHIVE" == "$PROJECT_ROOT/tools/archives/mpi4py-4.1.1.tar.gz" ]]; then
    EXPECTED_SHA256="eb2c8489bdbc47fdc6b26ca7576e927a11b070b6de196a443132766b3d0a2a22"
fi

if [[ -n "$WHEEL_CANDIDATE" \
    && -z "${LOCAL_MPI4PY_ARCHIVE:-}" \
    && -z "${LOCAL_MPI4PY_SHA256:-}" ]]; then
    ARCHIVE="$WHEEL_CANDIDATE"
    if [[ "$(basename "$ARCHIVE")" == "mpi4py-4.1.1-cp312-cp312-manylinux1_x86_64.whl" ]]; then
        EXPECTED_SHA256="ed3d9b619bf197a290f7fd67eb61b1c2a5c204afd9621651a50dc0b1c1280d45"
    else
        EXPECTED_SHA256=""
    fi
fi

log() {
    printf '[install-local-mpi4py] %s\n' "$*"
}

die() {
    printf '[install-local-mpi4py] ERROR: %s\n' "$*" >&2
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

verify_archive() {
    if [[ -z "$EXPECTED_SHA256" ]]; then
        log "Archive checksum validation skipped by LOCAL_MPI4PY_SHA256="
        return
    fi

    if ! command -v sha256sum >/dev/null 2>&1; then
        log "WARNING: sha256sum not found; cannot validate archive checksum"
        return
    fi

    local actual_sha256
    actual_sha256="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
    if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
        die "Archive checksum mismatch for $ARCHIVE"
    fi
}

verify_file_checksum() {
    local file_path="${1:?file path required}"
    local expected_sha256="${2:-}"

    if [[ -z "$expected_sha256" ]]; then
        return
    fi

    if ! command -v sha256sum >/dev/null 2>&1; then
        log "WARNING: sha256sum not found; cannot validate $file_path"
        return
    fi

    local actual_sha256
    actual_sha256="$(sha256sum "$file_path" | awk '{print $1}')"
    if [[ "$actual_sha256" != "$expected_sha256" ]]; then
        die "Checksum mismatch for $file_path"
    fi
}

check_import_from_target() {
    KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" python3 - "$TARGET_DIR" <<'PY' >/dev/null 2>&1
from pathlib import Path
import importlib
import sys

target = Path(sys.argv[1]).resolve()
module = importlib.import_module("mpi4py")
module_path = Path(module.__file__).resolve()
if target not in module_path.parents:
    raise SystemExit(1)
print(module_path)
PY
}

cython_available_from_target() {
    KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" python3 - <<'PY' >/dev/null 2>&1
import Cython
print(Cython.__version__)
PY
}

install_cython_if_needed() {
    if [[ "$ARCHIVE" == *.whl ]]; then
        return
    fi

    if cython_available_from_target; then
        return
    fi

    require_file "$CYTHON_WHEEL"
    verify_file_checksum "$CYTHON_WHEEL" "$CYTHON_SHA256"

    log "Installing vendored Cython build helper into: $TARGET_DIR"
    if ! KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
        python_pip install \
        --no-index \
        --no-deps \
        --upgrade \
        --target "$TARGET_DIR" \
        "$CYTHON_WHEEL" \
        >>"$BUILD_LOG" 2>&1; then
        tail -n 120 "$BUILD_LOG" >&2 || true
        die "Cython install failed; mpi4py source builds require Cython >= 3.0.1"
    fi
}

require_command python3
require_file "$ARCHIVE"
verify_archive
ensure_python_pip || die "pip is required to build the local mpi4py package"

mkdir -p "$TARGET_DIR" "$(dirname "$BUILD_LOG")"

if check_import_from_target; then
    log "mpi4py already imports from local target: $TARGET_DIR"
    KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" python3 - <<'PY'
import mpi4py
print(f"[install-local-mpi4py] version: {mpi4py.__version__}")
print(f"[install-local-mpi4py] module: {mpi4py.__file__}")
PY
    exit 0
fi

if [[ "$ARCHIVE" != *.whl ]]; then
    require_command "$MPICC_COMMAND"
fi
install_cython_if_needed

log "Installing mpi4py $MPI4PY_VERSION into: $TARGET_DIR"
log "Package archive: $ARCHIVE"
if [[ "$ARCHIVE" != *.whl ]]; then
    log "MPI compiler wrapper: $MPICC_COMMAND"
fi
log "Build log: $BUILD_LOG"

if ! KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
    MPICC="$MPICC_COMMAND" \
    python_pip install \
    --no-index \
    --no-build-isolation \
    --no-deps \
    --upgrade \
    --force-reinstall \
    --target "$TARGET_DIR" \
    "$ARCHIVE" \
    >"$BUILD_LOG" 2>&1; then
    tail -n 120 "$BUILD_LOG" >&2 || true
    die "mpi4py build failed. Make sure mpicc, Python development headers, and MPI development files are installed."
fi

if ! check_import_from_target; then
    tail -n 120 "$BUILD_LOG" >&2 || true
    die "mpi4py build finished, but import validation failed"
fi

KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 \
PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" python3 - <<'PY'
import mpi4py
print(f"[install-local-mpi4py] version: {mpi4py.__version__}")
print(f"[install-local-mpi4py] module: {mpi4py.__file__}")
PY

log "Local Python dependency path:"
log "  $TARGET_DIR"
