#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Install optional local development Python tools from vendored wheels.
#
# This script installs pytest and matplotlib into .local/python without using
# sudo, without touching global Python packages, and without downloading
# anything. pytest enables the fuller local test command; matplotlib enables PNG
# graph generation for monitoring bundles.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

ARCHIVE_DIR="${LOCAL_PYTHON_WHEEL_DIR:-$PROJECT_ROOT/tools/archives}"
TARGET_DIR="${LOCAL_PYTHON_DEPS_DIR:-$PROJECT_ROOT/.local/python}"
BUILD_LOG="${LOCAL_OPTIONAL_PYTHON_TOOLS_LOG:-$PROJECT_ROOT/.local/optional-python-tools-install.log}"

# shellcheck source=./python_pip_common.sh
source "$PROJECT_ROOT/scripts/python_pip_common.sh"

PACKAGES=(
    pytest==9.0.3
    matplotlib==3.10.9
)

log() {
    printf '[install-local-optional-python-tools] %s\n' "$*"
}

die() {
    printf '[install-local-optional-python-tools] ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local command_name="${1:?command name required}"
    command -v "$command_name" >/dev/null 2>&1 || die "Required command not found: $command_name"
}

validate_wheel_available() {
    local pattern="${1:?wheel pattern required}"
    compgen -G "$ARCHIVE_DIR/$pattern" >/dev/null || die "Missing vendored wheel matching: $ARCHIVE_DIR/$pattern"
}

check_imports_from_target() {
    MPLCONFIGDIR="$PROJECT_ROOT/.local/matplotlib" \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
        python3 - "$TARGET_DIR" <<'PY' >/dev/null 2>&1
from pathlib import Path
import importlib
import sys

target = Path(sys.argv[1]).resolve()
for module_name in ("pytest", "matplotlib"):
    module = importlib.import_module(module_name)
    module_path = Path(module.__file__).resolve()
    if target not in module_path.parents:
        raise SystemExit(f"{module_name} imported from outside local target: {module_path}")
PY
}

require_command python3
[[ -d "$ARCHIVE_DIR" ]] || die "Wheel archive directory not found: $ARCHIVE_DIR"
ensure_python_pip || die "pip is required to install optional local Python tools"

validate_wheel_available "pytest-9.0.3-*.whl"
validate_wheel_available "matplotlib-3.10.9-*.whl"

mkdir -p "$TARGET_DIR" "$(dirname "$BUILD_LOG")"
mkdir -p "$PROJECT_ROOT/.local/matplotlib"

if check_imports_from_target; then
    log "pytest and matplotlib already import from local target: $TARGET_DIR"
    MPLCONFIGDIR="$PROJECT_ROOT/.local/matplotlib" \
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
        python3 - <<'PY'
import matplotlib
import pytest
print(f"[install-local-optional-python-tools] pytest: {pytest.__version__}")
print(f"[install-local-optional-python-tools] matplotlib: {matplotlib.__version__}")
PY
    exit 0
fi

log "Installing optional Python tools into: $TARGET_DIR"
log "Wheel archive directory: $ARCHIVE_DIR"
log "Install log: $BUILD_LOG"

if ! python_pip install \
    --no-index \
    --find-links "$ARCHIVE_DIR" \
    --upgrade \
    --target "$TARGET_DIR" \
    "${PACKAGES[@]}" \
    >"$BUILD_LOG" 2>&1; then
    tail -n 120 "$BUILD_LOG" >&2 || true
    die "Optional Python tool install failed. Check that compatible wheels exist under tools/archives."
fi

if ! check_imports_from_target; then
    tail -n 120 "$BUILD_LOG" >&2 || true
    die "Install finished, but pytest/matplotlib did not import from $TARGET_DIR"
fi

MPLCONFIGDIR="$PROJECT_ROOT/.local/matplotlib" \
PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
    python3 - <<'PY'
import matplotlib
import pytest
print(f"[install-local-optional-python-tools] pytest: {pytest.__version__}")
print(f"[install-local-optional-python-tools] matplotlib: {matplotlib.__version__}")
PY

log "Local Python dependency path:"
log "  $TARGET_DIR"
