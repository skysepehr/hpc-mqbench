#!/usr/bin/env bash

# -----------------------------------------------------------------------------
# Source-able helper for scripts that need pip without assuming the active Python
# module ships it by default.
#
# Some HPC Python modules provide `ensurepip` and `venv` but do not expose
# `python3 -m pip` directly. The benchmark installers only need pip as a build
# frontend for vendored archives, so this helper creates a repo-local virtual
# environment under .local/pip-venv when necessary and runs pip from there.
# -----------------------------------------------------------------------------

python_has_wheel_build_command() {
    local python_command="${1:?python command required}"
    "$python_command" - <<'PY' >/dev/null 2>&1
import wheel.bdist_wheel
PY
}

ensure_repo_local_pip_venv() {
    local venv_dir="${LOCAL_PIP_VENV_DIR:-$PROJECT_ROOT/.local/pip-venv}"
    local venv_python="$venv_dir/bin/python"
    local wheel_archive="${LOCAL_WHEEL_BUILD_ARCHIVE:-$PROJECT_ROOT/tools/archives/wheel-0.45.1-py3-none-any.whl}"

    if [[ ! -x "$venv_python" ]]; then
        printf '[python-pip] creating repo-local pip venv: %s\n' "$venv_dir"
        mkdir -p "$(dirname "$venv_dir")"
        python3 -m venv "$venv_dir"
    fi

    if ! "$venv_python" -m pip --version >/dev/null 2>&1; then
        printf '[python-pip] bootstrapping pip inside repo-local venv\n'
        "$venv_python" -m ensurepip --upgrade
    fi

    if ! "$venv_python" -m pip --version >/dev/null 2>&1; then
        printf '[python-pip] ERROR: pip is not available through python3 or %s\n' "$venv_python" >&2
        return 1
    fi

    if ! python_has_wheel_build_command "$venv_python"; then
        if [[ ! -f "$wheel_archive" ]]; then
            printf '[python-pip] ERROR: vendored wheel build archive not found: %s\n' "$wheel_archive" >&2
            return 1
        fi
        printf '[python-pip] installing vendored wheel build helper into repo-local pip venv\n'
        "$venv_python" -m pip install --no-index --no-deps --upgrade "$wheel_archive"
    fi

    PYTHON_PIP_RUNNER=("$venv_python" -m pip)
}

ensure_python_pip() {
    if python3 -m pip --version >/dev/null 2>&1 && python_has_wheel_build_command python3; then
        PYTHON_PIP_RUNNER=(python3 -m pip)
        return 0
    fi

    if python3 -m pip --version >/dev/null 2>&1; then
        printf '[python-pip] python3 -m pip is available, but wheel.bdist_wheel is missing; using repo-local pip venv\n'
    fi

    ensure_repo_local_pip_venv
}

python_pip() {
    "${PYTHON_PIP_RUNNER[@]}" "$@"
}
