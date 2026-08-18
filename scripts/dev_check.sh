#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Fast local correctness checks.
#
# This intentionally avoids requiring Kafka, Slurm, mpi4py, confluent-kafka, or a
# running broker. Use it before starting local Kafka or submitting Slurm jobs.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

if [[ "${KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH:-0}" != "1" && -d "$PROJECT_ROOT/.local/python" ]]; then
    export PYTHONPATH="$PROJECT_ROOT/.local/python:$PROJECT_ROOT:${PYTHONPATH:-}"
else
    export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"
fi
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-${TMPDIR:-/tmp}/kafka-simple-benchmark-pycache}"

echo "[dev-check] Running smoke tests"
python3 -B tests/smoke_test.py

echo "[dev-check] Running reproducible-workflow tests"
python3 -B tests/test_reproducible_workflow.py

if python3 -c "import pytest" >/dev/null 2>&1; then
    echo "[dev-check] Running pytest"
    python3 -m pytest -q tests
else
    echo "[dev-check] pytest is not installed; skipping pytest run"
fi

echo "[dev-check] Checking shell syntax"
while IFS= read -r -d '' script; do
    bash -n "$script"
done < <(
    find scripts tests -type f -name '*.sh' -print0
    printf '%s\0' benchmark.sh run_all.sh
)

echo "[dev-check] Compiling Python files"
python3 -B -m compileall -q models src tests scripts

echo "[dev-check] Validating JSON configs"
while IFS= read -r -d '' config; do
    python3 -m json.tool "$config" >/dev/null
done < <(find configs schemas -type f -name '*.json' -print0)

echo "[dev-check] Checking backend registry"
./benchmark.sh backend list >/dev/null

echo "[dev-check] All local development checks passed"
