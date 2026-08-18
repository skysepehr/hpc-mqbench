#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

RESULTS_DIR="${1:-}"
OUTPUT_DIR="${2:-}"
if [[ -z "$RESULTS_DIR" || -z "$OUTPUT_DIR" ]]; then
    printf 'Usage: %s MACHINE_RESULTS_DIR REPORT_OUTPUT_DIR [--compile-pdf] [--skip-figures]\n' "$0" >&2
    exit 2
fi
shift 2

# Reporting dependencies are loaded only by this optional post-processing path.
# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"
export_benchmark_hpc_python_env "$PROJECT_ROOT"
if [[ -d "$PROJECT_ROOT/analysis_deps" ]]; then
    export PYTHONPATH="$PROJECT_ROOT/analysis_deps:$PYTHONPATH"
fi
mkdir -p "$PROJECT_ROOT/.local/matplotlib"
export MPLCONFIGDIR="${MPLCONFIGDIR:-$PROJECT_ROOT/.local/matplotlib}"

exec python3 -B "$PROJECT_ROOT/scripts/generate_reproducible_report.py" \
    --results-dir "$RESULTS_DIR" \
    --output-dir "$OUTPUT_DIR" \
    "$@"
