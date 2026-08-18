#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if (( $# < 2 )); then
    cat >&2 <<'EOF'
Usage:
  ./scripts/compare_cases.sh <case-dir-or-final_report.json> <case-dir-or-final_report.json> [...]
  ./scripts/compare_cases.sh --html reports/compare_report.html <case-dir-or-final_report.json> [...]

Example:
  ./scripts/compare_cases.sh results/case_14229116 results/case_14237168
  ./scripts/compare_cases.sh --html reports/compare_report.html results/case_14229116 results/case_14237168
EOF
    exit 2
fi

PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -m src.benchmark.case_compare "$@"
