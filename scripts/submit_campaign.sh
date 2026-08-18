#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

BACKEND_ID="${1:-}"
MANIFEST="${2:-}"
DRY_RUN="${DRY_RUN:-0}"
RUN_ID="${BENCHMARK_CAMPAIGN_RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
START_SEQUENCE="${CAMPAIGN_START_SEQUENCE:-1}"
END_SEQUENCE="${CAMPAIGN_END_SEQUENCE:-0}"

usage() {
    printf 'Usage: %s BACKEND MANIFEST.csv\n' "$0" >&2
    exit 2
}

[[ -n "$BACKEND_ID" && -f "$MANIFEST" ]] || usage
[[ "$START_SEQUENCE" =~ ^[1-9][0-9]*$ ]] || {
    printf '[campaign-submit] ERROR: CAMPAIGN_START_SEQUENCE must be positive\n' >&2
    exit 1
}
[[ "$END_SEQUENCE" =~ ^[0-9]+$ ]] || {
    printf '[campaign-submit] ERROR: CAMPAIGN_END_SEQUENCE must be non-negative\n' >&2
    exit 1
}
if (( END_SEQUENCE > 0 && END_SEQUENCE < START_SEQUENCE )); then
    printf '[campaign-submit] ERROR: end sequence precedes start sequence\n' >&2
    exit 1
fi

mapfile -t rows < <(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -B - \
        "$BACKEND_ID" "$MANIFEST" <<'PY'
import csv
import sys
from pathlib import Path

from src.benchmark.config_loader import load_benchmark_config

backend_id, manifest_name = sys.argv[1:]
manifest = Path(manifest_name)
rows = []
seen_case_ids = set()
with manifest.open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle)
    required = {"case_id", "config_path"}
    missing = required - set(reader.fieldnames or ())
    if missing:
        raise SystemExit(
            "campaign manifest missing column(s): " + ", ".join(sorted(missing))
        )
    for file_order, row in enumerate(reader, start=1):
        case_id = str(row.get("case_id", "")).strip()
        config_path = str(row.get("config_path", "")).strip()
        if not case_id or not config_path:
            raise SystemExit(f"manifest row {file_order} has an empty case_id or config_path")
        if case_id in seen_case_ids:
            raise SystemExit(f"duplicate campaign case_id: {case_id}")
        seen_case_ids.add(case_id)
        config = load_benchmark_config(config_path)
        if config.backend_id != backend_id:
            raise SystemExit(
                f"{case_id}: config backend {config.backend_id!r} does not match "
                f"selected backend {backend_id!r}"
            )
        block = int(str(row.get("block", "1") or "1"))
        order = int(str(row.get("order", file_order) or file_order))
        stage = str(row.get("stage", "cases") or "cases").strip()
        rows.append((block, order, file_order, stage, case_id, config_path))

for block, order, _file_order, stage, case_id, config_path in sorted(rows):
    print("\t".join((str(block), str(order), stage, case_id, config_path)))
PY
)

if (( ${#rows[@]} == 0 )); then
    printf '[campaign-submit] ERROR: manifest contains no cases\n' >&2
    exit 1
fi

LEDGER_DIR="$PROJECT_ROOT/results/runs/$BACKEND_ID/$RUN_ID/submission"
LEDGER="$LEDGER_DIR/jobs.tsv"
mkdir -p "$LEDGER_DIR"
printf 'sequence\tblock\torder\tstage\tcase_id\tconfig_path\tjob_id\tdependency\n' > "$LEDGER"

previous_job=""
sequence=0
for row in "${rows[@]}"; do
    IFS=$'\t' read -r block order stage case_id config_path <<< "$row"
    sequence=$((sequence + 1))
    if (( sequence < START_SEQUENCE )); then
        continue
    fi
    if (( END_SEQUENCE > 0 && sequence > END_SEQUENCE )); then
        break
    fi

    dependency=""
    if [[ -n "$previous_job" ]]; then
        dependency="afterok:$previous_job"
    fi
    case_dir="$PROJECT_ROOT/results/runs/$BACKEND_ID/$RUN_ID/$stage/$case_id"

    if [[ "$DRY_RUN" == "1" ]]; then
        printf '[campaign-submit] %03d block=%s order=%s case=%s dependency=%s\n' \
            "$sequence" "$block" "$order" "$case_id" "${dependency:-none}"
        job_id="dry-run-$sequence"
    else
        output="$(
            RUN_PREFLIGHT="$(( sequence == START_SEQUENCE ? 1 : 0 ))" \
            SLURM_EXCLUSIVE=1 \
            SLURM_DEPENDENCY="$dependency" \
            SLURM_JOB_NAME="${BACKEND_ID}-${case_id:0:55}" \
            CASE_ID_OVERRIDE="$case_id" \
            CASE_NAME_OVERRIDE="$case_id" \
            CASE_DIR_OVERRIDE="$case_dir" \
            "$PROJECT_ROOT/scripts/submit_hpc_case.sh" "$config_path"
        )"
        printf '%s\n' "$output"
        job_id="$(printf '%s\n' "$output" | awk '/Submitted batch job/ {print $4}' | tail -n 1)"
        if [[ -z "$job_id" ]]; then
            printf '[campaign-submit] ERROR: no Slurm job ID for %s\n' "$case_id" >&2
            exit 1
        fi
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$sequence" "$block" "$order" "$stage" "$case_id" \
        "$config_path" "$job_id" "$dependency" >> "$LEDGER"
    previous_job="$job_id"
done

printf '[campaign-submit] Backend: %s\n' "$BACKEND_ID"
printf '[campaign-submit] Run ID: %s\n' "$RUN_ID"
printf '[campaign-submit] Submission ledger: %s\n' "$LEDGER"
