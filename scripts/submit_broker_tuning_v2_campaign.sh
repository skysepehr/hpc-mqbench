#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

MANIFEST="${1:-}"
if [[ -z "$MANIFEST" || ! -f "$MANIFEST" ]]; then
    printf 'Usage: %s <V2 campaign manifest.csv>\n' "$0" >&2
    exit 1
fi

RUN_ID="${V2_RUN_ID:-run_$(date +%Y%m%d_%H%M%S)}"
DRY_RUN="${DRY_RUN:-0}"
START_SEQUENCE="${V2_START_SEQUENCE:-1}"
END_SEQUENCE="${V2_END_SEQUENCE:-0}"
MANIFEST_PATH="$(cd "$(dirname "$MANIFEST")" && pwd)/$(basename "$MANIFEST")"
MANIFEST_STEM="$(basename "$MANIFEST" .csv)"
BATCH_DIR="${V2_BATCH_DIR:-$(dirname "$MANIFEST_PATH")/batches/$MANIFEST_STEM}"
LEDGER_DIR="${V2_LEDGER_DIR:-$PROJECT_ROOT/results/tuning_v2/$RUN_ID/submission}"
LEDGER="${V2_LEDGER_PATH:-$LEDGER_DIR/${MANIFEST_STEM}_jobs.tsv}"
LEDGER_LOCK="${LEDGER}.lock"
SUBMISSION_INTENT="${LEDGER}.submission-intent.json"

if [[ ! "$START_SEQUENCE" =~ ^[1-9][0-9]*$ ]]; then
    printf '[v2-submit] ERROR: V2_START_SEQUENCE must be a positive integer\n' >&2
    exit 1
fi
if [[ ! "$END_SEQUENCE" =~ ^[0-9]+$ ]]; then
    printf '[v2-submit] ERROR: V2_END_SEQUENCE must be zero or a positive integer\n' >&2
    exit 1
fi
if (( END_SEQUENCE > 0 && END_SEQUENCE < START_SEQUENCE )); then
    printf '[v2-submit] ERROR: V2_END_SEQUENCE cannot precede V2_START_SEQUENCE\n' >&2
    exit 1
fi

shopt -s nullglob
batches=("$BATCH_DIR"/batch_*.csv)
shopt -u nullglob
if (( ${#batches[@]} == 0 )); then
    printf '[v2-submit] ERROR: no generated batch manifests found in %s\n' "$BATCH_DIR" >&2
    printf '[v2-submit] Regenerate the V2 campaign before submission.\n' >&2
    exit 1
fi

mkdir -p "$LEDGER_DIR"
mkdir -p "$(dirname "$LEDGER")"
exec 9>>"$LEDGER_LOCK"
if ! flock -n 9; then
    printf '[v2-submit] ERROR: submission ledger is locked: %s\n' "$LEDGER" >&2
    exit 1
fi

ledger_header=$'batch_sequence\tstage\tbatch_id\tcase_count\tmanifest_path\tjob_id\tdependency'
if [[ -f "$LEDGER" ]]; then
    existing_header="$(head -n 1 "$LEDGER")"
    if [[ "$existing_header" != "$ledger_header" ]]; then
        printf '[v2-submit] ERROR: unexpected ledger header: %s\n' "$LEDGER" >&2
        exit 1
    fi
else
    printf '%s\n' "$ledger_header" > "$LEDGER"
fi

if [[ -e "$SUBMISSION_INTENT" ]]; then
    printf '[v2-submit] ERROR: unresolved submission intent: %s\n' \
        "$SUBMISSION_INTENT" >&2
    printf '[v2-submit] Reconcile the intent with Slurm before resuming.\n' >&2
    exit 1
fi

mapfile -t existing_rows < <(tail -n +2 "$LEDGER" | sed '/^[[:space:]]*$/d')
if (( ${#existing_rows[@]} > ${#batches[@]} )); then
    printf '[v2-submit] ERROR: ledger contains more rows than campaign batches\n' >&2
    exit 1
fi

previous_job=""
submitted_count="${#existing_rows[@]}"
for index in "${!existing_rows[@]}"; do
    IFS=$'\t' read -r recorded_sequence recorded_stage recorded_batch_id \
        recorded_case_count recorded_manifest recorded_job_id recorded_dependency \
        <<< "${existing_rows[$index]}"
    expected_batch="${batches[$index]}"
    expected_id="$(basename "$expected_batch" .csv)"
    expected_sequence="$((index + 1))"
    if [[ "$recorded_sequence" != "$expected_sequence" \
        || "$recorded_batch_id" != "$expected_id" \
        || "$(realpath "$recorded_manifest")" != "$(realpath "$expected_batch")" \
        || -z "$recorded_stage" \
        || -z "$recorded_job_id" ]]; then
        printf '[v2-submit] ERROR: ledger row %s does not match the campaign\n' \
            "$expected_sequence" >&2
        exit 1
    fi
    previous_job="$recorded_job_id"
done

if (( submitted_count > 0 )); then
    printf '[v2-submit] Reusing %s durable ledger row(s)\n' "$submitted_count"
fi

selected_index=0
for batch_path in "${batches[@]}"; do
    batch_sequence="${batch_path##*_}"
    batch_sequence="${batch_sequence%.csv}"
    batch_sequence="$((10#$batch_sequence))"
    if (( batch_sequence < START_SEQUENCE )); then
        continue
    fi
    if (( END_SEQUENCE > 0 && batch_sequence > END_SEQUENCE )); then
        break
    fi
    selected_index=$((selected_index + 1))
    if (( selected_index <= submitted_count )); then
        continue
    fi

    readarray -t batch_info < <(
        python3 - "$batch_path" <<'PY'
import csv
import sys
from pathlib import Path

path = Path(sys.argv[1])
rows = list(csv.DictReader(path.open("r", encoding="utf-8", newline="")))
if not rows:
    raise SystemExit(f"{path}: empty V2 batch")
stages = {str(row.get("stage", "")) for row in rows}
if len(stages) != 1:
    raise SystemExit(f"{path}: expected one V2 stage, found {sorted(stages)}")
print(next(iter(stages)))
print(len(rows))
PY
    )
    stage="${batch_info[0]}"
    case_count="${batch_info[1]}"
    batch_id="$(basename "$batch_path" .csv)"
    batch_output_dir="${PROJECT_ROOT}/results/tuning_v2/${RUN_ID}/${stage}/${batch_id}"
    if [[ -n "${V2_REPAIR_ID:-}" ]]; then
        batch_output_dir="${PROJECT_ROOT}/results/tuning_v2/${RUN_ID}/repairs/${V2_REPAIR_ID}/${stage}/${batch_id}"
    fi
    dependency=""
    if [[ -n "$previous_job" ]]; then
        dependency="afterok:$previous_job"
    fi

    printf '[v2-submit] batch=%s stage=%s cases=%s dependency=%s\n' \
        "$batch_id" "$stage" "$case_count" "${dependency:-none}"

    python3 -B - "$SUBMISSION_INTENT" "$batch_sequence" "$batch_path" \
        "$dependency" <<'PY'
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

path = Path(sys.argv[1])
payload = {
    "format": "messaging-benchmark.submission-intent.v1",
    "batch_sequence": int(sys.argv[2]),
    "manifest_path": str(Path(sys.argv[3]).resolve()),
    "dependency": sys.argv[4],
    "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "pid": os.getpid(),
}
temporary = path.with_suffix(path.suffix + ".tmp")
temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
temporary.replace(path)
PY

    output="$(
        RUN_PREFLIGHT="$(( submitted_count == 0 ? 1 : 0 ))" \
        DRY_RUN="$DRY_RUN" \
        SLURM_EXCLUSIVE=1 \
        SLURM_DEPENDENCY="$dependency" \
        SLURM_TIME="${SLURM_TIME:-02:00:00}" \
        SLURM_JOB_NAME="kbtv2-${stage:0:42}-${batch_id}" \
        SWEEP_ID="tuning_v2" \
        SWEEP_RUN_ID="$RUN_ID" \
        SWEEP_BATCH_DIR_OVERRIDE="$batch_output_dir" \
        SWEEP_MAX_CASES=30 \
        ENABLE_BROKER_PROCESS_MONITOR=1 \
        BROKER_PROCESS_MONITOR_INTERVAL_SEC=1 \
        PROMETHEUS_SCRAPE_INTERVAL_SEC=1 \
        MONITORING_RANGE_STEP_SEC=1 \
        "$PROJECT_ROOT/scripts/submit_simultaneous_budgeted_batch.sh" "$batch_path"
    )"
    printf '%s\n' "$output"

    submitted_count=$((submitted_count + 1))
    if [[ "$DRY_RUN" == "1" ]]; then
        job_id="dry-run-$submitted_count"
    else
        job_id="$(printf '%s\n' "$output" | awk '/Submitted batch job/ {print $4}' | tail -n 1)"
        if [[ -z "$job_id" ]]; then
            printf '[v2-submit] ERROR: could not parse Slurm job ID for %s\n' "$batch_id" >&2
            exit 1
        fi
    fi

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$batch_sequence" "$stage" "$batch_id" "$case_count" \
        "$batch_path" "$job_id" "$dependency" >> "$LEDGER"
    python3 -B - "$LEDGER" <<'PY'
import os
import sys

descriptor = os.open(sys.argv[1], os.O_RDONLY)
try:
    os.fsync(descriptor)
finally:
    os.close(descriptor)
PY
    rm -f "$SUBMISSION_INTENT"
    previous_job="$job_id"
done

if (( selected_index == 0 )); then
    printf '[v2-submit] ERROR: batch sequence filter selected no work\n' >&2
    exit 1
fi

printf '[v2-submit] Submission ledger: %s\n' "$LEDGER"
