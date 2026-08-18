#!/usr/bin/env bash
set -euo pipefail

# Slurm entrypoint for a benchmark iteration that must keep all scenarios on
# the same allocated nodes.

if [[ -n "${PROJECT_ROOT:-}" && -f "$PROJECT_ROOT/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"
elif [[ -n "${SLURM_SUBMIT_DIR:-}" && -f "$SLURM_SUBMIT_DIR/scripts/common.sh" ]]; then
    PROJECT_ROOT="$(cd "$SLURM_SUBMIT_DIR" && pwd)"
else
    PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fi
cd "$PROJECT_ROOT"

ITERATION_NAME="${1:-ordinary_no_compression}"
shift || true

if [[ "$#" -gt 0 ]]; then
    configs=("$@")
else
    configs=(
        "configs/one_broker_mpi_ingress_only.json"
        "configs/one_broker_mpi_egress_only.json"
        "configs/one_broker_mpi_simultaneous.json"
    )
fi

sanitize_token() {
    local value="${1:?value required}"
    value="$(printf '%s' "$value" | tr -c 'A-Za-z0-9_.-' '_')"
    value="${value##_}"
    value="${value%%_}"
    printf '%s\n' "${value:0:80}"
}

job_token="${SLURM_JOB_ID:-manual_$(date +%Y%m%d_%H%M%S)}"
iteration_token="$(sanitize_token "$ITERATION_NAME")"
ITERATION_ID="iteration_${job_token}_${iteration_token}"
ITERATION_DIR="${ITERATION_DIR_OVERRIDE:-$PROJECT_ROOT/results/$ITERATION_ID}"
mkdir -p "$ITERATION_DIR"

node_list_file="$ITERATION_DIR/nodes.txt"
case_status_file="$ITERATION_DIR/cases.tsv"
case_summary_file="$ITERATION_DIR/iteration_summary.json"

if [[ -n "${SLURM_JOB_NODELIST:-}" ]]; then
    scontrol show hostnames "$SLURM_JOB_NODELIST" > "$node_list_file"
else
    hostname > "$node_list_file"
fi

{
    printf 'iteration_id\t%s\n' "$ITERATION_ID"
    printf 'slurm_job_id\t%s\n' "${SLURM_JOB_ID:-}"
    printf 'iteration_name\t%s\n' "$ITERATION_NAME"
    printf 'started_unix\t%s\n' "$(date +%s)"
    printf 'nodes\t%s\n' "$(paste -sd, "$node_list_file")"
} > "$ITERATION_DIR/iteration.tsv"

printf 'index\tstatus\tcase_id\tcase_name\tconfig_path\tcase_dir\n' > "$case_status_file"

printf '[run-iteration] Iteration: %s\n' "$ITERATION_ID"
printf '[run-iteration] Node list: %s\n' "$(paste -sd, "$node_list_file")"
printf '[run-iteration] Order: ingress-only -> egress-only -> simultaneous\n'
printf '[run-iteration] Cases will be stored under: %s\n' "$ITERATION_DIR"

write_iteration_summary() {
    python3 - "$ITERATION_DIR" "$case_status_file" "$case_summary_file" <<'PY'
import csv
import json
import sys
import time
from pathlib import Path

iteration_dir = Path(sys.argv[1])
case_status_file = Path(sys.argv[2])
case_summary_file = Path(sys.argv[3])

metadata = {}
iteration_tsv = iteration_dir / "iteration.tsv"
if iteration_tsv.is_file():
    for line in iteration_tsv.read_text(encoding="utf-8").splitlines():
        if "\t" in line:
            key, value = line.split("\t", 1)
            metadata[key] = value

cases = []
with case_status_file.open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle, delimiter="\t")
    latest_by_case = {}
    for row in reader:
        latest_by_case[row.get("case_id", "")] = row
    for row in latest_by_case.values():
        case_dir = row.get("case_dir") or row.get("case_id", "")
        cases.append(
            {
                "index": int(row.get("index") or 0),
                "status": row.get("status", ""),
                "case_id": row.get("case_id", ""),
                "case_name": row.get("case_name", ""),
                "config_path": row.get("config_path", ""),
                "case_dir": case_dir,
            }
        )

cases.sort(key=lambda item: item["index"])
case_summary_file.write_text(
    json.dumps(
        {
            "format": "kafka_simple_iteration.v1",
            "iteration_id": metadata.get("iteration_id", iteration_dir.name),
            "iteration_name": metadata.get("iteration_name", ""),
            "slurm_job_id": metadata.get("slurm_job_id", ""),
            "iteration_dir": iteration_dir.name,
            "case_count": len(cases),
            "completed_count": sum(1 for case in cases if case["status"] == "completed"),
            "failed_count": sum(1 for case in cases if case["status"].startswith("failed")),
            "nodes": metadata.get("nodes", ""),
            "started_unix": metadata.get("started_unix", ""),
            "finished_unix": metadata.get("finished_unix", str(int(time.time()))),
            "cases": cases,
        },
        indent=2,
        sort_keys=True,
    ),
    encoding="utf-8",
)
PY
}

index=1
for config_path in "${configs[@]}"; do
    if [[ ! -f "$config_path" ]]; then
        printf '[run-iteration] ERROR: missing config: %s\n' "$config_path" >&2
        exit 1
    fi

    base_name="$(basename "$config_path" .json)"
    scenario_token="${base_name#one_broker_mpi_}"
    scenario_token="$(sanitize_token "$scenario_token")"
    case_id="$(printf 'case_%s_%02d_%s' "$job_token" "$index" "$scenario_token")"
    case_name="$base_name"
    case_dir="$ITERATION_DIR/$case_id"

    printf '\n[run-iteration] Starting %s/%s: %s -> %s\n' \
        "$index" "${#configs[@]}" "$config_path" "$case_id"
    printf '%s\trunning\t%s\t%s\t%s\t%s\n' \
        "$index" "$case_id" "$case_name" "$config_path" "$case_id" >> "$case_status_file"

    if CASE_ID_OVERRIDE="$case_id" \
        CASE_NAME_OVERRIDE="$case_name" \
        CASE_DIR_OVERRIDE="$case_dir" \
        ITERATION_ID="$ITERATION_ID" \
        ITERATION_NAME="$ITERATION_NAME" \
        "$PROJECT_ROOT/run_all.sh" "$config_path"; then
        printf '[run-iteration] Completed %s\n' "$case_id"
        printf '%s\tcompleted\t%s\t%s\t%s\t%s\n' \
            "$index" "$case_id" "$case_name" "$config_path" "$case_id" >> "$case_status_file"
    else
        status=$?
        printf '[run-iteration] FAILED %s with exit code %s\n' "$case_id" "$status" >&2
        printf '%s\tfailed:%s\t%s\t%s\t%s\t%s\n' \
            "$index" "$status" "$case_id" "$case_name" "$config_path" "$case_id" >> "$case_status_file"
        printf 'finished_unix\t%s\n' "$(date +%s)" >> "$ITERATION_DIR/iteration.tsv"
        write_iteration_summary
        exit "$status"
    fi

    index=$(( index + 1 ))
done

printf 'finished_unix\t%s\n' "$(date +%s)" >> "$ITERATION_DIR/iteration.tsv"
write_iteration_summary
printf '\n[run-iteration] All scenarios completed on the same node allocation.\n'
printf '[run-iteration] Iteration artifacts: %s\n' "$ITERATION_DIR"
