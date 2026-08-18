#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Collect benchmark artifacts for one case.
#
# This script gathers the most important files produced during a benchmark run
# and places them into a clean per-case result structure.
#
# Responsibilities:
# - validate the case directory
# - collect benchmark JSON outputs
# - collect final reports
# - collect broker / monitoring / MPI logs
# - write a small artifact manifest for easier review later
#
# Required environment variables:
# - CASE_DIR
#
# Optional environment variables:
# - DATA_DIR_NAME     (default: data)
# - REPORTS_DIR_NAME  (default: reports)
# - LOGS_DIR_NAME     (default: logs)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"

CASE_DIR="$(abspath "$CASE_DIR")"
DATA_DIR_NAME="${DATA_DIR_NAME:-${RESULTS_DIR_NAME:-data}}"
REPORTS_DIR_NAME="${REPORTS_DIR_NAME:-reports}"
LOGS_DIR_NAME="${LOGS_DIR_NAME:-logs}"

# Main case-local directories.
CASE_DATA_DIR="$CASE_DIR/$DATA_DIR_NAME"
CASE_REPORTS_DIR="$CASE_DIR/$REPORTS_DIR_NAME"
CASE_LOGS_DIR="$CASE_DIR/$LOGS_DIR_NAME"
CASE_MONITORING_DIR="$CASE_DIR/monitoring"
CASE_RUNTIME_DIR="$CASE_DIR/runtime"

ensure_dir "$CASE_DATA_DIR"
ensure_dir "$CASE_REPORTS_DIR"
ensure_dir "$CASE_LOGS_DIR"

MANIFEST_FILE="$CASE_DIR/artifact_manifest.txt"

log_info "Collecting artifacts for case: $CASE_DIR"

# -----------------------------------------------------------------------------
# Helper: copy a file into a destination directory if the source exists.
# -----------------------------------------------------------------------------
copy_if_exists() {
    local source_path="${1:?source path is required}"
    local dest_dir="${2:?destination directory is required}"

    if [[ -f "$source_path" ]]; then
        cp -f "$source_path" "$dest_dir/"
        log_info "Copied: $source_path -> $dest_dir"
    else
        log_warn "Missing artifact, skipped: $source_path"
    fi
}

copy_markdown_report() {
    local source_path="${1:?source path is required}"
    local dest_dir="${2:?destination directory is required}"

    if [[ ! -f "$source_path" ]]; then
        log_warn "Missing artifact, skipped: $source_path"
        return
    fi

    local dest_path="$dest_dir/$(basename "$source_path")"
    cp -f "$source_path" "$dest_path"
    python3 - "$dest_path" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")
text = text.replace("](reports/graphs/", "](graphs/")
text = text.replace("](monitoring/graphs/", "](../monitoring/graphs/")
path.write_text(text, encoding="utf-8")
PY
    log_info "Copied: $source_path -> $dest_dir"
}

list_relative_files() {
    local root_dir="${1:?root dir is required}"
    local target_dir="${2:?target dir is required}"

    python3 - "$root_dir" "$target_dir" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1]).resolve()
target = Path(sys.argv[2]).resolve()
if not target.is_dir():
    raise SystemExit(0)

for path in sorted(item for item in target.rglob("*") if item.is_file()):
    print(path.relative_to(root).as_posix())
PY
}

# -----------------------------------------------------------------------------
# Prometheus monitoring outputs are collected before monitoring is stopped.
# The collection script is best-effort, so missing or unreachable Prometheus does
# not hide benchmark artifacts.
# -----------------------------------------------------------------------------
if [[ -n "${PROMETHEUS_ENDPOINT:-}" || -f "$CASE_DIR/runtime/monitoring/prometheus_endpoint.txt" ]]; then
    CASE_DIR="$CASE_DIR" \
    DATA_DIR_NAME="$DATA_DIR_NAME" \
    REPORTS_DIR_NAME="$REPORTS_DIR_NAME" \
        "$SCRIPT_DIR/collect_monitoring_results.sh" \
        || log_warn "Monitoring snapshot collection failed"
else
    log_info "No Prometheus endpoint found; monitoring snapshot collection skipped"
fi

# The MPI workload can finish after a backend process has crashed. Merge the
# post-workload health checkpoint after monitoring collection and before copies
# are made so every report/data copy carries the same eligibility decision.
if [[ -f "$CASE_DIR/final_report.json" ]]; then
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
        python3 -B -m src.benchmark.backend_health apply \
        --output-dir "$CASE_DIR" \
        || log_warn "Could not merge backend health into final reports"
fi

# -----------------------------------------------------------------------------
# Benchmark outputs usually written by the Python benchmark/controller.
# These are copied after monitoring collection so final_report.* can include the
# merged monitoring summary when it is available.
# -----------------------------------------------------------------------------
copy_if_exists "$CASE_DIR/case_config_snapshot.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/benchmark_result.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/final_report.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/runtime/system_inventory/system_inventory.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/runtime/benchmark_events.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/runtime/backend_snapshot.json" "$CASE_DATA_DIR"
copy_if_exists "$CASE_DIR/runtime/backend_health.json" "$CASE_DATA_DIR"
if [[ "${BACKEND_ID:-kafka}" == "kafka" ]]; then
    for optional_artifact in \
        "$CASE_DIR/runtime/broker_profile_snapshot.json" \
        "$CASE_DIR/runtime/broker_profile.sha256" \
        "$CASE_DIR/runtime/broker_runtime_manifest.json"; do
        if [[ -f "$optional_artifact" ]]; then
            copy_if_exists "$optional_artifact" "$CASE_DATA_DIR"
        fi
    done
elif [[ "${BACKEND_ID:-}" == "pulsar" ]]; then
    copy_if_exists "$CASE_DIR/runtime/pulsar/pulsar_profile_snapshot.json" "$CASE_DATA_DIR"
    copy_if_exists "$CASE_DIR/runtime/pulsar/pulsar_profile.sha256" "$CASE_DATA_DIR"
    copy_if_exists "$CASE_DIR/runtime/pulsar/pulsar_runtime_manifest.json" "$CASE_DATA_DIR"
    copy_if_exists "$CASE_DIR/runtime/pulsar/conf/standalone.conf" "$CASE_DATA_DIR"
    copy_if_exists "$CASE_DIR/runtime/pulsar/conf/standalone.conf.sha256" "$CASE_DATA_DIR"
    copy_if_exists "$CASE_DIR/runtime/pulsar/topic_metadata.json" "$CASE_DATA_DIR"
fi

# Presentation artifacts are optional. Reproducible workflows use machine mode
# and retain JSON as the complete per-case source of truth.
if [[ "${BENCHMARK_REPORT_MODE:-full}" == "machine" ]]; then
    log_info "Per-case presentation skipped because BENCHMARK_REPORT_MODE=machine"
else
    copy_markdown_report "$CASE_DIR/final_report.md" "$CASE_REPORTS_DIR"
    if [[ "${BENCHMARK_REPORT_MODE:-full}" == "light" && ! -f "$CASE_DIR/final_report.html" ]]; then
        log_info "Per-case HTML report skipped because BENCHMARK_REPORT_MODE=light"
    else
        copy_if_exists "$CASE_DIR/final_report.html" "$CASE_REPORTS_DIR"
    fi
fi

# -----------------------------------------------------------------------------
# If logs were created directly in the case directory, preserve them.
# -----------------------------------------------------------------------------
copy_if_exists "$CASE_DIR/runtime/env_snapshot.txt" "$CASE_DATA_DIR"

# -----------------------------------------------------------------------------
# Build a simple artifact manifest.
#
# This makes it easy to inspect what exists in the case directory after the run.
# -----------------------------------------------------------------------------
{
    echo "Artifact manifest"
    echo "================="
    echo "Case directory: ."
    echo "Collected at: $(date '+%Y-%m-%d %H:%M:%S')"
    echo

    echo "[data]"
    list_relative_files "$CASE_DIR" "$CASE_DATA_DIR" || true
    echo

    echo "[reports]"
    list_relative_files "$CASE_DIR" "$CASE_REPORTS_DIR" || true
    echo

    echo "[logs]"
    list_relative_files "$CASE_DIR" "$CASE_LOGS_DIR" || true
    echo

    echo "[monitoring]"
    list_relative_files "$CASE_DIR" "$CASE_MONITORING_DIR" || true
    echo

    echo "[runtime]"
    list_relative_files "$CASE_DIR" "$CASE_RUNTIME_DIR" || true
    echo

} > "$MANIFEST_FILE"

log_info "Artifact manifest written to: $MANIFEST_FILE"
log_info "Artifact collection completed"
