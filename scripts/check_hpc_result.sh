#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

usage() {
    cat <<'EOF'
Usage:
  ./scripts/check_hpc_result.sh <result-dir> [slurm-log]

Examples:
  ./scripts/check_hpc_result.sh results/case_13963780
  ./scripts/check_hpc_result.sh results/campaign_13963846 logs/slurm-13963846.out

Checks a single-case or campaign result directory for the expected reports,
monitoring artifacts, Kafka broker throughput metrics, and obvious Slurm-log
errors. Slurm service cleanup steps may appear as FAILED 143 or CANCELLED after
the batch job completes; this script treats only explicit error markers in the
captured output as failures.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -lt 1 || $# -gt 2 ]]; then
    usage
    exit 0
fi

RESULT_DIR="$1"
SLURM_LOG="${2:-}"

python3 - "$RESULT_DIR" "$SLURM_LOG" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

result_dir = Path(sys.argv[1])
slurm_log_arg = sys.argv[2]
project_root = Path.cwd()

errors: list[str] = []
warnings: list[str] = []
passes: list[str] = []


def print_line(kind: str, message: str) -> None:
    print(f"[{kind}] {message}")


def load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        errors.append(f"missing JSON file: {path}")
        return {}
    except json.JSONDecodeError as exc:
        errors.append(f"invalid JSON file: {path}: {exc}")
        return {}
    return data if isinstance(data, dict) else {}


def relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path)


def check_file(path: Path, label: str, *, warn_only: bool = False) -> None:
    if path.is_file():
        passes.append(f"{label}: {relative(path)}")
    elif warn_only:
        warnings.append(f"missing {label}: {relative(path)}")
    else:
        errors.append(f"missing {label}: {relative(path)}")


def check_dir(path: Path, label: str, *, warn_only: bool = False) -> None:
    if path.is_dir():
        passes.append(f"{label}: {relative(path)}")
    elif warn_only:
        warnings.append(f"missing {label}: {relative(path)}")
    else:
        errors.append(f"missing {label}: {relative(path)}")


def broker_metrics_present(report: dict[str, Any]) -> bool:
    broker = report.get("kafka_broker_throughput", {})
    if not isinstance(broker, dict):
        return False
    metrics = broker.get("metrics", [])
    return bool(broker.get("enabled")) and isinstance(metrics, list) and bool(metrics)


def check_html_report(case_dir: Path, label: str) -> None:
    html_path = case_dir / "reports" / "final_report.html"
    if not html_path.is_file():
        return
    text = html_path.read_text(encoding="utf-8", errors="replace")
    required_labels = [
        "Run Verdict",
        "Sustained Throughput Verdict",
        "Client Runtime Diagnostics",
        "Timeline",
        "Metric Sources",
        "System Saturation During Kafka",
        "System Saturation Verdict",
        "System Capability",
        "Runtime Monitoring",
        "Single-process RAM read/write GB/s",
    ]
    for required_label in required_labels:
        if required_label in text:
            passes.append(f"{label} HTML contains {required_label}")
        else:
            warnings.append(f"{label} HTML missing {required_label}")
    graph_dir = case_dir / "monitoring" / "graphs"
    graph_count = len(list(graph_dir.glob("*.svg"))) if graph_dir.is_dir() else 0
    embedded_graph_count = text.count("<article class=\"graph-card")
    embedded_svg_count = text.count("<svg")
    if graph_count > 0:
        if embedded_graph_count <= 0 or embedded_svg_count <= 0:
            errors.append(
                f"{label} HTML has {graph_count} monitoring SVG(s), but no embedded graph gallery"
            )
        elif embedded_graph_count < graph_count:
            warnings.append(
                f"{label} HTML embeds {embedded_graph_count}/{graph_count} monitoring graph card(s)"
            )
        else:
            passes.append(f"{label} HTML embeds {embedded_graph_count} monitoring graph card(s)")


def check_case(case_dir: Path, case_id: str = "") -> None:
    label = case_id or case_dir.name
    check_dir(case_dir / "data", f"{label} data dir", warn_only=True)
    check_dir(case_dir / "logs", f"{label} logs dir", warn_only=True)
    check_dir(case_dir / "runtime", f"{label} runtime dir", warn_only=True)
    check_dir(case_dir / "reports", f"{label} reports dir")
    check_file(case_dir / "final_report.json", f"{label} final_report.json")
    check_file(case_dir / "reports" / "final_report.md", f"{label} report copy")
    check_file(case_dir / "reports" / "final_report.html", f"{label} HTML report")
    check_file(case_dir / "data" / "system_inventory.json", f"{label} system inventory data", warn_only=True)
    check_file(case_dir / "data" / "benchmark_events.json", f"{label} benchmark event data", warn_only=True)
    check_html_report(case_dir, label)

    report = load_json(case_dir / "final_report.json")
    status = report.get("case", {}).get("status")
    if status == "completed":
        passes.append(f"{label} status completed")
    else:
        errors.append(f"{label} status is {status!r}, expected 'completed'")

    if broker_metrics_present(report):
        passes.append(f"{label} Kafka broker throughput present")
    else:
        warnings.append(f"{label} Kafka broker throughput not populated")

    monitoring = report.get("monitoring", {})
    if isinstance(monitoring, dict) and monitoring.get("enabled"):
        passes.append(f"{label} monitoring object enabled")
    else:
        warnings.append(f"{label} monitoring not enabled in final_report.json")

    inventory = report.get("system_inventory", {})
    if isinstance(inventory, dict) and inventory.get("enabled"):
        passes.append(f"{label} system inventory object enabled")
        inventory_status = inventory.get("status")
        probe_summary = inventory.get("probe_summary", {})
        if inventory_status == "completed":
            passes.append(f"{label} system inventory completed")
        else:
            warnings.append(f"{label} system inventory status is {inventory_status!r}")
        if isinstance(probe_summary, dict):
            failed_required = int(probe_summary.get("failed_required_probe_count", 0) or 0)
            required_complete = bool(probe_summary.get("required_network_tests_completed", False))
            if failed_required:
                errors.append(f"{label} system inventory has {failed_required} failed or missing required probe(s)")
            elif required_complete:
                passes.append(f"{label} required iperf3 paths completed")
            else:
                warnings.append(f"{label} required iperf3 path status was not recorded")
    else:
        warnings.append(f"{label} system inventory not enabled in final_report.json")


if not result_dir.exists():
    errors.append(f"result directory does not exist: {result_dir}")
else:
    campaign_summary_path = result_dir / "campaign_summary.json"
    iteration_summary_path = result_dir / "iteration_summary.json"
    if iteration_summary_path.is_file():
        check_file(iteration_summary_path, "iteration_summary.json")
        check_file(result_dir / "iteration.tsv", "iteration.tsv", warn_only=True)
        check_file(result_dir / "cases.tsv", "cases.tsv", warn_only=True)
        summary = load_json(iteration_summary_path)
        case_count = int(summary.get("case_count", 0) or 0)
        completed_count = int(summary.get("completed_count", 0) or 0)
        failed_count = int(summary.get("failed_count", 0) or 0)
        if case_count > 0 and completed_count == case_count and failed_count == 0:
            passes.append(f"iteration completed {completed_count}/{case_count} cases")
        else:
            errors.append(
                "iteration completion mismatch: "
                f"case_count={case_count}, completed={completed_count}, "
                f"failed={failed_count}"
            )

        cases = summary.get("cases", [])
        if isinstance(cases, list):
            for case in cases:
                if not isinstance(case, dict):
                    continue
                case_path = result_dir / str(case.get("case_dir", ""))
                check_case(case_path, str(case.get("case_id", case_path.name)))
        else:
            errors.append("iteration_summary.json has no cases list")
    elif campaign_summary_path.is_file():
        check_file(campaign_summary_path, "campaign_summary.json")
        check_file(
            result_dir / "reports" / "campaign_summary.md",
            "campaign_summary.md",
        )
        check_file(
            result_dir / "reports" / "campaign_report.html",
            "campaign_report.html",
            warn_only=True,
        )
        summary = load_json(campaign_summary_path)
        case_count = int(summary.get("case_count", 0) or 0)
        completed_count = int(summary.get("completed_count", 0) or 0)
        failed_count = int(summary.get("failed_count", 0) or 0)
        if case_count > 0 and completed_count == case_count and failed_count == 0:
            passes.append(f"campaign completed {completed_count}/{case_count} cases")
        else:
            errors.append(
                "campaign completion mismatch: "
                f"case_count={case_count}, completed={completed_count}, "
                f"failed={failed_count}"
            )

        cases = summary.get("cases", [])
        if isinstance(cases, list):
            for case in cases:
                if not isinstance(case, dict):
                    continue
                case_path = result_dir / str(case.get("case_dir", ""))
                check_case(case_path, str(case.get("case_id", case_path.name)))
        else:
            errors.append("campaign_summary.json has no cases list")
    else:
        check_case(result_dir)


def guessed_slurm_log() -> Path | None:
    match = re.search(r"(\d+)$", result_dir.name)
    if not match:
        return None
    candidate = project_root / "logs" / f"slurm-{match.group(1)}.out"
    return candidate if candidate.is_file() else None


slurm_log = Path(slurm_log_arg) if slurm_log_arg else guessed_slurm_log()
if slurm_log is not None:
    check_file(slurm_log, "Slurm output log", warn_only=True)
    if slurm_log.is_file():
        text = slurm_log.read_text(encoding="utf-8", errors="replace")
        lowered = text.lower()
        explicit_errors = [
            line
            for line in text.splitlines()
            if "[error]" in line.lower()
            or "traceback" in line.lower()
            or "exception" in line.lower()
        ]
        if explicit_errors:
            errors.append(
                f"Slurm log contains {len(explicit_errors)} explicit error line(s)"
            )
        else:
            passes.append("Slurm log has no explicit [ERROR]/traceback/exception lines")
        if "campaign completed" in lowered or "case cleanup completed" in lowered:
            passes.append("Slurm log contains completion marker")
        else:
            warnings.append("Slurm log completion marker was not found")
else:
    warnings.append("no Slurm log supplied or guessed")

for message in passes:
    print_line("PASS", message)
for message in warnings:
    print_line("WARN", message)
for message in errors:
    print_line("FAIL", message)

print("")
print(f"Summary: {len(passes)} pass(es), {len(warnings)} warning(s), {len(errors)} failure(s)")
if errors:
    raise SystemExit(1)
PY
