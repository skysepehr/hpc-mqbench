from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
from typing import Any


BACKEND_HEALTH_FORMAT = "messaging-benchmark.backend-health.v1"


def load_backend_health(output_dir: str | Path) -> dict[str, Any] | None:
    path = Path(output_dir) / "runtime" / "backend_health.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "format": BACKEND_HEALTH_FORMAT,
            "status": "invalid",
            "failure_reasons": [f"Could not parse backend health artifact: {path}"],
            "checks": [],
        }
    if not isinstance(payload, dict):
        return {
            "format": BACKEND_HEALTH_FORMAT,
            "status": "invalid",
            "failure_reasons": ["Backend health artifact is not a JSON object"],
            "checks": [],
        }
    return payload


def merge_backend_health_into_result(
    benchmark_result: dict[str, Any],
    health: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach runtime health and fail eligibility closed when it is unhealthy."""
    if health is None:
        return benchmark_result

    benchmark_result["backend_health"] = deepcopy(health)
    status = str(health.get("status", "invalid")).strip().lower()
    if status == "healthy":
        return benchmark_result

    case = benchmark_result.setdefault("case", {})
    if isinstance(case, dict):
        case.setdefault("workload_status", case.get("status", "unknown"))
        case["status"] = "failed"

    failures = _health_failures(health)
    eligibility = benchmark_result.get("eligibility")
    if not isinstance(eligibility, dict):
        eligibility = {}
        benchmark_result["eligibility"] = eligibility
    eligibility["eligible"] = False
    eligibility["backend_health_status"] = status or "invalid"
    eligibility["failure_reasons"] = _deduplicate(
        [
            *(
                str(item)
                for item in eligibility.get("failure_reasons", [])
                if str(item).strip()
            ),
            *failures,
        ]
    )
    benchmark_result["eligible"] = False
    return benchmark_result


def apply_backend_health_to_report_files(output_dir: str | Path) -> bool:
    output_path = Path(output_dir)
    health = load_backend_health(output_path)
    report_path = output_path / "final_report.json"
    if health is None or not report_path.is_file():
        return False

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Final report is not a JSON object: {report_path}")
    merge_backend_health_into_result(payload, health)

    # Import lazily: ReportBuilder itself uses merge_backend_health_into_result.
    from src.benchmark.report_builder import ReportBuilder

    ReportBuilder(output_path).build(payload)
    return True


def _health_failures(health: dict[str, Any]) -> list[str]:
    failures = [
        str(item).strip()
        for item in health.get("failure_reasons", [])
        if str(item).strip()
    ]
    checks = health.get("checks", [])
    if isinstance(checks, list):
        for check in checks:
            if not isinstance(check, dict):
                continue
            if str(check.get("status", "")).lower() == "pass":
                continue
            name = str(check.get("name", "backend health check"))
            detail = str(check.get("detail", "failed")).strip()
            failures.append(f"{name}: {detail}")
    return _deduplicate(failures or ["Backend post-workload health check failed"])


def _deduplicate(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge post-workload backend health into case reports"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("--output-dir", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "apply":
        apply_backend_health_to_report_files(args.output_dir)
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
