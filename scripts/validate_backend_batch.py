#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from plan_backend_batch import build_batch_plan


CASE_FIELDS = (
    "sequence",
    "config_id",
    "case_id",
    "status",
    "backend_health",
    "profile_id",
    "profile_sha256",
    "eligible",
    "qualified",
    "balanced_mib_per_sec",
    "balanced_records_per_sec",
    "latency_valid",
    "latency_p99_us",
    "records_delivered",
    "records_consumed",
    "records_missing_after_drain",
    "invalid_envelopes",
    "duplicate_records",
    "out_of_order_records",
    "failed_sends",
    "max_flush_duration_sec",
    "scientifically_valid",
    "validation_failures",
    "source_report",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate one completed multi-case backend batch and write a compact "
            "case summary. Qualification failures are reported but do not make "
            "the batch mechanism incomplete."
        )
    )
    parser.add_argument("backend_id")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("results_root", type=Path)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--allow-profile-changes", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = args.project_root.resolve()
    results_root = args.results_root.resolve()
    if not results_root.is_dir():
        raise ValueError(f"Results root does not exist: {results_root}")
    plan = build_batch_plan(
        args.backend_id,
        args.manifest,
        project_root,
        require_one_profile=not args.allow_profile_changes,
    )
    reports, duplicate_case_ids = _discover_reports(results_root, args.backend_id)
    summaries = _discover_batch_summaries(results_root)
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else results_root / "acceptance"
    )

    mechanical_failures: list[str] = []
    if duplicate_case_ids:
        mechanical_failures.append(
            "duplicate top-level final reports: " + ", ".join(duplicate_case_ids)
        )
    expected_case_ids = {row["case_id"] for row in plan["rows"]}
    observed_required = expected_case_ids & set(reports)
    missing_case_ids = sorted(expected_case_ids - set(reports))
    if missing_case_ids:
        mechanical_failures.append(
            "missing required final report(s): " + ", ".join(missing_case_ids)
        )

    rows: list[dict[str, Any]] = []
    for sequence, planned in enumerate(plan["rows"], start=1):
        selected = reports.get(planned["case_id"])
        if selected is None:
            continue
        path, report = selected
        row = _case_row(
            sequence=sequence,
            planned=planned,
            report=report,
            report_path=path,
            results_root=results_root,
            expected_backend=args.backend_id,
            expected_profile_id=planned["profile_id"],
            expected_profile_sha256=planned["profile_sha256"],
        )
        rows.append(row)
        for reason in row["mechanical_failures"]:
            mechanical_failures.append(f"{planned['case_id']}: {reason}")

    summary_validation = _validate_batch_summary(
        summaries=summaries,
        expected_case_ids=expected_case_ids,
    )
    mechanical_failures.extend(summary_validation["failures"])
    mechanical_failures = list(dict.fromkeys(mechanical_failures))
    extra_case_ids = sorted(set(reports) - expected_case_ids)
    batch_complete = not mechanical_failures
    payload = {
        "format": "messaging-benchmark.backend-batch-acceptance.v1",
        "backend_id": args.backend_id,
        "batch_complete": batch_complete,
        "expected_case_count": int(plan["case_count"]),
        "observed_required_case_count": len(observed_required),
        "completed_case_count": sum(row["status"] == "completed" for row in rows),
        "scientifically_valid_case_count": sum(
            bool(row["scientifically_valid"]) for row in rows
        ),
        "eligible_case_count": sum(bool(row["eligible"]) for row in rows),
        "qualified_case_count": sum(bool(row["qualified"]) for row in rows),
        "latency_valid_case_count": sum(bool(row["latency_valid"]) for row in rows),
        "missing_case_ids": missing_case_ids,
        "extra_case_ids": extra_case_ids,
        "duplicate_case_ids": duplicate_case_ids,
        "profile_id": plan["profile_id"],
        "profile_sha256": plan["profile_sha256"],
        "profile_count": plan["profile_count"],
        "profiles": plan["profiles"],
        "batch_summary": summary_validation,
        "mechanical_failure_reasons": mechanical_failures,
        "cases": rows,
        "interpretation": (
            "batch_complete validates one-allocation execution, report completeness, "
            "backend health, immutable profile identity, and required metric fields. "
            "Eligibility and qualification remain per-workload scientific outcomes; "
            "an overdriven but correctly reported case does not by itself invalidate "
            "the batch mechanism."
        ),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "acceptance_report.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output_dir / "case_summary.csv", rows)
    (output_dir / "acceptance_report.md").write_text(
        _markdown(payload),
        encoding="utf-8",
    )
    print(f"[batch-validate] Batch complete: {str(batch_complete).lower()}")
    print(
        "[batch-validate] Reports: "
        f"{len(observed_required)}/{plan['case_count']}; "
        f"eligible={payload['eligible_case_count']}; "
        f"qualified={payload['qualified_case_count']}"
    )
    print(f"[batch-validate] Output: {output_dir}")
    return 0 if batch_complete else 1


def _discover_reports(
    results_root: Path,
    backend_id: str,
) -> tuple[dict[str, tuple[Path, dict[str, Any]]], list[str]]:
    reports: dict[str, tuple[Path, dict[str, Any]]] = {}
    duplicates: set[str] = set()
    for path in sorted(results_root.rglob("final_report.json")):
        if path.parent.name in {"data", "reports"}:
            continue
        report = _read_json(path)
        case = _dict(report.get("case"))
        config = _dict(report.get("config"))
        system = _dict(report.get("system_under_test"))
        observed_backend = str(
            system.get("backend_id") or config.get("backend_id") or ""
        )
        if observed_backend != backend_id:
            continue
        case_id = str(case.get("case_id", "")).strip()
        if not case_id:
            continue
        if case_id in reports:
            duplicates.add(case_id)
            continue
        reports[case_id] = (path, report)
    return reports, sorted(duplicates)


def _discover_batch_summaries(results_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    return [
        (path, _read_json(path))
        for path in sorted(results_root.rglob("batch_summary.json"))
    ]


def _case_row(
    *,
    sequence: int,
    planned: dict[str, Any],
    report: dict[str, Any],
    report_path: Path,
    results_root: Path,
    expected_backend: str,
    expected_profile_id: str,
    expected_profile_sha256: str,
) -> dict[str, Any]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    system = _dict(report.get("system_under_test"))
    health = _dict(report.get("backend_health"))
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    records = _dict(common.get("records"))
    latency = _dict(common.get("latency_end_to_end"))
    qualification = _dict(common.get("qualification"))
    latency_validation = _dict(report.get("latency_validation"))
    aggregated = _dict(report.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    correctness = _dict(aggregated.get("record_correctness"))

    profile_id = str(system.get("profile_id", ""))
    profile_sha256 = str(system.get("profile_sha256", ""))
    producer_mib = _number_or_none(throughput.get("producer_mib_per_sec"))
    consumer_mib = _number_or_none(throughput.get("consumer_mib_per_sec"))
    balanced_mib = _number_or_none(throughput.get("balanced_mib_per_sec"))
    producer_records = _number_or_none(throughput.get("producer_records_per_sec"))
    consumer_records = _number_or_none(throughput.get("consumer_records_per_sec"))
    balanced_records = _number_or_none(throughput.get("balanced_records_per_sec"))
    latency_enabled = bool(config.get("latency_enabled", False))
    latency_valid = (
        latency.get("valid") is True
        and (not latency_enabled or latency_validation.get("valid") is True)
    )

    delivered = _integer(producers.get("messages_delivered"))
    consumed = _integer(records.get("consumed"))
    missing = _integer(
        records.get(
            "missing",
            correctness.get("missing_after_drain_records", -1),
        )
    )
    invalid = _integer(correctness.get("invalid_envelope_count", -1))
    duplicate = _integer(correctness.get("duplicate_offset_count", -1))
    out_of_order = _integer(correctness.get("out_of_order_offset_count", -1))
    failed = _integer(producers.get("messages_failed"))
    flush_sec = _number_or_none(producers.get("max_flush_duration_sec"))

    failures: list[str] = []
    if case.get("case_id") != planned["case_id"]:
        failures.append(f"report case ID is {case.get('case_id')!r}")
    if case.get("status") != "completed":
        failures.append(f"case status is {case.get('status', 'unknown')}")
    if health.get("status") != "healthy":
        failures.append(f"backend health is {health.get('status', 'not_recorded')}")
    if system.get("backend_id") != expected_backend:
        failures.append(f"system backend is {system.get('backend_id')!r}")
    if profile_id != expected_profile_id:
        failures.append(f"profile ID is {profile_id!r}")
    if profile_sha256 != expected_profile_sha256:
        failures.append("immutable profile checksum does not match the batch plan")
    metric_values = {
        "producer_mib_per_sec": producer_mib,
        "consumer_mib_per_sec": consumer_mib,
        "balanced_mib_per_sec": balanced_mib,
        "producer_records_per_sec": producer_records,
        "consumer_records_per_sec": consumer_records,
        "balanced_records_per_sec": balanced_records,
    }
    for name, value in metric_values.items():
        if value is None or value < 0:
            failures.append(f"required throughput metric {name} is invalid")
    if all(value is not None for value in (producer_mib, consumer_mib, balanced_mib)):
        if not math.isclose(
            float(balanced_mib),
            min(float(producer_mib), float(consumer_mib)),
            rel_tol=1e-9,
            abs_tol=1e-9,
        ):
            failures.append("balanced MiB/s is not min(producer, consumer)")
    if all(
        value is not None
        for value in (producer_records, consumer_records, balanced_records)
    ):
        if not math.isclose(
            float(balanced_records),
            min(float(producer_records), float(consumer_records)),
            rel_tol=1e-9,
            abs_tol=1e-6,
        ):
            failures.append("balanced records/s is not min(producer, consumer)")
    if min(delivered, consumed, missing, invalid, duplicate, out_of_order, failed) < 0:
        failures.append("record-accounting fields are missing or negative")
    if flush_sec is None or flush_sec < 0:
        failures.append("producer flush duration is missing or negative")
    if latency_enabled and not latency_valid:
        failures.append("enabled end-to-end latency validation is invalid")

    correctness_valid = (
        missing == 0
        and invalid == 0
        and duplicate == 0
        and out_of_order == 0
    )
    scientifically_valid = (
        qualification.get("eligible") is True
        and correctness_valid
        and (not latency_enabled or latency_valid)
    )
    return {
        "sequence": sequence,
        "config_id": planned["config_id"],
        "case_id": planned["case_id"],
        "status": str(case.get("status", "unknown")),
        "backend_health": str(health.get("status", "not_recorded")),
        "profile_id": profile_id,
        "profile_sha256": profile_sha256,
        "eligible": qualification.get("eligible") is True,
        "qualified": qualification.get("qualified") is True,
        "producer_mib_per_sec": producer_mib,
        "consumer_mib_per_sec": consumer_mib,
        "balanced_mib_per_sec": balanced_mib,
        "producer_records_per_sec": producer_records,
        "consumer_records_per_sec": consumer_records,
        "balanced_records_per_sec": balanced_records,
        "latency_valid": latency_valid,
        "latency_p99_us": _number_or_none(latency.get("p99_us")),
        "records_delivered": delivered,
        "records_consumed": consumed,
        "records_missing_after_drain": missing,
        "invalid_envelopes": invalid,
        "duplicate_records": duplicate,
        "out_of_order_records": out_of_order,
        "failed_sends": failed,
        "max_flush_duration_sec": flush_sec,
        "scientifically_valid": scientifically_valid,
        "mechanical_failures": failures,
        "validation_failures": "; ".join(failures),
        "source_report": _relative(report_path, results_root),
    }


def _validate_batch_summary(
    *,
    summaries: list[tuple[Path, dict[str, Any]]],
    expected_case_ids: set[str],
) -> dict[str, Any]:
    failures: list[str] = []
    if len(summaries) != 1:
        failures.append(
            f"expected one batch_summary.json, found {len(summaries)}"
        )
        return {
            "valid": False,
            "summary_count": len(summaries),
            "failures": failures,
        }
    path, summary = summaries[0]
    rows = summary.get("cases") if isinstance(summary.get("cases"), list) else []
    latest = {
        str(row.get("case_id", "")): row
        for row in rows
        if isinstance(row, dict) and row.get("case_id")
    }
    if set(latest) != expected_case_ids:
        failures.append("batch summary case IDs do not match the manifest")
    if int(summary.get("case_count", -1)) != len(expected_case_ids):
        failures.append("batch summary case_count does not match the manifest")
    if int(summary.get("completed_count", -1)) != len(expected_case_ids):
        failures.append("batch summary does not mark every case completed")
    if int(summary.get("failed_count", -1)) != 0:
        failures.append("batch summary contains failed cases")
    if int(summary.get("skipped_count", -1)) != 0:
        failures.append("batch summary contains skipped cases")
    return {
        "valid": not failures,
        "summary_count": 1,
        "source": path.name,
        "case_count": summary.get("case_count"),
        "completed_count": summary.get("completed_count"),
        "failed_count": summary.get("failed_count"),
        "skipped_count": summary.get("skipped_count"),
        "failures": failures,
    }


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CASE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Backend Batch Acceptance",
        "",
        f"- Backend: `{report['backend_id']}`",
        f"- Batch complete: **{str(report['batch_complete']).lower()}**",
        (
            "- Required reports: "
            f"{report['observed_required_case_count']}/{report['expected_case_count']}"
        ),
        f"- Scientifically valid cases: {report['scientifically_valid_case_count']}",
        f"- Eligible cases: {report['eligible_case_count']}",
        f"- Qualified cases: {report['qualified_case_count']}",
        f"- Latency-valid cases: {report['latency_valid_case_count']}",
        f"- Profile: `{report['profile_id']}`",
        "",
        report["interpretation"],
        "",
    ]
    failures = report["mechanical_failure_reasons"]
    if failures:
        lines.extend(["## Mechanical Failures", ""])
        lines.extend(f"- {reason}" for reason in failures)
        lines.append("")
    lines.extend(
        [
            "## Cases",
            "",
            "| # | Config | Complete | Eligible | Qualified | Balanced MiB/s | p99 us | Missing |",
            "|---:|---|---|---|---|---:|---:|---:|",
        ]
    )
    for row in report["cases"]:
        lines.append(
            f"| {row['sequence']} | `{row['config_id']}` | "
            f"{_yes(row['status'] == 'completed')} | {_yes(row['eligible'])} | "
            f"{_yes(row['qualified'])} | {_format_number(row['balanced_mib_per_sec'], 3)} | "
            f"{_format_number(row['latency_p99_us'], 1)} | "
            f"{row['records_missing_after_drain']} |"
        )
    return "\n".join(lines) + "\n"


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _integer(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _number_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _yes(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _format_number(value: Any, places: int) -> str:
    number = _number_or_none(value)
    return "n/a" if number is None else f"{number:.{places}f}"


if __name__ == "__main__":
    raise SystemExit(main())
