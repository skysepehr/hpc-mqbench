#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import statistics
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "pilots" / "corrected"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "pilot-acceptance"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate completed Pulsar instrumentation and memory pilots before "
            "authorizing Phase 1 submission."
        )
    )
    parser.add_argument(
        "--results-root",
        action="append",
        type=Path,
        required=True,
        help=(
            "Result root containing pilot final reports. Repeat this option "
            "when instrumentation and memory pilots use separate run IDs."
        ),
    )
    parser.add_argument(
        "--instrumentation-manifest",
        default=str(DEFAULT_CONFIG_ROOT / "instrumentation_overhead_manifest.csv"),
    )
    parser.add_argument(
        "--memory-manifest",
        default=str(DEFAULT_CONFIG_ROOT / "memory_acceptance_manifest.csv"),
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reports = _discover_reports(args.results_root)
    instrumentation_manifest = _read_manifest(Path(args.instrumentation_manifest))
    memory_manifest = _read_manifest(Path(args.memory_manifest))
    instrumentation = _validate_manifest(instrumentation_manifest, reports)
    memory = _validate_manifest(memory_manifest, reports)

    failures = instrumentation["failures"] + memory["failures"]
    overhead = _instrumentation_overhead(
        instrumentation_manifest,
        instrumentation["case_rows"],
    )
    failures.extend(overhead["failures"])
    memory_comparison = _memory_comparison(
        memory_manifest,
        memory["case_rows"],
    )
    failures.extend(memory_comparison["failures"])

    authorized = not failures
    report = {
        "format": "messaging-benchmark.pulsar-pilot-acceptance.v1",
        "backend_id": "pulsar",
        "phase1_submission_authorized": authorized,
        "results_roots": [str(path) for path in args.results_root],
        "required_case_count": (
            len(instrumentation_manifest) + len(memory_manifest)
        ),
        "observed_case_count": (
            len(instrumentation["case_rows"]) + len(memory["case_rows"])
        ),
        "instrumentation": overhead,
        "memory": memory_comparison,
        "case_validation": {
            "instrumentation": instrumentation["case_rows"],
            "memory": memory["case_rows"],
        },
        "failure_reasons": sorted(set(failures)),
        "rules": {
            "missing_after_drain_records": 0,
            "invalid_envelopes": 0,
            "duplicate_records": 0,
            "out_of_order_records": 0,
            "failed_send_percent_max": 0.1,
            "flush_duration_sec_max": 10.0,
            "latency_validation_required_when_enabled": True,
            "instrumentation_overhead_percent_max": 3.0,
            "h16_to_h8_throughput_ratio_min_per_anchor": 0.97,
            "h16_to_h8_p99_latency_ratio_max_per_anchor": 1.10,
        },
    }

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "acceptance_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "acceptance_report.md").write_text(
        _markdown(report),
        encoding="utf-8",
    )
    print(
        "[pulsar-pilot-validate] Phase 1 submission authorized: "
        f"{str(authorized).lower()}"
    )
    print(
        "[pulsar-pilot-validate] report: "
        f"{output_dir / 'acceptance_report.json'}"
    )
    return 0 if authorized else 1


def _discover_reports(results_roots: list[Path]) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for results_root in results_roots:
        if not results_root.exists():
            raise ValueError(f"Results root does not exist: {results_root}")
        candidates = (
            [results_root]
            if results_root.is_file()
            else sorted(results_root.rglob("final_report.json"))
        )
        for path in candidates:
            if path.parent.name in {"data", "reports"}:
                continue
            payload = json.loads(path.read_text(encoding="utf-8"))
            case_id = str(_dict(payload.get("case")).get("case_id", "")).strip()
            if not case_id:
                continue
            if case_id in reports:
                raise ValueError(f"Duplicate final report for case_id={case_id}")
            payload["_source_path"] = str(path)
            reports[case_id] = payload
    return reports


def _read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError(f"Empty pilot manifest: {path}")
    return rows


def _validate_manifest(
    manifest: list[dict[str, str]],
    reports: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    for manifest_row in manifest:
        case_id = manifest_row["case_id"]
        report = reports.get(case_id)
        if report is None:
            failures.append(f"missing final report: {case_id}")
            continue
        row = _case_row(report)
        rows.append(row)
        for reason in row["acceptance_failures"]:
            failures.append(f"{case_id}: {reason}")
    return {"case_rows": rows, "failures": failures}


def _case_row(report: dict[str, Any]) -> dict[str, Any]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    settings = _dict(config.get("backend_settings"))
    workload_metadata = _dict(_dict(config.get("extra")).get("campaign_metadata"))
    aggregated = _dict(report.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    correctness = _dict(aggregated.get("record_correctness"))
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    latency_common = _dict(common.get("latency_end_to_end"))
    latency_validation = _dict(report.get("latency_validation"))
    health = _dict(report.get("backend_health"))

    attempted = int(producers.get("messages_attempted", 0) or 0)
    failed = int(producers.get("messages_failed", 0) or 0)
    failed_percent = 100.0 * failed / attempted if attempted else 100.0
    latency_enabled = bool(config.get("latency_enabled", False))
    failures: list[str] = []
    if case.get("status") != "completed":
        failures.append(f"case status is {case.get('status', 'unknown')}")
    if health.get("status") != "healthy":
        failures.append(f"backend health is {health.get('status', 'not_recorded')}")
    checks = {
        "missing after drain": int(
            correctness.get("missing_after_drain_records", -1)
        ),
        "invalid envelopes": int(correctness.get("invalid_envelope_count", -1)),
        "duplicate records": int(correctness.get("duplicate_offset_count", -1)),
        "out-of-order records": int(
            correctness.get("out_of_order_offset_count", -1)
        ),
    }
    for name, value in checks.items():
        if value != 0:
            failures.append(f"{name} is {value}, expected 0")
    if failed_percent > 0.1:
        failures.append(f"failed-send percentage is {failed_percent:.6f}%")
    flush_sec = float(producers.get("max_flush_duration_sec", 0.0) or 0.0)
    if flush_sec > 10.0:
        failures.append(f"maximum producer flush is {flush_sec:.6f} s")
    if latency_enabled and not bool(latency_validation.get("valid", False)):
        failures.append("latency validation is not valid")

    return {
        "case_id": case.get("case_id"),
        "profile_id": settings.get("profile_id"),
        "block": int(workload_metadata.get("block", 0) or 0),
        "anchor": workload_metadata.get("anchor", "moderate"),
        "latency_enabled": latency_enabled,
        "balanced_mib_per_sec": float(
            throughput.get("balanced_mib_per_sec", 0.0) or 0.0
        ),
        "p99_latency_us": float(latency_common.get("p99_us", 0.0) or 0.0),
        "failed_send_percent": failed_percent,
        "max_flush_duration_sec": flush_sec,
        "missing_after_drain_records": checks["missing after drain"],
        "latency_valid": bool(latency_validation.get("valid", False)),
        "acceptance_valid": not failures,
        "acceptance_failures": failures,
        "source_path": report.get("_source_path"),
    }


def _instrumentation_overhead(
    manifest: list[dict[str, str]],
    case_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    by_case = {row["case_id"]: row for row in case_rows}
    block_ratios: list[dict[str, Any]] = []
    failures: list[str] = []
    for block in sorted({int(row["block"]) for row in manifest}):
        block_manifest = [row for row in manifest if int(row["block"]) == block]
        observed = [by_case.get(row["case_id"]) for row in block_manifest]
        observed = [row for row in observed if row is not None]
        enabled = next(
            (row for row in observed if row["latency_enabled"]),
            None,
        )
        disabled = next(
            (row for row in observed if not row["latency_enabled"]),
            None,
        )
        if enabled is None or disabled is None:
            failures.append(f"instrumentation block {block} is incomplete")
            continue
        baseline = disabled["balanced_mib_per_sec"]
        if baseline <= 0:
            failures.append(
                f"instrumentation block {block} has zero disabled throughput"
            )
            continue
        overhead = 100.0 * (
            baseline - enabled["balanced_mib_per_sec"]
        ) / baseline
        block_ratios.append(
            {
                "block": block,
                "disabled_balanced_mib_per_sec": baseline,
                "enabled_balanced_mib_per_sec": enabled["balanced_mib_per_sec"],
                "overhead_percent": overhead,
            }
        )
    median_overhead = (
        statistics.median(row["overhead_percent"] for row in block_ratios)
        if block_ratios
        else None
    )
    if median_overhead is None or median_overhead > 3.0:
        failures.append(
            "median instrumentation overhead is unavailable or exceeds 3%"
        )
    return {
        "required_case_count": len(manifest),
        "observed_case_count": len(case_rows),
        "block_results": block_ratios,
        "median_overhead_percent": median_overhead,
        "passed": not failures,
        "failures": failures,
    }


def _memory_comparison(
    manifest: list[dict[str, str]],
    case_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in case_rows:
        groups.setdefault((str(row["profile_id"]), str(row["anchor"])), []).append(
            row
        )
    summaries: list[dict[str, Any]] = []
    for (profile_id, anchor), rows in sorted(groups.items()):
        summaries.append(
            {
                "profile_id": profile_id,
                "anchor": anchor,
                "repeat_count": len(rows),
                "valid_repeat_count": sum(row["acceptance_valid"] for row in rows),
                "median_balanced_mib_per_sec": statistics.median(
                    row["balanced_mib_per_sec"] for row in rows
                ),
                "median_p99_latency_us": statistics.median(
                    row["p99_latency_us"] for row in rows
                ),
            }
        )
    index = {
        (row["profile_id"], row["anchor"]): row
        for row in summaries
    }
    comparisons: list[dict[str, Any]] = []
    failures: list[str] = []
    for anchor in ("moderate", "stress"):
        p1 = index.get(("P1", anchor))
        h16 = index.get(("BASELINE_H16_D32", anchor))
        if p1 is None or h16 is None:
            failures.append(f"memory anchor {anchor} is incomplete")
            continue
        if p1["repeat_count"] != 3 or h16["repeat_count"] != 3:
            failures.append(f"memory anchor {anchor} does not have 3 repeats/profile")
        throughput_ratio = _ratio(
            h16["median_balanced_mib_per_sec"],
            p1["median_balanced_mib_per_sec"],
        )
        latency_ratio = _ratio(
            h16["median_p99_latency_us"],
            p1["median_p99_latency_us"],
        )
        comparisons.append(
            {
                "anchor": anchor,
                "h16_to_h8_throughput_ratio": throughput_ratio,
                "h16_to_h8_p99_latency_ratio": latency_ratio,
            }
        )
        if throughput_ratio is None or throughput_ratio < 0.97:
            failures.append(
                f"{anchor}: H16/H8 throughput ratio is unavailable or below 0.97"
            )
        if latency_ratio is None or latency_ratio > 1.10:
            failures.append(
                f"{anchor}: H16/H8 p99 latency ratio is unavailable or above 1.10"
            )
    return {
        "required_case_count": len(manifest),
        "observed_case_count": len(case_rows),
        "group_summaries": summaries,
        "profile_comparisons": comparisons,
        "passed": not failures,
        "failures": failures,
    }


def _markdown(report: dict[str, Any]) -> str:
    authorized = report["phase1_submission_authorized"]
    lines = [
        "# Pulsar Corrected-Pilot Acceptance",
        "",
        f"**Phase 1 submission authorized:** `{str(authorized).lower()}`",
        "",
        (
            "Authorization requires complete healthy cases, exact post-flush "
            "drain accounting, valid sampled latency, bounded producer flush, "
            "acceptable instrumentation overhead, and no material H16/H8 "
            "throughput or p99-latency regression."
        ),
        "",
        "## Instrumentation",
        "",
        "| Block | Disabled MiB/s | Enabled MiB/s | Overhead (%) |",
        "|---:|---:|---:|---:|",
    ]
    for row in report["instrumentation"]["block_results"]:
        lines.append(
            "| {block} | {disabled_balanced_mib_per_sec:.3f} | "
            "{enabled_balanced_mib_per_sec:.3f} | {overhead_percent:.3f} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Memory Profiles",
            "",
            "| Profile | Anchor | Repeats | Valid | Median MiB/s | Median p99 (us) |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in report["memory"]["group_summaries"]:
        lines.append(
            "| {profile_id} | {anchor} | {repeat_count} | {valid_repeat_count} | "
            "{median_balanced_mib_per_sec:.3f} | {median_p99_latency_us:.3f} |".format(
                **row
            )
        )
    lines.extend(["", "## Failures", ""])
    failures = report["failure_reasons"]
    if failures:
        lines.extend(f"- {failure}" for failure in failures)
    else:
        lines.append("- None.")
    lines.append("")
    return "\n".join(lines)


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator > 0 else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
