#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.qualification import (  # noqa: E402
    BACKLOG_DENOMINATOR_ATTEMPTED,
    BACKLOG_LIMIT_PERCENT,
    FAILED_SEND_LIMIT_PERCENT,
    FLUSH_LIMIT_SEC,
    producer_operational_metrics,
    producer_operational_reasons,
)
DEFAULT_MANIFEST = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "phase2"
    / "memory_screening_manifest.csv"
)
DEFAULT_PLAN = DEFAULT_MANIFEST.with_name("phase2_campaign_plan.json")
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "phase2-screening"
BASELINE_PROFILE = "BASELINE_H16_D32"
SUSTAINABLE_ANCHORS = ("cfg_120", "cfg_101", "cfg_093", "cfg_001")
TRANSITION_ANCHOR = "cfg_089"

CASE_FIELDS = (
    "screening_order",
    "case_id",
    "anchor",
    "anchor_purpose",
    "profile_id",
    "profile_sha256",
    "heap_gib",
    "direct_memory_gib",
    "status",
    "backend_health",
    "eligible",
    "qualified",
    "qualification_reason",
    "balanced_mib_per_sec",
    "balanced_records_per_sec",
    "latency_valid",
    "latency_p99_us",
    "latency_p99_9_us",
    "latency_sample_count",
    "messages_attempted",
    "messages_enqueued",
    "pending_messages_at_flush_start",
    "backlog_denominator",
    "backlog_denominator_value",
    "producer_backlog_percent",
    "records_delivered",
    "records_consumed",
    "records_late_drained",
    "records_missing_after_drain",
    "duplicate_records",
    "out_of_order_records",
    "failed_send_percent",
    "max_flush_duration_sec",
    "pulsar_bytes_in_peak_mib_per_sec",
    "pulsar_bytes_out_peak_mib_per_sec",
    "pulsar_process_rss_peak_gib",
    "pulsar_jvm_heap_peak_gib",
    "pulsar_jvm_direct_nio_peak_gib",
    "pulsar_managed_ledger_direct_pool_allocated_peak_gib",
    "pulsar_managed_ledger_direct_pool_used_peak_gib",
    "pulsar_direct_memory_usage_peak_percent",
    "pulsar_jvm_gc_time_rate_peak",
    "pulsar_message_backlog_peak",
    "tmpfs_used_peak_percent",
    "post_reset_tmpfs_free_percent",
    "source_report",
)

PROFILE_FIELDS = (
    "screening_rank",
    "selected_for_confirmation",
    "profile_id",
    "heap_gib",
    "direct_memory_gib",
    "case_count",
    "complete_count",
    "healthy_count",
    "eligible_count",
    "latency_valid_count",
    "correct_count",
    "qualified_count",
    "sustainable_qualified_count",
    "all_required_valid",
    "geometric_mean_throughput_ratio_to_baseline",
    "worst_anchor_throughput_ratio_to_baseline",
    "geometric_mean_p99_latency_ratio_to_baseline",
    "max_producer_backlog_percent",
    "max_failed_send_percent",
    "max_flush_duration_sec",
    "max_process_rss_gib",
    "max_jvm_heap_gib",
    "max_jvm_direct_nio_gib",
    "max_managed_ledger_direct_pool_allocated_gib",
    "max_managed_ledger_direct_pool_used_gib",
    "max_direct_memory_usage_percent",
    "max_jvm_gc_time_rate",
    "max_tmpfs_used_percent",
    "min_post_reset_tmpfs_free_percent",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Strictly validate and analyze the 30-case Pulsar Phase 2 "
            "memory-profile screen."
        )
    )
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--campaign-plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--machine-only",
        action="store_true",
        help="Write only validated JSON/CSV and checksums.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = _read_manifest(args.manifest)
    plan = _read_json(args.campaign_plan)
    reports, duplicates = _discover_reports(args.results_root)
    validation, rows = _validate_and_collect(
        manifest=manifest,
        plan=plan,
        reports=reports,
        duplicates=duplicates,
        results_root=args.results_root,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    validation_path = args.output_dir / "pulsar_phase2_screening_validation.json"
    _write_json(validation_path, validation)
    if not validation["valid"]:
        if not args.machine_only:
            _write_incomplete_report(
                args.output_dir / "pulsar_phase2_screening_report.md",
                validation,
            )
        print("[pulsar-phase2-screening] analysis incomplete")
        for reason in validation["failure_reasons"]:
            print(f"[pulsar-phase2-screening] {reason}")
        return 1

    profile_rows, selection = _profile_analysis(rows)
    selection["source_evidence"] = {
        "screening_results_root": args.results_root.name,
        "slurm_job_ids": _slurm_job_ids(args.results_root),
        "manifest_sha256": _sha256(args.manifest),
        "campaign_plan_sha256": _sha256(args.campaign_plan),
        "validation_sha256": _sha256(validation_path),
    }
    _write_csv(args.output_dir / "pulsar_phase2_screening_cases.csv", CASE_FIELDS, rows)
    _write_json(
        args.output_dir / "pulsar_phase2_screening_cases.json",
        {
            "format": "messaging-benchmark.pulsar-phase2-screening-cases.v1",
            "case_count": len(rows),
            "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "cases": rows,
        },
    )
    _write_csv(
        args.output_dir / "pulsar_phase2_screening_profiles.csv",
        PROFILE_FIELDS,
        profile_rows,
    )
    _write_json(
        args.output_dir / "pulsar_phase2_candidate_selection.json",
        selection,
    )
    if not args.machine_only:
        _write_markdown(
            args.output_dir / "pulsar_phase2_screening_report.md",
            profile_rows,
            selection,
            validation,
        )
    _write_checksums(args.output_dir)
    print(f"[pulsar-phase2-screening] cases: {len(rows)}")
    print(
        "[pulsar-phase2-screening] confirmation profiles: "
        + ", ".join(selection["confirmation_profiles"])
    )
    print(f"[pulsar-phase2-screening] output: {args.output_dir}")
    return 0


def _read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {
        "order",
        "case_id",
        "anchor",
        "anchor_purpose",
        "profile_id",
        "profile_sha256",
        "heap_gib",
        "direct_memory_gib",
    }
    missing = required - set(rows[0] if rows else {})
    if missing:
        raise ValueError(
            "Phase 2 screening manifest is empty or missing fields: "
            + ", ".join(sorted(missing))
        )
    return rows


def _discover_reports(
    root: Path,
) -> tuple[dict[str, tuple[Path, dict[str, Any]]], list[str]]:
    if not root.exists():
        raise ValueError(f"Results root does not exist: {root}")
    candidates_by_case: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
    for path in sorted(root.rglob("final_report.json")):
        if path.parent.name in {"data", "reports"}:
            continue
        report = _read_json(path)
        system = _dict(report.get("system_under_test"))
        if system.get("backend_id") != "pulsar":
            continue
        case_id = str(_dict(report.get("case")).get("case_id", ""))
        if not case_id:
            continue
        candidates_by_case.setdefault(case_id, []).append((path, report))
    selected: dict[str, tuple[Path, dict[str, Any]]] = {}
    duplicates: list[str] = []
    for case_id, candidates in candidates_by_case.items():
        candidates.sort(
            key=lambda item: _report_selection_key(item[0], item[1]),
            reverse=True,
        )
        preferred = candidates[0]
        preferred_completed = (
            _dict(preferred[1].get("case")).get("status") == "completed"
        )
        preferred_hash = _canonical_json_sha256(preferred[1])
        if any(
            preferred_completed
            and _dict(report.get("case")).get("status") == "completed"
            and _canonical_json_sha256(report) != preferred_hash
            for _, report in candidates[1:]
        ):
            duplicates.append(case_id)
        selected[case_id] = preferred
    return selected, sorted(duplicates)


def _validate_and_collect(
    *,
    manifest: list[dict[str, str]],
    plan: dict[str, Any],
    reports: dict[str, tuple[Path, dict[str, Any]]],
    duplicates: list[str],
    results_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failures: list[str] = []
    expected_ids = [row["case_id"] for row in manifest]
    expected_profiles = {
        str(item["profile_id"]): str(item["profile_sha256"])
        for item in plan.get("profiles", [])
        if isinstance(item, dict)
    }
    if len(manifest) != 30:
        failures.append(f"manifest has {len(manifest)} cases, expected 30")
    if len(set(expected_ids)) != 30:
        failures.append("manifest case IDs are not unique")
    if duplicates:
        failures.append(
            "conflicting completed reports: " + ", ".join(duplicates)
        )
    if len(expected_profiles) != 6:
        failures.append("campaign plan does not define exactly six profiles")

    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    for manifest_row in manifest:
        case_id = manifest_row["case_id"]
        selected = reports.get(case_id)
        if selected is None:
            missing.append(case_id)
            continue
        path, report = selected
        row = _case_row(manifest_row, report, path, results_root)
        rows.append(row)
        expected_profile_sha = expected_profiles.get(row["profile_id"])
        checks = (
            (row["status"] == "completed", "status is not completed"),
            (row["backend_health"] == "healthy", "backend is not healthy"),
            (row["profile_id"] == manifest_row["profile_id"], "profile ID drift"),
            (
                row["profile_sha256"] == manifest_row["profile_sha256"],
                "manifest profile checksum drift",
            ),
            (
                row["profile_sha256"] == expected_profile_sha,
                "campaign profile checksum drift",
            ),
            (
                not row["eligible"] or row["latency_valid"],
                "eligible case has invalid latency",
            ),
            (
                not row["eligible"] or row["records_missing_after_drain"] == 0,
                "eligible case has missing records",
            ),
            (
                not row["eligible"] or row["duplicate_records"] == 0,
                "eligible case has duplicate records",
            ),
            (
                not row["eligible"] or row["out_of_order_records"] == 0,
                "eligible case has out-of-order records",
            ),
            (
                row["records_delivered"]
                == row["records_consumed"]
                + row["records_late_drained"]
                + row["records_missing_after_drain"],
                "record accounting does not balance",
            ),
            (
                row["balanced_mib_per_sec"] is not None
                and row["balanced_mib_per_sec"] >= 0,
                "balanced throughput is invalid",
            ),
            (
                row["balanced_records_per_sec"] is not None
                and row["balanced_records_per_sec"] >= 0,
                "balanced record rate is invalid",
            ),
            (
                row["tmpfs_used_peak_percent"] is not None,
                "tmpfs peak evidence is missing",
            ),
            (
                row["post_reset_tmpfs_free_percent"] is not None,
                "post-reset tmpfs evidence is missing",
            ),
        )
        for passed, reason in checks:
            if not passed:
                failures.append(f"{case_id}: {reason}")
        expected_qualified = (
            row["eligible"]
            and row["producer_backlog_percent"] <= BACKLOG_LIMIT_PERCENT
            and row["max_flush_duration_sec"] <= FLUSH_LIMIT_SEC
            and row["failed_send_percent"] <= FAILED_SEND_LIMIT_PERCENT
        )
        if row["qualified"] is not expected_qualified:
            failures.append(f"{case_id}: qualification result does not match policy")

    if missing:
        failures.append(
            f"missing {len(missing)} required report(s): " + ", ".join(missing)
        )
    extras = sorted(set(reports) - set(expected_ids))
    validation = {
        "format": "messaging-benchmark.pulsar-phase2-screening-validation.v1",
        "valid": not failures,
        "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "expected_case_count": 30,
        "manifest_case_count": len(manifest),
        "observed_case_count": len(rows),
        "eligible_case_count": sum(bool(row["eligible"]) for row in rows),
        "qualified_case_count": sum(bool(row["qualified"]) for row in rows),
        "extra_case_ids": extras,
        "results_root": results_root.name,
        "failure_reasons": list(dict.fromkeys(failures)),
    }
    return validation, sorted(rows, key=lambda row: row["screening_order"])


def _case_row(
    manifest: dict[str, str],
    report: dict[str, Any],
    path: Path,
    results_root: Path,
) -> dict[str, Any]:
    case = _dict(report.get("case"))
    system = _dict(report.get("system_under_test"))
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    records = _dict(common.get("records"))
    latency = _dict(common.get("latency_end_to_end"))
    qualification = _dict(common.get("qualification"))
    producers = _dict(_dict(report.get("aggregated_metrics")).get("producers"))
    health = _dict(report.get("backend_health"))
    monitoring = _dict(_dict(report.get("backend_metrics")).get("pulsar"))
    monitoring = _dict(monitoring.get("monitoring")) or _dict(report.get("monitoring"))
    metric_map = {
        str(item.get("id")): item
        for item in monitoring.get("collected_metrics", [])
        if isinstance(item, dict) and item.get("id")
    }
    operational = producer_operational_metrics(
        producers,
        backlog_denominator=BACKLOG_DENOMINATOR_ATTEMPTED,
    )
    failed_percent = float(operational["failed_send_percent"])
    flush_sec = float(operational["max_flush_duration_sec"])
    eligible = qualification.get("eligible") is True
    qualified = eligible and bool(operational["thresholds_satisfied"])
    reasons = _qualification_reasons(
        eligible=eligible,
        operational=operational,
    )
    tmpfs_peak, post_reset_free = _tmpfs_evidence(path.parent)
    return {
        "screening_order": int(manifest["order"]),
        "case_id": manifest["case_id"],
        "anchor": manifest["anchor"],
        "anchor_purpose": manifest["anchor_purpose"],
        "profile_id": str(system.get("profile_id", "")),
        "profile_sha256": str(system.get("profile_sha256", "")),
        "heap_gib": int(manifest["heap_gib"]),
        "direct_memory_gib": int(manifest["direct_memory_gib"]),
        "status": str(case.get("status", "unknown")),
        "backend_health": str(health.get("status", "not_recorded")),
        "eligible": eligible,
        "qualified": qualified,
        "qualification_reason": "; ".join(reasons),
        "balanced_mib_per_sec": _optional_number(throughput.get("balanced_mib_per_sec")),
        "balanced_records_per_sec": _optional_number(throughput.get("balanced_records_per_sec")),
        "latency_valid": latency.get("valid") is True,
        "latency_p99_us": _optional_number(latency.get("p99_us")),
        "latency_p99_9_us": _optional_number(latency.get("p99_9_us")),
        "latency_sample_count": _integer(latency.get("count")),
        "messages_attempted": int(operational["messages_attempted"]),
        "messages_enqueued": int(operational["messages_enqueued"]),
        "pending_messages_at_flush_start": int(
            operational["pending_messages_at_flush_start"]
        ),
        "backlog_denominator": str(operational["backlog_denominator"]),
        "backlog_denominator_value": int(
            operational["backlog_denominator_value"]
        ),
        "producer_backlog_percent": float(
            operational["producer_backlog_percent"]
        ),
        "records_delivered": _integer(records.get("published")),
        "records_consumed": _integer(records.get("consumed")),
        "records_late_drained": _integer(records.get("late_drained")),
        "records_missing_after_drain": _integer(records.get("missing")),
        "duplicate_records": _integer(records.get("duplicate")),
        "out_of_order_records": _integer(records.get("out_of_order")),
        "failed_send_percent": failed_percent,
        "max_flush_duration_sec": flush_sec,
        "pulsar_bytes_in_peak_mib_per_sec": _bytes_to_mib(_metric(metric_map, "pulsar_bytes_in_rate")),
        "pulsar_bytes_out_peak_mib_per_sec": _bytes_to_mib(_metric(metric_map, "pulsar_bytes_out_rate")),
        "pulsar_process_rss_peak_gib": _bytes_to_gib(_metric(metric_map, "pulsar_process_resident_memory_bytes")),
        "pulsar_jvm_heap_peak_gib": _bytes_to_gib(_metric(metric_map, "pulsar_jvm_heap_used_bytes")),
        "pulsar_jvm_direct_nio_peak_gib": _bytes_to_gib(_metric(metric_map, "pulsar_jvm_direct_memory_used_bytes")),
        "pulsar_managed_ledger_direct_pool_allocated_peak_gib": _bytes_to_gib(
            _metric(metric_map, "pulsar_managed_ledger_direct_pool_allocated_bytes")
        ),
        "pulsar_managed_ledger_direct_pool_used_peak_gib": _bytes_to_gib(
            _metric(metric_map, "pulsar_managed_ledger_direct_pool_used_bytes")
        ),
        "pulsar_direct_memory_usage_peak_percent": _metric(
            metric_map, "pulsar_direct_memory_usage_percent"
        ),
        "pulsar_jvm_gc_time_rate_peak": _metric(metric_map, "pulsar_jvm_gc_time_rate"),
        "pulsar_message_backlog_peak": _metric(metric_map, "pulsar_message_backlog"),
        "tmpfs_used_peak_percent": tmpfs_peak,
        "post_reset_tmpfs_free_percent": post_reset_free,
        "source_report": _relative(path, results_root),
    }


def _profile_analysis(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_cell = {(row["profile_id"], row["anchor"]): row for row in rows}
    baseline = {
        anchor: by_cell[(BASELINE_PROFILE, anchor)]
        for anchor in (*SUSTAINABLE_ANCHORS, TRANSITION_ANCHOR)
    }
    profile_records: list[dict[str, Any]] = []
    for profile_id in sorted({row["profile_id"] for row in rows}):
        items = [row for row in rows if row["profile_id"] == profile_id]
        ratios = []
        latency_ratios = []
        for anchor in SUSTAINABLE_ANCHORS:
            row = by_cell[(profile_id, anchor)]
            control = baseline[anchor]
            ratios.append(row["balanced_mib_per_sec"] / control["balanced_mib_per_sec"])
            latency_ratios.append(row["latency_p99_us"] / control["latency_p99_us"])
        correct_count = sum(
            row["records_missing_after_drain"] == 0
            and row["duplicate_records"] == 0
            and row["out_of_order_records"] == 0
            for row in items
        )
        record = {
            "screening_rank": None,
            "selected_for_confirmation": False,
            "profile_id": profile_id,
            "heap_gib": items[0]["heap_gib"],
            "direct_memory_gib": items[0]["direct_memory_gib"],
            "case_count": len(items),
            "complete_count": sum(row["status"] == "completed" for row in items),
            "healthy_count": sum(row["backend_health"] == "healthy" for row in items),
            "eligible_count": sum(row["eligible"] for row in items),
            "latency_valid_count": sum(row["latency_valid"] for row in items),
            "correct_count": correct_count,
            "qualified_count": sum(row["qualified"] for row in items),
            "sustainable_qualified_count": sum(
                by_cell[(profile_id, anchor)]["qualified"]
                for anchor in SUSTAINABLE_ANCHORS
            ),
            "all_required_valid": (
                len(items) == 5
                and all(row["status"] == "completed" for row in items)
                and all(row["backend_health"] == "healthy" for row in items)
                and all(row["eligible"] for row in items)
                and all(row["latency_valid"] for row in items)
                and correct_count == 5
                and all(row["tmpfs_used_peak_percent"] is not None for row in items)
                and all(row["post_reset_tmpfs_free_percent"] is not None for row in items)
            ),
            "geometric_mean_throughput_ratio_to_baseline": _geometric_mean(ratios),
            "worst_anchor_throughput_ratio_to_baseline": min(ratios),
            "geometric_mean_p99_latency_ratio_to_baseline": _geometric_mean(latency_ratios),
            "max_producer_backlog_percent": max(
                row["producer_backlog_percent"] for row in items
            ),
            "max_failed_send_percent": max(row["failed_send_percent"] for row in items),
            "max_flush_duration_sec": max(row["max_flush_duration_sec"] for row in items),
            "max_process_rss_gib": _max_optional(row["pulsar_process_rss_peak_gib"] for row in items),
            "max_jvm_heap_gib": _max_optional(row["pulsar_jvm_heap_peak_gib"] for row in items),
            "max_jvm_direct_nio_gib": _max_optional(row["pulsar_jvm_direct_nio_peak_gib"] for row in items),
            "max_managed_ledger_direct_pool_allocated_gib": _max_optional(
                row["pulsar_managed_ledger_direct_pool_allocated_peak_gib"] for row in items
            ),
            "max_managed_ledger_direct_pool_used_gib": _max_optional(
                row["pulsar_managed_ledger_direct_pool_used_peak_gib"] for row in items
            ),
            "max_direct_memory_usage_percent": _max_optional(
                row["pulsar_direct_memory_usage_peak_percent"] for row in items
            ),
            "max_jvm_gc_time_rate": _max_optional(row["pulsar_jvm_gc_time_rate_peak"] for row in items),
            "max_tmpfs_used_percent": _max_optional(row["tmpfs_used_peak_percent"] for row in items),
            "min_post_reset_tmpfs_free_percent": _min_optional(
                row["post_reset_tmpfs_free_percent"] for row in items
            ),
        }
        profile_records.append(record)

    ranked = sorted(profile_records, key=_profile_key)
    for rank, record in enumerate(ranked, start=1):
        record["screening_rank"] = rank
    candidates = [
        record
        for record in ranked
        if record["profile_id"] != BASELINE_PROFILE and record["all_required_valid"]
    ][:2]
    for record in candidates:
        record["selected_for_confirmation"] = True
    selection_ready = len(candidates) == 2
    selection = {
        "format": "messaging-benchmark.pulsar-phase2-candidate-selection.v1",
        "status": "ready_for_confirmation" if selection_ready else "insufficient_valid_profiles",
        "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "screening_observations_per_profile_anchor": 1,
        "baseline_profile": BASELINE_PROFILE,
        "selected_candidate_profiles": [record["profile_id"] for record in candidates],
        "confirmation_profiles": [BASELINE_PROFILE, *[record["profile_id"] for record in candidates]],
        "selection_rule": (
            "Exclude the baseline from candidacy; require five complete, healthy, "
            "eligible, latency-valid, correctness-valid cases with tmpfs evidence; "
            "then order by sustainable-anchor qualified count, geometric-mean "
            "balanced-throughput ratio to baseline, geometric-mean p99-latency "
            "ratio, worst-anchor throughput ratio, flush time, and profile ID."
        ),
        "interpretation": (
            "Screening nominates profiles for repeated confirmation only. The five "
            "anchors are the historically executed set selected before the backlog-rule "
            "correction; this analysis does not imply that the retrospectively corrected "
            "Phase 1 shortlist was run."
        ),
        "profiles": ranked,
    }
    return sorted(profile_records, key=lambda row: row["screening_rank"]), selection


def _profile_key(record: dict[str, Any]) -> tuple[Any, ...]:
    return (
        not record["all_required_valid"],
        -record["sustainable_qualified_count"],
        -_sortable_high(record["geometric_mean_throughput_ratio_to_baseline"]),
        _sortable_low(record["geometric_mean_p99_latency_ratio_to_baseline"]),
        -_sortable_high(record["worst_anchor_throughput_ratio_to_baseline"]),
        record["max_flush_duration_sec"],
        record["profile_id"],
    )


def _tmpfs_evidence(case_dir: Path) -> tuple[float | None, float | None]:
    peaks: list[float] = []
    for path in sorted((case_dir / "monitoring").glob("pulsar_process_*.csv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                value = _optional_number(row.get("tmpfs_used_percent"))
                if value is not None:
                    peaks.append(value)
    reset_path = case_dir / "logs" / "pulsar" / "post-case-storage-reset.log"
    reset_free = None
    if reset_path.is_file():
        match = re.search(
            r"^post_reset_free_percent=(\d+(?:\.\d+)?)$",
            reset_path.read_text(encoding="utf-8", errors="replace"),
            flags=re.MULTILINE,
        )
        if match:
            reset_free = float(match.group(1))
    return (max(peaks) if peaks else None, reset_free)


def _qualification_reasons(
    *, eligible: bool, operational: dict[str, float | int | bool]
) -> list[str]:
    reasons: list[str] = []
    if not eligible:
        reasons.append("case is ineligible")
    reasons.extend(producer_operational_reasons(operational))
    return reasons or [
        "eligible and shared Kafka/Pulsar application-level backlog, flush, and "
        "failed-send thresholds satisfied"
    ]


def _write_markdown(
    path: Path,
    profiles: list[dict[str, Any]],
    selection: dict[str, Any],
    validation: dict[str, Any],
) -> None:
    lines = [
        "# Pulsar Phase 2 Memory-Profile Screening",
        "",
        "This qualification-first screen compares six immutable JVM memory "
        "profiles over five historically executed workload anchors. These anchors "
        "were selected before the Phase 1 backlog-rule correction and are not the "
        "retrospectively corrected shortlist. Each cell has one observation, so "
        "selected profiles require repeated confirmation.",
        "",
        "A row qualifies when producer backlog at flush start is at most 5% of "
        "measurement-period publication attempts, maximum flush is at most 10 "
        "seconds, and failed sends "
        "are at most 0.1%. Post-drain correctness remains an eligibility gate.",
        "",
        f"- Cases validated: {validation['observed_case_count']}/30",
        f"- Selection status: `{selection['status']}`",
        "- Confirmation profiles: "
        + ", ".join(f"`{item}`" for item in selection["confirmation_profiles"]),
        "",
        "| Rank | Profile | Heap/direct GiB | Valid | Anchor-set qualified | Throughput ratio | p99 ratio | Worst-anchor ratio | Max backlog % | Max RSS GiB | Max tmpfs % |",
        "|---:|---|---:|:---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in profiles:
        lines.append(
            f"| {row['screening_rank']} | `{row['profile_id']}` | "
            f"{row['heap_gib']}/{row['direct_memory_gib']} | "
            f"{'yes' if row['all_required_valid'] else 'no'} | "
            f"{row['sustainable_qualified_count']}/4 | "
            f"{_fmt(row['geometric_mean_throughput_ratio_to_baseline'])} | "
            f"{_fmt(row['geometric_mean_p99_latency_ratio_to_baseline'])} | "
            f"{_fmt(row['worst_anchor_throughput_ratio_to_baseline'])} | "
            f"{_fmt(row['max_producer_backlog_percent'])} | "
            f"{_fmt(row['max_process_rss_gib'])} | "
            f"{_fmt(row['max_tmpfs_used_percent'])} |"
        )
    lines.extend(
        [
            "",
            "The throughput and p99 columns are within-anchor ratios to "
            f"`{BASELINE_PROFILE}`. Resource observations are supporting evidence; "
            "they do not override correctness, eligibility, or qualification.",
            "",
            selection["interpretation"],
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_incomplete_report(path: Path, validation: dict[str, Any]) -> None:
    lines = ["# Pulsar Phase 2 Screening Incomplete", ""]
    lines.extend(f"- {reason}" for reason in validation["failure_reasons"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, fields: Iterable[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_checksums(output_dir: Path) -> None:
    checksum_path = output_dir / "SHA256SUMS"
    paths = sorted(
        path for path in output_dir.iterdir() if path.is_file() and path != checksum_path
    )
    checksum_path.write_text(
        "".join(f"{_sha256(path)}  {path.name}\n" for path in paths),
        encoding="utf-8",
    )


def _metric(metric_map: dict[str, dict[str, Any]], metric_id: str) -> float | None:
    return _optional_number(_dict(metric_map.get(metric_id)).get("max_value"))


def _bytes_to_mib(value: float | None) -> float | None:
    return value / 1048576.0 if value is not None else None


def _bytes_to_gib(value: float | None) -> float | None:
    return value / 1073741824.0 if value is not None else None


def _geometric_mean(values: Iterable[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and value > 0]
    return math.exp(sum(math.log(value) for value in clean) / len(clean)) if clean else None


def _max_optional(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return max(clean) if clean else None


def _min_optional(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return min(clean) if clean else None


def _sortable_high(value: float | None) -> float:
    return value if value is not None and math.isfinite(value) else -math.inf


def _sortable_low(value: float | None) -> float:
    return value if value is not None and math.isfinite(value) else math.inf


def _number(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _integer(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _report_selection_key(
    path: Path, report: dict[str, Any]
) -> tuple[int, int, int, str]:
    return (
        int(_dict(report.get("case")).get("status") == "completed"),
        int(path.parent.name not in {"data", "reports"}),
        -len(path.parts),
        str(path),
    )


def _slurm_job_ids(results_root: Path) -> list[str]:
    job_ids = set()
    for path in (results_root / "batches").glob("*_job_*"):
        suffix = path.name.rsplit("_job_", 1)[-1]
        if suffix.isdigit():
            job_ids.add(suffix)
    return sorted(job_ids, key=int)


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _fmt(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


if __name__ == "__main__":
    raise SystemExit(main())
