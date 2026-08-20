#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.qualification import (  # noqa: E402
    BACKLOG_DENOMINATOR_ATTEMPTED,
    BACKLOG_LIMIT_PERCENT,
    COMMON_QUALIFICATION_POLICY_ID,
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
    / "phase1"
    / "phase1_manifest.csv"
)
DEFAULT_PLAN = DEFAULT_MANIFEST.with_name("phase1_campaign_plan.json")
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "phase1"
REPORT_FORMAT = "messaging-benchmark.pulsar-phase1-analysis.v1"
HISTORICALLY_EXECUTED_PHASE2_ANCHORS = (
    "cfg_120",
    "cfg_101",
    "cfg_093",
    "cfg_089",
    "cfg_001",
)
HISTORICALLY_EXECUTED_FINAL_VALIDATION_CONFIGS = (
    "cfg_001",
    "cfg_007",
    "cfg_049",
    "cfg_055",
    "cfg_063",
    "cfg_080",
    "cfg_089",
    "cfg_093",
    "cfg_101",
    "cfg_120",
)

PARAMETER_LABELS = {
    "producer_ranks": "Producer MPI ranks",
    "consumer_ranks": "Consumer MPI ranks",
    "partitions": "Topic partitions",
    "payload_size_bytes": "Payload size (bytes)",
    "batching_max_messages": "Batching maximum messages",
    "batching_max_bytes": "Batching maximum bytes",
    "batching_max_publish_delay_ms": "Batching maximum delay (ms)",
    "max_pending_messages": "Pending messages per producer",
    "max_pending_messages_across_partitions": (
        "Pending messages across partitions"
    ),
    "receiver_queue_size": "Receiver queue size",
    "max_total_receiver_queue_size_across_partitions": (
        "Total receiver queue across partitions"
    ),
}

VARIED_FIELDS = (
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "payload_size_bytes",
    "batching_max_messages",
    "batching_max_bytes",
    "batching_max_publish_delay_ms",
    "max_pending_messages",
    "max_pending_messages_across_partitions",
    "receiver_queue_size",
    "max_total_receiver_queue_size_across_partitions",
)

CASE_FIELDS = (
    "primary_rank",
    "raw_throughput_rank",
    "config_id",
    "case_id",
    "design_note",
    "status",
    "backend_health",
    "profile_id",
    "profile_sha256",
    "eligible",
    "qualified",
    "qualification_reason",
    "producer_mib_per_sec",
    "consumer_mib_per_sec",
    "balanced_mib_per_sec",
    "producer_records_per_sec",
    "consumer_records_per_sec",
    "balanced_records_per_sec",
    "latency_valid",
    "latency_p50_us",
    "latency_p95_us",
    "latency_p99_us",
    "latency_p99_9_us",
    "latency_mean_us",
    "latency_max_us",
    "latency_sample_count",
    "clock_uncertainty_us_max",
    "clock_drift_us_max",
    "records_attempted",
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
    "missing_after_drain_percent",
    "failed_sends",
    "failed_send_percent",
    "max_flush_duration_sec",
    "monitoring_query_count",
    "monitoring_completed_query_count",
    "monitoring_missing_metric_count",
    "pulsar_bytes_in_peak_mib_per_sec",
    "pulsar_bytes_out_peak_mib_per_sec",
    "pulsar_service_ib0_rx_peak_mib_per_sec",
    "pulsar_service_ib0_tx_peak_mib_per_sec",
    "pulsar_process_rss_peak_gib",
    "pulsar_jvm_heap_peak_gib",
    "pulsar_jvm_gc_time_rate_peak",
    "pulsar_message_backlog_peak",
    *VARIED_FIELDS,
    "source_report",
)

CONTROLLED_FIELDS = (
    "config_id",
    "parameter",
    "parameter_label",
    "baseline_value",
    "tested_value",
    "balanced_mib_per_sec",
    "baseline_balanced_mib_per_sec",
    "balanced_mib_per_sec_delta",
    "balanced_mib_per_sec_change_percent",
    "balanced_records_per_sec",
    "baseline_balanced_records_per_sec",
    "balanced_records_per_sec_delta",
    "latency_p99_us",
    "baseline_latency_p99_us",
    "latency_p99_us_delta",
    "qualified",
)

SHORTLIST_FIELDS = (
    "selection_order",
    "config_id",
    "case_id",
    "selection_categories",
    "primary_rank",
    "eligible",
    "qualified",
    "balanced_mib_per_sec",
    "balanced_records_per_sec",
    "latency_p99_us",
    "producer_backlog_percent",
    "missing_after_drain_percent",
    "failed_send_percent",
    "max_flush_duration_sec",
    "design_note",
    *VARIED_FIELDS,
)

BACKLOG_SENSITIVITY_FIELDS = (
    "backlog_threshold_percent",
    "eligible_case_count",
    "qualified_case_count",
    "unqualified_eligible_case_count",
    "highest_qualified_config_id",
    "highest_qualified_balanced_mib_per_sec",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a strict qualification-first report and Phase 2 workload "
            "shortlist from all 120 Pulsar Phase 1 cases."
        )
    )
    parser.add_argument("--results-root", action="append", type=Path, default=[])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--campaign-plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--shortlist-size", type=int, default=10)
    parser.add_argument(
        "--skip-figures",
        action="store_true",
        help="Skip optional Matplotlib rendering for dependency-free contract tests.",
    )
    parser.add_argument(
        "--machine-only",
        action="store_true",
        help="Write only validated JSON/CSV, manifests, and checksums.",
    )
    parser.add_argument(
        "--refresh-manifest-only",
        action="store_true",
        help="Refresh artifact_manifest.csv and SHA256SUMS after PDF compilation.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.refresh_manifest_only:
        if not args.output_dir.is_dir():
            raise FileNotFoundError(
                f"Phase 1 output directory does not exist: {args.output_dir}"
            )
        _write_artifact_manifest(args.output_dir)
        print(f"[pulsar-phase1] refreshed manifest: {args.output_dir}")
        return 0
    if not args.results_root:
        raise ValueError("at least one --results-root is required")

    manifest = _read_manifest(args.manifest)
    plan = _read_json(args.campaign_plan)
    reports, duplicate_cases = _discover_reports(args.results_root)
    validation, rows = _validate_and_collect(
        manifest=manifest,
        plan=plan,
        reports=reports,
        duplicate_cases=duplicate_cases,
        results_roots=args.results_root,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(args.output_dir / "pulsar_phase1_validation.json", validation)
    if not validation["valid"]:
        if not args.machine_only:
            _write_incomplete_markdown(
                args.output_dir / "pulsar_phase1_report.md",
                validation,
            )
        print("[pulsar-phase1] analysis incomplete")
        for reason in validation["failure_reasons"]:
            print(f"[pulsar-phase1] {reason}")
        return 1

    _rank_rows(rows)
    shortlist = select_shortlist(rows, args.shortlist_size)
    controlled = _controlled_comparisons(rows)
    backlog_sensitivity = _backlog_threshold_sensitivity(rows)
    summary = _analysis_summary(
        rows,
        shortlist,
        validation,
        plan,
        backlog_sensitivity,
    )
    figures: dict[str, dict[str, str]] = {}
    if not args.machine_only:
        figures = (
            _figure_paths(args.output_dir)
            if args.skip_figures
            else _write_figures(args.output_dir, rows, shortlist, controlled)
        )
    _write_csv(args.output_dir / "pulsar_phase1_cases.csv", CASE_FIELDS, rows)
    _write_json(
        args.output_dir / "pulsar_phase1_cases.json",
        {
            "format": REPORT_FORMAT,
            "backend_id": "pulsar",
            "campaign_id": "pulsar-phase1-screening",
            "case_count": len(rows),
            "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "cases": rows,
        },
    )
    _write_json(args.output_dir / "pulsar_phase1_summary.json", summary)
    _write_csv(
        args.output_dir / "pulsar_phase1_backlog_sensitivity.csv",
        BACKLOG_SENSITIVITY_FIELDS,
        backlog_sensitivity,
    )
    _write_json(
        args.output_dir / "pulsar_phase1_backlog_sensitivity.json",
        {
            "format": "messaging-benchmark.pulsar-phase1-backlog-sensitivity.v1",
            "backend_id": "pulsar",
            "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "fixed_max_flush_duration_sec": FLUSH_LIMIT_SEC,
            "fixed_max_failed_send_percent": FAILED_SEND_LIMIT_PERCENT,
            "thresholds": backlog_sensitivity,
        },
    )
    _write_csv(
        args.output_dir / "pulsar_phase1_controlled_comparisons.csv",
        CONTROLLED_FIELDS,
        controlled,
    )
    _write_csv(
        args.output_dir / "pulsar_phase1_shortlist.csv",
        SHORTLIST_FIELDS,
        shortlist,
    )
    _write_json(
        args.output_dir / "pulsar_phase1_shortlist.json",
        {
            "format": "messaging-benchmark.pulsar-phase1-shortlist.v1",
            "backend_id": "pulsar",
            "selection_status": "retrospectively_corrected_after_campaign_execution",
            "selection_rule": (
                "shared Kafka/Pulsar application-level qualification, category coverage, "
                "then primary qualification-first rank"
            ),
            "qualification_policy": {
                "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
                "backlog_denominator_scope": (
                    "measurement-period publication attempts"
                ),
                "producer_backlog_percent_max": BACKLOG_LIMIT_PERCENT,
                "max_flush_duration_sec_max": FLUSH_LIMIT_SEC,
                "failed_send_percent_max": FAILED_SEND_LIMIT_PERCENT,
            },
            "historically_executed_phase2_anchor_ids": list(
                HISTORICALLY_EXECUTED_PHASE2_ANCHORS
            ),
            "historically_executed_final_validation_ids": list(
                HISTORICALLY_EXECUTED_FINAL_VALIDATION_CONFIGS
            ),
            "historical_execution_note": (
                "This corrected shortlist was derived after Phase 2 and final "
                "validation had run. Only workload IDs present in the historical "
                "manifests were actually repeated; newly selected IDs were not validated."
            ),
            "configuration_count": len(shortlist),
            "configurations": shortlist,
        },
    )
    if not args.machine_only:
        _write_markdown(
            args.output_dir / "pulsar_phase1_report.md",
            rows,
            shortlist,
            controlled,
            summary,
        )
        _write_html(
            args.output_dir / "pulsar_phase1_report.html",
            rows,
            shortlist,
            controlled,
            summary,
            figures,
        )
        _write_latex(
            args.output_dir / "pulsar_phase1_report.tex",
            rows,
            shortlist,
            controlled,
            summary,
            plan,
            validation,
            figures,
        )
    _write_artifact_manifest(args.output_dir)
    print(f"[pulsar-phase1] cases: {len(rows)}")
    print(f"[pulsar-phase1] shortlist: {len(shortlist)}")
    print(f"[pulsar-phase1] output: {args.output_dir}")
    return 0


def _read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {"case_id", "config_id", "design_note", *VARIED_FIELDS}
    missing = required - set(rows[0] if rows else {})
    if missing:
        raise ValueError(
            f"Phase 1 manifest is empty or missing fields: {', '.join(sorted(missing))}"
        )
    return rows


def _discover_reports(
    results_roots: list[Path],
) -> tuple[dict[str, tuple[Path, dict[str, Any], Path]], list[str]]:
    candidates_by_case: dict[
        str, list[tuple[Path, dict[str, Any], Path]]
    ] = {}
    for root in results_roots:
        if not root.exists():
            raise ValueError(f"Results root does not exist: {root}")
        candidates = [root] if root.is_file() else sorted(root.rglob("final_report.json"))
        for path in candidates:
            if path.parent.name in {"data", "reports"}:
                continue
            payload = _read_json(path)
            system = _dict(payload.get("system_under_test"))
            config = _dict(payload.get("config"))
            if str(system.get("backend_id") or config.get("backend_id")) != "pulsar":
                continue
            case_id = str(_dict(payload.get("case")).get("case_id", "")).strip()
            if not case_id:
                continue
            candidates_by_case.setdefault(case_id, []).append((path, payload, root))
    selected: dict[str, tuple[Path, dict[str, Any], Path]] = {}
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
            for _, report, _ in candidates[1:]
        ):
            duplicates.append(case_id)
        selected[case_id] = preferred
    return selected, sorted(duplicates)


def _validate_and_collect(
    *,
    manifest: list[dict[str, str]],
    plan: dict[str, Any],
    reports: dict[str, tuple[Path, dict[str, Any], Path]],
    duplicate_cases: list[str],
    results_roots: list[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failures: list[str] = []
    expected_count = int(plan.get("configuration_count", 0) or 0)
    if expected_count != 120:
        failures.append(f"campaign plan expects {expected_count} cases, not 120")
    if len(manifest) != expected_count:
        failures.append(
            f"manifest contains {len(manifest)} rows, expected {expected_count}"
        )
    case_ids = [row["case_id"] for row in manifest]
    config_ids = [row["config_id"] for row in manifest]
    if len(case_ids) != len(set(case_ids)):
        failures.append("manifest contains duplicate case IDs")
    if len(config_ids) != len(set(config_ids)):
        failures.append("manifest contains duplicate configuration IDs")
    if duplicate_cases:
        failures.append(
            "conflicting completed reports found for case(s): "
            + ", ".join(duplicate_cases)
        )

    expected_profile = str(plan.get("profile_id", ""))
    expected_sha = str(plan.get("profile_sha256", ""))
    rows: list[dict[str, Any]] = []
    missing_reports: list[str] = []
    for manifest_row in manifest:
        case_id = manifest_row["case_id"]
        selected = reports.get(case_id)
        if selected is None:
            missing_reports.append(case_id)
            continue
        path, report, root = selected
        row = _case_row(manifest_row, report, path, root)
        rows.append(row)
        if row["status"] != "completed":
            failures.append(f"{case_id}: status is {row['status']}")
        if row["backend_health"] != "healthy":
            failures.append(f"{case_id}: backend health is {row['backend_health']}")
        if row["profile_id"] != expected_profile:
            failures.append(
                f"{case_id}: profile {row['profile_id']} != {expected_profile}"
            )
        if row["profile_sha256"] != expected_sha:
            failures.append(f"{case_id}: immutable profile checksum mismatch")
        for name in (
            "producer_mib_per_sec",
            "consumer_mib_per_sec",
            "balanced_mib_per_sec",
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
        ):
            value = row[name]
            if value is None or value < 0:
                failures.append(f"{case_id}: invalid {name}={value}")
        if all(
            row[name] is not None
            for name in (
                "producer_mib_per_sec",
                "consumer_mib_per_sec",
                "balanced_mib_per_sec",
            )
        ) and not math.isclose(
            row["balanced_mib_per_sec"],
            min(row["producer_mib_per_sec"], row["consumer_mib_per_sec"]),
            rel_tol=1e-9,
            abs_tol=1e-6,
        ):
            failures.append(f"{case_id}: balanced MiB/s does not equal the minimum")
        if all(
            row[name] is not None
            for name in (
                "producer_records_per_sec",
                "consumer_records_per_sec",
                "balanced_records_per_sec",
            )
        ) and not math.isclose(
            row["balanced_records_per_sec"],
            min(
                row["producer_records_per_sec"],
                row["consumer_records_per_sec"],
            ),
            rel_tol=1e-9,
            abs_tol=1e-6,
        ):
            failures.append(
                f"{case_id}: balanced records/s does not equal the minimum"
            )
        if row["records_delivered"] != (
            row["records_consumed"]
            + row["records_late_drained"]
            + row["records_missing_after_drain"]
        ):
            failures.append(f"{case_id}: delivered-record accounting does not balance")
        if row["eligible"] and any(
            row[name] != 0
            for name in (
                "records_missing_after_drain",
                "duplicate_records",
                "out_of_order_records",
            )
        ):
            failures.append(f"{case_id}: eligible case has a correctness violation")
        if row["eligible"] and not row["latency_valid"]:
            failures.append(f"{case_id}: eligible case has invalid latency")
        expected_qualified = bool(row["eligible"]) and (
            row["producer_backlog_percent"] <= BACKLOG_LIMIT_PERCENT
            and row["max_flush_duration_sec"] <= FLUSH_LIMIT_SEC
            and row["failed_send_percent"] <= FAILED_SEND_LIMIT_PERCENT
        )
        if row["qualified"] is not expected_qualified:
            failures.append(f"{case_id}: qualification result does not match policy")

    if missing_reports:
        failures.append(
            f"missing {len(missing_reports)} required final report(s): "
            + ", ".join(missing_reports[:10])
            + (" ..." if len(missing_reports) > 10 else "")
        )
    extra_cases = sorted(set(reports) - set(case_ids))
    validation = {
        "format": "messaging-benchmark.pulsar-phase1-validation.v1",
        "backend_id": "pulsar",
        "valid": not failures,
        "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "expected_case_count": expected_count,
        "manifest_case_count": len(manifest),
        "observed_required_case_count": len(rows),
        "extra_pulsar_report_count": len(extra_cases),
        "extra_pulsar_case_ids": extra_cases,
        "expected_profile_id": expected_profile,
        "expected_profile_sha256": expected_sha,
        "results_roots": [root.name for root in results_roots],
        "failure_reasons": list(dict.fromkeys(failures)),
    }
    return validation, rows


def _case_row(
    manifest: dict[str, str],
    report: dict[str, Any],
    path: Path,
    root: Path,
) -> dict[str, Any]:
    case = _dict(report.get("case"))
    system = _dict(report.get("system_under_test"))
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    records = _dict(common.get("records"))
    latency = _dict(common.get("latency_end_to_end"))
    qualification = _dict(common.get("qualification"))
    aggregated = _dict(report.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    health = _dict(report.get("backend_health"))
    eligibility = _dict(report.get("eligibility"))
    backend_metrics = _dict(report.get("backend_metrics"))
    pulsar_metrics = _dict(backend_metrics.get("pulsar"))
    monitoring = _dict(pulsar_metrics.get("monitoring"))
    if not monitoring:
        monitoring = _dict(report.get("monitoring"))
    metric_map = {
        str(item.get("id")): item
        for item in monitoring.get("collected_metrics", [])
        if isinstance(item, dict) and item.get("id")
    }

    operational = producer_operational_metrics(
        producers,
        backlog_denominator=BACKLOG_DENOMINATOR_ATTEMPTED,
    )
    attempted = int(operational["messages_attempted"])
    delivered = _integer(producers.get("messages_delivered"))
    failed = int(operational["messages_failed"])
    missing = _integer(records.get("missing"))
    failed_percent = float(operational["failed_send_percent"])
    missing_percent = 100.0 * missing / delivered if delivered > 0 else math.inf
    flush_sec = float(operational["max_flush_duration_sec"])
    eligible = qualification.get("eligible") is True
    qualified = eligible and bool(operational["thresholds_satisfied"])
    reasons = [str(item) for item in eligibility.get("failure_reasons", [])]
    reasons.extend(producer_operational_reasons(operational))
    if not reasons and qualified:
        reasons.append(
            "eligible and shared Kafka/Pulsar application-level backlog, flush, "
            "and failed-send thresholds satisfied"
        )
    elif not reasons:
        reasons.append("not qualified; no structured reason was recorded")

    row: dict[str, Any] = {
        "primary_rank": None,
        "raw_throughput_rank": None,
        "config_id": manifest["config_id"],
        "case_id": manifest["case_id"],
        "design_note": manifest["design_note"],
        "status": str(case.get("status", "unknown")),
        "backend_health": str(health.get("status", "not_recorded")),
        "profile_id": str(system.get("profile_id", "")),
        "profile_sha256": str(system.get("profile_sha256", "")),
        "eligible": eligible,
        "qualified": qualified,
        "qualification_reason": "; ".join(dict.fromkeys(reasons)),
        "producer_mib_per_sec": _optional_number(throughput.get("producer_mib_per_sec")),
        "consumer_mib_per_sec": _optional_number(throughput.get("consumer_mib_per_sec")),
        "balanced_mib_per_sec": _optional_number(throughput.get("balanced_mib_per_sec")),
        "producer_records_per_sec": _optional_number(throughput.get("producer_records_per_sec")),
        "consumer_records_per_sec": _optional_number(throughput.get("consumer_records_per_sec")),
        "balanced_records_per_sec": _optional_number(throughput.get("balanced_records_per_sec")),
        "latency_valid": latency.get("valid") is True,
        "latency_p50_us": _optional_number(latency.get("p50_us")),
        "latency_p95_us": _optional_number(latency.get("p95_us")),
        "latency_p99_us": _optional_number(latency.get("p99_us")),
        "latency_p99_9_us": _optional_number(latency.get("p99_9_us")),
        "latency_mean_us": _optional_number(latency.get("mean_us")),
        "latency_max_us": _optional_number(latency.get("max_us")),
        "latency_sample_count": _integer(latency.get("count")),
        "clock_uncertainty_us_max": _optional_number(
            latency.get("clock_uncertainty_us_max")
        ),
        "clock_drift_us_max": _optional_number(latency.get("clock_drift_us_max")),
        "records_attempted": attempted,
        "messages_attempted": attempted,
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
        "records_delivered": delivered,
        "records_consumed": _integer(records.get("consumed")),
        "records_late_drained": _integer(records.get("late_drained")),
        "records_missing_after_drain": missing,
        "duplicate_records": _integer(records.get("duplicate")),
        "out_of_order_records": _integer(records.get("out_of_order")),
        "missing_after_drain_percent": missing_percent,
        "failed_sends": failed,
        "failed_send_percent": failed_percent,
        "max_flush_duration_sec": flush_sec,
        "monitoring_query_count": _integer(monitoring.get("query_count")),
        "monitoring_completed_query_count": _integer(
            monitoring.get("completed_query_count")
        ),
        "monitoring_missing_metric_count": len(
            [
                item
                for item in monitoring.get("missing_metrics", [])
                if isinstance(item, dict)
            ]
        ),
        "pulsar_bytes_in_peak_mib_per_sec": _bytes_to_mib(
            _metric_number(metric_map, "pulsar_bytes_in_rate", "max_value")
        ),
        "pulsar_bytes_out_peak_mib_per_sec": _bytes_to_mib(
            _metric_number(metric_map, "pulsar_bytes_out_rate", "max_value")
        ),
        "pulsar_service_ib0_rx_peak_mib_per_sec": _decimal_mb_to_mib(
            _metric_number(
                metric_map,
                "pulsar_service_ib0_receive_mbps",
                "max_value",
            )
        ),
        "pulsar_service_ib0_tx_peak_mib_per_sec": _decimal_mb_to_mib(
            _metric_number(
                metric_map,
                "pulsar_service_ib0_transmit_mbps",
                "max_value",
            )
        ),
        "pulsar_process_rss_peak_gib": _bytes_to_gib(
            _metric_number(
                metric_map,
                "pulsar_process_resident_memory_bytes",
                "max_value",
            )
        ),
        "pulsar_jvm_heap_peak_gib": _bytes_to_gib(
            _metric_number(
                metric_map,
                "pulsar_jvm_heap_used_bytes",
                "max_value",
            )
        ),
        "pulsar_jvm_gc_time_rate_peak": _metric_number(
            metric_map,
            "pulsar_jvm_gc_time_rate",
            "max_value",
        ),
        "pulsar_message_backlog_peak": _metric_number(
            metric_map,
            "pulsar_message_backlog",
            "max_value",
        ),
        "source_report": _relative(path, root),
    }
    row.update({field: int(manifest[field]) for field in VARIED_FIELDS})
    return row


def _rank_rows(rows: list[dict[str, Any]]) -> None:
    eligible = [row for row in rows if row["eligible"]]
    ineligible = [row for row in rows if not row["eligible"]]
    eligible.sort(key=_primary_key)
    ineligible.sort(key=lambda row: row["config_id"])
    rows[:] = [*eligible, *ineligible]
    for row in rows:
        row["primary_rank"] = None
        row["raw_throughput_rank"] = None
    for rank, row in enumerate(eligible, start=1):
        row["primary_rank"] = rank
    for rank, row in enumerate(
        sorted(
            eligible,
            key=lambda item: (-item["balanced_mib_per_sec"], item["config_id"]),
        ),
        start=1,
    ):
        row["raw_throughput_rank"] = rank


def _primary_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        not row["qualified"],
        -row["balanced_mib_per_sec"],
        row["producer_backlog_percent"],
        row["max_flush_duration_sec"],
        row["failed_send_percent"],
        row["config_id"],
    )


def _backlog_threshold_sensitivity(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    eligible = [row for row in rows if row["eligible"]]
    output: list[dict[str, Any]] = []
    for threshold in (2.0, 5.0, 10.0):
        qualified = [
            row
            for row in eligible
            if row["producer_backlog_percent"] <= threshold
            and row["max_flush_duration_sec"] <= FLUSH_LIMIT_SEC
            and row["failed_send_percent"] <= FAILED_SEND_LIMIT_PERCENT
        ]
        leader = min(qualified, key=_primary_key) if qualified else None
        output.append(
            {
                "backlog_threshold_percent": threshold,
                "eligible_case_count": len(eligible),
                "qualified_case_count": len(qualified),
                "unqualified_eligible_case_count": len(eligible) - len(qualified),
                "highest_qualified_config_id": (
                    leader["config_id"] if leader is not None else None
                ),
                "highest_qualified_balanced_mib_per_sec": (
                    leader["balanced_mib_per_sec"] if leader is not None else None
                ),
            }
        )
    return output


def select_shortlist(
    rows: list[dict[str, Any]],
    size: int = 10,
) -> list[dict[str, Any]]:
    if size <= 0:
        raise ValueError("shortlist size must be positive")
    selected: dict[str, dict[str, Any]] = {}
    categories: dict[str, list[str]] = {}

    def add(row: dict[str, Any] | None, category: str) -> None:
        if row is None:
            return
        config_id = row["config_id"]
        selected.setdefault(config_id, row)
        categories.setdefault(config_id, [])
        if category not in categories[config_id]:
            categories[config_id].append(category)

    eligible = [row for row in rows if row["eligible"]]
    qualified = [row for row in eligible if row["qualified"]]
    for row in qualified[:3]:
        add(row, "qualified throughput leader")
    add(
        max(eligible, key=lambda row: row["balanced_mib_per_sec"]),
        "raw throughput upper bound",
    )
    add(
        max(eligible, key=lambda row: row["balanced_records_per_sec"]),
        "record-rate leader",
    )
    latency_rows = [row for row in qualified if row["latency_valid"]]
    add(
        min(latency_rows, key=lambda row: row["latency_p99_us"]) if latency_rows else None,
        "qualified p99 latency leader",
    )
    add(_transition_row(eligible), "qualification-boundary reference")
    add(
        next((row for row in eligible if row["design_note"] == "baseline"), None),
        "manifest baseline",
    )
    add(
        next(
            (
                row
                for row in eligible
                if row["design_note"].startswith("one-factor:")
            ),
            None,
        ),
        "controlled one-factor leader",
    )
    add(
        next(
            (
                row
                for row in eligible
                if row["design_note"].startswith("rank-pair:")
            ),
            None,
        ),
        "rank-pair leader",
    )
    for row in eligible:
        if len(selected) >= size:
            break
        add(row, "primary-rank coverage")

    output: list[dict[str, Any]] = []
    for order, row in enumerate(list(selected.values())[:size], start=1):
        output.append(
            {
                "selection_order": order,
                **{field: row.get(field) for field in SHORTLIST_FIELDS if field not in {"selection_order", "selection_categories"}},
                "selection_categories": "; ".join(categories[row["config_id"]]),
            }
        )
    return output


def _transition_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    unqualified = [row for row in rows if not row["qualified"]]
    if unqualified:
        return min(
            unqualified,
            key=lambda row: (
                max(0.0, row["failed_send_percent"] - 0.1) / 0.1
                + max(
                    0.0,
                    row["producer_backlog_percent"] - BACKLOG_LIMIT_PERCENT,
                )
                / BACKLOG_LIMIT_PERCENT
                + max(0.0, row["max_flush_duration_sec"] - 10.0) / 10.0,
                -row["balanced_mib_per_sec"],
                row["config_id"],
            ),
        )
    qualified = [row for row in rows if row["qualified"]]
    return max(
        qualified,
        key=lambda row: (
            max(
                row["failed_send_percent"] / 0.1,
                row["producer_backlog_percent"] / BACKLOG_LIMIT_PERCENT,
                row["max_flush_duration_sec"] / 10.0,
            ),
            row["balanced_mib_per_sec"],
        ),
        default=None,
    )


def _controlled_comparisons(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    baseline = next(
        (row for row in rows if row["design_note"] == "baseline"),
        None,
    )
    if baseline is None:
        raise ValueError("Phase 1 results do not contain the manifest baseline")
    output: list[dict[str, Any]] = []
    for row in rows:
        note = str(row["design_note"])
        if not note.startswith("one-factor:"):
            continue
        parameter, raw_value = note.split(":", 1)[1].split("=", 1)
        tested_value = int(raw_value)
        throughput_delta = (
            row["balanced_mib_per_sec"] - baseline["balanced_mib_per_sec"]
        )
        output.append(
            {
                "config_id": row["config_id"],
                "parameter": parameter,
                "parameter_label": PARAMETER_LABELS.get(parameter, parameter),
                "baseline_value": baseline[parameter],
                "tested_value": tested_value,
                "balanced_mib_per_sec": row["balanced_mib_per_sec"],
                "baseline_balanced_mib_per_sec": baseline[
                    "balanced_mib_per_sec"
                ],
                "balanced_mib_per_sec_delta": throughput_delta,
                "balanced_mib_per_sec_change_percent": (
                    100.0
                    * throughput_delta
                    / baseline["balanced_mib_per_sec"]
                ),
                "balanced_records_per_sec": row["balanced_records_per_sec"],
                "baseline_balanced_records_per_sec": baseline[
                    "balanced_records_per_sec"
                ],
                "balanced_records_per_sec_delta": (
                    row["balanced_records_per_sec"]
                    - baseline["balanced_records_per_sec"]
                ),
                "latency_p99_us": row["latency_p99_us"],
                "baseline_latency_p99_us": baseline["latency_p99_us"],
                "latency_p99_us_delta": (
                    row["latency_p99_us"] - baseline["latency_p99_us"]
                ),
                "qualified": row["qualified"],
            }
        )
    return sorted(
        output,
        key=lambda item: (
            -abs(item["balanced_mib_per_sec_delta"]),
            item["config_id"],
        ),
    )


def _analysis_summary(
    rows: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    validation: dict[str, Any],
    plan: dict[str, Any],
    backlog_sensitivity: list[dict[str, Any]],
) -> dict[str, Any]:
    eligible_rows = [row for row in rows if row["eligible"]]
    qualified_rows = [row for row in eligible_rows if row["qualified"]]
    valid_latency_rows = [row for row in qualified_rows if row["latency_valid"]]
    throughput_values = [row["balanced_mib_per_sec"] for row in eligible_rows]
    latency_values = [
        row["latency_p99_us"] for row in eligible_rows if row["latency_valid"]
    ]
    leader = min(eligible_rows, key=_primary_key)
    raw_leader = max(eligible_rows, key=lambda row: row["balanced_mib_per_sec"])
    record_leader = max(
        eligible_rows,
        key=lambda row: row["balanced_records_per_sec"],
    )
    latency_leader = min(valid_latency_rows, key=lambda row: row["latency_p99_us"])
    baseline = next(row for row in rows if row["design_note"] == "baseline")
    unqualified = [row for row in rows if not row["qualified"]]
    return {
        "format": "messaging-benchmark.pulsar-phase1-summary.v1",
        "backend_id": "pulsar",
        "campaign_id": "pulsar-phase1-screening",
        "screening_status": "complete-single-observation-screening",
        "ranking_prerequisite": "scientifically eligible observation",
        "primary_ranking_rule": [
            "qualified before unqualified",
            "balanced MiB/s descending",
            "producer backlog percent ascending",
            "maximum flush duration ascending",
            "failed-send percent ascending",
            "configuration ID ascending",
        ],
        "secondary_metrics": [
            "sampled producer-to-consumer p99 latency",
        ],
        "case_count": len(rows),
        "completed_case_count": sum(row["status"] == "completed" for row in rows),
        "eligible_case_count": sum(bool(row["eligible"]) for row in rows),
        "qualified_case_count": sum(bool(row["qualified"]) for row in rows),
        "latency_valid_case_count": sum(bool(row["latency_valid"]) for row in rows),
        "unqualified_case_count": len(unqualified),
        "unqualified_configurations": [
            {
                "config_id": row["config_id"],
                "reason": row["qualification_reason"],
                "balanced_mib_per_sec": row["balanced_mib_per_sec"],
                "latency_p99_us": row["latency_p99_us"],
                "producer_backlog_percent": row["producer_backlog_percent"],
                "max_flush_duration_sec": row["max_flush_duration_sec"],
                "failed_send_percent": row["failed_send_percent"],
            }
            for row in unqualified
        ],
        "qualification_policy": {
            "policy_id": COMMON_QUALIFICATION_POLICY_ID,
            "interpretation": (
                "Shared Kafka/Pulsar application-level operational thresholds"
            ),
            "backlog_denominator": BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "producer_backlog_percent_max": BACKLOG_LIMIT_PERCENT,
            "max_flush_duration_sec_max": FLUSH_LIMIT_SEC,
            "failed_send_percent_max": FAILED_SEND_LIMIT_PERCENT,
            "backlog_exceeded_case_count": sum(
                row["producer_backlog_percent"] > BACKLOG_LIMIT_PERCENT
                for row in rows
            ),
            "flush_exceeded_case_count": sum(
                row["max_flush_duration_sec"] > FLUSH_LIMIT_SEC for row in rows
            ),
            "failed_send_exceeded_case_count": sum(
                row["failed_send_percent"] > FAILED_SEND_LIMIT_PERCENT
                for row in rows
            ),
        },
        "backlog_threshold_sensitivity": backlog_sensitivity,
        "primary_leader": _summary_case(leader),
        "raw_throughput_leader": _summary_case(raw_leader),
        "record_rate_leader": _summary_case(record_leader),
        "qualified_p99_latency_leader": _summary_case(latency_leader),
        "manifest_baseline": _summary_case(baseline),
        "throughput_mib_per_sec": {
            "minimum": min(throughput_values),
            "median": statistics.median(throughput_values),
            "maximum": max(throughput_values),
            "q1": _quartile(throughput_values, 0.25),
            "q3": _quartile(throughput_values, 0.75),
        },
        "latency_p99_us": {
            "minimum": min(latency_values),
            "median": statistics.median(latency_values),
            "maximum": max(latency_values),
        },
        "record_accounting": {
            "attempted": sum(row["records_attempted"] for row in rows),
            "delivered": sum(row["records_delivered"] for row in rows),
            "consumed_during_measurement": sum(
                row["records_consumed"] for row in rows
            ),
            "late_drained": sum(row["records_late_drained"] for row in rows),
            "missing_after_drain": sum(
                row["records_missing_after_drain"] for row in rows
            ),
            "duplicates": sum(row["duplicate_records"] for row in rows),
            "out_of_order": sum(row["out_of_order_records"] for row in rows),
            "failed_sends": sum(row["failed_sends"] for row in rows),
        },
        "clock_validation": {
            "maximum_uncertainty_us": max(
                row["clock_uncertainty_us_max"] or 0.0 for row in rows
            ),
            "maximum_drift_us": max(
                row["clock_drift_us_max"] or 0.0 for row in rows
            ),
            "configured_limit_us": 250.0,
        },
        "monitoring": {
            "cases_with_monitoring": sum(
                row["monitoring_completed_query_count"] > 0 for row in rows
            ),
            "query_count_per_case": sorted(
                set(row["monitoring_query_count"] for row in rows)
            ),
            "completed_query_count_per_case": sorted(
                set(row["monitoring_completed_query_count"] for row in rows)
            ),
            "optional_missing_metric_count_per_case": sorted(
                set(row["monitoring_missing_metric_count"] for row in rows)
            ),
        },
        "shortlist_config_ids": [row["config_id"] for row in shortlist],
        "shortlist_status": "retrospectively_corrected_after_campaign_execution",
        "historically_executed_phase2_anchor_ids": list(
            HISTORICALLY_EXECUTED_PHASE2_ANCHORS
        ),
        "historically_executed_final_validation_ids": list(
            HISTORICALLY_EXECUTED_FINAL_VALIDATION_CONFIGS
        ),
        "profile_id": plan["profile_id"],
        "profile_sha256": plan["profile_sha256"],
        "validation_format": validation["format"],
        "validation_passed": validation["valid"],
    }


def _summary_case(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "config_id": row["config_id"],
        "primary_rank": row["primary_rank"],
        "eligible": row["eligible"],
        "qualified": row["qualified"],
        "balanced_mib_per_sec": row["balanced_mib_per_sec"],
        "balanced_records_per_sec": row["balanced_records_per_sec"],
        "latency_p99_us": row["latency_p99_us"],
        "producer_backlog_percent": row["producer_backlog_percent"],
        "max_flush_duration_sec": row["max_flush_duration_sec"],
        "payload_size_bytes": row["payload_size_bytes"],
        "producer_ranks": row["producer_ranks"],
        "consumer_ranks": row["consumer_ranks"],
        "partitions": row["partitions"],
    }


def _write_figures(
    output_dir: Path,
    rows: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    controlled: list[dict[str, Any]],
) -> dict[str, dict[str, str]]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

    figure_dir = output_dir / "figures"
    figure_dir.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "legend.fontsize": 8,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.bbox": "tight",
        }
    )
    paths: dict[str, dict[str, str]] = {}

    fig, ax = plt.subplots(figsize=(10.4, 5.2))
    ax.set_xlim(0, 10.4)
    ax.set_ylim(0, 5.2)
    ax.axis("off")

    def box(
        x: float,
        y: float,
        width: float,
        height: float,
        title: str,
        lines: list[str],
    ) -> None:
        patch = FancyBboxPatch(
            (x, y),
            width,
            height,
            boxstyle="round,pad=0.04,rounding_size=0.04",
            linewidth=1.1,
            edgecolor="#173f73",
            facecolor="#f7f9fc",
        )
        ax.add_patch(patch)
        ax.text(x + 0.18, y + height - 0.34, title, weight="bold", fontsize=10)
        ax.text(
            x + 0.18,
            y + height - 0.72,
            "\n".join(f"- {line}" for line in lines),
            va="top",
            linespacing=1.35,
        )

    box(
        0.35,
        2.9,
        2.65,
        1.75,
        "Producer / controller node",
        ["MPI controller", "16-64 producer ranks", "Python pulsar-client"],
    )
    box(
        3.85,
        2.9,
        2.7,
        1.75,
        "Pulsar service node",
        ["One standalone process", "Broker + BookKeeper + metadata", "RAM-backed storage"],
    )
    box(
        7.4,
        2.9,
        2.65,
        1.75,
        "Consumer node",
        ["16-64 consumer ranks", "Shared subscription", "Coordinated drain"],
    )
    box(
        3.85,
        0.45,
        2.7,
        1.45,
        "Monitoring node",
        ["Prometheus", "Native Pulsar metrics", "Node and process metrics"],
    )
    for start, end, label in (
        ((3.0, 3.75), (3.85, 3.75), "publish"),
        ((6.55, 3.75), (7.4, 3.75), "consume"),
    ):
        ax.add_patch(
            FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=14, color="#173f73")
        )
        ax.text((start[0] + end[0]) / 2, 3.95, label, ha="center", color="#173f73")
    for x in (1.7, 5.2, 8.7):
        ax.add_patch(
            FancyArrowPatch(
                (x, 2.9),
                (5.3, 1.9),
                arrowstyle="-|>",
                mutation_scale=12,
                linestyle="--",
                linewidth=1.0,
                color="#555555",
            )
        )
    ax.text(
        5.2,
        0.08,
        "Each case restarts the standalone service, creates one partitioned topic, "
        "runs simultaneous load, drains, collects evidence, and clears storage.",
        ha="center",
        fontsize=8.5,
    )
    paths["architecture"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_architecture"
    )

    qualified_values = [
        row["balanced_mib_per_sec"] for row in rows if row["qualified"]
    ]
    unqualified_values = [
        row["balanced_mib_per_sec"] for row in rows if not row["qualified"]
    ]
    fig, ax = plt.subplots(figsize=(7.2, 4.2))
    bins = 18
    ax.hist(
        [qualified_values, unqualified_values],
        bins=bins,
        color=["#1f6f50", "#b42318"],
        alpha=0.82,
        stacked=True,
        label=(
            f"qualified (n={len(qualified_values)})",
            f"eligible, unqualified (n={len(unqualified_values)})",
        ),
    )
    ax.set_xlabel("Balanced application throughput (MiB/s)")
    ax.set_ylabel("Configuration count")
    ax.legend()
    ax.grid(axis="y", alpha=0.2)
    paths["throughput_distribution"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_throughput_distribution"
    )

    shortlist_ids = {row["config_id"] for row in shortlist}
    fig, ax = plt.subplots(figsize=(6.4, 5.4))
    for qualified, color, label in (
        (True, "#1f6f50", "qualified"),
        (False, "#b42318", "unqualified"),
    ):
        selected = [row for row in rows if row["qualified"] is qualified]
        ax.scatter(
            [row["producer_mib_per_sec"] for row in selected],
            [row["consumer_mib_per_sec"] for row in selected],
            s=27,
            alpha=0.78,
            color=color,
            label=label,
        )
    limit = max(
        max(row["producer_mib_per_sec"], row["consumer_mib_per_sec"])
        for row in rows
    )
    ax.plot([0, limit], [0, limit], color="#555555", linestyle="--", linewidth=1)
    for row in rows:
        if row["config_id"] in shortlist_ids:
            ax.annotate(
                row["config_id"],
                (row["producer_mib_per_sec"], row["consumer_mib_per_sec"]),
                xytext=(4, 4),
                textcoords="offset points",
                fontsize=7,
            )
    ax.set_xlabel("Producer delivered throughput (MiB/s)")
    ax.set_ylabel("Consumer received throughput (MiB/s)")
    ax.legend()
    ax.grid(alpha=0.2)
    paths["throughput_balance"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_producer_consumer_balance"
    )

    fig, ax = plt.subplots(figsize=(7.4, 4.8))
    for qualified, color, label in (
        (True, "#1f6f50", "qualified"),
        (False, "#b42318", "eligible, unqualified"),
    ):
        selected = [
            row
            for row in rows
            if row["eligible"] and row["qualified"] is qualified
        ]
        ax.scatter(
            [row["producer_backlog_percent"] for row in selected],
            [row["balanced_mib_per_sec"] for row in selected],
            s=30,
            alpha=0.8,
            color=color,
            label=label,
        )
    ax.axvline(
        BACKLOG_LIMIT_PERCENT,
        color="#1d4ed8",
        linestyle="--",
        linewidth=1.3,
        label="5% backlog threshold",
    )
    label_offsets = ((6, 8), (6, -13), (-38, 8), (-38, -13))
    labelled = [row for row in rows if row["config_id"] in shortlist_ids]
    for index, row in enumerate(labelled):
        ax.annotate(
            row["config_id"],
            (row["producer_backlog_percent"], row["balanced_mib_per_sec"]),
            xytext=label_offsets[index % len(label_offsets)],
            textcoords="offset points",
            fontsize=7,
            arrowprops={"arrowstyle": "-", "color": "#6b7280", "linewidth": 0.45},
        )
    ax.set_xlabel("Producer backlog at flush start (%)")
    ax.set_ylabel("Balanced application throughput (MiB/s)")
    ax.legend()
    ax.grid(alpha=0.2)
    paths["throughput_backlog"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_throughput_backlog"
    )

    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    for qualified, color, label in (
        (True, "#1f6f50", "qualified"),
        (False, "#b42318", "unqualified"),
    ):
        selected = [row for row in rows if row["qualified"] is qualified]
        ax.scatter(
            [row["balanced_mib_per_sec"] for row in selected],
            [row["latency_p99_us"] / 1_000_000.0 for row in selected],
            s=30,
            alpha=0.8,
            color=color,
            label=label,
        )
    for row in rows:
        if row["config_id"] in shortlist_ids:
            ax.annotate(
                row["config_id"],
                (row["balanced_mib_per_sec"], row["latency_p99_us"] / 1_000_000.0),
                xytext=(4, 4),
                textcoords="offset points",
                fontsize=7,
            )
    ax.set_xlabel("Balanced application throughput (MiB/s)")
    ax.set_ylabel("Sampled end-to-end p99 latency (s)")
    ax.legend()
    ax.grid(alpha=0.2)
    paths["throughput_latency"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_throughput_latency"
    )

    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    payloads = sorted({row["payload_size_bytes"] for row in rows})
    colors = plt.get_cmap("viridis")
    for index, payload in enumerate(payloads):
        selected = [row for row in rows if row["payload_size_bytes"] == payload]
        ax.scatter(
            [row["balanced_mib_per_sec"] for row in selected],
            [row["balanced_records_per_sec"] for row in selected],
            s=29,
            alpha=0.76,
            color=colors(index / max(1, len(payloads) - 1)),
            label=f"{payload:,} B",
        )
    for row in rows:
        if row["config_id"] in shortlist_ids:
            ax.annotate(
                row["config_id"],
                (row["balanced_mib_per_sec"], row["balanced_records_per_sec"]),
                xytext=(4, 4),
                textcoords="offset points",
                fontsize=7,
            )
    ax.set_xlabel("Balanced application throughput (MiB/s)")
    ax.set_ylabel("Balanced record throughput (records/s)")
    ax.legend(title="Payload", ncols=2)
    ax.grid(alpha=0.2)
    paths["payload_tradeoff"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_payload_tradeoff"
    )

    top_controlled = controlled[:15]
    fig, ax = plt.subplots(figsize=(8.0, 5.4))
    labels = [
        f"{item['config_id']}: {item['parameter_label']}={item['tested_value']:,}"
        for item in reversed(top_controlled)
    ]
    values = [
        item["balanced_mib_per_sec_delta"] for item in reversed(top_controlled)
    ]
    ax.barh(
        range(len(values)),
        values,
        color=["#1f6f50" if value >= 0 else "#b42318" for value in values],
    )
    ax.set_yticks(range(len(labels)), labels=labels)
    ax.axvline(0, color="#333333", linewidth=0.9)
    ax.set_xlabel("Balanced-throughput change from cfg_001 (MiB/s)")
    ax.grid(axis="x", alpha=0.2)
    paths["controlled_effects"] = _save_figure(
        fig, figure_dir / "pulsar_phase1_controlled_effects"
    )
    return paths


def _save_figure(fig: Any, base_path: Path) -> dict[str, str]:
    import matplotlib.pyplot as plt

    pdf_path = base_path.with_suffix(".pdf")
    png_path = base_path.with_suffix(".png")
    fig.tight_layout()
    fig.savefig(pdf_path)
    fig.savefig(png_path, dpi=180)
    plt.close(fig)
    return {
        "pdf": pdf_path.relative_to(base_path.parent.parent).as_posix(),
        "png": png_path.relative_to(base_path.parent.parent).as_posix(),
    }


def _figure_paths(output_dir: Path) -> dict[str, dict[str, str]]:
    stems = {
        "architecture": "pulsar_phase1_architecture",
        "throughput_distribution": "pulsar_phase1_throughput_distribution",
        "throughput_balance": "pulsar_phase1_producer_consumer_balance",
        "throughput_backlog": "pulsar_phase1_throughput_backlog",
        "throughput_latency": "pulsar_phase1_throughput_latency",
        "payload_tradeoff": "pulsar_phase1_payload_tradeoff",
        "controlled_effects": "pulsar_phase1_controlled_effects",
    }
    return {
        key: {
            "pdf": (Path("figures") / f"{stem}.pdf").as_posix(),
            "png": (Path("figures") / f"{stem}.png").as_posix(),
        }
        for key, stem in stems.items()
    }


def _write_markdown(
    path: Path,
    rows: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    controlled: list[dict[str, Any]],
    summary: dict[str, Any],
) -> None:
    leader = summary["primary_leader"]
    record_leader = summary["record_rate_leader"]
    latency_leader = summary["qualified_p99_latency_leader"]
    accounting = summary["record_accounting"]
    exceptions = summary["unqualified_configurations"]
    policy = summary["qualification_policy"]
    sensitivity = summary["backlog_threshold_sensitivity"]
    if exceptions:
        exception_summary = (
            f"{len(exceptions)} cases remained eligible and correct but did not "
            f"qualify: {policy['backlog_exceeded_case_count']} exceeded the 5% "
            f"producer-backlog limit, {policy['flush_exceeded_case_count']} exceeded "
            "the 10-second flush limit, and "
            f"{policy['failed_send_exceeded_case_count']} exceeded the 0.1% "
            "failed-send limit. A case can exceed more than one threshold; exact "
            "evidence remains in the case table and validation artifacts."
        )
    else:
        exception_summary = "Every eligible case met all qualification thresholds."
    lines = [
        "# Verified Exploratory Analysis of Apache Pulsar Phase 1",
        "",
        "## Executive Summary",
        "",
        (
            "This report analyzes 120 completed single observations under the fixed "
            "`BASELINE_H16_D32` standalone Pulsar profile. It is a qualification-first "
            "workload screen and does not establish repeatability or a universal "
            "Pulsar optimum. The shortlist in this regenerated report is a "
            "retrospective correction; later campaigns had already executed a "
            "different historical workload set."
        ),
        "",
        f"- Complete cases: {summary['completed_case_count']} / 120",
        f"- Eligible and latency-valid cases: {summary['eligible_case_count']} / 120",
        f"- Qualified cases: {summary['qualified_case_count']} / 120",
        f"- Highest qualified observation: `{leader['config_id']}` at "
        f"{leader['balanced_mib_per_sec']:.3f} MiB/s",
        f"- Highest record rate: `{record_leader['config_id']}` at "
        f"{record_leader['balanced_records_per_sec']:,.0f} records/s",
        f"- Lowest qualified p99: `{latency_leader['config_id']}` at "
        f"{latency_leader['latency_p99_us'] / 1_000_000:.3f} s",
        f"- Missing, duplicate, out-of-order, and failed records: "
        f"{accounting['missing_after_drain']}, {accounting['duplicates']}, "
        f"{accounting['out_of_order']}, and {accounting['failed_sends']}",
        "",
        "## Architecture And Method",
        "",
        "![Four-node Pulsar benchmark architecture](figures/pulsar_phase1_architecture.png)",
        "",
        (
            "Each case used an exclusive four-node allocation: one standalone Pulsar "
            "service node, one producer/controller node, one consumer node, and one "
            "monitoring node. The service combined broker, BookKeeper, and metadata "
            "roles. Each case used 15 seconds of excluded warm-up, 30 seconds of "
            "simultaneous measurement, and up to 60 seconds of coordinated post-flush "
            "drain. One in ten records carried sampled end-to-end latency evidence."
        ),
        "",
        "## Qualification And Verification",
        "",
        (
            "A case qualifies only when pending messages at producer flush start are "
            "at most 5% of measurement-period publication attempts, maximum "
            "producer flush is at most 10 "
            "seconds, and failed publications are at most 0.1% of attempted "
            "publications. Eligibility separately requires healthy complete execution, "
            "valid sampled latency, and zero post-drain missing, duplicate, and "
            "out-of-order records. Eligible qualified cases rank first by balanced "
            "MiB/s, then lower producer backlog, shorter flush time, lower failed-send "
            "percentage, and configuration ID. p99 latency is reported as a secondary "
            "metric and does not affect the Phase 1 primary rank."
        ),
        "",
        exception_summary + " All cases had valid clock-calibrated latency and exact "
        "post-drain record accounting.",
        "",
        "### Backlog Threshold Sensitivity",
        "",
        (
            "Only the backlog threshold changes in this sensitivity check. The flush "
            "and failed-send limits remain fixed at 10 seconds and 0.1%, respectively."
        ),
        "",
        "| Backlog limit % | Eligible cases | Qualified cases | Unqualified eligible | Highest qualified config | Highest qualified MiB/s |",
        "|---:|---:|---:|---:|---|---:|",
    ]
    for item in sensitivity:
        lines.append(
            f"| {item['backlog_threshold_percent']:.0f} | "
            f"{item['eligible_case_count']} | {item['qualified_case_count']} | "
            f"{item['unqualified_eligible_case_count']} | "
            f"`{item['highest_qualified_config_id']}` | "
            f"{item['highest_qualified_balanced_mib_per_sec']:.3f} |"
        )
    lines.extend(
        [
        "",
        "## Primary Ranking",
        "",
        "| Rank | Config | Eligible | Qualified | Balanced MiB/s | Records/s | p99 us | Backlog % | Missing % | Flush s | Failed % |",
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in rows[:20]:
        lines.append(_md_result_row(row))
    lines.extend(
        [
            "",
            "The complete 120-row ranking and all eleven varied fields are available "
            "in `pulsar_phase1_cases.csv` and `.json`.",
            "",
            "![Throughput distribution](figures/pulsar_phase1_throughput_distribution.png)",
            "",
            "![Balanced throughput versus producer backlog](figures/pulsar_phase1_throughput_backlog.png)",
            "",
            "![Throughput and latency](figures/pulsar_phase1_throughput_latency.png)",
            "",
            "## Controlled One-Factor Findings",
            "",
            "Each row changes exactly one field from `cfg_001`. The values are single-run "
            "screening contrasts, so large changes identify useful Phase 2 workload "
            "directions but do not provide confidence intervals.",
            "",
            "| Config | Parameter | Tested | Delta MiB/s | Change % | Delta p99 s |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for item in controlled[:15]:
        lines.append(
            f"| `{item['config_id']}` | {item['parameter_label']} | "
            f"{item['tested_value']:,} | {item['balanced_mib_per_sec_delta']:+.3f} | "
            f"{item['balanced_mib_per_sec_change_percent']:+.2f}% | "
            f"{item['latency_p99_us_delta'] / 1_000_000:+.3f} |"
        )
    lines.extend(
        [
            "",
            "![Largest controlled effects](figures/pulsar_phase1_controlled_effects.png)",
            "",
            "## Retrospectively Corrected Phase 1 Shortlist",
            "",
            "| Order | Config | Categories | Rank | Qualified | MiB/s | Records/s | p99 us |",
            "|---:|---|---|---:|---|---:|---:|---:|",
        ]
    )
    for row in shortlist:
        lines.append(
            f"| {row['selection_order']} | `{row['config_id']}` | "
            f"{row['selection_categories']} | {row['primary_rank']} | "
            f"{_yes_no(row['qualified'])} | {row['balanced_mib_per_sec']:.3f} | "
            f"{row['balanced_records_per_sec']:,.0f} | {_fmt(row['latency_p99_us'], 3)} |"
        )
    lines.extend(
        [
            "",
            "This shortlist is the result of applying the corrected backlog rule after "
            "Phase 2 and final validation had already run. It is not the workload set "
            "that was historically executed, and newly selected configurations were "
            "not repeated. The complete report identifies the historical Phase 2 "
            "anchors and final-validation configurations separately.",
            "",
            "## Monitoring Scope",
            "",
            "All 120 cases retained 24 of 31 monitoring queries with five healthy "
            "Prometheus targets. The seven consistently unavailable optional metrics "
            "were total direct-memory usage and six BookKeeper queue/cache metrics. "
            "The retained summaries are sparse broad-window samples and therefore "
            "support bottleneck diagnosis but do not override application-level "
            "qualification or establish synchronized resource efficiency.",
            "",
            "## Metric Definitions",
            "",
            "- **MiB/s:** bytes per second divided by 1,048,576.",
            "- **Balanced throughput:** minimum of producer and consumer throughput.",
            "- **Records/s:** successful application records per measurement second.",
            "- **p99:** 99th percentile sampled producer-to-consumer latency in microseconds.",
            "- **Backlog %:** pending messages at producer flush start divided by measurement-period publication attempts.",
            "- **Missing %:** records absent after drain divided by callback-confirmed publications.",
            "- **Flush s:** maximum producer-client flush duration across producer ranks.",
            "- **Failed %:** failed publications divided by attempted publications.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_html(
    path: Path,
    rows: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    controlled: list[dict[str, Any]],
    summary: dict[str, Any],
    figures: dict[str, dict[str, str]],
) -> None:
    report_rows = "".join(
        "<tr>"
        f"<td>{row['primary_rank']}</td><td>{html.escape(row['config_id'])}</td>"
        f"<td>{_yes_no(row['eligible'])}</td><td>{_yes_no(row['qualified'])}</td>"
        f"<td>{row['balanced_mib_per_sec']:.3f}</td>"
        f"<td>{row['balanced_records_per_sec']:,.0f}</td>"
        f"<td>{_fmt(row['latency_p99_us'], 3)}</td>"
        f"<td>{row['producer_backlog_percent']:.6f}</td>"
        f"<td>{row['missing_after_drain_percent']:.6f}</td>"
        f"<td>{row['max_flush_duration_sec']:.3f}</td>"
        f"<td>{row['failed_send_percent']:.6f}</td>"
        "</tr>"
        for row in rows
    )
    sensitivity_rows = "".join(
        "<tr>"
        f"<td>{item['backlog_threshold_percent']:.0f}</td>"
        f"<td>{item['eligible_case_count']}</td>"
        f"<td>{item['qualified_case_count']}</td>"
        f"<td>{item['unqualified_eligible_case_count']}</td>"
        f"<td>{html.escape(str(item['highest_qualified_config_id']))}</td>"
        f"<td>{item['highest_qualified_balanced_mib_per_sec']:.3f}</td>"
        "</tr>"
        for item in summary["backlog_threshold_sensitivity"]
    )
    shortlist_rows = "".join(
        "<tr>"
        f"<td>{row['selection_order']}</td>"
        f"<td>{html.escape(row['config_id'])}</td>"
        f"<td>{html.escape(row['selection_categories'])}</td>"
        f"<td>{row['primary_rank']}</td>"
        f"<td>{row['balanced_mib_per_sec']:.3f}</td>"
        "</tr>"
        for row in shortlist
    )
    controlled_rows = "".join(
        "<tr>"
        f"<td>{html.escape(item['config_id'])}</td>"
        f"<td>{html.escape(item['parameter_label'])}</td>"
        f"<td>{item['tested_value']:,}</td>"
        f"<td>{item['balanced_mib_per_sec_delta']:+.3f}</td>"
        f"<td>{item['balanced_mib_per_sec_change_percent']:+.2f}%</td>"
        f"<td>{item['latency_p99_us_delta'] / 1_000_000:+.3f}</td>"
        "</tr>"
        for item in controlled[:15]
    )
    leader = summary["primary_leader"]
    content = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Pulsar Phase 1</title>
<style>body{{font:15px sans-serif;max-width:1200px;margin:2rem auto;line-height:1.5}}
table{{border-collapse:collapse;width:100%;margin:1rem 0}}th,td{{border:1px solid #999;padding:.35rem;text-align:right}}
th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3){{text-align:left}}
img{{max-width:100%;height:auto;margin:1rem 0}}</style></head>
<body><h1>Verified Exploratory Analysis of Apache Pulsar Phase 1</h1>
<p>This fixed-profile, 120-case single-observation screen is not a repeatability or universal-optimum claim. Its corrected shortlist was computed retrospectively and is distinct from the workloads historically executed in Phase 2 and final validation.</p>
<p><strong>Primary observation:</strong> {html.escape(leader['config_id'])} at {leader['balanced_mib_per_sec']:.3f} MiB/s. All 120 cases were complete, eligible, and latency-valid; {summary['qualified_case_count']} qualified.</p>
<h2>Architecture and method</h2><img src="{figures['architecture']['png']}" alt="Four-node Pulsar benchmark architecture">
<h2>Backlog threshold sensitivity</h2><p>Only the producer-backlog limit changes; flush remains &le; 10 s and failed sends remain &le; 0.1%.</p><table><thead><tr><th>Backlog limit %</th><th>Eligible</th><th>Qualified</th><th>Unqualified eligible</th><th>Highest qualified config</th><th>Highest qualified MiB/s</th></tr></thead><tbody>{sensitivity_rows}</tbody></table>
<h2>Primary ranking</h2><p>Qualification requires producer backlog at flush start &le; 5%, flush &le; 10 s, and failed sends &le; 0.1%. Missing, duplicate, and out-of-order records are eligibility checks, not qualification thresholds. Eligible cases rank by qualification, balanced throughput, backlog, flush, failed sends, and configuration ID; p99 is secondary.</p><table><thead><tr><th>Rank</th><th>Config</th><th>Eligible</th><th>Qualified</th><th>Balanced MiB/s</th><th>Records/s</th><th>p99 us</th><th>Backlog %</th><th>Missing %</th><th>Flush s</th><th>Failed %</th></tr></thead><tbody>{report_rows}</tbody></table>
<img src="{figures['throughput_backlog']['png']}" alt="Balanced throughput versus producer backlog with 5 percent threshold">
<img src="{figures['throughput_latency']['png']}" alt="Balanced throughput versus p99 latency">
<h2>Controlled one-factor findings</h2><table><thead><tr><th>Config</th><th>Parameter</th><th>Tested value</th><th>Delta MiB/s</th><th>Change</th><th>Delta p99 s</th></tr></thead><tbody>{controlled_rows}</tbody></table>
<h2>Retrospectively corrected Phase 1 shortlist</h2><p>This corrected list was not the historical Phase 2/final-validation execution set; newly selected configurations were not validated.</p><table><thead><tr><th>Order</th><th>Config</th><th>Categories</th><th>Rank</th><th>MiB/s</th></tr></thead><tbody>{shortlist_rows}</tbody></table>
<p>MiB/s uses 1,048,576 bytes. Balanced throughput is the minimum of producer and consumer throughput. p99 is sampled end-to-end producer-to-consumer latency. The 43 one-factor comparisons are single observations and must not be interpreted as confidence-bounded causal estimates.</p></body></html>"""
    path.write_text(content, encoding="utf-8")


def _write_latex(
    path: Path,
    rows: list[dict[str, Any]],
    shortlist: list[dict[str, Any]],
    controlled: list[dict[str, Any]],
    summary: dict[str, Any],
    plan: dict[str, Any],
    validation: dict[str, Any],
    figures: dict[str, dict[str, str]],
) -> None:
    leader = next(row for row in rows if row["primary_rank"] == 1)
    record_leader = max(rows, key=lambda row: row["balanced_records_per_sec"])
    latency_leader = min(
        (row for row in rows if row["qualified"] and row["latency_valid"]),
        key=lambda row: row["latency_p99_us"],
    )
    baseline = next(row for row in rows if row["design_note"] == "baseline")
    unqualified = [row for row in rows if not row["qualified"]]
    accounting = summary["record_accounting"]
    clock = summary["clock_validation"]
    policy = summary["qualification_policy"]
    sensitivity_by_limit = {
        int(item["backlog_threshold_percent"]): item
        for item in summary["backlog_threshold_sensitivity"]
    }

    parameter_rows = "\n".join(
        f"{_tex(PARAMETER_LABELS[field])} & "
        f"{_tex(', '.join(f'{value:,}' for value in sorted({row[field] for row in rows})))} & "
        f"{baseline[field]:,} \\\\"
        for field in VARIED_FIELDS
    )
    ranking_rows = "\n".join(
        f"{row['primary_rank']} & \\texttt{{{_tex(row['config_id'])}}} & "
        f"{_status_label(row)} & {row['producer_mib_per_sec']:.3f} & "
        f"{row['consumer_mib_per_sec']:.3f} & {row['balanced_mib_per_sec']:.3f} & "
        f"{row['balanced_records_per_sec']:,.0f} & "
        f"{row['latency_p99_us'] / 1_000_000:.3f} & "
        f"{row['producer_backlog_percent']:.3f} & "
        f"{row['max_flush_duration_sec']:.3f} & "
        f"{row['failed_send_percent']:.3f} \\\\"
        for row in rows[:15]
    )
    sensitivity_rows = "\n".join(
        f"{item['backlog_threshold_percent']:.0f} & "
        f"{item['eligible_case_count']} & {item['qualified_case_count']} & "
        f"{item['unqualified_eligible_case_count']} & "
        f"\\texttt{{{_tex(str(item['highest_qualified_config_id']))}}} & "
        f"{item['highest_qualified_balanced_mib_per_sec']:.3f} \\\\"
        for item in summary["backlog_threshold_sensitivity"]
    )
    shortlist_rows = "\n".join(
        f"{row['selection_order']} & \\texttt{{{_tex(row['config_id'])}}} & "
        f"{row['primary_rank']} & {_yes_no(row['qualified'])} & "
        f"{row['balanced_mib_per_sec']:.3f} & "
        f"{row['balanced_records_per_sec']:,.0f} & "
        f"{row['latency_p99_us'] / 1_000_000:.3f} & "
        f"{_tex(row['selection_categories'])} \\\\"
        for row in shortlist
    )
    controlled_rows = "\n".join(
        f"\\texttt{{{_tex(item['config_id'])}}} & "
        f"{_tex(item['parameter_label'])} & {item['tested_value']:,} & "
        f"{item['balanced_mib_per_sec']:.3f} & "
        f"{item['balanced_mib_per_sec_delta']:+.3f} & "
        f"{item['balanced_mib_per_sec_change_percent']:+.2f} & "
        f"{item['latency_p99_us_delta'] / 1_000_000:+.3f} \\\\"
        for item in controlled[:15]
    )
    full_ranking_rows = "\n".join(
        f"{row['primary_rank']} & \\texttt{{{_tex(row['config_id'])}}} & "
        f"{_status_label(row)} & {row['payload_size_bytes']:,} & "
        f"{row['balanced_mib_per_sec']:.3f} & "
        f"{row['balanced_records_per_sec']:,.0f} & "
        f"{row['latency_p99_us'] / 1_000_000:.3f} & "
        f"{row['producer_backlog_percent']:.3f} & "
        f"{row['max_flush_duration_sec']:.3f} & "
        f"{row['failed_send_percent']:.3f} \\\\"
        for row in rows
    )

    key_cases: list[tuple[str, dict[str, Any]]] = []
    seen_key_cases: set[str] = set()
    candidates = [
        ("Highest qualified throughput", leader),
        ("Highest record rate", record_leader),
        ("Lowest qualified p99", latency_leader),
        ("Manifest baseline", baseline),
        ("Closest qualification boundary", _transition_row(rows)),
    ]
    for role, row in candidates:
        if row["config_id"] in seen_key_cases:
            continue
        seen_key_cases.add(row["config_id"])
        key_cases.append((role, row))
    key_case_rows = "\n".join(
        f"{_tex(role)} & \\texttt{{{_tex(row['config_id'])}}} & "
        f"{row['producer_ranks']}/{row['consumer_ranks']} & "
        f"{row['payload_size_bytes']:,} & {row['balanced_mib_per_sec']:.3f} & "
        f"{row['balanced_records_per_sec']:,.0f} & "
        f"{row['latency_p99_us'] / 1_000_000:.3f} & "
        f"{row['producer_backlog_percent']:.3f} & "
        f"{row['max_flush_duration_sec']:.3f} \\\\"
        for role, row in key_cases
    )
    resource_rows = "\n".join(
        f"\\texttt{{{_tex(row['config_id'])}}} & "
        f"{_tex_num(row['pulsar_process_rss_peak_gib'], 2)} & "
        f"{_tex_num(row['pulsar_jvm_heap_peak_gib'], 2)} & "
        f"{_tex_num(row['pulsar_bytes_in_peak_mib_per_sec'], 1)} & "
        f"{_tex_num(row['pulsar_bytes_out_peak_mib_per_sec'], 1)} & "
        f"{_tex_num(row['pulsar_service_ib0_rx_peak_mib_per_sec'], 1)} & "
        f"{_tex_num(row['pulsar_service_ib0_tx_peak_mib_per_sec'], 1)} & "
        f"{_tex_num(row['pulsar_jvm_gc_time_rate_peak'], 3)} \\\\"
        for _, row in key_cases
    )
    exception_rows = "\n".join(
        f"\\texttt{{{_tex(row['config_id'])}}} & "
        f"{row['balanced_mib_per_sec']:.3f} & "
        f"{row['latency_p99_us'] / 1_000_000:.3f} & "
        f"{row['producer_backlog_percent']:.6f} & "
        f"{row['max_flush_duration_sec']:.3f} & "
        f"{row['failed_send_percent']:.6f} & "
        f"{row['missing_after_drain_percent']:.6f} & "
        f"{row['duplicate_records'] + row['out_of_order_records']} \\\\"
        for row in unqualified
    )
    exception_text = (
        f"{len(unqualified)} observations remained eligible and correct but did "
        f"not qualify. {policy['backlog_exceeded_case_count']} exceeded the "
        f"5\\% producer-backlog threshold, "
        f"{policy['flush_exceeded_case_count']} exceeded the 10-second flush "
        f"threshold, and {policy['failed_send_exceeded_case_count']} exceeded "
        "the 0.1\\% failed-send threshold; categories may overlap."
    )
    if unqualified:
        primary_exception_text = (
            f"The {len(unqualified)} eligible but unqualified observations are "
            "separated by the producer-backlog rule; their threshold evidence is reported in "
            "Appendix~\\ref{app:exceptions}."
        )
        distribution_exception_text = (
            f"Red histogram segments count the {len(unqualified)} eligible but "
            "unqualified cases in each throughput bin."
        )
        balance_exception_text = (
            "The qualification exceptions show larger producer/consumer gaps than "
            "the leading qualified configurations, consistent with work remaining "
            "in producer queues during shutdown."
        )
        latency_exception_text = (
            "Latency remains a secondary comparison: an observation that exceeds "
            "the backlog, flush, or failed-send threshold cannot become qualified "
            "because of a favorable p99 value."
        )
        conclusion_exception_text = (
            f"The {len(unqualified)} unqualified observations provide transition- "
            "and overload-region evidence; their exact failed criteria are retained in "
            "Appendix~\\ref{app:exceptions}."
        )
        exception_appendix = rf"""\section{{Qualification Exceptions}}
\label{{app:exceptions}}
Table~\ref{{tab:exceptions}} gives all threshold evidence for the {len(unqualified)} eligible but unqualified cases. They remained scientifically eligible because execution, latency, and record correctness were valid.

\begingroup
\scriptsize
\begin{{longtable}}{{lrrrrrrr}}
\caption{{Qualification evidence for every eligible but unqualified Phase 1 case. Backlog is pending producer messages at flush start divided by measurement-period publication attempts. Missing is post-drain missing records divided by callback-confirmed publications and is shown only as correctness evidence; it is not a qualification threshold. Correctness errors is duplicate plus out-of-order records.}}\label{{tab:exceptions}}\\
\toprule
Config & Balanced MiB/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) & Missing (\%) & Correctness errors \\
\midrule
\endfirsthead
\caption[]{{Qualification evidence (continued).}}\\
\toprule
Config & Balanced MiB/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) & Missing (\%) & Correctness errors \\
\midrule
\endhead
{exception_rows}
\bottomrule
\end{{longtable}}
\endgroup
"""
    else:
        primary_exception_text = (
            "No eligible observation failed an operational qualification threshold."
        )
        distribution_exception_text = "Every eligible case qualified."
        balance_exception_text = "Every plotted case qualified."
        latency_exception_text = "No eligible case failed qualification."
        conclusion_exception_text = "Every eligible case qualified."
        exception_appendix = r"""\section{Qualification Exceptions}
\label{app:exceptions}
No eligible Phase 1 case failed an operational qualification threshold, so no exception table is required.
"""
    run_roots = ", ".join(
        f"\\nolinkurl{{{name}}}" for name in validation["results_roots"]
    )
    producer_rank_effect = next(
        item for item in controlled if item["config_id"] == "cfg_007"
    )
    payload_effect = next(
        item for item in controlled if item["config_id"] == "cfg_020"
    )
    pending_low_effect = next(
        item for item in controlled if item["config_id"] == "cfg_037"
    )
    pending_high_effect = next(
        item for item in controlled if item["config_id"] == "cfg_038"
    )

    preamble = r"""\documentclass[11pt,a4paper]{article}
\usepackage[T1]{fontenc}
\usepackage[utf8]{inputenc}
\usepackage{lmodern}
\usepackage{microtype}
\usepackage{graphicx}
\usepackage{amsmath}
\usepackage{amssymb}
\usepackage{geometry}
\usepackage{booktabs}
\usepackage{tabularx}
\usepackage{longtable}
\usepackage{array}
\usepackage{hyperref}
\usepackage{xcolor}
\usepackage{xspace}
\geometry{margin=2.2cm}
\setlength{\parindent}{0pt}
\setlength{\parskip}{0.55em}
\setlength{\emergencystretch}{2em}
\renewcommand{\arraystretch}{1.12}
\hypersetup{
  colorlinks=true,
  linkcolor=blue,
  urlcolor=blue,
  pdftitle={Verified Exploratory Analysis of Apache Pulsar Phase 1},
  pdfauthor={Sepehr Mahmoodian},
  pdfsubject={120-configuration fixed-profile workload screening}
}
\newcommand{\mibs}{\ensuremath{\,\mathrm{MiB/s}}\xspace}
\newcommand{\rps}{\ensuremath{\,\mathrm{records/s}}\xspace}
"""
    title = r"""\title{Verified Exploratory Analysis of Apache Pulsar Phase 1\\
\large 120-Configuration Fixed-Profile Workload Screening}
\author{Sepehr Mahmoodian}
\date{August 2026}
\begin{document}
\maketitle
\tableofcontents
\clearpage
"""
    introduction = rf"""\section{{Introduction}}
This report presents the completed Phase 1 workload screening campaign for the Apache Pulsar backend of HPC-MQBench, a reproducible, Slurm-orchestrated, qualification-first benchmark suite for distributed messaging systems. The campaign stresses a fixed Pulsar service profile with 120 simultaneous producer/consumer configurations on exclusive GWDG HPC allocations. It varies workload concurrency, partition count, record payload, producer batching and pending queues, and consumer receiver queues while preserving the service runtime. The qualification and shortlist presented here were recomputed retrospectively with the same application-level producer-backlog, flush, and failed-send rule used by the Kafka analysis after the later campaigns had already executed.

The report is qualification-first and deliberately conservative. Every Phase 1 configuration has one observation. Consequently, the highest measured value is described as the highest qualified single observation, not as a repeatable optimum or final recommendation. The retrospectively corrected shortlist is not the historical Phase 2 or final-validation workload set, and newly selected configurations were not validated. Apache Pulsar 5.0.0-M1 is a milestone build, not a claim about a stable Pulsar 5.0 release, and the standalone topology in this report must remain explicit in any later comparison with Kafka or another messaging system.

Figure~\ref{{fig:architecture}} describes the logical allocation used for every case. It is an architecture diagram rather than a result: solid horizontal arrows represent application records, dashed arrows represent monitoring collection, and the standalone service box contains the colocated broker, BookKeeper, and metadata roles.

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.97\linewidth]{{{figures['architecture']['pdf']}}}
\caption{{Logical four-node Pulsar benchmark architecture. The producer/controller node launches MPI producer ranks and coordinates the case; the service node runs one standalone Pulsar process containing broker, BookKeeper, and metadata roles; the consumer node uses a shared subscription and coordinated post-flush drain; and the monitoring node collects native Pulsar, process, and node telemetry.}}
\label{{fig:architecture}}
\end{{figure}}
"""
    executive = rf"""\section{{Executive Summary}}
All 120 required reports were present, completed, healthy, eligible, scientifically valid, and latency-valid. Of these, {summary['qualified_case_count']} met all three operational thresholds. The highest-ranked qualified observation was \texttt{{{_tex(leader['config_id'])}}} at {leader['balanced_mib_per_sec']:.3f}\mibs and {leader['balanced_records_per_sec']:,.0f}\rps. It used {leader['producer_ranks']} producer ranks, {leader['consumer_ranks']} consumer ranks, {leader['partitions']} partitions, and an {leader['payload_size_bytes']:,}-byte payload. Its producer-to-consumer sampled p99 latency was {leader['latency_p99_us'] / 1_000_000:.3f} seconds.

The Phase 1 primary rank first separates qualified from unqualified eligible observations and then orders by balanced throughput, producer backlog, flush duration, failed-send percentage, and configuration ID. p99 latency is retained as a secondary metric and does not influence this workload ranking.

The byte-throughput and record-throughput objectives select different workloads. \texttt{{{_tex(record_leader['config_id'])}}} achieved the highest balanced record rate, {record_leader['balanced_records_per_sec']:,.0f}\rps, with a {record_leader['payload_size_bytes']:,}-byte payload and {record_leader['balanced_mib_per_sec']:.3f}\mibs. \texttt{{{_tex(latency_leader['config_id'])}}} had the lowest qualified sampled p99, {latency_leader['latency_p99_us'] / 1_000_000:.3f} seconds, but its byte throughput was {latency_leader['balanced_mib_per_sec']:.3f}\mibs. These contrasts justify retaining separate throughput, record-rate, and latency anchors in Phase 2.

{exception_text} Post-drain missing, duplicate, and out-of-order records remain eligibility and correctness checks and are not reused as backlog. This Phase 1 report does not retroactively claim that the corrected shortlist was executed.
"""
    scope = rf"""\section{{Benchmark Goal and Scope}}
The primary application metric is balanced throughput,
\begin{{equation}}
T_{{\mathrm{{balanced}}}}=\min\left(T_{{\mathrm{{producer}}}},T_{{\mathrm{{consumer}}}}\right),
\label{{eq:balanced-throughput}}
\end{{equation}}
where byte throughput is divided by 1,048,576 and therefore reported as MiB/s. Balanced record throughput applies the same minimum to producer and consumer records/s. Equation~\ref{{eq:balanced-throughput}} prevents a producer-only rate from being treated as useful end-to-end throughput when the consumer side cannot keep pace.

The fixed system under test was profile \texttt{{{_tex(plan['profile_id'])}}}, checksum \texttt{{{_tex_breakable(plan['profile_sha256'])}}}. The profile used one standalone service, Apache Pulsar {plan['runtime_provenance']['pulsar_product_version']}, Python client {plan['runtime_provenance']['pulsar_python_client_version']}, Java {plan['runtime_provenance']['java_major']}, a 16~GiB JVM heap, and a 32~GiB direct-memory limit. Phase 1 assesses workload behavior only; it does not determine whether this heap/direct-memory combination is the best service configuration.
"""
    architecture = rf"""\section{{Architecture and Fixed Runtime}}
Each exclusive allocation contained four physical nodes with 192 logical CPUs and approximately 404~GB RAM per node. One node ran the standalone Pulsar service, one ran producer MPI ranks plus rank-zero control, one ran consumer MPI ranks, and one ran Prometheus and result collection. Kafka was not active in these cases, and Pulsar results were not pooled with Kafka results.

Each producer rank created one Pulsar client and one producer; each consumer rank created one client and one shared-subscription consumer. The configured \texttt{{virtual\_devices\_per\_rank}} value represents logical workload sources and does not create one network connection per simulated device. The benchmark client count for a case is the producer-rank count plus the consumer-rank count.

Table~\ref{{tab:fixed-settings}} records the principal settings held constant across the 120 cases. Topic partitions are excluded from this table because the partition count was intentionally varied per case.

\begin{{table}}[!htbp]
\centering
\small
\begin{{tabularx}}{{\linewidth}}{{@{{}}p{{0.39\linewidth}}X@{{}}}}
\toprule
Fixed control & Value \\
\midrule
Service topology & One standalone process containing broker, BookKeeper, and metadata services \\
Managed-ledger ensemble/write/ack quorum & 1/1/1 \\
Subscription & Shared; one persistent partitioned topic per case \\
Compression & None \\
Producer send timeout & 120,000 ms \\
Queue-full behavior & \texttt{{block\_if\_queue\_full=true}} \\
Negative-ack redelivery delay & 60,000 ms \\
Automatic topic creation & Disabled; topic created explicitly \\
Java and memory & Java 21; \texttt{{-Xms16g -Xmx16g -XX:MaxDirectMemorySize=32g}} \\
Optional Pulsar services & Functions Worker and stream storage disabled \\
Storage & Case-local RAM-backed storage \\
Offered-rate mode & Unbounded steady load; no fixed records/s target \\
Timing & 15 s excluded warm-up; 30 s measurement; up to 60 s coordinated drain \\
Latency & Deterministic one-in-ten sampling; 100 clock-calibration samples before and after \\
\bottomrule
\end{{tabularx}}
\caption{{Fixed Pulsar service, client-semantic, timing, and measurement controls used throughout Phase 1. These settings define the environment in which workload parameters were screened; they are not claimed to be universally optimal.}}
\label{{tab:fixed-settings}}
\end{{table}}
"""
    procedure = rf"""\section{{Test Method and Execution Procedure}}
Each case began from a versioned JSON configuration. The batch runner verified the accepted immutable profile, loaded the GWDG module environment, allocated role-specific nodes, created case-local RAM-backed directories, started Prometheus and node exporters, started the standalone Pulsar process, waited for readiness, and explicitly created the configured partitioned topic and shared subscription.

Producer and consumer ranks then ran simultaneously. Warm-up records were excluded. At the end of the 30-second measurement interval, producers stopped creating new records and flushed their clients. Consumers continued polling until all callback-confirmed publications had either been observed or the 60-second post-flush drain expired. The runner independently retained measured consumption, late-drained records, records missing after drain, duplicates, out-of-order records, and failed publications. Pulsar and case-local storage were stopped and cleared before the next configuration in the same allocation.

The 120 fixed-seed cases were divided into four sequential Slurm jobs of 30 configurations. Each job reused one exclusive four-node allocation but restarted Pulsar and reset storage between cases. Input seed \texttt{{{plan['seed']}}} generated mixed configurations; seed \texttt{{{plan['execution_order_seed']}}} randomized execution order. The four completed result roots were {run_roots}.
"""
    design = rf"""\section{{Sweep Design and Parameter Space}}
The design is a budgeted screen rather than a full factorial experiment. The Cartesian product of all eleven value sets contains 26,460,000 combinations. Phase 1 retained {plan['design_counts']['baseline']} baseline, {plan['design_counts']['one_factor']} one-factor-at-a-time, {plan['design_counts']['rank_pair']} unique producer/consumer rank-pair, and {plan['design_counts']['seeded_mixed']} seeded mixed configurations. Complete eleven-field tuples were used to remove duplicates before assigning \texttt{{cfg\_001}} through \texttt{{cfg\_120}}.

Table~\ref{{tab:parameter-space}} defines every varied field, its observed value set, and the \texttt{{cfg\_001}} baseline value. Producer and consumer ranks are independent factors. Partition count is a topic-layout factor applied when the partitioned topic is created; it is not a fixed standalone-service property.

\begin{{table}}[!htbp]
\centering
\small
\begin{{tabularx}}{{\linewidth}}{{@{{}}p{{0.39\linewidth}}Xr@{{}}}}
\toprule
Parameter & Tested values & Baseline \\
\midrule
{parameter_rows}
\bottomrule
\end{{tabularx}}
\caption{{Parameter space for the 120-case Pulsar Phase 1 design. Integer values use the units named in the first column. Batching and queue fields map directly to the Pulsar Python-client settings represented in each versioned case JSON.}}
\label{{tab:parameter-space}}
\end{{table}}
"""
    verification = rf"""\section{{Metrics, Eligibility, Qualification, and Verification}}
Producer throughput uses callback-confirmed publications during the measurement period. Consumer throughput uses records consumed during that period. Late-drained records participate in correctness accounting but do not inflate measured consumer throughput. Record rates are calculated directly from counts and duration; equivalently for a fixed payload $P$ they satisfy
\begin{{equation}}
\begin{{aligned}}
R_{{\mathrm{{producer}}}}&=T_{{\mathrm{{producer}}}}\frac{{1,048,576}}{{P}}, &
R_{{\mathrm{{consumer}}}}&=T_{{\mathrm{{consumer}}}}\frac{{1,048,576}}{{P}},\\
R_{{\mathrm{{balanced}}}}&=\min(R_{{\mathrm{{producer}}}},R_{{\mathrm{{consumer}}}}).&&
\end{{aligned}}
\label{{eq:record-throughput}}
\end{{equation}}

Eligibility and qualification are separate. Eligibility requires a completed healthy service and result, valid clock-calibrated latency, balanced record accounting, and zero missing, duplicate, out-of-order, invalid, and unexplained-surplus records. Producer backlog is calculated at flush start as
\begin{{equation}}
B_{{\mathrm{{producer}}}}=100\frac{{N_{{\mathrm{{pending,flush}}}}}}{{N_{{\mathrm{{attempted}}}}}}.
\label{{eq:producer-backlog}}
\end{{equation}}
An eligible case qualifies when
\begin{{equation}}
B_{{\mathrm{{producer}}}}\leq5\%\quad\land\quad
F_{{\max}}\leq10\,\mathrm{{s}}\quad\land\quad
E_{{\mathrm{{failed}}}}\leq0.1\%,
\label{{eq:qualification}}
\end{{equation}}
where $N_{{\mathrm{{pending,flush}}}}$ is the number of producer messages still pending immediately before flush, $N_{{\mathrm{{attempted}}}}$ is the number of publication attempts during measurement, $F_{{\max}}$ is the maximum producer-client flush duration across producer ranks, and $E_{{\mathrm{{failed}}}}$ is failed publications divided by attempted publications. Values equal to a threshold qualify. Post-drain missing, duplicate, and out-of-order records affect eligibility and correctness only.

Equation~\ref{{eq:qualification}} is the shared application-level qualification rule used by the Kafka and Pulsar analyses. It compares client-observed operational behavior and does not assert that backend-specific delivery, durability, or topology semantics are identical.

Latency uses the nanosecond producer timestamp carried in the fixed-size benchmark envelope and the corrected consumer receive time. MPI-node clocks were calibrated with 100 ping-pong samples before and after each case. Latency was rejected if uncertainty or drift exceeded 250 microseconds, if a negative sample appeared, if sampled-record accounting was incomplete, or if correctness failed.

Table~\ref{{tab:verification}} summarizes the strict data gate. All derived balanced rates equal the corresponding producer/consumer minimum, and every case satisfies the exact identity delivered = measured-consumed + late-drained + missing.

\begin{{table}}[!htbp]
\centering
\begin{{tabular}}{{lr}}
\toprule
Verification item & Result \\
\midrule
Manifest configurations / unique configuration IDs & 120 / 120 \\
Completed and healthy reports & {summary['completed_case_count']} \\
Scientifically eligible reports & {summary['eligible_case_count']} \\
Latency-valid reports & {summary['latency_valid_case_count']} \\
Qualified reports & {summary['qualified_case_count']} \\
Unqualified but eligible reports & {summary['unqualified_case_count']} \\
Missing after drain / duplicate / out-of-order records & {accounting['missing_after_drain']} / {accounting['duplicates']} / {accounting['out_of_order']} \\
Failed publications & {accounting['failed_sends']} \\
Maximum observed clock uncertainty & {clock['maximum_uncertainty_us']:.3f} $\mu$s \\
Maximum observed clock drift & {clock['maximum_drift_us']:.3f} $\mu$s \\
Immutable profile checksum mismatches & 0 \\
\bottomrule
\end{{tabular}}
\caption{{Phase 1 verification summary after matching the canonical manifest, four batch manifests, four acceptance reports, and 120 final reports. Eligibility includes health, completeness, latency, and exact correctness; qualification additionally applies Equation~\ref{{eq:qualification}}.}}
\label{{tab:verification}}
\end{{table}}

Table~\ref{{tab:backlog-sensitivity}} tests whether the Phase 1 conclusion depends strongly on the selected backlog boundary. Only the backlog limit changes from 2\% to 5\% or 10\%; the flush and failed-publication limits remain fixed at 10~s and 0.1\%. The highest-qualified configuration in each row is selected using the same throughput-first ordering as the primary rank.

\begin{{table}}[!htbp]
\centering
\small
\begin{{tabular}}{{rrrrlr}}
\toprule
Backlog limit (\%) & Eligible & Qualified & Unqualified eligible & Highest qualified & MiB/s \\
\midrule
{sensitivity_rows}
\bottomrule
\end{{tabular}}
\caption{{Phase 1 backlog-threshold sensitivity with maximum flush duration fixed at 10~s and failed sends fixed at 0.1\%. Eligible is the number of scientifically valid cases; Qualified applies the row's backlog limit plus the two fixed limits; Unqualified eligible is their difference; Highest qualified and MiB/s identify the top balanced-throughput observation under that threshold.}}
\label{{tab:backlog-sensitivity}}
\end{{table}}

Tightening the backlog limit from 5\% to 2\% changes the qualified count from {sensitivity_by_limit[5]['qualified_case_count']} to {sensitivity_by_limit[2]['qualified_case_count']}; relaxing it to 10\% changes the count to {sensitivity_by_limit[10]['qualified_case_count']}. The highest-qualified observations at these limits are \texttt{{{_tex(str(sensitivity_by_limit[2]['highest_qualified_config_id']))}}}, \texttt{{{_tex(str(sensitivity_by_limit[5]['highest_qualified_config_id']))}}}, and \texttt{{{_tex(str(sensitivity_by_limit[10]['highest_qualified_config_id']))}}}, respectively. This sensitivity analysis changes only the declared decision boundary and does not alter any measured value.
"""
    primary = rf"""\section{{Primary Screening Results}}
Eligibility is a prerequisite: only scientifically eligible observations receive a primary rank. Within that set, the rank is lexicographic: qualified before unqualified, then higher balanced MiB/s, lower producer backlog, shorter flush duration, lower failed-send percentage, and configuration ID. Sampled p99 latency remains a secondary reported metric and does not affect the Phase 1 primary rank. Table~\ref{{tab:primary-ranking}} reports the first 15 rows. The complete 120-row rank appears in Appendix~\ref{{app:full-ranking}} and in the generated CSV/JSON artifacts.

\begin{{table}}[!htbp]
\centering
\scriptsize
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{rllrrrrrrrr}}
\toprule
Rank & Config & Status & Producer & Consumer & Balanced & Records/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) \\
\midrule
{ranking_rows}
\bottomrule
\end{{tabular}}}}
\caption{{Primary qualification-first ranking excerpt. Producer and Consumer are callback-confirmed publication and measured consumption throughput in MiB/s; Balanced is their minimum from Equation~\ref{{eq:balanced-throughput}}; Records/s is the balanced record rate; p99 is secondary sampled producer-to-consumer latency; Backlog is defined by Equation~\ref{{eq:producer-backlog}}; Flush is the maximum producer-client flush duration; and Failed is failed publications as a percentage of attempted publications.}}
\label{{tab:primary-ranking}}
\end{{table}}

Figure~\ref{{fig:throughput-distribution}} summarizes the throughput distribution. {primary_exception_text} \texttt{{{_tex(leader['config_id'])}}} is both the raw byte-throughput leader and a qualified case, unlike the Kafka V1 raw upper-bound result.

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.82\linewidth]{{{figures['throughput_distribution']['pdf']}}}
\caption{{Distribution of balanced application throughput for all {len(rows)} Phase 1 configurations. The histogram contains the {summary['qualified_case_count']} qualified cases. {distribution_exception_text}}}
\label{{fig:throughput-distribution}}
\end{{figure}}

Figure~\ref{{fig:throughput-balance}} compares producer and consumer throughput. A point on the diagonal has equal rates; a point below the diagonal is consumer-limited. {balance_exception_text}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.78\linewidth]{{{figures['throughput_balance']['pdf']}}}
\caption{{Producer delivered throughput versus consumer measured throughput for all 120 configurations. The dashed diagonal is equality, colors use the retrospectively corrected qualification status, and labels identify the ten corrected Phase 1 shortlist candidates. Balanced throughput is the smaller coordinate for each point.}}
\label{{fig:throughput-balance}}
\end{{figure}}

Figure~\ref{{fig:throughput-backlog}} makes the operational boundary explicit. The horizontal coordinate is producer backlog at flush start from Equation~\ref{{eq:producer-backlog}}, and the vertical coordinate is measured balanced throughput from Equation~\ref{{eq:balanced-throughput}}. Each point is one eligible Phase 1 configuration; the dashed vertical line is the 5\% backlog threshold. Green points satisfy backlog, flush, and failed-send limits, whereas red points fail at least one of those limits. A point to the left of the line can therefore remain red if its flush or failed-send value exceeds its fixed threshold.

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.84\linewidth]{{{figures['throughput_backlog']['pdf']}}}
\caption{{Balanced application throughput versus producer backlog for all 120 eligible Phase 1 configurations. The vertical dashed line marks the shared 5\% Kafka/Pulsar application-level backlog threshold. Colors represent full operational qualification, not backlog alone, and labels identify the retrospectively corrected shortlist. The plot shows why raw throughput cannot override work still pending when producer flush begins.}}
\label{{fig:throughput-backlog}}
\end{{figure}}

Table~\ref{{tab:key-cases}} separates the principal byte-rate, record-rate, latency, baseline, and qualification-boundary observations. This prevents one scalar throughput rank from hiding the payload-dependent record-rate objective or the latency trade-off.

\begin{{table}}[!htbp]
\centering
\scriptsize
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{llrrrrrrr}}
\toprule
Role & Config & P/C ranks & Payload (B) & Balanced MiB/s & Records/s & p99 (s) & Backlog (\%) & Flush (s) \\
\midrule
{key_case_rows}
\bottomrule
\end{{tabular}}}}
\caption{{Key Phase 1 observations selected by distinct screening objectives. P/C ranks denotes producer/consumer MPI ranks. These are single observations under the fixed service profile; the role labels describe why each case matters for subsequent validation and do not constitute final recommendations.}}
\label{{tab:key-cases}}
\end{{table}}
"""
    latency = rf"""\section{{Latency and Payload Trade-offs}}
Figure~\ref{{fig:throughput-latency}} plots all valid p99 measurements against balanced byte throughput. Every point represents one 30-second maximum-load case with deterministic one-in-ten record sampling. The figure does not measure idle-system service time or a fixed offered-rate latency curve. High p99 values therefore include queueing accumulated under unconstrained offered load.

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.82\linewidth]{{{figures['throughput_latency']['pdf']}}}
\caption{{Balanced application throughput versus sampled producer-to-consumer p99 latency for all 120 cases. Green points satisfy the corrected backlog, flush, and failed-send thresholds; red points remain eligible but fail at least one of those thresholds. Labels identify retrospectively shortlisted cases. Latency is reported in seconds for readability and is valid only after the per-case clock and correctness gates.}}
\label{{fig:throughput-latency}}
\end{{figure}}

The lowest qualified p99 was \texttt{{{_tex(latency_leader['config_id'])}}} at {latency_leader['latency_p99_us'] / 1_000_000:.3f} seconds. The byte-throughput leader \texttt{{{_tex(leader['config_id'])}}} reached {leader['balanced_mib_per_sec']:.3f}\mibs with p99 {leader['latency_p99_us'] / 1_000_000:.3f} seconds. {latency_exception_text}

Figure~\ref{{fig:payload-tradeoff}} explains why MiB/s and records/s must both be retained. Larger records can increase transferred bytes per second while reducing the number of application records processed. \texttt{{{_tex(record_leader['config_id'])}}} is the record-rate leader at {record_leader['balanced_records_per_sec']:,.0f}\rps, whereas \texttt{{{_tex(leader['config_id'])}}} is the byte-rate leader at {leader['balanced_mib_per_sec']:.3f}\mibs.

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.82\linewidth]{{{figures['payload_tradeoff']['pdf']}}}
\caption{{Balanced record throughput versus balanced byte throughput for all 120 cases, grouped by configured payload size. Each point is one Phase 1 observation; labels identify shortlist cases. The plot demonstrates that maximizing MiB/s and maximizing records/s are different workload objectives.}}
\label{{fig:payload-tradeoff}}
\end{{figure}}
"""
    controlled_section = rf"""\section{{Controlled Parameter Findings}}
\label{{sec:controlled}}
The 43 one-factor configurations change exactly one field from \texttt{{cfg\_001}}, whose balanced throughput was {baseline['balanced_mib_per_sec']:.3f}\mibs and sampled p99 was {baseline['latency_p99_us'] / 1_000_000:.3f} seconds. Table~\ref{{tab:controlled}} and Figure~\ref{{fig:controlled}} rank the 15 largest absolute byte-throughput changes. Because each treatment and baseline was observed once, the differences are screening contrasts without uncertainty intervals; small differences may reflect run-to-run noise and must not be treated as causal estimates.

The largest positive controlled contrast was producer concurrency: \texttt{{{_tex(producer_rank_effect['config_id'])}}} changed producer ranks from {producer_rank_effect['baseline_value']} to {producer_rank_effect['tested_value']} and increased balanced throughput by {producer_rank_effect['balanced_mib_per_sec_delta']:.3f}\mibs ({producer_rank_effect['balanced_mib_per_sec_change_percent']:.2f}\%). Increasing payload to {payload_effect['tested_value']:,} bytes added {payload_effect['balanced_mib_per_sec_delta']:.3f}\mibs but increased p99 by {payload_effect['latency_p99_us_delta'] / 1_000_000:.3f} seconds. Reducing the cross-partition pending limit to {pending_low_effect['tested_value']:,} increased throughput by {pending_low_effect['balanced_mib_per_sec_delta']:.3f}\mibs and reduced p99 by {-pending_low_effect['latency_p99_us_delta'] / 1_000_000:.3f} seconds in its single observation, while increasing it to {pending_high_effect['tested_value']:,} reduced throughput by {abs(pending_high_effect['balanced_mib_per_sec_delta']):.3f}\mibs and increased p99 by {pending_high_effect['latency_p99_us_delta'] / 1_000_000:.3f} seconds. These queue findings motivate a focused repeated comparison; they do not prove a universal queue optimum.

\begin{{table}}[!htbp]
\centering
\scriptsize
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{llrrrrr}}
\toprule
Config & Changed parameter & Tested value & Balanced MiB/s & Delta MiB/s & Change (\%) & Delta p99 (s) \\
\midrule
{controlled_rows}
\bottomrule
\end{{tabular}}}}
\caption{{Largest one-factor Phase 1 contrasts by absolute balanced-throughput change from baseline \texttt{{cfg\_001}}. Delta MiB/s and Change are treatment minus baseline; Delta p99 is the corresponding change in sampled producer-to-consumer p99 latency. Each contrast has one baseline observation and one treatment observation.}}
\label{{tab:controlled}}
\end{{table}}

\begin{{figure}}[!htbp]
\centering
\includegraphics[width=0.92\linewidth]{{{figures['controlled_effects']['pdf']}}}
\caption{{Fifteen largest absolute one-factor balanced-throughput changes relative to \texttt{{cfg\_001}}. Green bars are positive and red bars are negative. The bars are exploratory single-run contrasts and do not show confidence intervals.}}
\label{{fig:controlled}}
\end{{figure}}
"""
    resources = rf"""\section{{Resource and Bottleneck Evidence}}
All 120 reports contained five healthy Prometheus targets and completed 24 of 31 configured monitoring queries without query errors. Native Pulsar ingress/egress, JVM heap, partial NIO direct buffers, process resident set size (RSS), garbage-collection rates, service-node \texttt{{ib0}} traffic, managed-ledger storage, message backlog, and node resources were retained. Seven optional metrics were consistently unavailable: total Pulsar direct-memory usage plus six BookKeeper queue/cache/throttling metrics.

Table~\ref{{tab:resources}} gives compact supporting evidence for the key cases from Table~\ref{{tab:key-cases}}. Values are broad monitoring-window maxima. Depending on the metric and endpoint timing, each summary contains four to six numeric samples spanning startup, warm-up, measurement, and drain rather than a synchronized measurement-only interval. They are suitable for identifying gross exhaustion and for selecting Phase 2 diagnostics, but they must not be used as a precise efficiency ranking or to override Equation~\ref{{eq:qualification}}.

\begin{{table}}[!htbp]
\centering
\scriptsize
\resizebox{{\linewidth}}{{!}}{{%
\begin{{tabular}}{{lrrrrrrr}}
\toprule
Config & RSS GiB & Heap GiB & Native in & Native out & \texttt{{ib0}} RX & \texttt{{ib0}} TX & GC s/s \\
\midrule
{resource_rows}
\bottomrule
\end{{tabular}}}}
\caption{{Broad-window resource maxima for selected Phase 1 cases. RSS is the standalone Pulsar process resident set; Heap is JVM heap used; Native in/out are Pulsar broker byte rates converted to MiB/s; \texttt{{ib0}} RX/TX are exact service-interface rates in MiB/s; and GC s/s is the maximum sampled rate of accumulated garbage-collection seconds per second. The values are sparse, not synchronized percentiles.}}
\label{{tab:resources}}
\end{{table}}

Process RSS legitimately exceeds JVM heap because the standalone service also uses direct/native buffers, mapped storage, thread stacks, and colocated BookKeeper/metadata components. The available JVM NIO direct-buffer metric is only partial direct-memory evidence and cannot prove total direct-memory headroom. Likewise, absent BookKeeper queue metrics prevent a queue-specific storage bottleneck conclusion. Phase 2 should retain process RSS, heap, GC, exact \texttt{{ib0}}, Pulsar ingress/egress, and add reliable total-direct-memory and BookKeeper queue telemetry if available.
"""
    shortlist_section = rf"""\section{{Retrospectively Corrected Phase 1 Shortlist}}
The data-driven shortlist in Table~\ref{{tab:shortlist}} covers the top three qualified byte-throughput observations, raw throughput, record-rate leadership, lowest qualified p99, the closest backlog-based qualification boundary, the manifest baseline, the highest-ranked one-factor case, the highest-ranked rank-pair case, and additional primary-rank coverage. A configuration may satisfy multiple categories.

This shortlist was recomputed after Phase 2 and final validation had already executed. It must therefore be interpreted as a corrected Phase 1 selection result, not as the historical execution manifest. The historical Phase 2 anchors were \texttt{{cfg\_120}}, \texttt{{cfg\_101}}, \texttt{{cfg\_093}}, \texttt{{cfg\_089}}, and \texttt{{cfg\_001}}. The historical final-validation set was \texttt{{cfg\_001}}, \texttt{{cfg\_007}}, \texttt{{cfg\_049}}, \texttt{{cfg\_055}}, \texttt{{cfg\_063}}, \texttt{{cfg\_080}}, \texttt{{cfg\_089}}, \texttt{{cfg\_093}}, \texttt{{cfg\_101}}, and \texttt{{cfg\_120}}. Configurations newly admitted by the corrected shortlist were not run in either later campaign.

\begin{{table}}[!htbp]
\centering
\scriptsize
\begin{{tabularx}}{{\linewidth}}{{@{{}}r l r r r r r X@{{}}}}
\toprule
Order & Config & Rank & Qualified & MiB/s & Records/s & p99 (s) & Selection categories \\
\midrule
{shortlist_rows}
\bottomrule
\end{{tabularx}}
\caption{{Retrospectively corrected ten-configuration Phase 1 shortlist under the shared Kafka/Pulsar application-level qualification rule. Rank is the corrected qualification-first Phase 1 rank; rates and p99 are single-run screening observations; Selection categories state the data-derived reason for retaining each workload. This table does not claim that all listed configurations were executed in Phase 2 or final validation.}}
\label{{tab:shortlist}}
\end{{table}}

The shortlist is not a final recommendation and is not substituted for the immutable historical manifests. Later-campaign conclusions in the complete report are based only on workload/profile combinations that were actually executed.
"""
    limitations = r"""\section{Limitations}
\begin{itemize}
\item Each workload has one Phase 1 observation. There are no within-configuration confidence intervals, and small parameter contrasts cannot be separated from run-to-run variation.
\item The mixed design is not factorial. Associations among mixed configurations are confounded and are not causal parameter effects.
\item Latency was measured under unconstrained maximum offered load. It is valid producer-to-consumer latency, but it is not a fixed-rate service-latency curve or idle-response metric.
\item The standalone process colocates broker, BookKeeper, and metadata services on one node with RAM-backed storage and quorum 1/1/1. Results do not generalize directly to a replicated production Pulsar cluster.
\item Apache Pulsar 5.0.0-M1 is a milestone build. Future versions may differ.
\item Monitoring summaries are sparse broad-window samples. Total direct-memory use and BookKeeper queue/cache metrics were unavailable, so detailed storage-queue and direct-memory bottleneck claims are intentionally withheld.
\item The corrected Pulsar qualification uses the same producer backlog, flush, and failed-send thresholds as Kafka, but cross-system comparison still requires explicit semantic, durability, topology, and version normalization.
\end{itemize}
"""
    conclusion = rf"""\section{{Conclusion}}
Pulsar Phase 1 is complete: {len(rows)} unique configurations produced {summary['completed_case_count']} completed reports; {summary['eligible_case_count']} were eligible, {summary['latency_valid_case_count']} had valid latency, and {summary['qualified_case_count']} qualified. The highest qualified byte-throughput observation was \texttt{{{_tex(leader['config_id'])}}} at {leader['balanced_mib_per_sec']:.3f}\mibs, the highest record-rate observation was \texttt{{{_tex(record_leader['config_id'])}}} at {record_leader['balanced_records_per_sec']:,.0f}\rps, and the lowest qualified sampled p99 was \texttt{{{_tex(latency_leader['config_id'])}}} at {latency_leader['latency_p99_us'] / 1_000_000:.3f} seconds. {conclusion_exception_text}

The corrected Phase 1 shortlist supports future workload selection and bounded single-run observations. It does not rewrite the later campaigns: Phase 2 and final validation remain evidence for their historically executed manifests only.
"""
    appendices = rf"""\appendix
\clearpage
\section{{Complete Qualification-First Ranking}}
\label{{app:full-ranking}}
Table~\ref{{tab:full-ranking}} contains all 120 Phase 1 observations. Status is qualified (Q) or eligible but unqualified (U); payload is application bytes per record; p99 is secondary sampled end-to-end latency; Backlog follows Equation~\ref{{eq:producer-backlog}}; Flush is maximum producer-client flush duration; and Failed is failed publications divided by attempted publications.

\scriptsize
\begin{{longtable}}{{rllrrrrrrr}}
\caption{{Complete corrected Phase 1 qualification-first ranking. Balanced MiB/s is the minimum producer/consumer application byte rate; Records/s is the corresponding balanced record rate; p99 is a secondary metric in seconds; Backlog is pending messages at flush start as a percentage of measurement-period publication attempts; Flush is in seconds; and Failed is percent of attempted publications.}}\label{{tab:full-ranking}}\\
\toprule
Rank & Config & Status & Payload & Balanced MiB/s & Records/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) \\
\midrule
\endfirsthead
\caption[]{{Complete Phase 1 qualification-first ranking (continued).}}\\
\toprule
Rank & Config & Status & Payload & Balanced MiB/s & Records/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) \\
\midrule
\endhead
{full_ranking_rows}
\bottomrule
\end{{longtable}}
\normalsize

{exception_appendix}

\section{{Provenance and Generated Artifacts}}
Table~\ref{{tab:provenance}} records the immutable campaign identity and analysis inputs. The generated artifact manifest contains SHA-256 checksums for the compact CSV, JSON, Markdown, HTML, LaTeX, and figure files. Raw HPC run directories remain separate and were not modified by this analysis.

\begin{{table}}[!htbp]
\centering
\small
\begin{{tabularx}}{{\linewidth}}{{@{{}}p{{0.34\linewidth}}X@{{}}}}
\toprule
Provenance item & Value \\
\midrule
Backend / campaign & Apache Pulsar / \texttt{{pulsar-phase1-screening}} \\
Product / client / Java & {plan['runtime_provenance']['pulsar_product_version']} / {plan['runtime_provenance']['pulsar_python_client_version']} / {plan['runtime_provenance']['java_major']} \\
Profile ID & \texttt{{{_tex(plan['profile_id'])}}} \\
Profile SHA-256 & \texttt{{{_tex_breakable(plan['profile_sha256'])}}} \\
Configuration seed / execution seed & {plan['seed']} / {plan['execution_order_seed']} \\
Canonical manifest & \nolinkurl{{configs/campaigns/pulsar/phase1/phase1_manifest.csv}} \\
Result roots & {run_roots} \\
Validation format & \nolinkurl{{{validation['format']}}}; valid = {_yes_no(validation['valid'])} \\
Analysis format & \nolinkurl{{{REPORT_FORMAT}}} \\
\bottomrule
\end{{tabularx}}
\caption{{Reproducibility and provenance for the complete Pulsar Phase 1 analysis. Repository-relative identifiers are used so the report contains no developer-specific absolute paths.}}
\label{{tab:provenance}}
\end{{table}}
\end{{document}}
"""
    path.write_text(
        "\n".join(
            (
                preamble,
                title,
                introduction,
                executive,
                scope,
                architecture,
                procedure,
                design,
                verification,
                primary,
                latency,
                controlled_section,
                resources,
                shortlist_section,
                limitations,
                conclusion,
                appendices,
            )
        ),
        encoding="utf-8",
    )


def _write_incomplete_markdown(path: Path, validation: dict[str, Any]) -> None:
    lines = [
        "# Apache Pulsar Phase 1 Screening Report",
        "",
        "**Status: incomplete; no ranking or Phase 2 shortlist was produced.**",
        "",
        f"Observed required reports: {validation['observed_required_case_count']} / {validation['expected_case_count']}.",
        "",
        "## Validation failures",
        "",
        *[f"- {reason}" for reason in validation["failure_reasons"]],
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def _md_result_row(row: dict[str, Any]) -> str:
    return (
        f"| {row['primary_rank']} | `{row['config_id']}` | {_yes_no(row['eligible'])} | "
        f"{_yes_no(row['qualified'])} | {row['balanced_mib_per_sec']:.3f} | "
        f"{row['balanced_records_per_sec']:,.0f} | {_fmt(row['latency_p99_us'], 3)} | "
        f"{row['producer_backlog_percent']:.6f} | "
        f"{row['missing_after_drain_percent']:.6f} | "
        f"{row['max_flush_duration_sec']:.3f} | {row['failed_send_percent']:.6f} |"
    )


def _write_csv(
    path: Path,
    fieldnames: Iterable[str],
    rows: list[dict[str, Any]],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _relative(path: Path, root: Path) -> str:
    if root.is_file():
        return path.name
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _optional_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _number(value: Any, *, default: float) -> float:
    observed = _optional_number(value)
    return observed if observed is not None else default


def _integer(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)


def _metric_number(
    metric_map: dict[str, dict[str, Any]],
    metric_id: str,
    field: str,
) -> float | None:
    metric = metric_map.get(metric_id)
    if not isinstance(metric, dict):
        return None
    return _optional_number(metric.get(field))


def _bytes_to_mib(value: float | None) -> float | None:
    return value / 1_048_576 if value is not None else None


def _bytes_to_gib(value: float | None) -> float | None:
    return value / 1_073_741_824 if value is not None else None


def _decimal_mb_to_mib(value: float | None) -> float | None:
    return value * 1_000_000 / 1_048_576 if value is not None else None


def _quartile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("cannot calculate a quartile for an empty sequence")
    if not 0.0 <= fraction <= 1.0:
        raise ValueError("quartile fraction must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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


def _yes_no(value: Any) -> str:
    return "yes" if value is True else "no"


def _fmt(value: Any, digits: int) -> str:
    return f"{value:,.{digits}f}" if isinstance(value, (int, float)) else "N/A"


def _status_label(row: dict[str, Any]) -> str:
    if row.get("eligible") is not True:
        return "I"
    return "Q" if row.get("qualified") is True else "U"


def _tex_num(value: Any, digits: int) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "--"
    number = float(value)
    if not math.isfinite(number):
        return "--"
    return f"{number:,.{digits}f}"


def _tex(value: Any) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "{": r"\{",
        "}": r"\}",
        "_": r"\_",
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
        "$": r"\$",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(character, character) for character in str(value))


def _tex_breakable(value: Any, width: int = 8) -> str:
    text = str(value)
    if width <= 0:
        raise ValueError("breakable TeX chunk width must be positive")
    return r"\allowbreak{}".join(
        _tex(text[offset : offset + width])
        for offset in range(0, len(text), width)
    )


def _write_artifact_manifest(output_dir: Path) -> None:
    manifest_path = output_dir / "artifact_manifest.csv"
    checksum_path = output_dir / "SHA256SUMS"
    root_artifacts = {
        "pulsar_phase1_backlog_sensitivity.csv",
        "pulsar_phase1_backlog_sensitivity.json",
        "pulsar_phase1_cases.csv",
        "pulsar_phase1_cases.json",
        "pulsar_phase1_controlled_comparisons.csv",
        "pulsar_phase1_report.html",
        "pulsar_phase1_report.md",
        "pulsar_phase1_report.pdf",
        "pulsar_phase1_report.tex",
        "pulsar_phase1_shortlist.csv",
        "pulsar_phase1_shortlist.json",
        "pulsar_phase1_summary.json",
        "pulsar_phase1_validation.json",
    }
    rows: list[dict[str, Any]] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file() or path in {manifest_path, checksum_path}:
            continue
        relative = path.relative_to(output_dir)
        is_figure = (
            len(relative.parts) == 2
            and relative.parts[0] == "figures"
            and path.suffix in {".pdf", ".png"}
        )
        if path.parent == output_dir:
            if path.name not in root_artifacts:
                continue
        elif not is_figure:
            continue
        rows.append(
            {
                "artifact": relative.as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    _write_csv(
        manifest_path,
        ("artifact", "size_bytes", "sha256"),
        rows,
    )
    checksum_paths = [output_dir / row["artifact"] for row in rows]
    checksum_paths.append(manifest_path)
    checksum_path.write_text(
        "".join(
            f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
            f"{path.relative_to(output_dir).as_posix()}\n"
            for path in checksum_paths
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    raise SystemExit(main())
