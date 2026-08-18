#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import analyze_pulsar_phase2_screening as screening  # noqa: E402


DEFAULT_ROOT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "final_validation"
DEFAULT_MANIFEST = DEFAULT_ROOT / "final_validation_manifest.csv"
DEFAULT_PLAN = DEFAULT_ROOT / "final_validation_plan.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "final-validation"
EXPECTED_PROFILE = "BASELINE_H16_D32"
RETROSPECTIVELY_CORRECTED_PHASE1_SHORTLIST = (
    "cfg_120",
    "cfg_080",
    "cfg_057",
    "cfg_101",
    "cfg_093",
    "cfg_019",
    "cfg_001",
    "cfg_037",
    "cfg_049",
    "cfg_115",
)

CASE_FIELDS = (
    "block",
    "order",
    "repeat_index",
    "config_id",
    "case_id",
    "selection_categories",
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "payload_size_bytes",
    "profile_id",
    "profile_sha256",
    "status",
    "backend_health",
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
    "pulsar_bytes_in_peak_mib_per_sec",
    "pulsar_bytes_out_peak_mib_per_sec",
    "pulsar_process_rss_peak_gib",
    "pulsar_jvm_heap_peak_gib",
    "pulsar_jvm_direct_nio_peak_gib",
    "pulsar_managed_ledger_direct_pool_allocated_peak_gib",
    "pulsar_managed_ledger_direct_pool_used_peak_gib",
    "pulsar_jvm_gc_time_rate_peak",
    "pulsar_message_backlog_peak",
    "tmpfs_used_peak_percent",
    "post_reset_tmpfs_free_percent",
    "source_results_root",
    "source_report",
)

SUMMARY_FIELDS = (
    "final_rank",
    "validated_recommendation",
    "config_id",
    "selection_categories",
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "payload_size_bytes",
    "repeat_count",
    "completed_repeats",
    "healthy_repeats",
    "eligible_repeats",
    "qualified_repeats",
    "latency_valid_repeats",
    "correct_repeats",
    "median_producer_mib_per_sec",
    "median_consumer_mib_per_sec",
    "median_balanced_mib_per_sec",
    "balanced_mib_per_sec_iqr",
    "median_producer_records_per_sec",
    "median_consumer_records_per_sec",
    "median_balanced_records_per_sec",
    "balanced_records_per_sec_iqr",
    "median_latency_p50_us",
    "median_latency_p95_us",
    "median_latency_p99_us",
    "latency_p99_us_iqr",
    "median_latency_p99_9_us",
    "median_producer_backlog_percent",
    "max_producer_backlog_percent",
    "median_missing_after_drain_percent",
    "median_max_flush_duration_sec",
    "max_failed_send_percent",
    "median_pulsar_bytes_in_peak_mib_per_sec",
    "median_pulsar_bytes_out_peak_mib_per_sec",
    "median_pulsar_process_rss_peak_gib",
    "median_pulsar_jvm_heap_peak_gib",
    "median_tmpfs_used_peak_percent",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Strictly validate and aggregate the five-block Pulsar final "
            "workload-validation campaign."
        )
    )
    parser.add_argument("--results-root", action="append", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--skip-figure", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = _read_manifest(args.manifest)
    plan = _read_json(args.plan)
    reports, duplicates = _discover_reports(args.results_root)
    validation, rows = _validate_and_collect(
        manifest=manifest,
        manifest_sha256=_sha256(args.manifest),
        plan=plan,
        reports=reports,
        duplicate_case_ids=duplicates,
        results_roots=args.results_root,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    validation_path = args.output_dir / "pulsar_final_validation_validation.json"
    _write_json(validation_path, validation)
    if not validation["valid"]:
        _write_incomplete_report(
            args.output_dir / "pulsar_final_validation_report.md", validation
        )
        screening._write_checksums(args.output_dir)
        print("[pulsar-final-validation] analysis incomplete")
        for reason in validation["failure_reasons"]:
            print(f"[pulsar-final-validation] {reason}")
        return 1

    summaries = summarize_configurations(rows)
    rank_summaries(summaries)
    recommendation = _recommendation(summaries, validation, args, plan)
    recommendation_id = recommendation.get("config_id")
    for row in summaries:
        row["validated_recommendation"] = row["config_id"] == recommendation_id

    _write_csv(args.output_dir / "pulsar_final_validation_cases.csv", CASE_FIELDS, rows)
    _write_json(
        args.output_dir / "pulsar_final_validation_cases.json",
        {
            "format": "messaging-benchmark.pulsar-final-validation-cases.v1",
            "backend_id": "pulsar",
            "campaign_id": "pulsar-final-workload-validation",
            "case_count": len(rows),
            "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "cases": rows,
        },
    )
    _write_csv(
        args.output_dir / "pulsar_final_validation_summary.csv",
        SUMMARY_FIELDS,
        summaries,
    )
    _write_json(
        args.output_dir / "pulsar_final_validation_summary.json",
        {
            "format": "messaging-benchmark.pulsar-final-validation-summary.v1",
            "configuration_count": len(summaries),
            "observations_per_configuration": 5,
            "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "aggregation_scope": "eligible repeats within each configuration",
            "ranking_prerequisite": (
                "only eligible repeats contribute to aggregate performance metrics"
            ),
            "ranking_rule": recommendation["ranking_rule"],
            "secondary_metrics": [
                "median valid producer-to-consumer p99 latency",
            ],
            "workload_set_status": "historically_executed_pre_correction_shortlist",
            "retrospectively_corrected_phase1_shortlist": list(
                RETROSPECTIVELY_CORRECTED_PHASE1_SHORTLIST
            ),
            "interpretation": (
                "Only the ten configurations in the historical final-validation "
                "manifest were repeated. Newly admitted configurations in the "
                "retrospectively corrected Phase 1 shortlist were not validated."
            ),
            "configurations": summaries,
        },
    )
    _write_json(
        args.output_dir / "pulsar_final_validation_recommendation.json",
        recommendation,
    )
    _write_markdown(
        args.output_dir / "pulsar_final_validation_report.md",
        summaries,
        recommendation,
        validation,
    )
    if not args.skip_figure:
        _write_figure(args.output_dir, summaries)
    screening._write_checksums(args.output_dir)
    print(f"[pulsar-final-validation] cases: {len(rows)}")
    print(f"[pulsar-final-validation] configurations: {len(summaries)}")
    print(
        "[pulsar-final-validation] recommendation: "
        f"{recommendation.get('config_id', 'none')} ({recommendation['status']})"
    )
    print(f"[pulsar-final-validation] output: {args.output_dir}")
    return 0


def _read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {
        "block",
        "order",
        "case_id",
        "config_id",
        "profile_id",
        "profile_sha256",
        "anchor_purpose",
        "producer_ranks",
        "consumer_ranks",
        "partitions",
        "payload_size_bytes",
    }
    missing = required - set(rows[0] if rows else {})
    if missing:
        raise ValueError(
            "Final-validation manifest is empty or missing fields: "
            + ", ".join(sorted(missing))
        )
    return rows


def _discover_reports(
    roots: list[Path],
) -> tuple[dict[str, tuple[Path, dict[str, Any], Path]], list[str]]:
    candidates_by_case: dict[
        str, list[tuple[Path, dict[str, Any], Path]]
    ] = {}
    for root in roots:
        if not root.exists():
            raise ValueError(f"Results root does not exist: {root}")
        for path in sorted(root.rglob("final_report.json")):
            if path.parent.name in {"data", "reports"}:
                continue
            report = _read_json(path)
            system = _dict(report.get("system_under_test"))
            if system.get("backend_id") != "pulsar":
                continue
            case_id = str(_dict(report.get("case")).get("case_id", "")).strip()
            if not case_id:
                continue
            candidates_by_case.setdefault(case_id, []).append((path, report, root))
    selected: dict[str, tuple[Path, dict[str, Any], Path]] = {}
    duplicates: list[str] = []
    for case_id, candidates in candidates_by_case.items():
        candidates.sort(
            key=lambda item: screening._report_selection_key(item[0], item[1]),
            reverse=True,
        )
        preferred = candidates[0]
        preferred_completed = (
            _dict(preferred[1].get("case")).get("status") == "completed"
        )
        preferred_hash = screening._canonical_json_sha256(preferred[1])
        if any(
            preferred_completed
            and _dict(report.get("case")).get("status") == "completed"
            and screening._canonical_json_sha256(report) != preferred_hash
            for _, report, _ in candidates[1:]
        ):
            duplicates.append(case_id)
        selected[case_id] = preferred
    return selected, sorted(duplicates)


def _validate_and_collect(
    *,
    manifest: list[dict[str, str]],
    manifest_sha256: str,
    plan: dict[str, Any],
    reports: dict[str, tuple[Path, dict[str, Any], Path]],
    duplicate_case_ids: list[str],
    results_roots: list[Path],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failures: list[str] = []
    case_ids = [row["case_id"] for row in manifest]
    config_ids = [row["config_id"] for row in manifest]
    blocks = [row["block"] for row in manifest]
    expected_configs = {str(item) for item in plan.get("configuration_ids", [])}
    checks = (
        (len(manifest) == 50, f"manifest has {len(manifest)} rows, expected 50"),
        (len(set(case_ids)) == 50, "manifest case IDs are not unique"),
        (len(expected_configs) == 10, "plan does not define ten configurations"),
        (set(config_ids) == expected_configs, "manifest configuration set differs from plan"),
        (
            Counter(config_ids) == Counter({item: 5 for item in expected_configs}),
            "each configuration must occur exactly five times",
        ),
        (
            Counter(blocks) == Counter({str(block): 10 for block in range(1, 6)}),
            "each randomized block must contain ten cases",
        ),
        (
            plan.get("format")
            == "messaging-benchmark.pulsar-final-validation-plan.v1",
            "unexpected campaign plan format",
        ),
        (
            plan.get("status") == "authorized_for_submission"
            and plan.get("submission_authorized") is True,
            "campaign plan was not authorized for submission",
        ),
        (plan.get("case_count") == 50, "plan case count differs from 50"),
        (plan.get("block_count") == 5, "plan block count differs from five"),
        (
            plan.get("observations_per_configuration") == 5,
            "plan repeat count differs from five",
        ),
        (
            plan.get("manifest_sha256") == manifest_sha256,
            "campaign manifest checksum differs from the authorized plan",
        ),
        (plan.get("profile_id") == EXPECTED_PROFILE, "plan frozen profile changed"),
    )
    failures.extend(reason for passed, reason in checks if not passed)
    for block in range(1, 6):
        block_rows = [row for row in manifest if row["block"] == str(block)]
        if {row["config_id"] for row in block_rows} != expected_configs:
            failures.append(f"block {block} does not contain each configuration once")
        if sorted(int(row["order"]) for row in block_rows) != list(range(1, 11)):
            failures.append(f"block {block} order is not a permutation of 1..10")
    if duplicate_case_ids:
        failures.append(
            "conflicting completed reports found for case(s): "
            + ", ".join(duplicate_case_ids)
        )

    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    expected_profile_sha = str(plan.get("profile_sha256", ""))
    for manifest_row in manifest:
        case_id = manifest_row["case_id"]
        selected = reports.get(case_id)
        if selected is None:
            missing.append(case_id)
            continue
        path, report, root = selected
        row = _case_row(manifest_row, report, path, root)
        rows.append(row)
        failures.extend(
            f"{case_id}: {reason}"
            for reason in _reported_config_failures(manifest_row, report)
        )
        failures.extend(
            f"{case_id}: {reason}"
            for reason in _case_structural_failures(
                row,
                expected_profile_sha=expected_profile_sha,
                manifest_profile_sha=manifest_row["profile_sha256"],
            )
        )
    if missing:
        failures.append(
            f"missing {len(missing)} required report(s): "
            + ", ".join(missing[:10])
            + (" ..." if len(missing) > 10 else "")
        )

    extra_ids = sorted(set(reports) - set(case_ids))
    validation = {
        "format": "messaging-benchmark.pulsar-final-validation-validation.v1",
        "valid": not failures,
        "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "expected_case_count": 50,
        "manifest_case_count": len(manifest),
        "observed_case_count": len(rows),
        "configuration_count": len(set(config_ids)),
        "block_count": len(set(blocks)),
        "eligible_case_count": sum(row["eligible"] for row in rows),
        "qualified_case_count": sum(row["qualified"] for row in rows),
        "extra_case_ids": extra_ids,
        "results_roots": [root.name for root in results_roots],
        "slurm_job_ids": sorted(
            {
                job_id
                for root in results_roots
                for job_id in screening._slurm_job_ids(root)
            },
            key=int,
        ),
        "failure_reasons": list(dict.fromkeys(failures)),
    }
    return validation, sorted(rows, key=lambda row: (row["block"], row["order"]))


def _case_row(
    manifest: dict[str, str],
    report: dict[str, Any],
    path: Path,
    results_root: Path,
) -> dict[str, Any]:
    base = screening._case_row(manifest, report, path, results_root)
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    records = _dict(common.get("records"))
    latency = _dict(common.get("latency_end_to_end"))
    producers = _dict(_dict(report.get("aggregated_metrics")).get("producers"))
    attempted = _integer(producers.get("messages_attempted"))
    delivered = _integer(producers.get("messages_delivered"))
    failed = _integer(producers.get("messages_failed"))
    missing = _integer(records.get("missing"))
    row = {
        **base,
        "block": int(manifest["block"]),
        "order": int(manifest["order"]),
        "repeat_index": int(manifest["block"]),
        "config_id": manifest["config_id"],
        "case_id": manifest["case_id"],
        "selection_categories": manifest["anchor_purpose"],
        "producer_ranks": int(manifest["producer_ranks"]),
        "consumer_ranks": int(manifest["consumer_ranks"]),
        "partitions": int(manifest["partitions"]),
        "payload_size_bytes": int(manifest["payload_size_bytes"]),
        "producer_mib_per_sec": _optional_number(throughput.get("producer_mib_per_sec")),
        "consumer_mib_per_sec": _optional_number(throughput.get("consumer_mib_per_sec")),
        "producer_records_per_sec": _optional_number(
            throughput.get("producer_records_per_sec")
        ),
        "consumer_records_per_sec": _optional_number(
            throughput.get("consumer_records_per_sec")
        ),
        "latency_p50_us": _optional_number(latency.get("p50_us")),
        "latency_p95_us": _optional_number(latency.get("p95_us")),
        "latency_p99_9_us": _optional_number(latency.get("p99_9_us")),
        "latency_mean_us": _optional_number(latency.get("mean_us")),
        "latency_max_us": _optional_number(latency.get("max_us")),
        "clock_uncertainty_us_max": _optional_number(
            latency.get("clock_uncertainty_us_max")
        ),
        "clock_drift_us_max": _optional_number(latency.get("clock_drift_us_max")),
        "records_attempted": attempted,
        "records_delivered": delivered,
        "failed_sends": failed,
        "missing_after_drain_percent": (
            100.0 * missing / delivered if delivered > 0 else math.inf
        ),
        "source_results_root": results_root.name,
    }
    row.pop("screening_order", None)
    row.pop("anchor", None)
    row.pop("anchor_purpose", None)
    row.pop("heap_gib", None)
    row.pop("direct_memory_gib", None)
    return row


def _reported_config_failures(
    manifest: dict[str, str], report: dict[str, Any]
) -> list[str]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config")) or _dict(case.get("config"))
    backend = _dict(_dict(config.get("backend_settings")))
    metadata = _dict(_dict(config.get("extra")).get("campaign_metadata"))
    expected = {
        "case_id": manifest["case_id"],
        "campaign_id": "pulsar-final-workload-validation",
        "producer_ranks": int(manifest["producer_ranks"]),
        "consumer_ranks": int(manifest["consumer_ranks"]),
        "payload_size_bytes": int(manifest["payload_size_bytes"]),
        "warmup_sec": 15,
        "duration_sec": 30,
        "drain_timeout_sec": 60,
        "latency_sample_every": 10,
    }
    failures = [
        f"reported configuration field {key} drifted"
        for key, value in expected.items()
        if config.get(key) != value
    ]
    if config.get("backend_id") != "pulsar":
        failures.append("reported backend ID drifted")
    if config.get("latency_enabled") is not True:
        failures.append("reported latency enablement drifted")
    if backend.get("partitions") != int(manifest["partitions"]):
        failures.append("reported partition count drifted")
    if backend.get("profile_id") != EXPECTED_PROFILE:
        failures.append("reported backend profile ID drifted")
    if backend.get("profile_sha256") != manifest["profile_sha256"]:
        failures.append("reported backend profile checksum drifted")
    if metadata.get("anchor_config_id") != manifest["config_id"]:
        failures.append("reported source configuration ID drifted")
    if metadata.get("stage") != "final-workload-validation":
        failures.append("reported campaign stage drifted")
    return failures


def _case_structural_failures(
    row: dict[str, Any], *, expected_profile_sha: str, manifest_profile_sha: str
) -> list[str]:
    failures: list[str] = []
    checks = (
        (row["status"] == "completed", "status is not completed"),
        (row["profile_id"] == EXPECTED_PROFILE, "profile ID drift"),
        (row["profile_sha256"] == manifest_profile_sha, "manifest profile checksum drift"),
        (row["profile_sha256"] == expected_profile_sha, "plan profile checksum drift"),
        (
            row["records_delivered"]
            == row["records_consumed"]
            + row["records_late_drained"]
            + row["records_missing_after_drain"],
            "delivered-record accounting does not balance",
        ),
    )
    failures.extend(reason for passed, reason in checks if not passed)

    for producer_name, consumer_name, balanced_name, label in (
        (
            "producer_mib_per_sec",
            "consumer_mib_per_sec",
            "balanced_mib_per_sec",
            "MiB/s",
        ),
        (
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
            "records/s",
        ),
    ):
        values = [row.get(producer_name), row.get(consumer_name), row.get(balanced_name)]
        if row["eligible"] and any(value is None or value < 0 for value in values):
            failures.append(f"eligible repeat has invalid {label}")
        if all(value is not None for value in values) and not math.isclose(
            float(values[2]),
            min(float(values[0]), float(values[1])),
            rel_tol=1e-9,
            abs_tol=1e-6,
        ):
            failures.append(f"balanced {label} does not equal producer/consumer minimum")

    if row["eligible"]:
        if row["backend_health"] != "healthy":
            failures.append("eligible repeat has unhealthy backend")
        if not row["latency_valid"]:
            failures.append("eligible repeat has invalid latency")
        if any(
            row[name] != 0
            for name in (
                "records_missing_after_drain",
                "duplicate_records",
                "out_of_order_records",
            )
        ):
            failures.append("eligible repeat has a correctness violation")
    expected_qualified = (
        row["eligible"]
        and row["producer_backlog_percent"] <= screening.BACKLOG_LIMIT_PERCENT
        and row["max_flush_duration_sec"] <= screening.FLUSH_LIMIT_SEC
        and row["failed_send_percent"] <= screening.FAILED_SEND_LIMIT_PERCENT
    )
    if row["qualified"] is not expected_qualified:
        failures.append(
            "qualification result does not match the shared Kafka/Pulsar "
            "application-level rule"
        )
    return failures


def summarize_configurations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for config_id in sorted({row["config_id"] for row in rows}):
        items = [row for row in rows if row["config_id"] == config_id]
        eligible = [row for row in items if row["eligible"]]
        correct = [
            row
            for row in items
            if row["records_missing_after_drain"] == 0
            and row["duplicate_records"] == 0
            and row["out_of_order_records"] == 0
        ]
        first = items[0]
        summaries.append(
            {
                "final_rank": None,
                "validated_recommendation": False,
                "config_id": config_id,
                "selection_categories": first["selection_categories"],
                "producer_ranks": first["producer_ranks"],
                "consumer_ranks": first["consumer_ranks"],
                "partitions": first["partitions"],
                "payload_size_bytes": first["payload_size_bytes"],
                "repeat_count": len(items),
                "completed_repeats": sum(row["status"] == "completed" for row in items),
                "healthy_repeats": sum(row["backend_health"] == "healthy" for row in items),
                "eligible_repeats": len(eligible),
                "qualified_repeats": sum(row["qualified"] for row in items),
                "latency_valid_repeats": sum(row["latency_valid"] for row in items),
                "correct_repeats": len(correct),
                "median_producer_mib_per_sec": _median_field(eligible, "producer_mib_per_sec"),
                "median_consumer_mib_per_sec": _median_field(eligible, "consumer_mib_per_sec"),
                "median_balanced_mib_per_sec": _median_field(eligible, "balanced_mib_per_sec"),
                "balanced_mib_per_sec_iqr": _iqr_field(eligible, "balanced_mib_per_sec"),
                "median_producer_records_per_sec": _median_field(eligible, "producer_records_per_sec"),
                "median_consumer_records_per_sec": _median_field(eligible, "consumer_records_per_sec"),
                "median_balanced_records_per_sec": _median_field(eligible, "balanced_records_per_sec"),
                "balanced_records_per_sec_iqr": _iqr_field(eligible, "balanced_records_per_sec"),
                "median_latency_p50_us": _median_field(eligible, "latency_p50_us"),
                "median_latency_p95_us": _median_field(eligible, "latency_p95_us"),
                "median_latency_p99_us": _median_field(eligible, "latency_p99_us"),
                "latency_p99_us_iqr": _iqr_field(eligible, "latency_p99_us"),
                "median_latency_p99_9_us": _median_field(eligible, "latency_p99_9_us"),
                "median_producer_backlog_percent": _median_field(
                    eligible, "producer_backlog_percent"
                ),
                "max_producer_backlog_percent": _max_field(
                    eligible, "producer_backlog_percent"
                ),
                "median_missing_after_drain_percent": _median_field(
                    eligible, "missing_after_drain_percent"
                ),
                "median_max_flush_duration_sec": _median_field(
                    eligible, "max_flush_duration_sec"
                ),
                "max_failed_send_percent": _max_field(eligible, "failed_send_percent"),
                "median_pulsar_bytes_in_peak_mib_per_sec": _median_field(
                    eligible, "pulsar_bytes_in_peak_mib_per_sec"
                ),
                "median_pulsar_bytes_out_peak_mib_per_sec": _median_field(
                    eligible, "pulsar_bytes_out_peak_mib_per_sec"
                ),
                "median_pulsar_process_rss_peak_gib": _median_field(
                    eligible, "pulsar_process_rss_peak_gib"
                ),
                "median_pulsar_jvm_heap_peak_gib": _median_field(
                    eligible, "pulsar_jvm_heap_peak_gib"
                ),
                "median_tmpfs_used_peak_percent": _median_field(
                    eligible, "tmpfs_used_peak_percent"
                ),
            }
        )
    return summaries


def rank_summaries(rows: list[dict[str, Any]]) -> None:
    rows.sort(key=_ranking_key)
    for rank, row in enumerate(rows, start=1):
        row["final_rank"] = rank


def _ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(row["qualified_repeats"]),
        -_sortable_high(row["median_balanced_mib_per_sec"]),
        _sortable_low(row["balanced_mib_per_sec_iqr"]),
        _sortable_low(row["median_producer_backlog_percent"]),
        _sortable_low(row["median_max_flush_duration_sec"]),
        _sortable_low(row["max_failed_send_percent"]),
        row["config_id"],
    )


def _recommendation(
    summaries: list[dict[str, Any]],
    validation: dict[str, Any],
    args: argparse.Namespace,
    plan: dict[str, Any],
) -> dict[str, Any]:
    leader = summaries[0] if summaries else None
    fully_validated = bool(
        leader
        and leader["eligible_repeats"] == 5
        and leader["qualified_repeats"] == 5
    )
    status = (
        "validated_recommendation"
        if fully_validated
        else "best_observed_but_not_fully_validated"
        if leader and leader["eligible_repeats"] > 0 and leader["qualified_repeats"] > 0
        else "no_sustainable_recommendation"
    )
    return {
        "format": "messaging-benchmark.pulsar-final-validation-recommendation.v1",
        "status": status,
        "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "config_id": leader["config_id"] if leader else None,
        "profile_id": plan.get("profile_id"),
        "profile_sha256": plan.get("profile_sha256"),
        "eligible_repeats": leader["eligible_repeats"] if leader else 0,
        "qualified_repeats": leader["qualified_repeats"] if leader else 0,
        "median_balanced_mib_per_sec": (
            leader["median_balanced_mib_per_sec"] if leader else None
        ),
        "median_balanced_records_per_sec": (
            leader["median_balanced_records_per_sec"] if leader else None
        ),
        "median_latency_p99_us": leader["median_latency_p99_us"] if leader else None,
        "balanced_mib_per_sec_iqr": leader["balanced_mib_per_sec_iqr"] if leader else None,
        "median_producer_backlog_percent": (
            leader["median_producer_backlog_percent"] if leader else None
        ),
        "median_max_flush_duration_sec": (
            leader["median_max_flush_duration_sec"] if leader else None
        ),
        "max_failed_send_percent": (
            leader["max_failed_send_percent"] if leader else None
        ),
        "ranking_rule": [
            "qualified repeats descending",
            "median balanced MiB/s descending",
            "balanced-throughput IQR ascending",
            "median producer backlog percent ascending",
            "median maximum flush duration ascending",
            "maximum failed-send percent ascending",
            "configuration ID ascending",
        ],
        "historical_plan_ranking_rule": plan.get("ranking_rule", []),
        "historical_plan_ranking_rule_status": (
            "superseded_not_applied; retained only as campaign-plan provenance"
        ),
        "workload_set_status": "historically_executed_pre_correction_shortlist",
        "retrospectively_corrected_phase1_shortlist": list(
            RETROSPECTIVELY_CORRECTED_PHASE1_SHORTLIST
        ),
        "interpretation": (
            "The recommendation is the highest-ranked repeated workload under "
            "the frozen single-standalone profile. It is not a universal Pulsar "
            "optimum. Eligibility is a prerequisite for repeat aggregation, and "
            "p99 latency is reported separately rather than used to rank workloads."
        ),
        "source_evidence": {
            "manifest_sha256": _sha256(args.manifest),
            "plan_sha256": _sha256(args.plan),
            "results_roots": validation["results_roots"],
            "slurm_job_ids": validation["slurm_job_ids"],
        },
    }


def _write_markdown(
    path: Path,
    summaries: list[dict[str, Any]],
    recommendation: dict[str, Any],
    validation: dict[str, Any],
) -> None:
    lines = [
        "# Pulsar Final Repeated Workload Validation",
        "",
        "Ten historically selected Phase 1 configurations ran once in each of five "
        "fixed-seed randomized blocks under the frozen `BASELINE_H16_D32` "
        "profile. Medians and interquartile ranges (IQR = Q3 - Q1) are "
        "calculated from each configuration's eligible individual repeats.",
        "",
        "This is the workload set actually executed before the Phase 1 qualification "
        "correction. It is distinct from the retrospectively corrected shortlist; "
        "newly shortlisted configurations were not validated.",
        "",
        f"- Required reports: {validation['observed_case_count']}/50",
        f"- Eligible repeats: {validation['eligible_case_count']}/50",
        f"- Qualified repeats: {validation['qualified_case_count']}/50",
        f"- Recommendation status: `{recommendation['status']}`",
        f"- Recommended configuration: `{recommendation.get('config_id')}`",
        "",
        "A repeat qualifies only if it is eligible, producer backlog at flush start "
        "is at most 5% of measurement-period publication attempts, maximum "
        "producer flush is at most 10 "
        "seconds, and failed publications are at most 0.1%. Post-drain missing, "
        "duplicate, and out-of-order records remain eligibility checks. Only "
        "eligible repeats enter aggregate metrics. Ranking uses qualified-repeat "
        "count, median balanced MiB/s, throughput IQR, backlog, flush, failed-send "
        "evidence, and configuration ID in that order. p99 latency is secondary.",
        "",
        "| Rank | Config | Eligible | Qualified | Median MiB/s | IQR MiB/s | Median records/s | Median p99 (s) | Median backlog % | Median flush s | Max failed % |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            f"| {row['final_rank']} | `{row['config_id']}` | "
            f"{row['eligible_repeats']}/5 | {row['qualified_repeats']}/5 | "
            f"{_fmt(row['median_balanced_mib_per_sec'])} | "
            f"{_fmt(row['balanced_mib_per_sec_iqr'])} | "
            f"{_fmt_integer(row['median_balanced_records_per_sec'])} | "
            f"{_fmt_seconds(row['median_latency_p99_us'])} | "
            f"{_fmt(row['median_producer_backlog_percent'])} | "
            f"{_fmt(row['median_max_flush_duration_sec'])} | "
            f"{_fmt(row['max_failed_send_percent'])} |"
        )
    lines.extend(["", recommendation["interpretation"], ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_incomplete_report(path: Path, validation: dict[str, Any]) -> None:
    lines = ["# Pulsar Final Validation Incomplete", ""]
    lines.extend(f"- {reason}" for reason in validation["failure_reasons"])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_figure(output_dir: Path, rows: list[dict[str, Any]]) -> bool:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    required_fields = (
        "median_balanced_mib_per_sec",
        "balanced_mib_per_sec_iqr",
        "median_latency_p99_us",
    )
    if any(row.get(field) is None for row in rows for field in required_fields):
        return False
    ordered = sorted(rows, key=lambda row: row["final_rank"], reverse=True)
    labels = [row["config_id"] for row in ordered]
    positions = list(range(len(ordered)))
    throughput = [row["median_balanced_mib_per_sec"] for row in ordered]
    throughput_iqr = [row["balanced_mib_per_sec_iqr"] for row in ordered]
    latency = [
        row["median_latency_p99_us"] / 1_000_000.0 for row in ordered
    ]
    qualified = [row["qualified_repeats"] for row in ordered]
    colors = ["#166534" if row["final_rank"] == 1 else "#1d4ed8" for row in ordered]
    fig, axes = plt.subplots(1, 3, figsize=(12.2, 5.2), constrained_layout=True)
    axes[0].barh(positions, qualified, color=colors)
    axes[0].set_xlim(0, 5.25)
    axes[0].set_xlabel("Qualified repeats (out of 5)")
    axes[1].errorbar(
        throughput,
        positions,
        xerr=[value / 2.0 for value in throughput_iqr],
        fmt="o",
        color="#1d4ed8",
        ecolor="#64748b",
        capsize=3,
    )
    axes[1].set_xlabel("Median balanced MiB/s (half-IQR bars)")
    axes[2].scatter(latency, positions, c=colors, s=46)
    axes[2].set_xlabel("Median producer-to-consumer p99 (s)")
    for axis in axes:
        axis.set_yticks(positions, labels)
        axis.grid(axis="x", alpha=0.22)
        axis.set_axisbelow(True)
    fig.savefig(output_dir / "pulsar_final_validation_summary.pdf", bbox_inches="tight")
    fig.savefig(
        output_dir / "pulsar_final_validation_summary.png",
        dpi=180,
        bbox_inches="tight",
    )
    plt.close(fig)
    return True


def _median_field(rows: list[dict[str, Any]], field: str) -> float | None:
    values = _values(rows, field)
    return statistics.median(values) if values else None


def _iqr_field(rows: list[dict[str, Any]], field: str) -> float | None:
    values = _values(rows, field)
    return _iqr(values) if values else None


def _max_field(rows: list[dict[str, Any]], field: str) -> float | None:
    values = _values(rows, field)
    return max(values) if values else None


def _values(rows: list[dict[str, Any]], field: str) -> list[float]:
    return [
        float(row[field])
        for row in rows
        if row.get(field) is not None and math.isfinite(float(row[field]))
    ]


def _iqr(values: Iterable[float]) -> float:
    ordered = sorted(float(value) for value in values)
    return _quantile(ordered, 0.75) - _quantile(ordered, 0.25)


def _quantile(ordered: list[float], probability: float) -> float:
    if not ordered:
        raise ValueError("quantile requires at least one value")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (position - lower) * (ordered[upper] - ordered[lower])


def _sortable_high(value: Any) -> float:
    return float(value) if value is not None and math.isfinite(float(value)) else -math.inf


def _sortable_low(value: Any) -> float:
    return float(value) if value is not None and math.isfinite(float(value)) else math.inf


def _write_csv(path: Path, fields: Iterable[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


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


def _fmt(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def _fmt_integer(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):,.0f}"


def _fmt_seconds(value_us: Any) -> str:
    return "n/a" if value_us is None else f"{float(value_us) / 1_000_000.0:.3f}"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
