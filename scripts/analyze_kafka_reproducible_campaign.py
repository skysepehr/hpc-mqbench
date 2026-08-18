#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
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


MEASUREMENT_CONTRACT_ID = "measurement.messaging.reproducible.v1"
ANALYSIS_FORMAT = "messaging-benchmark.reproducible-analysis.v1"
CONFIG_FIELDS = (
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "batch_size",
    "linger_ms",
    "payload_size_bytes",
    "producer_queue_messages",
    "producer_queue_kbytes",
    "consumer_fetch_min_bytes",
    "consumer_fetch_wait_max_ms",
    "consumer_fetch_message_max_bytes",
)
CASE_FIELDS = (
    "primary_rank",
    "raw_throughput_rank",
    "config_id",
    "case_id",
    "workload_config_id",
    "block",
    "block_order",
    "status",
    "eligible",
    "is_qualified",
    "qualified",
    "qualification_status",
    "qualification_reason",
    "qualification_policy_id",
    "measurement_contract_id",
    "throughput_unit",
    "producer_mib_per_sec",
    "consumer_mib_per_sec",
    "balanced_mib_per_sec",
    "producer_delivered_MBps",
    "consumer_received_MBps",
    "balanced_app_MBps",
    "producer_records_per_sec",
    "consumer_records_per_sec",
    "balanced_records_per_sec",
    "messages_attempted",
    "messages_enqueued",
    "pending_messages_at_flush_start",
    "backlog_denominator",
    "backlog_denominator_value",
    "pending_backlog_percent",
    "flush_sec",
    "messages_failed",
    "failed_send_percent",
    "latency_valid",
    "latency_sample_count",
    "latency_p50_us",
    "latency_p95_us",
    "latency_p99_us",
    "latency_p99_9_us",
    "clock_uncertainty_us_max",
    "clock_drift_us_max",
    "records_published",
    "records_consumed",
    "records_late_drained",
    "records_missing_after_drain",
    "duplicate_records",
    "out_of_order_records",
    "invalid_envelope_records",
    "surplus_records",
    *CONFIG_FIELDS,
    "manifest_note",
    "source_report",
    "source_report_sha256",
)
SHORTLIST_FIELDS = (
    "selection_order",
    "config_id",
    "selection_categories",
    "primary_rank",
    "raw_throughput_rank",
    "qualification_status",
    "balanced_mib_per_sec",
    "balanced_records_per_sec",
    "pending_backlog_percent",
    "flush_sec",
    "failed_send_percent",
    "payload_size_bytes",
    "manifest_note",
)
VALIDATION_SUMMARY_FIELDS = (
    "validation_rank",
    "original_config_id",
    "repeats",
    "eligible_count",
    "ineligible_count",
    "qualified_count",
    "qualified_frequency",
    "median_balanced_mib_per_sec",
    "median_balanced_app_MBps",
    "median_balanced_records_per_sec",
    "mean_balanced_mib_per_sec",
    "mean_balanced_app_MBps",
    "min_balanced_mib_per_sec",
    "min_balanced_app_MBps",
    "max_balanced_mib_per_sec",
    "max_balanced_app_MBps",
    "iqr_balanced_mib_per_sec",
    "iqr_balanced_app_MBps",
    "stdev_balanced_mib_per_sec",
    "stdev_balanced_app_MBps",
    "cv_balanced_mib_per_sec",
    "cv_balanced_app_MBps",
    "median_producer_mib_per_sec",
    "median_producer_delivered_MBps",
    "median_consumer_mib_per_sec",
    "median_consumer_received_MBps",
    "median_producer_records_per_sec",
    "median_consumer_records_per_sec",
    "median_pending_backlog_percent",
    "max_pending_backlog_percent",
    "median_flush_sec",
    "max_flush_sec",
    "median_failed_send_percent",
    "max_failed_send_percent",
    "median_latency_p99_us",
    "throughput_unit",
    "qualified_blocks",
    "case_ids",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate and aggregate a reproducibly instrumented backend Phase 1 "
            "or repeated-validation campaign."
        )
    )
    parser.add_argument("--backend", choices=("kafka", "pulsar"), default="kafka")
    parser.add_argument("--stage", choices=("phase1", "validation"), required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--results-root", action="append", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = _read_manifest(args.manifest, args.stage)
    reports, duplicates = _discover_reports(args.results_root, args.backend)
    rows: list[dict[str, Any]] = []
    failures: list[str] = []

    if duplicates:
        failures.append(
            "duplicate final reports for case IDs: " + ", ".join(duplicates)
        )
    expected_ids = {_manifest_case_id(row, args.stage) for row in manifest}
    missing_ids = sorted(expected_ids - set(reports))
    if missing_ids:
        failures.append(
            f"missing {len(missing_ids)} final report(s): "
            + ", ".join(missing_ids[:10])
            + (" ..." if len(missing_ids) > 10 else "")
        )

    for manifest_row in manifest:
        case_id = _manifest_case_id(manifest_row, args.stage)
        candidate = reports.get(case_id)
        if candidate is None:
            continue
        path, report, source_root = candidate
        row, row_failures = _case_row(
            manifest_row,
            report,
            path,
            source_root,
            stage=args.stage,
        )
        rows.append(row)
        failures.extend(f"{case_id}: {reason}" for reason in row_failures)

    if args.stage == "phase1":
        _rank_phase1(rows)
    else:
        rows.sort(
            key=lambda row: (
                _integer(row.get("block")),
                _integer(row.get("block_order")),
                str(row.get("case_id", "")),
            )
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    validation = {
        "format": f"{ANALYSIS_FORMAT}.validation",
        "backend_id": args.backend,
        "stage": args.stage,
        "valid": not failures,
        "expected_case_count": len(manifest),
        "observed_case_count": len(rows),
        "completed_case_count": sum(row["status"] == "completed" for row in rows),
        "eligible_case_count": sum(bool(row["eligible"]) for row in rows),
        "ineligible_case_count": sum(not bool(row["eligible"]) for row in rows),
        "ineligible_case_ids": [
            str(row["case_id"]) for row in rows if not bool(row["eligible"])
        ],
        "qualified_case_count": sum(bool(row["qualified"]) for row in rows),
        "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
        "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
        "backlog_event": "pending producer delivery callbacks at flush start",
        "backlog_denominator": "measurement-period send attempts",
        "failure_reasons": list(dict.fromkeys(failures)),
    }
    _write_json(args.output_dir / "campaign_validation.json", validation)
    _write_csv(args.output_dir / "campaign_cases.csv", CASE_FIELDS, rows)
    _write_json(
        args.output_dir / "campaign_cases.json",
        {
            "format": ANALYSIS_FORMAT,
            "stage": args.stage,
            "case_count": len(rows),
            "cases": rows,
        },
    )

    if args.stage == "phase1":
        _write_phase1_compatibility_outputs(args.output_dir, rows)
        if not failures and len(manifest) == 120 and len(rows) == 120:
            _write_phase1_selection(args.output_dir, rows)
    else:
        summaries = _summarize_validation(rows)
        _write_validation_outputs(args.output_dir, rows, summaries)

    _write_checksums(args.output_dir)
    print(f"[reproducible-analysis] backend: {args.backend}")
    print(f"[reproducible-analysis] stage: {args.stage}")
    print(f"[reproducible-analysis] expected cases: {len(manifest)}")
    print(f"[reproducible-analysis] observed cases: {len(rows)}")
    print(f"[reproducible-analysis] valid: {validation['valid']}")
    if failures:
        for reason in list(dict.fromkeys(failures)):
            print(f"[reproducible-analysis] {reason}", file=sys.stderr)
        return 1
    return 0


def _read_manifest(path: Path, stage: str) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    required = {"config_id", "config_path"}
    if stage == "validation":
        required.update({"block", "order"})
    missing = required - set(rows[0] if rows else {})
    if missing:
        raise ValueError(
            f"{path}: empty manifest or missing fields: {', '.join(sorted(missing))}"
        )
    if stage == "validation":
        unidentified = [
            _manifest_case_id(row, stage)
            for row in rows
            if not _manifest_workload_config_id(row)
        ]
        if unidentified:
            raise ValueError(
                f"{path}: validation rows do not identify their source workload: "
                + ", ".join(unidentified[:10])
            )
    case_ids = [_manifest_case_id(row, stage) for row in rows]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError(f"{path}: duplicate config_id values")
    return rows


def _discover_reports(
    roots: list[Path],
    backend_id: str = "kafka",
) -> tuple[dict[str, tuple[Path, dict[str, Any], Path]], list[str]]:
    candidates: dict[str, list[tuple[Path, dict[str, Any], Path]]] = defaultdict(list)
    duplicates: set[str] = set()
    for root in roots:
        if not root.exists():
            raise FileNotFoundError(f"results root does not exist: {root}")
        paths = [root] if root.is_file() else sorted(root.rglob("final_report.json"))
        for path in paths:
            report = _read_json(path)
            config = _dict(report.get("config"))
            case = _dict(report.get("case"))
            system = _dict(report.get("system_under_test"))
            if str(system.get("backend_id") or config.get("backend_id")) != backend_id:
                continue
            case_id = str(case.get("case_id") or config.get("case_id") or "").strip()
            if not case_id:
                continue
            candidates[case_id].append((path, report, root))

    selected: dict[str, tuple[Path, dict[str, Any], Path]] = {}
    for case_id, values in candidates.items():
        values.sort(
            key=lambda item: (
                int(_dict(item[1].get("case")).get("status") == "completed"),
                *_report_preference(item[0]),
            ),
            reverse=True,
        )
        preferred = values[0]
        preferred_hash = _canonical_json_sha256(preferred[1])
        conflicts = [
            path
            for path, report, _ in values[1:]
            if _dict(report.get("case")).get("status") == "completed"
            and _dict(preferred[1].get("case")).get("status") == "completed"
            if path.parent.name not in {"data", "reports"}
            and _canonical_json_sha256(report) != preferred_hash
        ]
        if conflicts:
            duplicates.add(case_id)
        selected[case_id] = preferred
    return selected, sorted(duplicates)


def _case_row(
    manifest: dict[str, str],
    report: dict[str, Any],
    path: Path,
    source_root: Path,
    *,
    stage: str,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    common = _dict(report.get("common_metrics"))
    throughput = _dict(common.get("throughput"))
    records = _dict(common.get("records"))
    latency = _dict(common.get("latency_end_to_end"))
    common_qualification = _dict(common.get("qualification"))
    contract = _dict(common.get("measurement_contract"))
    delivery = _dict(common.get("producer_delivery"))
    eligibility = _dict(report.get("eligibility"))
    aggregated = _dict(report.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    correctness = _dict(aggregated.get("record_correctness"))
    extra = _dict(config.get("extra"))
    producer_config = _dict(extra.get("kafka_producer_config"))
    consumer_config = _dict(extra.get("kafka_consumer_config"))
    campaign_metadata = _dict(extra.get("campaign_metadata"))

    if report.get("result_schema_version") != "messaging-benchmark.result.v1":
        failures.append("portable result schema is missing or unsupported")
    if str(config.get("qualification_policy_id")) != COMMON_QUALIFICATION_POLICY_ID:
        failures.append("common qualification policy is not recorded")
    contract_id = str(contract.get("id") or campaign_metadata.get("measurement_contract_id") or "")
    if contract_id != MEASUREMENT_CONTRACT_ID:
        failures.append("reproducible measurement contract is not recorded")

    operational = producer_operational_metrics(
        producers,
        backlog_denominator=BACKLOG_DENOMINATOR_ATTEMPTED,
    )
    if delivery.get("backlog_denominator") != BACKLOG_DENOMINATOR_ATTEMPTED:
        failures.append("backlog denominator is not measurement-period send attempts")
    _compare_number(
        failures,
        "backlog percentage",
        delivery.get("backlog_percent"),
        operational["producer_backlog_percent"],
    )
    _compare_number(
        failures,
        "failed-send percentage",
        delivery.get("failed_send_percent"),
        operational["failed_send_percent"],
    )

    producer_mib = _required_number(throughput, "producer_mib_per_sec", failures)
    consumer_mib = _required_number(throughput, "consumer_mib_per_sec", failures)
    balanced_mib = _required_number(throughput, "balanced_mib_per_sec", failures)
    producer_rps = _required_number(throughput, "producer_records_per_sec", failures)
    consumer_rps = _required_number(throughput, "consumer_records_per_sec", failures)
    balanced_rps = _required_number(throughput, "balanced_records_per_sec", failures)
    if _finite(producer_mib) and _finite(consumer_mib):
        _compare_number(failures, "balanced MiB/s", balanced_mib, min(producer_mib, consumer_mib))
    if _finite(producer_rps) and _finite(consumer_rps):
        _compare_number(
            failures,
            "balanced records/s",
            balanced_rps,
            min(producer_rps, consumer_rps),
        )
    for label, value in (
        ("producer MiB/s", producer_mib),
        ("consumer MiB/s", consumer_mib),
        ("balanced MiB/s", balanced_mib),
        ("producer records/s", producer_rps),
        ("consumer records/s", consumer_rps),
        ("balanced records/s", balanced_rps),
    ):
        if not _finite(value) or value < 0:
            failures.append(f"{label} is missing, non-finite, or negative")

    status = str(case.get("status", "unknown"))
    eligible = common_qualification.get("eligible") is True
    qualified = common_qualification.get("qualified") is True
    expected_qualified = eligible and bool(operational["thresholds_satisfied"])
    if qualified != expected_qualified:
        failures.append("qualification result does not match the shared thresholds")
    latency_valid = latency.get("valid") is True
    if bool(config.get("latency_enabled")) and eligible and not latency_valid:
        failures.append("eligible run has invalid end-to-end latency")

    correctness_values = {
        "records_missing_after_drain": _integer(correctness.get("missing_after_drain_records"), -1),
        "duplicate_records": _integer(correctness.get("duplicate_offset_count"), -1),
        "out_of_order_records": _integer(correctness.get("out_of_order_offset_count"), -1),
        "invalid_envelope_records": _integer(correctness.get("invalid_envelope_count"), -1),
        "surplus_records": _integer(correctness.get("unexplained_surplus_records"), -1),
    }
    if eligible and any(value != 0 for value in correctness_values.values()):
        failures.append("eligible run has a correctness-accounting violation")

    reasons = [
        str(item)
        for item in eligibility.get("failure_reasons", [])
        if str(item).strip()
    ]
    reasons.extend(producer_operational_reasons(operational))
    if not reasons:
        reasons.append("eligible and all shared qualification thresholds satisfied")

    workload_config_id = str(
        _manifest_workload_config_id(manifest)
        or extra.get("sweep_config_id")
        or manifest.get("config_id")
    )
    config_values = {
        "producer_ranks": _integer(config.get("producer_ranks")),
        "consumer_ranks": _integer(config.get("consumer_ranks")),
        "partitions": _integer(config.get("partitions")),
        "batch_size": _integer(config.get("batch_size")),
        "linger_ms": _integer(config.get("linger_ms")),
        "payload_size_bytes": _integer(config.get("payload_size_bytes")),
        "producer_queue_messages": _integer(
            producer_config.get("queue.buffering.max.messages")
        ),
        "producer_queue_kbytes": _integer(
            producer_config.get("queue.buffering.max.kbytes")
        ),
        "consumer_fetch_min_bytes": _integer(consumer_config.get("fetch.min.bytes")),
        "consumer_fetch_wait_max_ms": _integer(
            consumer_config.get("fetch.wait.max.ms")
        ),
        "consumer_fetch_message_max_bytes": _integer(
            consumer_config.get("fetch.message.max.bytes")
        ),
    }
    row = {
        "primary_rank": None,
        "raw_throughput_rank": None,
        "config_id": workload_config_id if stage == "validation" else manifest["config_id"],
        "case_id": _manifest_case_id(manifest, stage),
        "workload_config_id": workload_config_id,
        "block": _integer(manifest.get("block")),
        "block_order": _integer(manifest.get("order")),
        "status": status,
        "eligible": eligible,
        "is_qualified": qualified,
        "qualified": qualified,
        "qualification_status": _qualification_status(eligible, qualified),
        "qualification_reason": "; ".join(dict.fromkeys(reasons)),
        "qualification_policy_id": str(config.get("qualification_policy_id", "")),
        "measurement_contract_id": contract_id,
        "throughput_unit": "MiB/s",
        "producer_mib_per_sec": producer_mib,
        "consumer_mib_per_sec": consumer_mib,
        "balanced_mib_per_sec": balanced_mib,
        # Compatibility aliases. Their unit is declared by throughput_unit.
        "producer_delivered_MBps": producer_mib,
        "consumer_received_MBps": consumer_mib,
        "balanced_app_MBps": balanced_mib,
        "producer_records_per_sec": producer_rps,
        "consumer_records_per_sec": consumer_rps,
        "balanced_records_per_sec": balanced_rps,
        "messages_attempted": int(operational["messages_attempted"]),
        "messages_enqueued": int(operational["messages_enqueued"]),
        "pending_messages_at_flush_start": int(
            operational["pending_messages_at_flush_start"]
        ),
        "backlog_denominator": str(operational["backlog_denominator"]),
        "backlog_denominator_value": int(operational["backlog_denominator_value"]),
        "pending_backlog_percent": _optional_number(
            operational["producer_backlog_percent"]
        ),
        "flush_sec": _optional_number(operational["max_flush_duration_sec"]),
        "messages_failed": int(operational["messages_failed"]),
        "failed_send_percent": _optional_number(
            operational["failed_send_percent"]
        ),
        "latency_valid": latency_valid,
        "latency_sample_count": _integer(latency.get("count")),
        "latency_p50_us": _optional_number(latency.get("p50_us")),
        "latency_p95_us": _optional_number(latency.get("p95_us")),
        "latency_p99_us": _optional_number(latency.get("p99_us")),
        "latency_p99_9_us": _optional_number(latency.get("p99_9_us")),
        "clock_uncertainty_us_max": _optional_number(
            latency.get("clock_uncertainty_us_max")
        ),
        "clock_drift_us_max": _optional_number(latency.get("clock_drift_us_max")),
        "records_published": _integer(records.get("published")),
        "records_consumed": _integer(records.get("consumed")),
        "records_late_drained": _integer(records.get("late_drained")),
        **correctness_values,
        **config_values,
        "manifest_note": str(manifest.get("notes", "")),
        "source_report": _display_path(path, source_root),
        "source_report_sha256": _sha256(path),
    }
    return row, failures


def _rank_phase1(rows: list[dict[str, Any]]) -> None:
    rows.sort(key=_phase1_key)
    for rank, row in enumerate(rows, start=1):
        row["primary_rank"] = rank
    for rank, row in enumerate(
        sorted(
            rows,
            key=lambda item: (
                -_number(item.get("balanced_mib_per_sec"), -math.inf),
                str(item.get("config_id", "")),
            ),
        ),
        start=1,
    ):
        row["raw_throughput_rank"] = rank


def _phase1_key(row: dict[str, Any]) -> tuple[Any, ...]:
    status_order = {"qualified": 0, "overdriven": 1, "ineligible": 2}
    return (
        status_order.get(str(row.get("qualification_status")), 3),
        -_number(row.get("balanced_mib_per_sec"), -math.inf),
        _number(row.get("pending_backlog_percent"), math.inf),
        _number(row.get("flush_sec"), math.inf),
        _number(row.get("failed_send_percent"), math.inf),
        str(row.get("config_id", "")),
    )


def _summarize_validation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["workload_config_id"])].append(row)
    summaries: list[dict[str, Any]] = []
    for config_id, repeats in grouped.items():
        eligible = [row for row in repeats if row["eligible"]]
        throughput = [float(row["balanced_mib_per_sec"]) for row in eligible]
        producer_throughput = [float(row["producer_mib_per_sec"]) for row in eligible]
        consumer_throughput = [float(row["consumer_mib_per_sec"]) for row in eligible]
        record_throughput = [float(row["balanced_records_per_sec"]) for row in eligible]
        producer_rps = [float(row["producer_records_per_sec"]) for row in eligible]
        consumer_rps = [float(row["consumer_records_per_sec"]) for row in eligible]
        backlog = [float(row["pending_backlog_percent"]) for row in eligible]
        flush = [float(row["flush_sec"]) for row in eligible]
        failed = [float(row["failed_send_percent"]) for row in eligible]
        p99 = [
            float(row["latency_p99_us"])
            for row in eligible
            if _finite(row.get("latency_p99_us"))
        ]
        qualified = [row for row in eligible if row["qualified"]]
        stdev = statistics.stdev(throughput) if len(throughput) > 1 else 0.0
        mean = statistics.fmean(throughput) if throughput else None
        iqr_value = _iqr(throughput)
        summary = {
            "validation_rank": None,
            "original_config_id": config_id,
            "repeats": len(repeats),
            "eligible_count": len(eligible),
            "ineligible_count": len(repeats) - len(eligible),
            "qualified_count": len(qualified),
            "qualified_frequency": len(qualified) / len(repeats) if repeats else 0.0,
            "median_balanced_mib_per_sec": _median(throughput),
            "median_balanced_app_MBps": _median(throughput),
            "median_balanced_records_per_sec": _median(record_throughput),
            "mean_balanced_mib_per_sec": mean,
            "mean_balanced_app_MBps": mean,
            "min_balanced_mib_per_sec": min(throughput) if throughput else None,
            "min_balanced_app_MBps": min(throughput) if throughput else None,
            "max_balanced_mib_per_sec": max(throughput) if throughput else None,
            "max_balanced_app_MBps": max(throughput) if throughput else None,
            "iqr_balanced_mib_per_sec": iqr_value,
            "iqr_balanced_app_MBps": iqr_value,
            "stdev_balanced_mib_per_sec": stdev if throughput else None,
            "stdev_balanced_app_MBps": stdev if throughput else None,
            "cv_balanced_mib_per_sec": stdev / mean if mean and mean > 0 else None,
            "cv_balanced_app_MBps": stdev / mean if mean and mean > 0 else None,
            "median_producer_mib_per_sec": _median(producer_throughput),
            "median_producer_delivered_MBps": _median(producer_throughput),
            "median_consumer_mib_per_sec": _median(consumer_throughput),
            "median_consumer_received_MBps": _median(consumer_throughput),
            "median_producer_records_per_sec": _median(producer_rps),
            "median_consumer_records_per_sec": _median(consumer_rps),
            "median_pending_backlog_percent": _median(backlog),
            "max_pending_backlog_percent": max(backlog) if backlog else None,
            "median_flush_sec": _median(flush),
            "max_flush_sec": max(flush) if flush else None,
            "median_failed_send_percent": _median(failed),
            "max_failed_send_percent": max(failed) if failed else None,
            "median_latency_p99_us": _median(p99),
            "throughput_unit": "MiB/s",
            "qualified_blocks": ",".join(
                str(row["block"]) for row in qualified
            ),
            "case_ids": ",".join(str(row["case_id"]) for row in repeats),
        }
        summaries.append(summary)
    summaries.sort(key=_validation_key)
    for rank, row in enumerate(summaries, start=1):
        row["validation_rank"] = rank
    return summaries


def _validation_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -_integer(row.get("qualified_count")),
        -_number(row.get("median_balanced_mib_per_sec"), -math.inf),
        _number(row.get("iqr_balanced_mib_per_sec"), math.inf),
        _number(row.get("median_pending_backlog_percent"), math.inf),
        _number(row.get("median_flush_sec"), math.inf),
        _number(row.get("median_failed_send_percent"), math.inf),
        str(row.get("original_config_id", "")),
    )


def _write_phase1_compatibility_outputs(
    output_dir: Path,
    rows: list[dict[str, Any]],
) -> None:
    _write_csv(output_dir / "sweep_summary.csv", CASE_FIELDS, rows)
    _write_json(
        output_dir / "sweep_summary.json",
        {
            "format": ANALYSIS_FORMAT,
            "throughput_unit": "MiB/s",
            "case_count": len(rows),
            "completed_count": sum(row["status"] == "completed" for row in rows),
            "rows": rows,
        },
    )


def _write_phase1_selection(
    output_dir: Path,
    rows: list[dict[str, Any]],
) -> None:
    shortlist, anchors = _select_phase1_workloads(rows)
    csv_rows = [
        {
            **item,
            "selection_categories": ",".join(item["selection_categories"]),
        }
        for item in shortlist
    ]
    _write_csv(
        output_dir / "validation_shortlist.csv",
        SHORTLIST_FIELDS,
        csv_rows,
    )
    _write_json(
        output_dir / "validation_shortlist.json",
        {
            "format": "messaging-benchmark.kafka-phase1-selection.v1",
            "source_stage": "phase1",
            "source_case_count": len(rows),
            "selection_policy": (
                "qualification-first leaders plus boundary, overload, "
                "record-rate, baseline, and controlled one-factor evidence"
            ),
            "shortlist": shortlist,
            "v2_profile_anchors": anchors,
            "v2_profile_anchors_ready": len(anchors) == 4,
        },
    )


def _select_phase1_workloads(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(rows) != 120:
        raise ValueError("Phase 1 selection requires exactly 120 analyzed cases")

    ranked = sorted(rows, key=_phase1_key)
    qualified = [row for row in ranked if row["qualification_status"] == "qualified"]
    eligible = [row for row in ranked if row["eligible"]]
    overdriven = [
        row for row in ranked if row["qualification_status"] == "overdriven"
    ]
    selected: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any] | None, category: str) -> None:
        if row is None:
            return
        config_id = str(row["config_id"])
        if config_id not in selected:
            selected[config_id] = _selection_record(row)
        categories = selected[config_id]["selection_categories"]
        if category not in categories:
            categories.append(category)

    for row in qualified[:3]:
        add(row, "qualified_leader")

    boundary_candidates = [
        row
        for row in overdriven
        if _number(row.get("pending_backlog_percent"), math.inf)
        > BACKLOG_LIMIT_PERCENT
        and _number(row.get("flush_sec"), math.inf) <= FLUSH_LIMIT_SEC
        and _number(row.get("failed_send_percent"), math.inf)
        <= FAILED_SEND_LIMIT_PERCENT
    ]
    boundary_candidates.sort(
        key=lambda row: (
            _number(row.get("pending_backlog_percent"), math.inf)
            - BACKLOG_LIMIT_PERCENT,
            -_number(row.get("balanced_mib_per_sec"), -math.inf),
            str(row.get("config_id", "")),
        )
    )
    add(boundary_candidates[0] if boundary_candidates else None, "closest_backlog_boundary")
    add(overdriven[0] if overdriven else None, "high_throughput_transition")
    add(
        min(eligible, key=lambda row: _integer(row.get("raw_throughput_rank"))),
        "raw_throughput_upper_bound",
    )
    add(
        max(
            eligible,
            key=lambda row: (
                _number(row.get("balanced_records_per_sec"), -math.inf),
                -_integer(row.get("primary_rank")),
            ),
            default=None,
        ),
        "record_rate_pressure",
    )
    add(
        next(
            (row for row in eligible if row.get("manifest_note") == "baseline"),
            None,
        ),
        "manifest_baseline",
    )
    add(
        next(
            (
                row
                for row in eligible
                if str(row.get("manifest_note", "")).startswith("one-factor:")
            ),
            None,
        ),
        "controlled_one_factor",
    )
    for row in eligible:
        if len(selected) >= 10:
            break
        add(row, "qualification_rank_fill")
    if len(selected) != 10:
        raise ValueError(f"Phase 1 selection produced {len(selected)} workloads, expected 10")

    shortlist = list(selected.values())
    for order, item in enumerate(shortlist, start=1):
        item["selection_order"] = order

    anchors: list[dict[str, Any]] = []
    used: set[str] = set()

    def add_anchor(row: dict[str, Any] | None, role: str) -> None:
        if row is None or str(row["config_id"]) in used:
            return
        used.add(str(row["config_id"]))
        anchors.append(
            {
                "config_id": str(row["config_id"]),
                "role": role,
                "qualification_status": str(row["qualification_status"]),
                "primary_rank": _integer(row.get("primary_rank")),
                "balanced_mib_per_sec": _optional_number(
                    row.get("balanced_mib_per_sec")
                ),
                "balanced_records_per_sec": _optional_number(
                    row.get("balanced_records_per_sec")
                ),
            }
        )

    if qualified:
        add_anchor(qualified[0], "qualified_sustainable_leader")
    if len(qualified) > 1:
        add_anchor(qualified[1], "qualified_sustainable_secondary")
    for row in sorted(
        eligible,
        key=lambda item: (
            -_number(item.get("balanced_records_per_sec"), -math.inf),
            str(item.get("config_id", "")),
        ),
    ):
        if len(anchors) >= 3:
            break
        add_anchor(row, "high_record_rate_pressure")
    for row in sorted(
        eligible,
        key=lambda item: (
            _integer(item.get("raw_throughput_rank")),
            str(item.get("config_id", "")),
        ),
    ):
        if len(anchors) >= 4:
            break
        add_anchor(row, "raw_throughput_upper_bound")
    return shortlist, anchors


def _selection_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "selection_order": None,
        "config_id": str(row["config_id"]),
        "selection_categories": [],
        "primary_rank": _integer(row.get("primary_rank")),
        "raw_throughput_rank": _integer(row.get("raw_throughput_rank")),
        "qualification_status": str(row.get("qualification_status", "")),
        "balanced_mib_per_sec": _optional_number(row.get("balanced_mib_per_sec")),
        "balanced_records_per_sec": _optional_number(
            row.get("balanced_records_per_sec")
        ),
        "pending_backlog_percent": _optional_number(
            row.get("pending_backlog_percent")
        ),
        "flush_sec": _optional_number(row.get("flush_sec")),
        "failed_send_percent": _optional_number(row.get("failed_send_percent")),
        "payload_size_bytes": _integer(row.get("payload_size_bytes")),
        "manifest_note": str(row.get("manifest_note", "")),
    }


def _write_validation_outputs(
    output_dir: Path,
    rows: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
) -> None:
    _write_csv(output_dir / "validation_repeats_by_case.csv", CASE_FIELDS, rows)
    _write_json(output_dir / "validation_repeats_by_case.json", rows)
    _write_csv(
        output_dir / "validation_summary_by_original.csv",
        VALIDATION_SUMMARY_FIELDS,
        summaries,
    )
    _write_json(output_dir / "validation_summary_by_original.json", summaries)


def _manifest_case_id(row: dict[str, Any], stage: str) -> str:
    if str(row.get("case_id", "")).strip():
        return str(row["case_id"]).strip()
    return str(row.get("config_id", "")).strip()


def _manifest_workload_config_id(row: dict[str, Any]) -> str:
    """Normalize the source-workload identity used by backend manifests."""
    for field in ("workload_config_id", "anchor_id", "anchor"):
        value = str(row.get(field, "")).strip()
        if value:
            return value
    case_id = str(row.get("case_id", "")).strip()
    config_id = str(row.get("config_id", "")).strip()
    if config_id and config_id != case_id:
        return config_id
    return ""


def _qualification_status(eligible: bool, qualified: bool) -> str:
    if not eligible:
        return "ineligible"
    return "qualified" if qualified else "overdriven"


def _compare_number(
    failures: list[str],
    label: str,
    observed: Any,
    expected: Any,
) -> None:
    if not _finite(observed) or not _finite(expected):
        failures.append(f"{label} is missing or non-finite")
        return
    if not math.isclose(float(observed), float(expected), rel_tol=1e-9, abs_tol=1e-9):
        failures.append(
            f"{label} mismatch: observed={float(observed):.12g}, "
            f"expected={float(expected):.12g}"
        )


def _required_number(
    values: dict[str, Any],
    key: str,
    failures: list[str],
) -> float:
    value = _optional_number(values.get(key))
    if value is None:
        failures.append(f"common_metrics.throughput.{key} is missing")
        return math.nan
    return value


def _optional_number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _number(value: Any, default: float) -> float:
    number = _optional_number(value)
    return number if number is not None else default


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _finite(value: Any) -> bool:
    return _optional_number(value) is not None


def _median(values: Iterable[float]) -> float | None:
    available = list(values)
    return statistics.median(available) if available else None


def _iqr(values: Iterable[float]) -> float | None:
    available = sorted(values)
    if not available:
        return None
    if len(available) == 1:
        return 0.0
    quartiles = statistics.quantiles(available, n=4, method="inclusive")
    return quartiles[2] - quartiles[0]


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path,
    fieldnames: Iterable[str],
    rows: Iterable[dict[str, Any]],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_checksums(root: Path) -> None:
    paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}"
        for path in paths
    ]
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def _report_preference(path: Path) -> tuple[int, int, str]:
    return (
        int(path.parent.name not in {"data", "reports"}),
        -len(path.parts),
        str(path),
    )


def _display_path(path: Path, source_root: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        try:
            return path.resolve().relative_to(source_root.resolve()).as_posix()
        except ValueError:
            return path.name


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
