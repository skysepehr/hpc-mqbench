#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import re
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.broker_profile import load_broker_profile
from src.benchmark.qualification import (
    COMMON_QUALIFICATION_POLICY_ID,
    backlog_denominator_for_policy,
    producer_operational_metrics,
)


BACKLOG_LIMIT_PERCENT = 5.0
FLUSH_LIMIT_SEC = 10.0
FAILED_LIMIT_PERCENT = 0.1
MAX_TMPFS_USED_PERCENT = 75.0
REQUIRED_JMX_METRICS = (
    "kafka_jmx_bytes_in_counter_rate",
    "kafka_jmx_bytes_out_counter_rate",
    "kafka_request_queue_size",
    "kafka_response_queue_size",
    "kafka_request_handler_idle_ratio",
    "kafka_request_queue_time_p99_ms",
    "kafka_local_time_p99_ms",
    "kafka_response_queue_time_p99_ms",
    "kafka_total_time_p99_ms",
    "jvm_heap_used_gb",
    "jvm_gc_collection_rate",
    "jvm_gc_time_rate_ms_per_sec",
)

# Kafka 4.2 does not expose NetworkProcessorAvgIdlePercent on the tested broker.
# Keep collecting and reporting it when present, but do not reject an otherwise
# complete case solely because this version-specific MBean is absent.
OPTIONAL_JMX_METRICS = ("kafka_network_processor_idle_ratio",)
LEGACY_MAX_LOAD_ANCHORS = ("cfg_074", "cfg_115", "cfg_059", "cfg_119")
LEGACY_SUSTAINABLE_ANCHORS = ("cfg_074", "cfg_115")
LEGACY_ANCHOR_PURPOSES = {
    "cfg_074": "validated sustainable leader",
    "cfg_115": "stable qualified secondary",
    "cfg_059": "small-payload, high-record-rate pressure",
    "cfg_119": "overload and raw-throughput reference",
    "latency_anchor": "fixed-rate low-latency reference",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze V2 broker-tuning, latency, and final-validation results"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--results-root",
        help="raw V2 result tree containing final_report.json files",
    )
    source.add_argument(
        "--case-summary",
        help="compact V2 cases CSV or JSON produced by this analyzer",
    )
    parser.add_argument(
        "--stage",
        action="append",
        help="include only the named V2 stage; may be repeated",
    )
    parser.add_argument(
        "--output-dir",
        default="analysis_v2",
        help="separate V2 artifact directory",
    )
    parser.add_argument(
        "--compile-pdf",
        action="store_true",
        help="compile the generated standalone V2 LaTeX report",
    )
    parser.add_argument(
        "--machine-only",
        action="store_true",
        help="Write only validated JSON/CSV, manifests, and checksums.",
    )
    return parser.parse_args()


def load_case_summary(path: Path) -> list[dict[str, Any]]:
    """Load compact typed case rows without requiring raw monitoring files."""
    if not path.is_file():
        raise FileNotFoundError(f"Case-summary dataset not found: {path}")
    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list) or not all(
            isinstance(row, dict) for row in payload
        ):
            raise ValueError("Case-summary JSON must contain a list of objects")
        return payload
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [
            {key: _parse_compact_value(value) for key, value in row.items()}
            for row in csv.DictReader(handle)
        ]


def _parse_compact_value(value: str | None) -> Any:
    if value is None or value == "":
        return None
    if value == "True":
        return True
    if value == "False":
        return False
    if re.fullmatch(r"-?[0-9]+", value):
        return int(value)
    if re.fullmatch(
        r"-?(?:[0-9]+\.[0-9]*|[0-9]*\.[0-9]+)(?:[eE][+-]?[0-9]+)?",
        value,
    ):
        return float(value)
    return value


def nearest_rank(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def median_or_none(values: Iterable[float | None]) -> float | None:
    available = [float(value) for value in values if value is not None]
    return statistics.median(available) if available else None


def iqr(values: Iterable[float]) -> float | None:
    available = sorted(float(value) for value in values)
    if not available:
        return None
    if len(available) == 1:
        return 0.0
    quartiles = statistics.quantiles(available, n=4, method="inclusive")
    return quartiles[2] - quartiles[0]


def geometric_mean(values: Iterable[float]) -> float | None:
    available = [float(value) for value in values]
    if not available or any(value <= 0 for value in available):
        return None
    return math.exp(sum(math.log(value) for value in available) / len(available))


def case_dir_for_report(path: Path) -> Path:
    return path.parent.parent if path.parent.name == "data" else path.parent


def report_attempt_key(
    path: Path,
    report: dict[str, Any],
) -> tuple[float, int, float, str]:
    events = report.get("timeline", {}).get("events", [])
    event_times = []
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            try:
                event_times.append(float(event["unix"]))
            except (KeyError, TypeError, ValueError):
                continue
    try:
        modified = path.stat().st_mtime
    except OSError:
        modified = 0.0
    return (
        max(event_times, default=modified),
        int(path.parent.name != "data"),
        modified,
        str(path),
    )


def discover_reports(results_root: Path) -> list[tuple[Path, dict[str, Any]]]:
    by_case: dict[str, tuple[Path, dict[str, Any]]] = {}
    for path in results_root.rglob("final_report.json"):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"{path}: invalid JSON: {exc}") from exc
        config = report.get("config", {})
        extra = config.get("extra", {}) if isinstance(config, dict) else {}
        if not isinstance(extra, dict) or not extra.get("v2_case_id"):
            continue
        case_id = str(extra["v2_case_id"])
        candidate = (path, report)
        current = by_case.get(case_id)
        if (
            current is None
            or report_attempt_key(*candidate) > report_attempt_key(*current)
        ):
            by_case[case_id] = candidate
    return [by_case[key] for key in sorted(by_case)]


def read_numeric_metric(
    case_dir: Path,
    metric_id: str,
    start_unix: float,
    end_unix: float,
) -> list[float]:
    path = case_dir / "monitoring" / "csv" / f"{metric_id}.csv"
    if not path.is_file():
        return []
    values: list[float] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                timestamp = float(row["timestamp"])
                value = float(row["value"])
            except (KeyError, TypeError, ValueError):
                continue
            if start_unix <= timestamp <= end_unix and math.isfinite(value):
                values.append(value)
    return values


def read_process_metrics(
    case_dir: Path,
    start_unix: float,
    end_unix: float,
) -> dict[str, Any]:
    rows: list[dict[str, str]] = []
    for path in sorted((case_dir / "monitoring").glob("broker_process_*.csv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    timestamp = float(row["timestamp_unix"])
                except (KeyError, TypeError, ValueError):
                    continue
                if start_unix <= timestamp <= end_unix:
                    rows.append(row)

    def values(name: str) -> list[float]:
        output: list[float] = []
        for row in rows:
            try:
                value = float(row[name])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(value):
                output.append(value)
        return output

    cpu = values("process_cpu_percent_of_allocated")
    rss = values("process_rss_bytes")
    rx = values("ib0_rx_bytes_per_sec")
    tx = values("ib0_tx_bytes_per_sec")
    tmpfs = values("tmpfs_used_percent")
    return {
        "sample_count": len(rows),
        "cpu_allocated_percent_mean": statistics.fmean(cpu) if cpu else None,
        "cpu_allocated_percent_p95": nearest_rank(cpu, 0.95),
        "rss_gib_mean": (
            statistics.fmean(rss) / (1024 ** 3) if rss else None
        ),
        "rss_gib_p95": (
            nearest_rank(rss, 0.95) / (1024 ** 3) if rss else None
        ),
        "ib0_rx_mibps_mean": (
            statistics.fmean(rx) / (1024 ** 2) if rx else None
        ),
        "ib0_rx_mibps_p95": (
            nearest_rank(rx, 0.95) / (1024 ** 2) if rx else None
        ),
        "ib0_tx_mibps_mean": (
            statistics.fmean(tx) / (1024 ** 2) if tx else None
        ),
        "ib0_tx_mibps_p95": (
            nearest_rank(tx, 0.95) / (1024 ** 2) if tx else None
        ),
        "tmpfs_used_percent_max": max(tmpfs) if tmpfs else None,
    }


def verify_runtime_profile(
    case_dir: Path,
    config: dict[str, Any],
) -> tuple[bool, str]:
    runtime_path = case_dir / "runtime" / "broker_runtime_manifest.json"
    if not runtime_path.is_file():
        return False, "broker runtime manifest missing"
    try:
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        profile_path = case_dir / "runtime" / "broker_profile_snapshot.json"
        profile = load_broker_profile(profile_path)
    except (json.JSONDecodeError, ValueError, OSError) as exc:
        return False, f"invalid broker runtime profile: {exc}"
    expected_id = config.get("broker_profile_id")
    expected_hash = config.get("broker_profile_sha256")
    valid = (
        runtime.get("profile_id") == expected_id == profile.profile_id
        and runtime.get("profile_sha256") == expected_hash == profile.sha256
    )
    return valid, "valid" if valid else "profile ID or hash drift"


def extract_iperf_capacity(report: dict[str, Any]) -> dict[str, float | None]:
    inventory = report.get("system_inventory", {})
    tests = inventory.get("network_tests", []) if isinstance(inventory, dict) else []
    capacities: dict[str, float | None] = {
        "producer_to_broker_iperf_MBps": None,
        "broker_to_consumer_iperf_MBps": None,
    }
    for record in tests if isinstance(tests, list) else []:
        if not isinstance(record, dict) or record.get("status") != "completed":
            continue
        label = record.get("label")
        key = (
            "producer_to_broker_iperf_MBps"
            if label == "producer_to_broker"
            else (
                "broker_to_consumer_iperf_MBps"
                if label == "broker_to_consumer"
                else None
            )
        )
        if key is None:
            continue
        try:
            value = float(record["megabytes_per_second"])
        except (KeyError, TypeError, ValueError):
            continue
        if math.isfinite(value) and value > 0:
            capacities[key] = value
    return capacities


def extract_row(path: Path, report: dict[str, Any]) -> dict[str, Any]:
    case_dir = case_dir_for_report(path)
    config = report.get("config", {})
    extra = config.get("extra", {}) if isinstance(config, dict) else {}
    aggregated = report.get("aggregated_metrics", {})
    producers = aggregated.get("producers", {})
    consumers = aggregated.get("consumers", {})
    correctness = aggregated.get("record_correctness", {})
    producer_config = extra.get("kafka_producer_config", {})
    consumer_config = extra.get("kafka_consumer_config", {})

    common = report.get("common_metrics", {})
    common = common if isinstance(common, dict) else {}
    common_throughput = common.get("throughput", {})
    common_throughput = (
        common_throughput if isinstance(common_throughput, dict) else {}
    )
    producer_bps = float(producers.get("throughput_bytes_per_sec", 0.0))
    consumer_bps = float(consumers.get("throughput_bytes_per_sec", 0.0))
    producer_rps = float(
        common_throughput.get(
            "producer_records_per_sec",
            producers.get("throughput_msgs_per_sec", 0.0),
        )
    )
    consumer_rps = float(
        common_throughput.get(
            "consumer_records_per_sec",
            consumers.get("throughput_msgs_per_sec", 0.0),
        )
    )
    producer_mibps = float(
        common_throughput.get("producer_mib_per_sec", producer_bps / (1024 ** 2))
    )
    consumer_mibps = float(
        common_throughput.get("consumer_mib_per_sec", consumer_bps / (1024 ** 2))
    )
    balanced_mibps = min(producer_mibps, consumer_mibps)
    balanced_rps = min(producer_rps, consumer_rps)
    policy_id = str(config.get("qualification_policy_id") or "qualification.kafka.v1")
    operational = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(policy_id),
    )
    failures = int(operational["messages_failed"])
    backlog_percent = float(operational["producer_backlog_percent"])
    failed_percent = float(operational["failed_send_percent"])
    flush_sec = float(operational["max_flush_duration_sec"])
    thresholds_satisfied = bool(operational["thresholds_satisfied"])
    campaign_metadata = extra.get("campaign_metadata", {})
    campaign_metadata = (
        campaign_metadata if isinstance(campaign_metadata, dict) else {}
    )
    measurement_contract_id = str(
        campaign_metadata.get("measurement_contract_id", "")
    )
    measurement_contract_valid = (
        policy_id != COMMON_QUALIFICATION_POLICY_ID
        or measurement_contract_id == "measurement.messaging.reproducible.v1"
    )

    start_unix = float(producers.get("first_start_time_unix", 0.0))
    end_unix = float(producers.get("last_send_loop_end_time_unix", 0.0))
    process = read_process_metrics(case_dir, start_unix, end_unix)
    jmx_values = {
        metric_id: read_numeric_metric(case_dir, metric_id, start_unix, end_unix)
        for metric_id in REQUIRED_JMX_METRICS
    }
    jmx_summary = {
        f"{metric_id}_mean": (
            statistics.fmean(values) if values else None
        )
        for metric_id, values in jmx_values.items()
    }
    jmx_summary.update(
        {
            f"{metric_id}_p95": nearest_rank(values, 0.95)
            for metric_id, values in jmx_values.items()
        }
    )
    jmx_summary.update(
        {
            f"{metric_id}_last": values[-1] if values else None
            for metric_id, values in jmx_values.items()
        }
    )
    missing_jmx = [
        metric_id for metric_id, values in jmx_values.items() if not values
    ]
    iperf = extract_iperf_capacity(report)
    ingress_mean_bps = jmx_summary.get(
        "kafka_jmx_bytes_in_counter_rate_mean"
    )
    egress_mean_bps = jmx_summary.get(
        "kafka_jmx_bytes_out_counter_rate_mean"
    )
    producer_capacity = iperf["producer_to_broker_iperf_MBps"]
    consumer_capacity = iperf["broker_to_consumer_iperf_MBps"]
    ingress_capacity_ratio = (
        float(ingress_mean_bps) / 1_000_000 / producer_capacity
        if ingress_mean_bps is not None and producer_capacity
        else None
    )
    egress_capacity_ratio = (
        float(egress_mean_bps) / 1_000_000 / consumer_capacity
        if egress_mean_bps is not None and consumer_capacity
        else None
    )
    capacity_ratios = [
        value
        for value in (ingress_capacity_ratio, egress_capacity_ratio)
        if value is not None
    ]

    profile_valid, profile_reason = verify_runtime_profile(case_dir, config)
    latency = report.get("latency_validation", {})
    histogram = consumers.get("latency_histogram", {})
    latency_enabled = bool(config.get("latency_enabled", False))
    latency_valid = bool(latency.get("valid", False)) if latency_enabled else True
    clock_calibrations = (
        latency.get("clock_calibrations", [])
        if isinstance(latency, dict)
        else []
    )
    clock_uncertainties_us = [
        float(record["max_uncertainty_ns"]) / 1_000
        for record in clock_calibrations
        if isinstance(record, dict)
        and record.get("max_uncertainty_ns") is not None
    ]
    clock_drifts_us = [
        float(record["drift_ns"]) / 1_000
        for record in clock_calibrations
        if isinstance(record, dict) and record.get("drift_ns") is not None
    ]
    p99_ns = histogram.get("p99_ns") if isinstance(histogram, dict) else None
    correctness_valid = (
        int(correctness.get("missing_after_drain_records", -1)) == 0
        and int(correctness.get("unexplained_surplus_records", -1)) == 0
        and int(correctness.get("invalid_envelope_count", -1)) == 0
        and int(correctness.get("duplicate_offset_count", -1)) == 0
        and int(correctness.get("out_of_order_offset_count", -1)) == 0
        and int(producers.get("flush_remaining_messages", -1)) == 0
    )
    tmpfs_max = process.get("tmpfs_used_percent_max")
    resources_valid = (
        process.get("sample_count", 0) > 0
        and not missing_jmx
        and producer_capacity is not None
        and consumer_capacity is not None
        and tmpfs_max is not None
        and tmpfs_max <= MAX_TMPFS_USED_PERCENT
    )
    completed = report.get("case", {}).get("status") == "completed"
    eligible = (
        completed
        and profile_valid
        and latency_valid
        and correctness_valid
        and resources_valid
        and measurement_contract_valid
    )
    qualified = eligible and thresholds_satisfied
    eligibility_failures = []
    if not completed:
        eligibility_failures.append("case incomplete")
    if not profile_valid:
        eligibility_failures.append(profile_reason)
    if not latency_valid:
        eligibility_failures.append("latency validation failed")
    if not correctness_valid:
        eligibility_failures.append("record correctness failed")
    if not resources_valid:
        eligibility_failures.append("resource evidence incomplete")
    if not measurement_contract_valid:
        eligibility_failures.append("reproducible measurement contract is missing")

    request_last = jmx_summary.get("kafka_request_queue_size_last")
    response_last = jmx_summary.get("kafka_response_queue_size_last")
    unresolved_queue = (
        (request_last is not None and request_last > 0)
        or (response_last is not None and response_last > 0)
    )
    return {
        "case_id": str(extra.get("v2_case_id", "")),
        "stage": str(extra.get("v2_stage", "")),
        "anchor_id": str(extra.get("v2_anchor_id", "")),
        "anchor_role": str(extra.get("v2_anchor_role", "")),
        "block": int(extra.get("v2_block", 0)),
        "order": int(extra.get("v2_order", 0)),
        "broker_profile_id": str(config.get("broker_profile_id", "")),
        "broker_profile_sha256": str(config.get("broker_profile_sha256", "")),
        "qualification_policy_id": policy_id,
        "measurement_contract_id": measurement_contract_id,
        "measurement_contract_valid": measurement_contract_valid,
        "producer_ranks": int(config.get("producer_ranks", 0)),
        "consumer_ranks": int(config.get("consumer_ranks", 0)),
        "partitions": int(config.get("partitions", 0)),
        "payload_size_bytes": int(config.get("payload_size_bytes", 0)),
        "batch_size_bytes": int(config.get("batch_size", 0)),
        "linger_ms": int(config.get("linger_ms", 0)),
        "producer_queue_messages": int(
            producer_config.get("queue.buffering.max.messages", 0)
        ),
        "producer_queue_kib": int(
            producer_config.get("queue.buffering.max.kbytes", 0)
        ),
        "consumer_fetch_min_bytes": int(
            consumer_config.get("fetch.min.bytes", 0)
        ),
        "consumer_fetch_wait_ms": int(
            consumer_config.get("fetch.wait.max.ms", 0)
        ),
        "consumer_fetch_max_bytes": int(
            consumer_config.get("fetch.message.max.bytes", 0)
        ),
        "warmup_sec": int(config.get("warmup_sec", 0)),
        "measurement_sec": int(config.get("duration_sec", 0)),
        "drain_timeout_sec": int(config.get("drain_timeout_sec", 0)),
        "latency_enabled": latency_enabled,
        "latency_sample_every": int(config.get("latency_sample_every", 1)),
        "target_records_per_sec": config.get("target_records_per_sec"),
        "completed": completed,
        "profile_valid": profile_valid,
        "profile_reason": profile_reason,
        "latency_valid": latency_valid,
        "correctness_valid": correctness_valid,
        "resources_valid": resources_valid,
        "eligible": eligible,
        "eligibility_reason": (
            "eligible" if eligible else "; ".join(eligibility_failures)
        ),
        "qualified": qualified,
        "producer_mibps": producer_mibps,
        "consumer_mibps": consumer_mibps,
        "balanced_mibps": balanced_mibps,
        "producer_records_per_sec": producer_rps,
        "consumer_records_per_sec": consumer_rps,
        "balanced_records_per_sec": balanced_rps,
        "pending_backlog_percent": backlog_percent,
        "backlog_denominator": operational["backlog_denominator"],
        "backlog_denominator_value": operational["backlog_denominator_value"],
        "flush_sec": flush_sec,
        "failed_send_count": failures,
        "failed_send_percent": failed_percent,
        "latency_p50_ms": (
            float(histogram["p50_ns"]) / 1e6
            if isinstance(histogram, dict) and histogram.get("p50_ns") is not None
            else None
        ),
        "latency_p95_ms": (
            float(histogram["p95_ns"]) / 1e6
            if isinstance(histogram, dict) and histogram.get("p95_ns") is not None
            else None
        ),
        "latency_p99_ms": float(p99_ns) / 1e6 if p99_ns is not None else None,
        "latency_p999_ms": (
            float(histogram["p999_ns"]) / 1e6
            if isinstance(histogram, dict) and histogram.get("p999_ns") is not None
            else None
        ),
        "latency_mean_ms": (
            float(histogram["mean_ns"]) / 1e6
            if isinstance(histogram, dict) and histogram.get("mean_ns") is not None
            else None
        ),
        "latency_max_ms": (
            float(histogram["max_ns"]) / 1e6
            if isinstance(histogram, dict) and histogram.get("max_ns") is not None
            else None
        ),
        "latency_sample_count": (
            int(histogram.get("count", 0)) if isinstance(histogram, dict) else 0
        ),
        "latency_delivered_sample_count": int(
            latency.get("delivered_sample_count", 0)
        ),
        "latency_overflow_count": (
            int(histogram.get("overflow_count", 0))
            if isinstance(histogram, dict)
            else 0
        ),
        "latency_negative_count": (
            int(histogram.get("negative_count", 0))
            if isinstance(histogram, dict)
            else 0
        ),
        "clock_uncertainty_us_max": (
            max(clock_uncertainties_us) if clock_uncertainties_us else None
        ),
        "clock_drift_us_max": max(clock_drifts_us) if clock_drifts_us else None,
        "missing_after_drain_records": int(
            correctness.get("missing_after_drain_records", -1)
        ),
        "unexplained_surplus_records": int(
            correctness.get("unexplained_surplus_records", -1)
        ),
        "invalid_envelope_count": int(
            correctness.get("invalid_envelope_count", -1)
        ),
        "duplicate_offset_count": int(correctness.get("duplicate_offset_count", -1)),
        "out_of_order_offset_count": int(
            correctness.get("out_of_order_offset_count", -1)
        ),
        "flush_remaining_messages": int(
            producers.get("flush_remaining_messages", -1)
        ),
        "unresolved_request_queue": unresolved_queue,
        "missing_required_jmx_metrics": ",".join(missing_jmx),
        **iperf,
        "kafka_ingress_to_iperf_capacity_ratio": ingress_capacity_ratio,
        "kafka_egress_to_iperf_capacity_ratio": egress_capacity_ratio,
        "kafka_to_iperf_capacity_ratio": (
            max(capacity_ratios) if len(capacity_ratios) == 2 else None
        ),
        "report_path": str(path),
        **process,
        **jmx_summary,
    }


def instrumentation_decision(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    pilot = [row for row in rows if row["stage"] == "instrumentation_pilot"]
    if len(pilot) != 6:
        return None
    if not all(bool(row["eligible"]) for row in pilot):
        return {
            "required_cases_valid": False,
            "sampling_decision_valid": False,
            "reason": "one or more instrumentation pilot cases are ineligible",
        }
    enabled_rows = [row for row in pilot if bool(row["latency_enabled"])]
    if any(int(row.get("latency_sample_every", 0)) != 10 for row in enabled_rows):
        return {
            "required_cases_valid": False,
            "sampling_decision_valid": False,
            "reason": "latency-enabled instrumentation cases are not deterministic 1-in-10",
        }
    disabled = [row["balanced_mibps"] for row in pilot if not row["latency_enabled"]]
    enabled = [row["balanced_mibps"] for row in enabled_rows]
    if len(disabled) != 3 or len(enabled) != 3:
        return None
    disabled_median = statistics.median(disabled)
    enabled_median = statistics.median(enabled)
    paired_overheads = []
    for block in (1, 2, 3):
        off = [
            row["balanced_mibps"]
            for row in pilot
            if row["block"] == block and not row["latency_enabled"]
        ]
        on = [
            row["balanced_mibps"]
            for row in pilot
            if row["block"] == block and row["latency_enabled"]
        ]
        if len(off) == 1 and len(on) == 1 and off[0] > 0:
            paired_overheads.append((off[0] - on[0]) / off[0])
    overhead = (
        statistics.median(paired_overheads)
        if len(paired_overheads) == 3
        else math.inf
    )
    return {
        "required_cases_valid": True,
        "disabled_median_balanced_mibps": disabled_median,
        "enabled_1_in_10_median_balanced_mibps": enabled_median,
        "throughput_overhead_fraction": overhead,
        "throughput_overhead_percent": 100.0 * overhead,
        "paired_overhead_fractions": paired_overheads,
        "threshold_percent": 3.0,
        "latency_sample_every": 10,
        "sampling_decision_valid": True,
        "throughput_overhead_within_threshold": overhead <= 0.03,
    }


def maximum_load_anchors(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    ordered = sorted(
        (
            row
            for row in rows
            if row.get("stage") in {"profile_screening", "profile_confirmation"}
            and row.get("anchor_id") != "latency_anchor"
        ),
        key=lambda row: (
            0 if row.get("stage") == "profile_screening" else 1,
            int(row.get("block", 0)),
            int(row.get("order", 0)),
            str(row.get("anchor_id", "")),
        ),
    )
    anchors: list[str] = []
    for row in ordered:
        anchor = str(row.get("anchor_id", ""))
        if anchor and anchor not in anchors:
            anchors.append(anchor)
    if len(anchors) == 4:
        return tuple(anchors)
    present = {str(row.get("anchor_id", "")) for row in rows}
    legacy = tuple(anchor for anchor in LEGACY_MAX_LOAD_ANCHORS if anchor in present)
    return legacy if len(legacy) == 4 else tuple(anchors)


def sustainable_anchors(
    rows: list[dict[str, Any]],
    max_load: tuple[str, ...],
) -> tuple[str, ...]:
    roles = {
        str(row.get("anchor_id", "")): str(row.get("anchor_role", ""))
        for row in rows
    }
    selected = tuple(
        anchor
        for anchor in max_load
        if roles.get(anchor, "").startswith("qualified_sustainable_")
    )
    if len(selected) == 2:
        return selected
    legacy = tuple(anchor for anchor in LEGACY_SUSTAINABLE_ANCHORS if anchor in max_load)
    return legacy if len(legacy) == 2 else selected


def anchor_purpose(row: dict[str, Any]) -> str:
    role = str(row.get("anchor_role", "")).strip()
    if role:
        return role.replace("_", " ").replace(":", ": ")
    return LEGACY_ANCHOR_PURPOSES.get(
        str(row.get("anchor_id", "")),
        "benchmark anchor",
    )


def screening_selection(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    screening = [row for row in rows if row["stage"] == "profile_screening"]
    if not screening:
        return None
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in screening:
        grouped[row["broker_profile_id"]].append(row)
    if "B0" not in grouped:
        return None
    max_load = maximum_load_anchors(screening)
    sustainable = sustainable_anchors(screening, max_load)
    if len(max_load) != 4 or len(sustainable) != 2:
        return None
    b0_by_anchor = {row["anchor_id"]: row for row in grouped["B0"]}
    records = []
    for profile_id, profile_rows in grouped.items():
        by_anchor = {row["anchor_id"]: row for row in profile_rows}
        ratios = []
        for anchor in max_load:
            row = by_anchor.get(anchor)
            b0 = b0_by_anchor.get(anchor)
            if row and b0 and b0["balanced_mibps"] > 0:
                ratios.append(row["balanced_mibps"] / b0["balanced_mibps"])
        records.append(
            {
                "profile_id": profile_id,
                "valid_run_count": sum(bool(row["eligible"]) for row in profile_rows),
                "sustainable_anchor_qualified_count": sum(
                    bool(row["qualified"] and row["eligible"])
                    for row in profile_rows
                    if row["anchor_id"] in sustainable
                ),
                "geometric_mean_throughput_ratio_to_B0": (
                    geometric_mean(ratios) if len(ratios) == 4 else None
                ),
            }
        )
    candidates = [record for record in records if record["profile_id"] != "B0"]
    candidates.sort(
        key=lambda record: (
            record["valid_run_count"],
            record["sustainable_anchor_qualified_count"],
            record["geometric_mean_throughput_ratio_to_B0"] or -math.inf,
            record["profile_id"],
        ),
        reverse=True,
    )
    selected = [record["profile_id"] for record in candidates[:2]]
    return {
        "profiles": records,
        "confirmation_profiles": ["B0", *selected],
        "selection_rule": (
            "valid-run count, sustainable-anchor qualification, then geometric "
            "mean throughput ratio to B0"
        ),
        "maximum_load_anchors": list(max_load),
        "sustainable_anchors": list(sustainable),
    }


def final_profile_selection(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    profile_rows = [
        row
        for row in rows
        if row["stage"] in {"profile_screening", "profile_confirmation"}
    ]
    if not any(row["stage"] == "profile_confirmation" for row in profile_rows):
        return None
    max_load = maximum_load_anchors(profile_rows)
    sustainable = sustainable_anchors(profile_rows, max_load)
    if len(max_load) != 4 or len(sustainable) != 2:
        return None
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in profile_rows:
        grouped[row["broker_profile_id"]].append(row)
    b0_rows = grouped.get("B0", [])
    if not b0_rows:
        return None

    def by_anchor(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        output: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            output[item["anchor_id"]].append(item)
        return output

    b0_anchor = by_anchor(b0_rows)
    baseline_complete = (
        len(b0_rows) == 15
        and all(bool(row["eligible"]) for row in b0_rows)
        and all(
            len(b0_anchor[anchor]) == 3
            for anchor in (*max_load, "latency_anchor")
        )
    )
    b0_qualified = {
        anchor: sum(
            bool(row["qualified"] and row["eligible"])
            for row in b0_anchor[anchor]
        )
        for anchor in sustainable
    }
    b0_medians = {
        anchor: median_or_none(
            row["balanced_mibps"]
            for row in b0_anchor[anchor]
            if row["eligible"]
        )
        for anchor in max_load
    }
    b0_latency = median_or_none(
        row["latency_p99_ms"]
        for row in b0_anchor["latency_anchor"]
        if row["eligible"]
    )
    b0_failed = sum(int(row["failed_send_count"]) for row in b0_rows)
    b0_flush_remaining = sum(int(row["flush_remaining_messages"]) for row in b0_rows)

    records = []
    passing = []
    for profile_id, items in grouped.items():
        anchors = by_anchor(items)
        required_count = 3 * 5
        complete = (
            len(items) == required_count
            and all(bool(row["eligible"]) for row in items)
            and all(len(anchors[anchor]) == 3 for anchor in (*max_load, "latency_anchor"))
        )
        qualified_counts = {
            anchor: sum(bool(row["qualified"]) for row in anchors[anchor])
            for anchor in sustainable
        }
        ratios = []
        for anchor in max_load:
            median_value = median_or_none(
                row["balanced_mibps"] for row in anchors[anchor]
            )
            baseline = b0_medians.get(anchor)
            if median_value is not None and baseline:
                ratios.append(median_value / baseline)
        gm_ratio = geometric_mean(ratios) if len(ratios) == 4 else None
        latency_p99 = median_or_none(
            row["latency_p99_ms"] for row in anchors["latency_anchor"]
        )
        latency_ratio = (
            latency_p99 / b0_latency
            if latency_p99 is not None and b0_latency
            else None
        )
        no_correctness_regression = (
            all(int(row["missing_after_drain_records"]) == 0 for row in items)
            and sum(int(row["failed_send_count"]) for row in items) <= b0_failed
            and sum(int(row["flush_remaining_messages"]) for row in items)
            <= b0_flush_remaining
        )
        resource_limits = all(
            row["tmpfs_used_percent_max"] is not None
            and row["tmpfs_used_percent_max"] <= MAX_TMPFS_USED_PERCENT
            and not row["unresolved_request_queue"]
            for row in items
        )
        throughput_spread = median_or_none(
            iqr(row["balanced_mibps"] for row in anchors[anchor])
            for anchor in max_load
        )
        request_queue_p95 = max(
            (
                float(row["kafka_request_queue_size_p95"])
                for row in items
                if row.get("kafka_request_queue_size_p95") is not None
            ),
            default=math.inf,
        )
        response_queue_p95 = max(
            (
                float(row["kafka_response_queue_size_p95"])
                for row in items
                if row.get("kafka_response_queue_size_p95") is not None
            ),
            default=math.inf,
        )
        handler_idle = median_or_none(
            row.get("kafka_request_handler_idle_ratio_mean") for row in items
        )
        network_idle = median_or_none(
            row.get("kafka_network_processor_idle_ratio_mean") for row in items
        )
        gc_time_rate = median_or_none(
            row.get("jvm_gc_time_rate_ms_per_sec_p95") for row in items
        )
        passes = (
            profile_id != "B0"
            and baseline_complete
            and complete
            and all(
                qualified_counts[anchor] >= b0_qualified[anchor]
                for anchor in sustainable
            )
            and gm_ratio is not None
            and gm_ratio >= 1.03
            and latency_ratio is not None
            and latency_ratio <= 1.10
            and no_correctness_regression
            and resource_limits
        )
        record = {
            "profile_id": profile_id,
            "required_measurements_valid": complete,
            "qualified_counts": qualified_counts,
            "B0_qualified_counts": b0_qualified,
            "geometric_mean_median_throughput_ratio_to_B0": gm_ratio,
            "median_latency_anchor_p99_ms": latency_p99,
            "latency_p99_ratio_to_B0": latency_ratio,
            "no_correctness_regression": no_correctness_regression,
            "resource_limits_pass": resource_limits,
            "passes_all_rules": passes,
            "median_anchor_throughput_iqr_mibps": throughput_spread,
            "max_request_queue_p95": request_queue_p95,
            "max_response_queue_p95": response_queue_p95,
            "median_request_handler_idle_ratio": handler_idle,
            "median_network_processor_idle_ratio": network_idle,
            "median_gc_time_rate_p95_ms_per_sec": gc_time_rate,
            "worst_anchor_median_mibps": min(
                (
                    median_or_none(row["balanced_mibps"] for row in anchors[anchor])
                    or 0.0
                )
                for anchor in max_load
            ),
        }
        records.append(record)
        if passes:
            passing.append(record)

    def low(value: float | None) -> float:
        return math.inf if value is None else value

    def high(value: float | None) -> float:
        return -math.inf if value is None else value

    passing.sort(
        key=lambda record: (
            -high(record["geometric_mean_median_throughput_ratio_to_B0"]),
            low(record["latency_p99_ratio_to_B0"]),
            -high(record["worst_anchor_median_mibps"]),
            low(record["median_anchor_throughput_iqr_mibps"]),
            low(record["max_request_queue_p95"]),
            low(record["max_response_queue_p95"]),
            -high(record["median_request_handler_idle_ratio"]),
            -high(record["median_network_processor_idle_ratio"]),
            low(record["median_gc_time_rate_p95_ms_per_sec"]),
            record["profile_id"],
        ),
    )
    winner = passing[0]["profile_id"] if passing else "B0"
    return {
        "winner": winner,
        "retained_B0": not passing,
        "baseline_complete": baseline_complete,
        "profiles": records,
        "maximum_load_anchors": list(max_load),
        "sustainable_anchors": list(sustainable),
        "scope_statement": (
            "The winner is the best eligible profile among B0-B5 for this "
            "single-broker GWDG environment, not a universal Kafka optimum."
        ),
    }


def final_validation_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["stage"] == "final_validation":
            grouped[row["anchor_id"]].append(row)
    output = []
    for config_id, items in grouped.items():
        eligible = [row for row in items if row["eligible"]]
        throughput = [row["balanced_mibps"] for row in eligible]
        p99_values = [
            row["latency_p99_ms"]
            for row in eligible
            if row["latency_p99_ms"] is not None
        ]
        output.append(
            {
                "config_id": config_id,
                "total_repeats": len(items),
                "eligible_repeats": len(eligible),
                "qualified_repeats": sum(bool(row["qualified"]) for row in eligible),
                "median_balanced_mibps": (
                    statistics.median(throughput) if throughput else None
                ),
                "median_balanced_records_per_sec": median_or_none(
                    row["balanced_records_per_sec"] for row in eligible
                ),
                "median_p99_latency_ms": (
                    statistics.median(p99_values) if p99_values else None
                ),
                "median_p50_latency_ms": median_or_none(
                    row["latency_p50_ms"] for row in eligible
                ),
                "median_p95_latency_ms": median_or_none(
                    row["latency_p95_ms"] for row in eligible
                ),
                "median_p999_latency_ms": median_or_none(
                    row["latency_p999_ms"] for row in eligible
                ),
                "median_mean_latency_ms": median_or_none(
                    row["latency_mean_ms"] for row in eligible
                ),
                "median_max_latency_ms": median_or_none(
                    row["latency_max_ms"] for row in eligible
                ),
                "median_latency_sample_count": median_or_none(
                    row["latency_sample_count"] for row in eligible
                ),
                "maximum_clock_uncertainty_us": (
                    max(
                        row["clock_uncertainty_us_max"]
                        for row in eligible
                        if row["clock_uncertainty_us_max"] is not None
                    )
                    if any(
                        row["clock_uncertainty_us_max"] is not None
                        for row in eligible
                    )
                    else None
                ),
                "maximum_clock_drift_us": (
                    max(
                        row["clock_drift_us_max"]
                        for row in eligible
                        if row["clock_drift_us_max"] is not None
                    )
                    if any(
                        row["clock_drift_us_max"] is not None
                        for row in eligible
                    )
                    else None
                ),
                "throughput_iqr_mibps": iqr(throughput),
                "median_backlog_percent": median_or_none(
                    row["pending_backlog_percent"] for row in eligible
                ),
                "median_flush_sec": median_or_none(
                    row["flush_sec"] for row in eligible
                ),
                "median_failed_send_percent": median_or_none(
                    row["failed_send_percent"] for row in eligible
                ),
                "median_broker_cpu_allocated_percent": median_or_none(
                    row["cpu_allocated_percent_mean"] for row in eligible
                ),
                "median_kafka_jmx_ingress_mibps": (
                    (
                        median_or_none(
                            row["kafka_jmx_bytes_in_counter_rate_mean"]
                            for row in eligible
                        )
                        or 0.0
                    )
                    / (1024 ** 2)
                    if eligible
                    else None
                ),
                "median_kafka_jmx_egress_mibps": (
                    (
                        median_or_none(
                            row["kafka_jmx_bytes_out_counter_rate_mean"]
                            for row in eligible
                        )
                        or 0.0
                    )
                    / (1024 ** 2)
                    if eligible
                    else None
                ),
                "median_jvm_heap_used_gb": median_or_none(
                    row["jvm_heap_used_gb_mean"] for row in eligible
                ),
                "median_kafka_to_iperf_capacity_ratio": median_or_none(
                    row["kafka_to_iperf_capacity_ratio"] for row in eligible
                ),
                "maximum_tmpfs_used_percent": (
                    max(
                        row["tmpfs_used_percent_max"]
                        for row in eligible
                        if row["tmpfs_used_percent_max"] is not None
                    )
                    if any(
                        row["tmpfs_used_percent_max"] is not None
                        for row in eligible
                    )
                    else None
                ),
            }
        )
    def missing_last(value: float | None) -> float:
        return math.inf if value is None else value

    def descending_missing_last(value: float | None) -> float:
        return math.inf if value is None else -value

    output.sort(
        key=lambda row: (
            -(row["qualified_repeats"]),
            descending_missing_last(row["median_balanced_mibps"]),
            missing_last(row["throughput_iqr_mibps"]),
            missing_last(row["median_backlog_percent"]),
            missing_last(row["median_flush_sec"]),
            missing_last(row["median_failed_send_percent"]),
            row["config_id"],
        )
    )
    for rank, row in enumerate(output, start=1):
        row["rank"] = rank
    return output


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def fmt(value: Any, digits: int = 3) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def sorted_case_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            str(row["stage"]),
            int(row["block"]),
            int(row["order"]),
            str(row["broker_profile_id"]),
            str(row["anchor_id"]),
        ),
    )


def duration_summary(rows: list[dict[str, Any]]) -> tuple[str, str, str]:
    def values(field: str) -> str:
        durations = sorted({int(row[field]) for row in rows})
        return ", ".join(str(value) for value in durations)

    return values("warmup_sec"), values("measurement_sec"), values("drain_timeout_sec")


def anchor_setting_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_anchor: dict[str, dict[str, Any]] = {}
    for row in rows:
        by_anchor.setdefault(str(row["anchor_id"]), row)
    order = [*maximum_load_anchors(rows), "latency_anchor"]
    return [by_anchor[anchor] for anchor in order if anchor in by_anchor]


def broker_profile_setting_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for profile_id in sorted({str(row["broker_profile_id"]) for row in rows}):
        profile_path = (
            PROJECT_ROOT / "configs" / "tuning_v2" / "broker_profiles"
            / f"{profile_id}.json"
        )
        profile = load_broker_profile(profile_path)
        settings = profile.payload["settings"]
        heap_match = re.search(r"-Xmx(\d+)([gGmM])", settings["heap_opts"])
        if heap_match is None:
            heap_gib = None
        else:
            heap_value = int(heap_match.group(1))
            heap_gib = (
                float(heap_value)
                if heap_match.group(2).lower() == "g"
                else heap_value / 1024.0
            )
        output.append(
            {
                "profile_id": profile_id,
                "network_threads": int(settings["num_network_threads"]),
                "io_threads": int(settings["num_io_threads"]),
                "heap_gib": heap_gib,
                "log_segment_bytes": int(settings["log_segment_bytes"]),
            }
        )
    return output


def markdown_report(
    rows: list[dict[str, Any]],
    decisions: dict[str, Any],
    final_summary: list[dict[str, Any]],
) -> str:
    warmup_values, measurement_values, drain_values = duration_summary(rows)
    anchors = anchor_setting_rows(rows)
    profiles = broker_profile_setting_rows(rows)
    lines = [
        "# Kafka Broker-Tuning and Latency Validation V2",
        "",
        "V2 is analyzed separately from the historical V1 screening and validation data. "
        "No V1 observation is pooled into a V2 comparison.",
        "",
        "## Measurement Method",
        "",
        f"The cases in this artifact use excluded warm-up duration(s) of "
        f"{warmup_values} seconds, measurement duration(s) of {measurement_values} "
        f"seconds, and drain timeout(s) of {drain_values} seconds. End-to-end latency is "
        "consumer receive time minus producer send time after MPI clock correction. "
        "A run is latency-valid only when all active ranks satisfy the 250-us "
        "uncertainty and drift limits, every delivered sampled record is observed, "
        "and no negative latency is observed.",
        "",
        "Qualification remains backlog <= 5%, flush <= 10 s, and failed sends <= "
        "0.1%. Resource and latency evidence can reject an invalid run but cannot "
        "make an overdriven run qualified.",
        "",
        "The Kafka-to-iperf capacity ratio is the larger of directional Kafka JMX "
        "ingress/producer-to-broker capacity and JMX egress/broker-to-consumer "
        "capacity. The iperf capacities are measured by each case's system "
        "inventory. This ratio is a capacity comparison, not exact concurrent "
        "link utilization.",
        "",
        "Kafka 4.2 on the tested nodes does not expose the legacy "
        "`NetworkProcessorAvgIdlePercent` MBean. Network-processor idle is "
        "therefore reported as unavailable rather than estimated; request-handler "
        "idle and Kafka-process CPU remain the supported saturation evidence.",
        "",
        f"- Unique V2 reports: **{len(rows)}**",
        f"- Eligible reports: **{sum(bool(row['eligible']) for row in rows)}**",
        "",
        "## Test Architecture and Procedure",
        "",
        "Each case uses one exclusive four-node Slurm allocation: a producer/controller "
        "node, a single Kafka broker node, a consumer node, and a monitoring/result "
        "collection node. Producer and consumer workers run through MPI and mpi4py. "
        "Kafka records travel producer to broker to consumer over `ib0`; Prometheus, "
        "JMX exporter, Kafka exporter, node_exporter, and a Kafka-process monitor "
        "collect one-second supporting measurements.",
        "",
        "Profile screening contains 30 cases: six broker profiles are each executed "
        "once for each of five workload anchors. Profile order is randomized with "
        "seed `20260728`. Every case performs clock calibration, excluded warm-up, "
        "the configured measurement interval, producer flush, consumer drain, and "
        "record-accounting validation. Clock calibration uses 100 MPI ping-pong "
        "samples before and after the case; latency is rejected when uncertainty or "
        "drift exceeds 250 microseconds.",
        "",
        "Every new V2 case uses 15 seconds of excluded warm-up, a 30-second "
        "measurement interval, and up to 60 seconds of consumer drain. The four "
        "maximum-load anchors and the later validation workloads are selected from "
        "the completed new V1 Phase 1 analysis; the synthetic latency anchor uses "
        "the same timing at a fixed aggregate offered rate.",
        "",
        "## Definitions and Abbreviations",
        "",
        "- **B0-B5:** broker tuning profile identifiers; B0 is the original baseline.",
        "- **Anchor:** a fixed producer, consumer, topic, and payload configuration used to compare broker profiles.",
        "- **MPI:** Message Passing Interface; **HPC:** high-performance computing.",
        "- **MiB/s:** application payload throughput in 1,048,576 bytes per second.",
        "- **records/s:** Kafka application records completed per second.",
        "- **p50, p95, p99, p99.9:** latency percentiles; p99 is the value not exceeded by 99% of measured records.",
        "- **JMX:** Java Management Extensions; **JVM:** Java Virtual Machine; **G1:** Garbage-First garbage collector.",
        "- **CPU %:** Kafka-process CPU normalized by the logical CPUs allocated by Slurm.",
        "- **tmpfs:** RAM-backed filesystem used for Kafka logs in this experiment.",
        "- **iperf3:** network capacity probe. Kafka/iperf is a capacity ratio, not concurrent link utilization.",
        "- **Eligible:** completed with valid profile hash, clocks, latency samples, record accounting, and resource evidence.",
        "- **Qualified:** backlog <= 5%, flush <= 10 seconds, and failed sends <= 0.1%. Eligibility does not make an overdriven case qualified.",
        "- **Missing/surplus:** records absent after drain, or consumer samples exceeding producer-confirmed deliveries.",
        "",
        "## Broker Profiles and Fixed Settings",
        "",
        "| Profile | Network threads | I/O threads | Heap GiB | Log segment bytes |",
        "|---|---:|---:|---:|---:|",
    ]
    for profile in profiles:
        lines.append(
            f"| `{profile['profile_id']}` | {profile['network_threads']} | "
            f"{profile['io_threads']} | {fmt(profile['heap_gib'], 0)} | "
            f"{profile['log_segment_bytes']} |"
        )
    lines.extend(
        [
            "",
            "All profiles use Java 17 with G1, one broker, replication factor 1, "
            "`acks=1`, no compression, 1-MiB socket send and receive buffers, a "
            "100-MiB maximum socket request, `queued.max.requests=1000`, Kafka traffic "
            "on `ib0`, and RAM-backed `/dev/shm` log storage. Only network threads, "
            "I/O threads, heap size, and B5's log-segment size vary.",
            "",
            "## Workload Anchor Settings",
            "",
            "| Anchor | Purpose | Producer/consumer ranks | Partitions | Payload bytes | Measurement s | Target records/s |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in anchors:
        lines.append(
            f"| `{row['anchor_id']}` | "
            f"{anchor_purpose(row)} | "
            f"{row['producer_ranks']}/{row['consumer_ranks']} | "
            f"{row['partitions']} | {row['payload_size_bytes']} | "
            f"{row['measurement_sec']} | "
            f"{fmt(row['target_records_per_sec'], 0) if row['target_records_per_sec'] is not None else 'maximum load'} |"
        )
    lines.extend(
        [
            "",
            "| Anchor | Batch bytes | Linger ms | Producer queue messages | Producer queue KiB | Fetch min bytes | Fetch wait ms | Fetch max bytes |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in anchors:
        lines.append(
            f"| `{row['anchor_id']}` | {row['batch_size_bytes']} | "
            f"{row['linger_ms']} | {row['producer_queue_messages']} | "
            f"{row['producer_queue_kib']} | "
            f"{row['consumer_fetch_min_bytes']} | "
            f"{row['consumer_fetch_wait_ms']} | "
            f"{row['consumer_fetch_max_bytes']} |"
        )
    lines.append("")
    if decisions.get("instrumentation"):
        decision = decisions["instrumentation"]
        lines.extend(["## Instrumentation Decision", ""])
        if decision.get("sampling_decision_valid"):
            lines.extend(
                [
                    f"- Median overhead: **{fmt(decision['throughput_overhead_percent'])}%**",
                    f"- Fixed sampling: **1-in-{decision['latency_sample_every']}**",
                    "- Record identity and correctness accounting: **every record**",
                ]
            )
        else:
            lines.append(
                "- No sampling decision: "
                f"{decision.get('reason', 'required pilot evidence is incomplete')}."
            )
        lines.append("")
    if decisions.get("profile_selection"):
        selection = decisions["profile_selection"]
        lines.extend(
            [
                "## Broker Profile Selection",
                "",
                f"- Frozen winner: **{selection['winner']}**",
                f"- Retained B0: **{selection['retained_B0']}**",
                f"- Scope: {selection['scope_statement']}",
                "",
            ]
        )
    if final_summary:
        lines.extend(
            [
                "## Final Validation",
                "",
                "| Rank | Config | Eligible | Qualified | Median MiB/s | Median records/s | IQR MiB/s | Backlog % | Flush s | Failed % | Median p99 ms |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in final_summary:
            lines.append(
                f"| {row['rank']} | `{row['config_id']}` | "
                f"{row['eligible_repeats']}/{row['total_repeats']} | "
                f"{row['qualified_repeats']}/{row['total_repeats']} | "
                f"{fmt(row['median_balanced_mibps'])} | "
                f"{fmt(row['median_balanced_records_per_sec'], 0)} | "
                f"{fmt(row['throughput_iqr_mibps'])} | "
                f"{fmt(row['median_backlog_percent'])} | "
                f"{fmt(row['median_flush_sec'])} | "
                f"{fmt(row['median_failed_send_percent'], 4)} | "
                f"{fmt(row['median_p99_latency_ms'])} |"
            )
        lines.extend(
            [
                "",
                "Each repeat must first be eligible. Configurations are ranked "
                "by qualified-repeat count, median balanced throughput, "
                "throughput IQR, median backlog, median flush time, median "
                "failed-send percentage, and configuration ID. p99 latency is "
                "reported as secondary evidence and is not a final-ranking "
                "criterion.",
                "",
            ]
        )
        lines.extend(
            [
                "## Final Latency Evidence",
                "",
                "| Config | p50 ms | p95 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Samples | Max clock uncertainty us | Max drift us |",
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in final_summary:
            lines.append(
                f"| `{row['config_id']}` | "
                f"{fmt(row['median_p50_latency_ms'])} | "
                f"{fmt(row['median_p95_latency_ms'])} | "
                f"{fmt(row['median_p99_latency_ms'])} | "
                f"{fmt(row['median_p999_latency_ms'])} | "
                f"{fmt(row['median_mean_latency_ms'])} | "
                f"{fmt(row['median_max_latency_ms'])} | "
                f"{fmt(row['median_latency_sample_count'], 0)} | "
                f"{fmt(row['maximum_clock_uncertainty_us'])} | "
                f"{fmt(row['maximum_clock_drift_us'])} |"
            )
        lines.extend(
            [
                "",
                "Latency percentiles are bounded-histogram estimates for each "
                "eligible repeat; the table reports the median estimate across "
                "repeats. Sample count is the median delivered sample count. Clock "
                "columns are the worst valid-rank values across eligible repeats.",
                "",
            ]
        )
        lines.extend(
            [
                "## Final Resource Evidence",
                "",
                "| Config | Broker CPU % allocated | JMX in MiB/s | JMX out MiB/s | Heap GB | Kafka/iperf ratio | Max tmpfs % |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in final_summary:
            lines.append(
                f"| `{row['config_id']}` | "
                f"{fmt(row['median_broker_cpu_allocated_percent'])} | "
                f"{fmt(row['median_kafka_jmx_ingress_mibps'])} | "
                f"{fmt(row['median_kafka_jmx_egress_mibps'])} | "
                f"{fmt(row['median_jvm_heap_used_gb'])} | "
                f"{fmt(row['median_kafka_to_iperf_capacity_ratio'])} | "
                f"{fmt(row['maximum_tmpfs_used_percent'])} |"
            )
        lines.extend(
            [
                "",
                "CPU is Kafka-process utilization normalized by the logical CPUs "
                "allowed to that process by Slurm. JMX byte rates are Kafka-specific; "
                "the exact `ib0` counters retained in the case CSV include all traffic "
                "on that interface and are supporting evidence.",
                "",
            ]
        )
    else:
        observed = sorted_case_rows(rows)
        lines.extend(
            [
                "## Observed V2 Cases",
                "",
                "These are individual observations from the stages present in this "
                "artifact. They are not a final repeated-validation ranking.",
                "",
                "| Profile | Anchor | Stage | Eligible | Qualified | Balanced MiB/s | Records/s | p99 ms | Backlog % | Flush s | Failed % |",
                "|---|---|---|---|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in observed:
            lines.append(
                f"| `{row['broker_profile_id']}` | `{row['anchor_id']}` | "
                f"`{row['stage']}` | {row['eligible']} | {row['qualified']} | "
                f"{fmt(row['balanced_mibps'])} | "
                f"{fmt(row['balanced_records_per_sec'], 0)} | "
                f"{fmt(row['latency_p99_ms'])} | "
                f"{fmt(row['pending_backlog_percent'])} | "
                f"{fmt(row['flush_sec'])} | "
                f"{fmt(row['failed_send_percent'])} |"
            )
        lines.extend(
            [
                "",
                "## Eligibility and Record Accounting",
                "",
                "| Profile | Anchor | Eligibility reason | Latency valid | Correctness valid | Resources valid | Missing | Surplus | Flush remaining |",
                "|---|---|---|---|---|---|---:|---:|---:|",
            ]
        )
        for row in observed:
            lines.append(
                f"| `{row['broker_profile_id']}` | `{row['anchor_id']}` | "
                f"{row['eligibility_reason']} | {row['latency_valid']} | "
                f"{row['correctness_valid']} | {row['resources_valid']} | "
                f"{row['missing_after_drain_records']} | "
                f"{row['unexplained_surplus_records']} | "
                f"{row['flush_remaining_messages']} |"
            )
        lines.extend(
            [
                "",
                "## End-to-End Latency Evidence",
                "",
                "| Profile | Anchor | Valid | p50 ms | p95 ms | p99 ms | p99.9 ms | Consumer samples | Producer delivered samples | Max uncertainty us | Max drift us |",
                "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in observed:
            lines.append(
                f"| `{row['broker_profile_id']}` | `{row['anchor_id']}` | "
                f"{row['latency_valid']} | {fmt(row['latency_p50_ms'])} | "
                f"{fmt(row['latency_p95_ms'])} | {fmt(row['latency_p99_ms'])} | "
                f"{fmt(row['latency_p999_ms'])} | "
                f"{row['latency_sample_count']} | "
                f"{row['latency_delivered_sample_count']} | "
                f"{fmt(row['clock_uncertainty_us_max'])} | "
                f"{fmt(row['clock_drift_us_max'])} |"
            )
        lines.extend(
            [
                "",
                "Latency values from rows marked invalid are retained for diagnostic "
                "transparency but must not be used as performance evidence.",
                "",
                "## Resource Evidence",
                "",
                "| Profile | Anchor | Valid | Broker CPU % | JMX in MiB/s | JMX out MiB/s | Heap GB | Kafka/iperf ratio | Max tmpfs % |",
                "|---|---|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in observed:
            lines.append(
                f"| `{row['broker_profile_id']}` | `{row['anchor_id']}` | "
                f"{row['resources_valid']} | "
                f"{fmt(row['cpu_allocated_percent_mean'])} | "
                f"{fmt(row['kafka_jmx_bytes_in_counter_rate_mean'] / (1024 ** 2) if row['kafka_jmx_bytes_in_counter_rate_mean'] is not None else None)} | "
                f"{fmt(row['kafka_jmx_bytes_out_counter_rate_mean'] / (1024 ** 2) if row['kafka_jmx_bytes_out_counter_rate_mean'] is not None else None)} | "
                f"{fmt(row['jvm_heap_used_gb_mean'])} | "
                f"{fmt(row['kafka_to_iperf_capacity_ratio'])} | "
                f"{fmt(row['tmpfs_used_percent_max'])} |"
            )
        lines.extend(
            [
                "",
                "CPU is Kafka-process utilization normalized by the logical CPUs "
                "allowed by Slurm. JMX byte rates are Kafka-specific, while the "
                "Kafka-to-iperf value is a capacity ratio rather than concurrent link "
                "utilization.",
                "",
            ]
        )
    return "\n".join(lines)


def html_report(markdown: str) -> str:
    def inline(value: str) -> str:
        escaped = html.escape(value)
        escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
        return re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)

    lines = markdown.splitlines()
    body_parts: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not line:
            index += 1
            continue
        if line.startswith("#"):
            level = min(6, len(line) - len(line.lstrip("#")))
            body_parts.append(
                f"<h{level}>{inline(line[level:].strip())}</h{level}>"
            )
            index += 1
            continue
        if line.startswith("|") and index + 1 < len(lines):
            separator = lines[index + 1]
            if separator.startswith("|") and set(
                separator.replace("|", "").replace(":", "").replace("-", "")
            ) <= {" "}:
                headers = [cell.strip() for cell in line.strip("|").split("|")]
                index += 2
                table_rows: list[list[str]] = []
                while index < len(lines) and lines[index].startswith("|"):
                    table_rows.append(
                        [
                            cell.strip()
                            for cell in lines[index].strip("|").split("|")
                        ]
                    )
                    index += 1
                body_parts.append("<table><thead><tr>")
                body_parts.extend(f"<th>{inline(cell)}</th>" for cell in headers)
                body_parts.append("</tr></thead><tbody>")
                for row in table_rows:
                    body_parts.append("<tr>")
                    body_parts.extend(f"<td>{inline(cell)}</td>" for cell in row)
                    body_parts.append("</tr>")
                body_parts.append("</tbody></table>")
                continue
        if line.startswith("- "):
            body_parts.append("<ul>")
            while index < len(lines) and lines[index].startswith("- "):
                body_parts.append(f"<li>{inline(lines[index][2:])}</li>")
                index += 1
            body_parts.append("</ul>")
            continue
        paragraph = [line]
        index += 1
        while index < len(lines) and lines[index] and not (
            lines[index].startswith(("#", "|", "- "))
        ):
            paragraph.append(lines[index])
            index += 1
        body_parts.append(f"<p>{inline(' '.join(paragraph))}</p>")

    body = "\n".join(body_parts)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Kafka Broker-Tuning V2</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:1100px;margin:2rem auto;"
        "line-height:1.5;color:#17202a}code{font-family:monospace}"
        "table{border-collapse:collapse;width:100%;margin:1rem 0 2rem}"
        "th,td{border:1px solid #cbd5e1;padding:.45rem .6rem;text-align:right}"
        "th:first-child,td:first-child{text-align:left}th{background:#eef3f8}</style>"
        f"</head><body>{body}</body></html>\n"
    )


def latex_escape(value: str) -> str:
    replacements = {
        "\\": r"\textbackslash{}",
        "_": r"\_",
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
    }
    return "".join(replacements.get(character, character) for character in value)


def latex_report(
    rows: list[dict[str, Any]],
    decisions: dict[str, Any],
    final_summary: list[dict[str, Any]],
) -> str:
    final_table_rows = "\n".join(
        "{} & {} & {}/{} & {}/{} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            row["rank"],
            latex_escape(row["config_id"]),
            row["eligible_repeats"],
            row["total_repeats"],
            row["qualified_repeats"],
            row["total_repeats"],
            fmt(row["median_balanced_mibps"]),
            fmt(row["median_balanced_records_per_sec"], 0),
            fmt(row["throughput_iqr_mibps"]),
            fmt(row["median_backlog_percent"]),
            fmt(row["median_flush_sec"]),
            fmt(row["median_failed_send_percent"], 4),
            fmt(row["median_p99_latency_ms"]),
        )
        for row in final_summary
    )
    final_resource_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["config_id"]),
            fmt(row["median_broker_cpu_allocated_percent"]),
            fmt(row["median_kafka_jmx_ingress_mibps"]),
            fmt(row["median_kafka_jmx_egress_mibps"]),
            fmt(row["median_jvm_heap_used_gb"]),
            fmt(row["median_kafka_to_iperf_capacity_ratio"]),
        )
        for row in final_summary
    )
    final_latency_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["config_id"]),
            fmt(row["median_p50_latency_ms"]),
            fmt(row["median_p95_latency_ms"]),
            fmt(row["median_p99_latency_ms"]),
            fmt(row["median_p999_latency_ms"]),
            fmt(row["median_mean_latency_ms"]),
            fmt(row["median_max_latency_ms"]),
            fmt(row["median_latency_sample_count"], 0),
            fmt(row["maximum_clock_uncertainty_us"]),
            fmt(row["maximum_clock_drift_us"]),
        )
        for row in final_summary
    )
    observed = sorted_case_rows(rows)
    observed_table_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["broker_profile_id"]),
            latex_escape(row["anchor_id"]),
            "yes" if row["eligible"] else "no",
            "yes" if row["qualified"] else "no",
            fmt(row["balanced_mibps"]),
            fmt(row["balanced_records_per_sec"], 0),
            fmt(row["latency_p99_ms"]),
            fmt(row["pending_backlog_percent"]),
            fmt(row["flush_sec"]),
            fmt(row["failed_send_percent"]),
        )
        for row in observed
    )
    observed_validity_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["broker_profile_id"]),
            latex_escape(row["anchor_id"]),
            "yes" if row["latency_valid"] else "no",
            "yes" if row["correctness_valid"] else "no",
            "yes" if row["resources_valid"] else "no",
            row["missing_after_drain_records"],
            row["unexplained_surplus_records"],
            row["flush_remaining_messages"],
        )
        for row in observed
    )
    observed_latency_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["broker_profile_id"]),
            latex_escape(row["anchor_id"]),
            "yes" if row["latency_valid"] else "no",
            fmt(row["latency_p50_ms"]),
            fmt(row["latency_p95_ms"]),
            fmt(row["latency_p99_ms"]),
            fmt(row["latency_p999_ms"]),
            row["latency_sample_count"],
            row["latency_delivered_sample_count"],
            fmt(row["clock_uncertainty_us_max"]),
            fmt(row["clock_drift_us_max"]),
        )
        for row in observed
    )
    observed_resource_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["broker_profile_id"]),
            latex_escape(row["anchor_id"]),
            "yes" if row["resources_valid"] else "no",
            fmt(row["cpu_allocated_percent_mean"]),
            fmt(
                row["kafka_jmx_bytes_in_counter_rate_mean"] / (1024 ** 2)
                if row["kafka_jmx_bytes_in_counter_rate_mean"] is not None
                else None
            ),
            fmt(
                row["kafka_jmx_bytes_out_counter_rate_mean"] / (1024 ** 2)
                if row["kafka_jmx_bytes_out_counter_rate_mean"] is not None
                else None
            ),
            fmt(row["jvm_heap_used_gb_mean"]),
            fmt(row["kafka_to_iperf_capacity_ratio"]),
            fmt(row["tmpfs_used_percent_max"]),
        )
        for row in observed
    )
    profiles = broker_profile_setting_rows(rows)
    anchors = anchor_setting_rows(rows)
    profile_table_rows = "\n".join(
        "{} & {} & {} & {} & {} \\\\".format(
            latex_escape(profile["profile_id"]),
            profile["network_threads"],
            profile["io_threads"],
            fmt(profile["heap_gib"], 0),
            profile["log_segment_bytes"],
        )
        for profile in profiles
    )
    anchor_topology_rows = "\n".join(
        "{} & {} & {}/{} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["anchor_id"]),
            latex_escape(
                anchor_purpose(row)
            ),
            row["producer_ranks"],
            row["consumer_ranks"],
            row["partitions"],
            row["payload_size_bytes"],
            row["measurement_sec"],
            (
                fmt(row["target_records_per_sec"], 0)
                if row["target_records_per_sec"] is not None
                else "maximum"
            ),
        )
        for row in anchors
    )
    anchor_client_rows = "\n".join(
        "{} & {} & {} & {} & {} & {} & {} & {} \\\\".format(
            latex_escape(row["anchor_id"]),
            row["batch_size_bytes"],
            row["linger_ms"],
            row["producer_queue_messages"],
            row["producer_queue_kib"],
            row["consumer_fetch_min_bytes"],
            row["consumer_fetch_wait_ms"],
            row["consumer_fetch_max_bytes"],
        )
        for row in anchors
    )
    warmup_values, measurement_values, drain_values = duration_summary(rows)
    winner = (decisions.get("profile_selection") or {}).get(
        "winner",
        "not selected",
    )
    if winner == "not selected":
        selection_statement = (
            "No broker profile has been selected; selection requires complete "
            "screening and confirmation evidence."
        )
    else:
        selection_statement = (
            rf"The selected broker profile is "
            rf"\texttt{{{latex_escape(str(winner))}}}; this is the best eligible "
            "profile among the six tested profiles for this single-broker GWDG "
            "environment, not a universal Kafka optimum."
        )
    if final_summary:
        evidence_sections = rf"""
\begin{{landscape}}
\section{{Final Validation}}
\scriptsize
\begin{{longtable}}{{r l r r r r r r r r r}}
\toprule
Rank & Configuration & Eligible/total & Qualified/total & Median MiB/s &
Median records/s & IQR MiB/s & Backlog \% & Flush s & Failed \% &
Median p99 ms \\
\midrule
{final_table_rows}
\bottomrule
\end{{longtable}}
\normalsize
Each repeat must first be eligible. Configurations are ranked by
qualified-repeat count, median balanced throughput, throughput IQR, median
backlog, median flush time, median failed-send percentage, and configuration
ID. Median p99 latency is secondary evidence, not a final-ranking criterion.
\end{{landscape}}
\begin{{landscape}}
\section{{End-to-End Latency Evidence}}
\scriptsize
\begin{{longtable}}{{l r r r r r r r r r}}
\toprule
Configuration & p50 & p95 & p99 & p99.9 & Mean & Max & Samples &
Max uncertainty & Max drift \\
 & ms & ms & ms & ms & ms & ms & records & $\mu$s & $\mu$s \\
\midrule
{final_latency_rows}
\bottomrule
\end{{longtable}}
\normalsize
Percentiles are bounded-histogram estimates for each eligible repeat; values
shown are medians across repeats. Sample count is the median delivered sample
count. Clock columns are worst valid-rank values across eligible repeats.
\end{{landscape}}
\section{{Resource Evidence}}
Kafka-process CPU is normalized by the logical CPUs allowed by Slurm. JMX byte
rates are Kafka-specific. The Kafka-to-iperf value is a directional capacity
ratio, not exact concurrent link utilization.
\begin{{longtable}}{{l r r r r r}}
\toprule
Configuration & CPU \% & JMX in MiB/s & JMX out MiB/s & Heap GB &
Kafka/iperf ratio \\
\midrule
{final_resource_rows}
\bottomrule
\end{{longtable}}
"""
    else:
        evidence_sections = rf"""
\begin{{landscape}}
\section{{Observed V2 Cases}}
These are individual observations from the stages present in this artifact;
they are not a final repeated-validation ranking.
\scriptsize
\begin{{longtable}}{{l l c c r r r r r r}}
\toprule
Profile & Anchor & Eligible & Qualified & Balanced & Records/s & p99 &
Backlog & Flush & Failed \\
 & & & & MiB/s & & ms & \% & s & \% \\
\midrule
{observed_table_rows}
\bottomrule
\end{{longtable}}
\section{{Eligibility and Record Accounting}}
\begin{{longtable}}{{l l c c c r r r}}
\toprule
Profile & Anchor & Latency & Correctness & Resources & Missing & Surplus &
Flush remaining \\
\midrule
{observed_validity_rows}
\bottomrule
\end{{longtable}}
\normalsize
Eligibility requires a completed case, an unchanged broker profile, valid
latency and resource evidence, and complete record accounting. Surplus is the
consumer sample count exceeding producer-confirmed deliveries. Flush remaining
is the number of producer messages still queued after the flush timeout.
\section{{End-to-End Latency Evidence}}
\scriptsize
\begin{{longtable}}{{l l c r r r r r r r r}}
\toprule
Profile & Anchor & Valid & p50 & p95 & p99 & p99.9 & Consumer & Producer &
Max uncertainty & Max drift \\
 & & & ms & ms & ms & ms & samples & delivered & $\mu$s & $\mu$s \\
\midrule
{observed_latency_rows}
\bottomrule
\end{{longtable}}
\normalsize
Latency values from rows marked invalid are retained for diagnostic
transparency but must not be used as performance evidence.
\section{{Resource Evidence}}
\scriptsize
\begin{{longtable}}{{l l c r r r r r r}}
\toprule
Profile & Anchor & Valid & CPU \% & JMX in & JMX out & Heap GB &
Kafka/iperf & Tmpfs \% \\
 & & & & MiB/s & MiB/s & & ratio & max \\
\midrule
{observed_resource_rows}
\bottomrule
\end{{longtable}}
\normalsize
Kafka-process CPU is normalized by the logical CPUs allowed by Slurm. JMX byte
rates are Kafka-specific. The Kafka-to-iperf value is a directional capacity
ratio, not exact concurrent link utilization.
\end{{landscape}}
"""
    return rf"""\documentclass[11pt]{{article}}
\usepackage[margin=1in]{{geometry}}
\usepackage{{booktabs}}
\usepackage{{longtable}}
\usepackage{{pdflscape}}
\title{{Focused Kafka Broker-Tuning and Latency Validation V2}}
\author{{}}
\date{{}}
\begin{{document}}
\maketitle
\section{{Scope}}
This V2 analysis is separate from the historical V1 screening and validation
data. It contains {len(rows)} unique case reports, of which
{sum(bool(row["eligible"]) for row in rows)} satisfy every eligibility gate.
{selection_statement}

\section{{Test Architecture and Procedure}}
Each benchmark case uses an exclusive four-node Slurm allocation: one
producer/controller node, one single-broker Kafka node, one consumer node, and
one monitoring and result-collection node. Producer and consumer workers run
through MPI and \texttt{{mpi4py}}. Kafka records travel from producers through
the broker to consumers over \texttt{{ib0}}. Prometheus, JMX exporter, Kafka
exporter, \texttt{{node\_exporter}}, and a Kafka-process monitor collect
one-second supporting measurements.

Profile screening comprises 30 cases: each of six broker profiles is executed
once for each of five workload anchors. Profile order is randomized using seed
\texttt{{20260728}}. A case performs 100-sample MPI clock calibration, excluded
warm-up, the configured measurement interval, producer flush, consumer drain,
post-run clock calibration, and exact record-accounting checks. The cases in
this artifact use warm-up duration(s) of {warmup_values} seconds, measurement
duration(s) of {measurement_values} seconds, and drain timeout(s) of
{drain_values} seconds.

Every new V2 case uses 15 seconds of excluded warm-up, a 30-second measurement
interval, and up to 60 seconds of consumer drain. The four maximum-load anchors
and the later validation workloads are selected from the completed new V1
Phase 1 analysis. The synthetic fixed-rate latency anchor uses the same timing.

End-to-end latency is consumer receive time minus producer send time after MPI
clock correction. Latency is valid only when every active rank remains within
250 microseconds of clock uncertainty and drift, every producer-confirmed
sample is observed by a consumer, record envelopes are valid, and no negative
latency is recorded. Resource and correctness checks can reject a run but
cannot make an overdriven run qualified.

\section{{Definitions and Abbreviations}}
\begin{{description}}
\item[B0--B5] Broker tuning profile identifiers; B0 is the original baseline.
\item[Anchor] A fixed producer, consumer, topic, and payload configuration used
to compare broker profiles.
\item[MPI/HPC] Message Passing Interface and high-performance computing.
\item[MiB/s] Application payload throughput in 1,048,576 bytes per second.
\item[records/s] Kafka application records completed per second.
\item[p50/p95/p99/p99.9] End-to-end latency percentiles. For example, p99 is
the latency not exceeded by 99\% of measured records.
\item[JMX/JVM/G1] Java Management Extensions, Java Virtual Machine, and the
Garbage-First garbage collector.
\item[CPU \%] Kafka-process CPU consumption normalized by the logical CPUs
allocated by Slurm.
\item[tmpfs] The RAM-backed filesystem used for Kafka logs.
\item[iperf3] A network-capacity probe. Kafka/iperf is the larger directional
Kafka-JMX-byte-rate-to-iperf-capacity ratio; it is not simultaneous link
utilization.
\item[Eligible] Completed with an unchanged broker-profile hash, valid clocks
and latency samples, exact record accounting, and complete resource evidence.
\item[Qualified] Backlog at most 5\%, producer flush at most 10 seconds, and
failed sends at most 0.1\%. An eligible overload observation may remain
unqualified.
\item[Missing/surplus] Records absent after drain, or consumer samples exceeding
producer-confirmed deliveries.
\end{{description}}

\section{{Broker Profiles and Fixed Benchmark Settings}}
\begin{{longtable}}{{l r r r r}}
\caption{{Broker profiles compared during V2 screening. Heap is the fixed
\texttt{{-Xms}} and \texttt{{-Xmx}} size.}}\\
\toprule
Profile & Network threads & I/O threads & Heap GiB & Log segment bytes \\
\midrule
{profile_table_rows}
\bottomrule
\end{{longtable}}

All profiles use Java 17 with G1, one Kafka broker, replication factor 1,
\texttt{{acks=1}}, no compression, 1-MiB socket send and receive buffers, a
100-MiB maximum socket request, and a queued-request limit of 1,000
(\texttt{{queued.max.requests}}). Kafka traffic uses \texttt{{ib0}}, and log
storage uses the RAM-backed \texttt{{/dev/shm}} filesystem.
Only network threads, I/O threads, heap size, and B5's log-segment size vary.

\begin{{landscape}}
\section{{Workload Anchor Configurations}}
\scriptsize
\begin{{longtable}}{{l p{{4.0cm}} r r r r r}}
\caption{{Topology, payload, duration, and offered-rate settings for the five
workload anchors. P/C denotes producer/consumer MPI ranks.}}\\
\toprule
Anchor & Purpose & P/C ranks & Partitions & Payload bytes & Measurement s &
Target records/s \\
\midrule
{anchor_topology_rows}
\bottomrule
\end{{longtable}}

\begin{{longtable}}{{l r r r r r r r}}
\caption{{Producer and consumer client settings fixed within each workload
anchor. Queue KiB is \texttt{{queue.buffering.max.kbytes}}; fetch columns are
consumer fetch minimum, wait, and maximum.}}\\
\toprule
Anchor & Batch bytes & Linger ms & Queue messages & Queue KiB & Fetch min bytes
& Fetch wait ms & Fetch max bytes \\
\midrule
{anchor_client_rows}
\bottomrule
\end{{longtable}}
\normalsize
\end{{landscape}}
{evidence_sections}
\end{{document}}
"""


def main() -> None:
    args = parse_args()
    if args.machine_only and args.compile_pdf:
        raise SystemExit("--machine-only cannot be combined with --compile-pdf")
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = PROJECT_ROOT / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.case_summary:
        summary_path = Path(args.case_summary).resolve()
        rows = load_case_summary(summary_path)
        source_description = str(summary_path)
    else:
        results_root = Path(args.results_root).resolve()
        reports = discover_reports(results_root)
        rows = [extract_row(path, report) for path, report in reports]
        source_description = str(results_root)
    if args.stage:
        requested_stages = set(args.stage)
        rows = [row for row in rows if row["stage"] in requested_stages]
    if not rows:
        stage_note = f" for stage(s) {', '.join(args.stage)}" if args.stage else ""
        raise SystemExit(
            f"No V2 case rows found in {source_description}{stage_note}"
        )

    instrumentation = instrumentation_decision(rows)
    screening = screening_selection(rows)
    profile_selection = final_profile_selection(rows)
    final_summary = final_validation_summary(rows)
    decisions = {
        "instrumentation": instrumentation,
        "screening_selection": screening,
        "profile_selection": profile_selection,
        "measurement_limitations": {
            "kafka_network_processor_idle_ratio": (
                "Kafka 4.2 did not expose the legacy "
                "NetworkProcessorAvgIdlePercent MBean on the tested broker; "
                "the value is left unavailable and is not estimated."
            ),
        },
    }

    write_csv(output_dir / "kafka_broker_tuning_v2_cases.csv", rows)
    write_csv(output_dir / "kafka_broker_tuning_v2_final_ranking.csv", final_summary)
    (output_dir / "kafka_broker_tuning_v2_cases.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "kafka_broker_tuning_v2_decisions.json").write_text(
        json.dumps(decisions, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not args.machine_only:
        markdown = markdown_report(rows, decisions, final_summary)
        (output_dir / "kafka_broker_tuning_v2_report.md").write_text(
            markdown + "\n",
            encoding="utf-8",
        )
        (output_dir / "kafka_broker_tuning_v2_report.html").write_text(
            html_report(markdown),
            encoding="utf-8",
        )
        tex_path = output_dir / "kafka_broker_tuning_v2_report.tex"
        tex_path.write_text(
            latex_report(rows, decisions, final_summary),
            encoding="utf-8",
        )
        if args.compile_pdf:
            subprocess.run(
                [
                    "pdflatex",
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    tex_path.name,
                ],
                cwd=output_dir,
                check=True,
            )
            subprocess.run(
                [
                    "pdflatex",
                    "-interaction=nonstopmode",
                    "-halt-on-error",
                    tex_path.name,
                ],
                cwd=output_dir,
                check=True,
            )
    _write_artifact_manifest(output_dir)


def _write_artifact_manifest(output_dir: Path) -> None:
    manifest = output_dir / "artifact_manifest.csv"
    checksum = output_dir / "SHA256SUMS"
    rows = []
    for path in sorted(output_dir.iterdir()):
        if not path.is_file() or path in {manifest, checksum}:
            continue
        rows.append(
            {
                "path": path.name,
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    with manifest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("path", "size_bytes", "sha256"))
        writer.writeheader()
        writer.writerows(rows)
    checksum_rows = [*rows, {
        "path": manifest.name,
        "size_bytes": manifest.stat().st_size,
        "sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
    }]
    checksum.write_text(
        "".join(f"{row['sha256']}  {row['path']}\n" for row in checksum_rows),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
