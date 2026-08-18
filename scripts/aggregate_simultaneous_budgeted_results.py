#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.case_compare import load_report, summarize_report

DEFAULT_RESULTS_ROOT = PROJECT_ROOT / "results" / "sweeps" / "simultaneous_budgeted"


SUMMARY_FIELDS = [
    "config_id",
    "workload_config_id",
    "config_path",
    "status",
    "scenario",
    "topic",
    "partitions",
    "producer_ranks",
    "consumer_ranks",
    "batch_size",
    "linger_ms",
    "payload_size_bytes",
    "producer_queue_messages",
    "producer_queue_kbytes",
    "consumer_fetch_min_bytes",
    "consumer_fetch_wait_max_ms",
    "consumer_fetch_message_max_bytes",
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
    "broker_ingress_MBps",
    "broker_egress_MBps",
    "broker_combined_MBps",
    "failed_send_percent",
    "flush_sec",
    "pending_backlog_percent",
    "backlog_denominator",
    "latency_p50_us",
    "latency_p95_us",
    "latency_p99_us",
    "latency_valid",
    "eligible",
    "qualification_status",
    "throughput_verdict",
    "primary_bottleneck_conclusion",
    "broker_cpu_peak_percent",
    "producer_cpu_peak_percent",
    "consumer_cpu_peak_percent",
    "broker_ram_peak_GB",
    "broker_network_rx_peak_MBps",
    "broker_network_tx_peak_MBps",
    "case_dir",
    "report_path",
]

VARIED_CLIENT_CONFIG_FIELDS = [
    "producer_queue_messages",
    "producer_queue_kbytes",
    "consumer_fetch_min_bytes",
    "consumer_fetch_wait_max_ms",
    "consumer_fetch_message_max_bytes",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate simultaneous budgeted sweep case results."
    )
    parser.add_argument("--run-dir", help="Specific sweep run directory to aggregate.")
    parser.add_argument(
        "--results-root",
        default=str(DEFAULT_RESULTS_ROOT),
        help="Root containing run_*/ directories.",
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir) if args.run_dir else _latest_run_dir(Path(args.results_root))
    rows = aggregate_run(run_dir)
    write_summary(run_dir, rows)
    print(f"[sweep-aggregate] run: {_project_relative(run_dir)}")
    print(f"[sweep-aggregate] cases: {len(rows)}")
    print(f"[sweep-aggregate] wrote: {_project_relative(run_dir / 'sweep_summary.csv')}")
    return 0


def aggregate_run(run_dir: Path) -> list[dict[str, Any]]:
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Sweep run directory not found: {run_dir}")
    status_rows = _load_status_rows(run_dir)
    rows_by_id: dict[str, dict[str, Any]] = {}

    for report_path in sorted(run_dir.glob("batch_*_job_*/cases/*/final_report.json")):
        row = _row_from_report(report_path)
        rows_by_id[row["config_id"]] = row

    for status in status_rows:
        config_id = status.get("config_id") or status.get("case_id") or ""
        if not config_id:
            continue
        if config_id in rows_by_id:
            if status.get("status"):
                rows_by_id[config_id]["status"] = status["status"]
            if status.get("config_path"):
                rows_by_id[config_id]["config_path"] = status["config_path"]
            continue
        rows_by_id[config_id] = _row_from_status(status)

    rows = list(rows_by_id.values())
    rows.sort(key=lambda row: _config_sort_key(str(row.get("config_id", ""))))
    return rows


def write_summary(run_dir: Path, rows: list[dict[str, Any]]) -> None:
    csv_path = run_dir / "sweep_summary.csv"
    json_path = run_dir / "sweep_summary.json"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    payload = {
        "format": "kafka_simple_simultaneous_budgeted_sweep_summary.v1",
        "run_dir": _project_relative(run_dir),
        "case_count": len(rows),
        "completed_count": sum(1 for row in rows if str(row.get("status")) == "completed"),
        "rows": rows,
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _row_from_report(report_path: Path) -> dict[str, Any]:
    report = load_report(report_path)
    summary = summarize_report(report)
    case = _dict(report.get("case"))
    config = _dict(report.get("config") or case.get("config"))
    notes = _dict(case.get("notes"))
    extra = _dict(config.get("extra"))
    campaign_metadata = _dict(extra.get("campaign_metadata"))
    case_id = str(case.get("case_id") or report_path.parent.name)
    workload_config_id = str(
        campaign_metadata.get("workload_config_id")
        or extra.get("sweep_config_id")
        or case_id
    )
    config_id = (
        case_id
        if campaign_metadata.get("measurement_contract_id")
        else str(extra.get("sweep_config_id") or case_id)
    )
    producer = _float(summary.get("producer_delivered_mb_s"))
    consumer = _float(summary.get("consumer_received_mb_s"))
    balanced = min(v for v in [producer, consumer] if v > 0) if producer > 0 and consumer > 0 else 0.0
    failed_percent = _float(summary.get("failed_send_percent"))
    flush_sec = _float(summary.get("flush_sec"))
    pending_percent = _float(summary.get("pending_flush_percent"))
    client_config = _client_config_values(config)
    return {
        "config_id": config_id,
        "workload_config_id": workload_config_id,
        "config_path": notes.get("config_path") or config.get("config_path") or "",
        "status": case.get("status", "unknown"),
        "scenario": config.get("scenario", summary.get("scenario", "unknown")),
        "topic": config.get("topic_name", ""),
        "partitions": config.get("partitions", summary.get("partitions", "")),
        "producer_ranks": config.get("producer_ranks", summary.get("producer_ranks", "")),
        "consumer_ranks": config.get("consumer_ranks", summary.get("consumer_ranks", "")),
        "batch_size": config.get("batch_size", ""),
        "linger_ms": config.get("linger_ms", ""),
        "payload_size_bytes": config.get("payload_size_bytes", summary.get("payload_size_bytes", "")),
        **client_config,
        "throughput_unit": "MiB/s",
        "producer_mib_per_sec": round(producer, 3),
        "consumer_mib_per_sec": round(consumer, 3),
        "balanced_mib_per_sec": round(balanced, 3),
        "producer_delivered_MBps": round(producer, 3),
        "consumer_received_MBps": round(consumer, 3),
        "balanced_app_MBps": round(balanced, 3),
        "producer_records_per_sec": round(
            _float(summary.get("producer_records_per_sec"))
        ),
        "consumer_records_per_sec": round(
            _float(summary.get("consumer_records_per_sec"))
        ),
        "balanced_records_per_sec": round(
            _float(summary.get("balanced_records_per_sec"))
        ),
        "broker_ingress_MBps": round(_float(summary.get("broker_ingress_mb_s")), 3),
        "broker_egress_MBps": round(_float(summary.get("broker_egress_mb_s")), 3),
        "broker_combined_MBps": round(_float(summary.get("broker_combined_mb_s")), 3),
        "failed_send_percent": round(failed_percent, 5),
        "flush_sec": round(flush_sec, 3),
        "pending_backlog_percent": round(pending_percent, 3),
        "backlog_denominator": summary.get("backlog_denominator", ""),
        "latency_p50_us": _float_or_empty(summary.get("latency_p50_us")),
        "latency_p95_us": _float_or_empty(summary.get("latency_p95_us")),
        "latency_p99_us": _float_or_empty(summary.get("latency_p99_us")),
        "latency_valid": summary.get("latency_valid", ""),
        "eligible": summary.get("eligible", ""),
        "qualification_status": _qualification_status(summary),
        "throughput_verdict": summary.get("sustained_verdict", ""),
        "primary_bottleneck_conclusion": summary.get("primary_conclusion", ""),
        "broker_cpu_peak_percent": round(_float(summary.get("broker_cpu_peak_percent")), 3),
        "producer_cpu_peak_percent": round(_float(summary.get("producer_cpu_peak_percent")), 3),
        "consumer_cpu_peak_percent": round(_float(summary.get("consumer_cpu_peak_percent")), 3),
        "broker_ram_peak_GB": round(_float(summary.get("broker_ram_peak_gb")), 3),
        "broker_network_rx_peak_MBps": round(
            _role_peak(_dict(report.get("monitoring")), "node_network_receive_mbps", "broker"),
            3,
        ),
        "broker_network_tx_peak_MBps": round(
            _role_peak(_dict(report.get("monitoring")), "node_network_transmit_mbps", "broker"),
            3,
        ),
        "case_dir": _project_relative(report_path.parent),
        "report_path": _project_relative(report_path),
    }


def _row_from_status(status: dict[str, str]) -> dict[str, Any]:
    config_id = status.get("config_id") or status.get("case_id") or ""
    client_config = _client_config_values_from_path(status.get("config_path", ""))
    return {
        "config_id": config_id,
        "workload_config_id": status.get("workload_config_id", config_id),
        "config_path": status.get("config_path", ""),
        "status": status.get("status", "not_started"),
        "scenario": status.get("scenario", "simultaneous"),
        "topic": status.get("topic_name", ""),
        "partitions": status.get("partitions", ""),
        "producer_ranks": status.get("producer_ranks", ""),
        "consumer_ranks": status.get("consumer_ranks", ""),
        "batch_size": status.get("batch_size", ""),
        "linger_ms": status.get("linger_ms", ""),
        "payload_size_bytes": status.get("payload_size_bytes", ""),
        **client_config,
        "throughput_unit": "MiB/s",
        "producer_mib_per_sec": 0.0,
        "consumer_mib_per_sec": 0.0,
        "balanced_mib_per_sec": 0.0,
        "producer_delivered_MBps": 0.0,
        "consumer_received_MBps": 0.0,
        "balanced_app_MBps": 0.0,
        "producer_records_per_sec": 0,
        "consumer_records_per_sec": 0,
        "balanced_records_per_sec": 0,
        "broker_ingress_MBps": 0.0,
        "broker_egress_MBps": 0.0,
        "broker_combined_MBps": 0.0,
        "failed_send_percent": 0.0,
        "flush_sec": 0.0,
        "pending_backlog_percent": 0.0,
        "backlog_denominator": "",
        "latency_p50_us": "",
        "latency_p95_us": "",
        "latency_p99_us": "",
        "latency_valid": "",
        "eligible": False,
        "qualification_status": "ineligible",
        "throughput_verdict": status.get("reason", ""),
        "primary_bottleneck_conclusion": status.get("reason", ""),
        "broker_cpu_peak_percent": 0.0,
        "producer_cpu_peak_percent": 0.0,
        "consumer_cpu_peak_percent": 0.0,
        "broker_ram_peak_GB": 0.0,
        "broker_network_rx_peak_MBps": 0.0,
        "broker_network_tx_peak_MBps": 0.0,
        "case_dir": status.get("case_dir", ""),
        "report_path": "",
    }


def _load_status_rows(run_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(run_dir.glob("batch_*_job_*/sweep_cases.tsv")):
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                normalized = {
                    key: value.rstrip("\r") if isinstance(value, str) else value
                    for key, value in row.items()
                }
                if not str(normalized.get("config_id") or "").strip():
                    continue
                rows.append(normalized)
    return rows


def _latest_run_dir(results_root: Path) -> Path:
    if not results_root.is_dir():
        raise FileNotFoundError(f"Sweep results root not found: {results_root}")
    candidates = [path for path in results_root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No sweep run directories found under: {results_root}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _qualification_status(summary: dict[str, Any]) -> str:
    eligible = summary.get("eligible")
    qualified = summary.get("qualified")
    if eligible is False:
        return "ineligible"
    if qualified is True:
        return "qualified"
    if qualified is False:
        return "overdriven"
    return "unknown"


def _float_or_empty(value: Any) -> float | str:
    if value is None or value == "":
        return ""
    return round(_float(value), 3)


def _role_peak(monitoring: dict[str, Any], metric_id: str, role: str) -> float:
    peaks: list[float] = []
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        if metric.get("id") != metric_id:
            continue
        for series in _list(metric.get("series")):
            series = _dict(series)
            labels = _dict(series.get("labels"))
            if labels.get("role") == role:
                peaks.append(_float(series.get("max_value")))
    return max(peaks, default=0.0)


def _client_config_values(config: dict[str, Any]) -> dict[str, Any]:
    extra = _dict(config.get("extra"))
    producer_config = _dict(extra.get("kafka_producer_config"))
    consumer_config = _dict(extra.get("kafka_consumer_config"))
    return {
        "producer_queue_messages": _int_or_empty(
            producer_config.get("queue.buffering.max.messages")
        ),
        "producer_queue_kbytes": _int_or_empty(
            producer_config.get("queue.buffering.max.kbytes")
        ),
        "consumer_fetch_min_bytes": _int_or_empty(
            consumer_config.get("fetch.min.bytes")
        ),
        "consumer_fetch_wait_max_ms": _int_or_empty(
            consumer_config.get("fetch.wait.max.ms")
        ),
        "consumer_fetch_message_max_bytes": _int_or_empty(
            consumer_config.get("fetch.message.max.bytes")
        ),
    }


def _client_config_values_from_path(config_path: str) -> dict[str, Any]:
    empty = {key: "" for key in VARIED_CLIENT_CONFIG_FIELDS}
    if not config_path:
        return empty
    path = Path(config_path)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty
    if not isinstance(payload, dict):
        return empty
    return _client_config_values(payload)


def _config_sort_key(value: str) -> tuple[str, int]:
    prefix, _, suffix = value.partition("_")
    try:
        return prefix, int(suffix)
    except ValueError:
        return value, 0


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_or_empty(value: Any) -> int | str:
    if value in (None, ""):
        return ""
    try:
        return int(value)
    except (TypeError, ValueError):
        return str(value)


if __name__ == "__main__":
    raise SystemExit(main())
