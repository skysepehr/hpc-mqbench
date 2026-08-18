from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any

from src.benchmark.qualification import (
    backlog_denominator_for_policy,
    producer_operational_metrics,
)


def load_report(path: str | Path) -> dict[str, Any]:
    report_path = Path(path)
    if report_path.is_dir():
        report_path = report_path / "final_report.json"
    with report_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"report is not a JSON object: {report_path}")
    data.setdefault("_report_path", str(report_path))
    return data


def summarize_report(report: dict[str, Any]) -> dict[str, Any]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config") or case.get("config"))
    aggregated = _dict(report.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    analysis = _dict(report.get("bottleneck_analysis"))
    monitoring = _dict(report.get("monitoring"))
    common = _dict(report.get("common_metrics"))
    common_throughput = _dict(common.get("throughput"))
    common_delivery = _dict(common.get("producer_delivery"))
    common_latency = _dict(common.get("latency_end_to_end"))
    common_qualification = _dict(common.get("qualification"))

    producer_attempt = _rate_mib_per_sec(
        producers,
        bytes_key="send_attempt_throughput_bytes_per_sec",
        mib_key="send_attempt_throughput_mebibytes_per_sec",
        decimal_mb_key="send_attempt_throughput_megabytes_per_sec",
    )
    producer_delivered = _first_positive(
        common_throughput.get("producer_mib_per_sec"),
        _rate_mib_per_sec(producers),
    )
    consumer_received = _first_positive(
        common_throughput.get("consumer_mib_per_sec"),
        _rate_mib_per_sec(consumers),
    )
    broker_ingress = _broker_metric_mib_s(report, "in")
    broker_egress = _broker_metric_mib_s(report, "out")
    broker_combined = broker_ingress + broker_egress

    enqueued = _int(producers.get("messages_enqueued"))
    attempted = _int(producers.get("messages_attempted"))
    pending = _int(producers.get("pending_messages_at_flush_start"))
    failed = _int(producers.get("messages_failed"))
    operations = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(
            str(config.get("qualification_policy_id", ""))
        ),
    )
    pending_percent = _float(
        common_delivery.get(
            "backlog_percent",
            operations["producer_backlog_percent"],
        )
    )
    failed_percent = _float(analysis.get("producer_failed_send_percent"))
    if failed_percent == 0.0 and attempted:
        failed_percent = failed / attempted * 100.0
    throughput_verdict = _dict(report.get("throughput_verdict"))
    flush_sec = _float(producers.get("max_flush_duration_sec"))
    clean_sustainable = _first_positive(
        common_throughput.get("balanced_mib_per_sec"),
        _min_positive([producer_delivered, consumer_received]),
    )
    sustained_label = _qualification_label(
        common_qualification,
        pending_percent,
        failed_percent,
        flush_sec,
    )

    return {
        "label": case.get("case_id") or Path(str(report.get("_report_path", ""))).parent.name,
        "case_name": case.get("case_name", "unknown"),
        "status": case.get("status", "unknown"),
        "sustained_verdict": sustained_label,
        # Compatibility keys retain ``mb_s`` in their names, but all values in
        # this summary are canonical binary MiB/s rates.
        "throughput_unit": "MiB/s",
        "clean_sustainable_mb_s": clean_sustainable,
        "scenario": config.get("scenario", "unknown"),
        "partitions": config.get("partitions", "unknown"),
        "producer_ranks": config.get("producer_ranks", "unknown"),
        "consumer_ranks": config.get("consumer_ranks", "unknown"),
        "payload_size_bytes": config.get("payload_size_bytes", "unknown"),
        "producer_attempt_mb_s": producer_attempt,
        "producer_delivered_mb_s": producer_delivered,
        "producer_records_per_sec": _float(
            common_throughput.get("producer_records_per_sec")
            or producers.get("throughput_msgs_per_sec")
        ),
        "producer_delivery_efficiency_percent": (
            producer_delivered / producer_attempt * 100.0 if producer_attempt else 0.0
        ),
        "consumer_received_mb_s": consumer_received,
        "consumer_records_per_sec": _float(
            common_throughput.get("consumer_records_per_sec")
            or consumers.get("throughput_msgs_per_sec")
        ),
        "balanced_records_per_sec": _float(
            common_throughput.get("balanced_records_per_sec")
        ),
        "broker_ingress_mb_s": broker_ingress,
        "broker_egress_mb_s": broker_egress,
        "broker_combined_mb_s": broker_combined,
        "pending_flush_percent": pending_percent,
        "failed_send_percent": failed_percent,
        "flush_sec": flush_sec,
        "backlog_denominator": operations["backlog_denominator"],
        "eligible": common_qualification.get("eligible"),
        "qualified": common_qualification.get("qualified"),
        "latency_valid": common_latency.get("valid"),
        "latency_p50_us": common_latency.get("p50_us"),
        "latency_p95_us": common_latency.get("p95_us"),
        "latency_p99_us": common_latency.get("p99_us"),
        "producer_to_broker_capacity_mb_s": _network_capacity_mib_s(
            report, "producer_to_broker"
        ),
        "broker_to_consumer_capacity_mb_s": _network_capacity_mib_s(
            report, "broker_to_consumer"
        ),
        "broker_cpu_peak_percent": _role_peak(monitoring, "node_cpu_busy_percent", "broker"),
        "broker_ram_peak_gb": _role_peak(monitoring, "node_memory_used_gb", "broker"),
        "producer_cpu_peak_percent": _role_peak(monitoring, "node_cpu_busy_percent", "producer_controller"),
        "consumer_cpu_peak_percent": _role_peak(monitoring, "node_cpu_busy_percent", "consumer"),
        "primary_conclusion": analysis.get("primary_conclusion", "n/a"),
    }


def build_comparison_markdown(reports: list[dict[str, Any]]) -> str:
    summaries = [summarize_report(report) for report in reports]
    labels = [str(summary["label"]) for summary in summaries]
    rows = [
        ("Case name", "case_name", "text"),
        ("Status", "status", "text"),
        ("Sustained verdict", "sustained_verdict", "text"),
        ("Clean sustainable MiB/s", "clean_sustainable_mb_s", "mb"),
        ("Scenario", "scenario", "text"),
        ("Partitions", "partitions", "text"),
        ("Producer ranks", "producer_ranks", "text"),
        ("Consumer ranks", "consumer_ranks", "text"),
        ("Payload bytes", "payload_size_bytes", "text"),
        ("Producer attempt MiB/s", "producer_attempt_mb_s", "mb"),
        ("Producer delivered MiB/s", "producer_delivered_mb_s", "mb"),
        ("Producer delivery efficiency", "producer_delivery_efficiency_percent", "percent"),
        ("Consumer received MiB/s", "consumer_received_mb_s", "mb"),
        ("Broker ingress avg MiB/s", "broker_ingress_mb_s", "mb"),
        ("Broker egress avg MiB/s", "broker_egress_mb_s", "mb"),
        ("Broker combined avg MiB/s", "broker_combined_mb_s", "mb"),
        ("Pending at flush", "pending_flush_percent", "percent"),
        ("Failed sends", "failed_send_percent", "percent3"),
        ("Flush time sec", "flush_sec", "sec"),
        ("Producer->broker iperf MiB/s", "producer_to_broker_capacity_mb_s", "mb"),
        ("Broker->consumer iperf MiB/s", "broker_to_consumer_capacity_mb_s", "mb"),
        ("Broker CPU peak", "broker_cpu_peak_percent", "percent"),
        ("Broker RAM peak GB", "broker_ram_peak_gb", "gb"),
        ("Producer CPU peak", "producer_cpu_peak_percent", "percent"),
        ("Consumer CPU peak", "consumer_cpu_peak_percent", "percent"),
        ("Primary conclusion", "primary_conclusion", "text"),
    ]
    lines = ["# Kafka Benchmark Case Comparison", ""]
    lines.append("| Metric | " + " | ".join(labels) + " |")
    lines.append("| --- | " + " | ".join(["---:" for _ in labels]) + " |")
    for title, key, kind in rows:
        values = [_format_value(summary.get(key), kind) for summary in summaries]
        lines.append("| " + " | ".join([title, *values]) + " |")
    return "\n".join(lines) + "\n"


def build_comparison_html(reports: list[dict[str, Any]]) -> str:
    summaries = [summarize_report(report) for report in reports]
    labels = [str(summary["label"]) for summary in summaries]
    rows = [
        ("Status", "status", "text"),
        ("Sustained verdict", "sustained_verdict", "text"),
        ("Scenario", "scenario", "text"),
        ("Partitions", "partitions", "text"),
        ("Producer ranks", "producer_ranks", "text"),
        ("Consumer ranks", "consumer_ranks", "text"),
        ("Payload bytes", "payload_size_bytes", "text"),
        ("Clean sustainable MiB/s", "clean_sustainable_mb_s", "mb"),
        ("Producer attempt MiB/s", "producer_attempt_mb_s", "mb"),
        ("Producer delivered MiB/s", "producer_delivered_mb_s", "mb"),
        ("Producer delivery efficiency", "producer_delivery_efficiency_percent", "percent"),
        ("Consumer received MiB/s", "consumer_received_mb_s", "mb"),
        ("Broker ingress avg MiB/s", "broker_ingress_mb_s", "mb"),
        ("Broker egress avg MiB/s", "broker_egress_mb_s", "mb"),
        ("Broker combined avg MiB/s", "broker_combined_mb_s", "mb"),
        ("Pending at flush", "pending_flush_percent", "percent"),
        ("Failed sends", "failed_send_percent", "percent3"),
        ("Flush time sec", "flush_sec", "sec"),
        ("Producer->broker iperf MiB/s", "producer_to_broker_capacity_mb_s", "mb"),
        ("Broker->consumer iperf MiB/s", "broker_to_consumer_capacity_mb_s", "mb"),
        ("Broker CPU peak", "broker_cpu_peak_percent", "percent"),
        ("Broker RAM peak GB", "broker_ram_peak_gb", "gb"),
        ("Producer CPU peak", "producer_cpu_peak_percent", "percent"),
        ("Consumer CPU peak", "consumer_cpu_peak_percent", "percent"),
        ("Primary conclusion", "primary_conclusion", "text"),
    ]
    table_rows = []
    for title, key, kind in rows:
        values = []
        numeric_values = [
            _float(summary.get(key))
            for summary in summaries
            if kind in {"mb", "gb", "percent", "percent3", "sec"}
        ]
        best_value = max(numeric_values, default=None)
        lower_is_better = key in {"pending_flush_percent", "failed_send_percent", "flush_sec"}
        if lower_is_better:
            best_value = min(numeric_values, default=None)
        for summary in summaries:
            text = _format_value(summary.get(key), kind)
            css_class = ""
            if best_value is not None and _float(summary.get(key)) == best_value:
                css_class = " class=\"best\""
            values.append(f"<td{css_class}>{_escape(text)}</td>")
        table_rows.append(f"<tr><th>{_escape(title)}</th>{''.join(values)}</tr>")
    cards = "".join(
        f"<article><span>{_escape(summary['label'])}</span><strong>{_escape(summary['case_name'])}</strong><em>{_escape(summary['sustained_verdict'])}</em></article>"
        for summary in summaries
    )
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>Kafka Benchmark Case Comparison</title>",
            "<style>",
            _comparison_css(),
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            "<section class=\"hero\">",
            "<p>Kafka MPI Benchmark</p>",
            "<h1>Case Comparison</h1>",
            "<div class=\"cards\">" + cards + "</div>",
            "</section>",
            "<section>",
            "<p class=\"note\">Green cells mark the best value per numeric row. For pending backlog, failed sends, and flush time, lower is better. For throughput and utilization rows, higher is highlighted.</p>",
            "<div class=\"table-wrap\"><table><thead><tr><th>Metric</th>"
            + "".join(f"<th>{_escape(label)}</th>" for label in labels)
            + "</tr></thead><tbody>"
            + "".join(table_rows)
            + "</tbody></table></div>",
            "</section>",
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare Kafka benchmark final_report.json files.")
    parser.add_argument("--html", help="Optional output path for a standalone HTML comparison report")
    parser.add_argument("reports", nargs="+", help="Case directory or final_report.json path")
    args = parser.parse_args(argv)
    reports = [load_report(path) for path in args.reports]
    if args.html:
        Path(args.html).write_text(build_comparison_html(reports), encoding="utf-8")
    print(build_comparison_markdown(reports), end="")
    return 0


def _broker_metric_mib_s(report: dict[str, Any], direction: str) -> float:
    metric_ids = {
        "in": ("kafka_jmx_bytes_in_counter_rate", "kafka_jmx_bytes_in_one_minute_rate"),
        "out": ("kafka_jmx_bytes_out_counter_rate", "kafka_jmx_bytes_out_one_minute_rate"),
    }[direction]
    metrics = _list(_dict(report.get("kafka_broker_throughput")).get("metrics"))
    for metric_id in metric_ids:
        for metric in metrics:
            metric = _dict(metric)
            if metric.get("id") != metric_id:
                continue
            if metric.get("avg_value") is not None:
                return _float(metric.get("avg_value")) / 1_048_576.0
            if metric.get("avg_megabytes_per_sec") is not None:
                return (
                    _float(metric.get("avg_megabytes_per_sec"))
                    * 1_000_000.0
                    / 1_048_576.0
                )
    return 0.0


def _network_capacity_mib_s(report: dict[str, Any], path_label: str) -> float:
    analysis = _dict(report.get("bottleneck_analysis"))
    for item in _list(analysis.get("network_path_comparisons")):
        item = _dict(item)
        if item.get("path") == path_label:
            return _float(item.get("capacity_mb_s")) * 1_000_000.0 / 1_048_576.0
    inventory = _dict(report.get("system_inventory"))
    for item in _list(inventory.get("network_tests")):
        item = _dict(item)
        if item.get("label") == path_label and item.get("status") == "completed":
            return _float(item.get("gbps")) * 1_000_000_000.0 / 8.0 / 1_048_576.0
    return 0.0


def _rate_mib_per_sec(
    metrics: dict[str, Any],
    *,
    bytes_key: str = "throughput_bytes_per_sec",
    mib_key: str = "throughput_mebibytes_per_sec",
    decimal_mb_key: str = "throughput_megabytes_per_sec",
) -> float:
    if metrics.get(bytes_key) is not None:
        return _float(metrics.get(bytes_key)) / 1_048_576.0
    if metrics.get(mib_key) is not None:
        return _float(metrics.get(mib_key))
    if metrics.get(decimal_mb_key) is not None:
        return _float(metrics.get(decimal_mb_key)) * 1_000_000.0 / 1_048_576.0
    return 0.0


def _first_positive(*values: Any) -> float:
    for value in values:
        number = _float(value)
        if number > 0:
            return number
    return 0.0


def _role_peak(monitoring: dict[str, Any], metric_id: str, role: str) -> float:
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        if metric.get("id") != metric_id:
            continue
        peaks = []
        for series in _list(metric.get("series")):
            series = _dict(series)
            labels = _dict(series.get("labels"))
            if labels.get("role") == role:
                peaks.append(_float(series.get("max_value")))
        return max(peaks, default=0.0)
    return 0.0


def _format_value(value: Any, kind: str) -> str:
    if kind == "mb":
        return f"{_float(value):.1f}"
    if kind == "gb":
        return f"{_float(value):.1f}"
    if kind == "percent":
        return f"{_float(value):.1f}%"
    if kind == "percent3":
        return f"{_float(value):.3f}%"
    if kind == "sec":
        return f"{_float(value):.1f}"
    return str(value)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _min_positive(values: list[float]) -> float:
    positives = [value for value in values if value > 0]
    return min(positives, default=0.0)


def _qualification_label(
    qualification: dict[str, Any],
    pending_percent: float,
    failed_percent: float,
    flush_sec: float,
) -> str:
    if qualification.get("eligible") is False:
        return "Ineligible"
    if qualification.get("qualified") is True:
        return "Qualified"
    if qualification.get("qualified") is False:
        return "Overdriven"
    if pending_percent > 5.0 or failed_percent > 0.1 or flush_sec > 10.0:
        return "Overdriven"
    return "Qualified"


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _comparison_css() -> str:
    return """
:root { --ink: #18212f; --muted: #5b677a; --line: #d9e0ea; --panel: #f7f9fc; --accent: #126c8f; --best: #e8f7ed; }
* { box-sizing: border-box; }
body { margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--ink); background: #fff; line-height: 1.45; }
main { width: min(1480px, calc(100vw - 40px)); margin: 0 auto; padding: 28px 0 48px; }
section { padding: 24px 0; border-bottom: 1px solid var(--line); }
.hero p { color: var(--accent); text-transform: uppercase; font-size: 12px; font-weight: 700; margin: 0 0 8px; }
h1 { margin: 0 0 18px; font-size: 34px; letter-spacing: 0; }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; }
article { border: 1px solid var(--line); border-radius: 8px; background: var(--panel); padding: 14px; }
article span, article em { display: block; color: var(--muted); font-style: normal; }
article strong { display: block; margin: 6px 0; overflow-wrap: anywhere; }
.note { color: var(--muted); max-width: 980px; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { padding: 9px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; overflow-wrap: anywhere; }
thead th { background: #eef3f8; }
tbody th { background: #f8fafc; min-width: 220px; }
td.best { background: var(--best); font-weight: 700; }
@media (max-width: 900px) { main { width: min(100vw - 24px, 1480px); } }
""".strip()


if __name__ == "__main__":
    raise SystemExit(main())
