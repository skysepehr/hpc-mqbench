from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any

from src.benchmark.html_report import enrich_report_diagnostics, write_html_report
from src.benchmark.system_inventory import (
    load_system_inventory,
    merge_system_inventory_into_result,
)


MONITORING_SECTION_START = "<!-- monitoring-summary:start -->"
MONITORING_SECTION_END = "<!-- monitoring-summary:end -->"

BROKER_THROUGHPUT_SECTION_START = "<!-- kafka-broker-throughput:start -->"
BROKER_THROUGHPUT_SECTION_END = "<!-- kafka-broker-throughput:end -->"

BROKER_THROUGHPUT_METRICS: tuple[tuple[str, str, str], ...] = (
    (
        "kafka_jmx_messages_in_counter_rate",
        "Kafka Broker Messages In Rate",
        "messages/sec",
    ),
    (
        "kafka_jmx_bytes_in_counter_rate",
        "Kafka Broker Bytes In Rate",
        "bytes/sec",
    ),
    (
        "kafka_jmx_bytes_out_counter_rate",
        "Kafka Broker Bytes Out Rate",
        "bytes/sec",
    ),
    (
        "kafka_jmx_bytes_total_counter_rate",
        "Kafka Broker Combined Bytes In+Out Rate",
        "bytes/sec",
    ),
    (
        "kafka_jmx_messages_in_one_minute_rate",
        "Kafka Broker Messages In One-Minute Rate",
        "messages/sec",
    ),
    (
        "kafka_jmx_bytes_in_one_minute_rate",
        "Kafka Broker Bytes In One-Minute Rate",
        "bytes/sec",
    ),
    (
        "kafka_jmx_bytes_out_one_minute_rate",
        "Kafka Broker Bytes Out One-Minute Rate",
        "bytes/sec",
    ),
)

BROKER_THROUGHPUT_NAME_TO_ID = {
    title.lower(): metric_id for metric_id, title, _unit in BROKER_THROUGHPUT_METRICS
}
BROKER_THROUGHPUT_NAME_TO_ID["kafka broker combined bytes in plus out rate"] = (
    "kafka_jmx_bytes_total_counter_rate"
)


def load_monitoring_snapshot(snapshot_path: str | Path) -> dict[str, Any] | None:
    """
    Load a monitoring snapshot if it exists and is valid JSON.
    """
    path = Path(snapshot_path)
    if not path.is_file():
        return None

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(f"Monitoring snapshot must be a JSON object: {path}")
    return data


def build_monitoring_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    """
    Convert the raw Prometheus snapshot into a compact report-friendly summary.
    """
    if snapshot.get("format") == "monitoring_bundle.v1":
        return _build_bundle_summary(snapshot)

    queries = snapshot.get("queries", [])
    if not isinstance(queries, list):
        queries = []

    query_summaries: list[dict[str, Any]] = []
    completed_queries = 0
    error_queries = 0
    total_samples = 0

    for query in queries:
        if not isinstance(query, dict):
            continue

        status = str(query.get("status", "unknown"))
        sample_count = int(query.get("sample_count", 0) or 0)
        if status == "completed":
            completed_queries += 1
        elif status == "error":
            error_queries += 1
        total_samples += sample_count

        query_summaries.append(
            {
                "name": query.get("name", "unknown"),
                "query": query.get("query", ""),
                "status": status,
                "sample_count": sample_count,
                "truncated": bool(query.get("truncated", False)),
                "value_preview": query.get("value_preview", ""),
            }
        )

    targets = snapshot.get("targets", {})
    if not isinstance(targets, dict):
        targets = {}

    return {
        "backend_id": snapshot.get("backend_id", "kafka"),
        "enabled": snapshot.get("status") != "skipped",
        "status": snapshot.get("status", "unknown"),
        "collected_at": snapshot.get("collected_at"),
        "endpoint": snapshot.get("endpoint"),
        "prometheus_url": snapshot.get("endpoint"),
        "target_health_counts": targets.get("health_counts", {}),
        "active_target_count": targets.get("active_count", 0),
        "dropped_target_count": targets.get("dropped_count", 0),
        "query_count": len(query_summaries),
        "completed_query_count": completed_queries,
        "error_query_count": error_queries,
        "total_sample_count": total_samples,
        "queries": query_summaries,
        "errors": snapshot.get("errors", []),
        "artifacts": {
            "json": "data/monitoring_snapshot.json",
            "markdown": "reports/monitoring_summary.md",
        },
    }


def _build_bundle_summary(bundle: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize the richer local monitoring bundle summary for final reports.
    """
    targets = bundle.get("targets", {})
    if not isinstance(targets, dict):
        targets = {}

    collected_metrics = [
        metric
        for metric in bundle.get("collected_metrics", [])
        if isinstance(metric, dict)
    ]
    missing_metrics = [
        metric for metric in bundle.get("missing_metrics", []) if isinstance(metric, dict)
    ]
    graphs = [
        graph
        for graph in bundle.get("graphs", [])
        if isinstance(graph, dict)
    ]

    return {
        "backend_id": bundle.get("backend_id", "kafka"),
        "enabled": bool(bundle.get("enabled", True)),
        "status": bundle.get("status", "unknown"),
        "source": "local_prometheus_bundle",
        "prometheus_url": bundle.get("prometheus_url"),
        "endpoint": bundle.get("prometheus_url"),
        "collected_at": bundle.get("created_at"),
        "time_window": bundle.get("time_window", {}),
        "summary_path": bundle.get("summary_path", "monitoring/monitoring_summary.json"),
        "raw_dir": bundle.get("raw_dir", "monitoring/raw"),
        "csv_dir": bundle.get("csv_dir", "monitoring/csv"),
        "graphs_dir": bundle.get("graphs_dir", "monitoring/graphs"),
        "target_health_counts": targets.get("health_counts", {}),
        "active_target_count": targets.get("active_count", 0),
        "dropped_target_count": targets.get("dropped_count", 0),
        "targets": targets,
        "node_roles": bundle.get("node_roles", {}),
        "events": bundle.get("events", []),
        "collected_metrics": collected_metrics,
        "missing_metrics": missing_metrics,
        "graphs": graphs,
        "query_count": len(collected_metrics) + len(missing_metrics),
        "completed_query_count": len(collected_metrics),
        "error_query_count": len(bundle.get("errors", [])),
        "total_sample_count": sum(
            int(metric.get("sample_count", 0) or 0) for metric in collected_metrics
        ),
        "errors": bundle.get("errors", []),
        "artifacts": {
            "summary": bundle.get("summary_path", "monitoring/monitoring_summary.json"),
            "raw_dir": bundle.get("raw_dir", "monitoring/raw"),
            "csv_dir": bundle.get("csv_dir", "monitoring/csv"),
            "graphs_dir": bundle.get("graphs_dir", "monitoring/graphs"),
        },
    }


def merge_monitoring_into_result(
    benchmark_result: dict[str, Any],
    snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Return a benchmark result dictionary with monitoring summary information.
    """
    merged = dict(benchmark_result)
    if snapshot is None:
        merged["monitoring"] = {
            "status": "not_collected",
            "enabled": False,
            "artifacts": {
                "json": "data/monitoring_snapshot.json",
                "markdown": "reports/monitoring_summary.md",
            },
        }
    else:
        merged["monitoring"] = build_monitoring_summary(snapshot)
    return merged


def merge_monitoring_into_report_files(
    output_dir: str | Path,
    snapshot_path: str | Path | None = None,
) -> None:
    """
    Merge a collected monitoring snapshot into final_report.json and .md.
    """
    output_path = Path(output_dir)
    resolved_snapshot_path = Path(snapshot_path) if snapshot_path is not None else None
    if resolved_snapshot_path is None:
        for candidate in (
            output_path / "data" / "monitoring_snapshot.json",
            output_path / "results" / "monitoring_snapshot.json",
        ):
            if candidate.is_file():
                resolved_snapshot_path = candidate
                break
        else:
            resolved_snapshot_path = output_path / "data" / "monitoring_snapshot.json"

    snapshot = load_monitoring_snapshot(resolved_snapshot_path)
    summary = build_monitoring_summary(snapshot) if snapshot is not None else None

    json_path = output_path / "final_report.json"
    benchmark_result_for_html: dict[str, Any] | None = None
    backend_id = str((summary or {}).get("backend_id", "kafka"))
    if json_path.is_file():
        with json_path.open("r", encoding="utf-8") as handle:
            benchmark_result = json.load(handle)
        if isinstance(benchmark_result, dict):
            config = benchmark_result.get("config", {})
            if isinstance(config, dict):
                backend_id = str(config.get("backend_id", backend_id))
            benchmark_result = merge_monitoring_into_result(benchmark_result, snapshot)
            benchmark_result = merge_system_inventory_into_result(
                benchmark_result,
                load_system_inventory(output_path),
            )
            if backend_id == "kafka":
                benchmark_result["kafka_broker_throughput"] = (
                    extract_kafka_broker_throughput(
                        benchmark_result.get("monitoring", {})
                    )
                )
            else:
                # The initial case report is written before post-run Prometheus
                # collection. Refresh the portable/backend namespaces once the
                # backend-specific monitoring evidence is available.
                from src.benchmark.backends import get_backend
                from src.benchmark.core.result_schema import enrich_result_schema

                enrich_result_schema(
                    benchmark_result,
                    get_backend(backend_id),
                )
            enrich_report_diagnostics(benchmark_result)
            with json_path.open("w", encoding="utf-8") as handle:
                json.dump(benchmark_result, handle, indent=2)
            data_dir = output_path / "data"
            data_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(json_path, data_dir / "final_report.json")
            benchmark_result_for_html = benchmark_result

    markdown_path = output_path / "final_report.md"
    if markdown_path.is_file() and summary is not None:
        original = markdown_path.read_text(encoding="utf-8")
        section = build_monitoring_markdown_section(summary)
        updated = replace_or_append_monitoring_section(original, section)
        if backend_id == "kafka":
            broker_section = build_kafka_broker_throughput_markdown_section(
                extract_kafka_broker_throughput(summary)
            )
            updated = replace_or_append_kafka_broker_throughput_section(
                updated,
                broker_section,
            )
        markdown_path.write_text(updated, encoding="utf-8")
        reports_dir = output_path / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_text = markdown_path.read_text(encoding="utf-8")
        report_text = report_text.replace("](reports/graphs/", "](graphs/")
        report_text = report_text.replace("](monitoring/graphs/", "](../monitoring/graphs/")
        (reports_dir / "final_report.md").write_text(report_text, encoding="utf-8")

    if benchmark_result_for_html is not None and _report_mode() == "full":
        write_html_report(output_path, benchmark_result_for_html)


def _report_mode() -> str:
    mode = os.environ.get("BENCHMARK_REPORT_MODE", "full").strip().lower()
    return mode if mode in {"full", "light", "machine"} else "full"


def build_monitoring_markdown_section(summary: dict[str, Any]) -> str:
    """
    Build the Markdown section inserted into final_report.md.
    """
    if summary.get("source") == "local_prometheus_bundle":
        return _build_bundle_markdown_section(summary)

    lines = [
        MONITORING_SECTION_START,
        "## Monitoring Summary",
        "",
        f"- **Status:** {summary.get('status', 'unknown')}",
        f"- **Collected At:** {summary.get('collected_at', 'unknown')}",
        f"- **Prometheus Endpoint:** {summary.get('endpoint', 'unknown')}",
        f"- **Active Targets:** {summary.get('active_target_count', 0)}",
        f"- **Dropped Targets:** {summary.get('dropped_target_count', 0)}",
        f"- **Target Health Counts:** {json.dumps(summary.get('target_health_counts', {}), sort_keys=True)}",
        f"- **Completed Queries:** {summary.get('completed_query_count', 0)} / {summary.get('query_count', 0)}",
        f"- **Total Samples Captured:** {summary.get('total_sample_count', 0)}",
        "",
        "| Metric Group | Samples | Value Preview | Status |",
        "|---|---:|---|---|",
    ]

    for query in summary.get("queries", []):
        if not isinstance(query, dict):
            continue
        name = _pipe_safe(query.get("name", "unknown"))
        sample_count = query.get("sample_count", 0)
        value_preview = _pipe_safe(query.get("value_preview", ""))
        status = _pipe_safe(query.get("status", "unknown"))
        lines.append(f"| {name} | {sample_count} | {value_preview} | {status} |")

    errors = summary.get("errors", [])
    if errors:
        lines.extend(["", "Monitoring collection notes:"])
        for error in errors:
            lines.append(f"- {error}")

    lines.extend(
        [
            "",
            "Raw monitoring artifacts:",
            "- `data/monitoring_snapshot.json`",
            "- `reports/monitoring_summary.md`",
            MONITORING_SECTION_END,
            "",
        ]
    )
    return "\n".join(lines)


def _build_bundle_markdown_section(summary: dict[str, Any]) -> str:
    """
    Build a richer Markdown section for local monitoring range data and graphs.
    """
    time_window = summary.get("time_window", {})
    collected_metrics = [
        metric
        for metric in summary.get("collected_metrics", [])
        if isinstance(metric, dict)
    ]
    missing_metrics = [
        metric for metric in summary.get("missing_metrics", []) if isinstance(metric, dict)
    ]
    graphs = [
        graph
        for graph in summary.get("graphs", [])
        if isinstance(graph, dict) and graph.get("status") == "generated"
    ]

    lines = [
        MONITORING_SECTION_START,
        "## Monitoring",
        "",
        f"- **Status:** {summary.get('status', 'unknown')}",
        f"- **Prometheus URL:** {summary.get('prometheus_url', 'unknown')}",
        f"- **Window Start:** {time_window.get('start', 'unknown')}",
        f"- **Window End:** {time_window.get('end', 'unknown')}",
        f"- **Step (sec):** {time_window.get('step_sec', 'unknown')}",
        f"- **Active Targets:** {summary.get('active_target_count', 0)}",
        f"- **Target Health Counts:** {json.dumps(summary.get('target_health_counts', {}), sort_keys=True)}",
        f"- **Collected Metrics:** {len(collected_metrics)}",
        f"- **Missing Metrics:** {len(missing_metrics)}",
        "",
        "Monitoring artifacts:",
        f"- Raw JSON: `{summary.get('raw_dir', 'monitoring/raw')}`",
        f"- CSV: `{summary.get('csv_dir', 'monitoring/csv')}`",
        f"- Graphs: `{summary.get('graphs_dir', 'monitoring/graphs')}`",
        f"- Summary: `{summary.get('summary_path', 'monitoring/monitoring_summary.json')}`",
        "",
    ]

    node_role_section = _build_node_role_map_section(summary.get("node_roles", {}))
    if node_role_section:
        lines.extend(node_role_section)

    if collected_metrics:
        lines.extend(_build_node_exporter_section(collected_metrics, summary.get("node_roles", {})))
        lines.extend(_build_jmx_exporter_section(collected_metrics, summary.get("node_roles", {})))
        lines.extend(_build_kafka_exporter_section(collected_metrics))
        lines.extend(_build_pulsar_metrics_section(collected_metrics))
        lines.extend(
            [
                "### Metric Artifacts",
                "",
                "| Metric | Source | Unit | Samples | CSV | Graph |",
                "|---|---|---|---:|---|---|",
            ]
        )
        for metric in collected_metrics:
            graph = metric.get("graph", {})
            graph_path = ""
            if isinstance(graph, dict) and graph.get("status") == "generated":
                graph_path = str(graph.get("path", ""))
            lines.append(
                "| {title} | {category} | {unit} | {samples} | `{csv_path}` | `{graph_path}` |".format(
                    title=_pipe_safe(metric.get("title", metric.get("id", "unknown"))),
                    category=_pipe_safe(metric.get("category", "")),
                    unit=_pipe_safe(metric.get("unit", "")),
                    samples=metric.get("sample_count", 0),
                    csv_path=_pipe_safe(metric.get("csv_path", "")),
                    graph_path=_pipe_safe(graph_path),
                )
            )
        lines.append("")

    if missing_metrics:
        lines.extend(["### Missing Metrics", ""])
        for metric in missing_metrics[:20]:
            lines.append(
                "- **{title}:** {reason}".format(
                    title=metric.get("title", metric.get("id", "unknown")),
                    reason=metric.get("reason", "not available"),
                )
            )
        if len(missing_metrics) > 20:
            lines.append(f"- ... {len(missing_metrics) - 20} more missing metrics")
        lines.append("")

    if graphs:
        lines.extend(["### Graphs", ""])
        for graph in graphs:
            graph_path = graph.get("path")
            metric_id = graph.get("metric_id", "metric")
            if graph_path:
                lines.append(f"![{metric_id}]({graph_path})")
                lines.append("")

    errors = summary.get("errors", [])
    if errors:
        lines.extend(["Monitoring collection notes:", ""])
        for error in errors:
            lines.append(f"- {error}")
        lines.append("")

    lines.extend(
        [
            "Local monitoring graphs are for functional validation of the pipeline. "
            "Final performance conclusions should still come from the monitored "
            "HPC/Slurm runs.",
            MONITORING_SECTION_END,
            "",
        ]
    )
    return "\n".join(lines)


def _build_node_role_map_section(node_roles: Any) -> list[str]:
    if not isinstance(node_roles, dict):
        return []
    nodes = [node for node in node_roles.get("nodes", []) if isinstance(node, dict)]
    if not nodes:
        return []

    lines = [
        "### Node Role Map",
        "",
        "| Node | Service IP | Role | MPI Ranks | Exporters |",
        "|---|---|---|---|---|",
    ]
    for node in nodes:
        exporters = []
        for exporter in node.get("exporters", []):
            if not isinstance(exporter, dict):
                continue
            name = exporter.get("name", "exporter")
            endpoint = exporter.get("endpoint", "")
            exporters.append(f"{name}@{endpoint}" if endpoint else str(name))
        lines.append(
            "| {node_name} | {address} | {role} | {ranks} | {exporters} |".format(
                node_name=_pipe_safe(node.get("node", "")),
                address=_pipe_safe(node.get("service_address", "")),
                role=_pipe_safe(node.get("primary_role", "")),
                ranks=_pipe_safe(_format_rank_summary(node.get("mpi_ranks", []))),
                exporters=_pipe_safe(", ".join(exporters) if exporters else ""),
            )
        )
    lines.append("")
    return lines


def _build_node_exporter_section(
    collected_metrics: list[dict[str, Any]],
    node_roles: Any,
) -> list[str]:
    metric_by_id = _metrics_by_id(collected_metrics)
    nodes = _report_nodes(node_roles, collected_metrics, category="system")
    if not nodes:
        return []

    lines = [
        "### Node Exporter",
        "",
        "| Node | Role | Avg CPU % | Max CPU % | Avg RAM Used GB | Max RAM Used GB | Min RAM Available GB | RAM Total GB | Avg RX MB/s | Max RX MB/s | Avg TX MB/s | Max TX MB/s |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for node_name, role in nodes:
        cpu_avg = _node_metric_value(
            metric_by_id,
            ("node_cpu_busy_percent",),
            node_name,
            node_roles,
            "avg_value",
        )
        cpu_max = _node_metric_value(
            metric_by_id,
            ("node_cpu_busy_percent",),
            node_name,
            node_roles,
            "max_value",
        )
        if cpu_avg is None:
            cpu_avg = _node_metric_value(
                metric_by_id,
                ("node_cpu_busy_ratio",),
                node_name,
                node_roles,
                "avg_value",
                factor=100.0,
            )
        if cpu_max is None:
            cpu_max = _node_metric_value(
                metric_by_id,
                ("node_cpu_busy_ratio",),
                node_name,
                node_roles,
                "max_value",
                factor=100.0,
            )

        mem_used_avg = _node_metric_value(
            metric_by_id,
            ("node_memory_used_gb",),
            node_name,
            node_roles,
            "avg_value",
        )
        mem_used_max = _node_metric_value(
            metric_by_id,
            ("node_memory_used_gb",),
            node_name,
            node_roles,
            "max_value",
        )
        mem_available_min = _node_metric_value(
            metric_by_id,
            ("node_memory_available_gb",),
            node_name,
            node_roles,
            "min_value",
        )
        mem_total = _node_metric_value(
            metric_by_id,
            ("node_memory_total_gb",),
            node_name,
            node_roles,
            "max_value",
        )

        if mem_available_min is None:
            mem_available_min = _node_metric_value(
                metric_by_id,
                ("node_memory_available_bytes",),
                node_name,
                node_roles,
                "min_value",
                factor=1 / 1_000_000_000,
            )
        if mem_total is None:
            mem_total = _node_metric_value(
                metric_by_id,
                ("node_memory_total_bytes",),
                node_name,
                node_roles,
                "max_value",
                factor=1 / 1_000_000_000,
            )
        if mem_used_avg is None:
            total_avg = _node_metric_value(
                metric_by_id,
                ("node_memory_total_bytes",),
                node_name,
                node_roles,
                "avg_value",
                factor=1 / 1_000_000_000,
            )
            avail_avg = _node_metric_value(
                metric_by_id,
                ("node_memory_available_bytes",),
                node_name,
                node_roles,
                "avg_value",
                factor=1 / 1_000_000_000,
            )
            if total_avg is not None and avail_avg is not None:
                mem_used_avg = total_avg - avail_avg
        if mem_used_max is None:
            total_max = _node_metric_value(
                metric_by_id,
                ("node_memory_total_bytes",),
                node_name,
                node_roles,
                "max_value",
                factor=1 / 1_000_000_000,
            )
            avail_min = _node_metric_value(
                metric_by_id,
                ("node_memory_available_bytes",),
                node_name,
                node_roles,
                "min_value",
                factor=1 / 1_000_000_000,
            )
            if total_max is not None and avail_min is not None:
                mem_used_max = total_max - avail_min

        rx_avg = _node_metric_value(
            metric_by_id,
            ("node_network_receive_mbps",),
            node_name,
            node_roles,
            "avg_value",
        )
        rx_max = _node_metric_value(
            metric_by_id,
            ("node_network_receive_mbps",),
            node_name,
            node_roles,
            "max_value",
        )
        tx_avg = _node_metric_value(
            metric_by_id,
            ("node_network_transmit_mbps",),
            node_name,
            node_roles,
            "avg_value",
        )
        tx_max = _node_metric_value(
            metric_by_id,
            ("node_network_transmit_mbps",),
            node_name,
            node_roles,
            "max_value",
        )
        if rx_avg is None:
            rx_avg = _node_metric_value(
                metric_by_id,
                ("node_network_receive_bytes_rate_by_instance",),
                node_name,
                node_roles,
                "avg_value",
                factor=1 / 1_000_000,
            )
        if rx_max is None:
            rx_max = _node_metric_value(
                metric_by_id,
                ("node_network_receive_bytes_rate_by_instance",),
                node_name,
                node_roles,
                "max_value",
                factor=1 / 1_000_000,
            )
        if tx_avg is None:
            tx_avg = _node_metric_value(
                metric_by_id,
                ("node_network_transmit_bytes_rate_by_instance",),
                node_name,
                node_roles,
                "avg_value",
                factor=1 / 1_000_000,
            )
        if tx_max is None:
            tx_max = _node_metric_value(
                metric_by_id,
                ("node_network_transmit_bytes_rate_by_instance",),
                node_name,
                node_roles,
                "max_value",
                factor=1 / 1_000_000,
            )

        lines.append(
            "| {node} | {role} | {cpu_avg} | {cpu_max} | {mem_used_avg} | {mem_used_max} | {mem_available_min} | {mem_total} | {rx_avg} | {rx_max} | {tx_avg} | {tx_max} |".format(
                node=_pipe_safe(node_name),
                role=_pipe_safe(role),
                cpu_avg=_format_metric_cell(cpu_avg),
                cpu_max=_format_metric_cell(cpu_max),
                mem_used_avg=_format_metric_cell(mem_used_avg),
                mem_used_max=_format_metric_cell(mem_used_max),
                mem_available_min=_format_metric_cell(mem_available_min),
                mem_total=_format_metric_cell(mem_total),
                rx_avg=_format_metric_cell(rx_avg),
                rx_max=_format_metric_cell(rx_max),
                tx_avg=_format_metric_cell(tx_avg),
                tx_max=_format_metric_cell(tx_max),
            )
        )
    lines.append("")
    return lines


def _build_jmx_exporter_section(
    collected_metrics: list[dict[str, Any]],
    node_roles: Any,
) -> list[str]:
    metric_by_id = _metrics_by_id(collected_metrics)
    specs = (
        ("kafka_jmx_messages_in_counter_rate", "Messages in", "messages/sec", 1.0),
        ("kafka_jmx_bytes_in_counter_rate", "Bytes in", "MB/s", 1 / 1_000_000),
        ("kafka_jmx_bytes_out_counter_rate", "Bytes out", "MB/s", 1 / 1_000_000),
        ("jvm_heap_used_gb", "JVM heap used", "GB", 1.0),
        ("jvm_heap_used_bytes", "JVM heap used", "GB", 1 / 1_000_000_000),
        ("jvm_nonheap_used_gb", "JVM non-heap used", "GB", 1.0),
        ("jvm_nonheap_used_bytes", "JVM non-heap used", "GB", 1 / 1_000_000_000),
        ("jvm_thread_count", "JVM threads", "threads", 1.0),
        ("kafka_jmx_under_replicated_partitions", "Under-replicated partitions", "partitions", 1.0),
        ("kafka_jmx_active_controller_count", "Active controller count", "controllers", 1.0),
    )
    rows: list[str] = []
    for metric_id, label, unit, factor in specs:
        metric = metric_by_id.get(metric_id)
        if not isinstance(metric, dict):
            continue
        series = metric.get("series", [])
        if isinstance(series, list) and series:
            for item in series:
                if not isinstance(item, dict):
                    continue
                node_name, role = _series_node_role(item, node_roles)
                rows.append(
                    "| {node} | {role} | {metric} | {avg} | {max_value} | {unit} | {samples} |".format(
                        node=_pipe_safe(node_name),
                        role=_pipe_safe(role),
                        metric=_pipe_safe(label),
                        avg=_format_metric_cell(_scaled(item.get("avg_value"), factor)),
                        max_value=_format_metric_cell(_scaled(item.get("max_value"), factor)),
                        unit=_pipe_safe(unit),
                        samples=item.get("sample_count", 0),
                    )
                )
        else:
            rows.append(
                "| {node} | {role} | {metric} | {avg} | {max_value} | {unit} | {samples} |".format(
                    node="",
                    role="",
                    metric=_pipe_safe(label),
                    avg=_format_metric_cell(_scaled(metric.get("avg_value"), factor)),
                    max_value=_format_metric_cell(_scaled(metric.get("max_value"), factor)),
                    unit=_pipe_safe(unit),
                    samples=metric.get("sample_count", 0),
                )
            )

    if not rows:
        return []
    return [
        "### JMX Exporter",
        "",
        "| Node | Role | Metric | Avg | Max | Unit | Samples |",
        "|---|---|---|---:|---:|---|---:|",
        *rows,
        "",
    ]


def _build_kafka_exporter_section(collected_metrics: list[dict[str, Any]]) -> list[str]:
    rows: list[str] = []
    for metric in collected_metrics:
        if not isinstance(metric, dict) or metric.get("category") != "kafka_exporter":
            continue
        rows.append(
            "| {metric} | {unit} | {series} | {samples} | {avg} | {max_value} | `{csv}` |".format(
                metric=_pipe_safe(metric.get("title", metric.get("id", "unknown"))),
                unit=_pipe_safe(metric.get("unit", "")),
                series=metric.get("series_count", 0),
                samples=metric.get("sample_count", 0),
                avg=_format_metric_cell(metric.get("avg_value")),
                max_value=_format_metric_cell(metric.get("max_value")),
                csv=_pipe_safe(metric.get("csv_path", "")),
            )
        )
    if not rows:
        return []
    return [
        "### Kafka Exporter",
        "",
        "| Metric | Unit | Series | Samples | Avg | Max | CSV |",
        "|---|---|---:|---:|---:|---:|---|",
        *rows,
        "",
    ]


def _build_pulsar_metrics_section(
    collected_metrics: list[dict[str, Any]],
) -> list[str]:
    rows: list[str] = []
    for metric in collected_metrics:
        if not isinstance(metric, dict) or metric.get("category") not in {
            "pulsar",
            "pulsar_jvm",
            "pulsar_storage",
        }:
            continue
        rows.append(
            "| {metric} | {unit} | {series} | {samples} | {avg} | {max_value} | `{csv}` |".format(
                metric=_pipe_safe(metric.get("title", metric.get("id", "unknown"))),
                unit=_pipe_safe(metric.get("unit", "")),
                series=metric.get("series_count", 0),
                samples=metric.get("sample_count", 0),
                avg=_format_metric_cell(metric.get("avg_value")),
                max_value=_format_metric_cell(metric.get("max_value")),
                csv=_pipe_safe(metric.get("csv_path", "")),
            )
        )
    if not rows:
        return []
    return [
        "### Pulsar Native Metrics",
        "",
        (
            "Pulsar exposes these broker, managed-ledger, and JVM metrics "
            "directly from its HTTP metrics endpoint."
        ),
        "",
        "| Metric | Unit | Series | Samples | Avg | Max | CSV |",
        "|---|---|---:|---:|---:|---:|---|",
        *rows,
        "",
    ]


def replace_or_append_monitoring_section(markdown: str, section: str) -> str:
    """
    Insert or replace the generated monitoring section in Markdown text.
    """
    start = markdown.find(MONITORING_SECTION_START)
    end = markdown.find(MONITORING_SECTION_END)

    if start != -1 and end != -1 and end > start:
        end += len(MONITORING_SECTION_END)
        return f"{markdown[:start].rstrip()}\n\n{section}{markdown[end:].lstrip()}"

    notes_header = "\n## Notes\n"
    notes_index = markdown.find(notes_header)
    if notes_index != -1:
        return (
            f"{markdown[:notes_index].rstrip()}\n\n"
            f"{section}"
            f"{markdown[notes_index:].lstrip()}"
        )

    return f"{markdown.rstrip()}\n\n{section}"


def extract_kafka_broker_throughput(monitoring: dict[str, Any]) -> dict[str, Any]:
    """
    Extract report-ready Kafka broker throughput metrics from monitoring data.
    """
    if not isinstance(monitoring, dict) or not monitoring:
        return {
            "enabled": False,
            "status": "not_collected",
            "metrics": [],
            "reason": "monitoring object is absent",
        }

    metrics: list[dict[str, Any]] = []
    title_by_id = {
        metric_id: title for metric_id, title, _unit in BROKER_THROUGHPUT_METRICS
    }
    unit_by_id = {
        metric_id: unit for metric_id, _title, unit in BROKER_THROUGHPUT_METRICS
    }

    collected_metrics = monitoring.get("collected_metrics", [])
    if isinstance(collected_metrics, list):
        collected_by_id = {
            metric.get("id"): metric
            for metric in collected_metrics
            if isinstance(metric, dict)
        }
        for metric_id, title, unit in BROKER_THROUGHPUT_METRICS:
            metric = collected_by_id.get(metric_id)
            if not isinstance(metric, dict):
                continue
            graph = metric.get("graph", {})
            metrics.append(
                {
                    "id": metric_id,
                    "title": metric.get("title", title),
                    "unit": metric.get("unit", unit),
                    "source": "prometheus_range",
                    "query": metric.get("query", ""),
                    "sample_count": metric.get("sample_count", 0),
                    "numeric_sample_count": metric.get("numeric_sample_count", 0),
                    "min_value": metric.get("min_value"),
                    "avg_value": metric.get("avg_value"),
                    "max_value": metric.get("max_value"),
                    "avg_megabytes_per_sec": _bytes_per_sec_to_mb_or_none(
                        metric.get("avg_value"),
                        metric.get("unit", unit),
                    ),
                    "max_megabytes_per_sec": _bytes_per_sec_to_mb_or_none(
                        metric.get("max_value"),
                        metric.get("unit", unit),
                    ),
                    "series": metric.get("series", []),
                    "csv_path": metric.get("csv_path", ""),
                    "raw_path": metric.get("raw_path", ""),
                    "graph_path": (
                        graph.get("path")
                        if isinstance(graph, dict) and graph.get("status") == "generated"
                        else ""
                    ),
                }
            )

    queries = monitoring.get("queries", [])
    if not metrics and isinstance(queries, list):
        for query in queries:
            if not isinstance(query, dict):
                continue
            query_name = str(query.get("name", "")).lower()
            metric_id = BROKER_THROUGHPUT_NAME_TO_ID.get(query_name)
            if not metric_id:
                continue
            metrics.append(
                {
                    "id": metric_id,
                    "title": title_by_id.get(metric_id, query.get("name", metric_id)),
                    "unit": unit_by_id.get(metric_id, ""),
                    "source": "prometheus_snapshot",
                    "query": query.get("query", ""),
                    "sample_count": query.get("sample_count", 0),
                    "status": query.get("status", "unknown"),
                    "value_preview": query.get("value_preview", ""),
                    "avg_megabytes_per_sec": _bytes_per_sec_to_mb_or_none(
                        query.get("value_preview", ""),
                        unit_by_id.get(metric_id, ""),
                    ),
                }
            )

    return {
        "enabled": bool(metrics),
        "status": "collected" if metrics else "not_collected",
        "monitoring_status": monitoring.get("status", "unknown"),
        "prometheus_url": monitoring.get("prometheus_url") or monitoring.get("endpoint"),
        "source": monitoring.get("source", "prometheus_snapshot"),
        "metrics": metrics,
        "reason": "" if metrics else "broker throughput metrics were not found in monitoring data",
    }


def build_kafka_broker_throughput_markdown_section(
    broker_throughput: dict[str, Any],
) -> str:
    """
    Build the generated Kafka Broker Throughput Markdown block.
    """
    lines = [
        BROKER_THROUGHPUT_SECTION_START,
        "## Kafka Broker Throughput",
        "",
    ]

    metrics = [
        metric
        for metric in broker_throughput.get("metrics", [])
        if isinstance(metric, dict)
    ]
    if not metrics:
        lines.append(
            "Kafka broker throughput was not collected for this case. Enable "
            "Prometheus/JMX monitoring to populate this section."
        )
        reason = broker_throughput.get("reason")
        if reason:
            lines.append(f"- **Reason:** {reason}")
        lines.extend([BROKER_THROUGHPUT_SECTION_END, ""])
        return "\n".join(lines)

    lines.extend(
        [
            "These values come from Kafka broker JMX metrics collected by "
            "Prometheus. Counter-derived rates are preferred for short runs.",
            "",
            "| Node | Role | Metric | Avg / Value | Max | Avg MB/s | Max MB/s | Unit | Samples | Artifact / Query |",
            "|---|---|---|---:|---:|---:|---:|---|---:|---|",
        ]
    )

    for metric in metrics:
        artifact = metric.get("csv_path") or metric.get("query") or ""
        series = [item for item in metric.get("series", []) if isinstance(item, dict)]
        if series:
            for item in series:
                labels = item.get("labels", {})
                if not isinstance(labels, dict):
                    labels = {}
                node = labels.get("node") or labels.get("instance", "")
                role = labels.get("role", "")
                avg_value = item.get("avg_value")
                max_value = item.get("max_value")
                lines.append(
                    "| {node} | {role} | {title} | {value} | {max_value} | {avg_mb} | {max_mb} | {unit} | {samples} | {artifact} |".format(
                        node=_pipe_safe(node),
                        role=_pipe_safe(role),
                        title=_pipe_safe(metric.get("title", metric.get("id", "unknown"))),
                        value=_format_metric_cell(avg_value),
                        max_value=_format_metric_cell(max_value),
                        avg_mb=_format_metric_cell(
                            _bytes_per_sec_to_mb_or_none(avg_value, metric.get("unit", ""))
                        ),
                        max_mb=_format_metric_cell(
                            _bytes_per_sec_to_mb_or_none(max_value, metric.get("unit", ""))
                        ),
                        unit=_pipe_safe(metric.get("unit", "")),
                        samples=item.get("sample_count", 0),
                        artifact=_pipe_safe(artifact),
                    )
                )
            continue

        value = metric.get("avg_value")
        if value is None:
            value = metric.get("value_preview", "")
        lines.append(
            "| {node} | {role} | {title} | {value} | {max_value} | {avg_mb} | {max_mb} | {unit} | {samples} | {artifact} |".format(
                node="",
                role="",
                title=_pipe_safe(metric.get("title", metric.get("id", "unknown"))),
                value=_format_metric_cell(value),
                max_value=_format_metric_cell(metric.get("max_value")),
                avg_mb=_format_metric_cell(metric.get("avg_megabytes_per_sec")),
                max_mb=_format_metric_cell(metric.get("max_megabytes_per_sec")),
                unit=_pipe_safe(metric.get("unit", "")),
                samples=metric.get("sample_count", 0),
                artifact=_pipe_safe(artifact),
            )
        )

    lines.extend(["", BROKER_THROUGHPUT_SECTION_END, ""])
    return "\n".join(lines)


def replace_or_append_kafka_broker_throughput_section(
    markdown: str,
    section: str,
) -> str:
    """
    Replace the generated broker throughput section even for older reports.
    """
    marker_start = markdown.find(BROKER_THROUGHPUT_SECTION_START)
    marker_end = markdown.find(BROKER_THROUGHPUT_SECTION_END)
    if marker_start != -1 and marker_end != -1 and marker_end > marker_start:
        marker_end += len(BROKER_THROUGHPUT_SECTION_END)
        return f"{markdown[:marker_start].rstrip()}\n\n{section}{markdown[marker_end:].lstrip()}"

    header = "\n## Kafka Broker Throughput\n"
    start = markdown.find(header)
    if start == -1 and markdown.startswith("## Kafka Broker Throughput\n"):
        start = 0
    if start != -1:
        content_start = start + (1 if markdown[start] == "\n" else 0)
        next_header = markdown.find("\n## ", content_start + len("## Kafka Broker Throughput\n"))
        if next_header == -1:
            return f"{markdown[:content_start].rstrip()}\n\n{section}"
        return (
            f"{markdown[:content_start].rstrip()}\n\n"
            f"{section}"
            f"{markdown[next_header:].lstrip()}"
        )

    metric_header = "\n## Metric Summary\n"
    metric_index = markdown.find(metric_header)
    if metric_index != -1:
        next_header = markdown.find("\n## ", metric_index + len(metric_header))
        if next_header != -1:
            return (
                f"{markdown[:next_header].rstrip()}\n\n"
                f"{section}"
                f"{markdown[next_header:].lstrip()}"
            )

    return f"{markdown.rstrip()}\n\n{section}"


def _format_rank_summary(ranks: Any) -> str:
    if not isinstance(ranks, list) or not ranks:
        return ""

    by_role: dict[str, list[int]] = {}
    for item in ranks:
        if not isinstance(item, dict):
            continue
        try:
            rank = int(item.get("rank"))
        except (TypeError, ValueError):
            continue
        role = str(item.get("role", "unknown"))
        by_role.setdefault(role, []).append(rank)

    parts = []
    for role in ("controller", "producer", "consumer", "unknown"):
        values = sorted(by_role.get(role, []))
        if not values:
            continue
        parts.append(f"{role}: {_compact_ranges(values)}")
    return ", ".join(parts)


def _compact_ranges(values: list[int]) -> str:
    if not values:
        return ""
    ranges: list[str] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def _metrics_by_id(metrics: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(metric.get("id")): metric
        for metric in metrics
        if isinstance(metric, dict) and metric.get("id")
    }


def _node_role_lookup(node_roles: Any) -> tuple[dict[str, str], dict[str, str], list[tuple[str, str]]]:
    node_to_role: dict[str, str] = {}
    address_to_node: dict[str, str] = {}
    ordered_nodes: list[tuple[str, str]] = []

    if not isinstance(node_roles, dict):
        return node_to_role, address_to_node, ordered_nodes

    for item in node_roles.get("nodes", []):
        if not isinstance(item, dict):
            continue
        node = str(item.get("node", ""))
        if not node:
            continue
        role = str(item.get("primary_role", "unknown"))
        node_to_role[node] = role
        ordered_nodes.append((node, role))
        address = str(item.get("service_address", ""))
        if address:
            address_to_node[address] = node
    return node_to_role, address_to_node, ordered_nodes


def _report_nodes(
    node_roles: Any,
    collected_metrics: list[dict[str, Any]],
    category: str,
) -> list[tuple[str, str]]:
    _node_to_role, _address_to_node, ordered_nodes = _node_role_lookup(node_roles)
    if ordered_nodes:
        return ordered_nodes

    discovered: dict[str, str] = {}
    for metric in collected_metrics:
        if not isinstance(metric, dict) or metric.get("category") != category:
            continue
        for series in metric.get("series", []):
            if not isinstance(series, dict):
                continue
            node, role = _series_node_role(series, node_roles)
            if node:
                discovered[node] = role
    return sorted(discovered.items())


def _node_metric_value(
    metric_by_id: dict[str, dict[str, Any]],
    metric_ids: tuple[str, ...],
    node_name: str,
    node_roles: Any,
    field: str,
    factor: float = 1.0,
) -> float | None:
    for metric_id in metric_ids:
        metric = metric_by_id.get(metric_id)
        if not isinstance(metric, dict):
            continue
        for series in metric.get("series", []):
            if not isinstance(series, dict):
                continue
            series_node, _role = _series_node_role(series, node_roles)
            if series_node != node_name:
                continue
            return _scaled(series.get(field), factor)
    return None


def _series_node_role(series: dict[str, Any], node_roles: Any) -> tuple[str, str]:
    labels = series.get("labels", {})
    if not isinstance(labels, dict):
        labels = {}
    node = str(labels.get("node", "") or "")
    role = str(labels.get("role", "") or "")
    node_to_role, address_to_node, _ordered_nodes = _node_role_lookup(node_roles)

    if not node:
        instance = str(labels.get("instance", "") or "")
        host = instance.split(":", 1)[0]
        node = address_to_node.get(host, host)
    if not role and node:
        role = node_to_role.get(node, "")
    return node, role


def _scaled(value: Any, factor: float = 1.0) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value) * factor
    except (TypeError, ValueError):
        return None


def _pipe_safe(value: object) -> str:
    return str(value).replace("|", "\\|")


def _format_metric_number(value: object) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return _pipe_safe(value)
    if abs(numeric) >= 1000:
        return f"{numeric:,.3f}"
    return f"{numeric:.6g}"


def _format_metric_cell(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return _pipe_safe(value)
    if abs(numeric) >= 1000:
        return f"{numeric:,.3f}"
    return f"{numeric:.6g}"


def _bytes_per_sec_to_mb_or_none(value: Any, unit: Any) -> float | None:
    """
    Convert byte-per-second metric values to decimal MB/s for reports.
    """
    if str(unit).strip().lower() != "bytes/sec":
        return None
    try:
        return float(value) / 1_000_000
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge Prometheus monitoring snapshot data into final reports"
    )
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--snapshot", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    merge_monitoring_into_report_files(args.output_dir, args.snapshot)


if __name__ == "__main__":
    main()
