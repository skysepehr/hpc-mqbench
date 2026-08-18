from __future__ import annotations

import base64
import html
import json
import os
import shutil
from pathlib import Path
from typing import Any

from src.benchmark.qualification import (
    backlog_denominator_for_policy,
    producer_operational_metrics,
)


def write_html_report(output_dir: str | Path, benchmark_result: dict[str, Any]) -> None:
    """Write a self-contained HTML report with embedded graph artifacts."""
    output_path = Path(output_dir)
    html_text = build_html_report(output_path, benchmark_result)
    report_path = output_path / "final_report.html"
    report_path.write_text(html_text, encoding="utf-8")

    reports_dir = output_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(report_path, reports_dir / "final_report.html")


def enrich_report_diagnostics(benchmark_result: dict[str, Any]) -> dict[str, Any]:
    """Attach computed report diagnostics used by JSON and HTML outputs."""
    case = _dict(benchmark_result.get("case"))
    config = _dict(benchmark_result.get("config"))
    scenario_execution = _dict(benchmark_result.get("scenario_execution"))
    aggregated = _dict(benchmark_result.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    monitoring = _dict(benchmark_result.get("monitoring"))
    inventory = _dict(benchmark_result.get("system_inventory"))
    broker_throughput = _dict(benchmark_result.get("kafka_broker_throughput"))

    benchmark_result.setdefault("runtime_options", _build_runtime_options())
    benchmark_result["scenario_classification"] = _build_scenario_classification(
        case,
        config,
        scenario_execution,
    )
    benchmark_result["bottleneck_analysis"] = _build_bottleneck_analysis(
        config,
        producers,
        consumers,
        broker_throughput,
        monitoring,
        inventory,
    )
    benchmark_result["throughput_verdict"] = _build_throughput_verdict(
        config,
        producers,
        consumers,
        broker_throughput,
    )
    return benchmark_result


def build_html_report(output_dir: Path, benchmark_result: dict[str, Any]) -> str:
    enrich_report_diagnostics(benchmark_result)
    case = _dict(benchmark_result.get("case"))
    config = _dict(benchmark_result.get("config"))
    aggregated = _dict(benchmark_result.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    monitoring = _dict(benchmark_result.get("monitoring"))
    inventory = _dict(benchmark_result.get("system_inventory"))
    broker_throughput = _dict(benchmark_result.get("kafka_broker_throughput"))
    scenario_classification = _dict(benchmark_result.get("scenario_classification"))
    bottleneck_analysis = _dict(benchmark_result.get("bottleneck_analysis"))
    throughput_verdict = _dict(benchmark_result.get("throughput_verdict"))
    egress_prefill = _dict(benchmark_result.get("egress_prefill"))
    runtime_options = _dict(benchmark_result.get("runtime_options"))

    title = f"Kafka MPI Benchmark Report - {case.get('case_name', 'unknown_case')}"
    sections = [
        _hero_section(case, config, scenario_classification),
        _benchmark_configuration_section(case, config, runtime_options),
        _executive_summary_section(config, producers, consumers, broker_throughput, egress_prefill),
        _run_verdict_section(case, config, producers, consumers, broker_throughput, monitoring, inventory),
        _scenario_result_interpretation_section(
            scenario_classification,
            producers,
            consumers,
            broker_throughput,
            egress_prefill,
        ),
        _node_role_section(monitoring, inventory),
        _system_inventory_section(inventory),
        _system_saturation_section(monitoring, inventory),
        _monitoring_section(monitoring, inventory),
        _graph_gallery_section(output_dir, benchmark_result),
        _scenario_classification_section(scenario_classification),
        _sustained_throughput_verdict_section(throughput_verdict),
        _bottleneck_analysis_section(bottleneck_analysis),
        _producer_flush_diagnostics_section(producers),
        _client_runtime_diagnostics_section(producers, consumers),
        _definitions_section(),
        _exporter_source_map_section(monitoring, inventory),
        _report_completeness_section(monitoring, inventory),
        _timeline_section(benchmark_result),
        _exporter_explanation_section(),
        _interpretation_section(config, producers, consumers, broker_throughput, monitoring, inventory),
    ]

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{_escape(title)}</title>",
            "<style>",
            _css(),
            "</style>",
            "</head>",
            "<body>",
            '<main class="page">',
            *sections,
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def _hero_section(
    case: dict[str, Any],
    config: dict[str, Any],
    classification: dict[str, Any],
) -> str:
    rows = [
        ("Case ID", case.get("case_id", "unknown")),
        ("Status", case.get("status", "unknown")),
        ("Scenario", classification.get("label", config.get("scenario", "unknown"))),
        ("Configured scenario", config.get("scenario", "unknown")),
        ("Topic", config.get("topic_name", "unknown")),
    ]
    return f"""
<section class="hero">
  <div class="hero-copy">
    <p class="eyebrow">Kafka MPI Benchmark</p>
    <h1>{_escape(case.get('case_name', 'unknown_case'))}</h1>
    <p class="lede">One-broker MPI/Python Kafka benchmark with JMX broker metrics, per-node exporter metrics, and same-allocation system capability measurements.</p>
  </div>
  <div class="table-wrap compact">{_table(['Field', 'Value'], rows)}</div>
</section>
"""


def _executive_summary_section(
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    egress_prefill: dict[str, Any],
) -> str:
    scenario = str(config.get("scenario", "unknown"))
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_total = _broker_combined_metric(broker_throughput, broker_in, broker_out)
    summary_cards: list[str] = []
    if scenario in {"ingress_ramp", "simultaneous", "consume_and_process"}:
        summary_cards.append(_kpi("Producer delivered", f"{_metric_mib_s(producers):.1f} MiB/s", f"{_int(producers.get('messages_delivered'))} messages"))
    if scenario in {"egress_only", "simultaneous", "consume_and_process"}:
        summary_cards.append(_kpi("Consumer received", f"{_metric_mib_s(consumers):.1f} MiB/s", f"{_int(consumers.get('messages_received'))} messages"))
    if scenario in {"ingress_ramp", "simultaneous", "consume_and_process"}:
        summary_cards.append(_kpi("Broker ingress avg", _broker_avg_kpi_value(broker_in), "JMX bytes-in average"))
        summary_cards.append(_kpi("Broker ingress peak", _broker_peak_kpi_value(broker_in), "highest JMX bytes-in sample"))
    if scenario in {"egress_only", "simultaneous", "consume_and_process"}:
        summary_cards.append(_kpi("Broker egress avg", _broker_avg_kpi_value(broker_out), "JMX bytes-out average"))
        summary_cards.append(_kpi("Broker egress peak", _broker_peak_kpi_value(broker_out), "highest JMX bytes-out sample"))
    if scenario in {"simultaneous", "consume_and_process"}:
        summary_cards.append(_kpi("Broker combined avg", _broker_avg_kpi_value(broker_total), "JMX bytes-in plus bytes-out"))
    if scenario == "egress_only" and egress_prefill:
        summary_cards.append(_kpi("Prefill delivered", f"{_int(egress_prefill.get('messages_delivered'))}", "setup messages, not measured throughput"))

    return f"""
<section>
  <h2>Executive Summary</h2>
  <p class="section-note">Avg is the arithmetic mean over the sampled monitoring window; peak is the highest individual sample in that same window.</p>
  <div class="kpi-grid">{''.join(summary_cards)}</div>
</section>
"""


def _benchmark_configuration_section(
    case: dict[str, Any],
    config: dict[str, Any],
    runtime_options: dict[str, Any],
) -> str:
    extra = _dict(config.get("extra"))
    rows = [
        ("Config file", _config_path_text(case)),
        ("Case ID", case.get("case_id", "unknown")),
        ("Scenario", _scenario_plain_label(str(config.get("scenario", "unknown")))),
        ("Topic name", config.get("topic_name", "unknown")),
        ("Partitions", config.get("partitions", "unknown")),
        ("Replication factor", config.get("replication_factor", "unknown")),
        ("Producer ranks", config.get("producer_ranks", "unknown")),
        ("Consumer ranks", config.get("consumer_ranks", "unknown")),
        ("Duration seconds", config.get("duration_sec", "unknown")),
        ("Payload size bytes", config.get("payload_size_bytes", "unknown")),
        ("Virtual devices per rank", config.get("virtual_devices_per_rank", "unknown")),
        ("Acks", config.get("acks", "unknown")),
        ("Compression type", config.get("compression_type", "unknown")),
        ("Batch size", config.get("batch_size", "unknown")),
        ("linger.ms", config.get("linger_ms", "unknown")),
        ("Common client settings", _compact_json(extra.get("kafka_common_client_config"))),
        ("Producer/client settings", _compact_json(extra.get("kafka_producer_config"))),
        ("Consumer/client settings", _compact_json(extra.get("kafka_consumer_config"))),
        ("Broker settings", _broker_settings_text(extra)),
        ("RAM-backed runtime", _runtime_option_text(runtime_options, "enable_ram_backed_runtime")),
        ("System inventory", _runtime_option_text(runtime_options, "enable_system_inventory")),
    ]
    full_config = json.dumps(config, indent=2, sort_keys=True)
    return f"""
<section class="config-used">
  <h2>Benchmark Configuration Used</h2>
  <p class="section-note">This is the effective normalized configuration used for this run, shown before throughput results so runs can be compared later without opening the JSON artifact.</p>
  <div class="table-wrap compact">{_table(['Setting', 'Value'], rows)}</div>
  <details class="config-details">
    <summary>Full effective configuration JSON</summary>
    <pre>{_escape(full_config)}</pre>
  </details>
</section>
"""


def _scenario_result_interpretation_section(
    classification: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    egress_prefill: dict[str, Any],
) -> str:
    scenario_type = str(classification.get("scenario_type", "unknown"))
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_total = _broker_combined_metric(broker_throughput, broker_in, broker_out)

    if scenario_type == "ingress_only":
        rows = [
            ("Measured workload", "Ingress-only: producer ranks write to Kafka; consumer ranks are assigned but inactive."),
            ("Main producer result", f"{_metric_mib_s(producers):.1f} MiB/s delivered by MPI/librdkafka callbacks."),
            ("Main broker result", f"{_broker_avg_mib_s(broker_in):.1f} MiB/s broker ingress avg from JMX." if broker_in else "Broker ingress JMX unavailable."),
            ("Supporting signals", f"flush {_float(producers.get('max_flush_duration_sec')):.1f}s; failed sends {_failed_send_text(producers)}; pending {_pending_flush_text(producers)}."),
            ("Not measured", "Consumer throughput is intentionally inactive and should not be interpreted as a result for this scenario."),
        ]
    elif scenario_type == "egress_only":
        rows = [
            ("Measured workload", "Egress-only: Kafka is prefilled first, then consumer ranks read from Kafka."),
            ("Main consumer result", f"{_metric_mib_s(consumers):.1f} MiB/s received by MPI consumers."),
            ("Main broker result", f"{_broker_avg_mib_s(broker_out):.1f} MiB/s broker egress avg from JMX." if broker_out else "Broker egress JMX unavailable."),
            ("Prefill status", _prefill_text(egress_prefill)),
            ("Not measured", "Producer prefill is setup traffic and is not counted as measured producer throughput."),
        ]
    else:
        rows = [
            ("Measured workload", "Simultaneous: producer and consumer ranks run together."),
            ("Producer result", f"{_metric_mib_s(producers):.1f} MiB/s delivered by MPI/librdkafka callbacks."),
            ("Consumer result", f"{_metric_mib_s(consumers):.1f} MiB/s received by MPI consumers."),
            ("Broker ingress", f"{_broker_avg_mib_s(broker_in):.1f} MiB/s avg from JMX." if broker_in else "Broker ingress JMX unavailable."),
            ("Broker egress", f"{_broker_avg_mib_s(broker_out):.1f} MiB/s avg from JMX." if broker_out else "Broker egress JMX unavailable."),
            ("Broker combined pressure", f"{_broker_avg_mib_s(broker_total):.1f} MiB/s ingress+egress avg." if broker_total else "Broker combined rate unavailable."),
        ]

    return f"""
<section>
  <h2>Scenario-Specific Result Interpretation</h2>
  <p class="section-note">{_escape(classification.get('label', 'Scenario interpretation'))}</p>
  <div class="table-wrap">{_table(['Signal', 'Interpretation'], rows)}</div>
</section>
"""


def _scenario_classification_section(classification: dict[str, Any]) -> str:
    if not classification:
        return ""
    rows = [
        ("Configured scenario", classification.get("configured_scenario", "unknown")),
        ("Active roles", ", ".join(_list(classification.get("active_roles"))) or "unknown"),
        ("Plain label", classification.get("label", "unknown")),
        ("Scenario type", classification.get("scenario_type", "unknown")),
        ("Evidence", classification.get("evidence", "not recorded")),
    ]
    warnings = _list(classification.get("warnings"))
    warning_html = "".join(f"<li>{_escape(item)}</li>" for item in warnings)
    if not warning_html:
        warning_html = "<li>Scenario config and active roles are consistent.</li>"
    return f"""
<section>
  <h2>Scenario Classification</h2>
  <p class="section-note">This identifies whether the measured workload is simultaneous, ingress-only, or egress-only. Role assignment and active execution are shown separately because inactive ranks may still be assigned to preserve node placement.</p>
  <div class="table-wrap compact">{_table(['Field', 'Value'], rows)}</div>
  <ul class="notes warnings">{warning_html}</ul>
</section>
"""


def _build_runtime_options() -> dict[str, Any]:
    return {
        "enable_ram_backed_runtime": _env_runtime_option("ENABLE_RAM_BACKED_RUNTIME", "1"),
        "enable_system_inventory": _env_runtime_option("ENABLE_SYSTEM_INVENTORY", "1"),
    }


def _env_runtime_option(name: str, default: str) -> dict[str, str]:
    raw = os.environ.get(name)
    if raw is None:
        raw = default
        source = "default"
    else:
        source = "environment"
    return {
        "name": name,
        "value": raw,
        "enabled": "yes" if str(raw).strip().lower() not in {"0", "false", "no", "off"} else "no",
        "source": source,
    }


def _runtime_option_text(runtime_options: dict[str, Any], key: str) -> str:
    option = _dict(runtime_options.get(key))
    if not option:
        return "unknown"
    return f"{option.get('enabled', 'unknown')} ({option.get('name', key)}={option.get('value', 'unknown')}, {option.get('source', 'unknown')})"


def _config_path_text(case: dict[str, Any]) -> str:
    notes = _dict(case.get("notes"))
    path = str(notes.get("config_path") or notes.get("config_abspath") or "")
    if not path:
        return "not recorded"
    abspath = str(notes.get("config_abspath") or "")
    if abspath and abspath != path:
        return f"{path} ({abspath})"
    return path


def _scenario_plain_label(scenario: str) -> str:
    return {
        "ingress_ramp": "ingress-only",
        "egress_only": "egress-only",
        "simultaneous": "simultaneous",
        "consume_and_process": "simultaneous consume-and-process",
    }.get(scenario, scenario or "unknown")


def _scenario_has_measured_producer(scenario: str) -> bool:
    return scenario in {"ingress_ramp", "simultaneous", "consume_and_process"}


def _scenario_has_measured_consumer(scenario: str) -> bool:
    return scenario in {"egress_only", "simultaneous", "consume_and_process"}


def _compact_json(value: Any) -> str:
    if isinstance(value, dict) and value:
        return json.dumps(value, sort_keys=True)
    if isinstance(value, list) and value:
        return json.dumps(value)
    return "not configured"


def _broker_settings_text(extra: dict[str, Any]) -> str:
    keys = [
        "kafka_heap_opts",
        "kafka_num_network_threads",
        "kafka_num_io_threads",
        "kafka_socket_send_buffer_bytes",
        "kafka_socket_receive_buffer_bytes",
        "kafka_queued_max_requests",
        "kafka_message_max_bytes",
        "kafka_log_segment_bytes",
    ]
    settings = {key: extra[key] for key in keys if key in extra}
    return _compact_json(settings)


def _prefill_text(egress_prefill: dict[str, Any]) -> str:
    if not egress_prefill:
        return "No prefill artifact was merged into this report."
    if not egress_prefill.get("enabled"):
        return f"{egress_prefill.get('status', 'unknown')}: {egress_prefill.get('reason', 'prefill artifact unavailable')}"
    return (
        f"{egress_prefill.get('status', 'unknown')}; "
        f"delivered {_int(egress_prefill.get('messages_delivered'))} setup messages; "
        f"consumed {_int(egress_prefill.get('messages_consumed'))}; "
        f"remaining {_int(egress_prefill.get('messages_remaining'))}."
    )


def _definitions_section() -> str:
    rows = [
        ("Avg over window", "Arithmetic mean of Prometheus samples in the report monitoring window; use this for sustained throughput comparisons."),
        ("Peak sample", "Highest sampled value in the same window; useful for burst capacity, not sustained throughput."),
        ("Producer delivered", "MPI/librdkafka client bytes that received delivery callbacks."),
        ("Producer send-attempt", "Bytes the producer ranks attempted to enqueue/send before delivery filtering and flush behavior."),
        ("Consumer received", "Bytes the MPI consumer ranks actually consumed."),
        ("Broker ingress/egress", "Kafka broker bytes-in/bytes-out from JMX exporter rate metrics, converted to MiB/s."),
        ("Broker combined throughput", "For simultaneous read/write tests, broker data-plane pressure is broker ingress plus broker egress. Directional ingress and egress remain the primary Kafka rates."),
        ("Node CPU/RAM/network", "Per-node host metrics from Node exporter; CPU is percent, RAM is GB, network is MB/s."),
        ("Capability bandwidth", "Pre-Kafka iperf3 link test in Gbit/s and MB/s; separate from observed Kafka traffic."),
        ("librdkafka client stats", "Producer/consumer client callback summaries for queue depth, tx/rx bytes, broker RTT, consumer-group state, retries/errors, and lag-related signals."),
    ]
    return f"""
<section>
  <h2>Definitions and Units</h2>
  <p class="section-note">Application and Kafka JMX payload rates use MiB/s (1 MiB = 1,048,576 bytes). Node-network and iperf summaries retain their source units where explicitly labeled. Average and peak answer different questions.</p>
  <div class="table-wrap">{_table(['Term', 'Meaning'], rows)}</div>
</section>
"""


def _exporter_explanation_section() -> str:
    rows = [
        ("MPI/app metrics", "End-to-end producer and consumer throughput observed by the benchmark ranks."),
        ("JMX exporter", "Per Kafka broker JVM only: bytes in/out, messages in, JVM heap, threads, and broker internals."),
        ("Node exporter", "Per-node CPU percent, RAM in GB, disk and network utilization during the benchmark window."),
        ("Kafka exporter", "Cluster/topic/partition/offset/consumer-lag sanity from one exporter on the monitoring node. It is not a per-node exporter and not the primary throughput source."),
    ]
    return f"""
<section>
  <h2>Metric Sources</h2>
  <p class="section-note">Throughput conclusions should combine the client view, the broker JMX view, and the node-level resource view.</p>
  <div class="table-wrap">{_table(['Source', 'Use in this report'], rows)}</div>
</section>
"""


def _exporter_source_map_section(monitoring: dict[str, Any], inventory: dict[str, Any]) -> str:
    rows = [
        (
            "Producer node",
            _nodes_for_roles(monitoring, inventory, {"producer", "producer_controller"}),
            "MPI producer metrics plus Node exporter CPU/RAM/network.",
            "No JMX exporter for Python/MPI producers; Kafka exporter does not measure producer-node resources.",
        ),
        (
            "Consumer node",
            _nodes_for_roles(monitoring, inventory, {"consumer"}),
            "MPI consumer metrics plus Node exporter CPU/RAM/network; Kafka exporter helps with offset/lag sanity when consumer groups are visible.",
            "No JMX exporter for Python/MPI consumers.",
        ),
        (
            "Broker node",
            _nodes_for_roles(monitoring, inventory, {"broker"}),
            "Kafka JMX exporter for broker/JVM metrics plus Node exporter for host resources.",
            "JMX exporter is attached to Kafka broker JVMs only.",
        ),
        (
            "Monitoring node",
            _nodes_for_roles(monitoring, inventory, {"monitoring"}),
            "Prometheus plus Kafka exporter plus Node exporter.",
            "Kafka exporter runs once here and queries Kafka as a cluster-level source.",
        ),
    ]
    return f"""
<section>
  <h2>Exporter Source Map</h2>
  <p class="section-note">The producer and consumer clients are Python/MPI processes, so their direct throughput comes from MPI/app metrics. JMX is broker-side for Kafka JVMs; Kafka exporter is cluster/topic/lag-side, not per producer or consumer node.</p>
  <div class="table-wrap wide">{_table(['Role', 'Node(s)', 'Metrics used', 'Scope note'], rows)}</div>
</section>
"""


def _report_completeness_section(monitoring: dict[str, Any], inventory: dict[str, Any]) -> str:
    if not monitoring or monitoring.get("status") == "not_collected":
        return """
<section>
  <h2>Report Completeness</h2>
  <p class="section-note">Prometheus target information was not collected.</p>
</section>
"""
    target_rows = _prometheus_target_rows(monitoring)
    completeness_rows = _exporter_completeness_rows(monitoring, inventory)
    missing = [row for row in completeness_rows if str(row[2]).lower() not in {"present", "collected"}]
    note_items = []
    if missing:
        note_items.extend(f"{row[0]}: {row[3]}" for row in missing)
    else:
        note_items.append("All expected exporter target groups were present in Prometheus.")
    if _kafka_exporter_lag_unavailable(monitoring):
        note_items.append("Kafka exporter up, consumer lag metric unavailable.")
    notes = "".join(f"<li>{_escape(item)}</li>" for item in note_items)
    return f"""
<section>
  <h2>Report Completeness</h2>
  <p class="section-note">This section checks whether the expected Prometheus exporter targets were present for this run.</p>
  <ul class="notes warnings">{notes}</ul>
  <div class="table-wrap">{_table(['Exporter group', 'Expected scope', 'Status', 'Observed targets'], completeness_rows)}</div>
  <h3>Active Prometheus Targets</h3>
  <div class="table-wrap wide">{_table(['Job', 'Endpoint', 'Health', 'Node', 'Role', 'Last error'], target_rows)}</div>
</section>
"""


def _run_verdict_section(
    case: dict[str, Any],
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    monitoring: dict[str, Any],
    inventory: dict[str, Any],
) -> str:
    verdict, warnings = _run_verdict(case, config, producers, consumers, broker_throughput, monitoring, inventory)
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_total = _broker_combined_metric(broker_throughput, broker_in, broker_out)
    rows = [
        ("Producer delivered", f"{_metric_mib_s(producers):.1f} MiB/s"),
        ("Producer send-attempt", f"{_send_attempt_mib_s(producers):.1f} MiB/s"),
        ("Consumer received", f"{_metric_mib_s(consumers):.1f} MiB/s"),
        ("Broker JMX ingress avg", f"{_broker_avg_mib_s(broker_in):.1f} MiB/s" if broker_in else "n/a"),
        ("Broker JMX ingress peak sample", f"{_broker_peak_mib_s(broker_in):.1f} MiB/s" if broker_in else "n/a"),
        ("Broker JMX egress avg", f"{_broker_avg_mib_s(broker_out):.1f} MiB/s" if broker_out else "n/a"),
        ("Broker JMX egress peak sample", f"{_broker_peak_mib_s(broker_out):.1f} MiB/s" if broker_out else "n/a"),
        ("Broker JMX combined avg", f"{_broker_avg_mib_s(broker_total):.1f} MiB/s" if broker_total else "n/a"),
        ("Producer failed sends", _failed_send_text(producers)),
        ("Producer flush time", f"{_float(producers.get('max_flush_duration_sec')):.1f} sec"),
        ("System inventory", inventory.get("status", "unknown") if inventory else "not_collected"),
    ]
    if _has_flush_diagnostics(producers):
        rows.insert(-1, ("Pending at flush start", _pending_flush_text(producers)))
        rows.insert(-1, ("Callbacks during flush", _callbacks_during_flush_text(producers)))
    warning_items = "".join(f"<li>{_escape(item)}</li>" for item in warnings) or "<li>No report warnings detected.</li>"
    return f"""
<section>
  <h2>Run Verdict</h2>
  <div class="verdict {verdict['class']}">
    <strong>{_escape(verdict['label'])}</strong>
    <span>{_escape(verdict['summary'])}</span>
  </div>
  <p class="section-note">Average values describe the sampled monitoring window. Peak sample values are the highest individual Prometheus samples and can be short bursts.</p>
  <div class="table-wrap compact">{_table(['Signal', 'Value'], rows)}</div>
  <ul class="notes warnings">{warning_items}</ul>
</section>
"""


def _bottleneck_analysis_section(analysis: dict[str, Any]) -> str:
    if not analysis:
        return ""
    network_rows = []
    for path in _list(analysis.get("network_path_comparisons")):
        path = _dict(path)
        network_rows.append(
            (
                path.get("path", "unknown"),
                path.get("nodes", "unknown"),
                _mb_s_text(path.get("capacity_mb_s")),
                _mb_s_text(path.get("kafka_avg_mb_s")),
                _percent_text(path.get("avg_utilization_percent")),
                _mb_s_text(path.get("kafka_peak_mb_s")),
                _percent_text(path.get("peak_utilization_percent")),
                path.get("conclusion", "unknown"),
            )
        )
    chain_rows = []
    for item in _list(analysis.get("throughput_chain")):
        item = _dict(item)
        chain_rows.append(
            (
                item.get("stage", "unknown"),
                item.get("source", "unknown"),
                _mb_s_text(item.get("mb_s")),
                item.get("meaning", ""),
            )
        )
    system_rows = []
    for item in _list(analysis.get("system_comparisons")):
        item = _dict(item)
        system_rows.append(
            (
                item.get("resource", "unknown"),
                item.get("node", "unknown"),
                item.get("measured_capability", "n/a"),
                _mb_s_text(item.get("observed_demand_mb_s")),
                _percent_text(item.get("utilization_percent")),
                item.get("conclusion", "unknown"),
            )
        )
    flag_rows = _bottleneck_flag_rows(analysis)
    conclusion_items = "".join(
        f"<li>{_escape(item)}</li>"
        for item in _list(analysis.get("conclusions"))
    ) or "<li>No bottleneck conclusion was computed.</li>"
    return f"""
<section>
  <h2>Bottleneck Analysis</h2>
  <p class="section-note">{_escape(analysis.get('primary_conclusion', 'No primary conclusion was computed.'))}</p>
  <h3>System Saturation Verdict</h3>
  <div class="table-wrap">{_table(['Check', 'Status', 'Evidence'], flag_rows)}</div>
  <h3>Network Capability vs Kafka Traffic</h3>
  <div class="table-wrap wide">{_table(['Path', 'Nodes', 'iperf capability', 'Kafka avg', 'Avg utilization', 'Kafka peak', 'Peak utilization', 'Conclusion'], network_rows)}</div>
  <h3>Kafka Throughput Chain</h3>
  <div class="table-wrap">{_table(['Stage', 'Source', 'Throughput', 'Meaning'], chain_rows)}</div>
  <h3>Broker System Headroom</h3>
  <div class="table-wrap">{_table(['Resource', 'Node', 'Measured capability', 'Observed demand', 'Utilization', 'Conclusion'], system_rows)}</div>
  <ul class="notes warnings">{conclusion_items}</ul>
</section>
"""


def _sustained_throughput_verdict_section(verdict: dict[str, Any]) -> str:
    if not verdict:
        return ""
    check_rows = []
    for check in _list(verdict.get("checks")):
        check = _dict(check)
        check_rows.append(
            (
                check.get("name", "unknown"),
                check.get("status", "unknown"),
                check.get("evidence", ""),
            )
        )
    scenario = str(verdict.get("scenario", "unknown"))
    summary_rows = [
        ("Verdict", verdict.get("label", "unknown")),
        ("Clean sustainable throughput", _mb_s_text(verdict.get("clean_sustainable_mb_s"))),
    ]
    if _scenario_has_measured_producer(scenario):
        summary_rows.extend(
            [
                ("Producer delivery efficiency", _percent_text(verdict.get("producer_delivery_efficiency_percent"))),
                ("Producer pending at flush", _percent_text(verdict.get("producer_pending_at_flush_percent"))),
                ("Producer failed sends", _percent_text(verdict.get("producer_failed_send_percent"))),
                ("Producer flush time", f"{_float(verdict.get('producer_flush_sec')):.1f} sec"),
            ]
        )
    if _scenario_has_measured_consumer(scenario):
        summary_rows.append(
            ("Consumer received", _mb_s_text(verdict.get("consumer_received_mb_s")))
        )
    return f"""
<section>
  <h2>Sustained Throughput Verdict</h2>
  <div class="verdict {verdict.get('class', 'warn')}">
    <strong>{_escape(verdict.get('label', 'Unknown'))}</strong>
    <span>{_escape(verdict.get('summary', 'No sustained-throughput verdict was computed.'))}</span>
  </div>
  <p class="section-note">This separates a high offered/broker rate from a clean sustained benchmark result. A run is considered overdriven when backlog, failed sends, or flush time show that the client pipeline could not drain cleanly.</p>
  <div class="table-wrap compact">{_table(['Signal', 'Value'], summary_rows)}</div>
  <h3>Verdict Checks</h3>
  <div class="table-wrap">{_table(['Check', 'Status', 'Evidence'], check_rows)}</div>
</section>
"""


def _producer_flush_diagnostics_section(producers: dict[str, Any]) -> str:
    if not producers or not _has_flush_diagnostics(producers):
        return ""
    rows = [
        ("Messages attempted", _int(producers.get("messages_attempted"))),
        ("Messages enqueued to librdkafka", _int(producers.get("messages_enqueued"))),
        ("Messages delivered before flush", _int(producers.get("messages_delivered_at_flush_start"))),
        ("Messages delivered during flush", _int(producers.get("messages_delivered_during_flush"))),
        ("Messages failed before flush", _int(producers.get("messages_failed_at_flush_start"))),
        ("Messages failed during flush", _int(producers.get("messages_failed_during_flush"))),
        ("Pending messages at flush start", _int(producers.get("pending_messages_at_flush_start"))),
        ("Approx pending bytes at flush start", _bytes_text(producers.get("pending_bytes_at_flush_start"))),
        ("Producer queue length at flush start", _int(producers.get("producer_queue_len_at_flush_start"))),
        ("Producer queue length after flush", _int(producers.get("producer_queue_len_after_flush"))),
        ("Flush remaining messages", _int(producers.get("flush_remaining_messages"))),
        ("Delivery callbacks during flush", _int(producers.get("delivery_callbacks_during_flush"))),
        ("Callback rate during flush", f"{_float(producers.get('delivery_callback_rate_during_flush_per_sec')):.1f} callbacks/s"),
        ("Synchronous produce errors", _format_counts(producers.get("produce_error_counts"))),
        ("Async delivery errors", _format_counts(producers.get("delivery_error_counts"))),
    ]
    return f"""
<section>
  <h2>Producer Flush Diagnostics</h2>
  <p class="section-note">These counters explain the gap between active send-attempt throughput and final delivered throughput. A large pending backlog or many callbacks during flush means the producer ended the send window before Kafka had confirmed all queued records.</p>
  <div class="table-wrap compact">{_table(['Signal', 'Value'], rows)}</div>
</section>
"""


def _client_runtime_diagnostics_section(
    producers: dict[str, Any],
    consumers: dict[str, Any],
) -> str:
    producer_stats = _dict(producers.get("librdkafka_stats"))
    consumer_stats = _dict(consumers.get("librdkafka_stats"))
    if not producer_stats and not consumer_stats:
        return ""
    rows = [
        _client_stats_row("Producers", producer_stats),
        _client_stats_row("Consumers", consumer_stats),
    ]
    return f"""
<section>
  <h2>Client Runtime Diagnostics</h2>
  <p class="section-note">These are aggregated librdkafka statistics callback summaries from the Python/MPI clients. They explain client-side queue pressure, broker round-trip timing, byte counters, consumer group churn, and whether client stats were actually sampled.</p>
  <div class="table-wrap wide">{_table(['Role', 'Stats ranks', 'Samples', 'TX MB', 'RX MB', 'TX errors', 'RX errors', 'Max local queue', 'Max broker requests waiting for response', 'Max broker RTT p95', 'Max partition queue/fetch', 'Consumer lag max', 'Rebalances', 'States'], rows)}</div>
  <p class="section-note">Max broker requests waiting for response is the largest per-client-rank count of Kafka requests already sent to a broker but not answered yet. It is not a message count and not a total across all ranks.</p>
</section>
"""


def _timeline_section(benchmark_result: dict[str, Any]) -> str:
    events = _events_from_result(benchmark_result)
    if not events:
        return ""
    rows = [
        (
            event.get("label", event.get("name", "event")),
            _format_event_time(event),
            event.get("source", "unknown"),
        )
        for event in events
    ]
    return f"""
<section>
  <h2>Timeline</h2>
  <p class="section-note">These event timestamps are drawn as dashed vertical markers on the monitoring graphs, including CPU, RAM, and network plots.</p>
  <div class="table-wrap compact">{_table(['Event', 'UTC time', 'Source'], rows)}</div>
</section>
"""


def _node_role_section(monitoring: dict[str, Any], inventory: dict[str, Any]) -> str:
    nodes = _node_role_rows(monitoring, inventory)
    if not nodes:
        return ""
    return f"""
<section>
  <h2>Node Role Map</h2>
  <p class="section-note">Hostnames, service addresses, MPI ranks, and exporters are shown together so every graph can be tied back to a physical node role.</p>
  <div class="table-wrap">{_table(['Node', 'Address', 'Role', 'MPI ranks', 'Exporters'], nodes)}</div>
</section>
"""


def _system_inventory_section(inventory: dict[str, Any]) -> str:
    if not inventory or inventory.get("status") == "not_collected":
        return """
<section>
  <h2>System Capability</h2>
  <p class="section-note">System capability inventory was not collected for this run.</p>
</section>
"""
    probe_summary = _dict(inventory.get("probe_summary"))
    summary_notes = _list(probe_summary.get("warnings"))
    node_rows = []
    for node in _list(inventory.get("nodes")):
        cpu = _dict(node.get("cpu"))
        memory = _dict(node.get("memory"))
        bandwidth = _dict(memory.get("bandwidth_probe"))
        metrics = _dict(bandwidth.get("metrics"))
        network = _dict(node.get("network"))
        numa = _dict(node.get("numa"))
        node_rows.append(
            (
                node.get("node", "unknown"),
                node.get("role", "unknown"),
                cpu.get("model_name", "unknown"),
                cpu.get("architecture", "unknown"),
                _cpu_shape(cpu),
                f"{_float(memory.get('total_gb')):.1f}",
                _ram_bandwidth(metrics, bandwidth),
                network.get("interface", "unknown"),
                _gbit(network.get("link_speed_gbit"), include_unit=True),
                numa.get("node_count", "unknown"),
            )
        )
    iperf_rows = []
    for test in _list(inventory.get("network_tests")):
        iperf_rows.append(
            (
                test.get("label", "unknown"),
                f"{test.get('source_node', 'unknown')} -> {test.get('target_node', 'unknown')}",
                test.get("status", "unknown"),
                _gbit(test.get("gbit_per_second"), include_unit=True),
                f"{_float(test.get('megabytes_per_second')):.1f}" if test.get("megabytes_per_second") is not None else "n/a",
                test.get("parallel_streams", test.get("parallel_streams_requested", "unknown")),
                _iperf_note(test),
            )
        )
    iperf_table = _table(
        ["Path", "Nodes", "Status", "Capability Gbit/s", "Capability MB/s", "Streams", "Summary note"],
        iperf_rows,
    ) if iperf_rows else "<p class=\"section-note\">No iperf3 path tests were recorded.</p>"
    warnings_html = "".join(f"<li>{_escape(note)}</li>" for note in summary_notes)
    if not warnings_html and inventory.get("status") == "completed":
        warnings_html = "<li>All required system capability probes completed.</li>"
    elif not warnings_html:
        warnings_html = "<li>No capability probe warnings were recorded.</li>"
    return f"""
<section>
  <h2>System Capability</h2>
  <p class="section-note">Capability probes run before Kafka starts, on the same Slurm allocation, so they do not distort the Kafka workload. Single-process RAM bandwidth GB/s is reported with directional read/write probe kernels and mixed STREAM-style copy/triad kernels; these are not full-node aggregate memory bandwidth.</p>
  <ul class="notes warnings">{warnings_html}</ul>
  <div class="table-wrap wide">{_table(['Node', 'Role', 'CPU model', 'Arch', 'CPU shape', 'RAM GB', 'Single-process RAM read/write GB/s', 'NIC', 'Link Gbit/s', 'NUMA nodes'], node_rows)}</div>
  <h3>Network Capability</h3>
  <div class="table-wrap">{iperf_table}</div>
</section>
"""


def _monitoring_section(monitoring: dict[str, Any], inventory: dict[str, Any]) -> str:
    if not monitoring or monitoring.get("status") == "not_collected":
        return """
<section>
  <h2>Runtime Monitoring</h2>
  <p class="section-note">Prometheus monitoring was not collected for this run.</p>
</section>
"""
    metrics = [_dict(metric) for metric in _list(monitoring.get("collected_metrics"))]
    system_rows = []
    jmx_rate_rows = []
    jmx_counter_rows = []
    kafka_rows = []
    for metric in metrics:
        category = str(metric.get("category", ""))
        if category == "system":
            for series in _list(metric.get("series")):
                labels = _dict(series.get("labels"))
                system_rows.append(
                    (
                        labels.get("node", labels.get("instance", "unknown")),
                        labels.get("role", "unknown"),
                        metric.get("title", metric.get("id", "metric")),
                        metric.get("unit", ""),
                        f"{_float(series.get('avg_value')):.3f}",
                        f"{_float(series.get('max_value')):.3f}",
                    )
                )
        elif category == "kafka_jmx":
            if _is_rate_metric(metric):
                jmx_rate_rows.append(_jmx_rate_metric_row(metric))
            else:
                jmx_counter_rows.append(_metric_row(metric))
        elif category == "kafka_exporter":
            kafka_rows.append(_metric_row(metric))

    network_rows = _observed_network_rows(monitoring, inventory)
    network_section = ""
    if network_rows:
        network_section = f"""
  <h3>Observed Network vs Link</h3>
  <div class="table-wrap wide">{_table(['Node', 'Role', 'Link capability', 'Avg RX over window MB/s', 'Peak RX sample MB/s', 'Avg TX over window MB/s', 'Peak TX sample MB/s'], network_rows)}</div>
"""
    return f"""
<section>
  <h2>Runtime Monitoring</h2>
  <p class="section-note">Window: {_escape(_dict(monitoring.get('time_window')).get('start', 'unknown'))} to {_escape(_dict(monitoring.get('time_window')).get('end', 'unknown'))}. RAM graphs use GB; network graphs use MB/s; CPU graphs use percent.</p>
  {network_section}
  <h3>Node Exporter</h3>
  <div class="table-wrap wide">{_table(['Node', 'Role', 'Metric', 'Unit', 'Avg over window', 'Peak sample'], system_rows)}</div>
  <h3>JMX Exporter Rate Metrics</h3>
  <div class="table-wrap wide">{_table(['Metric', 'Unit', 'Series', 'Avg over window', 'Peak sample', 'Avg MiB/s', 'Peak MiB/s'], jmx_rate_rows)}</div>
  <h3>JMX Exporter Counters and State</h3>
  <div class="table-wrap">{_table(['Metric', 'Unit', 'Series', 'Avg over window', 'Peak sample'], jmx_counter_rows)}</div>
  <h3>Kafka Exporter</h3>
  <div class="table-wrap">{_table(['Metric', 'Unit', 'Series', 'Avg over window', 'Peak sample'], kafka_rows)}</div>
</section>
"""


def _system_saturation_section(monitoring: dict[str, Any], inventory: dict[str, Any]) -> str:
    if not monitoring or monitoring.get("status") == "not_collected":
        return ""
    rows = _system_saturation_rows(monitoring, inventory)
    if not rows:
        return ""
    time_window = _dict(monitoring.get("time_window"))
    return f"""
<section>
  <h2>System Saturation During Kafka</h2>
  <p class="section-note">Window: {_escape(time_window.get('start', 'unknown'))} to {_escape(time_window.get('end', 'unknown'))}. These are Node exporter values over the Kafka benchmark monitoring window: CPU is percent, RAM is GB, and network is MB/s.</p>
  <div class="table-wrap wide">{_table(['Node', 'Role', 'CPU avg %', 'CPU peak %', 'RAM avg GB', 'RAM peak GB', 'RAM total GB', 'RAM peak %', 'Avg RX MB/s', 'Peak RX MB/s', 'Avg TX MB/s', 'Peak TX MB/s'], rows)}</div>
</section>
"""


def _graph_gallery_section(output_dir: Path, benchmark_result: dict[str, Any]) -> str:
    graphs = _graph_items(output_dir, benchmark_result)
    if not graphs:
        return ""
    cards = []
    for graph in graphs:
        embedded = graph.get("embedded", "")
        if not embedded:
            continue
        cards.append(
            f"<article class=\"graph-card graph-source-{_escape(graph.get('source', 'report'))}\">"
            f"<h3>{_escape(graph.get('title', 'Graph'))}</h3>"
            f"<p>{_escape(graph.get('caption', ''))}</p>"
            f"<div class=\"graph-box\">{embedded}</div>"
            "</article>"
        )
    if not cards:
        return ""
    return f"""
<section>
  <h2>Graphs</h2>
  <p class="section-note">Graphs are embedded directly in this HTML file so the report remains readable when opened by itself. Dashed vertical markers show exact Kafka, MPI, producer, consumer, and load-pressure event times when available.</p>
  <div class="graph-grid">{''.join(cards)}</div>
</section>
"""


def _interpretation_section(
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    monitoring: dict[str, Any],
    inventory: dict[str, Any],
) -> str:
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_total = _broker_combined_metric(broker_throughput, broker_in, broker_out)
    iperf = _best_iperf_gbit(inventory)
    cpu_max = _max_metric(monitoring, "node_cpu_busy_percent")
    ram_max = _max_metric(monitoring, "node_memory_used_gb")
    producer_mb = _metric_mib_s(producers)
    consumer_mb = _metric_mib_s(consumers)
    broker_mb = _broker_avg_mib_s(broker_in) if broker_in else 0.0
    broker_combined_mb = _broker_avg_mib_s(broker_total) if broker_total else 0.0
    ram_comparison = _broker_ram_comparison(inventory, broker_combined_mb)
    notes = [
        f"Producer delivered throughput was {producer_mb:.1f} MiB/s and consumer throughput was {consumer_mb:.1f} MiB/s.",
        f"Kafka broker bytes-in from JMX averaged {broker_mb:.1f} MiB/s." if broker_in else "Kafka broker JMX throughput was not available.",
        f"Simultaneous broker combined data-plane rate averaged {broker_combined_mb:.1f} MiB/s (ingress plus egress)." if broker_total else "Broker combined ingress+egress rate was not available.",
        _broker_delivery_note(broker_mb, producer_mb, consumer_mb),
        f"The fastest measured iperf3 path was {iperf:.2f} Gbit/s." if iperf else "iperf3 path capability was not available.",
        _ram_bottleneck_note(ram_comparison),
        f"Peak observed node CPU was {cpu_max:.1f}% and peak observed RAM used was {ram_max:.1f} GB." if cpu_max or ram_max else "Node exporter CPU/RAM peaks were not available.",
        _ram_runtime_note(inventory),
    ]
    return f"""
<section>
  <h2>Interpretation</h2>
  <ul class="notes">{''.join(f'<li>{_escape(note)}</li>' for note in notes)}</ul>
</section>
"""


def _broker_delivery_note(broker_mb: float, producer_mb: float, consumer_mb: float) -> str:
    if broker_mb <= 0:
        return "Broker JMX ingress should be inspected once broker throughput data is available."
    if producer_mb > 0 and broker_mb > producer_mb * 1.1:
        return (
            f"Broker JMX ingress was higher than producer delivered throughput "
            f"({broker_mb:.1f} MiB/s vs {producer_mb:.1f} MiB/s), so inspect "
            "client delivery callbacks, retries, flush backlog, and request latency."
        )
    if consumer_mb > 0 and broker_mb > consumer_mb * 1.1:
        return (
            f"Broker JMX ingress was higher than consumer received throughput "
            f"({broker_mb:.1f} MiB/s vs {consumer_mb:.1f} MiB/s), so inspect "
            "consumer fetch, poll, and drain behavior."
        )
    return "Broker JMX ingress is close to the observed app throughput; inspect resource saturation next."


def _graph_items(output_dir: Path, benchmark_result: dict[str, Any]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for graph in _list(_dict(benchmark_result.get("report_artifacts")).get("graphs")):
        graph = _dict(graph)
        path = str(graph.get("path", ""))
        items.append(
            {
                "title": str(graph.get("title", graph.get("id", "Benchmark graph"))),
                "caption": str(graph.get("description", "MPI benchmark graph")),
                "source": "mpi",
                "embedded": _embed_graph(output_dir, path),
            }
        )

    monitoring = _dict(benchmark_result.get("monitoring"))
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        graph = _dict(metric.get("graph"))
        if graph.get("status") != "generated":
            continue
        title = str(metric.get("title", metric.get("id", "Monitoring graph")))
        unit = str(metric.get("unit", ""))
        caption = _graph_caption(metric)
        items.append(
            {
                "title": title,
                "caption": caption,
                "source": str(metric.get("category", "monitoring")),
                "embedded": _embed_graph(output_dir, str(graph.get("path", ""))),
            }
        )
    return items


def _embed_graph(output_dir: Path, relative_path: str) -> str:
    if not relative_path:
        return ""
    graph_path = (output_dir / relative_path).resolve()
    try:
        graph_path.relative_to(output_dir.resolve())
    except ValueError:
        return ""
    if not graph_path.is_file():
        return ""
    if graph_path.suffix.lower() == ".svg":
        return graph_path.read_text(encoding="utf-8")
    if graph_path.suffix.lower() == ".png":
        payload = base64.b64encode(graph_path.read_bytes()).decode("ascii")
        return f'<img src="data:image/png;base64,{payload}" alt="embedded graph">'
    return ""


def _node_role_rows(monitoring: dict[str, Any], inventory: dict[str, Any]) -> list[tuple[Any, ...]]:
    node_roles = _dict(monitoring.get("node_roles"))
    rows = []
    for node in _list(node_roles.get("nodes")):
        node = _dict(node)
        ranks = _format_mpi_ranks(_list(node.get("mpi_ranks")))
        exporters = ", ".join(str(item.get("name")) for item in _list(node.get("exporters")) if isinstance(item, dict))
        rows.append(
            (
                node.get("node", "unknown"),
                node.get("service_address", "unknown"),
                node.get("primary_role", "unknown"),
                ranks or "none",
                exporters or "none",
            )
        )
    if rows:
        return rows
    for node in _list(inventory.get("nodes")):
        node = _dict(node)
        rows.append((node.get("node", "unknown"), node.get("service_address", "unknown"), node.get("role", "unknown"), "unknown", "unknown"))
    return rows


def _nodes_for_roles(monitoring: dict[str, Any], inventory: dict[str, Any], roles: set[str]) -> str:
    names = []
    node_roles = _dict(monitoring.get("node_roles"))
    for node in _list(node_roles.get("nodes")):
        node = _dict(node)
        primary_role = str(node.get("primary_role", ""))
        all_roles = {primary_role}
        for rank in _list(node.get("mpi_ranks")):
            rank = _dict(rank)
            role = str(rank.get("role", ""))
            if role:
                all_roles.add(role)
        if all_roles & roles:
            name = str(node.get("node", ""))
            if name:
                names.append(name)
    if not names:
        for node in _list(inventory.get("nodes")):
            node = _dict(node)
            if str(node.get("role", "")) in roles and node.get("node"):
                names.append(str(node.get("node")))
    return ", ".join(dict.fromkeys(names)) or "not observed"


def _prometheus_target_rows(monitoring: dict[str, Any]) -> list[tuple[Any, ...]]:
    rows = []
    for target in _list(_dict(monitoring.get("targets")).get("active_targets")):
        target = _dict(target)
        labels = _dict(target.get("labels"))
        rows.append(
            (
                labels.get("job", target.get("job", "unknown")),
                target.get("scrape_url", target.get("endpoint", "unknown")),
                target.get("health", "unknown"),
                labels.get("node", "unknown"),
                labels.get("role", "unknown"),
                target.get("last_error", ""),
            )
        )
    return rows


def _exporter_completeness_rows(monitoring: dict[str, Any], inventory: dict[str, Any]) -> list[tuple[Any, ...]]:
    targets = _prometheus_target_rows(monitoring)
    by_job: dict[str, list[tuple[Any, ...]]] = {}
    for row in targets:
        by_job.setdefault(str(row[0]), []).append(row)

    node_count = len(_list(inventory.get("nodes")))
    broker_count = sum(1 for node in _list(inventory.get("nodes")) if _dict(node).get("role") == "broker")
    expected = [
        ("node_exporter", f"all allocated nodes ({node_count})", "system"),
        ("kafka_jmx", f"Kafka broker JVMs ({broker_count})", "kafka_jmx"),
        ("kafka_exporter", "one monitoring-node exporter for cluster/topic/lag sanity", "kafka_exporter"),
    ]
    rows = []
    for job, scope, category in expected:
        observed = by_job.get(job, [])
        healthy = [row for row in observed if str(row[2]).lower() == "up"]
        has_metrics = _has_metric_category(monitoring, category)
        status = "present" if healthy else "missing"
        if job == "node_exporter" and node_count and len(healthy) < node_count:
            status = "partial" if healthy else "missing"
        if job == "kafka_jmx" and broker_count and len(healthy) < broker_count:
            status = "partial" if healthy else "missing"
        if status in {"missing", "partial"} and has_metrics:
            status = "collected"
        rows.append(
            (
                job,
                scope,
                status,
                _target_observation_text(observed, has_metrics),
            )
        )
    return rows


def _has_metric_category(monitoring: dict[str, Any], category: str) -> bool:
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        if metric.get("category") == category and _int(metric.get("numeric_sample_count")) > 0:
            return True
    return False


def _target_observation_text(targets: list[tuple[Any, ...]], has_metrics: bool) -> str:
    if targets:
        text = ", ".join(
            f"{row[1]} ({row[2]})"
            for row in targets
        )
        if has_metrics and not any(str(row[2]).lower() == "up" for row in targets):
            text += "; metrics collected in report"
        return text
    if has_metrics:
        return "metrics collected in report; target snapshot unavailable"
    return "none"


def _kafka_exporter_lag_unavailable(monitoring: dict[str, Any]) -> bool:
    if not _kafka_exporter_is_present(monitoring):
        return False
    if _metric_by_id(monitoring, "kafka_exporter_consumer_lag"):
        return False
    for metric in _list(monitoring.get("missing_metrics")):
        metric = _dict(metric)
        if metric.get("id") == "kafka_exporter_consumer_lag":
            return True
    return False


def _kafka_exporter_is_present(monitoring: dict[str, Any]) -> bool:
    for target in _list(_dict(monitoring.get("targets")).get("active_targets")):
        labels = _dict(_dict(target).get("labels"))
        if labels.get("job") == "kafka_exporter" and str(_dict(target).get("health", "")).lower() == "up":
            return True
    return _has_metric_category(monitoring, "kafka_exporter")


def _metric_row(metric: dict[str, Any]) -> tuple[Any, ...]:
    return (
        metric.get("title", metric.get("id", "metric")),
        metric.get("unit", ""),
        metric.get("series_count", len(_list(metric.get("series")))),
        f"{_float(metric.get('avg_value')):.3f}",
        f"{_float(metric.get('max_value')):.3f}",
    )


def _jmx_rate_metric_row(metric: dict[str, Any]) -> tuple[Any, ...]:
    avg = _float(metric.get("avg_value"))
    peak = _float(metric.get("max_value"))
    unit = str(metric.get("unit", ""))
    avg_mb = avg / 1_048_576.0 if unit == "bytes/sec" else None
    peak_mb = peak / 1_048_576.0 if unit == "bytes/sec" else None
    return (
        metric.get("title", metric.get("id", "metric")),
        unit,
        metric.get("series_count", len(_list(metric.get("series")))),
        f"{avg:.3f}",
        f"{peak:.3f}",
        f"{avg_mb:.3f}" if avg_mb is not None else "n/a",
        f"{peak_mb:.3f}" if peak_mb is not None else "n/a",
    )


def _is_rate_metric(metric: dict[str, Any]) -> bool:
    metric_id = str(metric.get("id", ""))
    unit = str(metric.get("unit", ""))
    return "rate" in metric_id or unit.endswith("/sec")


def _build_scenario_classification(
    case: dict[str, Any],
    config: dict[str, Any],
    scenario_execution: dict[str, Any],
) -> dict[str, Any]:
    configured = str(config.get("scenario", "unknown"))
    active_roles = sorted(str(role) for role in _list(scenario_execution.get("active_roles")) if role)
    active_role_set = set(active_roles)
    expected_by_scenario = {
        "simultaneous": {"producer", "consumer"},
        "consume_and_process": {"producer", "consumer"},
        "ingress_ramp": {"producer"},
        "egress_only": {"consumer"},
    }
    label_by_scenario = {
        "simultaneous": "Simultaneous producer + consumer",
        "consume_and_process": "Simultaneous producer + consumer processing",
        "ingress_ramp": "Ingress-only producer",
        "egress_only": "Egress-only consumer",
    }
    type_by_scenario = {
        "simultaneous": "simultaneous",
        "consume_and_process": "simultaneous",
        "ingress_ramp": "ingress_only",
        "egress_only": "egress_only",
    }
    expected = expected_by_scenario.get(configured, set())
    warnings: list[str] = []
    if expected and active_role_set and active_role_set != expected:
        warnings.append(
            "Configured scenario and recorded active roles differ: "
            f"expected {', '.join(sorted(expected))}, got {', '.join(active_roles)}."
        )
    if not active_roles:
        warnings.append("Active roles were not recorded in scenario_execution.")

    case_name = str(case.get("case_name", "")).lower()
    name_hints = {
        "simultaneous": "simultaneous",
        "ingress": "ingress_ramp",
        "egress": "egress_only",
    }
    hinted = [scenario for token, scenario in name_hints.items() if token in case_name]
    if hinted and configured not in hinted:
        warnings.append(
            "Case name appears to describe a different scenario than the config: "
            f"name={case.get('case_name', 'unknown')}, scenario={configured}."
        )

    return {
        "configured_scenario": configured,
        "active_roles": active_roles,
        "expected_active_roles": sorted(expected),
        "label": label_by_scenario.get(configured, "Unknown scenario"),
        "scenario_type": type_by_scenario.get(configured, "unknown"),
        "is_consistent": not warnings,
        "warnings": warnings,
        "evidence": (
            f"config.scenario={configured}; "
            f"scenario_execution.active_roles={', '.join(active_roles) or 'not recorded'}"
        ),
    }


def _build_bottleneck_analysis(
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    monitoring: dict[str, Any],
    inventory: dict[str, Any],
) -> dict[str, Any]:
    scenario = str(config.get("scenario", "unknown"))
    producer_measured = _scenario_has_measured_producer(scenario)
    consumer_measured = _scenario_has_measured_consumer(scenario)
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_total = _broker_combined_metric(broker_throughput, broker_in, broker_out)
    broker_in_avg = _broker_avg_mib_s(broker_in) if broker_in else 0.0
    broker_in_peak = _broker_peak_mib_s(broker_in) if broker_in else 0.0
    broker_out_avg = _broker_avg_mib_s(broker_out) if broker_out else 0.0
    broker_out_peak = _broker_peak_mib_s(broker_out) if broker_out else 0.0
    broker_total_avg = _broker_avg_mib_s(broker_total) if broker_total else broker_in_avg + broker_out_avg
    broker_total_peak = _broker_peak_mib_s(broker_total) if broker_total else broker_in_peak + broker_out_peak
    producer_send_attempt = _send_attempt_mib_s(producers)
    producer_delivered = _metric_mib_s(producers)
    consumer_received = _metric_mib_s(consumers)
    iperf_by_label = _completed_iperf_by_label(inventory)

    path_comparisons = [
        _network_path_comparison(
            "producer_to_broker",
            iperf_by_label.get("producer_to_broker"),
            broker_in_avg,
            broker_in_peak,
        ),
        _network_path_comparison(
            "broker_to_consumer",
            iperf_by_label.get("broker_to_consumer"),
            broker_out_avg,
            broker_out_peak,
        ),
    ]
    path_comparisons = [item for item in path_comparisons if item]

    throughput_chain = []
    if producer_measured:
        throughput_chain.extend([
            {
            "stage": "Producer send-attempt",
            "source": "MPI/app",
            "mb_s": producer_send_attempt,
            "meaning": "Bytes producer ranks tried to enqueue during the active send window.",
            },
            {
            "stage": "Producer delivered",
            "source": "MPI/app",
            "mb_s": producer_delivered,
            "meaning": "Bytes confirmed by librdkafka delivery callbacks.",
            },
        ])
    if scenario in {"ingress_ramp", "simultaneous", "consume_and_process"}:
        throughput_chain.append(
        {
            "stage": "Broker ingress avg",
            "source": "JMX exporter",
            "mb_s": broker_in_avg,
            "meaning": "Kafka broker bytes-in average over the monitoring window.",
        })
    if scenario in {"egress_only", "simultaneous", "consume_and_process"}:
        throughput_chain.append(
        {
            "stage": "Broker egress avg",
            "source": "JMX exporter",
            "mb_s": broker_out_avg,
            "meaning": "Kafka broker bytes-out average over the monitoring window.",
        })
    if scenario in {"simultaneous", "consume_and_process"}:
        throughput_chain.append(
        {
            "stage": "Broker combined ingress+egress avg",
            "source": "JMX exporter",
            "mb_s": broker_total_avg,
            "meaning": "Simultaneous broker data-plane pressure: bytes-in plus bytes-out.",
        })
    if consumer_measured:
        throughput_chain.append(
            {
            "stage": "Consumer received",
            "source": "MPI/app",
            "mb_s": consumer_received,
            "meaning": "Bytes the MPI consumer ranks actually consumed.",
            }
        )

    conclusions: list[str] = []
    system_comparisons = []
    ram_comparison = _broker_ram_comparison(inventory, broker_total_avg)
    if ram_comparison:
        system_comparisons.append(ram_comparison)
        ram_util = _float(ram_comparison.get("utilization_percent"))
        if ram_util and ram_util < 50.0:
            conclusions.append(
                f"Broker combined Kafka traffic used about {ram_util:.1f}% of measured single-process broker RAM copy aggregate bandwidth, so RAM bandwidth is unlikely to be the current ceiling."
            )
        elif ram_util:
            conclusions.append(
                f"Broker combined Kafka traffic used about {ram_util:.1f}% of measured single-process broker RAM copy aggregate bandwidth; inspect RAM/NUMA pressure."
            )
    network_not_saturated = False
    for item in path_comparisons:
        avg_util = _float(item.get("avg_utilization_percent"))
        if avg_util and avg_util < 80.0:
            network_not_saturated = True
            conclusions.append(
                f"{item['path']} used {avg_util:.1f}% of measured iperf capacity on average, so raw path bandwidth was not saturated."
            )

    operations = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(
            str(config.get("qualification_policy_id", ""))
        ),
    )
    pending_ratio = float(operations["producer_backlog_percent"])
    failed_ratio = float(operations["failed_send_percent"])
    flush_sec = float(operations["max_flush_duration_sec"])
    backlog_denominator = str(operations["backlog_denominator"])
    producer_stats = _dict(producers.get("librdkafka_stats"))
    max_client_queue = _float(producer_stats.get("max_msg_cnt"))
    max_waitresp = _float(producer_stats.get("max_broker_waitresp_cnt"))
    max_cpu_percent = _max_metric(monitoring, "node_cpu_busy_percent")
    max_ram_percent = _max_ram_used_percent(monitoring)
    max_network_util = max(
        (_float(item.get("avg_utilization_percent")) for item in path_comparisons),
        default=0.0,
    )
    max_ram_bandwidth_util = max(
        (_float(item.get("utilization_percent")) for item in system_comparisons),
        default=0.0,
    )
    consumer_drain_gap = bool(
        consumer_measured
        and broker_out_avg > 0
        and consumer_received > 0
        and consumer_received < broker_out_avg * 0.85
    )
    bottleneck_flags = {
        "cpu_saturated": max_cpu_percent >= 85.0,
        "ram_capacity_saturated": max_ram_percent >= 85.0,
        "ram_bandwidth_saturated": max_ram_bandwidth_util >= 80.0,
        "network_saturated": max_network_util >= 80.0,
        "producer_backlog": producer_measured and pending_ratio > 5.0,
        "producer_failed_sends": producer_measured and failed_ratio > 0.1,
        "producer_flush_high": producer_measured and flush_sec > 10.0,
        "consumer_drain_gap": consumer_drain_gap,
    }
    if producer_measured and pending_ratio > 5.0:
        conclusions.append(
            f"Producer backlog present: {pending_ratio:.1f}% of {backlog_denominator} were still pending at flush start."
        )
    if producer_measured and failed_ratio > 0.1:
        conclusions.append(
            f"Producer send failures above threshold: {failed_ratio:.3f}% of attempted sends failed."
        )
    if producer_measured and flush_sec > 10.0:
        conclusions.append(f"Producer flush time was high at {flush_sec:.1f}s.")
    if producer_measured and max_client_queue > 0:
        conclusions.append(
            f"librdkafka producer stats saw local queue depth up to {max_client_queue:.0f} messages."
        )
    if producer_measured and max_waitresp > 0:
        conclusions.append(
            f"librdkafka producer stats saw up to {max_waitresp:.0f} broker requests waiting for response on one client rank."
        )
    if consumer_drain_gap:
        conclusions.append(
            "Consumer received throughput is lower than broker egress; inspect consumer fetch/drain behavior."
        )
    if producer_measured and broker_in_avg > 0 and producer_delivered > 0 and producer_delivered < broker_in_avg * 0.9:
        conclusions.append(
            "Delivered producer throughput is lower than broker ingress; inspect producer callbacks, retries, and flush backlog."
        )
    if _kafka_exporter_lag_unavailable(monitoring):
        conclusions.append("Kafka exporter is up, but consumer lag metric is unavailable for this run.")

    producer_pressure = producer_measured and (pending_ratio > 5.0 or failed_ratio > 0.1)
    if network_not_saturated and (producer_pressure or consumer_drain_gap):
        primary = "Kafka/client pipeline is the likely bottleneck; measured network capability is much higher than observed Kafka traffic."
    elif network_not_saturated:
        primary = "Observed Kafka traffic is below raw network capability; inspect Kafka/client limits before network hardware."
    elif path_comparisons:
        primary = "Kafka traffic is close to measured path capability; network or fabric-level limits may matter next."
    else:
        primary = "Network capability data is unavailable; bottleneck diagnosis is based on client and broker metrics only."

    return {
        "primary_conclusion": primary,
        "throughput_unit": "MiB/s",
        "network_path_comparisons": path_comparisons,
        "throughput_chain": throughput_chain,
        "system_comparisons": system_comparisons,
        "producer_delivered_mb_s": producer_delivered,
        "consumer_received_mb_s": consumer_received,
        "broker_ingress_avg_mb_s": broker_in_avg,
        "broker_egress_avg_mb_s": broker_out_avg,
        "broker_combined_avg_mb_s": broker_total_avg,
        "broker_combined_peak_mb_s": broker_total_peak,
        "broker_combined_peak_note": broker_total.get("peak_note", "peak sample from collected combined metric") if broker_total else "",
        "producer_pending_at_flush_percent": pending_ratio,
        "producer_backlog_denominator": backlog_denominator,
        "producer_failed_send_percent": failed_ratio,
        "producer_flush_sec": flush_sec,
        "consumer_drain_gap": consumer_drain_gap,
        "network_not_saturated": network_not_saturated,
        "bottleneck_flags": bottleneck_flags,
        "system_saturation": {
            "max_cpu_percent": max_cpu_percent,
            "max_ram_used_percent": max_ram_percent,
            "max_network_utilization_percent": max_network_util,
            "max_ram_bandwidth_utilization_percent": max_ram_bandwidth_util,
        },
        "conclusions": conclusions or [primary],
}


def _build_throughput_verdict(
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
) -> dict[str, Any]:
    scenario = str(config.get("scenario", "unknown"))
    producer_measured = _scenario_has_measured_producer(scenario)
    consumer_measured = _scenario_has_measured_consumer(scenario)
    producer_attempt = _send_attempt_mib_s(producers)
    producer_delivered = _metric_mib_s(producers)
    consumer_received = _metric_mib_s(consumers)
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    broker_out = _broker_metric(broker_throughput, "kafka_jmx_bytes_out_counter_rate")
    broker_in_avg = _broker_avg_mib_s(broker_in) if broker_in else 0.0
    broker_out_avg = _broker_avg_mib_s(broker_out) if broker_out else 0.0
    delivery_efficiency = (
        producer_delivered / producer_attempt * 100.0
        if producer_attempt > 0
        else 0.0
    )
    operations = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(
            str(config.get("qualification_policy_id", ""))
        ),
    )
    pending_ratio = float(operations["producer_backlog_percent"])
    failed_ratio = float(operations["failed_send_percent"])
    flush_sec = float(operations["max_flush_duration_sec"])
    backlog_denominator = str(operations["backlog_denominator"])
    measured_values = []
    if producer_measured and producer_delivered > 0:
        measured_values.append(producer_delivered)
    if consumer_measured and consumer_received > 0:
        measured_values.append(consumer_received)
    clean_sustainable = min(measured_values, default=0.0)

    checks = []
    if producer_measured:
        checks.extend(
            [
                _quality_check(
                    "Producer failed sends",
                    failed_ratio <= 0.1,
                    f"{failed_ratio:.3f}% failed; threshold <= 0.100%.",
                ),
                _quality_check(
                    "Producer pending backlog",
                    pending_ratio <= 5.0,
                    f"{pending_ratio:.1f}% pending at flush start divided by "
                    f"{backlog_denominator}; threshold <= 5.0%.",
                ),
                _quality_check(
                    "Producer flush duration",
                    flush_sec <= 10.0,
                    f"{flush_sec:.1f}s flush; threshold <= 10.0s.",
                ),
                _quality_check(
                    "Producer delivery efficiency",
                    delivery_efficiency >= 85.0,
                    f"{delivery_efficiency:.1f}% delivered/attempted; threshold >= 85.0%.",
                    fail_status="warn",
                ),
            ]
        )
    if consumer_measured:
        checks.append(
            _quality_check(
                "Consumer measured throughput present",
                consumer_received > 0,
                f"{consumer_received:.1f} MiB/s received in measured consumer phase.",
                fail_status="warn",
            )
        )
    checks.append(
        _quality_check(
            "librdkafka stats sampled",
            (
                producer_measured
                and _int(_dict(producers.get("librdkafka_stats")).get("sample_count")) > 0
            )
            or (
                consumer_measured
                and _int(_dict(consumers.get("librdkafka_stats")).get("sample_count")) > 0
            ),
            "At least one active producer or consumer stats callback sample is present.",
            fail_status="warn",
        )
    )

    overdriven = producer_measured and (
        pending_ratio > 5.0 or failed_ratio > 0.1 or flush_sec > 10.0
    )
    if overdriven:
        label = "Overdriven"
        css_class = "warn"
        summary = (
            "The run completed, but offered load exceeded clean producer/client "
            "drain capacity. Treat delivered throughput, not broker ingress, as "
            "the sustainable result for this configuration."
        )
    else:
        label = "Qualified"
        css_class = "pass"
        summary = "The measured scenario drained cleanly without role-inappropriate target checks."

    return {
        "label": label,
        "class": css_class,
        "summary": summary,
        "scenario": scenario,
        "throughput_unit": "MiB/s",
        "clean_sustainable_mb_s": clean_sustainable,
        "producer_attempt_mb_s": producer_attempt,
        "producer_delivered_mb_s": producer_delivered,
        "consumer_received_mb_s": consumer_received,
        "broker_ingress_avg_mb_s": broker_in_avg,
        "broker_egress_avg_mb_s": broker_out_avg,
        "producer_delivery_efficiency_percent": delivery_efficiency,
        "producer_pending_at_flush_percent": pending_ratio,
        "producer_failed_send_percent": failed_ratio,
        "producer_flush_sec": flush_sec,
        "checks": checks,
    }


def _quality_check(
    name: str,
    passed: bool,
    evidence: str,
    *,
    fail_status: str = "fail",
) -> dict[str, str]:
    return {
        "name": name,
        "status": "pass" if passed else fail_status,
        "evidence": evidence,
    }


def _completed_iperf_by_label(inventory: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for test in _list(inventory.get("network_tests")):
        test = _dict(test)
        label = str(test.get("label", ""))
        if label and test.get("status") == "completed" and _float(test.get("megabytes_per_second")) > 0:
            result[label] = test
    return result


def _bottleneck_flag_rows(analysis: dict[str, Any]) -> list[tuple[Any, ...]]:
    flags = _dict(analysis.get("bottleneck_flags"))
    saturation = _dict(analysis.get("system_saturation"))
    return [
        (
            "CPU saturated",
            _yes_no(flags.get("cpu_saturated")),
            f"Peak node CPU {_float(saturation.get('max_cpu_percent')):.1f}% (threshold 85%).",
        ),
        (
            "RAM capacity saturated",
            _yes_no(flags.get("ram_capacity_saturated")),
            f"Peak node RAM used {_float(saturation.get('max_ram_used_percent')):.1f}% of total (threshold 85%).",
        ),
        (
            "RAM bandwidth saturated",
            _yes_no(flags.get("ram_bandwidth_saturated")),
            f"Broker combined Kafka traffic used {_float(saturation.get('max_ram_bandwidth_utilization_percent')):.1f}% of measured RAM copy aggregate bandwidth (threshold 80%).",
        ),
        (
            "Network saturated",
            _yes_no(flags.get("network_saturated")),
            f"Highest Kafka/iperf path utilization {_float(saturation.get('max_network_utilization_percent')):.1f}% (threshold 80%).",
        ),
        (
            "Producer backlog",
            _yes_no(flags.get("producer_backlog")),
            f"{_float(analysis.get('producer_pending_at_flush_percent')):.1f}% of "
            f"{analysis.get('producer_backlog_denominator', 'the documented denominator')} "
            "pending at flush start.",
        ),
        (
            "Producer failed sends",
            _yes_no(flags.get("producer_failed_sends")),
            f"{_float(analysis.get('producer_failed_send_percent')):.3f}% failed sends (threshold 0.100%).",
        ),
        (
            "Producer flush high",
            _yes_no(flags.get("producer_flush_high")),
            f"Flush duration {_float(analysis.get('producer_flush_sec')):.1f}s (threshold 10s).",
        ),
        (
            "Consumer drain gap",
            _yes_no(flags.get("consumer_drain_gap")),
            (
                f"Consumer received {_float(analysis.get('consumer_received_mb_s')):.1f} MiB/s "
                f"vs broker egress {_float(analysis.get('broker_egress_avg_mb_s')):.1f} MiB/s."
            ),
        ),
    ]


def _broker_ram_comparison(inventory: dict[str, Any], broker_combined_mb_s: float) -> dict[str, Any]:
    broker = _broker_inventory_node(inventory)
    if not broker:
        return {}
    memory = _dict(broker.get("memory"))
    probe = _dict(memory.get("bandwidth_probe"))
    metrics = _dict(probe.get("metrics"))
    read_gb_s = _float(metrics.get("read_gb_s"))
    write_gb_s = _float(metrics.get("write_gb_s"))
    copy_gb_s = _float(metrics.get("copy_gb_s"))
    triad_gb_s = _float(metrics.get("triad_gb_s"))
    if copy_gb_s <= 0:
        return {
            "resource": "Broker RAM bandwidth",
            "node": broker.get("node", "unknown"),
            "measured_capability": f"{probe.get('status', 'unknown')}: {probe.get('reason', 'no copy bandwidth metric')}",
            "observed_demand_mb_s": broker_combined_mb_s,
            "utilization_percent": None,
            "conclusion": "RAM probe unavailable",
        }
    capacity_mb_s = copy_gb_s * 1_000_000_000.0 / 1_048_576.0
    utilization = _utilization_percent(broker_combined_mb_s, capacity_mb_s)
    conclusion = "RAM bandwidth unlikely bottleneck"
    if utilization is not None and utilization >= 80.0:
        conclusion = "RAM bandwidth may be limiting"
    elif utilization is not None and utilization >= 50.0:
        conclusion = "RAM bandwidth should be inspected"
    capability_parts = []
    if read_gb_s > 0:
        capability_parts.append(f"read {read_gb_s:.2f} GB/s")
    if write_gb_s > 0:
        capability_parts.append(f"write {write_gb_s:.2f} GB/s")
    capability_parts.append(f"copy aggregate {copy_gb_s:.2f} GB/s")
    if triad_gb_s > 0:
        capability_parts.append(f"triad mixed {triad_gb_s:.2f} GB/s")
    return {
        "resource": "Broker RAM bandwidth",
        "node": broker.get("node", "unknown"),
        "measurement_scope": probe.get("measurement_scope", "single_process_stream_style"),
        "measured_capability": "; ".join(capability_parts),
        "read_gb_s": read_gb_s if read_gb_s > 0 else None,
        "write_gb_s": write_gb_s if write_gb_s > 0 else None,
        "copy_gb_s": copy_gb_s,
        "triad_gb_s": triad_gb_s if triad_gb_s > 0 else None,
        "capacity_mb_s": capacity_mb_s,
        "observed_demand_mb_s": broker_combined_mb_s,
        "utilization_percent": utilization,
        "conclusion": conclusion,
    }


def _broker_inventory_node(inventory: dict[str, Any]) -> dict[str, Any]:
    for node in _list(inventory.get("nodes")):
        node = _dict(node)
        if node.get("role") == "broker":
            return node
    return {}


def _network_path_comparison(
    label: str,
    iperf: dict[str, Any] | None,
    kafka_avg_mb_s: float,
    kafka_peak_mb_s: float,
) -> dict[str, Any]:
    if not iperf:
        return {
            "path": label,
            "nodes": "not measured",
            "status": "missing_capacity",
            "capacity_mb_s": None,
            "kafka_avg_mb_s": kafka_avg_mb_s,
            "avg_utilization_percent": None,
            "kafka_peak_mb_s": kafka_peak_mb_s,
            "peak_utilization_percent": None,
            "conclusion": "iperf3 capability missing",
        }
    capacity = (
        _float(iperf.get("megabytes_per_second"))
        * 1_000_000.0
        / 1_048_576.0
    )
    avg_util = _utilization_percent(kafka_avg_mb_s, capacity)
    peak_util = _utilization_percent(kafka_peak_mb_s, capacity)
    conclusion = "network not saturated" if avg_util is not None and avg_util < 80.0 else "near measured path capability"
    return {
        "path": label,
        "nodes": f"{iperf.get('source_node', 'unknown')} -> {iperf.get('target_node', 'unknown')}",
        "status": iperf.get("status", "unknown"),
        "capacity_mb_s": capacity,
        "capacity_gbit_s": _float(iperf.get("gbit_per_second")),
        "kafka_avg_mb_s": kafka_avg_mb_s,
        "avg_utilization_percent": avg_util,
        "kafka_peak_mb_s": kafka_peak_mb_s,
        "peak_utilization_percent": peak_util,
        "summary_source": iperf.get("summary_source"),
        "measured_seconds": iperf.get("measured_seconds", iperf.get("seconds")),
        "conclusion": conclusion,
    }


def _utilization_percent(value: float, capacity: float) -> float | None:
    if capacity <= 0:
        return None
    return value / capacity * 100.0


def _run_verdict(
    case: dict[str, Any],
    config: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    broker_throughput: dict[str, Any],
    monitoring: dict[str, Any],
    inventory: dict[str, Any],
) -> tuple[dict[str, str], list[str]]:
    warnings: list[str] = []
    broker_in = _broker_metric(broker_throughput, "kafka_jmx_bytes_in_counter_rate")
    case_status = str(case.get("status", "unknown"))

    if case_status != "completed":
        warnings.append(f"Case status is {case_status}, expected completed.")
    if not broker_in:
        warnings.append("Kafka broker JMX throughput was not populated.")
    if _int(producers.get("messages_failed")) > 0:
        warnings.append(f"Producer reported {_int(producers.get('messages_failed'))} failed sends.")
    failed_ratio = _ratio_percent(producers.get("messages_failed"), producers.get("messages_attempted"))
    if failed_ratio > 0.1:
        warnings.append(f"Producer failed sends are above threshold: {failed_ratio:.3f}% > 0.100%.")
    operations = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(
            str(config.get("qualification_policy_id", ""))
        ),
    )
    pending_ratio = float(operations["producer_backlog_percent"])
    if pending_ratio > 5.0:
        warnings.append(
            "Producer pending backlog at flush start is high: "
            f"{pending_ratio:.1f}% of "
            f"{operations['backlog_denominator']}."
        )
    if _float(producers.get("max_flush_duration_sec")) > 10.0:
        warnings.append(f"Producer flush time was {_float(producers.get('max_flush_duration_sec')):.1f}s; this can depress delivered MiB/s.")
    if _kafka_exporter_lag_unavailable(monitoring):
        warnings.append("Kafka exporter is up, but consumer lag metric is unavailable.")
    if monitoring and monitoring.get("status") not in {"completed", None}:
        warnings.append(f"Monitoring status is {monitoring.get('status')}.")
    probe_summary = _dict(inventory.get("probe_summary")) if inventory else {}
    failed_required = _int(probe_summary.get("failed_required_probe_count"))
    if inventory and inventory.get("status") != "completed":
        warnings.append(f"System inventory status is {inventory.get('status')}.")
    if failed_required:
        warnings.append(f"System inventory has {failed_required} failed or missing required probe(s).")

    if case_status != "completed":
        verdict = {
            "class": "fail",
            "label": "Failed",
            "summary": "The benchmark case did not complete successfully.",
        }
    elif warnings:
        verdict = {
            "class": "warn",
            "label": "Completed With Warnings",
            "summary": "The run completed, but one or more measurement or pipeline warnings need attention.",
        }
    else:
        verdict = {
            "class": "pass",
            "label": "Clean Run",
            "summary": "The case completed and the configured report checks look healthy.",
        }
    return verdict, warnings


def _events_from_result(benchmark_result: dict[str, Any]) -> list[dict[str, Any]]:
    raw_events = []
    raw_events.extend(_list(_dict(benchmark_result.get("timeline")).get("events")))
    raw_events.extend(_list(_dict(benchmark_result.get("monitoring")).get("events")))
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for event in raw_events:
        event = _dict(event)
        name = str(event.get("name", "event"))
        try:
            unix_time = float(event.get("unix", 0.0))
        except (TypeError, ValueError):
            continue
        if unix_time <= 0:
            continue
        event["unix"] = unix_time
        event.setdefault("label", name.replace("_", " ").title())
        deduped[(name, f"{unix_time:.3f}")] = event
    return sorted(deduped.values(), key=lambda item: float(item.get("unix", 0.0)))


def _format_event_time(event: dict[str, Any]) -> str:
    iso = str(event.get("iso", ""))
    if iso:
        return iso.replace("+00:00", "Z")
    unix_time = _float(event.get("unix"))
    return f"{unix_time:.3f}" if unix_time else "unknown"


def _metric_by_id(monitoring: dict[str, Any], metric_id: str) -> dict[str, Any]:
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        if metric.get("id") == metric_id:
            return metric
    return {}


def _series_by_node(metric: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for series in _list(metric.get("series")):
        series = _dict(series)
        labels = _dict(series.get("labels"))
        node = str(labels.get("node", labels.get("instance", "")))
        if node:
            result[node] = series
    return result


def _observed_network_rows(monitoring: dict[str, Any], inventory: dict[str, Any]) -> list[tuple[Any, ...]]:
    rx = _series_by_node(_metric_by_id(monitoring, "node_network_receive_mbps"))
    tx = _series_by_node(_metric_by_id(monitoring, "node_network_transmit_mbps"))
    if not rx and not tx:
        return []
    links: dict[str, str] = {}
    roles: dict[str, str] = {}
    for node in _list(inventory.get("nodes")):
        node = _dict(node)
        name = str(node.get("node", ""))
        if not name:
            continue
        roles[name] = str(node.get("role", "unknown"))
        links[name] = _gbit(_dict(node.get("network")).get("link_speed_gbit"), include_unit=True)
    rows = []
    for node in sorted(set(rx) | set(tx)):
        rx_series = _dict(rx.get(node))
        tx_series = _dict(tx.get(node))
        labels = _dict(rx_series.get("labels") or tx_series.get("labels"))
        role = labels.get("role") or roles.get(node, "unknown")
        rows.append(
            (
                node,
                role,
                links.get(node, "n/a"),
                f"{_float(rx_series.get('avg_value')):.3f}",
                f"{_float(rx_series.get('max_value')):.3f}",
                f"{_float(tx_series.get('avg_value')):.3f}",
                f"{_float(tx_series.get('max_value')):.3f}",
            )
        )
    return rows


def _system_saturation_rows(monitoring: dict[str, Any], inventory: dict[str, Any]) -> list[tuple[Any, ...]]:
    cpu = _series_by_node(_metric_by_id(monitoring, "node_cpu_busy_percent"))
    memory_used = _series_by_node(_metric_by_id(monitoring, "node_memory_used_gb"))
    memory_total = _series_by_node(_metric_by_id(monitoring, "node_memory_total_gb"))
    rx = _series_by_node(_metric_by_id(monitoring, "node_network_receive_mbps"))
    tx = _series_by_node(_metric_by_id(monitoring, "node_network_transmit_mbps"))
    nodes = sorted(set(cpu) | set(memory_used) | set(memory_total) | set(rx) | set(tx))
    if not nodes:
        return []

    roles: dict[str, str] = {}
    for node in _list(inventory.get("nodes")):
        node = _dict(node)
        name = str(node.get("node", ""))
        if name:
            roles[name] = str(node.get("role", "unknown"))

    rows = []
    for node in nodes:
        cpu_series = _dict(cpu.get(node))
        memory_used_series = _dict(memory_used.get(node))
        memory_total_series = _dict(memory_total.get(node))
        rx_series = _dict(rx.get(node))
        tx_series = _dict(tx.get(node))
        labels = _dict(
            cpu_series.get("labels")
            or memory_used_series.get("labels")
            or memory_total_series.get("labels")
            or rx_series.get("labels")
            or tx_series.get("labels")
        )
        role = labels.get("role") or roles.get(node, "unknown")
        total_gb = _float(memory_total_series.get("avg_value")) or _float(memory_total_series.get("max_value"))
        peak_used_gb = _float(memory_used_series.get("max_value"))
        peak_percent = (peak_used_gb / total_gb * 100.0) if total_gb else 0.0
        rows.append(
            (
                node,
                role,
                f"{_float(cpu_series.get('avg_value')):.1f}",
                f"{_float(cpu_series.get('max_value')):.1f}",
                f"{_float(memory_used_series.get('avg_value')):.1f}",
                f"{peak_used_gb:.1f}",
                f"{total_gb:.1f}" if total_gb else "n/a",
                f"{peak_percent:.1f}%" if total_gb else "n/a",
                f"{_float(rx_series.get('avg_value')):.1f}",
                f"{_float(rx_series.get('max_value')):.1f}",
                f"{_float(tx_series.get('avg_value')):.1f}",
                f"{_float(tx_series.get('max_value')):.1f}",
            )
        )
    return rows


def _ram_runtime_note(inventory: dict[str, Any]) -> str:
    settings = _dict(inventory.get("settings"))
    ram_root = str(settings.get("ram_root", ""))
    if ram_root:
        return f"Kafka broker logs plus runtime temp/cache/client directories were RAM-backed under {ram_root}."
    return "RAM-backed runtime root was not recorded in system inventory."


def _ram_bottleneck_note(comparison: dict[str, Any]) -> str:
    if not comparison:
        return "Broker RAM bandwidth comparison was not available."
    utilization = comparison.get("utilization_percent")
    if utilization is None:
        return f"Broker RAM bandwidth comparison unavailable: {comparison.get('measured_capability', 'missing probe')}."
    return (
        "Broker combined Kafka traffic used "
        f"{_float(utilization):.1f}% of measured single-process RAM copy aggregate bandwidth "
        f"on {comparison.get('node', 'broker')}; {comparison.get('conclusion', 'inspect RAM bandwidth')}."
    )


def _broker_metric(broker_throughput: dict[str, Any], metric_id: str) -> dict[str, Any]:
    matched = {}
    for metric in _list(broker_throughput.get("metrics")):
        metric = _dict(metric)
        if metric.get("id") == metric_id:
            matched = metric
            break
    fallback_id = {
        "kafka_jmx_messages_in_counter_rate": "kafka_jmx_messages_in_one_minute_rate",
        "kafka_jmx_bytes_in_counter_rate": "kafka_jmx_bytes_in_one_minute_rate",
        "kafka_jmx_bytes_out_counter_rate": "kafka_jmx_bytes_out_one_minute_rate",
    }.get(metric_id)
    if matched and (_metric_has_signal(matched) or not fallback_id):
        return matched
    if fallback_id:
        for metric in _list(broker_throughput.get("metrics")):
            metric = _dict(metric)
            if metric.get("id") == fallback_id and _metric_has_signal(metric):
                metric = dict(metric)
                metric["fallback_for"] = metric_id
                return metric
    return matched


def _metric_has_signal(metric: dict[str, Any]) -> bool:
    return _float(metric.get("avg_value")) > 0.0 or _float(metric.get("max_value")) > 0.0


def _broker_combined_metric(
    broker_throughput: dict[str, Any],
    broker_in: dict[str, Any],
    broker_out: dict[str, Any],
) -> dict[str, Any]:
    collected = _broker_metric(broker_throughput, "kafka_jmx_bytes_total_counter_rate")
    if collected and _metric_has_signal(collected):
        return collected
    if not broker_in and not broker_out:
        return {}
    avg_value = _float(broker_in.get("avg_value")) + _float(broker_out.get("avg_value"))
    max_value = _float(broker_in.get("max_value")) + _float(broker_out.get("max_value"))
    return {
        "id": "kafka_jmx_bytes_total_counter_rate",
        "title": "Kafka Broker Combined Bytes In+Out Rate",
        "unit": "bytes/sec",
        "avg_value": avg_value,
        "max_value": max_value,
        "source": "derived_from_directional_jmx_rates",
        "peak_note": "fallback peak is the sum of directional peak samples and may overstate a time-aligned peak",
    }


def _broker_kpi_value(metric: dict[str, Any]) -> str:
    if not metric:
        return "n/a"
    return f"{_broker_avg_mib_s(metric):.1f} MiB/s"


def _broker_avg_kpi_value(metric: dict[str, Any]) -> str:
    if not metric:
        return "n/a"
    return f"{_broker_avg_mib_s(metric):.1f} MiB/s"


def _broker_peak_kpi_value(metric: dict[str, Any]) -> str:
    if not metric:
        return "n/a"
    return f"{_broker_peak_mib_s(metric):.1f} MiB/s"


def _broker_avg_mib_s(metric: dict[str, Any]) -> float:
    value = _float(metric.get("avg_value"))
    return value / 1_048_576.0


def _broker_peak_mib_s(metric: dict[str, Any]) -> float:
    value = _float(metric.get("max_value"))
    return value / 1_048_576.0


def _metric_mib_s(metrics: dict[str, Any]) -> float:
    if metrics.get("throughput_mebibytes_per_sec") is not None:
        return _float(metrics.get("throughput_mebibytes_per_sec"))
    if metrics.get("throughput_bytes_per_sec") is not None:
        return _float(metrics.get("throughput_bytes_per_sec")) / 1_048_576.0
    return (
        _float(metrics.get("throughput_megabytes_per_sec"))
        * 1_000_000.0
        / 1_048_576.0
    )


def _send_attempt_mib_s(metrics: dict[str, Any]) -> float:
    if metrics.get("send_attempt_throughput_mebibytes_per_sec") is not None:
        return _float(metrics.get("send_attempt_throughput_mebibytes_per_sec"))
    if metrics.get("send_attempt_throughput_bytes_per_sec") is not None:
        return (
            _float(metrics.get("send_attempt_throughput_bytes_per_sec"))
            / 1_048_576.0
        )
    return (
        _float(metrics.get("send_attempt_throughput_megabytes_per_sec"))
        * 1_000_000.0
        / 1_048_576.0
    )


def _best_iperf_gbit(inventory: dict[str, Any]) -> float:
    return max((_float(test.get("gbit_per_second")) for test in _list(inventory.get("network_tests"))), default=0.0)


def _max_metric(monitoring: dict[str, Any], metric_id: str) -> float:
    for metric in _list(monitoring.get("collected_metrics")):
        metric = _dict(metric)
        if metric.get("id") == metric_id:
            return _float(metric.get("max_value"))
    return 0.0


def _max_ram_used_percent(monitoring: dict[str, Any]) -> float:
    used = _series_by_node(_metric_by_id(monitoring, "node_memory_used_gb"))
    total = _series_by_node(_metric_by_id(monitoring, "node_memory_total_gb"))
    percents = []
    for node, used_series in used.items():
        total_series = _dict(total.get(node))
        total_gb = _float(total_series.get("avg_value")) or _float(total_series.get("max_value"))
        used_peak = _float(_dict(used_series).get("max_value"))
        if total_gb > 0 and used_peak > 0:
            percents.append(used_peak / total_gb * 100.0)
    return max(percents, default=0.0)


def _cpu_shape(cpu: dict[str, Any]) -> str:
    return "{s}S x {c}C x {t}T ({l} logical)".format(
        s=cpu.get("sockets", "?"),
        c=cpu.get("cores_per_socket", "?"),
        t=cpu.get("threads_per_core", "?"),
        l=cpu.get("logical_cpus", "?"),
    )


def _ram_bandwidth(metrics: dict[str, Any], probe: dict[str, Any]) -> str:
    if not metrics:
        return f"{probe.get('status', 'skipped')}: {probe.get('reason', 'n/a')}"
    parts = []
    read_gb_s = _float(metrics.get("read_gb_s"))
    write_gb_s = _float(metrics.get("write_gb_s"))
    if read_gb_s > 0:
        parts.append(f"read {read_gb_s:.1f}")
    else:
        parts.append("read n/a")
    if write_gb_s > 0:
        parts.append(f"write {write_gb_s:.1f}")
    else:
        parts.append("write n/a")
    parts.extend(
        [
            f"copy agg {_float(metrics.get('copy_gb_s')):.1f}",
            f"scale agg {_float(metrics.get('scale_gb_s')):.1f}",
            f"add mixed {_float(metrics.get('add_gb_s')):.1f}",
            f"triad mixed {_float(metrics.get('triad_gb_s')):.1f}",
        ]
    )
    return ", ".join(parts)


def _gbit(value: Any, include_unit: bool = False) -> str:
    if value is None or value == "":
        return "n/a"
    text = f"{_float(value):.2f}"
    return f"{text} Gbit/s" if include_unit else text


def _failed_send_text(producers: dict[str, Any]) -> str:
    failed = _int(producers.get("messages_failed"))
    attempted = _int(producers.get("messages_attempted"))
    percent = (failed / attempted * 100.0) if attempted else 0.0
    return f"{failed} ({percent:.3f}% of attempted sends)"


def _ratio_percent(numerator: Any, denominator: Any) -> float:
    denominator_value = _int(denominator)
    if denominator_value <= 0:
        return 0.0
    return _int(numerator) / denominator_value * 100.0


def _has_flush_diagnostics(producers: dict[str, Any]) -> bool:
    return any(
        key in producers
        for key in (
            "messages_enqueued",
            "pending_messages_at_flush_start",
            "producer_queue_len_at_flush_start",
            "delivery_callbacks_during_flush",
            "produce_error_counts",
            "delivery_error_counts",
        )
    )


def _pending_flush_text(producers: dict[str, Any]) -> str:
    pending = _int(producers.get("pending_messages_at_flush_start"))
    pending_bytes = _bytes_text(producers.get("pending_bytes_at_flush_start"))
    queue_len = _int(producers.get("producer_queue_len_at_flush_start"))
    return f"{pending} messages, approx {pending_bytes}, queue length {queue_len}"


def _callbacks_during_flush_text(producers: dict[str, Any]) -> str:
    callbacks = _int(producers.get("delivery_callbacks_during_flush"))
    rate = _float(producers.get("delivery_callback_rate_during_flush_per_sec"))
    delivered = _int(producers.get("messages_delivered_during_flush"))
    failed = _int(producers.get("messages_failed_during_flush"))
    return f"{callbacks} callbacks ({rate:.1f}/s), delivered {delivered}, failed {failed}"


def _client_stats_row(role: str, stats: dict[str, Any]) -> tuple[Any, ...]:
    if not stats:
        return (
            role,
            "0/0",
            "0",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "not collected",
        )
    enabled = bool(stats.get("enabled"))
    rank_count = _int(stats.get("rank_count"))
    enabled_count = _int(stats.get("stats_enabled_rank_count"))
    if not enabled:
        return (
            role,
            f"0/{rank_count}",
            "0",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            "n/a",
            stats.get("reason", "disabled or unavailable"),
        )
    return (
        role,
        f"{enabled_count}/{rank_count}",
        _int(stats.get("sample_count")),
        _bytes_to_mb_text(stats.get("tx_bytes_last")),
        _bytes_to_mb_text(stats.get("rx_bytes_last")),
        f"{_int(stats.get('txerrs_last'))}",
        f"{_int(stats.get('rxerrs_last'))}",
        _queue_text(stats.get("max_msg_cnt"), stats.get("max_msg_size")),
        f"{_float(stats.get('max_broker_waitresp_cnt')):.0f}",
        _microseconds_text(stats.get("max_broker_rtt_p95_us")),
        _partition_queue_text(stats),
        f"{_float(stats.get('max_toppar_consumer_lag')):.0f}",
        f"{_float(stats.get('max_cgrp_rebalance_cnt')):.0f}",
        _client_state_text(stats),
    )


def _queue_text(count: Any, bytes_value: Any) -> str:
    return f"{_float(count):.0f} msg / {_bytes_text(bytes_value)}"


def _partition_queue_text(stats: dict[str, Any]) -> str:
    producer_queue = _float(stats.get("max_toppar_msgq_cnt"))
    transmit_queue = _float(stats.get("max_toppar_xmit_msgq_cnt"))
    fetch_queue = _float(stats.get("max_toppar_fetchq_cnt"))
    parts = []
    if producer_queue:
        parts.append(f"msgq {producer_queue:.0f}")
    if transmit_queue:
        parts.append(f"xmit {transmit_queue:.0f}")
    if fetch_queue:
        parts.append(f"fetch {fetch_queue:.0f}")
    return ", ".join(parts) if parts else "n/a"


def _client_state_text(stats: dict[str, Any]) -> str:
    states = _dict(stats.get("broker_state_counts"))
    cgrp = _dict(stats.get("consumer_group_state_counts"))
    parts = []
    if states:
        parts.append("broker " + _format_counts(states))
    if cgrp:
        parts.append("group " + _format_counts(cgrp))
    return "; ".join(parts) if parts else "n/a"


def _mb_s_text(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return f"{_float(value):.1f} MiB/s"


def _percent_text(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return f"{_float(value):.1f}%"


def _yes_no(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _bytes_text(value: Any) -> str:
    bytes_value = _float(value)
    if bytes_value >= 1_000_000_000:
        return f"{bytes_value / 1_000_000_000:.2f} GB"
    if bytes_value >= 1_000_000:
        return f"{bytes_value / 1_000_000:.2f} MB"
    if bytes_value >= 1_000:
        return f"{bytes_value / 1_000:.2f} KB"
    return f"{bytes_value:.0f} bytes"


def _bytes_to_mb_text(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return f"{_float(value) / 1_000_000.0:.1f}"


def _microseconds_text(value: Any) -> str:
    if value is None or value == "":
        return "n/a"
    return f"{_float(value):.0f} us"


def _format_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    items = sorted(
        ((str(key), _int(count)) for key, count in value.items()),
        key=lambda item: (-item[1], item[0]),
    )
    return ", ".join(f"{key}: {count}" for key, count in items[:8])


def _iperf_note(test: dict[str, Any]) -> str:
    notes = []
    summary_source = test.get("summary_source")
    measured_seconds = test.get("measured_seconds", test.get("seconds"))
    if summary_source:
        notes.append(f"summary={summary_source}")
    if measured_seconds not in {None, ""}:
        notes.append(f"duration={_float(measured_seconds):.2f}s")
    reason = str(test.get("reason", ""))
    if reason:
        notes.append(reason)
    return "; ".join(notes)


def _format_mpi_ranks(ranks: list[Any]) -> str:
    grouped: dict[str, list[int]] = {}
    for rank in ranks:
        rank = _dict(rank)
        role = str(rank.get("role", "rank"))
        try:
            value = int(rank.get("rank"))
        except (TypeError, ValueError):
            continue
        grouped.setdefault(role, []).append(value)
    if not grouped:
        return "none"
    parts = []
    for role in sorted(grouped):
        parts.append(f"{_range_text(sorted(grouped[role]))} {role}")
    return "; ".join(parts)


def _range_text(values: list[int]) -> str:
    if not values:
        return ""
    ranges = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(f"{start}" if start == previous else f"{start}-{previous}")
        start = previous = value
    ranges.append(f"{start}" if start == previous else f"{start}-{previous}")
    return ", ".join(ranges)


def _graph_caption(metric: dict[str, Any]) -> str:
    category = str(metric.get("category", "monitoring"))
    unit = str(metric.get("unit", ""))
    title = str(metric.get("title", metric.get("id", "metric")))
    if category == "system":
        return f"Node exporter per-node host metric. Unit: {unit}. Legend labels use node plus role; numbered event markers are keyed below the graph."
    if category == "kafka_jmx":
        return f"JMX exporter broker/JVM metric: {title}. Unit: {unit}. Byte-rate values are converted to MiB/s in the tables."
    if category == "kafka_exporter":
        return f"Kafka exporter cluster/topic/offset/lag sanity metric. Unit: {unit}. Dense partition series are limited in the graph; full samples remain in CSV/JSON artifacts."
    if category == "jvm":
        return f"JMX exporter JVM metric from the Kafka broker process. Unit: {unit}."
    return f"Source: {category}. Unit: {unit}. Numbered event markers use UTC time."


def _kpi(label: str, value: str, detail: str) -> str:
    return f"""
<div class="kpi">
  <span>{_escape(label)}</span>
  <strong>{_escape(value)}</strong>
  <small>{_escape(detail)}</small>
</div>
"""


def _table(headers: list[str], rows: list[tuple[Any, ...]]) -> str:
    if not rows:
        return '<p class="section-note">No data available.</p>'
    head = "".join(f"<th>{_escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{_escape(value)}</td>" for value in row)
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


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


def _escape(value: Any) -> str:
    if isinstance(value, (dict, list)):
        value = json.dumps(value, sort_keys=True)
    return html.escape(str(value), quote=True)


def _css() -> str:
    return """
:root { color-scheme: light; --ink: #18212f; --muted: #5b677a; --line: #d9e0ea; --panel: #f7f9fc; --accent: #126c8f; --accent-2: #6b4e16; }
* { box-sizing: border-box; }
body { margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: var(--ink); background: #ffffff; line-height: 1.45; }
.page { width: min(1480px, calc(100vw - 40px)); margin: 0 auto; padding: 28px 0 48px; }
section { padding: 28px 0; border-bottom: 1px solid var(--line); }
.hero { display: grid; grid-template-columns: minmax(0, 0.9fr) minmax(640px, 1.1fr); gap: 28px; align-items: start; }
.hero > * { min-width: 0; }
.hero-copy { min-width: 0; max-width: 100%; overflow: hidden; }
.hero .table-wrap { grid-column: 1 / -1; }
.eyebrow { margin: 0 0 8px; color: var(--accent); font-weight: 700; text-transform: uppercase; font-size: 12px; letter-spacing: 0; }
h1 { margin: 0; max-width: 100%; font-size: clamp(28px, 2.0vw, 36px); line-height: 1.08; letter-spacing: 0; overflow-wrap: anywhere; word-break: break-all; }
h2 { margin: 0 0 10px; font-size: 24px; letter-spacing: 0; }
h3 { margin: 18px 0 8px; font-size: 17px; letter-spacing: 0; }
.lede, .section-note { color: var(--muted); max-width: 880px; overflow-wrap: anywhere; }
.compact-note { max-width: 740px; font-size: 13px; }
.config-used { background: #fbfcfe; }
.config-details { margin-top: 12px; border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; background: #fff; }
.config-details summary { cursor: pointer; font-weight: 700; color: #263348; }
pre { margin: 12px 0 0; padding: 12px; border-radius: 6px; background: #f4f7fb; overflow-x: auto; font-size: 12px; line-height: 1.45; white-space: pre; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; align-items: stretch; }
.kpi { border: 1px solid var(--line); border-radius: 8px; padding: 14px; background: var(--panel); min-height: 112px; min-width: 0; overflow: hidden; overflow-wrap: anywhere; }
.kpi span, .kpi small { display: block; color: var(--muted); }
.kpi strong { display: block; margin: 8px 0 6px; font-size: clamp(21px, 1.5vw, 24px); color: var(--accent); overflow-wrap: anywhere; }
.verdict { display: flex; gap: 12px; align-items: center; border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; margin: 10px 0 14px; background: var(--panel); }
.verdict strong { font-size: 18px; }
.verdict span { color: var(--muted); }
.verdict.pass { border-color: #8dc7a4; background: #f0faf4; }
.verdict.warn { border-color: #d6b36a; background: #fff8e8; }
.verdict.fail { border-color: #d98b8b; background: #fff0f0; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { padding: 9px 10px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; overflow-wrap: anywhere; }
th { background: #eef3f8; color: #263348; font-weight: 700; white-space: nowrap; }
td { min-width: 92px; }
.compact td { min-width: 0; }
.wide table { min-width: 1100px; }
.graph-grid { display: grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 18px; }
.graph-card { border: 1px solid var(--line); border-radius: 8px; padding: 14px; background: #fff; }
.graph-card h3 { margin-top: 0; }
.graph-card p { color: var(--muted); margin-top: 0; }
.graph-box { width: 100%; overflow-x: auto; }
.graph-box svg, .graph-box img { width: 100%; height: auto; display: block; }
.notes { margin: 0; padding-left: 20px; color: var(--ink); }
.warnings { margin-top: 12px; color: #3b4656; }
@media (max-width: 1180px) { .hero { grid-template-columns: 1fr; } }
@media (max-width: 900px) { .page { width: min(100vw - 24px, 1480px); } .kpi-grid, .graph-grid { grid-template-columns: 1fr; } h1 { font-size: 30px; } }
""".strip()
