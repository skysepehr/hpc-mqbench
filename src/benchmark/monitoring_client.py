from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """
    One Prometheus range query the local monitoring collector may run.
    """

    metric_id: str
    title: str
    query: str
    category: str
    required_any: tuple[str, ...]
    unit: str = ""
    required_for_complete: bool = True


LOCAL_METRIC_SPECS: tuple[MetricSpec, ...] = (
    MetricSpec(
        metric_id="node_cpu_busy_percent",
        title="Node CPU Busy Percent",
        query='100 * avg by (instance, node, role) (1 - rate(node_cpu_seconds_total{mode="idle"}[1m]))',
        category="system",
        required_any=("node_cpu_seconds_total",),
        unit="percent",
    ),
    MetricSpec(
        metric_id="node_memory_available_gb",
        title="Node Memory Available",
        query="node_memory_MemAvailable_bytes / 1000000000",
        category="system",
        required_any=("node_memory_MemAvailable_bytes",),
        unit="GB",
    ),
    MetricSpec(
        metric_id="node_memory_total_gb",
        title="Node Memory Total",
        query="node_memory_MemTotal_bytes / 1000000000",
        category="system",
        required_any=("node_memory_MemTotal_bytes",),
        unit="GB",
    ),
    MetricSpec(
        metric_id="node_memory_used_gb",
        title="Node Memory Used",
        query="(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1000000000",
        category="system",
        required_any=("node_memory_MemAvailable_bytes", "node_memory_MemTotal_bytes"),
        unit="GB",
    ),
    MetricSpec(
        metric_id="node_network_receive_mbps",
        title="Node Network Receive Rate",
        query='sum by (instance, node, role) (rate(node_network_receive_bytes_total{device!="lo"}[1m])) / 1000000',
        category="system",
        required_any=("node_network_receive_bytes_total",),
        unit="MB/s",
    ),
    MetricSpec(
        metric_id="node_network_transmit_mbps",
        title="Node Network Transmit Rate",
        query='sum by (instance, node, role) (rate(node_network_transmit_bytes_total{device!="lo"}[1m])) / 1000000',
        category="system",
        required_any=("node_network_transmit_bytes_total",),
        unit="MB/s",
    ),
    MetricSpec(
        metric_id="broker_ib0_receive_mbps",
        title="Broker ib0 Receive Rate",
        query='sum by (instance, node, role) (rate(node_network_receive_bytes_total{device="ib0",role="broker"}[30s])) / 1000000',
        category="system",
        required_any=("node_network_receive_bytes_total",),
        unit="MB/s",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="broker_ib0_transmit_mbps",
        title="Broker ib0 Transmit Rate",
        query='sum by (instance, node, role) (rate(node_network_transmit_bytes_total{device="ib0",role="broker"}[30s])) / 1000000',
        category="system",
        required_any=("node_network_transmit_bytes_total",),
        unit="MB/s",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_exporter_brokers",
        title="Kafka Exporter Broker Count",
        query="kafka_brokers",
        category="kafka_exporter",
        required_any=("kafka_brokers",),
        unit="brokers",
    ),
    MetricSpec(
        metric_id="kafka_exporter_topic_partitions",
        title="Kafka Exporter Topic Partitions",
        query="kafka_topic_partitions",
        category="kafka_exporter",
        required_any=("kafka_topic_partitions",),
        unit="partitions",
    ),
    MetricSpec(
        metric_id="kafka_exporter_current_offset",
        title="Kafka Topic Current Offset",
        query="kafka_topic_partition_current_offset",
        category="kafka_exporter",
        required_any=("kafka_topic_partition_current_offset",),
        unit="offset",
    ),
    MetricSpec(
        metric_id="kafka_exporter_oldest_offset",
        title="Kafka Topic Oldest Offset",
        query="kafka_topic_partition_oldest_offset",
        category="kafka_exporter",
        required_any=("kafka_topic_partition_oldest_offset",),
        unit="offset",
    ),
    MetricSpec(
        metric_id="kafka_exporter_consumer_lag",
        title="Kafka Consumer Group Lag",
        query=(
            "kafka_consumergroup_lag or "
            "kafka_consumergroup_lag_sum or "
            "kafka_consumergroup_group_lag or "
            "kafka_consumergroup_group_lag_sum"
        ),
        category="kafka_exporter",
        required_any=(
            "kafka_consumergroup_lag",
            "kafka_consumergroup_lag_sum",
            "kafka_consumergroup_group_lag",
            "kafka_consumergroup_group_lag_sum",
        ),
        unit="messages",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_jmx_messages_in_total",
        title="Kafka JMX Messages In Total",
        query="kafka_server_brokertopicmetrics_messagesinpersec_total",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_messagesinpersec_total",),
        unit="messages",
    ),
    MetricSpec(
        metric_id="kafka_jmx_messages_in_counter_rate",
        title="Kafka Broker Messages In Rate",
        query="sum by (instance, node, role) (rate(kafka_server_brokertopicmetrics_messagesinpersec_total[1m]))",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_messagesinpersec_total",),
        unit="messages/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_messages_in_one_minute_rate",
        title="Kafka Broker Messages In One-Minute Rate",
        query="sum by (instance, node, role) (kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate)",
        category="kafka_jmx",
        required_any=(
            "kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate",
        ),
        unit="messages/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_in_total",
        title="Kafka JMX Bytes In Total",
        query="kafka_server_brokertopicmetrics_bytesinpersec_total",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesinpersec_total",),
        unit="bytes",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_in_counter_rate",
        title="Kafka Broker Bytes In Rate",
        query="sum by (instance, node, role) (rate(kafka_server_brokertopicmetrics_bytesinpersec_total[1m]))",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesinpersec_total",),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_in_one_minute_rate",
        title="Kafka Broker Bytes In One-Minute Rate",
        query="sum by (instance, node, role) (kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate)",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate",),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_out_total",
        title="Kafka JMX Bytes Out Total",
        query="kafka_server_brokertopicmetrics_bytesoutpersec_total",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesoutpersec_total",),
        unit="bytes",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_out_counter_rate",
        title="Kafka Broker Bytes Out Rate",
        query="sum by (instance, node, role) (rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[1m]))",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesoutpersec_total",),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_total_counter_rate",
        title="Kafka Broker Combined Bytes In+Out Rate",
        query=(
            "sum by (instance, node, role) (rate(kafka_server_brokertopicmetrics_bytesinpersec_total[1m])) + "
            "sum by (instance, node, role) (rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[1m]))"
        ),
        category="kafka_jmx",
        required_any=(
            "kafka_server_brokertopicmetrics_bytesinpersec_total",
            "kafka_server_brokertopicmetrics_bytesoutpersec_total",
        ),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_bytes_out_one_minute_rate",
        title="Kafka Broker Bytes Out One-Minute Rate",
        query="sum by (instance, node, role) (kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate)",
        category="kafka_jmx",
        required_any=("kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate",),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="kafka_jmx_under_replicated_partitions",
        title="Kafka Under-Replicated Partitions",
        query="kafka_server_replicamanager_underreplicatedpartitions",
        category="kafka_jmx",
        required_any=("kafka_server_replicamanager_underreplicatedpartitions",),
        unit="partitions",
    ),
    MetricSpec(
        metric_id="kafka_jmx_active_controller_count",
        title="Kafka Active Controller Count",
        query="kafka_controller_kafkacontroller_activecontrollercount",
        category="kafka_jmx",
        required_any=("kafka_controller_kafkacontroller_activecontrollercount",),
        unit="controllers",
    ),
    MetricSpec(
        metric_id="kafka_request_queue_size",
        title="Kafka Request Queue Size",
        query="kafka_network_requestchannel_requestqueuesize",
        category="kafka_jmx",
        required_any=("kafka_network_requestchannel_requestqueuesize",),
        unit="requests",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_response_queue_size",
        title="Kafka Response Queue Size",
        query="kafka_network_requestchannel_responsequeuesize",
        category="kafka_jmx",
        required_any=("kafka_network_requestchannel_responsequeuesize",),
        unit="responses",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_request_handler_idle_ratio",
        title="Kafka Request Handler Idle Ratio",
        query="kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_oneminuterate",
        category="kafka_jmx",
        required_any=(
            "kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent_oneminuterate",
        ),
        unit="ratio",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_network_processor_idle_ratio",
        title="Kafka Network Processor Idle Ratio",
        query="kafka_network_socketserver_networkprocessoravgidlepercent_oneminuterate",
        category="kafka_jmx",
        required_any=(
            "kafka_network_socketserver_networkprocessoravgidlepercent_oneminuterate",
        ),
        unit="ratio",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_request_queue_time_p99_ms",
        title="Kafka Request Queue Time p99",
        query='kafka_network_requestmetrics_requestqueuetimems_99thpercentile',
        category="kafka_jmx",
        required_any=(
            "kafka_network_requestmetrics_requestqueuetimems_99thpercentile",
        ),
        unit="ms",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_local_time_p99_ms",
        title="Kafka Local Request Processing Time p99",
        query='kafka_network_requestmetrics_localtimems_99thpercentile',
        category="kafka_jmx",
        required_any=("kafka_network_requestmetrics_localtimems_99thpercentile",),
        unit="ms",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_response_queue_time_p99_ms",
        title="Kafka Response Queue Time p99",
        query='kafka_network_requestmetrics_responsequeuetimems_99thpercentile',
        category="kafka_jmx",
        required_any=(
            "kafka_network_requestmetrics_responsequeuetimems_99thpercentile",
        ),
        unit="ms",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="kafka_total_time_p99_ms",
        title="Kafka Total Request Time p99",
        query='kafka_network_requestmetrics_totaltimems_99thpercentile',
        category="kafka_jmx",
        required_any=("kafka_network_requestmetrics_totaltimems_99thpercentile",),
        unit="ms",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="jvm_heap_used_gb",
        title="Kafka JVM Heap Used",
        query="jvm_memory_heap_used_bytes / 1000000000",
        category="jvm",
        required_any=("jvm_memory_heap_used_bytes",),
        unit="GB",
    ),
    MetricSpec(
        metric_id="jvm_nonheap_used_gb",
        title="Kafka JVM Non-Heap Used",
        query="jvm_memory_nonheap_used_bytes / 1000000000",
        category="jvm",
        required_any=("jvm_memory_nonheap_used_bytes",),
        unit="GB",
    ),
    MetricSpec(
        metric_id="jvm_thread_count",
        title="Kafka JVM Thread Count",
        query="jvm_threads_threadcount",
        category="jvm",
        required_any=("jvm_threads_threadcount",),
        unit="threads",
    ),
    MetricSpec(
        metric_id="kafka_process_cpu_ratio",
        title="Kafka JVM Process CPU Ratio",
        query="jvm_operatingsystem_processcpuload",
        category="jvm",
        required_any=("jvm_operatingsystem_processcpuload",),
        unit="ratio",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="jvm_gc_collection_rate",
        title="Kafka JVM GC Collections",
        query=(
            "sum by (instance, node, role, gc) "
            "(rate(jvm_gc_collection_seconds_count[30s]))"
        ),
        category="jvm",
        required_any=("jvm_gc_collection_seconds_count",),
        unit="collections/sec",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="jvm_gc_time_rate_ms_per_sec",
        title="Kafka JVM GC Time Rate",
        query=(
            "1000 * sum by (instance, node, role, gc) "
            "(rate(jvm_gc_collection_seconds_sum[30s]))"
        ),
        category="jvm",
        required_any=("jvm_gc_collection_seconds_sum",),
        unit="ms/sec",
        required_for_complete=False,
    ),
)


PULSAR_METRIC_SPECS: tuple[MetricSpec, ...] = (
    MetricSpec(
        metric_id="pulsar_service_ib0_receive_mbps",
        title="Pulsar Service ib0 Receive Rate",
        query=(
            'sum by (instance, node, role) '
            '(rate(node_network_receive_bytes_total{device="ib0",role="pulsar_service"}[30s])) '
            "/ 1000000"
        ),
        category="system",
        required_any=("node_network_receive_bytes_total",),
        unit="MB/s",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_service_ib0_transmit_mbps",
        title="Pulsar Service ib0 Transmit Rate",
        query=(
            'sum by (instance, node, role) '
            '(rate(node_network_transmit_bytes_total{device="ib0",role="pulsar_service"}[30s])) '
            "/ 1000000"
        ),
        category="system",
        required_any=("node_network_transmit_bytes_total",),
        unit="MB/s",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_messages_in_rate",
        title="Pulsar Message Ingress Rate",
        query="sum(pulsar_broker_rate_in) or sum(pulsar_rate_in)",
        category="pulsar",
        required_any=("pulsar_broker_rate_in", "pulsar_rate_in"),
        unit="records/sec",
    ),
    MetricSpec(
        metric_id="pulsar_messages_out_rate",
        title="Pulsar Message Egress Rate",
        query="sum(pulsar_broker_rate_out) or sum(pulsar_rate_out)",
        category="pulsar",
        required_any=("pulsar_broker_rate_out", "pulsar_rate_out"),
        unit="records/sec",
    ),
    MetricSpec(
        metric_id="pulsar_bytes_in_rate",
        title="Pulsar Byte Ingress Rate",
        query="sum(pulsar_broker_throughput_in) or sum(pulsar_throughput_in)",
        category="pulsar",
        required_any=("pulsar_broker_throughput_in", "pulsar_throughput_in"),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="pulsar_bytes_out_rate",
        title="Pulsar Byte Egress Rate",
        query="sum(pulsar_broker_throughput_out) or sum(pulsar_throughput_out)",
        category="pulsar",
        required_any=("pulsar_broker_throughput_out", "pulsar_throughput_out"),
        unit="bytes/sec",
    ),
    MetricSpec(
        metric_id="pulsar_message_backlog",
        title="Pulsar Subscription Backlog",
        query="sum(pulsar_broker_msg_backlog) or sum(pulsar_msg_backlog)",
        category="pulsar",
        required_any=("pulsar_broker_msg_backlog", "pulsar_msg_backlog"),
        unit="records",
    ),
    MetricSpec(
        metric_id="pulsar_storage_size_bytes",
        title="Pulsar Managed-Ledger Storage Size",
        query="sum(pulsar_broker_storage_size) or sum(pulsar_storage_size)",
        category="pulsar",
        required_any=("pulsar_broker_storage_size", "pulsar_storage_size"),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_producer_count",
        title="Pulsar Connected Producer Count",
        query="sum(pulsar_broker_producers_count) or sum(pulsar_producers_count)",
        category="pulsar",
        required_any=("pulsar_broker_producers_count", "pulsar_producers_count"),
        unit="producers",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_consumer_count",
        title="Pulsar Connected Consumer Count",
        query="sum(pulsar_broker_consumers_count) or sum(pulsar_consumers_count)",
        category="pulsar",
        required_any=("pulsar_broker_consumers_count", "pulsar_consumers_count"),
        unit="consumers",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_jvm_heap_used_bytes",
        title="Pulsar JVM Heap Used",
        query='sum(jvm_memory_bytes_used{area="heap"})',
        category="pulsar_jvm",
        required_any=("jvm_memory_bytes_used",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_jvm_direct_memory_used_bytes",
        title="Pulsar JVM NIO Direct Buffer Used (Partial)",
        query='sum(jvm_buffer_pool_used_bytes{pool="direct"})',
        category="pulsar_jvm",
        required_any=("jvm_buffer_pool_used_bytes",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_managed_ledger_direct_pool_allocated_bytes",
        title="Pulsar Managed-Ledger Direct Pool Allocated",
        query="sum(pulsar_ml_cache_pool_allocated)",
        category="pulsar_jvm",
        required_any=("pulsar_ml_cache_pool_allocated",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_managed_ledger_direct_pool_used_bytes",
        title="Pulsar Managed-Ledger Direct Pool Used",
        query="sum(pulsar_ml_cache_pool_used)",
        category="pulsar_jvm",
        required_any=("pulsar_ml_cache_pool_used",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_direct_memory_usage_percent",
        title="Pulsar Broker Direct-Memory Usage",
        query="max(pulsar_lb_directMemory_usage)",
        category="pulsar_jvm",
        required_any=("pulsar_lb_directMemory_usage",),
        unit="percent",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_process_resident_memory_bytes",
        title="Pulsar Process Resident Memory",
        query="max(process_resident_memory_bytes)",
        category="pulsar_jvm",
        required_any=("process_resident_memory_bytes",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_jvm_thread_count",
        title="Pulsar JVM Thread Count",
        query="sum(jvm_threads_current)",
        category="pulsar_jvm",
        required_any=("jvm_threads_current",),
        unit="threads",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_jvm_gc_time_rate",
        title="Pulsar JVM Garbage-Collection Time Rate",
        query="sum(rate(jvm_gc_collection_seconds_sum[30s]))",
        category="pulsar_jvm",
        required_any=("jvm_gc_collection_seconds_sum",),
        unit="seconds/sec",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_jvm_gc_collection_rate",
        title="Pulsar JVM Garbage-Collection Rate",
        query="sum(rate(jvm_gc_collection_seconds_count[30s]))",
        category="pulsar_jvm",
        required_any=("jvm_gc_collection_seconds_count",),
        unit="collections/sec",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_journal_queue_size",
        title="Pulsar BookKeeper Journal Queue Size",
        query="sum(bookie_journal_JOURNAL_QUEUE_SIZE)",
        category="pulsar_storage",
        required_any=("bookie_journal_JOURNAL_QUEUE_SIZE",),
        unit="requests",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_force_write_queue_size",
        title="Pulsar BookKeeper Force-Write Queue Size",
        query="sum(bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE)",
        category="pulsar_storage",
        required_any=("bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE",),
        unit="requests",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_callback_queue_size",
        title="Pulsar BookKeeper Journal Callback Queue Size",
        query="sum(bookie_journal_JOURNAL_CB_QUEUE_SIZE)",
        category="pulsar_storage",
        required_any=("bookie_journal_JOURNAL_CB_QUEUE_SIZE",),
        unit="callbacks",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_write_cache_bytes",
        title="Pulsar BookKeeper Write Cache Size",
        query="sum(bookie_write_cache_size)",
        category="pulsar_storage",
        required_any=("bookie_write_cache_size",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_read_cache_bytes",
        title="Pulsar BookKeeper Read Cache Size",
        query="sum(bookie_read_cache_size)",
        category="pulsar_storage",
        required_any=("bookie_read_cache_size",),
        unit="bytes",
        required_for_complete=False,
    ),
    MetricSpec(
        metric_id="pulsar_bookie_throttled_write_rate",
        title="Pulsar BookKeeper Throttled Write Rate",
        query="sum(rate(bookie_throttled_write_requests[30s]))",
        category="pulsar_storage",
        required_any=("bookie_throttled_write_requests",),
        unit="requests/sec",
        required_for_complete=False,
    ),
)


def metric_specs_for_backend(backend_id: str) -> tuple[MetricSpec, ...]:
    """Return common and backend-specific metrics without cross-system queries."""
    normalized = backend_id.strip().lower()
    if normalized == "kafka":
        return LOCAL_METRIC_SPECS
    if normalized == "pulsar":
        common = tuple(
            spec
            for spec in LOCAL_METRIC_SPECS
            if spec.category == "system"
            and not spec.metric_id.startswith("broker_ib0_")
        )
        return common + PULSAR_METRIC_SPECS
    raise ValueError(f"Unsupported monitoring backend_id: {backend_id!r}")


def normalize_prometheus_url(prometheus_url: str) -> str:
    """
    Return a normalized Prometheus base URL without a trailing slash.
    """
    value = prometheus_url.strip()
    if not value:
        raise ValueError("prometheus_url must not be empty")
    if not value.startswith(("http://", "https://")):
        value = f"http://{value}"
    return value.rstrip("/")


def request_json(
    prometheus_url: str,
    path: str,
    params: dict[str, Any] | None = None,
    timeout_sec: float = 10.0,
) -> dict[str, Any]:
    """
    Request one Prometheus HTTP API endpoint with only the Python stdlib.
    """
    base_url = normalize_prometheus_url(prometheus_url)
    query = urllib.parse.urlencode(params or {})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"

    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        payload = response.read().decode("utf-8")

    data = json.loads(payload)
    if not isinstance(data, dict):
        raise RuntimeError(f"Prometheus returned a non-object response for {path}")
    return data


def check_prometheus(prometheus_url: str, timeout_sec: float = 5.0) -> dict[str, Any]:
    """
    Verify that Prometheus is reachable.
    """
    payload = request_json(
        prometheus_url,
        "/api/v1/status/buildinfo",
        timeout_sec=timeout_sec,
    )
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error", "Prometheus status check failed"))
    return payload


def query_range(
    prometheus_url: str,
    query: str,
    start: float,
    end: float,
    step: int | float,
    timeout_sec: float = 10.0,
) -> dict[str, Any]:
    """
    Query a Prometheus range vector.
    """
    payload = request_json(
        prometheus_url,
        "/api/v1/query_range",
        {
            "query": query,
            "start": f"{start:.3f}",
            "end": f"{end:.3f}",
            "step": str(step),
        },
        timeout_sec=timeout_sec,
    )
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error", "Prometheus range query failed"))
    return payload


def fetch_metric_names(prometheus_url: str, timeout_sec: float = 10.0) -> set[str]:
    """
    Fetch all metric names currently known to Prometheus.
    """
    payload = request_json(
        prometheus_url,
        "/api/v1/label/__name__/values",
        timeout_sec=timeout_sec,
    )
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error", "Could not fetch metric names"))
    values = payload.get("data", [])
    if not isinstance(values, list):
        return set()
    return {str(value) for value in values}


def fetch_targets(prometheus_url: str, timeout_sec: float = 10.0) -> dict[str, Any]:
    """
    Fetch and simplify Prometheus scrape target health.
    """
    payload = request_json(prometheus_url, "/api/v1/targets", timeout_sec=timeout_sec)
    data = payload.get("data", {})
    active_targets = data.get("activeTargets", []) if isinstance(data, dict) else []
    dropped_targets = data.get("droppedTargets", []) if isinstance(data, dict) else []

    health_counts: dict[str, int] = {}
    simplified_targets: list[dict[str, Any]] = []
    for target in active_targets:
        if not isinstance(target, dict):
            continue
        health = str(target.get("health", "unknown"))
        health_counts[health] = health_counts.get(health, 0) + 1
        simplified_targets.append(
            {
                "scrape_url": target.get("scrapeUrl"),
                "health": health,
                "last_error": target.get("lastError", ""),
                "labels": target.get("labels", {}),
            }
        )

    return {
        "active_count": len(active_targets),
        "dropped_count": len(dropped_targets),
        "health_counts": health_counts,
        "active_targets": simplified_targets,
    }


def export_metric_json(payload: dict[str, Any], output_path: str | Path) -> None:
    """
    Save one raw Prometheus response.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def export_metric_csv(payload: dict[str, Any], output_path: str | Path) -> int:
    """
    Convert a Prometheus matrix response into a simple long-form CSV.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = _matrix_rows(payload)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["series_index", "labels", "timestamp", "datetime_utc", "value"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def plot_metric_png(
    payload: dict[str, Any],
    output_path: str | Path,
    title: str,
    ylabel: str = "",
    events: list[dict[str, Any]] | None = None,
) -> tuple[bool, str | None]:
    """
    Generate a PNG graph when matplotlib is installed.
    """
    try:
        from src.benchmark.local_deps import ensure_repo_local_dependencies

        ensure_repo_local_dependencies()
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False, "matplotlib is not installed"
    except Exception as exc:
        return False, f"matplotlib setup failed: {type(exc).__name__}: {exc}"

    series = _select_plot_series(_matrix_series(payload), max_series=8)
    if not series:
        return False, "no numeric samples to plot"

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(10.5, 6.2))
    for index, item in enumerate(series):
        label = _series_label(item["metric"], index)
        ax.plot(item["timestamps"], item["values"], label=label, linewidth=1.4)

    sample_times = [timestamp.timestamp() for item in series for timestamp in item["timestamps"]]
    event_markers = _selected_event_markers(events or [], sample_times)
    for index, marker in enumerate(event_markers, start=1):
        marker_dt = dt.datetime.fromtimestamp(marker["unix"], tz=dt.timezone.utc)
        ax.axvline(marker_dt, color="#111827", linewidth=1.0, alpha=0.65, linestyle="--")
        ax.text(
            marker_dt,
            0.98,
            str(index),
            transform=ax.get_xaxis_transform(),
            va="top",
            ha="center",
            fontsize=8,
            fontweight="bold",
            color="#111827",
            bbox={"boxstyle": "circle,pad=0.15", "facecolor": "white", "edgecolor": "#111827", "linewidth": 0.8},
        )

    ax.set_title(title)
    ax.set_xlabel("Time")
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(True, alpha=0.3)
    if len(series) <= 8:
        ax.legend(fontsize="small", loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=2, frameon=False)
    if event_markers:
        key = "  ".join(f"{index}. {_event_graph_label(marker)}" for index, marker in enumerate(event_markers, start=1))
        fig.text(0.08, 0.02, key, fontsize=8, color="#111827", ha="left", va="bottom", wrap=True)
    fig.autofmt_xdate()
    fig.tight_layout(rect=(0, 0.12, 1, 1))
    fig.savefig(path)
    plt.close(fig)
    return True, None


def plot_metric_svg(
    payload: dict[str, Any],
    output_path: str | Path,
    title: str,
    ylabel: str = "",
    events: list[dict[str, Any]] | None = None,
) -> tuple[bool, str | None]:
    """
    Generate a lightweight SVG graph without optional plotting dependencies.
    """
    series = _select_plot_series(_matrix_series(payload), max_series=8)
    if not series:
        return False, "no numeric samples to plot"

    plotted_series = series
    timestamps = [
        timestamp.timestamp()
        for item in plotted_series
        for timestamp in item["timestamps"]
    ]
    values = [value for item in plotted_series for value in item["values"]]
    if not timestamps or not values:
        return False, "no numeric samples to plot"

    event_markers = _selected_event_markers(events or [], timestamps)
    event_timestamps = [marker["unix"] for marker in event_markers]

    min_time = min([*timestamps, *event_timestamps])
    max_time = max([*timestamps, *event_timestamps])
    if min_time == max_time:
        max_time = min_time + 1.0

    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        padding = max(abs(min_value) * 0.05, 1.0)
        min_value -= padding
        max_value += padding

    width = 1100
    height = 680
    left = 96
    right = 42
    top = 94
    bottom = 230
    plot_width = width - left - right
    plot_height = height - top - bottom
    colors = (
        "#2563eb",
        "#dc2626",
        "#059669",
        "#d97706",
        "#7c3aed",
        "#0891b2",
        "#be123c",
        "#4b5563",
    )

    def x_pos(timestamp: float) -> float:
        return left + ((timestamp - min_time) / (max_time - min_time)) * plot_width

    def y_pos(value: float) -> float:
        return top + plot_height - ((value - min_value) / (max_value - min_value)) * plot_height

    def fmt(value: float) -> str:
        if abs(value) >= 1000:
            return f"{value:,.0f}"
        if abs(value) >= 10:
            return f"{value:.2f}"
        return f"{value:.3f}"

    def time_label(timestamp: float) -> str:
        return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).strftime("%H:%M:%S")

    subtitle = "Prometheus range samples for the benchmark window. Numbered dashed markers are keyed below the plot."
    svg_lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        "<style>",
        ".title{font:700 26px Arial,sans-serif;fill:#17202a}",
        ".subtitle{font:14px Arial,sans-serif;fill:#52606d}",
        ".axis{font:12px Arial,sans-serif;fill:#607080}",
        ".label{font:12px Arial,sans-serif;fill:#374151}",
        ".event-label{font:11px Arial,sans-serif;fill:#111827;font-weight:700}",
        ".event-dot{fill:#ffffff;stroke:#111827;stroke-width:1.2}",
        ".grid{stroke:#dfe7f0;stroke-width:1}",
        ".event-line{stroke:#111827;stroke-width:1.2;stroke-dasharray:5 4;opacity:.7}",
        ".frame{stroke:#9fb1c4;stroke-width:1.2;fill:none}",
        "</style>",
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<rect x="12" y="12" width="{w}" height="{h}" rx="14" fill="#ffffff" stroke="#d8e0ea"/>'.format(
            w=width - 24,
            h=height - 24,
        ),
        f'<text class="title" x="34" y="46">{html.escape(title)}</text>',
        f'<text class="subtitle" x="34" y="72">{html.escape(subtitle)}</text>',
        f'<line class="frame" x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}"/>',
        f'<line class="frame" x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}"/>',
    ]

    for tick in range(6):
        fraction = tick / 5
        value = min_value + (max_value - min_value) * (1 - fraction)
        y = top + plot_height * fraction
        svg_lines.extend(
            [
                f'<line class="grid" x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}"/>',
                f'<text class="axis" x="{left - 12}" y="{y + 4:.2f}" text-anchor="end">{html.escape(fmt(value))}</text>',
            ]
        )

    for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
        timestamp = min_time + (max_time - min_time) * fraction
        x = x_pos(timestamp)
        svg_lines.append(
            f'<text class="axis" x="{x:.2f}" y="{top + plot_height + 26}" text-anchor="middle">{html.escape(time_label(timestamp))}</text>'
        )

    if ylabel:
        svg_lines.append(
            f'<text class="axis" x="28" y="{top + plot_height / 2:.2f}" transform="rotate(-90 28 {top + plot_height / 2:.2f})" text-anchor="middle">{html.escape(ylabel)}</text>'
        )

    for index, marker in enumerate(event_markers, start=1):
        x = x_pos(marker["unix"])
        svg_lines.extend(
            [
                f'<line class="event-line" x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{top + plot_height}"/>',
                f'<circle class="event-dot" cx="{x:.2f}" cy="{top + 16}" r="9"/>',
                f'<text class="event-label" x="{x:.2f}" y="{top + 20}" text-anchor="middle">{index}</text>',
            ]
        )

    legend_x = left
    legend_y = top + plot_height + 62
    legend_col_width = 420

    for index, item in enumerate(plotted_series):
        points = [
            f"{x_pos(timestamp.timestamp()):.2f},{y_pos(value):.2f}"
            for timestamp, value in zip(item["timestamps"], item["values"])
        ]
        if not points:
            continue
        color = colors[index % len(colors)]
        label = html.escape(_series_label(item["metric"], index))
        svg_lines.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2.8" stroke-linejoin="round" stroke-linecap="round" points="{" ".join(points)}"/>'
        )
        last_x, last_y = points[-1].split(",", 1)
        svg_lines.append(
            f'<circle cx="{last_x}" cy="{last_y}" r="3.8" fill="{color}" stroke="#ffffff" stroke-width="1.4"/>'
        )
        row_y = legend_y + (index % 4) * 22
        row_x = legend_x + (index // 4) * legend_col_width
        svg_lines.extend(
            [
                f'<line x1="{row_x}" y1="{row_y}" x2="{row_x + 22}" y2="{row_y}" stroke="{color}" stroke-width="4" stroke-linecap="round"/>',
                f'<text class="label" x="{row_x + 30}" y="{row_y + 4}">{label}</text>',
            ]
        )

    if event_markers:
        key_y = legend_y + 108
        svg_lines.append(f'<text class="axis" x="{left}" y="{key_y}">Event key:</text>')
        for index, marker in enumerate(event_markers, start=1):
            row_y = key_y + 22 + ((index - 1) % 4) * 18
            row_x = left + ((index - 1) // 4) * 470
            svg_lines.append(
                f'<text class="axis" x="{row_x}" y="{row_y}">{index}. {html.escape(_event_graph_label(marker))}</text>'
            )

    svg_lines.append(
        f'<text class="axis" x="{left + plot_width / 2:.2f}" y="{top + plot_height + 30}" text-anchor="middle">Time in UTC across the collected benchmark window</text>'
    )
    svg_lines.append("</svg>")

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(svg_lines), encoding="utf-8")
    return True, None


def collect_monitoring_bundle(
    prometheus_url: str,
    output_dir: str | Path,
    start: float,
    end: float,
    step_sec: int | float = 5,
    skip_graphs: bool = False,
    timeout_sec: float = 10.0,
    artifact_prefix: str = "monitoring",
    node_roles_path: str | Path | None = None,
    events_path: str | Path | None = None,
    backend_id: str = "kafka",
) -> dict[str, Any]:
    """
    Collect local Prometheus range data, CSV exports, optional graphs, and summary.
    """
    output_path = Path(output_dir)
    raw_dir = output_path / "raw"
    csv_dir = output_path / "csv"
    graphs_dir = output_path / "graphs"
    for directory in (raw_dir, csv_dir, graphs_dir):
        directory.mkdir(parents=True, exist_ok=True)

    normalized_url = normalize_prometheus_url(prometheus_url)
    summary = _base_summary(
        prometheus_url=normalized_url,
        output_dir=output_path,
        start=start,
        end=end,
        step_sec=step_sec,
        artifact_prefix=artifact_prefix,
    )
    summary["skip_graphs"] = skip_graphs
    summary["backend_id"] = backend_id
    summary["node_roles"] = load_node_roles(node_roles_path)
    summary["events"] = load_benchmark_events(events_path)

    try:
        check_prometheus(normalized_url, timeout_sec=timeout_sec)
        metric_names = fetch_metric_names(normalized_url, timeout_sec=timeout_sec)
        targets = fetch_targets(normalized_url, timeout_sec=timeout_sec)
    except Exception as exc:
        summary["status"] = "skipped"
        summary["errors"].append(f"Prometheus unavailable: {type(exc).__name__}: {exc}")
        _write_summary(summary, output_path)
        return summary

    summary["targets"] = targets
    (raw_dir / "metric_names.json").write_text(
        json.dumps(sorted(metric_names), indent=2),
        encoding="utf-8",
    )
    export_metric_json({"status": "success", "data": targets}, raw_dir / "targets.json")

    for spec in metric_specs_for_backend(backend_id):
        if not _spec_is_available(spec, metric_names):
            summary["missing_metrics"].append(
                {
                    "id": spec.metric_id,
                    "title": spec.title,
                    "category": spec.category,
                    "query": spec.query,
                    "required_for_complete": spec.required_for_complete,
                    "reason": (
                        "none of the required metric names are currently known "
                        f"to Prometheus: {', '.join(spec.required_any)}"
                    ),
                }
            )
            continue

        raw_path = raw_dir / f"{spec.metric_id}.json"
        csv_path = csv_dir / f"{spec.metric_id}.csv"
        png_graph_path = graphs_dir / f"{spec.metric_id}.png"
        svg_graph_path = graphs_dir / f"{spec.metric_id}.svg"

        try:
            payload = query_range(
                prometheus_url=normalized_url,
                query=spec.query,
                start=start,
                end=end,
                step=step_sec,
                timeout_sec=timeout_sec,
            )
            export_metric_json(payload, raw_path)
            row_count = export_metric_csv(payload, csv_path)
            series_count = _matrix_series_count(payload)
            numeric_stats = _matrix_numeric_stats(payload)
            series_stats = _matrix_series_stats(payload)

            graph_record: dict[str, Any] | None = None
            if skip_graphs:
                graph_record = {
                    "metric_id": spec.metric_id,
                    "status": "skipped",
                    "reason": "graph generation disabled",
                }
            else:
                plotted, reason = plot_metric_png(
                    payload,
                    png_graph_path,
                    title=spec.title,
                    ylabel=spec.unit,
                    events=summary["events"],
                )
                if plotted:
                    graph_record = {
                        "metric_id": spec.metric_id,
                        "status": "generated",
                        "format": "png",
                        "path": _relative_artifact_path(artifact_prefix, "graphs", png_graph_path.name),
                    }
                    summary["graphs"].append(graph_record)
                else:
                    svg_plotted, svg_reason = plot_metric_svg(
                        payload,
                        svg_graph_path,
                        title=spec.title,
                        ylabel=spec.unit,
                        events=summary["events"],
                    )
                    if svg_plotted:
                        graph_record = {
                            "metric_id": spec.metric_id,
                            "status": "generated",
                            "format": "svg",
                            "path": _relative_artifact_path(artifact_prefix, "graphs", svg_graph_path.name),
                            "fallback_reason": reason,
                        }
                        summary["graphs"].append(graph_record)
                    else:
                        graph_record = {
                            "metric_id": spec.metric_id,
                            "status": "skipped",
                            "reason": svg_reason or reason or "graph generation skipped",
                        }

            collected = {
                "id": spec.metric_id,
                "title": spec.title,
                "category": spec.category,
                "query": spec.query,
                "unit": spec.unit,
                "series_count": series_count,
                "sample_count": row_count,
                "numeric_sample_count": numeric_stats["count"],
                "min_value": numeric_stats["min"],
                "avg_value": numeric_stats["avg"],
                "max_value": numeric_stats["max"],
                "series": series_stats,
                "raw_path": _relative_artifact_path(artifact_prefix, "raw", raw_path.name),
                "csv_path": _relative_artifact_path(artifact_prefix, "csv", csv_path.name),
                "graph": graph_record,
            }
            summary["collected_metrics"].append(collected)
        except Exception as exc:
            summary["status"] = "partial"
            summary["errors"].append(
                f"Failed to collect {spec.metric_id}: {type(exc).__name__}: {exc}"
            )
            summary["missing_metrics"].append(
                {
                    "id": spec.metric_id,
                    "title": spec.title,
                    "category": spec.category,
                    "query": spec.query,
                    "required_for_complete": spec.required_for_complete,
                    "reason": f"query failed: {type(exc).__name__}: {exc}",
                }
            )

    required_missing = [
        metric
        for metric in summary["missing_metrics"]
        if metric.get("required_for_complete", True)
    ]
    if summary["status"] == "completed" and required_missing:
        summary["status"] = "partial"

    _write_summary(summary, output_path)
    return summary


def load_node_roles(path: str | Path | None) -> dict[str, Any]:
    """
    Load node role metadata written by run_case.sh, if available.
    """
    if path is None:
        return {}
    node_roles_path = Path(path)
    if not node_roles_path.is_file():
        return {}
    try:
        data = json.loads(node_roles_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def load_benchmark_events(path: str | Path | None) -> list[dict[str, Any]]:
    """Load Slurm and MPI event markers for monitoring graphs."""
    if path is None:
        return []
    events_path = Path(path)
    if not events_path.is_file():
        return []
    try:
        data = json.loads(events_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    raw_events = data.get("events", []) if isinstance(data, dict) else []
    if not isinstance(raw_events, list):
        return []
    events: list[dict[str, Any]] = []
    for raw_event in raw_events:
        if not isinstance(raw_event, dict):
            continue
        try:
            unix_time = float(raw_event.get("unix", 0.0))
        except (TypeError, ValueError):
            continue
        if unix_time <= 0:
            continue
        events.append(
            {
                "name": str(raw_event.get("name", "event")),
                "label": str(raw_event.get("label", raw_event.get("name", "event"))),
                "source": str(raw_event.get("source", "unknown")),
                "unix": unix_time,
                "iso": raw_event.get("iso") or _timestamp_to_iso(unix_time),
            }
        )
    return sorted(events, key=lambda item: item["unix"])


def write_unavailable_summary(
    prometheus_url: str,
    output_dir: str | Path,
    reason: str,
    start: float | None = None,
    end: float | None = None,
    step_sec: int | float = 5,
    artifact_prefix: str = "monitoring",
) -> dict[str, Any]:
    """
    Write a skipped monitoring summary when monitoring is optional.
    """
    now = time.time()
    summary = _base_summary(
        prometheus_url=normalize_prometheus_url(prometheus_url),
        output_dir=Path(output_dir),
        start=start if start is not None else now,
        end=end if end is not None else now,
        step_sec=step_sec,
        artifact_prefix=artifact_prefix,
    )
    summary["status"] = "skipped"
    summary["errors"].append(reason)
    _write_summary(summary, Path(output_dir))
    return summary


def _base_summary(
    prometheus_url: str,
    output_dir: Path,
    start: float,
    end: float,
    step_sec: int | float,
    artifact_prefix: str,
) -> dict[str, Any]:
    return {
        "format": "monitoring_bundle.v1",
        "enabled": True,
        "status": "completed",
        "prometheus_url": prometheus_url,
        "created_at": _timestamp_to_iso(time.time()),
        "time_window": {
            "start_unix": start,
            "end_unix": end,
            "start": _timestamp_to_iso(start),
            "end": _timestamp_to_iso(end),
            "step_sec": step_sec,
        },
        "summary_path": _relative_artifact_path(artifact_prefix, "monitoring_summary.json"),
        "raw_dir": _relative_artifact_path(artifact_prefix, "raw"),
        "csv_dir": _relative_artifact_path(artifact_prefix, "csv"),
        "graphs_dir": _relative_artifact_path(artifact_prefix, "graphs"),
        "output_dir": artifact_prefix.strip("/") or ".",
        "targets": {},
        "collected_metrics": [],
        "missing_metrics": [],
        "graphs": [],
        "errors": [],
    }


def _write_summary(summary: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "monitoring_summary.json"
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)


def _spec_is_available(spec: MetricSpec, metric_names: set[str]) -> bool:
    return any(metric_name in metric_names for metric_name in spec.required_any)


def _matrix_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    data = payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    if not isinstance(result, list):
        return rows

    for series_index, series in enumerate(result):
        if not isinstance(series, dict):
            continue
        metric = series.get("metric", {})
        values = series.get("values", [])
        if not isinstance(values, list):
            continue
        labels = json.dumps(metric, sort_keys=True)
        for sample in values:
            if not isinstance(sample, list) or len(sample) != 2:
                continue
            timestamp = float(sample[0])
            value = sample[1]
            rows.append(
                {
                    "series_index": series_index,
                    "labels": labels,
                    "timestamp": f"{timestamp:.3f}",
                    "datetime_utc": _timestamp_to_iso(timestamp),
                    "value": value,
                }
            )
    return rows


def _matrix_series(payload: dict[str, Any]) -> list[dict[str, Any]]:
    parsed: list[dict[str, Any]] = []
    data = payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    if not isinstance(result, list):
        return parsed

    for series in result[:12]:
        if not isinstance(series, dict):
            continue
        values = series.get("values", [])
        if not isinstance(values, list):
            continue
        timestamps: list[dt.datetime] = []
        numeric_values: list[float] = []
        for sample in values:
            if not isinstance(sample, list) or len(sample) != 2:
                continue
            try:
                timestamp = float(sample[0])
                value = float(sample[1])
            except (TypeError, ValueError):
                continue
            timestamps.append(dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc))
            numeric_values.append(value)
        if timestamps:
            parsed.append(
                {
                    "metric": series.get("metric", {}),
                    "timestamps": timestamps,
                    "values": numeric_values,
                }
            )
    return parsed


def _matrix_series_count(payload: dict[str, Any]) -> int:
    data = payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    return len(result) if isinstance(result, list) else 0


def _matrix_numeric_stats(payload: dict[str, Any]) -> dict[str, float | int | None]:
    values: list[float] = []
    data = payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    if not isinstance(result, list):
        return {"count": 0, "min": None, "avg": None, "max": None}

    for series in result:
        if not isinstance(series, dict):
            continue
        samples = series.get("values", [])
        if not isinstance(samples, list):
            continue
        for sample in samples:
            if not isinstance(sample, list) or len(sample) != 2:
                continue
            try:
                values.append(float(sample[1]))
            except (TypeError, ValueError):
                continue

    if not values:
        return {"count": 0, "min": None, "avg": None, "max": None}

    return {
        "count": len(values),
        "min": min(values),
        "avg": sum(values) / len(values),
        "max": max(values),
    }


def _matrix_series_stats(payload: dict[str, Any]) -> list[dict[str, Any]]:
    series_stats: list[dict[str, Any]] = []
    data = payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    if not isinstance(result, list):
        return series_stats

    for index, series in enumerate(result):
        if not isinstance(series, dict):
            continue
        metric = series.get("metric", {})
        if not isinstance(metric, dict):
            metric = {}
        values: list[float] = []
        first_value: float | None = None
        last_value: float | None = None
        samples = series.get("values", [])
        if not isinstance(samples, list):
            samples = []
        for sample in samples:
            if not isinstance(sample, list) or len(sample) != 2:
                continue
            try:
                numeric = float(sample[1])
            except (TypeError, ValueError):
                continue
            if first_value is None:
                first_value = numeric
            last_value = numeric
            values.append(numeric)

        if values:
            minimum = min(values)
            maximum = max(values)
            average = sum(values) / len(values)
        else:
            minimum = maximum = average = None

        series_stats.append(
            {
                "series_index": index,
                "labels": metric,
                "sample_count": len(samples),
                "numeric_sample_count": len(values),
                "first_value": first_value,
                "last_value": last_value,
                "min_value": minimum,
                "avg_value": average,
                "max_value": maximum,
            }
        )
    return series_stats


def _series_label(metric: dict[str, Any], fallback_index: int) -> str:
    if not metric:
        return f"series {fallback_index}"
    node = metric.get("node")
    role = metric.get("role")
    if node and role:
        return f"{node} {role}"
    if node:
        return str(node)
    topic = metric.get("topic")
    partition = metric.get("partition")
    if topic and partition is not None:
        return f"{topic} p{partition}"
    for key in ("topic", "instance", "job", "device", "partition"):
        if key in metric:
            return str(metric[key])
    return f"series {fallback_index}"


def _select_plot_series(series: list[dict[str, Any]], max_series: int) -> list[dict[str, Any]]:
    if len(series) <= max_series:
        return series

    def series_score(item: dict[str, Any]) -> float:
        values = item.get("values")
        if not isinstance(values, list) or not values:
            return 0.0
        numeric = []
        for value in values:
            try:
                numeric.append(abs(float(value)))
            except (TypeError, ValueError):
                continue
        return max(numeric, default=0.0)

    return sorted(series, key=series_score, reverse=True)[:max_series]


IMPORTANT_EVENT_NAMES = (
    "kafka_start_requested",
    "kafka_ready",
    "mpi_launch",
    "consumer_start",
    "producer_start",
    "load_pressure_start",
    "producer_send_loop_end",
    "workload_end",
)


def _selected_event_markers(
    events: list[dict[str, Any]],
    sample_timestamps: list[float],
    max_events: int = 8,
) -> list[dict[str, Any]]:
    candidates = _events_for_window(events, sample_timestamps)
    if len(candidates) <= max_events:
        return candidates

    by_name = {str(event.get("name", "")): event for event in candidates}
    selected = [
        by_name[name]
        for name in IMPORTANT_EVENT_NAMES
        if name in by_name
    ]
    for event in candidates:
        if len(selected) >= max_events:
            break
        if event not in selected:
            selected.append(event)
    return sorted(selected[:max_events], key=lambda item: float(item.get("unix", 0.0)))


def _events_for_window(
    events: list[dict[str, Any]],
    sample_timestamps: list[float],
    pad_sec: float = 300.0,
) -> list[dict[str, Any]]:
    if not events or not sample_timestamps:
        return []
    min_sample = min(sample_timestamps)
    max_sample = max(sample_timestamps)
    selected: list[dict[str, Any]] = []
    for event in events:
        try:
            unix_time = float(event.get("unix", 0.0))
        except (TypeError, ValueError):
            continue
        if min_sample - pad_sec <= unix_time <= max_sample + pad_sec:
            copied = dict(event)
            copied["unix"] = unix_time
            selected.append(copied)
    return selected


def _event_graph_label(event: dict[str, Any]) -> str:
    label = str(event.get("label") or event.get("name") or "event")
    label = label.replace("Benchmark ", "")
    label = label.replace(" requested", "")
    if len(label) > 24:
        label = label[:21].rstrip() + "..."
    timestamp = dt.datetime.fromtimestamp(
        float(event.get("unix", 0.0)),
        tz=dt.timezone.utc,
    ).strftime("%H:%M:%S")
    return f"{label} {timestamp}"


def _relative_artifact_path(prefix: str, *parts: str) -> str:
    clean_prefix = prefix.strip("/")
    clean_parts = [part.strip("/") for part in parts if part]
    if clean_prefix in {"", "."}:
        return "/".join(clean_parts)
    return "/".join([clean_prefix, *clean_parts])


def _timestamp_to_iso(timestamp: float) -> str:
    return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect Prometheus monitoring data as JSON, CSV, and graphs"
    )
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start", type=float, default=None)
    parser.add_argument("--end", type=float, default=None)
    parser.add_argument("--lookback-sec", type=float, default=60.0)
    parser.add_argument("--step-sec", type=float, default=5.0)
    parser.add_argument("--timeout-sec", type=float, default=10.0)
    parser.add_argument("--skip-graphs", action="store_true")
    parser.add_argument("--artifact-prefix", default="monitoring")
    parser.add_argument("--node-roles", default=None)
    parser.add_argument("--events", default=None)
    parser.add_argument("--backend-id", choices=("kafka", "pulsar"), default="kafka")
    parser.add_argument("--check-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.check_only:
        check_prometheus(args.prometheus_url, timeout_sec=args.timeout_sec)
        print(f"Prometheus reachable at {normalize_prometheus_url(args.prometheus_url)}")
        return

    end = args.end if args.end is not None else time.time()
    start = args.start if args.start is not None else end - args.lookback_sec
    summary = collect_monitoring_bundle(
        prometheus_url=args.prometheus_url,
        output_dir=args.output_dir,
        start=start,
        end=end,
        step_sec=args.step_sec,
        skip_graphs=args.skip_graphs,
        timeout_sec=args.timeout_sec,
        artifact_prefix=args.artifact_prefix,
        node_roles_path=args.node_roles,
        events_path=args.events,
        backend_id=args.backend_id,
    )
    print(json.dumps({"status": summary["status"], "output_dir": args.output_dir}, indent=2))


if __name__ == "__main__":
    main()
