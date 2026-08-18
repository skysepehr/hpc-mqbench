from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig
from src.benchmark.backends.base import BackendAdapter, BackendResourcePlan
from src.benchmark.monitoring_summary import (
    build_kafka_broker_throughput_markdown_section,
    build_monitoring_markdown_section,
    extract_kafka_broker_throughput,
)
from src.benchmark.qualification import (
    COMMON_QUALIFICATION_POLICY_ID,
    KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID,
    backlog_denominator_for_policy,
    producer_operational_metrics,
)


class KafkaBackendAdapter(BackendAdapter):
    backend_id = "kafka"
    adapter_version = "1"
    qualification_policy_ids = frozenset(
        {
            KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID,
            COMMON_QUALIFICATION_POLICY_ID,
        }
    )
    supports_full_report = True

    _VERSIONED_FIELDS = frozenset(
        {
            "broker_count",
            "partitions",
            "replication_factor",
            "topic_name",
            "acks",
            "compression_type",
            "batch_size",
            "linger_ms",
            "broker_profile_id",
            "broker_profile_sha256",
            "extra",
        }
    )

    _LIFECYCLE_SCRIPTS = {
        "preflight": "scripts/hpc_preflight.sh",
        "start": "scripts/start_brokers.sh",
        "wait-ready": "scripts/wait_for_brokers.sh",
        "check-health": "scripts/check_broker_health.sh",
        "create-stream": "scripts/create_topics.sh",
        "prefill": "scripts/prefill_topic.sh",
        "start-monitoring": "scripts/start_broker_process_monitor.sh",
        "start-metrics-exporter": "scripts/start_kafka_exporter.sh",
        "stop-monitoring": "scripts/stop_broker_process_monitor.sh",
        "stop-metrics-exporter": "scripts/stop_kafka_exporter.sh",
        "stop": "scripts/stop_brokers.sh",
    }

    def versioned_config_fields(self) -> frozenset[str]:
        return self._VERSIONED_FIELDS

    def normalize_versioned_config(
        self,
        backend_config: dict[str, Any],
    ) -> dict[str, Any]:
        return deepcopy(backend_config)

    def versioned_config_from_runtime(
        self,
        flat_config: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            key: deepcopy(flat_config[key])
            for key in self._VERSIONED_FIELDS
            if key in flat_config
        }

    def resource_plan(self, config: BenchmarkConfig) -> BackendResourcePlan:
        return BackendResourcePlan(
            service_nodes=config.broker_count,
            monitoring_nodes=1,
            producer_ranks=config.producer_ranks,
            consumer_ranks=config.consumer_ranks,
        )

    def runtime_environment(self, config: BenchmarkConfig) -> dict[str, str]:
        environment = super().runtime_environment(config)
        environment.update(
            {
                "BROKER_COUNT": str(config.broker_count),
                "PARTITIONS": str(config.partitions),
                "REPLICATION_FACTOR": str(config.replication_factor),
                "TOPIC_NAME": config.topic_name,
            }
        )
        return environment

    def uses_coordinated_consumer_drain(self, config: BenchmarkConfig) -> bool:
        """Keep Kafka consumers active through producer flush and final drain."""
        return config.scenario == "simultaneous"

    def validate_config(self, config: BenchmarkConfig) -> None:
        if config.backend_id != self.backend_id:
            raise ValueError(
                f"Kafka adapter received backend_id={config.backend_id!r}"
            )
        if config.qualification_policy_id not in self.qualification_policy_ids:
            raise ValueError(
                "Kafka adapter supports qualification policies "
                f"{sorted(self.qualification_policy_ids)}"
            )
        if config.broker_count != 1:
            raise ValueError("The current Kafka adapter supports one broker")

    def effective_settings(self, config: BenchmarkConfig) -> dict[str, Any]:
        return {
            "broker_count": config.broker_count,
            "partitions": config.partitions,
            "replication_factor": config.replication_factor,
            "topic_name": config.topic_name,
            "acks": config.acks,
            "compression_type": config.compression_type,
            "batch_size": config.batch_size,
            "linger_ms": config.linger_ms,
            "profile_id": config.broker_profile_id,
            "profile_sha256": config.broker_profile_sha256,
            "extra": deepcopy(config.extra),
        }

    def create_producer_worker(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        producer_index: int,
        clock_offset_ns: int,
    ) -> Any:
        from src.benchmark.backends.kafka.workers import ProducerWorker

        return ProducerWorker(
            config=config,
            rank=rank,
            bootstrap_servers=bootstrap_servers,
            producer_index=producer_index,
            clock_offset_ns=clock_offset_ns,
        )

    def create_consumer_worker(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        case_id: str,
        clock_offset_ns: int,
    ) -> Any:
        from src.benchmark.backends.kafka.workers import ConsumerWorker

        return ConsumerWorker(
            config=config,
            rank=rank,
            bootstrap_servers=bootstrap_servers,
            case_id=case_id,
            clock_offset_ns=clock_offset_ns,
        )

    def lifecycle_script(self, action: str, project_root: Path) -> Path | None:
        relative_path = self._LIFECYCLE_SCRIPTS.get(action)
        return project_root / relative_path if relative_path else None

    def normalize_metrics(
        self,
        benchmark_result: dict[str, Any],
    ) -> dict[str, Any]:
        monitoring = benchmark_result.get("monitoring", {})
        if not isinstance(monitoring, dict):
            monitoring = {}
        return {
            "broker_throughput": deepcopy(
                benchmark_result.get("kafka_broker_throughput", {})
            ),
            "monitoring": deepcopy(monitoring),
            "librdkafka": _librdkafka_metrics(
                benchmark_result.get("aggregated_metrics", {})
            ),
        }

    def prepare_result(self, benchmark_result: dict[str, Any]) -> None:
        benchmark_result["kafka_broker_throughput"] = (
            extract_kafka_broker_throughput(
                benchmark_result.get("monitoring", {})
            )
        )

    def monitoring_endpoint_specs(
        self,
        config: BenchmarkConfig,
    ) -> dict[str, Any]:
        return {
            "bootstrap_servers": {
                "runtime_file": "runtime/bootstrap_servers.txt",
                "default_port": 9092,
            },
            "prometheus": {
                "runtime_file": "monitoring/prometheus_endpoint.txt",
                "default_port": 9090,
            },
            "jmx_exporter": {
                "default_port_base": 7101,
                "instances": config.broker_count,
            },
            "kafka_exporter": {
                "runtime_file": (
                    "monitoring/kafka_exporter/kafka_exporter_endpoint.txt"
                ),
                "default_port": 9308,
            },
        }

    def configuration_report_lines(
        self,
        config: dict[str, Any],
    ) -> list[str]:
        lines = [
            f"- **Mode:** {config.get('mode', 'unknown')}",
            f"- **Scenario:** {config.get('scenario', 'unknown')}",
            f"- **Broker Count:** {config.get('broker_count', 'unknown')}",
            f"- **Topic Name:** {config.get('topic_name', 'unknown')}",
            f"- **Partitions:** {config.get('partitions', 'unknown')}",
            (
                "- **Replication Factor:** "
                f"{config.get('replication_factor', 'unknown')}"
            ),
            f"- **Acks:** {config.get('acks', 'unknown')}",
            f"- **Compression Type:** {config.get('compression_type', 'unknown')}",
            f"- **Batch Size:** {config.get('batch_size', 'unknown')}",
            f"- **Linger (ms):** {config.get('linger_ms', 'unknown')}",
            (
                "- **Payload Size (bytes):** "
                f"{config.get('payload_size_bytes', 'unknown')}"
            ),
            f"- **Payload Mode:** {config.get('payload_mode', 'unknown')}",
            f"- **Send Pattern:** {config.get('send_pattern', 'unknown')}",
            f"- **Producer Ranks:** {config.get('producer_ranks', 'unknown')}",
            f"- **Consumer Ranks:** {config.get('consumer_ranks', 'unknown')}",
            (
                "- **Virtual Devices per Rank:** "
                f"{config.get('virtual_devices_per_rank', 'unknown')}"
            ),
            (
                "- **Total Simulated Devices:** "
                f"{config.get('total_simulated_devices', 'unknown')}"
            ),
            (
                "- **Real Kafka Benchmark Clients:** "
                f"{config.get('benchmark_kafka_client_count', _client_count(config))}"
            ),
            (
                "- **Estimated Max Client-Broker Connections:** "
                f"{config.get('estimated_max_client_broker_connections', _connection_count(config))}"
            ),
            f"- **Duration (sec):** {config.get('duration_sec', 'unknown')}",
        ]
        if (
            config.get("warmup_sec", 0)
            or config.get("drain_timeout_sec", 0)
            or config.get("latency_enabled", False)
        ):
            lines.extend(
                [
                    f"- **Warm-up (sec):** {config.get('warmup_sec', 0)}",
                    (
                        "- **Drain timeout (sec):** "
                        f"{config.get('drain_timeout_sec', 0)}"
                    ),
                    (
                        "- **Aggregate target (records/s):** "
                        f"{config.get('target_records_per_sec') or 'maximum load'}"
                    ),
                    (
                        "- **End-to-end latency enabled:** "
                        f"{config.get('latency_enabled', False)}"
                    ),
                    (
                        "- **Latency sampling:** 1-in-"
                        f"{config.get('latency_sample_every', 1)}"
                    ),
                    (
                        "- **Broker profile:** "
                        f"{config.get('broker_profile_id', 'N/A')}"
                    ),
                    (
                        "- **Broker profile SHA-256:** "
                        f"`{config.get('broker_profile_sha256', 'N/A')}`"
                    ),
                ]
            )
        return lines

    def scale_report_lines(self, config: dict[str, Any]) -> list[str]:
        return [
            "| Quantity | Value | Meaning |",
            "|---|---:|---|",
            (
                "| Simulated logical devices | {devices} | Logical device IDs "
                "rotated through generated Kafka messages. |"
            ).format(devices=config.get("total_simulated_devices", "unknown")),
            (
                "| Real Kafka producer clients | {producers} | One "
                "`confluent_kafka.Producer` per producer MPI rank. |"
            ).format(producers=config.get("producer_ranks", "unknown")),
            (
                "| Real Kafka consumer clients | {consumers} | One "
                "`confluent_kafka.Consumer` per consumer MPI rank. |"
            ).format(consumers=config.get("consumer_ranks", "unknown")),
            (
                "| Total benchmark Kafka clients | {clients} | Producer plus "
                "consumer client objects created by this benchmark. |"
            ).format(
                clients=config.get(
                    "benchmark_kafka_client_count",
                    _client_count(config),
                )
            ),
            (
                "| Estimated max client-broker connections | {connections} | "
                "Coarse upper bound: benchmark clients multiplied by broker count. |"
            ).format(
                connections=config.get(
                    "estimated_max_client_broker_connections",
                    _connection_count(config),
                )
            ),
            "",
            (
                "Simulated logical devices are message identities, not one Kafka "
                "TCP connection per device."
            ),
        ]

    def primary_report_markdown(
        self,
        benchmark_result: dict[str, Any],
    ) -> str:
        return build_kafka_broker_throughput_markdown_section(
            benchmark_result.get("kafka_broker_throughput", {})
        )

    def monitoring_report_markdown(
        self,
        monitoring: dict[str, Any],
    ) -> str:
        return build_monitoring_markdown_section(monitoring)

    def report_notes(self) -> tuple[str, ...]:
        return (
            (
                "System-level monitoring data such as Kafka broker metrics, CPU, "
                "memory, disk, and network is collected separately when Prometheus "
                "monitoring is enabled. Local monitoring runs write `monitoring/`, "
                "while Slurm snapshot collection writes "
                "`data/monitoring_snapshot.json` and "
                "`reports/monitoring_summary.md`."
            ),
        )

    def latency_report_notes(self) -> tuple[str, ...]:
        return (
            (
                "librdkafka broker RTT remains a separate producer-to-broker "
                "client metric; it is not substituted for producer-to-consumer "
                "end-to-end latency."
            ),
        )

    def producer_enqueue_label(self) -> str:
        return "Messages Enqueued to librdkafka"

    def qualification_result(
        self,
        benchmark_result: dict[str, Any],
    ) -> bool | None:
        config = benchmark_result.get("config")
        aggregated = benchmark_result.get("aggregated_metrics")
        if not isinstance(config, dict) or not isinstance(aggregated, dict):
            return None
        producers = aggregated.get("producers")
        if not isinstance(producers, dict) or not producers:
            return None
        policy_id = str(
            config.get("qualification_policy_id")
            or KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID
        )
        metrics = producer_operational_metrics(
            producers,
            backlog_denominator=backlog_denominator_for_policy(policy_id),
        )
        return bool(metrics["thresholds_satisfied"])


def _librdkafka_metrics(aggregated: Any) -> dict[str, Any]:
    if not isinstance(aggregated, dict):
        return {}
    result: dict[str, Any] = {}
    for role_name in ("producers", "consumers"):
        role_metrics = aggregated.get(role_name, {})
        if not isinstance(role_metrics, dict):
            continue
        selected = {
            key: deepcopy(value)
            for key, value in role_metrics.items()
            if "librdkafka" in key or key.startswith("client_stats")
        }
        if selected:
            result[role_name] = selected
    return result


def _client_count(config: dict[str, Any]) -> int:
    return int(config.get("producer_ranks", 0) or 0) + int(
        config.get("consumer_ranks", 0) or 0
    )


def _connection_count(config: dict[str, Any]) -> int:
    return _client_count(config) * int(config.get("broker_count", 1) or 1)
