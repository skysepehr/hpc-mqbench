from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig
from src.benchmark.backends.base import BackendAdapter, BackendResourcePlan
from src.benchmark.qualification import (
    COMMON_QUALIFICATION_POLICY_ID,
    PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID,
    backlog_denominator_for_policy,
    producer_operational_metrics,
)


class PulsarBackendAdapter(BackendAdapter):
    """Single-service-node Pulsar implementation of the backend contract."""

    backend_id = "pulsar"
    adapter_version = "1"
    qualification_policy_ids = frozenset(
        {
            PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID,
            COMMON_QUALIFICATION_POLICY_ID,
        }
    )

    _VERSIONED_FIELDS = frozenset(
        {
            "service_count",
            "service_mode",
            "topic_name",
            "partitions",
            "subscription_name",
            "subscription_type",
            "profile_id",
            "profile_sha256",
            "producer",
            "consumer",
            "managed_ledger",
            "standalone_properties",
            "runtime",
        }
    )
    _LIFECYCLE_SCRIPTS = {
        "preflight": "scripts/backends/pulsar/preflight.sh",
        "start": "scripts/backends/pulsar/start.sh",
        "wait-ready": "scripts/backends/pulsar/wait_ready.sh",
        "check-health": "scripts/backends/pulsar/check_health.sh",
        "create-stream": "scripts/backends/pulsar/create_stream.sh",
        "delete-stream": "scripts/backends/pulsar/delete_stream.sh",
        "prefill": "scripts/backends/pulsar/prefill.sh",
        "start-monitoring": "scripts/backends/pulsar/start_process_monitor.sh",
        "start-metrics-exporter": "scripts/backends/pulsar/metrics_noop.sh",
        "stop-monitoring": "scripts/backends/pulsar/stop_process_monitor.sh",
        "stop-metrics-exporter": "scripts/backends/pulsar/metrics_noop.sh",
        "stop": "scripts/backends/pulsar/stop.sh",
    }

    def versioned_config_fields(self) -> frozenset[str]:
        return self._VERSIONED_FIELDS

    def normalize_versioned_config(
        self,
        backend_config: dict[str, Any],
    ) -> dict[str, Any]:
        return {"backend_settings": deepcopy(backend_config)}

    def versioned_config_from_runtime(
        self,
        flat_config: dict[str, Any],
    ) -> dict[str, Any]:
        settings = flat_config.get("backend_settings", {})
        if not isinstance(settings, dict):
            raise ValueError("backend_settings must be a dictionary")
        return deepcopy(settings)

    def validate_config(self, config: BenchmarkConfig) -> None:
        if config.backend_id != self.backend_id:
            raise ValueError(
                f"Pulsar adapter received backend_id={config.backend_id!r}"
            )
        if config.qualification_policy_id not in self.qualification_policy_ids:
            raise ValueError(
                "Pulsar adapter supports qualification policies "
                f"{sorted(self.qualification_policy_ids)}"
            )
        settings = config.backend_settings
        if not isinstance(settings, dict):
            raise ValueError("backend.pulsar must be an object")
        unknown = sorted(set(settings) - self._VERSIONED_FIELDS)
        if unknown:
            raise ValueError(
                "Unsupported backend.pulsar field(s): " + ", ".join(unknown)
            )
        if _positive_int(settings, "service_count") != 1:
            raise ValueError("The current Pulsar adapter supports one service node")
        if str(settings.get("service_mode", "")).strip() != "standalone":
            raise ValueError("Pulsar service_mode must be 'standalone'")
        if config.scenario != "simultaneous":
            raise ValueError(
                "The initial Pulsar adapter supports scenario='simultaneous' only"
            )
        topic_name = _required_text(settings, "topic_name")
        if not topic_name.startswith("persistent://"):
            raise ValueError("Pulsar topic_name must be a persistent:// topic URI")
        _positive_int(settings, "partitions")
        _required_text(settings, "subscription_name")
        if str(settings.get("subscription_type", "")).strip().lower() != "shared":
            raise ValueError("Pulsar subscription_type must be 'shared'")
        _validate_profile(settings)
        producer = _required_object(settings, "producer")
        consumer = _required_object(settings, "consumer")
        ledger = _required_object(settings, "managed_ledger")
        standalone_properties = _required_object(
            settings,
            "standalone_properties",
        )
        runtime = _required_object(settings, "runtime")
        _validate_producer(producer)
        _validate_consumer(consumer)
        for name in ("ensemble_size", "write_quorum", "ack_quorum"):
            if _positive_int(ledger, name) != 1:
                raise ValueError(f"Pulsar managed_ledger.{name} must be 1")
        _validate_standalone_properties(standalone_properties)
        _required_text(runtime, "product_version")
        _required_text(runtime, "jvm_memory")

    def effective_settings(self, config: BenchmarkConfig) -> dict[str, Any]:
        return deepcopy(config.backend_settings)

    def resource_plan(self, config: BenchmarkConfig) -> BackendResourcePlan:
        settings = config.backend_settings
        return BackendResourcePlan(
            service_nodes=int(settings.get("service_count", 1)),
            monitoring_nodes=1,
            producer_ranks=config.producer_ranks,
            consumer_ranks=config.consumer_ranks,
        )

    def runtime_environment(self, config: BenchmarkConfig) -> dict[str, str]:
        settings = config.backend_settings
        environment = super().runtime_environment(config)
        environment.update(
            {
                "PULSAR_SERVICE_COUNT": str(settings["service_count"]),
                "BROKER_COUNT": str(settings["service_count"]),
                "PULSAR_TOPIC_NAME": str(settings["topic_name"]),
                "PARTITIONS": str(settings["partitions"]),
                "TOPIC_NAME": str(settings["topic_name"]),
                "REPLICATION_FACTOR": "1",
                "PULSAR_SUBSCRIPTION_NAME": str(settings["subscription_name"]),
                "PULSAR_PROFILE_ID": str(settings["profile_id"]),
                "PULSAR_PROFILE_SHA256": str(settings["profile_sha256"]),
                "PULSAR_PRODUCT_VERSION": str(
                    settings["runtime"]["product_version"]
                ),
                "PULSAR_MEM": str(settings["runtime"]["jvm_memory"]),
            }
        )
        return environment

    def uses_coordinated_consumer_drain(self, config: BenchmarkConfig) -> bool:
        """Keep Pulsar consumers active through producer flush and final drain."""
        return config.scenario == "simultaneous"

    def profile_identity(
        self,
        config: BenchmarkConfig,
    ) -> tuple[str | None, str | None]:
        settings = config.backend_settings
        return str(settings.get("profile_id")), str(settings.get("profile_sha256"))

    def identity(self, config: BenchmarkConfig) -> dict[str, Any]:
        identity = super().identity(config)
        identity["product_version"] = str(
            config.backend_settings.get("runtime", {}).get(
                "product_version",
                "not_recorded",
            )
        )
        return identity

    def identity_from_dict(self, config: dict[str, Any]) -> dict[str, Any]:
        settings = config.get("backend_settings", {})
        if not isinstance(settings, dict):
            settings = {}
        runtime = settings.get("runtime", {})
        if not isinstance(runtime, dict):
            runtime = {}
        return {
            "backend_id": self.backend_id,
            "adapter_version": self.adapter_version,
            "product_version": str(runtime.get("product_version", "not_recorded")),
            "profile_id": settings.get("profile_id"),
            "profile_sha256": settings.get("profile_sha256"),
        }

    def create_producer_worker(self, **kwargs: Any) -> Any:
        from src.benchmark.backends.pulsar.workers import PulsarProducerWorker

        return PulsarProducerWorker(**kwargs)

    def create_consumer_worker(self, **kwargs: Any) -> Any:
        from src.benchmark.backends.pulsar.workers import PulsarConsumerWorker

        return PulsarConsumerWorker(**kwargs)

    def lifecycle_script(self, action: str, project_root: Path) -> Path | None:
        relative_path = self._LIFECYCLE_SCRIPTS.get(action)
        return project_root / relative_path if relative_path else None

    def monitoring_endpoint_specs(
        self,
        config: BenchmarkConfig,
    ) -> dict[str, Any]:
        return {
            "service_url": {
                "runtime_file": "runtime/bootstrap_servers.txt",
                "default_port": 6650,
            },
            "admin_url": {
                "runtime_file": "runtime/pulsar_admin_url.txt",
                "default_port": 8080,
            },
            "prometheus": {
                "runtime_file": "runtime/monitoring/prometheus_endpoint.txt",
                "default_port": 9090,
            },
            "pulsar_metrics": {
                "runtime_file": "runtime/pulsar_metrics_endpoint.txt",
                "path": "/metrics/",
            },
        }

    def normalize_metrics(
        self,
        benchmark_result: dict[str, Any],
    ) -> dict[str, Any]:
        monitoring = benchmark_result.get("monitoring", {})
        return {
            "monitoring": deepcopy(monitoring) if isinstance(monitoring, dict) else {},
            "client": {
                "producer": "pulsar-client",
                "consumer": "pulsar-client Shared subscription",
            },
        }

    def qualification_result(
        self,
        benchmark_result: dict[str, Any],
    ) -> bool | None:
        aggregated = benchmark_result.get("aggregated_metrics")
        if not isinstance(aggregated, dict):
            return None
        producers = aggregated.get("producers")
        if not isinstance(producers, dict):
            return None
        config = benchmark_result.get("config")
        policy_id = (
            str(config.get("qualification_policy_id"))
            if isinstance(config, dict)
            else PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID
        )
        return bool(
            producer_operational_metrics(
                producers,
                backlog_denominator=backlog_denominator_for_policy(policy_id),
            )["thresholds_satisfied"]
        )

    def configuration_report_lines(
        self,
        config: dict[str, Any],
    ) -> list[str]:
        settings = config.get("backend_settings", {})
        producer = settings.get("producer", {})
        consumer = settings.get("consumer", {})
        ledger = settings.get("managed_ledger", {})
        runtime = settings.get("runtime", {})
        return [
            "- **Backend ID:** pulsar",
            f"- **Mode:** {config.get('mode', 'unknown')}",
            f"- **Scenario:** {config.get('scenario', 'unknown')}",
            f"- **Service count:** {settings.get('service_count', 'unknown')}",
            f"- **Service mode:** {settings.get('service_mode', 'unknown')}",
            f"- **Topic:** {settings.get('topic_name', 'unknown')}",
            f"- **Partitions:** {settings.get('partitions', 'unknown')}",
            (
                "- **Subscription name:** "
                f"{settings.get('subscription_name', 'unknown')}"
            ),
            f"- **Subscription type:** {settings.get('subscription_type', 'unknown')}",
            f"- **Payload size (bytes):** {config.get('payload_size_bytes', 'unknown')}",
            f"- **Payload mode:** {config.get('payload_mode', 'unknown')}",
            f"- **Send pattern:** {config.get('send_pattern', 'unknown')}",
            f"- **Producer ranks:** {config.get('producer_ranks', 'unknown')}",
            f"- **Consumer ranks:** {config.get('consumer_ranks', 'unknown')}",
            (
                "- **Virtual devices per producer rank:** "
                f"{config.get('virtual_devices_per_rank', 'unknown')}"
            ),
            (
                "- **Total simulated logical devices:** "
                f"{config.get('total_simulated_devices', 'unknown')}"
            ),
            (
                "- **Measurement duration (sec):** "
                f"{config.get('duration_sec', 'unknown')}"
            ),
            f"- **Warm-up (sec):** {config.get('warmup_sec', 0)}",
            f"- **Drain timeout (sec):** {config.get('drain_timeout_sec', 0)}",
            (
                "- **Aggregate target (records/s):** "
                f"{config.get('target_records_per_sec') or 'maximum load'}"
            ),
            (
                "- **End-to-end latency sampling:** "
                + (
                    "disabled"
                    if not config.get("latency_enabled", False)
                    else f"1-in-{config.get('latency_sample_every', 1)}"
                )
            ),
            (
                "- **Producer compression:** "
                f"{producer.get('compression_type', 'unknown')}"
            ),
            (
                "- **Producer batching enabled:** "
                f"{producer.get('batching_enabled', 'unknown')}"
            ),
            (
                "- **Producer batching maximum bytes:** "
                f"{producer.get('batching_max_bytes', 'unknown')}"
            ),
            (
                "- **Producer batching delay (ms):** "
                f"{producer.get('batching_max_publish_delay_ms', 'unknown')}"
            ),
            (
                "- **Producer pending-message limit:** "
                f"{producer.get('max_pending_messages', 'unknown')}"
            ),
            (
                "- **Consumer receiver queue:** "
                f"{consumer.get('receiver_queue_size', 'unknown')}"
            ),
            (
                "- **Managed-ledger ensemble/write/ack quorum:** "
                f"{ledger.get('ensemble_size', 'unknown')}/"
                f"{ledger.get('write_quorum', 'unknown')}/"
                f"{ledger.get('ack_quorum', 'unknown')}"
            ),
            f"- **Storage:** {runtime.get('storage', 'unknown')}",
            f"- **JVM memory:** `{runtime.get('jvm_memory', 'unknown')}`",
            f"- **Pulsar version:** {runtime.get('product_version', 'unknown')}",
            f"- **Profile:** {settings.get('profile_id', 'unknown')}",
            (
                "- **Profile SHA-256:** "
                f"`{settings.get('profile_sha256', 'unknown')}`"
            ),
        ]

    def scale_report_lines(self, config: dict[str, Any]) -> list[str]:
        producer_ranks = int(config.get("producer_ranks", 0) or 0)
        consumer_ranks = int(config.get("consumer_ranks", 0) or 0)
        return [
            "| Quantity | Value | Meaning |",
            "|---|---:|---|",
            (
                "| Simulated logical devices | {devices} | Logical device IDs "
                "rotated through generated Pulsar records. |"
            ).format(devices=config.get("total_simulated_devices", "unknown")),
            (
                "| Real Pulsar producer clients | {producers} | One "
                "`pulsar.Producer` per producer MPI rank. |"
            ).format(producers=producer_ranks),
            (
                "| Real Pulsar consumer clients | {consumers} | One Shared "
                "subscription consumer per consumer MPI rank. |"
            ).format(consumers=consumer_ranks),
            (
                "| Total benchmark Pulsar clients | {clients} | Producer plus "
                "consumer client objects created by this benchmark. |"
            ).format(clients=producer_ranks + consumer_ranks),
            "",
            (
                "Simulated logical devices are message identities multiplexed "
                "through producer clients, not one TCP connection per device."
            ),
        ]

    def report_notes(self) -> tuple[str, ...]:
        return (
            "Pulsar runs use one standalone service node containing broker, "
            "BookKeeper, and metadata services. This is a benchmark topology, "
            "not a production high-availability deployment.",
            "Kafka and Pulsar use the same application-level producer backlog, "
            "flush, and failed-send qualification thresholds. The Pulsar namespace "
            "records backend and campaign provenance; results are not silently "
            "pooled with Kafka campaigns.",
        )

    def latency_report_notes(self) -> tuple[str, ...]:
        return (
            "Pulsar latency is producer-to-consumer end-to-end latency from the "
            "record-envelope send timestamp to consumer receipt after clock "
            "calibration; no client-to-broker RTT is substituted for it.",
        )


def _required_text(settings: dict[str, Any], name: str) -> str:
    value = settings.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Pulsar {name} must be a non-empty string")
    return value.strip()


def _positive_int(settings: dict[str, Any], name: str) -> int:
    value = settings.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"Pulsar {name} must be a positive integer")
    return value


def _required_object(settings: dict[str, Any], name: str) -> dict[str, Any]:
    value = settings.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Pulsar {name} must be an object")
    return value


def _validate_profile(settings: dict[str, Any]) -> None:
    _required_text(settings, "profile_id")
    digest = _required_text(settings, "profile_sha256").lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("Pulsar profile_sha256 must be a 64-character hex digest")


def _validate_producer(settings: dict[str, Any]) -> None:
    if str(settings.get("compression_type", "none")).lower() != "none":
        raise ValueError("Initial Pulsar comparison profile requires no compression")
    for name in (
        "send_timeout_ms",
        "batching_max_messages",
        "batching_max_bytes",
        "batching_max_publish_delay_ms",
        "max_pending_messages",
        "max_pending_messages_across_partitions",
    ):
        _positive_int(settings, name)
    if not isinstance(settings.get("batching_enabled"), bool):
        raise ValueError("Pulsar producer.batching_enabled must be boolean")
    if not isinstance(settings.get("block_if_queue_full"), bool):
        raise ValueError("Pulsar producer.block_if_queue_full must be boolean")


def _validate_consumer(settings: dict[str, Any]) -> None:
    for name in (
        "receiver_queue_size",
        "max_total_receiver_queue_size_across_partitions",
        "negative_ack_redelivery_delay_ms",
    ):
        _positive_int(settings, name)


def _validate_standalone_properties(settings: dict[str, Any]) -> None:
    expected = {
        "allowAutoTopicCreation": False,
        "includeStandardPrometheusMetrics": True,
        "exposeTopicLevelMetricsInPrometheus": False,
        "exposeConsumerLevelMetricsInPrometheus": False,
    }
    if settings != expected:
        raise ValueError(
            "Pulsar standalone_properties must match the immutable baseline "
            "profile"
        )
