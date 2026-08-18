from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.benchmark.local_deps import ensure_repo_local_dependencies

ensure_repo_local_dependencies()

try:
    from confluent_kafka import Consumer, Producer
except ImportError as exc:
    raise RuntimeError(
        "Kafka benchmark execution requires confluent-kafka. Install the Python "
        "requirements or build the vendored source with "
        "./scripts/install_local_confluent_kafka.sh."
    ) from exc

from models.benchmark_config import BenchmarkConfig
from src.benchmark.client_stats import librdkafka_stats_interval_ms


@dataclass(slots=True)
class KafkaConnectionSettings:
    """
    Small container for Kafka connection-related values.

    This keeps connection details grouped together and avoids passing many
    loose parameters around the code.
    """

    bootstrap_servers: str
    client_id: str


class KafkaClientFactory:
    """
    Factory for building configured Kafka Producer and Consumer clients.

    Why use a factory?
    - keeps Kafka config in one place
    - avoids duplicating producer/consumer config logic
    - makes future tuning easier
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        connection: KafkaConnectionSettings,
    ) -> None:
        self.config = config
        self.connection = connection

    def create_producer(self, stats_cb: Any | None = None) -> Producer:
        """
        Create and return a configured Kafka Producer.

        The config values here are mapped from our benchmark config model
        to confluent-kafka / librdkafka producer settings.
        """
        producer_config: dict[str, Any] = {
            # Kafka bootstrap endpoints.
            "bootstrap.servers": self.connection.bootstrap_servers,

            # A readable client ID helps with debugging and broker-side logs.
            "client.id": self.connection.client_id,

            # Reliability / acknowledgement mode.
            "acks": self.config.acks,

            # Compression.
            "compression.type": self.config.compression_type,

            # Producer batching settings.
            "batch.size": self.config.batch_size,
            "linger.ms": self.config.linger_ms,

            # Delivery behavior.
            #
            # idempotence is left disabled for now because this benchmark is
            # focused on broad throughput experiments first. We can add it
            # later as a separate configuration dimension if needed.
            "enable.idempotence": False,
        }
        self._apply_stats_callback(producer_config, stats_cb)
        self._apply_large_message_producer_settings(producer_config)
        self._apply_extra_client_config(
            producer_config,
            "kafka_common_client_config",
        )
        self._apply_extra_client_config(
            producer_config,
            "kafka_producer_config",
        )

        return Producer(producer_config)

    def create_consumer(
        self,
        group_id: str,
        auto_offset_reset: str = "earliest",
        stats_cb: Any | None = None,
    ) -> Consumer:
        """
        Create and return a configured Kafka Consumer.

        Parameters
        ----------
        group_id:
            Consumer group ID for this benchmark run.

        auto_offset_reset:
            Used when there is no committed offset. Usually "earliest" is
            useful for benchmarking because it lets consumers read the full
            topic content when needed.
        """
        consumer_config: dict[str, Any] = {
            # Kafka bootstrap endpoints.
            "bootstrap.servers": self.connection.bootstrap_servers,

            # Human-readable client ID.
            "client.id": self.connection.client_id,

            # Consumer group.
            "group.id": group_id,

            # Offset behavior when no committed offset exists.
            "auto.offset.reset": auto_offset_reset,

            # Disable auto commit so benchmark control stays explicit and
            # results are easier to reason about.
            "enable.auto.commit": False,
        }
        self._apply_stats_callback(consumer_config, stats_cb)
        self._apply_large_message_consumer_settings(consumer_config)
        self._apply_extra_client_config(
            consumer_config,
            "kafka_common_client_config",
        )
        self._apply_extra_client_config(
            consumer_config,
            "kafka_consumer_config",
        )

        return Consumer(consumer_config)

    def _apply_stats_callback(
        self,
        client_config: dict[str, Any],
        stats_cb: Any | None,
    ) -> None:
        if stats_cb is None:
            return
        client_config["stats_cb"] = stats_cb
        client_config["statistics.interval.ms"] = librdkafka_stats_interval_ms(
            self.config.extra,
        )

    def _apply_extra_client_config(
        self,
        client_config: dict[str, Any],
        extra_key: str,
    ) -> None:
        """
        Apply explicit librdkafka tuning maps from config.extra.

        V1 keeps the top-level benchmark schema small, but high-throughput
        cases need a controlled way to tune queue, socket, and fetch settings.
        """
        raw_value = self.config.extra.get(extra_key)
        if raw_value is None:
            return
        if not isinstance(raw_value, dict):
            raise ValueError(f"extra.{extra_key} must be a JSON object")
        for key, value in raw_value.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(f"extra.{extra_key} keys must be non-empty strings")
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                client_config[key] = value
            else:
                raise ValueError(
                    f"extra.{extra_key}.{key} must be a string, number, boolean, or null"
                )

    def _large_message_limit_bytes(self) -> int | None:
        """
        Return the optional per-case Kafka large-message limit.

        The normal Kafka/librdkafka defaults are intentionally left untouched
        for small-message benchmark cases. Large payload campaigns can opt in by
        setting extra.kafka_message_max_bytes in the JSON config.
        """
        raw_value = self.config.extra.get("kafka_message_max_bytes")
        if raw_value is None:
            return None

        value = int(raw_value)
        if value <= 0:
            raise ValueError("extra.kafka_message_max_bytes must be greater than 0")
        return value

    def _receive_message_limit_bytes(self, message_limit: int) -> int:
        """
        Return the receive limit used by producers and consumers.

        Kafka responses include protocol overhead, so this is slightly larger
        than the record limit and never below librdkafka's usual 100 MB default.
        """
        raw_value = self.config.extra.get("kafka_receive_message_max_bytes")
        if raw_value is not None:
            value = int(raw_value)
            if value <= 0:
                raise ValueError(
                    "extra.kafka_receive_message_max_bytes must be greater than 0"
                )
            return value

        return max(100_000_000, message_limit + 1_048_576)

    def _apply_large_message_producer_settings(
        self,
        producer_config: dict[str, Any],
    ) -> None:
        """
        Add librdkafka producer limits for large single-record payload campaigns.
        """
        message_limit = self._large_message_limit_bytes()
        if message_limit is None:
            return

        producer_config["message.max.bytes"] = message_limit
        producer_config["receive.message.max.bytes"] = (
            self._receive_message_limit_bytes(message_limit)
        )

        # A single record may be larger than the nominal producer batch size.
        # Keep batch.size at least as large as the configured message limit.
        producer_config["batch.size"] = max(self.config.batch_size, message_limit)

    def _apply_large_message_consumer_settings(
        self,
        consumer_config: dict[str, Any],
    ) -> None:
        """
        Add librdkafka consumer fetch limits for large single-record payloads.
        """
        message_limit = self._large_message_limit_bytes()
        if message_limit is None:
            return

        consumer_config["fetch.message.max.bytes"] = message_limit
        consumer_config["receive.message.max.bytes"] = (
            self._receive_message_limit_bytes(message_limit)
        )

    @staticmethod
    def build_producer_client_id(rank: int) -> str:
        """
        Build a consistent producer client ID.
        """
        return f"benchmark-producer-rank-{rank}"

    @staticmethod
    def build_consumer_client_id(rank: int) -> str:
        """
        Build a consistent consumer client ID.
        """
        return f"benchmark-consumer-rank-{rank}"

    @staticmethod
    def build_consumer_group_id(case_id: str) -> str:
        """
        Build a consumer group ID for one benchmark case.

        We keep it case-specific so runs do not accidentally reuse offsets from
        another benchmark case.
        """
        return f"benchmark-group-{case_id}"
