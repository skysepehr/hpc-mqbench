from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from src.benchmark.local_deps import ensure_repo_local_dependencies

ensure_repo_local_dependencies()

try:
    from confluent_kafka import KafkaException, Producer
except ImportError as exc:
    raise RuntimeError(
        "Producer workers require confluent-kafka. Install requirements.txt or "
        "run ./scripts/install_local_confluent_kafka.sh to build the vendored "
        "source package into .local/python."
    ) from exc

from models.benchmark_config import BenchmarkConfig
from src.benchmark.kafka_client import KafkaClientFactory, KafkaConnectionSettings
from src.benchmark.client_stats import (
    LibrdkafkaStatsTracker,
    disabled_librdkafka_stats_summary,
    librdkafka_stats_enabled,
)
from src.benchmark.payload_generator import PayloadGenerator
from src.benchmark.record_envelope import encode_record_envelope
from src.benchmark.send_patterns import build_send_pattern_controller


@dataclass(slots=True)
class ProducerMetrics:
    """
    Metrics collected by one producer worker.

    These are local metrics for one MPI producer rank. Later, rank 0 will gather
    and aggregate them across all producers.
    """

    rank: int
    messages_attempted: int = 0
    messages_enqueued: int = 0
    messages_delivered: int = 0
    messages_failed: int = 0
    bytes_attempted: int = 0
    bytes_enqueued: int = 0
    bytes_delivered: int = 0
    warmup_messages_attempted: int = 0
    warmup_messages_enqueued: int = 0
    warmup_messages_delivered: int = 0
    warmup_messages_failed: int = 0
    warmup_bytes_attempted: int = 0
    warmup_bytes_enqueued: int = 0
    warmup_bytes_delivered: int = 0
    latency_envelopes_written: int = 0
    latency_samples_written: int = 0
    latency_samples_enqueued: int = 0
    latency_samples_delivered: int = 0
    latency_samples_failed: int = 0
    delivery_callbacks_seen: int = 0
    warmup_delivery_callbacks_seen: int = 0
    delivery_callbacks_seen_at_flush_start: int = 0
    messages_delivered_at_flush_start: int = 0
    messages_failed_at_flush_start: int = 0
    pending_messages_at_flush_start: int = 0
    pending_bytes_at_flush_start: int = 0
    producer_queue_len_at_flush_start: int = 0
    producer_queue_len_after_flush: int = 0
    flush_remaining_messages: int = 0
    produce_error_counts: dict[str, int] = field(default_factory=dict)
    delivery_error_counts: dict[str, int] = field(default_factory=dict)
    virtual_device_index_start: int = 0
    virtual_devices_assigned: int = 0
    start_time_monotonic: float = 0.0
    run_start_time_monotonic: float = 0.0
    warmup_end_time_monotonic: float = 0.0
    send_loop_end_time_monotonic: float = 0.0
    end_time_monotonic: float = 0.0
    start_time_unix: float = 0.0
    run_start_time_unix: float = 0.0
    warmup_end_time_unix: float = 0.0
    send_loop_end_time_unix: float = 0.0
    end_time_unix: float = 0.0
    librdkafka_stats: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_sec(self) -> float:
        """
        Benchmark duration observed by this producer.
        """
        if self.end_time_monotonic <= self.start_time_monotonic:
            return 0.0
        return self.end_time_monotonic - self.start_time_monotonic

    @property
    def throughput_msgs_per_sec(self) -> float:
        """
        Delivered messages per second.
        """
        duration = self.duration_sec
        if duration <= 0:
            return 0.0
        return self.messages_delivered / duration

    @property
    def throughput_bytes_per_sec(self) -> float:
        """
        Delivered bytes per second.
        """
        duration = self.duration_sec
        if duration <= 0:
            return 0.0
        return self.bytes_delivered / duration

    @property
    def throughput_megabytes_per_sec(self) -> float:
        """
        Delivered megabytes per second using decimal MB.
        """
        return self.throughput_bytes_per_sec / 1_000_000

    @property
    def throughput_mebibytes_per_sec(self) -> float:
        """Delivered mebibytes per second using 1,048,576 bytes."""
        return self.throughput_bytes_per_sec / 1_048_576

    @property
    def send_loop_duration_sec(self) -> float:
        """
        Time spent actively attempting sends before the final producer flush.
        """
        if self.send_loop_end_time_monotonic <= self.start_time_monotonic:
            return 0.0
        return self.send_loop_end_time_monotonic - self.start_time_monotonic

    @property
    def flush_duration_sec(self) -> float:
        """
        Time spent waiting for queued delivery callbacks after active sending.
        """
        if self.end_time_monotonic <= self.send_loop_end_time_monotonic:
            return 0.0
        return self.end_time_monotonic - self.send_loop_end_time_monotonic

    @property
    def send_attempt_throughput_msgs_per_sec(self) -> float:
        """
        Attempted send rate during the active send window.
        """
        duration = self.send_loop_duration_sec
        if duration <= 0:
            return 0.0
        return self.messages_attempted / duration

    @property
    def send_attempt_throughput_bytes_per_sec(self) -> float:
        """
        Attempted byte rate during the active send window.
        """
        duration = self.send_loop_duration_sec
        if duration <= 0:
            return 0.0
        return self.bytes_attempted / duration

    @property
    def send_attempt_throughput_megabytes_per_sec(self) -> float:
        """
        Attempted megabytes per second during the active send window.
        """
        return self.send_attempt_throughput_bytes_per_sec / 1_000_000

    @property
    def send_attempt_throughput_mebibytes_per_sec(self) -> float:
        """Attempted mebibytes per second during the active send window."""
        return self.send_attempt_throughput_bytes_per_sec / 1_048_576

    @property
    def delivery_callbacks_during_flush(self) -> int:
        return max(0, self.delivery_callbacks_seen - self.delivery_callbacks_seen_at_flush_start)

    @property
    def messages_delivered_during_flush(self) -> int:
        return max(0, self.messages_delivered - self.messages_delivered_at_flush_start)

    @property
    def messages_failed_during_flush(self) -> int:
        return max(0, self.messages_failed - self.messages_failed_at_flush_start)

    @property
    def delivery_callback_rate_during_flush_per_sec(self) -> float:
        duration = self.flush_duration_sec
        if duration <= 0:
            return 0.0
        return self.delivery_callbacks_during_flush / duration

    @property
    def estimated_unique_virtual_devices_attempted(self) -> int:
        """
        Estimate how many assigned virtual device IDs were used in send attempts.

        Payload generation uses deterministic round-robin device selection, so
        this estimate is exact for attempted sends without storing a large set of
        device IDs during high-throughput runs.
        """
        if self.virtual_devices_assigned <= 0:
            return 0
        return min(self.messages_attempted, self.virtual_devices_assigned)

    @property
    def estimated_unique_virtual_devices_delivered(self) -> int:
        """
        Estimate how many assigned virtual device IDs produced delivered records.

        This is a conservative coverage indicator for large IoT simulations. It
        avoids per-device bookkeeping overhead and is most meaningful when
        delivery failures are near zero.
        """
        if self.virtual_devices_assigned <= 0:
            return 0
        return min(self.messages_delivered, self.virtual_devices_assigned)

    @property
    def virtual_device_coverage_delivered_ratio(self) -> float:
        """
        Fraction of this rank's virtual-device range represented by deliveries.
        """
        if self.virtual_devices_assigned <= 0:
            return 0.0
        return self.estimated_unique_virtual_devices_delivered / self.virtual_devices_assigned

    def to_dict(self) -> dict[str, Any]:
        """
        Convert metrics to a JSON-friendly dictionary.
        """
        return {
            "rank": self.rank,
            "messages_attempted": self.messages_attempted,
            "messages_enqueued": self.messages_enqueued,
            "messages_delivered": self.messages_delivered,
            "messages_failed": self.messages_failed,
            "bytes_attempted": self.bytes_attempted,
            "bytes_enqueued": self.bytes_enqueued,
            "bytes_delivered": self.bytes_delivered,
            "warmup_messages_attempted": self.warmup_messages_attempted,
            "warmup_messages_enqueued": self.warmup_messages_enqueued,
            "warmup_messages_delivered": self.warmup_messages_delivered,
            "warmup_messages_failed": self.warmup_messages_failed,
            "warmup_bytes_attempted": self.warmup_bytes_attempted,
            "warmup_bytes_enqueued": self.warmup_bytes_enqueued,
            "warmup_bytes_delivered": self.warmup_bytes_delivered,
            "latency_envelopes_written": self.latency_envelopes_written,
            "latency_samples_written": self.latency_samples_written,
            "latency_samples_enqueued": self.latency_samples_enqueued,
            "latency_samples_delivered": self.latency_samples_delivered,
            "latency_samples_failed": self.latency_samples_failed,
            "delivery_callbacks_seen": self.delivery_callbacks_seen,
            "warmup_delivery_callbacks_seen": self.warmup_delivery_callbacks_seen,
            "delivery_callbacks_seen_at_flush_start": (
                self.delivery_callbacks_seen_at_flush_start
            ),
            "delivery_callbacks_during_flush": self.delivery_callbacks_during_flush,
            "delivery_callback_rate_during_flush_per_sec": (
                self.delivery_callback_rate_during_flush_per_sec
            ),
            "messages_delivered_at_flush_start": self.messages_delivered_at_flush_start,
            "messages_delivered_during_flush": self.messages_delivered_during_flush,
            "messages_failed_at_flush_start": self.messages_failed_at_flush_start,
            "messages_failed_during_flush": self.messages_failed_during_flush,
            "pending_messages_at_flush_start": self.pending_messages_at_flush_start,
            "pending_bytes_at_flush_start": self.pending_bytes_at_flush_start,
            "producer_queue_len_at_flush_start": self.producer_queue_len_at_flush_start,
            "producer_queue_len_after_flush": self.producer_queue_len_after_flush,
            "flush_remaining_messages": self.flush_remaining_messages,
            "produce_error_counts": dict(self.produce_error_counts),
            "delivery_error_counts": dict(self.delivery_error_counts),
            "virtual_device_index_start": self.virtual_device_index_start,
            "virtual_devices_assigned": self.virtual_devices_assigned,
            "estimated_unique_virtual_devices_attempted": (
                self.estimated_unique_virtual_devices_attempted
            ),
            "estimated_unique_virtual_devices_delivered": (
                self.estimated_unique_virtual_devices_delivered
            ),
            "virtual_device_coverage_delivered_ratio": (
                self.virtual_device_coverage_delivered_ratio
            ),
            "send_loop_duration_sec": self.send_loop_duration_sec,
            "flush_duration_sec": self.flush_duration_sec,
            "duration_sec": self.duration_sec,
            "run_start_time_unix": self.run_start_time_unix,
            "warmup_end_time_unix": self.warmup_end_time_unix,
            "start_time_unix": self.start_time_unix,
            "send_loop_end_time_unix": self.send_loop_end_time_unix,
            "end_time_unix": self.end_time_unix,
            "librdkafka_stats": dict(self.librdkafka_stats),
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
            "throughput_megabytes_per_sec": self.throughput_megabytes_per_sec,
            "throughput_mebibytes_per_sec": self.throughput_mebibytes_per_sec,
            "send_attempt_throughput_msgs_per_sec": (
                self.send_attempt_throughput_msgs_per_sec
            ),
            "send_attempt_throughput_bytes_per_sec": (
                self.send_attempt_throughput_bytes_per_sec
            ),
            "send_attempt_throughput_megabytes_per_sec": (
                self.send_attempt_throughput_megabytes_per_sec
            ),
            "send_attempt_throughput_mebibytes_per_sec": (
                self.send_attempt_throughput_mebibytes_per_sec
            ),
        }


class ProducerWorker:
    """
    One Kafka producer worker.

    This class combines:
    - one Kafka producer client
    - one payload generator
    - one send-pattern controller
    - one local metrics object

    In the MPI design, each producer rank will create one ProducerWorker.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        producer_index: int,
        clock_offset_ns: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        config:
            Validated benchmark configuration for this case.

        rank:
            MPI rank of this producer worker.

        bootstrap_servers:
            Comma-separated Kafka bootstrap servers.

        producer_index:
            Zero-based producer index among all producer ranks. This is useful
            for assigning virtual device ranges.
        """
        self.config = config
        self.rank = rank
        self.producer_index = producer_index
        self.clock_offset_ns = int(clock_offset_ns)

        # Assign each producer rank its own virtual-device range.
        device_index_start = producer_index * config.virtual_devices_per_rank

        self.payload_generator = PayloadGenerator(
            config=config,
            rank=rank,
            device_index_start=device_index_start,
        )

        connection = KafkaConnectionSettings(
            bootstrap_servers=bootstrap_servers,
            client_id=KafkaClientFactory.build_producer_client_id(rank),
        )
        self.stats_tracker = (
            LibrdkafkaStatsTracker(role="producer", rank=rank)
            if librdkafka_stats_enabled(config.extra)
            else None
        )
        self.producer: Producer = KafkaClientFactory(
            config=config,
            connection=connection,
        ).create_producer(
            stats_cb=self.stats_tracker.record if self.stats_tracker is not None else None,
        )

        self.send_pattern_controller = build_send_pattern_controller(config)
        self.metrics = ProducerMetrics(
            rank=rank,
            virtual_device_index_start=device_index_start,
            virtual_devices_assigned=config.virtual_devices_per_rank,
        )
        self._send_phase_complete = False
        self._closed = False

    def run(self, duration_sec: int | None = None) -> ProducerMetrics:
        """
        Run the producer loop for one benchmark duration.
        """
        self.run_send_phase(duration_sec)
        return self.flush_and_close()

    def run_send_phase(
        self,
        duration_sec: int | None = None,
    ) -> ProducerMetrics:
        """Run warm-up and measurement without flushing the producer queue."""
        if self._send_phase_complete:
            raise RuntimeError("Kafka producer send phase already completed")
        self.metrics.run_start_time_unix = time.time()
        self.metrics.run_start_time_monotonic = time.monotonic()
        active_duration_sec = duration_sec if duration_sec is not None else self.config.duration_sec
        warmup_deadline = (
            self.metrics.run_start_time_monotonic + self.config.warmup_sec
        )
        deadline = warmup_deadline + active_duration_sec
        self.metrics.warmup_end_time_monotonic = warmup_deadline
        self.metrics.warmup_end_time_unix = (
            self.metrics.run_start_time_unix + self.config.warmup_sec
        )
        self.metrics.start_time_monotonic = warmup_deadline
        self.metrics.start_time_unix = self.metrics.warmup_end_time_unix

        # Rate schedules start when workload emission starts, not when the
        # producer object was constructed during consumer readiness.
        self.send_pattern_controller = build_send_pattern_controller(self.config)
        message_index = 0

        while time.monotonic() < deadline:
            self.send_pattern_controller.before_send(message_index)

            generated = self.payload_generator.generate(message_index)
            message_key = generated.device_id.encode("utf-8")
            message_value = generated.payload_bytes
            measurement_record = time.monotonic() >= warmup_deadline
            latency_sampled = (
                self.config.latency_enabled
                and measurement_record
                and generated.sequence_number % self.config.latency_sample_every == 0
            )
            if self.config.record_envelope_enabled:
                send_time_ns = (
                    time.time_ns() + self.clock_offset_ns
                    if latency_sampled
                    else 0
                )
                message_value = encode_record_envelope(
                    message_value,
                    producer_rank=self.rank,
                    sequence=generated.sequence_number,
                    send_time_ns=send_time_ns,
                    measurement_record=measurement_record,
                    latency_sampled=latency_sampled,
                )
                self.metrics.latency_envelopes_written += 1
                if latency_sampled:
                    self.metrics.latency_samples_written += 1

            if measurement_record:
                self.metrics.messages_attempted += 1
                self.metrics.bytes_attempted += len(message_value)
            else:
                self.metrics.warmup_messages_attempted += 1
                self.metrics.warmup_bytes_attempted += len(message_value)

            try:
                self.producer.produce(
                    topic=self.config.topic_name,
                    key=message_key,
                    value=message_value,
                    on_delivery=(
                        lambda err, msg, measured=measurement_record,
                        sampled=latency_sampled: (
                            self._delivery_callback(err, msg, measured, sampled)
                        )
                    ),
                )
                if measurement_record:
                    self.metrics.messages_enqueued += 1
                    self.metrics.bytes_enqueued += len(message_value)
                    if latency_sampled:
                        self.metrics.latency_samples_enqueued += 1
                else:
                    self.metrics.warmup_messages_enqueued += 1
                    self.metrics.warmup_bytes_enqueued += len(message_value)
            except BufferError:
                # The local producer queue is full. Poll to serve delivery callbacks
                # and free queue space.
                if measurement_record:
                    self.metrics.messages_failed += 1
                else:
                    self.metrics.warmup_messages_failed += 1
                self._record_produce_error("BufferError")
                self.producer.poll(0.1)
            except KafkaException as exc:
                if measurement_record:
                    self.metrics.messages_failed += 1
                else:
                    self.metrics.warmup_messages_failed += 1
                self._record_produce_error(_error_key(exc))
                self.producer.poll(0.1)

            # Serve delivery callbacks frequently.
            self.producer.poll(0)

            self.send_pattern_controller.after_send(message_index)
            message_index += 1

        # Separate the active send window from the final flush. The end-to-end
        # delivered throughput includes flush time, while send-attempt throughput
        # captures the offered client-side load during the measured send loop.
        self.metrics.send_loop_end_time_unix = time.time()
        self.metrics.send_loop_end_time_monotonic = time.monotonic()
        self._send_phase_complete = True
        return self.metrics

    def flush_and_close(self) -> ProducerMetrics:
        """Snapshot pending callbacks, then flush all producer deliveries."""
        if not self._send_phase_complete:
            raise RuntimeError("Kafka producer flush requires a completed send phase")
        if self._closed:
            return self.metrics
        self.metrics.delivery_callbacks_seen_at_flush_start = (
            self.metrics.delivery_callbacks_seen
        )
        self.metrics.messages_delivered_at_flush_start = self.metrics.messages_delivered
        self.metrics.messages_failed_at_flush_start = self.metrics.messages_failed
        self.metrics.pending_messages_at_flush_start = max(
            0,
            self.metrics.messages_enqueued - self.metrics.delivery_callbacks_seen,
        )
        self.metrics.pending_bytes_at_flush_start = max(
            0,
            self.metrics.bytes_enqueued - self.metrics.bytes_delivered,
        )
        self.metrics.producer_queue_len_at_flush_start = _producer_len(self.producer)

        # Flush all queued messages at the end so delivered counts are as complete
        # as possible for reporting.
        flush_remaining = self.producer.flush(timeout=30.0)
        self.metrics.flush_remaining_messages = int(flush_remaining or 0)
        self.metrics.producer_queue_len_after_flush = _producer_len(self.producer)

        self.metrics.end_time_unix = time.time()
        self.metrics.end_time_monotonic = time.monotonic()
        self.metrics.librdkafka_stats = (
            self.stats_tracker.summary()
            if self.stats_tracker is not None
            else disabled_librdkafka_stats_summary(role="producer", rank=self.rank)
        )
        self._closed = True
        return self.metrics

    def _delivery_callback(
        self,
        err: object,
        msg: object,
        measurement_record: bool,
        latency_sampled: bool,
    ) -> None:
        """
        Kafka delivery callback.
        """
        if measurement_record:
            self.metrics.delivery_callbacks_seen += 1
        else:
            self.metrics.warmup_delivery_callbacks_seen += 1

        if err is not None:
            if measurement_record:
                self.metrics.messages_failed += 1
                if latency_sampled:
                    self.metrics.latency_samples_failed += 1
            else:
                self.metrics.warmup_messages_failed += 1
            self._record_delivery_error(_error_key(err))
            return

        if measurement_record:
            self.metrics.messages_delivered += 1
            if latency_sampled:
                self.metrics.latency_samples_delivered += 1
        else:
            self.metrics.warmup_messages_delivered += 1

        try:
            value = msg.value()
            if value is not None:
                if measurement_record:
                    self.metrics.bytes_delivered += len(value)
                else:
                    self.metrics.warmup_bytes_delivered += len(value)
        except Exception:
            # Metrics code must never break delivery handling.
            pass

    def _record_produce_error(self, key: str) -> None:
        self.metrics.produce_error_counts[key] = (
            self.metrics.produce_error_counts.get(key, 0) + 1
        )

    def _record_delivery_error(self, key: str) -> None:
        self.metrics.delivery_error_counts[key] = (
            self.metrics.delivery_error_counts.get(key, 0) + 1
        )


def _producer_len(producer: Producer) -> int:
    try:
        return int(len(producer))
    except Exception:
        return 0


def _error_key(error: object) -> str:
    try:
        name = error.name()
        if name:
            return str(name)
    except Exception:
        pass
    try:
        code = error.code()
        return f"code_{code}"
    except Exception:
        pass
    text = str(error).strip()
    if not text:
        return type(error).__name__
    return text.split(":", 1)[0][:80]
