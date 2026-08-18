from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any

from models.benchmark_config import BenchmarkConfig
from src.benchmark.local_deps import ensure_repo_local_dependencies
from src.benchmark.payload_generator import PayloadGenerator
from src.benchmark.record_envelope import (
    LatencyHistogram,
    decode_record_envelope,
    encode_record_envelope,
)
from src.benchmark.send_patterns import build_send_pattern_controller

ensure_repo_local_dependencies()

try:
    import pulsar
except ImportError as exc:  # pragma: no cover - exercised by HPC preflight
    raise RuntimeError(
        "Pulsar workers require pulsar-client. Run "
        "./scripts/install_local_pulsar_client.sh first."
    ) from exc


@dataclass(slots=True)
class PulsarProducerMetrics:
    rank: int
    virtual_device_index_start: int
    virtual_devices_assigned: int
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
    send_phase_completed: bool = False
    flush_completed: bool = False
    start_time_monotonic: float = 0.0
    run_start_time_monotonic: float = 0.0
    send_loop_end_time_monotonic: float = 0.0
    end_time_monotonic: float = 0.0
    start_time_unix: float = 0.0
    run_start_time_unix: float = 0.0
    warmup_end_time_unix: float = 0.0
    send_loop_end_time_unix: float = 0.0
    end_time_unix: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        duration = max(0.0, self.end_time_monotonic - self.start_time_monotonic)
        send_duration = max(
            0.0,
            self.send_loop_end_time_monotonic - self.start_time_monotonic,
        )
        flush_duration = max(
            0.0,
            self.end_time_monotonic - self.send_loop_end_time_monotonic,
        )
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
            "delivery_callbacks_seen_at_flush_start": self.delivery_callbacks_seen_at_flush_start,
            "delivery_callbacks_during_flush": max(
                0,
                self.delivery_callbacks_seen - self.delivery_callbacks_seen_at_flush_start,
            ),
            "messages_delivered_at_flush_start": self.messages_delivered_at_flush_start,
            "messages_delivered_during_flush": max(
                0,
                self.messages_delivered - self.messages_delivered_at_flush_start,
            ),
            "messages_failed_at_flush_start": self.messages_failed_at_flush_start,
            "messages_failed_during_flush": max(
                0,
                self.messages_failed - self.messages_failed_at_flush_start,
            ),
            "pending_messages_at_flush_start": self.pending_messages_at_flush_start,
            "pending_bytes_at_flush_start": self.pending_bytes_at_flush_start,
            "producer_queue_len_at_flush_start": self.producer_queue_len_at_flush_start,
            "producer_queue_len_after_flush": self.producer_queue_len_after_flush,
            "flush_remaining_messages": self.flush_remaining_messages,
            "produce_error_counts": dict(self.produce_error_counts),
            "delivery_error_counts": dict(self.delivery_error_counts),
            "send_phase_completed": self.send_phase_completed,
            "flush_completed": self.flush_completed,
            "virtual_device_index_start": self.virtual_device_index_start,
            "virtual_devices_assigned": self.virtual_devices_assigned,
            "estimated_unique_virtual_devices_attempted": min(
                self.messages_attempted,
                self.virtual_devices_assigned,
            ),
            "estimated_unique_virtual_devices_delivered": min(
                self.messages_delivered,
                self.virtual_devices_assigned,
            ),
            "send_loop_duration_sec": send_duration,
            "flush_duration_sec": flush_duration,
            "duration_sec": duration,
            "run_start_time_unix": self.run_start_time_unix,
            "warmup_end_time_unix": self.warmup_end_time_unix,
            "start_time_unix": self.start_time_unix,
            "send_loop_end_time_unix": self.send_loop_end_time_unix,
            "end_time_unix": self.end_time_unix,
            "librdkafka_stats": {},
            "throughput_msgs_per_sec": self.messages_delivered / duration if duration else 0.0,
            "throughput_bytes_per_sec": self.bytes_delivered / duration if duration else 0.0,
            "throughput_megabytes_per_sec": self.bytes_delivered / duration / 1_000_000 if duration else 0.0,
            "send_attempt_throughput_msgs_per_sec": self.messages_attempted / send_duration if send_duration else 0.0,
            "send_attempt_throughput_bytes_per_sec": self.bytes_attempted / send_duration if send_duration else 0.0,
            "send_attempt_throughput_megabytes_per_sec": self.bytes_attempted / send_duration / 1_000_000 if send_duration else 0.0,
        }


@dataclass(slots=True)
class PulsarConsumerMetrics:
    rank: int
    messages_polled: int = 0
    messages_received: int = 0
    messages_failed: int = 0
    bytes_received: int = 0
    warmup_messages_received: int = 0
    warmup_bytes_received: int = 0
    late_drained_messages: int = 0
    late_drained_bytes: int = 0
    pre_flush_drained_messages: int = 0
    pre_flush_drained_bytes: int = 0
    post_flush_drained_messages: int = 0
    post_flush_drained_bytes: int = 0
    invalid_envelope_count: int = 0
    duplicate_offset_count: int = 0
    out_of_order_offset_count: int = 0
    latency_histogram: LatencyHistogram = field(default_factory=LatencyHistogram)
    poll_timeouts: int = 0
    processing_operations: int = 0
    readiness_prepared: bool = False
    readiness_assignment_received: bool = False
    readiness_assignment_count: int = 0
    readiness_elapsed_sec: float = 0.0
    readiness_strict_assignment: bool = False
    readiness_note: str = ""
    benchmark_assignment_observed: bool = False
    benchmark_assignment_count_max: int = 0
    final_assignment_received: bool = False
    final_assignment_count: int = 0
    start_time_monotonic: float = 0.0
    run_start_time_monotonic: float = 0.0
    measurement_end_time_monotonic: float = 0.0
    drain_end_time_monotonic: float = 0.0
    end_time_monotonic: float = 0.0
    start_time_unix: float = 0.0
    run_start_time_unix: float = 0.0
    measurement_end_time_unix: float = 0.0
    drain_end_time_unix: float = 0.0
    post_flush_drain_start_time_unix: float = 0.0
    post_flush_drain_end_time_unix: float = 0.0
    coordinated_drain_enabled: bool = False
    coordinated_delivered_target_records: int = 0
    coordinated_consumed_records: int = 0
    drain_completion_reason: str = "not_started"
    end_time_unix: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        duration = max(
            0.0,
            self.measurement_end_time_monotonic - self.start_time_monotonic,
        )
        return {
            "rank": self.rank,
            "messages_polled": self.messages_polled,
            "messages_received": self.messages_received,
            "messages_failed": self.messages_failed,
            "bytes_received": self.bytes_received,
            "warmup_messages_received": self.warmup_messages_received,
            "warmup_bytes_received": self.warmup_bytes_received,
            "late_drained_messages": self.late_drained_messages,
            "late_drained_bytes": self.late_drained_bytes,
            "pre_flush_drained_messages": self.pre_flush_drained_messages,
            "pre_flush_drained_bytes": self.pre_flush_drained_bytes,
            "post_flush_drained_messages": self.post_flush_drained_messages,
            "post_flush_drained_bytes": self.post_flush_drained_bytes,
            "invalid_envelope_count": self.invalid_envelope_count,
            "duplicate_offset_count": self.duplicate_offset_count,
            "out_of_order_offset_count": self.out_of_order_offset_count,
            "duplicate_or_regressed_offset_count": self.duplicate_offset_count + self.out_of_order_offset_count,
            "latency_histogram": self.latency_histogram.to_dict(),
            "poll_timeouts": self.poll_timeouts,
            "processing_operations": self.processing_operations,
            "readiness_prepared": self.readiness_prepared,
            "readiness_assignment_received": self.readiness_assignment_received,
            "readiness_assignment_count": self.readiness_assignment_count,
            "readiness_elapsed_sec": self.readiness_elapsed_sec,
            "readiness_strict_assignment": self.readiness_strict_assignment,
            "readiness_note": self.readiness_note,
            "benchmark_assignment_observed": self.benchmark_assignment_observed,
            "benchmark_assignment_count_max": self.benchmark_assignment_count_max,
            "final_assignment_received": self.final_assignment_received,
            "final_assignment_count": self.final_assignment_count,
            "start_time_unix": self.start_time_unix,
            "run_start_time_unix": self.run_start_time_unix,
            "measurement_end_time_unix": self.measurement_end_time_unix,
            "drain_end_time_unix": self.drain_end_time_unix,
            "post_flush_drain_start_time_unix": (
                self.post_flush_drain_start_time_unix
            ),
            "post_flush_drain_end_time_unix": (
                self.post_flush_drain_end_time_unix
            ),
            "coordinated_drain_enabled": self.coordinated_drain_enabled,
            "coordinated_delivered_target_records": (
                self.coordinated_delivered_target_records
            ),
            "coordinated_consumed_records": self.coordinated_consumed_records,
            "drain_completion_reason": self.drain_completion_reason,
            "end_time_unix": self.end_time_unix,
            "librdkafka_stats": {},
            "duration_sec": duration,
            "throughput_msgs_per_sec": self.messages_received / duration if duration else 0.0,
            "throughput_bytes_per_sec": self.bytes_received / duration if duration else 0.0,
            "throughput_megabytes_per_sec": self.bytes_received / duration / 1_000_000 if duration else 0.0,
        }


class PulsarProducerWorker:
    def __init__(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        producer_index: int,
        clock_offset_ns: int,
    ) -> None:
        self.config = config
        self.rank = rank
        self.clock_offset_ns = int(clock_offset_ns)
        settings = config.backend_settings
        producer_settings = settings["producer"]
        self.client = pulsar.Client(bootstrap_servers)
        self.producer = self.client.create_producer(
            settings["topic_name"],
            producer_name=f"benchmark-producer-{rank}",
            send_timeout_millis=int(producer_settings["send_timeout_ms"]),
            compression_type=pulsar.CompressionType.NONE,
            max_pending_messages=int(producer_settings["max_pending_messages"]),
            max_pending_messages_across_partitions=int(
                producer_settings["max_pending_messages_across_partitions"]
            ),
            block_if_queue_full=bool(producer_settings["block_if_queue_full"]),
            batching_enabled=bool(producer_settings["batching_enabled"]),
            batching_max_messages=int(producer_settings["batching_max_messages"]),
            batching_max_allowed_size_in_bytes=int(
                producer_settings["batching_max_bytes"]
            ),
            batching_max_publish_delay_ms=int(
                producer_settings["batching_max_publish_delay_ms"]
            ),
        )
        device_start = producer_index * config.virtual_devices_per_rank
        self.payload_generator = PayloadGenerator(config, rank, device_start)
        self.metrics = PulsarProducerMetrics(
            rank=rank,
            virtual_device_index_start=device_start,
            virtual_devices_assigned=config.virtual_devices_per_rank,
        )
        self._metrics_lock = threading.Lock()
        self._send_phase_complete = False
        self._closed = False

    def run(self, duration_sec: int | None = None) -> PulsarProducerMetrics:
        self.run_send_phase(duration_sec)
        return self.flush_and_close()

    def run_send_phase(
        self,
        duration_sec: int | None = None,
    ) -> PulsarProducerMetrics:
        """Run warm-up and measurement without flushing the client queue."""
        if self._send_phase_complete:
            raise RuntimeError("Pulsar producer send phase already completed")
        active_duration = (
            self.config.duration_sec if duration_sec is None else duration_sec
        )
        self.metrics.run_start_time_unix = time.time()
        self.metrics.run_start_time_monotonic = time.monotonic()
        warmup_deadline = self.metrics.run_start_time_monotonic + self.config.warmup_sec
        deadline = warmup_deadline + active_duration
        self.metrics.warmup_end_time_unix = self.metrics.run_start_time_unix + self.config.warmup_sec
        self.metrics.start_time_unix = self.metrics.warmup_end_time_unix
        self.metrics.start_time_monotonic = warmup_deadline
        send_pattern = build_send_pattern_controller(self.config)
        message_index = 0

        while time.monotonic() < deadline:
            send_pattern.before_send(message_index)
            generated = self.payload_generator.generate(message_index)
            value = generated.payload_bytes
            measured = time.monotonic() >= warmup_deadline
            sampled = (
                self.config.latency_enabled
                and measured
                and generated.sequence_number % self.config.latency_sample_every == 0
            )
            if self.config.record_envelope_enabled:
                value = encode_record_envelope(
                    value,
                    producer_rank=self.rank,
                    sequence=generated.sequence_number,
                    send_time_ns=time.time_ns() + self.clock_offset_ns if sampled else 0,
                    measurement_record=measured,
                    latency_sampled=sampled,
                )
                self.metrics.latency_envelopes_written += 1
                if sampled:
                    self.metrics.latency_samples_written += 1
            self._record_attempt(measured, len(value))
            try:
                self.producer.send_async(
                    value,
                    lambda result, _message_id, measured=measured, sampled=sampled,
                    size=len(value): self._delivery_callback(
                        result,
                        measured=measured,
                        sampled=sampled,
                        size=size,
                    ),
                    partition_key=generated.device_id,
                    sequence_id=generated.sequence_number,
                )
                self._record_enqueue(measured, sampled, len(value))
            except Exception as exc:
                self._record_send_failure(measured, sampled, exc)
            send_pattern.after_send(message_index)
            message_index += 1

        self.metrics.send_loop_end_time_unix = time.time()
        self.metrics.send_loop_end_time_monotonic = time.monotonic()
        self.metrics.send_phase_completed = True
        self._send_phase_complete = True
        return self.metrics

    def flush_and_close(self) -> PulsarProducerMetrics:
        """Flush producer acknowledgements, snapshot the queue, and close."""
        if not self._send_phase_complete:
            raise RuntimeError("Pulsar producer flush requires a completed send phase")
        if self._closed:
            return self.metrics
        with self._metrics_lock:
            self.metrics.delivery_callbacks_seen_at_flush_start = self.metrics.delivery_callbacks_seen
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
        try:
            self.producer.flush()
            self.metrics.flush_completed = True
        except Exception as exc:
            self._count_error(self.metrics.delivery_error_counts, exc)
        with self._metrics_lock:
            self.metrics.flush_remaining_messages = max(
                0,
                self.metrics.messages_enqueued - self.metrics.delivery_callbacks_seen,
            )
        self.metrics.end_time_unix = time.time()
        self.metrics.end_time_monotonic = time.monotonic()
        try:
            self.producer.close()
        finally:
            self.client.close()
            self._closed = True
        return self.metrics

    def _record_attempt(self, measured: bool, size: int) -> None:
        if measured:
            self.metrics.messages_attempted += 1
            self.metrics.bytes_attempted += size
        else:
            self.metrics.warmup_messages_attempted += 1
            self.metrics.warmup_bytes_attempted += size

    def _record_enqueue(self, measured: bool, sampled: bool, size: int) -> None:
        with self._metrics_lock:
            if measured:
                self.metrics.messages_enqueued += 1
                self.metrics.bytes_enqueued += size
                if sampled:
                    self.metrics.latency_samples_enqueued += 1
            else:
                self.metrics.warmup_messages_enqueued += 1
                self.metrics.warmup_bytes_enqueued += size

    def _record_send_failure(self, measured: bool, sampled: bool, error: Exception) -> None:
        with self._metrics_lock:
            if measured:
                self.metrics.messages_failed += 1
                if sampled:
                    self.metrics.latency_samples_failed += 1
            else:
                self.metrics.warmup_messages_failed += 1
            self._count_error(self.metrics.produce_error_counts, error)

    def _delivery_callback(
        self,
        result: Any,
        *,
        measured: bool,
        sampled: bool,
        size: int,
    ) -> None:
        with self._metrics_lock:
            if measured:
                self.metrics.delivery_callbacks_seen += 1
            else:
                self.metrics.warmup_delivery_callbacks_seen += 1
            if result != pulsar.Result.Ok:
                if measured:
                    self.metrics.messages_failed += 1
                    if sampled:
                        self.metrics.latency_samples_failed += 1
                else:
                    self.metrics.warmup_messages_failed += 1
                key = str(result)
                self.metrics.delivery_error_counts[key] = self.metrics.delivery_error_counts.get(key, 0) + 1
                return
            if measured:
                self.metrics.messages_delivered += 1
                self.metrics.bytes_delivered += size
                if sampled:
                    self.metrics.latency_samples_delivered += 1
            else:
                self.metrics.warmup_messages_delivered += 1
                self.metrics.warmup_bytes_delivered += size

    @staticmethod
    def _count_error(target: dict[str, int], error: object) -> None:
        key = type(error).__name__ if isinstance(error, Exception) else str(error)
        target[key] = target.get(key, 0) + 1


class PulsarConsumerWorker:
    def __init__(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        case_id: str,
        clock_offset_ns: int,
    ) -> None:
        self.config = config
        self.rank = rank
        self.clock_offset_ns = int(clock_offset_ns)
        settings = config.backend_settings
        consumer_settings = settings["consumer"]
        self.client = pulsar.Client(bootstrap_servers)
        subscription = f"{settings['subscription_name']}-{case_id}"
        self.consumer = self.client.subscribe(
            settings["topic_name"],
            subscription_name=subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            consumer_name=f"benchmark-consumer-{rank}",
            receiver_queue_size=int(consumer_settings["receiver_queue_size"]),
            max_total_receiver_queue_size_across_partitions=int(
                consumer_settings["max_total_receiver_queue_size_across_partitions"]
            ),
            negative_ack_redelivery_delay_ms=int(
                consumer_settings["negative_ack_redelivery_delay_ms"]
            ),
            initial_position=pulsar.InitialPosition.Earliest,
        )
        self.metrics = PulsarConsumerMetrics(rank=rank)
        self._prepared = False
        self._run_started = False
        self._closed = False
        self._drain_stage = "measurement"
        self._last_positions: dict[tuple[str, int], tuple[int, int, int]] = {}

    def prepare(self, timeout_sec: float | None = None) -> None:
        if self._prepared:
            return
        timeout = float(timeout_sec or 30.0)
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if self.consumer.is_connected():
                self._prepared = True
                self.metrics.readiness_prepared = True
                self.metrics.readiness_assignment_received = True
                self.metrics.readiness_assignment_count = 1
                self.metrics.readiness_elapsed_sec = time.monotonic() - start
                self.metrics.readiness_note = "Pulsar Shared consumer connected"
                return
            time.sleep(0.1)
        raise TimeoutError(f"Pulsar consumer rank {self.rank} was not ready")

    def run(
        self,
        duration_sec: int | None = None,
        prepared: bool = False,
        close_after_run: bool = True,
    ) -> PulsarConsumerMetrics:
        self.run_measurement_phase(duration_sec=duration_sec, prepared=prepared)
        self.begin_post_flush_drain(
            delivered_target_records=0,
            coordinated=False,
        )
        deadline = time.monotonic() + self.config.drain_timeout_sec
        self._consume_until(deadline)
        self.complete_post_flush_drain(
            reason="legacy_timeout",
            globally_consumed_records=self.total_measurement_records,
        )
        if close_after_run:
            self.close()
        else:
            self.metrics.end_time_unix = time.time()
            self.metrics.end_time_monotonic = time.monotonic()
        return self.metrics

    @property
    def total_measurement_records(self) -> int:
        return self.metrics.messages_received + self.metrics.late_drained_messages

    def run_measurement_phase(
        self,
        duration_sec: int | None = None,
        prepared: bool = False,
    ) -> PulsarConsumerMetrics:
        """Consume warm-up and measurement records, without final drain."""
        if self._run_started:
            raise RuntimeError("Pulsar consumer measurement phase already started")
        if not prepared and not self._prepared:
            self.prepare()
        active_duration = (
            self.config.duration_sec if duration_sec is None else duration_sec
        )
        self.metrics.benchmark_assignment_observed = self.consumer.is_connected()
        self.metrics.benchmark_assignment_count_max = 1 if self.metrics.benchmark_assignment_observed else 0
        self.metrics.run_start_time_unix = time.time()
        self.metrics.run_start_time_monotonic = time.monotonic()
        self.metrics.start_time_monotonic = self.metrics.run_start_time_monotonic + self.config.warmup_sec
        self.metrics.start_time_unix = self.metrics.run_start_time_unix + self.config.warmup_sec
        self.metrics.measurement_end_time_monotonic = self.metrics.start_time_monotonic + active_duration
        self.metrics.measurement_end_time_unix = self.metrics.start_time_unix + active_duration
        self._run_started = True
        self._drain_stage = "measurement"
        self._consume_until(self.metrics.measurement_end_time_monotonic)
        return self.metrics

    def poll_during_producer_flush(self, max_duration_sec: float = 0.1) -> None:
        """Keep consuming while producer ranks wait for delivery callbacks."""
        self._drain_stage = "pre_flush"
        self._consume_until(time.monotonic() + max(0.001, max_duration_sec))

    def begin_post_flush_drain(
        self,
        *,
        delivered_target_records: int,
        coordinated: bool,
    ) -> None:
        """Start a drain window after every producer has completed flush."""
        now_unix = time.time()
        now_monotonic = time.monotonic()
        self._drain_stage = "post_flush"
        self.metrics.coordinated_drain_enabled = coordinated
        self.metrics.coordinated_delivered_target_records = max(
            0,
            int(delivered_target_records),
        )
        self.metrics.post_flush_drain_start_time_unix = now_unix
        self.metrics.drain_end_time_monotonic = (
            now_monotonic + self.config.drain_timeout_sec
        )
        self.metrics.drain_end_time_unix = (
            now_unix + self.config.drain_timeout_sec
        )
        self.metrics.drain_completion_reason = "running"

    def drain_step(self, max_duration_sec: float = 0.25) -> None:
        """Consume one bounded post-flush drain interval."""
        self._drain_stage = "post_flush"
        self._consume_until(time.monotonic() + max(0.001, max_duration_sec))

    def complete_post_flush_drain(
        self,
        *,
        reason: str,
        globally_consumed_records: int,
    ) -> None:
        """Record why the synchronized drain stopped."""
        now_unix = time.time()
        now_monotonic = time.monotonic()
        self.metrics.post_flush_drain_end_time_unix = now_unix
        self.metrics.drain_end_time_unix = now_unix
        self.metrics.drain_end_time_monotonic = now_monotonic
        self.metrics.coordinated_consumed_records = max(
            0,
            int(globally_consumed_records),
        )
        self.metrics.drain_completion_reason = reason
        self.metrics.end_time_unix = now_unix
        self.metrics.end_time_monotonic = now_monotonic

    def _consume_until(self, deadline_monotonic: float) -> None:
        while time.monotonic() < deadline_monotonic:
            remaining_ms = max(
                1,
                min(500, int((deadline_monotonic - time.monotonic()) * 1000)),
            )
            try:
                message = self.consumer.receive(timeout_millis=remaining_ms)
            except pulsar.Timeout:
                self.metrics.poll_timeouts += 1
                continue
            except Exception:
                self.metrics.messages_failed += 1
                continue
            self.metrics.messages_polled += 1
            self._handle_message(message)
            self.consumer.acknowledge(message)

    def _handle_message(self, message: Any) -> None:
        value = message.data()
        if not self.config.record_envelope_enabled:
            self.metrics.messages_received += 1
            self.metrics.bytes_received += len(value)
            return
        try:
            envelope = decode_record_envelope(value)
        except ValueError:
            self.metrics.invalid_envelope_count += 1
            return
        if not envelope.measurement_record:
            self.metrics.warmup_messages_received += 1
            self.metrics.warmup_bytes_received += len(value)
            return
        self._record_position(message)
        if time.monotonic() <= self.metrics.measurement_end_time_monotonic:
            self.metrics.messages_received += 1
            self.metrics.bytes_received += len(value)
        else:
            self.metrics.late_drained_messages += 1
            self.metrics.late_drained_bytes += len(value)
            if self._drain_stage == "pre_flush":
                self.metrics.pre_flush_drained_messages += 1
                self.metrics.pre_flush_drained_bytes += len(value)
            elif self._drain_stage == "post_flush":
                self.metrics.post_flush_drained_messages += 1
                self.metrics.post_flush_drained_bytes += len(value)
        if envelope.latency_sampled:
            self.metrics.latency_histogram.record_ns(
                time.time_ns() + self.clock_offset_ns - envelope.send_time_ns
            )

    def _record_position(self, message: Any) -> None:
        message_id = message.message_id()
        key = (str(message.topic_name()), int(message_id.partition()))
        position = (
            int(message_id.ledger_id()),
            int(message_id.entry_id()),
            int(message_id.batch_index()),
        )
        previous = self._last_positions.get(key)
        if previous == position:
            self.metrics.duplicate_offset_count += 1
        elif previous is not None and position < previous:
            self.metrics.out_of_order_offset_count += 1
        if previous is None or position > previous:
            self._last_positions[key] = position

    def close(self) -> None:
        if self._closed:
            return
        connected = self.consumer.is_connected()
        self.metrics.final_assignment_received = connected
        self.metrics.final_assignment_count = 1 if connected else 0
        try:
            self.consumer.close()
        finally:
            self.client.close()
            self._closed = True
        self.metrics.end_time_unix = time.time()
        self.metrics.end_time_monotonic = time.monotonic()
