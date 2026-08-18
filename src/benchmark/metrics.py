from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.benchmark.client_stats import aggregate_librdkafka_stats
from src.benchmark.record_envelope import LatencyHistogram


def _min_positive(current: float, value: Any) -> float:
    parsed = float(value or 0.0)
    if parsed <= 0:
        return current
    if current <= 0:
        return parsed
    return min(current, parsed)


def _merge_counts(target: dict[str, int], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        try:
            count = int(value)
        except (TypeError, ValueError):
            continue
        text_key = str(key)
        target[text_key] = target.get(text_key, 0) + count


@dataclass(slots=True)
class AggregatedProducerMetrics:
    """
    Aggregated metrics across all producer ranks.
    """

    producer_rank_count: int = 0
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
    delivery_callbacks_during_flush: int = 0
    messages_delivered_at_flush_start: int = 0
    messages_delivered_during_flush: int = 0
    messages_failed_at_flush_start: int = 0
    messages_failed_during_flush: int = 0
    pending_messages_at_flush_start: int = 0
    pending_bytes_at_flush_start: int = 0
    producer_queue_len_at_flush_start: int = 0
    producer_queue_len_after_flush: int = 0
    flush_remaining_messages: int = 0
    produce_error_counts: dict[str, int] = field(default_factory=dict)
    delivery_error_counts: dict[str, int] = field(default_factory=dict)
    virtual_devices_assigned: int = 0
    estimated_unique_virtual_devices_attempted: int = 0
    estimated_unique_virtual_devices_delivered: int = 0
    max_duration_sec: float = 0.0
    max_send_loop_duration_sec: float = 0.0
    max_flush_duration_sec: float = 0.0
    first_start_time_unix: float = 0.0
    last_start_time_unix: float = 0.0
    last_send_loop_end_time_unix: float = 0.0
    last_end_time_unix: float = 0.0
    librdkafka_stats: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput_msgs_per_sec(self) -> float:
        if self.max_duration_sec <= 0:
            return 0.0
        return self.messages_delivered / self.max_duration_sec

    @property
    def throughput_bytes_per_sec(self) -> float:
        if self.max_duration_sec <= 0:
            return 0.0
        return self.bytes_delivered / self.max_duration_sec

    @property
    def throughput_megabytes_per_sec(self) -> float:
        return self.throughput_bytes_per_sec / 1_000_000

    @property
    def throughput_mebibytes_per_sec(self) -> float:
        return self.throughput_bytes_per_sec / 1_048_576

    @property
    def send_attempt_throughput_msgs_per_sec(self) -> float:
        if self.max_send_loop_duration_sec <= 0:
            return 0.0
        return self.messages_attempted / self.max_send_loop_duration_sec

    @property
    def send_attempt_throughput_bytes_per_sec(self) -> float:
        if self.max_send_loop_duration_sec <= 0:
            return 0.0
        return self.bytes_attempted / self.max_send_loop_duration_sec

    @property
    def send_attempt_throughput_megabytes_per_sec(self) -> float:
        return self.send_attempt_throughput_bytes_per_sec / 1_000_000

    @property
    def send_attempt_throughput_mebibytes_per_sec(self) -> float:
        return self.send_attempt_throughput_bytes_per_sec / 1_048_576

    @property
    def delivery_callback_rate_during_flush_per_sec(self) -> float:
        if self.max_flush_duration_sec <= 0:
            return 0.0
        return self.delivery_callbacks_during_flush / self.max_flush_duration_sec

    @property
    def virtual_device_coverage_attempted_ratio(self) -> float:
        if self.virtual_devices_assigned <= 0:
            return 0.0
        return (
            self.estimated_unique_virtual_devices_attempted
            / self.virtual_devices_assigned
        )

    @property
    def virtual_device_coverage_delivered_ratio(self) -> float:
        if self.virtual_devices_assigned <= 0:
            return 0.0
        return (
            self.estimated_unique_virtual_devices_delivered
            / self.virtual_devices_assigned
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "producer_rank_count": self.producer_rank_count,
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
            "virtual_devices_assigned": self.virtual_devices_assigned,
            "estimated_unique_virtual_devices_attempted": (
                self.estimated_unique_virtual_devices_attempted
            ),
            "estimated_unique_virtual_devices_delivered": (
                self.estimated_unique_virtual_devices_delivered
            ),
            "virtual_device_coverage_attempted_ratio": (
                self.virtual_device_coverage_attempted_ratio
            ),
            "virtual_device_coverage_delivered_ratio": (
                self.virtual_device_coverage_delivered_ratio
            ),
            "max_duration_sec": self.max_duration_sec,
            "max_send_loop_duration_sec": self.max_send_loop_duration_sec,
            "max_flush_duration_sec": self.max_flush_duration_sec,
            "first_start_time_unix": self.first_start_time_unix,
            "last_start_time_unix": self.last_start_time_unix,
            "last_send_loop_end_time_unix": self.last_send_loop_end_time_unix,
            "last_end_time_unix": self.last_end_time_unix,
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


@dataclass(slots=True)
class AggregatedConsumerMetrics:
    """
    Aggregated metrics across all consumer ranks.
    """

    consumer_rank_count: int = 0
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
    latency_histogram: dict[str, Any] = field(default_factory=dict)
    poll_timeouts: int = 0
    processing_operations: int = 0
    readiness_prepared_count: int = 0
    readiness_assignment_count: int = 0
    readiness_assignment_received_count: int = 0
    readiness_strict_assignment_count: int = 0
    benchmark_assignment_observed_count: int = 0
    benchmark_assignment_count_max: int = 0
    final_assignment_received_count: int = 0
    final_assignment_count: int = 0
    max_readiness_elapsed_sec: float = 0.0
    max_duration_sec: float = 0.0
    first_start_time_unix: float = 0.0
    last_start_time_unix: float = 0.0
    last_end_time_unix: float = 0.0
    first_post_flush_drain_start_time_unix: float = 0.0
    last_post_flush_drain_end_time_unix: float = 0.0
    coordinated_drain_rank_count: int = 0
    coordinated_delivered_target_records: int = 0
    coordinated_consumed_records: int = 0
    drain_completion_reason_counts: dict[str, int] = field(default_factory=dict)
    librdkafka_stats: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput_msgs_per_sec(self) -> float:
        if self.max_duration_sec <= 0:
            return 0.0
        return self.messages_received / self.max_duration_sec

    @property
    def throughput_bytes_per_sec(self) -> float:
        if self.max_duration_sec <= 0:
            return 0.0
        return self.bytes_received / self.max_duration_sec

    @property
    def throughput_megabytes_per_sec(self) -> float:
        return self.throughput_bytes_per_sec / 1_000_000

    @property
    def throughput_mebibytes_per_sec(self) -> float:
        return self.throughput_bytes_per_sec / 1_048_576

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer_rank_count": self.consumer_rank_count,
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
            "duplicate_or_regressed_offset_count": (
                self.duplicate_offset_count + self.out_of_order_offset_count
            ),
            "latency_histogram": dict(self.latency_histogram),
            "poll_timeouts": self.poll_timeouts,
            "processing_operations": self.processing_operations,
            "readiness_prepared_count": self.readiness_prepared_count,
            "readiness_assignment_count": self.readiness_assignment_count,
            "readiness_assignment_received_count": (
                self.readiness_assignment_received_count
            ),
            "readiness_strict_assignment_count": self.readiness_strict_assignment_count,
            "benchmark_assignment_observed_count": (
                self.benchmark_assignment_observed_count
            ),
            "benchmark_assignment_count_max": self.benchmark_assignment_count_max,
            "final_assignment_received_count": self.final_assignment_received_count,
            "final_assignment_count": self.final_assignment_count,
            "max_readiness_elapsed_sec": self.max_readiness_elapsed_sec,
            "max_duration_sec": self.max_duration_sec,
            "first_start_time_unix": self.first_start_time_unix,
            "last_start_time_unix": self.last_start_time_unix,
            "last_end_time_unix": self.last_end_time_unix,
            "first_post_flush_drain_start_time_unix": (
                self.first_post_flush_drain_start_time_unix
            ),
            "last_post_flush_drain_end_time_unix": (
                self.last_post_flush_drain_end_time_unix
            ),
            "coordinated_drain_rank_count": self.coordinated_drain_rank_count,
            "coordinated_delivered_target_records": (
                self.coordinated_delivered_target_records
            ),
            "coordinated_consumed_records": self.coordinated_consumed_records,
            "drain_completion_reason_counts": dict(
                self.drain_completion_reason_counts
            ),
            "librdkafka_stats": dict(self.librdkafka_stats),
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
            "throughput_megabytes_per_sec": self.throughput_megabytes_per_sec,
            "throughput_mebibytes_per_sec": self.throughput_mebibytes_per_sec,
        }


@dataclass(slots=True)
class MetricsAggregator:
    """
    Aggregate producer and consumer metrics from JSON-friendly dictionaries.

    This is intentionally simple and works well with data gathered through MPI.
    """

    def aggregate_from_dicts(
        self,
        producer_metrics_list: list[dict[str, Any]],
        consumer_metrics_list: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Aggregate already-serialized producer and consumer metrics.
        """
        producers = AggregatedProducerMetrics()
        consumers = AggregatedConsumerMetrics()
        producer_librdkafka_stats: list[dict[str, Any]] = []
        consumer_librdkafka_stats: list[dict[str, Any]] = []
        consumer_latency_histograms: list[dict[str, Any]] = []

        for metrics in producer_metrics_list:
            producers.producer_rank_count += 1
            producers.messages_attempted += int(metrics.get("messages_attempted", 0))
            producers.messages_enqueued += int(metrics.get("messages_enqueued", 0))
            producers.messages_delivered += int(metrics.get("messages_delivered", 0))
            producers.messages_failed += int(metrics.get("messages_failed", 0))
            producers.bytes_attempted += int(metrics.get("bytes_attempted", 0))
            producers.bytes_enqueued += int(metrics.get("bytes_enqueued", 0))
            producers.bytes_delivered += int(metrics.get("bytes_delivered", 0))
            producers.warmup_messages_attempted += int(
                metrics.get("warmup_messages_attempted", 0)
            )
            producers.warmup_messages_enqueued += int(
                metrics.get("warmup_messages_enqueued", 0)
            )
            producers.warmup_messages_delivered += int(
                metrics.get("warmup_messages_delivered", 0)
            )
            producers.warmup_messages_failed += int(
                metrics.get("warmup_messages_failed", 0)
            )
            producers.warmup_bytes_attempted += int(
                metrics.get("warmup_bytes_attempted", 0)
            )
            producers.warmup_bytes_enqueued += int(
                metrics.get("warmup_bytes_enqueued", 0)
            )
            producers.warmup_bytes_delivered += int(
                metrics.get("warmup_bytes_delivered", 0)
            )
            producers.latency_envelopes_written += int(
                metrics.get("latency_envelopes_written", 0)
            )
            producers.latency_samples_written += int(
                metrics.get("latency_samples_written", 0)
            )
            producers.latency_samples_enqueued += int(
                metrics.get("latency_samples_enqueued", 0)
            )
            producers.latency_samples_delivered += int(
                metrics.get("latency_samples_delivered", 0)
            )
            producers.latency_samples_failed += int(
                metrics.get("latency_samples_failed", 0)
            )
            producers.delivery_callbacks_seen += int(
                metrics.get("delivery_callbacks_seen", 0)
            )
            producers.warmup_delivery_callbacks_seen += int(
                metrics.get("warmup_delivery_callbacks_seen", 0)
            )
            producers.delivery_callbacks_seen_at_flush_start += int(
                metrics.get("delivery_callbacks_seen_at_flush_start", 0)
            )
            producers.delivery_callbacks_during_flush += int(
                metrics.get("delivery_callbacks_during_flush", 0)
            )
            producers.messages_delivered_at_flush_start += int(
                metrics.get("messages_delivered_at_flush_start", 0)
            )
            producers.messages_delivered_during_flush += int(
                metrics.get("messages_delivered_during_flush", 0)
            )
            producers.messages_failed_at_flush_start += int(
                metrics.get("messages_failed_at_flush_start", 0)
            )
            producers.messages_failed_during_flush += int(
                metrics.get("messages_failed_during_flush", 0)
            )
            producers.pending_messages_at_flush_start += int(
                metrics.get("pending_messages_at_flush_start", 0)
            )
            producers.pending_bytes_at_flush_start += int(
                metrics.get("pending_bytes_at_flush_start", 0)
            )
            producers.producer_queue_len_at_flush_start += int(
                metrics.get("producer_queue_len_at_flush_start", 0)
            )
            producers.producer_queue_len_after_flush += int(
                metrics.get("producer_queue_len_after_flush", 0)
            )
            producers.flush_remaining_messages += int(
                metrics.get("flush_remaining_messages", 0)
            )
            _merge_counts(
                producers.produce_error_counts,
                metrics.get("produce_error_counts"),
            )
            _merge_counts(
                producers.delivery_error_counts,
                metrics.get("delivery_error_counts"),
            )
            producers.virtual_devices_assigned += int(
                metrics.get("virtual_devices_assigned", 0)
            )
            producers.estimated_unique_virtual_devices_attempted += int(
                metrics.get("estimated_unique_virtual_devices_attempted", 0)
            )
            producers.estimated_unique_virtual_devices_delivered += int(
                metrics.get("estimated_unique_virtual_devices_delivered", 0)
            )
            producers.max_duration_sec = max(
                producers.max_duration_sec,
                float(metrics.get("duration_sec", 0.0)),
            )
            producers.max_send_loop_duration_sec = max(
                producers.max_send_loop_duration_sec,
                float(metrics.get("send_loop_duration_sec", 0.0)),
            )
            producers.max_flush_duration_sec = max(
                producers.max_flush_duration_sec,
                float(metrics.get("flush_duration_sec", 0.0)),
            )
            producers.first_start_time_unix = _min_positive(
                producers.first_start_time_unix,
                metrics.get("start_time_unix"),
            )
            producers.last_start_time_unix = max(
                producers.last_start_time_unix,
                float(metrics.get("start_time_unix", 0.0)),
            )
            producers.last_send_loop_end_time_unix = max(
                producers.last_send_loop_end_time_unix,
                float(metrics.get("send_loop_end_time_unix", 0.0)),
            )
            producers.last_end_time_unix = max(
                producers.last_end_time_unix,
                float(metrics.get("end_time_unix", 0.0)),
            )
            stats = metrics.get("librdkafka_stats")
            if isinstance(stats, dict):
                producer_librdkafka_stats.append(stats)

        for metrics in consumer_metrics_list:
            consumers.consumer_rank_count += 1
            consumers.messages_polled += int(metrics.get("messages_polled", 0))
            consumers.messages_received += int(metrics.get("messages_received", 0))
            consumers.messages_failed += int(metrics.get("messages_failed", 0))
            consumers.bytes_received += int(metrics.get("bytes_received", 0))
            consumers.warmup_messages_received += int(
                metrics.get("warmup_messages_received", 0)
            )
            consumers.warmup_bytes_received += int(
                metrics.get("warmup_bytes_received", 0)
            )
            consumers.late_drained_messages += int(
                metrics.get("late_drained_messages", 0)
            )
            consumers.late_drained_bytes += int(
                metrics.get("late_drained_bytes", 0)
            )
            consumers.pre_flush_drained_messages += int(
                metrics.get("pre_flush_drained_messages", 0)
            )
            consumers.pre_flush_drained_bytes += int(
                metrics.get("pre_flush_drained_bytes", 0)
            )
            consumers.post_flush_drained_messages += int(
                metrics.get("post_flush_drained_messages", 0)
            )
            consumers.post_flush_drained_bytes += int(
                metrics.get("post_flush_drained_bytes", 0)
            )
            consumers.invalid_envelope_count += int(
                metrics.get("invalid_envelope_count", 0)
            )
            consumers.duplicate_offset_count += int(
                metrics.get("duplicate_offset_count", 0)
            )
            consumers.out_of_order_offset_count += int(
                metrics.get("out_of_order_offset_count", 0)
            )
            histogram = metrics.get("latency_histogram")
            if isinstance(histogram, dict):
                consumer_latency_histograms.append(histogram)
            consumers.poll_timeouts += int(metrics.get("poll_timeouts", 0))
            consumers.processing_operations += int(
                metrics.get("processing_operations", 0)
            )
            if bool(metrics.get("readiness_prepared", False)):
                consumers.readiness_prepared_count += 1
            if bool(metrics.get("readiness_assignment_received", False)):
                consumers.readiness_assignment_received_count += 1
            if bool(metrics.get("readiness_strict_assignment", False)):
                consumers.readiness_strict_assignment_count += 1
            if bool(metrics.get("benchmark_assignment_observed", False)):
                consumers.benchmark_assignment_observed_count += 1
            if bool(metrics.get("final_assignment_received", False)):
                consumers.final_assignment_received_count += 1
            consumers.readiness_assignment_count += int(
                metrics.get("readiness_assignment_count", 0)
            )
            consumers.benchmark_assignment_count_max += int(
                metrics.get("benchmark_assignment_count_max", 0)
            )
            consumers.final_assignment_count += int(
                metrics.get("final_assignment_count", 0)
            )
            consumers.max_readiness_elapsed_sec = max(
                consumers.max_readiness_elapsed_sec,
                float(metrics.get("readiness_elapsed_sec", 0.0)),
            )
            consumers.max_duration_sec = max(
                consumers.max_duration_sec,
                float(metrics.get("duration_sec", 0.0)),
            )
            consumers.first_start_time_unix = _min_positive(
                consumers.first_start_time_unix,
                metrics.get("start_time_unix"),
            )
            consumers.last_start_time_unix = max(
                consumers.last_start_time_unix,
                float(metrics.get("start_time_unix", 0.0)),
            )
            consumers.last_end_time_unix = max(
                consumers.last_end_time_unix,
                float(metrics.get("end_time_unix", 0.0)),
            )
            consumers.first_post_flush_drain_start_time_unix = _min_positive(
                consumers.first_post_flush_drain_start_time_unix,
                metrics.get("post_flush_drain_start_time_unix"),
            )
            consumers.last_post_flush_drain_end_time_unix = max(
                consumers.last_post_flush_drain_end_time_unix,
                float(metrics.get("post_flush_drain_end_time_unix", 0.0)),
            )
            if bool(metrics.get("coordinated_drain_enabled", False)):
                consumers.coordinated_drain_rank_count += 1
            consumers.coordinated_delivered_target_records = max(
                consumers.coordinated_delivered_target_records,
                int(metrics.get("coordinated_delivered_target_records", 0)),
            )
            consumers.coordinated_consumed_records = max(
                consumers.coordinated_consumed_records,
                int(metrics.get("coordinated_consumed_records", 0)),
            )
            drain_reason = str(
                metrics.get("drain_completion_reason", "not_recorded")
            )
            consumers.drain_completion_reason_counts[drain_reason] = (
                consumers.drain_completion_reason_counts.get(drain_reason, 0)
                + 1
            )
            stats = metrics.get("librdkafka_stats")
            if isinstance(stats, dict):
                consumer_librdkafka_stats.append(stats)

        producers.librdkafka_stats = aggregate_librdkafka_stats(
            producer_librdkafka_stats,
        )
        consumers.librdkafka_stats = aggregate_librdkafka_stats(
            consumer_librdkafka_stats,
        )
        consumers.latency_histogram = LatencyHistogram.merged(
            consumer_latency_histograms
        ).to_dict()

        consumed_with_drain = (
            consumers.messages_received + consumers.late_drained_messages
        )
        missing_after_drain = max(
            0,
            producers.messages_delivered - consumed_with_drain,
        )
        unexplained_surplus = max(
            0,
            consumed_with_drain - producers.messages_delivered,
        )

        return {
            "producers": producers.to_dict(),
            "consumers": consumers.to_dict(),
            "record_correctness": {
                "producer_delivered_measurement_records": (
                    producers.messages_delivered
                ),
                "consumer_received_in_measurement_records": (
                    consumers.messages_received
                ),
                "consumer_late_drained_records": consumers.late_drained_messages,
                "consumer_received_through_drain_records": consumed_with_drain,
                "missing_after_drain_records": missing_after_drain,
                "unexplained_surplus_records": unexplained_surplus,
                "invalid_envelope_count": consumers.invalid_envelope_count,
                "duplicate_offset_count": consumers.duplicate_offset_count,
                "out_of_order_offset_count": consumers.out_of_order_offset_count,
                "duplicate_or_regressed_offset_count": (
                    consumers.duplicate_offset_count
                    + consumers.out_of_order_offset_count
                ),
            },
        }
