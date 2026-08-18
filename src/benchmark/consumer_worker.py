from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from src.benchmark.local_deps import ensure_repo_local_dependencies

ensure_repo_local_dependencies()

try:
    from confluent_kafka import Consumer, KafkaError, Message
except ImportError as exc:
    raise RuntimeError(
        "Consumer workers require confluent-kafka. Install requirements.txt or "
        "run ./scripts/install_local_confluent_kafka.sh to build the vendored "
        "source package into .local/python."
    ) from exc

from models.benchmark_config import BenchmarkConfig
from src.benchmark.client_stats import (
    LibrdkafkaStatsTracker,
    disabled_librdkafka_stats_summary,
    librdkafka_stats_enabled,
)
from src.benchmark.kafka_client import KafkaClientFactory, KafkaConnectionSettings
from src.benchmark.record_envelope import (
    LatencyHistogram,
    decode_record_envelope,
    update_measurement_offset_order,
)


@dataclass(slots=True)
class ConsumerMetrics:
    """
    Metrics collected by one consumer worker.

    These are local metrics for one MPI consumer rank. Later, the controller
    rank can gather and aggregate them across all consumer ranks.
    """

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
    end_time_unix: float = 0.0
    post_flush_drain_start_time_unix: float = 0.0
    post_flush_drain_end_time_unix: float = 0.0
    coordinated_drain_enabled: bool = False
    coordinated_delivered_target_records: int = 0
    coordinated_consumed_records: int = 0
    drain_completion_reason: str = "not_started"
    librdkafka_stats: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_sec(self) -> float:
        """
        Measured consumer duration, excluding warm-up and drain.
        """
        if self.measurement_end_time_monotonic > self.start_time_monotonic:
            return self.measurement_end_time_monotonic - self.start_time_monotonic
        if self.end_time_monotonic > self.start_time_monotonic:
            return self.end_time_monotonic - self.start_time_monotonic
        return 0.0

    @property
    def throughput_msgs_per_sec(self) -> float:
        """
        Successfully received messages per second.
        """
        duration = self.duration_sec
        if duration <= 0:
            return 0.0
        return self.messages_received / duration

    @property
    def throughput_bytes_per_sec(self) -> float:
        """
        Successfully received bytes per second.
        """
        duration = self.duration_sec
        if duration <= 0:
            return 0.0
        return self.bytes_received / duration

    @property
    def throughput_megabytes_per_sec(self) -> float:
        """
        Successfully received megabytes per second using decimal MB.
        """
        return self.throughput_bytes_per_sec / 1_000_000

    @property
    def throughput_mebibytes_per_sec(self) -> float:
        """Successfully received mebibytes per second."""
        return self.throughput_bytes_per_sec / 1_048_576

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the metrics into a JSON-friendly dictionary.
        """
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
            "duplicate_or_regressed_offset_count": (
                self.duplicate_offset_count + self.out_of_order_offset_count
            ),
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
            "end_time_unix": self.end_time_unix,
            "post_flush_drain_start_time_unix": (
                self.post_flush_drain_start_time_unix
            ),
            "post_flush_drain_end_time_unix": self.post_flush_drain_end_time_unix,
            "coordinated_drain_enabled": self.coordinated_drain_enabled,
            "coordinated_delivered_target_records": (
                self.coordinated_delivered_target_records
            ),
            "coordinated_consumed_records": self.coordinated_consumed_records,
            "drain_completion_reason": self.drain_completion_reason,
            "librdkafka_stats": dict(self.librdkafka_stats),
            "duration_sec": self.duration_sec,
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
            "throughput_megabytes_per_sec": self.throughput_megabytes_per_sec,
            "throughput_mebibytes_per_sec": self.throughput_mebibytes_per_sec,
        }


class ConsumerWorker:
    """
    One Kafka consumer worker.

    This class combines:
    - one Kafka consumer client
    - one topic subscription
    - one consuming loop
    - one local metrics object

    In the MPI design, each consumer rank will create one ConsumerWorker.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        case_id: str,
        clock_offset_ns: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        config:
            Validated benchmark configuration for this case.

        rank:
            MPI rank of this consumer worker.

        bootstrap_servers:
            Comma-separated Kafka bootstrap servers.

        case_id:
            Case identifier used for building a case-specific consumer group.
        """
        self.config = config
        self.rank = rank
        self.case_id = case_id
        self.clock_offset_ns = int(clock_offset_ns)

        connection = KafkaConnectionSettings(
            bootstrap_servers=bootstrap_servers,
            client_id=KafkaClientFactory.build_consumer_client_id(rank),
        )

        # Build a case-specific consumer group so benchmark runs do not
        # accidentally share offsets across unrelated runs.
        group_id = KafkaClientFactory.build_consumer_group_id(case_id)
        self.stats_tracker = (
            LibrdkafkaStatsTracker(role="consumer", rank=rank)
            if librdkafka_stats_enabled(config.extra)
            else None
        )

        self.consumer: Consumer = KafkaClientFactory(
            config=config,
            connection=connection,
        ).create_consumer(
            group_id=group_id,
            stats_cb=self.stats_tracker.record if self.stats_tracker is not None else None,
        )

        self.metrics = ConsumerMetrics(rank=rank)
        self._is_prepared = False
        self._run_started = False
        self._closed = False
        self._drain_stage = "measurement"
        self._last_offsets: dict[tuple[str, int], int] = {}

    def prepare(self, timeout_sec: float | None = None) -> None:
        """
        Subscribe and poll long enough for the consumer group to join.

        Simultaneous producer/consumer scenarios need this before producers
        start sending, otherwise early messages can arrive before consumers have
        partition assignments. The minimum warmup avoids false failures when a
        consumer is valid but receives no partitions because there are more
        consumers than partitions.
        """
        if self._is_prepared:
            return

        timeout = timeout_sec
        if timeout is None:
            timeout = float(self.config.extra.get("consumer_readiness_timeout_sec", 30.0))

        min_poll_sec = float(self.config.extra.get("consumer_readiness_min_poll_sec", 2.0))
        strict_assignment = self._extra_bool(
            "consumer_readiness_require_assignment",
            default=False,
        )
        start_time = time.monotonic()
        deadline = start_time + timeout

        self.metrics.readiness_strict_assignment = strict_assignment
        self.consumer.subscribe([self.config.topic_name])

        while time.monotonic() < deadline:
            msg = self.consumer.poll(timeout=0.2)
            self._record_readiness_message_if_benchmark_started(msg)
            assignment = self.consumer.assignment()
            assignment_count = len(assignment)
            elapsed_sec = time.monotonic() - start_time

            self.metrics.readiness_elapsed_sec = elapsed_sec
            self.metrics.readiness_assignment_count = assignment_count

            if assignment_count > 0:
                self.metrics.readiness_prepared = True
                self.metrics.readiness_assignment_received = True
                self.metrics.readiness_note = "consumer received partition assignment"
                self._is_prepared = True
                return

            if elapsed_sec >= min_poll_sec and not strict_assignment:
                self.metrics.readiness_prepared = True
                self.metrics.readiness_assignment_received = False
                self.metrics.readiness_note = (
                    "consumer joined readiness window without partition assignment"
                )
                self._is_prepared = True
                return

        self.metrics.readiness_prepared = False
        self.metrics.readiness_note = (
            "consumer readiness timed out before partition assignment"
            if strict_assignment
            else "consumer readiness timed out"
        )
        raise TimeoutError(
            f"Consumer rank {self.rank} was not ready within {timeout:.1f}s"
        )

    def run(
        self,
        duration_sec: int | None = None,
        prepared: bool = False,
        close_after_run: bool = True,
    ) -> ConsumerMetrics:
        """
        Run the consumer loop for one benchmark duration.

        Main logic:
        1. subscribe to the benchmark topic
        2. poll repeatedly until the configured duration is reached
        3. count received messages and bytes
        4. optionally process messages later
        5. optionally close the consumer cleanly
        6. return local consumer metrics

        The MPI controller defers close until every consumer has finished its
        drain window. This prevents early ranks from leaving the consumer group
        and triggering a final partition reassignment for ranks that are still
        accounting for measurement records.
        """
        self.run_measurement_phase(duration_sec=duration_sec, prepared=prepared)
        self.begin_post_flush_drain(
            delivered_target_records=0,
            coordinated=False,
        )
        self._consume_until(time.monotonic() + self.config.drain_timeout_sec)
        self.complete_post_flush_drain(
            reason="legacy_timeout",
            globally_consumed_records=self.total_measurement_records,
        )
        if close_after_run:
            self.close()
        return self.metrics

    @property
    def total_measurement_records(self) -> int:
        return self.metrics.messages_received + self.metrics.late_drained_messages

    def run_measurement_phase(
        self,
        duration_sec: int | None = None,
        prepared: bool = False,
    ) -> ConsumerMetrics:
        """Consume warm-up and measurement records without the final drain."""
        if self._run_started:
            raise RuntimeError("Kafka consumer measurement phase already started")
        if not prepared and not self._is_prepared:
            self.prepare()
        self._record_benchmark_assignment()
        active_duration_sec = (
            self.config.duration_sec if duration_sec is None else duration_sec
        )
        self.metrics.run_start_time_unix = time.time()
        self.metrics.run_start_time_monotonic = time.monotonic()
        self.metrics.start_time_monotonic = (
            self.metrics.run_start_time_monotonic + self.config.warmup_sec
        )
        self.metrics.start_time_unix = (
            self.metrics.run_start_time_unix + self.config.warmup_sec
        )
        self.metrics.measurement_end_time_monotonic = (
            self.metrics.start_time_monotonic + active_duration_sec
        )
        self.metrics.measurement_end_time_unix = (
            self.metrics.start_time_unix + active_duration_sec
        )
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
        """Start the bounded drain after every producer has completed flush."""
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
        """Record the synchronized drain completion boundary and reason."""
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
            timeout = max(
                0.001,
                min(0.5, deadline_monotonic - time.monotonic()),
            )
            msg = self.consumer.poll(timeout=timeout)
            self.metrics.messages_polled += 1
            if (
                self.metrics.messages_polled % 1000 == 0
                or not self.metrics.benchmark_assignment_observed
            ):
                self._record_benchmark_assignment()
            if msg is None:
                self.metrics.poll_timeouts += 1
                continue
            if msg.error():
                self._handle_message_error(msg)
                continue
            self._handle_message(msg, received_monotonic=time.monotonic())

    def _record_benchmark_assignment(self) -> None:
        """
        Track whether this consumer eventually received partitions while active.

        Readiness can intentionally proceed without an assignment when there are
        more consumers than partitions. Recording the active-loop assignment
        separately keeps the report honest without making readiness brittle.
        """
        try:
            assignment_count = len(self.consumer.assignment())
        except Exception:
            return

        self.metrics.benchmark_assignment_count_max = max(
            self.metrics.benchmark_assignment_count_max,
            assignment_count,
        )
        if assignment_count > 0:
            self.metrics.benchmark_assignment_observed = True

    def _record_final_assignment(self) -> None:
        """
        Capture the final assignment state before the consumer leaves its group.
        """
        try:
            assignment_count = len(self.consumer.assignment())
        except Exception:
            assignment_count = 0

        self.metrics.final_assignment_count = assignment_count
        self.metrics.final_assignment_received = assignment_count > 0

    def _record_readiness_message_if_benchmark_started(
        self,
        msg: Message | None,
    ) -> None:
        """
        Preserve messages returned by readiness polls during egress-only runs.

        When a topic is prefilled, the first poll used to obtain assignment can
        also return a real message. If the benchmark timer has already started,
        that message belongs to the measured consume window and must be counted
        instead of silently discarded.
        """
        if msg is None or self.metrics.start_time_monotonic <= 0:
            return

        self.metrics.messages_polled += 1
        if msg.error():
            self._handle_message_error(msg)
            return

        self._handle_message(msg, received_monotonic=time.monotonic())

    def close(self) -> None:
        """
        Close the underlying Kafka consumer outside the normal run loop.

        The controller uses this when readiness fails before the benchmark starts.
        """
        if self._closed:
            return
        self._record_final_assignment()
        self.metrics.librdkafka_stats = (
            self.stats_tracker.summary()
            if self.stats_tracker is not None
            else disabled_librdkafka_stats_summary(role="consumer", rank=self.rank)
        )
        self.consumer.close()
        self._closed = True
        self.metrics.end_time_unix = time.time()
        self.metrics.end_time_monotonic = time.monotonic()

    def _handle_message(
        self,
        msg: Message,
        *,
        received_monotonic: float | None = None,
    ) -> None:
        """
        Process one successfully received Kafka message.

        For now this method only records metrics.
        Later, optional message processing can be inserted here.
        """
        value = msg.value()
        if value is None:
            return

        received_monotonic = (
            received_monotonic
            if received_monotonic is not None
            else time.monotonic()
        )
        if not self.config.record_envelope_enabled:
            self.metrics.messages_received += 1
            self.metrics.bytes_received += len(value)
            self._process_message_if_enabled(value)
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

        self._record_offset_order(
            msg,
            measurement_record=envelope.measurement_record,
        )
        if received_monotonic <= self.metrics.measurement_end_time_monotonic:
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
            receive_time_ns = time.time_ns() + self.clock_offset_ns
            self.metrics.latency_histogram.record_ns(
                receive_time_ns - envelope.send_time_ns
            )
        self._process_message_if_enabled(value)

    def _record_offset_order(
        self,
        msg: Message,
        *,
        measurement_record: bool,
    ) -> None:
        try:
            topic = str(msg.topic())
            partition = int(msg.partition())
            offset = int(msg.offset())
        except Exception:
            return
        result = update_measurement_offset_order(
            self._last_offsets,
            measurement_record=measurement_record,
            topic=topic,
            partition=partition,
            offset=offset,
        )
        if result == "duplicate":
            self.metrics.duplicate_offset_count += 1
        elif result == "out_of_order":
            self.metrics.out_of_order_offset_count += 1

    def _process_message_if_enabled(self, value: bytes) -> None:
        """
        Simulate lightweight consumer-side work for consume_and_process cases.

        This keeps processing deterministic and local to the benchmark client. It
        is intentionally simple so Kafka I/O remains the primary benchmark focus.
        """
        if self.config.scenario != "consume_and_process":
            return

        iterations = int(self.config.extra.get("consumer_processing_iterations", 1))
        checksum = 0
        for _ in range(max(1, iterations)):
            checksum ^= sum(value) & 0xFFFF

        # Store only the operation count. The checksum keeps the loop from being
        # optimized away while avoiding large per-message output.
        self.metrics.processing_operations += max(1, iterations)

    def _handle_message_error(self, msg: Message) -> None:
        """
        Handle Kafka message-level errors.

        Not every error means the benchmark should crash. For example,
        end-of-partition events may be reported as informational conditions.
        """
        error = msg.error()

        # Partition EOF is informational in many benchmark situations.
        if error is not None and error.code() == KafkaError._PARTITION_EOF:
            return

        self.metrics.messages_failed += 1

    def _extra_bool(self, key: str, default: bool = False) -> bool:
        """
        Parse optional boolean config values from JSON booleans or simple strings.
        """
        value = self.config.extra.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)
