from __future__ import annotations

import bisect
import struct
import zlib
from dataclasses import dataclass, field
from typing import Any, Iterable


ENVELOPE_MAGIC = b"KBL2"
ENVELOPE_VERSION = 1
ENVELOPE_SIZE_BYTES = 32
FLAG_MEASUREMENT = 1 << 0
FLAG_LATENCY_SAMPLED = 1 << 1
_ENVELOPE_STRUCT = struct.Struct(">4sBBHIQQI")


@dataclass(frozen=True, slots=True)
class RecordEnvelope:
    producer_rank: int
    sequence: int
    send_time_ns: int
    measurement_record: bool
    latency_sampled: bool

    @property
    def record_id(self) -> tuple[int, int]:
        return self.producer_rank, self.sequence


def encode_record_envelope(
    payload: bytes,
    *,
    producer_rank: int,
    sequence: int,
    send_time_ns: int,
    measurement_record: bool,
    latency_sampled: bool,
) -> bytes:
    """
    Replace the first 32 payload bytes with a binary benchmark envelope.

    The returned payload is exactly the same size as the input payload. The
    envelope therefore adds instrumentation without changing the configured
    Kafka record value size.
    """
    if len(payload) < ENVELOPE_SIZE_BYTES:
        raise ValueError(
            f"payload must be at least {ENVELOPE_SIZE_BYTES} bytes for latency"
        )
    if producer_rank < 0 or producer_rank > 0xFFFFFFFF:
        raise ValueError("producer_rank is outside uint32 range")
    if sequence < 0 or sequence > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("sequence is outside uint64 range")
    if send_time_ns < 0 or send_time_ns > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("send_time_ns is outside uint64 range")

    flags = 0
    if measurement_record:
        flags |= FLAG_MEASUREMENT
    if latency_sampled:
        flags |= FLAG_LATENCY_SAMPLED

    checksum_input = struct.pack(
        ">IQQB",
        producer_rank,
        sequence,
        send_time_ns,
        flags,
    )
    checksum = zlib.crc32(checksum_input) & 0xFFFFFFFF
    envelope = _ENVELOPE_STRUCT.pack(
        ENVELOPE_MAGIC,
        ENVELOPE_VERSION,
        flags,
        0,
        producer_rank,
        sequence,
        send_time_ns,
        checksum,
    )
    return envelope + payload[ENVELOPE_SIZE_BYTES:]


def decode_record_envelope(payload: bytes) -> RecordEnvelope:
    if len(payload) < ENVELOPE_SIZE_BYTES:
        raise ValueError("payload is shorter than the benchmark envelope")

    (
        magic,
        version,
        flags,
        _reserved,
        producer_rank,
        sequence,
        send_time_ns,
        checksum,
    ) = _ENVELOPE_STRUCT.unpack(payload[:ENVELOPE_SIZE_BYTES])
    if magic != ENVELOPE_MAGIC:
        raise ValueError("payload does not contain a V2 benchmark envelope")
    if version != ENVELOPE_VERSION:
        raise ValueError(f"unsupported benchmark envelope version: {version}")

    checksum_input = struct.pack(
        ">IQQB",
        producer_rank,
        sequence,
        send_time_ns,
        flags,
    )
    expected = zlib.crc32(checksum_input) & 0xFFFFFFFF
    if checksum != expected:
        raise ValueError("benchmark envelope checksum mismatch")

    latency_sampled = bool(flags & FLAG_LATENCY_SAMPLED)
    if latency_sampled and send_time_ns <= 0:
        raise ValueError("sampled envelope has no send timestamp")

    return RecordEnvelope(
        producer_rank=producer_rank,
        sequence=sequence,
        send_time_ns=send_time_ns,
        measurement_record=bool(flags & FLAG_MEASUREMENT),
        latency_sampled=latency_sampled,
    )


def update_measurement_offset_order(
    last_offsets: dict[tuple[str, int], int],
    *,
    measurement_record: bool,
    topic: str,
    partition: int,
    offset: int,
) -> str:
    """
    Update per-partition offset evidence for a measurement record.

    Warm-up records are deliberately excluded because consumer-group formation
    can reassign partitions before the synchronized measurement window.
    """
    if not measurement_record:
        return "warmup_ignored"

    key = (topic, partition)
    previous = last_offsets.get(key)
    result = "ordered"
    if previous is not None and offset == previous:
        result = "duplicate"
    elif previous is not None and offset < previous:
        result = "out_of_order"
    last_offsets[key] = max(
        offset,
        previous if previous is not None else offset,
    )
    return result


def default_latency_bucket_upper_bounds_us() -> tuple[int, ...]:
    """
    Return bounded, deterministic microsecond histogram bucket limits.

    Resolution is 1 us through 1 ms, 10 us through 10 ms, 100 us through
    100 ms, 1 ms through 1 s, 10 ms through 10 s, and 100 ms through 60 s.
    """
    values: list[int] = list(range(1, 1_001))
    values.extend(range(1_010, 10_001, 10))
    values.extend(range(10_100, 100_001, 100))
    values.extend(range(101_000, 1_000_001, 1_000))
    values.extend(range(1_010_000, 10_000_001, 10_000))
    values.extend(range(10_100_000, 60_000_001, 100_000))
    return tuple(values)


@dataclass(slots=True)
class LatencyHistogram:
    bucket_upper_bounds_us: tuple[int, ...] = field(
        default_factory=default_latency_bucket_upper_bounds_us
    )
    bucket_counts: list[int] = field(init=False)
    count: int = 0
    sum_ns: int = 0
    min_ns: int = 0
    max_ns: int = 0
    overflow_count: int = 0
    negative_count: int = 0

    def __post_init__(self) -> None:
        if not self.bucket_upper_bounds_us:
            raise ValueError("latency histogram requires at least one bucket")
        if tuple(sorted(set(self.bucket_upper_bounds_us))) != self.bucket_upper_bounds_us:
            raise ValueError("latency histogram bucket limits must be unique and sorted")
        self.bucket_counts = [0] * len(self.bucket_upper_bounds_us)

    def record_ns(self, latency_ns: int) -> bool:
        if latency_ns < 0:
            self.negative_count += 1
            return False

        latency_us = latency_ns / 1_000.0
        index = bisect.bisect_left(self.bucket_upper_bounds_us, latency_us)
        if index >= len(self.bucket_counts):
            self.overflow_count += 1
        else:
            self.bucket_counts[index] += 1

        self.count += 1
        self.sum_ns += latency_ns
        if self.min_ns == 0 or latency_ns < self.min_ns:
            self.min_ns = latency_ns
        self.max_ns = max(self.max_ns, latency_ns)
        return True

    def merge(self, other: "LatencyHistogram") -> None:
        if other.bucket_upper_bounds_us != self.bucket_upper_bounds_us:
            raise ValueError("cannot merge latency histograms with different buckets")
        self.bucket_counts = [
            left + right
            for left, right in zip(self.bucket_counts, other.bucket_counts)
        ]
        self.count += other.count
        self.sum_ns += other.sum_ns
        self.overflow_count += other.overflow_count
        self.negative_count += other.negative_count
        if other.min_ns > 0:
            if self.min_ns <= 0:
                self.min_ns = other.min_ns
            else:
                self.min_ns = min(self.min_ns, other.min_ns)
        self.max_ns = max(self.max_ns, other.max_ns)

    def percentile_ns(self, percentile: float) -> int | None:
        if self.count <= 0:
            return None
        if percentile < 0 or percentile > 100:
            raise ValueError("percentile must be between 0 and 100")

        target = max(1, int((percentile / 100.0) * self.count + 0.999999999))
        cumulative = 0
        for upper_us, count in zip(
            self.bucket_upper_bounds_us,
            self.bucket_counts,
        ):
            cumulative += count
            if cumulative >= target:
                return upper_us * 1_000
        return self.max_ns

    def to_dict(self) -> dict[str, Any]:
        nonzero_buckets = [
            [upper_us, count]
            for upper_us, count in zip(
                self.bucket_upper_bounds_us,
                self.bucket_counts,
            )
            if count
        ]
        return {
            "format": "latency_histogram.v1",
            "unit": "nanoseconds",
            "count": self.count,
            "mean_ns": (self.sum_ns / self.count) if self.count else None,
            "min_ns": self.min_ns if self.count else None,
            "p50_ns": self.percentile_ns(50.0),
            "p95_ns": self.percentile_ns(95.0),
            "p99_ns": self.percentile_ns(99.0),
            "p999_ns": self.percentile_ns(99.9),
            "max_ns": self.max_ns if self.count else None,
            "sum_ns": self.sum_ns,
            "negative_count": self.negative_count,
            "overflow_count": self.overflow_count,
            "nonzero_buckets_us": nonzero_buckets,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LatencyHistogram":
        histogram = cls()
        histogram.count = int(payload.get("count", 0))
        histogram.sum_ns = int(payload.get("sum_ns", 0))
        histogram.min_ns = int(payload.get("min_ns") or 0)
        histogram.max_ns = int(payload.get("max_ns") or 0)
        histogram.negative_count = int(payload.get("negative_count", 0))
        histogram.overflow_count = int(payload.get("overflow_count", 0))
        by_upper = {
            int(upper): int(count)
            for upper, count in payload.get("nonzero_buckets_us", [])
        }
        histogram.bucket_counts = [
            by_upper.get(upper, 0) for upper in histogram.bucket_upper_bounds_us
        ]
        return histogram

    @classmethod
    def merged(cls, payloads: Iterable[dict[str, Any]]) -> "LatencyHistogram":
        result = cls()
        for payload in payloads:
            if isinstance(payload, dict):
                result.merge(cls.from_dict(payload))
        return result
