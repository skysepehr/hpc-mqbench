from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ProducerMetricsModel:
    """
    Structured producer-side metrics.
    """

    producer_rank_count: int = 0
    messages_attempted: int = 0
    messages_delivered: int = 0
    messages_failed: int = 0
    bytes_attempted: int = 0
    bytes_delivered: int = 0
    delivery_callbacks_seen: int = 0
    virtual_devices_assigned: int = 0
    estimated_unique_virtual_devices_attempted: int = 0
    estimated_unique_virtual_devices_delivered: int = 0
    virtual_device_coverage_attempted_ratio: float = 0.0
    virtual_device_coverage_delivered_ratio: float = 0.0
    duration_sec: float = 0.0
    throughput_msgs_per_sec: float = 0.0
    throughput_bytes_per_sec: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "producer_rank_count": self.producer_rank_count,
            "messages_attempted": self.messages_attempted,
            "messages_delivered": self.messages_delivered,
            "messages_failed": self.messages_failed,
            "bytes_attempted": self.bytes_attempted,
            "bytes_delivered": self.bytes_delivered,
            "delivery_callbacks_seen": self.delivery_callbacks_seen,
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
            "duration_sec": self.duration_sec,
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
        }


@dataclass(slots=True)
class ConsumerMetricsModel:
    """
    Structured consumer-side metrics.
    """

    consumer_rank_count: int = 0
    messages_polled: int = 0
    messages_received: int = 0
    messages_failed: int = 0
    bytes_received: int = 0
    poll_timeouts: int = 0
    processing_operations: int = 0
    duration_sec: float = 0.0
    throughput_msgs_per_sec: float = 0.0
    throughput_bytes_per_sec: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "consumer_rank_count": self.consumer_rank_count,
            "messages_polled": self.messages_polled,
            "messages_received": self.messages_received,
            "messages_failed": self.messages_failed,
            "bytes_received": self.bytes_received,
            "poll_timeouts": self.poll_timeouts,
            "processing_operations": self.processing_operations,
            "duration_sec": self.duration_sec,
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
            "throughput_bytes_per_sec": self.throughput_bytes_per_sec,
        }


@dataclass(slots=True)
class MetricsModel:
    """
    Top-level benchmark metrics model.
    """

    world_size: int = 0
    active_broker_count: int = 0
    total_simulated_devices: int = 0

    producers: ProducerMetricsModel = field(default_factory=ProducerMetricsModel)
    consumers: ConsumerMetricsModel = field(default_factory=ConsumerMetricsModel)

    def to_dict(self) -> dict[str, Any]:
        return {
            "world_size": self.world_size,
            "active_broker_count": self.active_broker_count,
            "total_simulated_devices": self.total_simulated_devices,
            "producers": self.producers.to_dict(),
            "consumers": self.consumers.to_dict(),
        }
