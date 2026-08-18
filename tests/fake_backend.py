from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig
from src.benchmark.backends.base import BackendAdapter


@dataclass
class FakeMetrics:
    messages_attempted: int = 10
    messages_delivered: int = 10
    messages_received: int = 10
    bytes_delivered: int = 10240
    bytes_received: int = 10240
    duration_sec: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "messages_attempted": self.messages_attempted,
            "messages_delivered": self.messages_delivered,
            "messages_received": self.messages_received,
            "bytes_delivered": self.bytes_delivered,
            "bytes_received": self.bytes_received,
            "duration_sec": self.duration_sec,
        }


class FakeProducerWorker:
    def run(self) -> FakeMetrics:
        return FakeMetrics()


class FakeConsumerWorker:
    def __init__(self) -> None:
        self.metrics = FakeMetrics()

    def prepare(self) -> None:
        return None

    def run(self, **_: Any) -> FakeMetrics:
        return self.metrics

    def close(self) -> None:
        return None


class FakeBackendAdapter(BackendAdapter):
    backend_id = "fake"
    adapter_version = "test-1"
    qualification_policy_ids = frozenset({"qualification.fake.test"})

    def validate_config(self, config: BenchmarkConfig) -> None:
        if config.backend_id != self.backend_id:
            raise ValueError("fake backend received a different backend_id")
        if config.qualification_policy_id not in self.qualification_policy_ids:
            raise ValueError("unsupported fake qualification policy")

    def effective_settings(self, config: BenchmarkConfig) -> dict[str, Any]:
        return dict(config.backend_settings)

    def create_producer_worker(self, **_: Any) -> FakeProducerWorker:
        return FakeProducerWorker()

    def create_consumer_worker(self, **_: Any) -> FakeConsumerWorker:
        return FakeConsumerWorker()

    def lifecycle_script(self, action: str, project_root: Path) -> Path | None:
        if action in {
            "preflight",
            "start",
            "wait-ready",
            "check-health",
            "create-stream",
            "prefill",
            "start-monitoring",
            "stop-monitoring",
            "stop",
        }:
            return project_root / "tests" / "fake_backend_lifecycle.sh"
        return None

    def normalize_metrics(
        self,
        benchmark_result: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "fake_counter": int(
                benchmark_result.get("fake_counter", 0)
            )
        }

    def monitoring_endpoint_specs(
        self,
        config: BenchmarkConfig,
    ) -> dict[str, Any]:
        return {
            "fake_metrics": {
                "runtime_file": "runtime/fake_metrics_endpoint.txt",
                "stream_count": config.backend_settings.get("stream_count"),
            }
        }
