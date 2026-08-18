from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig
from src.benchmark.config_loader import load_experiment_case
from src.benchmark.local_deps import ensure_repo_local_dependencies
from src.benchmark.payload_generator import PayloadGenerator


@dataclass(slots=True)
class PrefillMetrics:
    """
    Metrics for one topic prefill operation.
    """

    topic: str
    messages_requested: int
    messages_attempted: int = 0
    messages_delivered: int = 0
    messages_failed: int = 0
    bytes_attempted: int = 0
    bytes_delivered: int = 0
    start_time_monotonic: float = 0.0
    end_time_monotonic: float = 0.0

    @property
    def duration_sec(self) -> float:
        if self.end_time_monotonic <= self.start_time_monotonic:
            return 0.0
        return self.end_time_monotonic - self.start_time_monotonic

    @property
    def throughput_msgs_per_sec(self) -> float:
        if self.duration_sec <= 0:
            return 0.0
        return self.messages_delivered / self.duration_sec

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "messages_requested": self.messages_requested,
            "messages_attempted": self.messages_attempted,
            "messages_delivered": self.messages_delivered,
            "messages_failed": self.messages_failed,
            "bytes_attempted": self.bytes_attempted,
            "bytes_delivered": self.bytes_delivered,
            "duration_sec": self.duration_sec,
            "throughput_msgs_per_sec": self.throughput_msgs_per_sec,
        }


def _load_confluent_producer() -> Any:
    """
    Import confluent-kafka only when the real prefill path is used.
    """
    ensure_repo_local_dependencies()
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "Topic prefill requires confluent-kafka. Install requirements.txt "
            "or run ./scripts/install_local_confluent_kafka.sh before running "
            "egress-only benchmarks."
        ) from exc
    return Producer


def default_prefill_message_count(config: BenchmarkConfig) -> int:
    """
    Choose a conservative default backlog size for egress-only benchmarks.
    """
    configured = config.extra.get("egress_prefill_messages")
    if configured is not None:
        return int(configured)

    if config.total_simulated_devices is not None and config.total_simulated_devices > 0:
        return int(config.total_simulated_devices)

    return max(1, config.consumer_ranks) * config.virtual_devices_per_rank


def prefill_topic(
    config: BenchmarkConfig,
    bootstrap_servers: str,
    message_count: int,
    client_id: str,
    progress_interval: int = 10_000,
) -> PrefillMetrics:
    """
    Produce a fixed backlog of messages before an egress-only benchmark.
    """
    if message_count <= 0:
        raise ValueError("message_count must be greater than 0")

    Producer = _load_confluent_producer()
    prefill_acks = str(config.extra.get("egress_prefill_acks", "all"))
    producer_config = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        # Prefill should favor correctness over matching benchmark producer
        # reliability settings, especially when the benchmark sweeps acks=0.
        "acks": prefill_acks,
        "compression.type": config.compression_type,
        "batch.size": config.batch_size,
        "linger.ms": config.linger_ms,
        "enable.idempotence": False,
    }
    common_config = config.extra.get("kafka_common_client_config", {})
    if isinstance(common_config, dict):
        producer_config.update(common_config)
    tuned_producer_config = config.extra.get("kafka_producer_config", {})
    if isinstance(tuned_producer_config, dict):
        producer_config.update(tuned_producer_config)
    producer_config["acks"] = prefill_acks
    producer_config["enable.idempotence"] = False
    producer = Producer(producer_config)

    payload_generator = PayloadGenerator(config=config, rank=0)
    metrics = PrefillMetrics(
        topic=config.topic_name,
        messages_requested=message_count,
        start_time_monotonic=time.monotonic(),
    )

    def delivery_callback(err: object, msg: object) -> None:
        if err is not None:
            metrics.messages_failed += 1
            return

        metrics.messages_delivered += 1
        value = msg.value()
        if value is not None:
            metrics.bytes_delivered += len(value)

    for message_index in range(message_count):
        generated = payload_generator.generate(message_index)
        key = generated.device_id.encode("utf-8")
        value = generated.payload_bytes

        while True:
            try:
                producer.produce(
                    topic=config.topic_name,
                    key=key,
                    value=value,
                    on_delivery=delivery_callback,
                )
                break
            except BufferError:
                producer.poll(0.1)

        metrics.messages_attempted += 1
        metrics.bytes_attempted += len(value)
        producer.poll(0)

        if progress_interval > 0 and (message_index + 1) % progress_interval == 0:
            print(
                f"[prefill] attempted={message_index + 1} "
                f"delivered={metrics.messages_delivered}",
                flush=True,
            )

    producer.flush(timeout=60.0)
    metrics.end_time_monotonic = time.monotonic()
    if metrics.messages_delivered != message_count:
        raise RuntimeError(
            "Topic prefill did not deliver all requested messages: "
            f"requested={message_count}, delivered={metrics.messages_delivered}, "
            f"failed={metrics.messages_failed}"
        )
    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prefill a Kafka topic for egress benchmarks")
    parser.add_argument("--config", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--case-id", default="prefill")
    parser.add_argument("--messages", type=int, default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--progress-interval", type=int, default=10_000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    case = load_experiment_case(
        path=args.config,
        case_id=args.case_id,
        case_name=args.case_id,
        output_dir=".",
    )

    message_count = (
        args.messages
        if args.messages is not None
        else default_prefill_message_count(case.config)
    )

    metrics = prefill_topic(
        config=case.config,
        bootstrap_servers=args.bootstrap_servers,
        message_count=message_count,
        client_id=f"benchmark-prefill-{args.case_id}",
        progress_interval=args.progress_interval,
    )

    result = {
        "case_id": args.case_id,
        "config": case.config.to_dict(),
        "prefill": metrics.to_dict(),
    }

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)

    print(json.dumps(result["prefill"], indent=2), flush=True)


if __name__ == "__main__":
    main()
