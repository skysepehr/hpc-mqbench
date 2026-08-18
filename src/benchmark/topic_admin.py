from __future__ import annotations

import argparse
import time
from typing import Any

from src.benchmark.local_deps import ensure_repo_local_dependencies


def _load_confluent_admin() -> tuple[Any, Any, Any, Any]:
    """
    Import confluent-kafka admin objects only when topic administration is used.
    """
    ensure_repo_local_dependencies()
    try:
        from confluent_kafka import KafkaException
        from confluent_kafka.admin import AdminClient, NewTopic
    except ImportError as exc:
        raise RuntimeError(
            "Topic administration requires confluent-kafka. "
            "Install requirements.txt or run "
            "./scripts/install_local_confluent_kafka.sh before running Kafka "
            "benchmarks."
        ) from exc

    return AdminClient, NewTopic, KafkaException, _load_kafka_error()


def _load_kafka_error() -> Any:
    ensure_repo_local_dependencies()
    try:
        from confluent_kafka import KafkaError
    except ImportError:
        return None
    return KafkaError


def _admin_client(bootstrap_servers: str) -> Any:
    AdminClient, _, _, _ = _load_confluent_admin()
    return AdminClient({"bootstrap.servers": bootstrap_servers})


def check_kafka_connection(bootstrap_servers: str, timeout_sec: float = 10.0) -> None:
    """
    Verify that Kafka metadata can be fetched from the bootstrap servers.
    """
    admin = _admin_client(bootstrap_servers)
    try:
        admin.list_topics(timeout=timeout_sec)
    except Exception as exc:
        raise RuntimeError(
            f"Could not connect to Kafka at {bootstrap_servers} within {timeout_sec}s"
        ) from exc


def topic_exists(
    bootstrap_servers: str,
    topic: str,
    timeout_sec: float = 10.0,
) -> bool:
    """
    Return True when the topic appears in Kafka metadata.
    """
    admin = _admin_client(bootstrap_servers)
    metadata = admin.list_topics(timeout=timeout_sec)
    return topic in metadata.topics


def wait_for_topic(
    bootstrap_servers: str,
    topic: str,
    exists: bool = True,
    timeout_sec: float = 30.0,
    poll_sec: float = 1.0,
) -> None:
    """
    Wait until a topic exists, or until it has disappeared after deletion.
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if topic_exists(bootstrap_servers, topic, timeout_sec=min(5.0, timeout_sec)) == exists:
            return
        time.sleep(poll_sec)

    state = "exist" if exists else "be deleted"
    raise TimeoutError(f"Timed out waiting for topic {topic!r} to {state}")


def create_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int,
    timeout_sec: float = 30.0,
) -> None:
    """
    Create one Kafka topic, treating an existing topic as success.
    """
    admin = _admin_client(bootstrap_servers)
    _, NewTopic, KafkaException, KafkaError = _load_confluent_admin()

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)]
    )

    try:
        futures[topic].result(timeout=timeout_sec)
    except KafkaException as exc:
        if _matches_error_code(exc, KafkaError, "TOPIC_ALREADY_EXISTS"):
            return
        raise RuntimeError(f"Failed to create topic {topic!r}") from exc

    wait_for_topic(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        exists=True,
        timeout_sec=timeout_sec,
    )


def delete_topic(
    bootstrap_servers: str,
    topic: str,
    timeout_sec: float = 30.0,
) -> None:
    """
    Delete one Kafka topic, treating an unknown topic as success.
    """
    admin = _admin_client(bootstrap_servers)
    _, _, KafkaException, KafkaError = _load_confluent_admin()

    futures = admin.delete_topics([topic], operation_timeout=timeout_sec)

    try:
        futures[topic].result(timeout=timeout_sec)
    except KafkaException as exc:
        if _matches_error_code(exc, KafkaError, "UNKNOWN_TOPIC_OR_PART"):
            return
        raise RuntimeError(f"Failed to delete topic {topic!r}") from exc

    wait_for_topic(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        exists=False,
        timeout_sec=timeout_sec,
    )


def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int,
    delete_first: bool = False,
    create: bool = True,
    timeout_sec: float = 30.0,
) -> None:
    """
    Optionally delete and/or create a topic for a benchmark run.
    """
    check_kafka_connection(bootstrap_servers, timeout_sec=timeout_sec)

    if delete_first:
        delete_topic(bootstrap_servers, topic, timeout_sec=timeout_sec)

    if create:
        create_topic(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            partitions=partitions,
            replication_factor=replication_factor,
            timeout_sec=timeout_sec,
        )


def _exception_code(exc: Exception, KafkaError: Any) -> int | None:
    """
    Extract a confluent-kafka error code from KafkaException when available.
    """
    if KafkaError is None or not getattr(exc, "args", None):
        return None

    error = exc.args[0]
    code_method = getattr(error, "code", None)
    if code_method is None:
        return None
    return code_method()


def _matches_error_code(exc: Exception, KafkaError: Any, code_name: str) -> bool:
    expected_code = getattr(KafkaError, code_name, None)
    if expected_code is None:
        return False
    return _exception_code(exc, KafkaError) == expected_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka topic administration helper")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--delete-first", action="store_true")
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.check_only:
        check_kafka_connection(args.bootstrap_servers, timeout_sec=args.timeout_sec)
        print(f"Kafka reachable at {args.bootstrap_servers}")
        return

    ensure_topic(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        delete_first=args.delete_first,
        create=args.create,
        timeout_sec=args.timeout_sec,
    )
    print(f"Topic is ready: {args.topic}")


if __name__ == "__main__":
    main()
