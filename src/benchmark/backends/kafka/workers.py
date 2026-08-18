"""Compatibility boundary for the existing Kafka worker implementations."""

from src.benchmark.consumer_worker import ConsumerWorker
from src.benchmark.producer_worker import ProducerWorker

__all__ = ["ConsumerWorker", "ProducerWorker"]
