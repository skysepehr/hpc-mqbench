from __future__ import annotations

import time
from dataclasses import dataclass

from models.benchmark_config import BenchmarkConfig


class BaseSendPatternController:
    """
    Base class for producer send-pattern control.

    A send pattern controls *when* messages are sent, not *what* the messages are.

    The producer loop calls:
    - before_send(message_index)
    - send one message
    - after_send(message_index)

    Different implementations can:
    - sleep before sending
    - create bursts
    - create quiet windows
    - emulate batched timing
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        self.config = config

    def before_send(self, message_index: int) -> None:
        """
        Hook called immediately before one message send attempt.

        Default implementation does nothing.
        """
        return

    def after_send(self, message_index: int) -> None:
        """
        Hook called immediately after one message send attempt.

        Default implementation does nothing.
        """
        return


class SteadySendPatternController(BaseSendPatternController):
    """
    Steady sending pattern.

    This pattern tries to send continuously without inserting artificial delays.
    It is the simplest pattern and acts like a baseline.
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        super().__init__(config)
        self.start_time = time.monotonic()
        self.records_per_sec = (
            float(config.target_records_per_sec) / config.producer_ranks
            if config.target_records_per_sec is not None and config.producer_ranks > 0
            else None
        )

    def before_send(self, message_index: int) -> None:
        if self.records_per_sec is None:
            return

        target_time = self.start_time + (message_index / self.records_per_sec)
        remaining = target_time - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)


class BatchedSendPatternController(BaseSendPatternController):
    """
    Batched sending pattern.

    This pattern emulates a producer that sends in small groups, then pauses briefly.
    It does not batch at the Kafka API level by itself; Kafka batching is still
    controlled separately through settings like batch.size and linger.ms.

    This pattern only changes the *timing* of message emission.
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        super().__init__(config)

        # Number of sends between short pauses.
        #
        # This is a lightweight first implementation. Later this can become
        # configurable from the benchmark config.
        self.messages_per_timing_batch = 100

        # Pause duration after each timing batch.
        self.pause_sec = 0.01

    def after_send(self, message_index: int) -> None:
        """
        After each timing batch, sleep a little to simulate grouped emission.
        """
        if (message_index + 1) % self.messages_per_timing_batch == 0:
            time.sleep(self.pause_sec)


class BurstSendPatternController(BaseSendPatternController):
    """
    Burst sending pattern.

    This pattern alternates between:
    - an active burst phase
    - a quiet pause phase

    This is useful for workloads that are not smooth and continuous.
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        super().__init__(config)

        # Number of messages sent in one burst before pausing.
        self.burst_size = 500

        # Pause duration after each burst.
        self.pause_sec = 0.05

    def after_send(self, message_index: int) -> None:
        """
        After each burst, insert a quiet period.
        """
        if (message_index + 1) % self.burst_size == 0:
            time.sleep(self.pause_sec)


class WindowedSendPatternController(BaseSendPatternController):
    """
    Windowed sending pattern.

    This pattern alternates between:
    - an active send window
    - an inactive window

    Unlike the burst controller, which pauses after a fixed number of messages,
    this controller works in time windows.

    This is useful when you want messages to arrive during active intervals,
    then stop during quiet intervals.
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        super().__init__(config)

        # Active window duration in seconds.
        self.active_window_sec = 1.0

        # Quiet window duration in seconds.
        self.quiet_window_sec = 0.5

        # Benchmark-local reference time.
        self.start_time = time.monotonic()

    def before_send(self, message_index: int) -> None:
        """
        If the current time falls inside a quiet window, wait until the next
        active window begins.
        """
        now = time.monotonic()
        elapsed = now - self.start_time

        cycle_length = self.active_window_sec + self.quiet_window_sec
        position_in_cycle = elapsed % cycle_length

        # If we are in the quiet part of the cycle, sleep until the next active window.
        if position_in_cycle > self.active_window_sec:
            remaining_quiet_time = cycle_length - position_in_cycle
            time.sleep(remaining_quiet_time)


@dataclass(slots=True)
class SendPatternMetadata:
    """
    Small metadata object describing which send pattern was built.
    """

    name: str


def build_send_pattern_controller(
    config: BenchmarkConfig,
) -> BaseSendPatternController:
    """
    Factory function that creates the correct send-pattern controller.

    Parameters
    ----------
    config:
        Validated benchmark configuration containing send_pattern.

    Returns
    -------
    BaseSendPatternController
        Concrete controller matching the configured send pattern.
    """
    if config.send_pattern == "steady":
        return SteadySendPatternController(config)

    if config.send_pattern == "batched":
        return BatchedSendPatternController(config)

    if config.send_pattern == "burst":
        return BurstSendPatternController(config)

    if config.send_pattern == "windowed":
        return WindowedSendPatternController(config)

    raise ValueError(f"Unsupported send_pattern: {config.send_pattern}")
