from __future__ import annotations

from dataclasses import dataclass
import math


@dataclass(slots=True)
class CoordinatedDrainState:
    """Deterministic stop state for a post-flush distributed drain."""

    delivered_target_records: int
    timeout_sec: float
    step_sec: float = 0.25
    consumed_records: int = 0
    steps_completed: int = 0
    completion_reason: str = "running"

    def __post_init__(self) -> None:
        if self.delivered_target_records < 0:
            raise ValueError("delivered_target_records must not be negative")
        if self.timeout_sec < 0:
            raise ValueError("timeout_sec must not be negative")
        if self.step_sec <= 0:
            raise ValueError("step_sec must be greater than 0")
        self.observe(self.consumed_records)

    @property
    def max_steps(self) -> int:
        return int(math.ceil(self.timeout_sec / self.step_sec))

    @property
    def complete(self) -> bool:
        return (
            self.delivered_target_records > 0
            and self.consumed_records >= self.delivered_target_records
        )

    @property
    def should_continue(self) -> bool:
        return (
            self.completion_reason == "running"
            and self.steps_completed < self.max_steps
        )

    def observe(self, consumed_records: int) -> None:
        """Record a global consumed count without advancing drain time."""
        if consumed_records < 0:
            raise ValueError("consumed_records must not be negative")
        self.consumed_records = consumed_records
        if self.delivered_target_records <= 0:
            self.completion_reason = "no_delivered_record_target"
        elif self.complete:
            self.completion_reason = "delivered_target_reached"
        elif self.steps_completed >= self.max_steps:
            self.completion_reason = "drain_timeout"
        else:
            self.completion_reason = "running"

    def record_step(self, consumed_records: int) -> None:
        """Advance one bounded drain interval and update the stop reason."""
        self.steps_completed += 1
        self.observe(consumed_records)
