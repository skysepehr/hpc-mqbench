from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from models.benchmark_config import BenchmarkConfig


class Role(str, Enum):
    """
    Supported MPI rank roles in the benchmark.
    """

    CONTROLLER = "controller"
    PRODUCER = "producer"
    CONSUMER = "consumer"


@dataclass(slots=True)
class RoleAssignment:
    """
    Describes the role assigned to one MPI rank.
    """

    rank: int
    role: Role
    producer_index: int | None = None
    consumer_index: int | None = None


def assign_role(rank: int, config: BenchmarkConfig) -> RoleAssignment:
    """
    Assign one MPI rank to controller, producer, or consumer.

    Mapping:
    - rank 0 -> controller
    - next P ranks -> producers
    - next C ranks -> consumers
    """
    if rank == 0:
        return RoleAssignment(rank=rank, role=Role.CONTROLLER)

    # Producer ranks are assigned immediately after rank 0.
    producer_start = 1
    producer_end = producer_start + config.producer_ranks - 1

    if producer_start <= rank <= producer_end:
        producer_index = rank - producer_start
        return RoleAssignment(
            rank=rank,
            role=Role.PRODUCER,
            producer_index=producer_index,
        )

    # Consumer ranks come after producer ranks.
    consumer_start = producer_end + 1
    consumer_end = consumer_start + config.consumer_ranks - 1

    if consumer_start <= rank <= consumer_end:
        consumer_index = rank - consumer_start
        return RoleAssignment(
            rank=rank,
            role=Role.CONSUMER,
            consumer_index=consumer_index,
        )

    raise ValueError(
        f"Rank {rank} is out of range for total configured MPI ranks "
        f"({config.total_mpi_ranks})"
    )


def active_roles_for_scenario(config: BenchmarkConfig) -> set[Role]:
    """
    Return the worker roles that should run for the configured scenario.

    Role assignment and scenario execution are separate concerns: a rank may be
    assigned as a producer or consumer, but that role can be idle in scenarios
    that intentionally exercise only ingress or only egress.
    """
    if config.scenario == "ingress_ramp":
        return {Role.PRODUCER}

    if config.scenario == "egress_only":
        return {Role.CONSUMER}

    if config.scenario in {"simultaneous", "consume_and_process"}:
        return {Role.PRODUCER, Role.CONSUMER}

    raise ValueError(f"Unsupported scenario: {config.scenario}")
