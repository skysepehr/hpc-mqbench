from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Any


CLOCK_PING_TAG = 27180
CLOCK_PONG_TAG = 27181


@dataclass(frozen=True, slots=True)
class ClockCalibration:
    rank: int
    offset_ns: int
    uncertainty_ns: int
    median_rtt_ns: int
    sample_count: int

    def to_dict(self) -> dict[str, int]:
        return {
            "rank": self.rank,
            "offset_ns": self.offset_ns,
            "uncertainty_ns": self.uncertainty_ns,
            "median_rtt_ns": self.median_rtt_ns,
            "sample_count": self.sample_count,
        }


def estimate_clock_offset(
    samples: list[tuple[int, int, int]],
    *,
    rank: int,
) -> ClockCalibration:
    """
    Estimate reference-minus-local clock offset from ping-pong timestamps.

    Each sample is `(reference_send_ns, remote_receive_ns,
    reference_receive_ns)`. The sample with the lowest round-trip time bounds
    scheduling noise most tightly; its half RTT is the uncertainty estimate.
    """
    if not samples:
        raise ValueError("clock calibration requires at least one sample")

    normalized: list[tuple[int, int]] = []
    for reference_send_ns, remote_receive_ns, reference_receive_ns in samples:
        rtt_ns = reference_receive_ns - reference_send_ns
        if rtt_ns < 0:
            raise ValueError("clock calibration sample has a negative RTT")
        midpoint_ns = reference_send_ns + (rtt_ns // 2)
        offset_ns = midpoint_ns - remote_receive_ns
        normalized.append((rtt_ns, offset_ns))

    best_rtt_ns, best_offset_ns = min(normalized, key=lambda item: item[0])
    return ClockCalibration(
        rank=rank,
        offset_ns=best_offset_ns,
        uncertainty_ns=(best_rtt_ns + 1) // 2,
        median_rtt_ns=int(statistics.median(item[0] for item in normalized)),
        sample_count=len(normalized),
    )


def calibrate_mpi_clocks(comm: Any, sample_count: int) -> dict[int, ClockCalibration]:
    """
    Calibrate every MPI rank's wall clock against rank 0.

    Rank 0 serializes the exchanges to avoid cross-rank message ambiguity.
    Results are broadcast so every rank can convert local wall-clock timestamps
    to the same rank-0 reference.
    """
    if sample_count <= 0:
        raise ValueError("sample_count must be greater than 0")

    rank = int(comm.Get_rank())
    world_size = int(comm.Get_size())
    serialized: dict[int, dict[str, int]] | None = None

    if rank == 0:
        calibrations: dict[int, ClockCalibration] = {
            0: ClockCalibration(
                rank=0,
                offset_ns=0,
                uncertainty_ns=0,
                median_rtt_ns=0,
                sample_count=sample_count,
            )
        }
        for remote_rank in range(1, world_size):
            samples: list[tuple[int, int, int]] = []
            for _ in range(sample_count):
                reference_send_ns = time.time_ns()
                comm.send(None, dest=remote_rank, tag=CLOCK_PING_TAG)
                remote_receive_ns = int(
                    comm.recv(source=remote_rank, tag=CLOCK_PONG_TAG)
                )
                reference_receive_ns = time.time_ns()
                samples.append(
                    (
                        reference_send_ns,
                        remote_receive_ns,
                        reference_receive_ns,
                    )
                )
            calibrations[remote_rank] = estimate_clock_offset(
                samples,
                rank=remote_rank,
            )
        serialized = {
            item_rank: calibration.to_dict()
            for item_rank, calibration in calibrations.items()
        }
    else:
        for _ in range(sample_count):
            comm.recv(source=0, tag=CLOCK_PING_TAG)
            comm.send(time.time_ns(), dest=0, tag=CLOCK_PONG_TAG)

    serialized = comm.bcast(serialized, root=0)
    return {
        item_rank: ClockCalibration(**payload)
        for item_rank, payload in serialized.items()
    }


def assess_clock_calibration(
    before: ClockCalibration,
    after: ClockCalibration,
    *,
    max_uncertainty_us: float,
    max_drift_us: float,
) -> dict[str, Any]:
    drift_ns = abs(after.offset_ns - before.offset_ns)
    uncertainty_ns = max(before.uncertainty_ns, after.uncertainty_ns)
    valid = (
        uncertainty_ns <= int(max_uncertainty_us * 1_000)
        and drift_ns <= int(max_drift_us * 1_000)
    )
    return {
        "rank": before.rank,
        "valid": valid,
        "before": before.to_dict(),
        "after": after.to_dict(),
        "max_uncertainty_ns": uncertainty_ns,
        "drift_ns": drift_ns,
        "limits": {
            "max_uncertainty_us": max_uncertainty_us,
            "max_drift_us": max_drift_us,
        },
    }
