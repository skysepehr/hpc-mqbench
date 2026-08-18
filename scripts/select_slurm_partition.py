#!/usr/bin/env python3
"""Select a schedulable Slurm partition from an explicit candidate set."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import re
import subprocess
import sys
from typing import Iterable


DEFAULT_CANDIDATES = ("standard96s", "medium96s")
SAFE_PARTITION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
SCHEDULABLE_STATES = {
    "allocated",
    "completing",
    "idle",
    "mixed",
}


@dataclass(frozen=True)
class PartitionStatus:
    name: str
    available: bool
    time_limit: str
    total_nodes: int
    schedulable_nodes: int
    idle_nodes: int
    pending_jobs: int
    minimum_cpus: int
    minimum_memory_mb: int
    features: tuple[str, ...]
    candidate_order: int

    def score(self, required_nodes: int) -> tuple[int, int, int, int, int, int]:
        return (
            int(self.idle_nodes >= required_nodes),
            self.idle_nodes,
            -self.pending_jobs,
            self.schedulable_nodes,
            self.minimum_memory_mb,
            -self.candidate_order,
        )

    def as_dict(self, required_nodes: int) -> dict[str, object]:
        return {
            "partition": self.name,
            "available": self.available,
            "time_limit": self.time_limit,
            "total_nodes": self.total_nodes,
            "schedulable_nodes": self.schedulable_nodes,
            "idle_nodes": self.idle_nodes,
            "pending_jobs": self.pending_jobs,
            "minimum_cpus": self.minimum_cpus,
            "minimum_memory_mb": self.minimum_memory_mb,
            "features": list(self.features),
            "has_required_idle_nodes": self.idle_nodes >= required_nodes,
        }


def parse_candidate_list(value: str) -> tuple[str, ...]:
    candidates: list[str] = []
    for raw in value.split(","):
        candidate = raw.strip()
        if not candidate:
            continue
        if not SAFE_PARTITION.fullmatch(candidate):
            raise ValueError(f"invalid Slurm partition name: {candidate!r}")
        if candidate not in candidates:
            candidates.append(candidate)
    if not candidates:
        raise ValueError("at least one Slurm partition candidate is required")
    return tuple(candidates)


def parse_sinfo(
    text: str,
    *,
    candidates: tuple[str, ...],
    pending_jobs: dict[str, int],
    minimum_cpus: int,
    minimum_memory_mb: int,
    required_features: frozenset[str],
) -> list[PartitionStatus]:
    rows: dict[str, list[dict[str, object]]] = {name: [] for name in candidates}
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split("|", 7)
        if len(fields) != 8:
            raise ValueError(f"unexpected sinfo row: {line!r}")
        raw_name, availability, time_limit, node_count, state, cpus, memory, features = fields
        name = raw_name.rstrip("*")
        if name not in rows:
            continue
        feature_set = frozenset(item for item in features.split(",") if item)
        row_cpus = _leading_integer(cpus)
        row_memory = _leading_integer(memory)
        normalized_state = _normalize_state(state)
        eligible_hardware = (
            availability == "up"
            and row_cpus >= minimum_cpus
            and row_memory >= minimum_memory_mb
            and required_features.issubset(feature_set)
        )
        rows[name].append(
            {
                "available": availability == "up",
                "time_limit": time_limit,
                "nodes": _leading_integer(node_count),
                "state": normalized_state,
                "cpus": row_cpus,
                "memory": row_memory,
                "features": feature_set,
                "eligible_hardware": eligible_hardware,
            }
        )

    statuses: list[PartitionStatus] = []
    for order, name in enumerate(candidates):
        candidate_rows = rows[name]
        hardware_rows = [row for row in candidate_rows if row["eligible_hardware"]]
        schedulable_rows = [
            row for row in hardware_rows if row["state"] in SCHEDULABLE_STATES
        ]
        statuses.append(
            PartitionStatus(
                name=name,
                available=bool(hardware_rows),
                time_limit=str(hardware_rows[0]["time_limit"]) if hardware_rows else "",
                total_nodes=sum(int(row["nodes"]) for row in hardware_rows),
                schedulable_nodes=sum(int(row["nodes"]) for row in schedulable_rows),
                idle_nodes=sum(
                    int(row["nodes"])
                    for row in hardware_rows
                    if row["state"] == "idle"
                ),
                pending_jobs=pending_jobs.get(name, 0),
                minimum_cpus=min(
                    (int(row["cpus"]) for row in hardware_rows), default=0
                ),
                minimum_memory_mb=min(
                    (int(row["memory"]) for row in hardware_rows), default=0
                ),
                features=tuple(
                    sorted(
                        set.intersection(
                            *(set(row["features"]) for row in hardware_rows)
                        )
                    )
                )
                if hardware_rows
                else (),
                candidate_order=order,
            )
        )
    return statuses


def select_partition(
    statuses: Iterable[PartitionStatus], required_nodes: int
) -> PartitionStatus:
    usable = [
        status
        for status in statuses
        if status.available and status.schedulable_nodes >= required_nodes
    ]
    if not usable:
        raise ValueError(
            "no candidate partition has enough matching schedulable nodes"
        )
    return max(usable, key=lambda status: status.score(required_nodes))


def _query_sinfo(candidates: tuple[str, ...]) -> str:
    return _run(
        [
            "sinfo",
            "--noheader",
            f"--partition={','.join(candidates)}",
            "--format=%P|%a|%l|%D|%T|%c|%m|%f",
        ]
    )


def _query_pending_jobs(candidates: tuple[str, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        output = _run(
            [
                "squeue",
                "--noheader",
                f"--partition={candidate}",
                "--states=PENDING",
                "--format=%i",
            ]
        )
        counts[candidate] = len([line for line in output.splitlines() if line.strip()])
    return counts


def _run(command: list[str]) -> str:
    try:
        completed = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ValueError(f"required Slurm command is unavailable: {command[0]}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise ValueError(f"{' '.join(command)} failed: {detail}")
    return completed.stdout


def _leading_integer(value: str) -> int:
    match = re.match(r"\s*([0-9]+)", value)
    if not match:
        return 0
    return int(match.group(1))


def _normalize_state(value: str) -> str:
    match = re.match(r"([A-Za-z_]+)", value.strip())
    return match.group(1).lower() if match else "unknown"


def _print_table(
    statuses: list[PartitionStatus],
    selected: PartitionStatus,
    required_nodes: int,
) -> None:
    print(
        "PARTITION\tSELECTED\tIDLE\tSCHEDULABLE\tPENDING\t"
        "MIN_CPUS\tMIN_MEMORY_MB\tTIME_LIMIT"
    )
    for status in statuses:
        print(
            f"{status.name}\t{'yes' if status.name == selected.name else 'no'}\t"
            f"{status.idle_nodes}\t{status.schedulable_nodes}\t"
            f"{status.pending_jobs}\t{status.minimum_cpus}\t"
            f"{status.minimum_memory_mb}\t{status.time_limit}"
        )
    print(
        f"Selected {selected.name} for {required_nodes} exclusive nodes.",
        file=sys.stderr,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidates",
        default=os.environ.get(
            "BENCHMARK_PARTITION_CANDIDATES", ",".join(DEFAULT_CANDIDATES)
        ),
        help="comma-separated non-shared Slurm partitions",
    )
    parser.add_argument("--required-nodes", type=int, default=4)
    parser.add_argument("--minimum-cpus", type=int, default=192)
    parser.add_argument("--minimum-memory-mb", type=int, default=240000)
    parser.add_argument(
        "--required-feature",
        action="append",
        default=[],
        help="feature required on every selected partition row; repeatable",
    )
    parser.add_argument(
        "--format", choices=("value", "table", "json"), default="value"
    )
    args = parser.parse_args(argv)
    if args.required_nodes <= 0 or args.minimum_cpus <= 0 or args.minimum_memory_mb < 0:
        parser.error("node, CPU, and memory requirements must be positive")
    try:
        args.candidates = parse_candidate_list(args.candidates)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        pending = _query_pending_jobs(args.candidates)
        statuses = parse_sinfo(
            _query_sinfo(args.candidates),
            candidates=args.candidates,
            pending_jobs=pending,
            minimum_cpus=args.minimum_cpus,
            minimum_memory_mb=args.minimum_memory_mb,
            required_features=frozenset(args.required_feature),
        )
        selected = select_partition(statuses, args.required_nodes)
    except ValueError as exc:
        print(f"partition selection failed: {exc}", file=sys.stderr)
        return 2

    if args.format == "value":
        print(selected.name)
        print(
            f"[partition-selector] selected {selected.name}: "
            f"idle={selected.idle_nodes}, pending={selected.pending_jobs}, "
            f"schedulable={selected.schedulable_nodes}",
            file=sys.stderr,
        )
    elif args.format == "table":
        _print_table(statuses, selected, args.required_nodes)
    else:
        print(
            json.dumps(
                {
                    "selected_partition": selected.name,
                    "required_nodes": args.required_nodes,
                    "partitions": [
                        status.as_dict(args.required_nodes) for status in statuses
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
