#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import re
import shlex
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config


SAFE_TOKEN = re.compile(r"^[A-Za-z0-9_.-]+$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate and normalize one multi-case backend batch."
    )
    parser.add_argument("backend_id")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--format", choices=("json", "shell", "tsv"), default="json")
    parser.add_argument("--allow-profile-changes", action="store_true")
    return parser.parse_args()


def build_batch_plan(
    backend_id: str,
    manifest_path: Path,
    project_root: Path,
    *,
    require_one_profile: bool = True,
) -> dict[str, Any]:
    project_root = project_root.resolve()
    manifest_path = manifest_path.resolve()
    adapter = get_backend(backend_id)
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"case_id", "config_path"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(
                "Batch manifest missing column(s): " + ", ".join(sorted(missing))
            )
        source_rows = list(reader)
    if not source_rows:
        raise ValueError(f"Batch manifest is empty: {manifest_path}")

    rows: list[dict[str, Any]] = []
    seen_case_ids: set[str] = set()
    seen_topics: set[str] = set()
    for file_order, source in enumerate(source_rows, start=1):
        case_id = str(source.get("case_id", "")).strip()
        raw_config_path = str(source.get("config_path", "")).strip()
        if not case_id or not raw_config_path:
            raise ValueError(
                f"Manifest row {file_order} has an empty case_id or config_path"
            )
        _require_safe_token(case_id, "case_id", file_order)
        if case_id in seen_case_ids:
            raise ValueError(f"Duplicate batch case_id: {case_id}")
        seen_case_ids.add(case_id)

        config_path = Path(raw_config_path)
        if not config_path.is_absolute():
            config_path = project_root / config_path
        config_path = config_path.resolve()
        if not config_path.is_file():
            raise ValueError(f"Batch config does not exist: {config_path}")
        config = load_benchmark_config(config_path)
        if config.backend_id != adapter.backend_id:
            raise ValueError(
                f"{case_id}: config backend {config.backend_id!r} does not match "
                f"selected backend {adapter.backend_id!r}"
            )
        if config.mode != "single" or config.scenario != "simultaneous":
            raise ValueError(
                f"{case_id}: batch runner requires mode=single and "
                "scenario=simultaneous"
            )

        plan = adapter.resource_plan(config)
        profile_id, profile_sha256 = adapter.profile_identity(config)
        environment = adapter.runtime_environment(config)
        topic_name = str(environment.get("TOPIC_NAME", "")).strip()
        if not topic_name:
            raise ValueError(f"{case_id}: backend runtime has no TOPIC_NAME")
        if topic_name in seen_topics:
            raise ValueError(f"Duplicate batch topic name: {topic_name}")
        seen_topics.add(topic_name)

        try:
            relative_config = config_path.relative_to(project_root)
        except ValueError:
            relative_config = config_path
        block = _positive_integer(source.get("block", "1"), "block", file_order)
        order = _positive_integer(
            source.get("order", file_order), "order", file_order
        )
        stage = str(source.get("stage", "cases") or "cases").strip()
        config_id = str(source.get("config_id", case_id) or case_id).strip()
        _require_safe_token(stage, "stage", file_order)
        _require_safe_token(config_id, "config_id", file_order)
        rows.append(
            {
                "file_order": file_order,
                "block": block,
                "order": order,
                "stage": stage,
                "case_id": case_id,
                "config_id": config_id,
                "config_path": str(relative_config),
                "backend_id": config.backend_id,
                "profile_id": str(profile_id or ""),
                "profile_sha256": str(profile_sha256 or ""),
                "service_nodes": plan.service_nodes,
                "monitoring_nodes": plan.monitoring_nodes,
                "producer_ranks": config.producer_ranks,
                "consumer_ranks": config.consumer_ranks,
                "total_mpi_ranks": plan.total_mpi_ranks,
                "warmup_sec": config.warmup_sec,
                "duration_sec": config.duration_sec,
                "drain_timeout_sec": config.drain_timeout_sec,
                "case_budget_sec": (
                    config.warmup_sec
                    + config.duration_sec
                    + config.drain_timeout_sec
                ),
                "topic_name": topic_name,
            }
        )

    rows.sort(key=lambda row: (row["block"], row["order"], row["file_order"]))
    service_counts = {row["service_nodes"] for row in rows}
    monitoring_counts = {row["monitoring_nodes"] for row in rows}
    profile_pairs = {
        (row["profile_id"], row["profile_sha256"])
        for row in rows
    }
    if len(service_counts) != 1 or len(monitoring_counts) != 1:
        raise ValueError("All batch cases must use the same node topology")
    if require_one_profile and len(profile_pairs) != 1:
        values = ", ".join(f"{pid}:{sha}" for pid, sha in sorted(profile_pairs))
        raise ValueError(
            "A batch requires one immutable backend profile; found " + values
        )

    sorted_profiles = [
        {"profile_id": profile_id, "profile_sha256": profile_sha256}
        for profile_id, profile_sha256 in sorted(profile_pairs)
    ]
    profile_id, profile_sha256 = next(iter(profile_pairs))
    return {
        "format": "messaging-benchmark.backend-batch-plan.v1",
        "backend_id": adapter.backend_id,
        "manifest": _display_path(manifest_path, project_root),
        "case_count": len(rows),
        "service_nodes": next(iter(service_counts)),
        "monitoring_nodes": next(iter(monitoring_counts)),
        "producer_nodes": 1,
        "consumer_nodes": 1,
        "node_count": (
            next(iter(service_counts))
            + next(iter(monitoring_counts))
            + 2
        ),
        "max_producer_ranks": max(row["producer_ranks"] for row in rows),
        "max_consumer_ranks": max(row["consumer_ranks"] for row in rows),
        "max_total_mpi_ranks": max(row["total_mpi_ranks"] for row in rows),
        "max_case_budget_sec": max(row["case_budget_sec"] for row in rows),
        "total_case_budget_sec": sum(row["case_budget_sec"] for row in rows),
        "profile_id": profile_id if len(profile_pairs) == 1 else "multiple",
        "profile_sha256": profile_sha256 if len(profile_pairs) == 1 else "multiple",
        "profile_count": len(profile_pairs),
        "profiles": sorted_profiles,
        "rows": rows,
    }


def render_shell(plan: dict[str, Any]) -> str:
    values = {
        "BACKEND_ID": plan["backend_id"],
        "ESTIMATED_CASES": plan["case_count"],
        "SERVICE_NODE_COUNT": plan["service_nodes"],
        "MONITORING_NODE_COUNT": plan["monitoring_nodes"],
        "NODE_COUNT": plan["node_count"],
        "MAX_PRODUCER_RANKS": plan["max_producer_ranks"],
        "MAX_CONSUMER_RANKS": plan["max_consumer_ranks"],
        "MAX_MPI_RANKS": plan["max_total_mpi_ranks"],
        "MAX_CASE_BUDGET_SEC": plan["max_case_budget_sec"],
        "TOTAL_CASE_BUDGET_SEC": plan["total_case_budget_sec"],
        "BATCH_PROFILE_ID": plan["profile_id"],
        "BATCH_PROFILE_SHA256": plan["profile_sha256"],
        "BATCH_PROFILE_COUNT": plan["profile_count"],
    }
    return "\n".join(
        f"{name}={shlex.quote(str(value))}" for name, value in values.items()
    ) + "\n"


def render_tsv(plan: dict[str, Any]) -> str:
    fields = (
        "sequence",
        "block",
        "order",
        "stage",
        "case_id",
        "config_id",
        "config_path",
        "profile_id",
        "profile_sha256",
        "producer_ranks",
        "consumer_ranks",
        "warmup_sec",
        "duration_sec",
        "drain_timeout_sec",
        "case_budget_sec",
        "topic_name",
    )
    output = ["\t".join(fields)]
    for sequence, row in enumerate(plan["rows"], start=1):
        values = {**row, "sequence": sequence}
        output.append("\t".join(str(values[field]) for field in fields))
    return "\n".join(output) + "\n"


def _display_path(path: Path, project_root: Path) -> str:
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return path.name


def _require_safe_token(value: str, field: str, row: int) -> None:
    if not SAFE_TOKEN.fullmatch(value):
        raise ValueError(
            f"Manifest row {row} has unsafe {field}={value!r}; "
            "use letters, digits, '.', '_', or '-'"
        )


def _positive_integer(value: Any, field: str, row: int) -> int:
    try:
        parsed = int(str(value or "").strip())
    except ValueError as exc:
        raise ValueError(
            f"Manifest row {row} has non-integer {field}={value!r}"
        ) from exc
    if parsed <= 0:
        raise ValueError(
            f"Manifest row {row} requires positive {field}, got {parsed}"
        )
    return parsed


def main() -> int:
    args = parse_args()
    plan = build_batch_plan(
        args.backend_id,
        args.manifest,
        args.project_root,
        require_one_profile=not args.allow_profile_changes,
    )
    if args.format == "shell":
        print(render_shell(plan), end="")
    elif args.format == "tsv":
        print(render_tsv(plan), end="")
    else:
        print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
