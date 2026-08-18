from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

from src.benchmark.workflow.base import WorkflowError


@dataclass(frozen=True, slots=True)
class RecoveryManifestPlan:
    manifest_paths: tuple[Path, ...]
    source_case_count: int
    completed_case_count: int
    retry_case_ids: tuple[str, ...]


def write_selected_case_manifests(
    *,
    source_manifests: Iterable[Path],
    selected_case_ids: Iterable[str],
    output_dir: Path,
) -> RecoveryManifestPlan:
    """Write immutable repair manifests containing exactly the selected cases."""
    sources = tuple(Path(path).resolve() for path in source_manifests)
    requested = tuple(dict.fromkeys(str(item).strip() for item in selected_case_ids))
    if not sources:
        raise WorkflowError("invalid-case recovery has no source manifests")
    if not requested or any(not item for item in requested):
        raise WorkflowError("invalid-case recovery has no valid case IDs")
    output_dir.mkdir(parents=True, exist_ok=False)
    requested_set = set(requested)
    found: set[str] = set()
    retry_paths: list[Path] = []
    source_count = 0
    for source in sources:
        if not source.is_file():
            raise WorkflowError(f"invalid-case source manifest is missing: {source}")
        with source.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = tuple(reader.fieldnames or ())
            rows = list(reader)
        if not fieldnames or not rows:
            raise WorkflowError(f"invalid-case source manifest is empty: {source}")
        id_field = "case_id" if "case_id" in fieldnames else "config_id"
        if id_field not in fieldnames:
            raise WorkflowError(f"manifest has no case identity column: {source}")
        selected: list[dict[str, str]] = []
        for row in rows:
            case_id = str(row.get(id_field, "")).strip()
            if not case_id:
                raise WorkflowError(f"manifest contains an empty case ID: {source}")
            source_count += 1
            if case_id not in requested_set:
                continue
            if case_id in found:
                raise WorkflowError(
                    f"invalid-case source manifests duplicate case ID: {case_id}"
                )
            found.add(case_id)
            selected.append(row)
        if not selected:
            continue
        target = output_dir / f"batch_{len(retry_paths) + 1:03d}.csv"
        with target.open("x", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(selected)
        retry_paths.append(target)
    missing = sorted(requested_set - found)
    if missing:
        raise WorkflowError(
            "invalid-case IDs are absent from source manifests: "
            + ", ".join(missing)
        )
    return RecoveryManifestPlan(
        manifest_paths=tuple(retry_paths),
        source_case_count=source_count,
        completed_case_count=0,
        retry_case_ids=requested,
    )


def write_incomplete_case_manifests(
    *,
    source_manifests: Iterable[Path],
    results_roots: Iterable[Path],
    output_dir: Path,
    backend_id: str,
) -> RecoveryManifestPlan:
    """Write one repair manifest per failed batch without copying completed cases."""
    sources = tuple(Path(path).resolve() for path in source_manifests)
    if not sources:
        raise WorkflowError("failed-batch recovery has no source manifests")
    completed_ids = _completed_case_ids(results_roots, backend_id)
    output_dir.mkdir(parents=True, exist_ok=False)
    retry_paths: list[Path] = []
    retry_ids: list[str] = []
    source_count = 0
    completed_count = 0
    for source in sources:
        if not source.is_file():
            raise WorkflowError(f"failed-batch source manifest is missing: {source}")
        with source.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = tuple(reader.fieldnames or ())
            rows = list(reader)
        if not fieldnames or not rows:
            raise WorkflowError(f"failed-batch source manifest is empty: {source}")
        id_field = "case_id" if "case_id" in fieldnames else "config_id"
        if id_field not in fieldnames:
            raise WorkflowError(f"manifest has no case identity column: {source}")
        pending: list[dict[str, str]] = []
        for row in rows:
            case_id = str(row.get(id_field, "")).strip()
            if not case_id:
                raise WorkflowError(f"manifest contains an empty case ID: {source}")
            source_count += 1
            if case_id in completed_ids:
                completed_count += 1
            else:
                pending.append(row)
                retry_ids.append(case_id)
        if not pending:
            continue
        target = output_dir / f"batch_{len(retry_paths) + 1:03d}.csv"
        with target.open("x", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(pending)
        retry_paths.append(target)
    if len(retry_ids) != len(set(retry_ids)):
        raise WorkflowError("failed-batch repair manifests contain duplicate case IDs")
    return RecoveryManifestPlan(
        manifest_paths=tuple(retry_paths),
        source_case_count=source_count,
        completed_case_count=completed_count,
        retry_case_ids=tuple(retry_ids),
    )


def _completed_case_ids(
    roots: Iterable[Path], backend_id: str
) -> set[str]:
    completed: set[str] = set()
    for raw_root in roots:
        root = Path(raw_root)
        if not root.exists():
            continue
        paths = (root,) if root.is_file() else root.rglob("final_report.json")
        for path in paths:
            try:
                report = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(report, dict):
                continue
            case = _dict(report.get("case"))
            config = _dict(report.get("config"))
            system = _dict(report.get("system_under_test"))
            report_backend = str(
                system.get("backend_id") or config.get("backend_id") or ""
            )
            if report_backend != backend_id or case.get("status") != "completed":
                continue
            case_id = str(case.get("case_id") or config.get("case_id") or "").strip()
            if case_id:
                completed.add(case_id)
    return completed


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
