from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from src.benchmark.backends import get_backend
from src.benchmark.config_loader import load_benchmark_config
from src.benchmark.workflow.base import COMMON_MEASUREMENT_CONTRACT, WorkflowError


def validate_campaign_manifest(
    manifest_path: Path,
    *,
    project_root: Path,
    backend_id: str,
    expected_case_count: int,
    allow_latency_disabled: bool = False,
) -> dict[str, Any]:
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != expected_case_count:
        raise WorkflowError(
            f"{manifest_path}: {len(rows)} cases, expected {expected_case_count}"
        )
    if not rows or "config_path" not in rows[0]:
        raise WorkflowError(f"{manifest_path}: config_path column is missing")
    id_field = "case_id" if "case_id" in rows[0] else "config_id"
    case_ids = [str(row.get(id_field, "")).strip() for row in rows]
    if not all(case_ids) or len(case_ids) != len(set(case_ids)):
        raise WorkflowError(f"{manifest_path}: case IDs are empty or duplicated")

    timing = COMMON_MEASUREMENT_CONTRACT["timing"]
    adapter = get_backend(backend_id)
    config_paths: list[Path] = []
    profile_pairs: set[tuple[str, str]] = set()
    failures: list[str] = []
    for row, case_id in zip(rows, case_ids):
        config_path = Path(str(row["config_path"]))
        if not config_path.is_absolute():
            config_path = project_root / config_path
        config_path = config_path.resolve()
        if not config_path.is_file():
            failures.append(f"{case_id}: config is missing: {config_path}")
            continue
        config_paths.append(config_path)
        try:
            config = load_benchmark_config(config_path)
        except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
            failures.append(f"{case_id}: invalid config: {exc}")
            continue
        if config.backend_id != backend_id:
            failures.append(
                f"{case_id}: backend is {config.backend_id}, expected {backend_id}"
            )
        for field, actual, expected in (
            ("warmup_sec", config.warmup_sec, timing["warmup_sec"]),
            ("duration_sec", config.duration_sec, timing["measurement_sec"]),
            ("drain_timeout_sec", config.drain_timeout_sec, timing["drain_timeout_sec"]),
        ):
            if actual != expected:
                failures.append(f"{case_id}: {field}={actual}, expected {expected}")
        if config.latency_enabled is not True and not allow_latency_disabled:
            failures.append(f"{case_id}: latency is disabled")
        if config.latency_sample_every != 10:
            failures.append(
                f"{case_id}: latency_sample_every={config.latency_sample_every}, expected 10"
            )
        if config.qualification_policy_id != "qualification.application.v1":
            failures.append(f"{case_id}: shared qualification policy is missing")
        metadata = config.extra.get("campaign_metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        if metadata.get("measurement_contract_id") != "measurement.messaging.reproducible.v1":
            failures.append(f"{case_id}: reproducible measurement contract is missing")
        if metadata.get("backlog_denominator") != "measurement-period send attempts":
            failures.append(f"{case_id}: attempted-send backlog denominator is missing")
        if (
            config.scenario == "simultaneous"
            and not adapter.uses_coordinated_consumer_drain(config)
        ):
            failures.append(
                f"{case_id}: backend adapter does not enable coordinated "
                "producer-flush/consumer-drain execution"
            )
        profile_id, profile_sha256 = adapter.profile_identity(config)
        profile_pairs.add((str(profile_id or ""), str(profile_sha256 or "")))
        if not profile_id or not profile_sha256:
            failures.append(f"{case_id}: immutable backend profile identity is incomplete")
    if failures:
        raise WorkflowError("; ".join(failures[:30]))
    return {
        "format": "messaging-benchmark.workflow-input-validation.v1",
        "backend_id": backend_id,
        "manifest": _display(manifest_path, project_root),
        "manifest_sha256": _sha256(manifest_path),
        "case_count": len(rows),
        "unique_case_count": len(set(case_ids)),
        "profile_count": len(profile_pairs),
        "profile_identities": [
            {"profile_id": profile_id, "profile_sha256": profile_sha256}
            for profile_id, profile_sha256 in sorted(profile_pairs)
        ],
        "config_sha256": {
            _display(path, project_root): _sha256(path) for path in config_paths
        },
        "common_contract": COMMON_MEASUREMENT_CONTRACT,
        "valid": True,
    }


def manifest_config_paths(manifest_path: Path, project_root: Path) -> list[Path]:
    """Resolve the immutable case inputs referenced by one campaign manifest."""
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    paths: list[Path] = []
    for row in rows:
        raw = str(row.get("config_path", "")).strip()
        if not raw:
            raise WorkflowError(f"{manifest_path}: a config_path value is empty")
        path = Path(raw)
        if not path.is_absolute():
            path = project_root / path
        path = path.resolve()
        if not path.is_file():
            raise WorkflowError(f"{manifest_path}: config is missing: {path}")
        paths.append(path)
    return list(dict.fromkeys(paths))


def verify_sha256_file(checksum_path: Path) -> dict[str, str]:
    root = checksum_path.parent
    verified: dict[str, str] = {}
    failures: list[str] = []
    for line_number, line in enumerate(
        checksum_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            expected, relative = stripped.split(None, 1)
        except ValueError:
            failures.append(f"line {line_number} is malformed")
            continue
        relative = relative.lstrip("*").strip()
        path = (root / relative).resolve()
        try:
            path.relative_to(root.resolve())
        except ValueError:
            failures.append(f"line {line_number} escapes checksum root")
            continue
        if not path.is_file():
            failures.append(f"missing {relative}")
            continue
        actual = _sha256(path)
        if actual != expected:
            failures.append(f"checksum mismatch {relative}")
        else:
            verified[relative] = actual
    if failures:
        raise WorkflowError(
            f"{checksum_path}: " + "; ".join(failures[:20])
        )
    if not verified:
        raise WorkflowError(f"{checksum_path}: no checksum entries")
    return verified


def write_validation(path: Path, validation: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())
