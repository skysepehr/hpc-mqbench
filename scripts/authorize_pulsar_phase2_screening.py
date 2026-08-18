#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLAN = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "phase2"
    / "phase2_campaign_plan.json"
)
DEFAULT_MANIFEST = DEFAULT_PLAN.parent / "memory_screening_manifest.csv"
DEFAULT_PROFILE_ROOT = (
    PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Record review authorization and immutable input checksums for the "
            "single Pulsar Phase 2 memory-screening job."
        )
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    validator = PROJECT_ROOT / "scripts" / "validate_pulsar_phase2_inputs.py"
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(validator),
            "--plan",
            str(args.plan),
            "--manifest",
            str(args.manifest),
            "--profile-root",
            str(args.profile_root),
            "--project-root",
            str(project_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        return completed.returncode
    validation = json.loads(completed.stdout)
    if validation.get("valid") is not True:
        raise ValueError("Phase 2 input validation did not pass")

    plan = _read_json(args.plan)
    with args.manifest.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    profile_paths = sorted(
        args.profile_root / f"{profile_id}.json"
        for profile_id in {str(row["profile_id"]) for row in rows}
    )
    config_paths = []
    for row in rows:
        path = Path(str(row["config_path"]))
        config_paths.append(path if path.is_absolute() else project_root / path)

    plan["status"] = "authorized_for_submission"
    plan["submission_authorized"] = True
    plan["authorization"] = {
        "authorized_on": date.today().isoformat(),
        "scope": "one 30-case Phase 2 memory-screening Slurm job",
        "manifest_sha256": _sha256(args.manifest),
        "profile_sha256": {
            _display_path(path, project_root): _sha256(path)
            for path in profile_paths
        },
        "generated_config_sha256": {
            _display_path(path, project_root): _sha256(path)
            for path in sorted(config_paths)
        },
        "validation": validation,
    }
    plan["note"] = (
        "The deterministic inputs passed local and GWDG validation and the "
        "user authorized exactly one screening job. This authorization does "
        "not extend to confirmation or later tuning jobs."
    )
    args.plan.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print("[pulsar-phase2-authorize] submission_authorized=true")
    print(f"[pulsar-phase2-authorize] plan: {args.plan}")
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root).as_posix()
    except ValueError:
        return str(path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
