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
DEFAULT_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase2" / "confirmation"
)
DEFAULT_PLAN = DEFAULT_ROOT / "confirmation_plan.json"
DEFAULT_MANIFEST = DEFAULT_ROOT / "memory_confirmation_manifest.csv"
DEFAULT_SELECTION = (
    PROJECT_ROOT
    / "results"
    / "rebuilt"
    / "pulsar"
    / "phase2-screening"
    / "pulsar_phase2_candidate_selection.json"
)
DEFAULT_PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authorize exactly one validated Pulsar Phase 2 confirmation job."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validator = PROJECT_ROOT / "scripts" / "validate_pulsar_phase2_confirmation_inputs.py"
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(validator),
            "--plan",
            str(args.plan),
            "--manifest",
            str(args.manifest),
            "--selection",
            str(args.selection),
            "--profile-root",
            str(args.profile_root),
            "--project-root",
            str(args.project_root),
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
    plan = _read_json(args.plan)
    with args.manifest.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    project_root = args.project_root.resolve()
    config_paths = []
    for row in rows:
        path = Path(str(row["config_path"]))
        config_paths.append(path if path.is_absolute() else project_root / path)
    profile_paths = sorted(
        args.profile_root / f"{profile_id}.json"
        for profile_id in {str(row["profile_id"]) for row in rows}
    )
    plan["status"] = "authorized_for_submission"
    plan["submission_authorized"] = True
    plan["authorization"] = {
        "authorized_on": date.today().isoformat(),
        "scope": "one 30-case Phase 2 memory-confirmation Slurm job",
        "screening_selection_sha256": _sha256(args.selection),
        "manifest_sha256": _sha256(args.manifest),
        "profile_sha256": {
            _display(path, project_root): _sha256(path) for path in profile_paths
        },
        "generated_config_sha256": {
            _display(path, project_root): _sha256(path) for path in sorted(config_paths)
        },
        "validation": validation,
    }
    plan["note"] = (
        "The complete screening evidence selected these candidates and the "
        "deterministic inputs passed validation. Authorization is limited to "
        "one confirmation job."
    )
    args.plan.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("[pulsar-phase2-confirmation-authorize] submission_authorized=true")
    print(f"[pulsar-phase2-confirmation-authorize] plan: {args.plan}")
    return 0


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root).as_posix()
    except ValueError:
        return str(path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
