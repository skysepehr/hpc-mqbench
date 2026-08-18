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
DEFAULT_ROOT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "final_validation"
DEFAULT_PLAN = DEFAULT_ROOT / "final_validation_plan.json"
DEFAULT_MANIFEST = DEFAULT_ROOT / "final_validation_manifest.csv"
DEFAULT_SHORTLIST = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1"
    / "pulsar_phase1_shortlist.json"
)
DEFAULT_FINAL_SELECTION = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "complete"
    / "data"
    / "phase2-confirmation"
    / "pulsar_phase2_final_selection.json"
)
DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1" / "generated_configs"
)
DEFAULT_PROFILE = (
    PROJECT_ROOT
    / "configs"
    / "backends"
    / "pulsar"
    / "profiles"
    / "BASELINE_H16_D32.json"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authorize the two validated Pulsar final-validation batch jobs."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--final-selection", type=Path, default=DEFAULT_FINAL_SELECTION)
    parser.add_argument(
        "--phase1-config-root",
        type=Path,
        default=DEFAULT_PHASE1_CONFIG_ROOT,
    )
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    validator = PROJECT_ROOT / "scripts" / "validate_pulsar_final_validation_inputs.py"
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(validator),
            "--plan",
            str(args.plan),
            "--manifest",
            str(args.manifest),
            "--shortlist",
            str(args.shortlist),
            "--final-selection",
            str(args.final_selection),
            "--phase1-config-root",
            str(args.phase1_config_root),
            "--profile",
            str(args.profile),
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
    rows = _read_csv(args.manifest)
    project_root = args.project_root.resolve()
    config_paths = []
    for row in rows:
        path = Path(str(row["config_path"]))
        config_paths.append(path if path.is_absolute() else project_root / path)
    batch_paths = sorted(args.plan.parent.glob("batches/batch_*.csv"))

    plan["status"] = "authorized_for_submission"
    plan["submission_authorized"] = True
    plan["authorization"] = {
        "authorized_on": date.today().isoformat(),
        "scope": "two sequential Pulsar final-validation Slurm jobs containing 30 and 20 cases",
        "canonical_manifest_sha256": _sha256(args.manifest),
        "batch_manifest_sha256": {
            _display(path, project_root): _sha256(path) for path in batch_paths
        },
        "generated_config_sha256": {
            _display(path, project_root): _sha256(path) for path in sorted(config_paths)
        },
        "validation": validation,
    }
    plan["note"] = (
        "The deterministic five-block inputs passed validation. Authorization "
        "is limited to the listed 30-case and 20-case batch manifests."
    )
    args.plan.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("[pulsar-final-validation-authorize] submission_authorized=true")
    print(f"[pulsar-final-validation-authorize] plan: {args.plan}")
    return 0


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


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
