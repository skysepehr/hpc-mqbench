#!/usr/bin/env python3
"""Check the companion artifact without modifying it; create hashes only on request.

Run ``python3 scripts/verify_artifact.py`` from any directory. Maintainers may
rebuild MANIFEST.json and SHA256SUMS with ``--write-manifest`` after an intentional
change. This checks inventory, integrity, and observation counts; run
``scripts/verify_paper.py`` separately for the manuscript's numerical checks.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
ROOT_METADATA = {"MANIFEST.json", "SHA256SUMS"}


class VerificationError(Exception):
    """An invalid or incomplete artifact."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def safe_path(value: object) -> str:
    require(isinstance(value, str) and bool(value), "Invalid empty/non-string path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and ".." not in path.parts
            and value == path.as_posix()
            and value != "." and "\\" not in value
            and not any(ord(char) < 32 or ord(char) == 127 for char in value),
            f"Unsafe or non-canonical path: {value!r}")
    return value


def payload_files() -> dict[str, Path]:
    """Never follow links; exclude only documented generated/cache locations."""
    result = {}
    for parent, directories, files in os.walk(ROOT, followlinks=False):
        base = Path(parent)
        for name in directories + files:
            path = base / name
            require(not path.is_symlink(), f"Symlinks are forbidden: {path.relative_to(ROOT)}")
        directories[:] = sorted(name for name in directories
                                if name not in {"build", "__pycache__"}
                                and not name.startswith("."))
        for name in sorted(files):
            path = base / name
            relative = safe_path(path.relative_to(ROOT).as_posix())
            require(path.is_file(), f"Not a regular file: {relative}")
            if relative in ROOT_METADATA or path.suffix == ".pyc":
                continue
            result[relative] = path
    return dict(sorted(result.items()))


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def role(path: str) -> str:
    if path.startswith("data/audit/"):
        return "selected_observation_rows"
    if path.startswith("data/kafka/source/"):
        return "retained_campaign_evidence"
    if path.startswith("data/kafka/derived/"):
        return "derived_analysis"
    if path.startswith("data/"):
        return "measurement_contract_or_provenance"
    if path.startswith("supplement/"):
        return "supplementary_analysis_or_configuration"
    if path.startswith("scripts/"):
        return "verification_or_analysis_script"
    if path.startswith("figures/"):
        return "manuscript_figure"
    if Path(path).suffix in {".tex", ".pdf", ".bib"}:
        return "manuscript"
    if path == "PROVENANCE.json":
        return "source_and_evidence_provenance"
    if path.startswith("LICENSE"):
        return "license"
    return "documentation_or_supporting_file"


def rows(relative: str) -> list[dict[str, str]]:
    with (ROOT / relative).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        require(bool(reader.fieldnames)
                and len(reader.fieldnames) == len(set(reader.fieldnames)),
                f"Missing or duplicate CSV header: {relative}")
        result = list(reader)
    require(all(None not in row and None not in row.values() for row in result),
            f"Malformed CSV row: {relative}")
    return result


def verify_observations() -> None:
    screen = rows("data/audit/phase1_cases.csv")
    require(len(screen) == 120 and len({row["config_id"] for row in screen}) == 120,
            "Expected 120 distinct selected screening configurations")
    artifact_base = "data/kafka/source/artifacts"
    for stage in ("v1", "v2"):
        relative = f"data/audit/{stage}_validation_repeats.csv"
        observations = rows(relative)
        require(len(observations) == 50
                and Counter(Counter(row["config_id"] for row in observations).values()) == {5: 10},
                f"Expected 50 {stage} validation observations: 10 configurations, five each")
        for original, copy in [
                ("data/audit/phase1_cases.csv", f"{artifact_base}/{stage}/phase1_cases.csv"),
                (relative, f"{artifact_base}/{stage}/validation_repeats.csv")]:
            require((ROOT / original).read_bytes() == (ROOT / copy).read_bytes(),
                    f"Retained copy disagrees with audit input: {copy}")
    profile = rows("data/kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv")
    counts = Counter(row["stage"] for row in profile)
    require(counts == {"instrumentation_pilot": 6, "rate_calibration": 3,
                       "profile_screening": 30, "profile_confirmation": 30,
                       "final_validation": 50},
            f"Unexpected complete-profile stage counts: {dict(counts)}")
    print("Observation inventory: 120 selected screening rows; V1=50; V2=50.")
    print("Complete-profile evidence: 119 rows = 69 auxiliary + 50 final validation;")
    print("the screening copies and final validation are reused evidence, not extra runs.")


def manifest_for(files: dict[str, Path]) -> dict:
    return {"schema_version": 1, "files": [
        {"path": relative, "bytes": path.stat().st_size,
         "sha256": digest(path), "role": role(relative)}
        for relative, path in files.items()]}


def checksum_text(files: dict[str, Path]) -> str:
    all_files = dict(files, **{"MANIFEST.json": ROOT / "MANIFEST.json"})
    return "".join(f"{digest(path)}  {relative}\n"
                   for relative, path in sorted(all_files.items()))


def verify_manifest(files: dict[str, Path]) -> None:
    document = json.loads((ROOT / "MANIFEST.json").read_text(encoding="utf-8"))
    require(isinstance(document, dict) and document.get("schema_version") == 1,
            "Unsupported manifest schema")
    entries = document.get("files")
    require(isinstance(entries, list), "Manifest files must be a list")
    by_path = {}
    for entry in entries:
        require(isinstance(entry, dict), "Invalid manifest entry")
        relative = safe_path(entry.get("path"))
        require(relative not in by_path, f"Duplicate manifest entry: {relative}")
        require(type(entry.get("bytes")) is int and entry["bytes"] >= 0,
                f"Invalid byte count: {relative}")
        require(isinstance(entry.get("sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is not None,
                f"Invalid SHA-256: {relative}")
        require(entry.get("role") == role(relative), f"Incorrect manifest role: {relative}")
        by_path[relative] = entry
    missing = sorted(set(by_path) - set(files))
    extra = sorted(set(files) - set(by_path))
    require(not missing and not extra, f"Inventory mismatch: missing={missing}; extra={extra}")
    for relative, path in files.items():
        require(path.stat().st_size == by_path[relative]["bytes"], f"Size mismatch: {relative}")
        require(digest(path) == by_path[relative]["sha256"], f"Hash mismatch: {relative}")
    actual_checksums = (ROOT / "SHA256SUMS").read_text(encoding="utf-8")
    require(actual_checksums == checksum_text(files),
            "SHA256SUMS does not exactly match the payload and MANIFEST.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-manifest", action="store_true",
                        help="explicitly rebuild MANIFEST.json and SHA256SUMS")
    options = parser.parse_args()
    try:
        files = payload_files()
        verify_observations()
        if options.write_manifest:
            (ROOT / "MANIFEST.json").write_text(
                json.dumps(manifest_for(files), indent=2, ensure_ascii=True) + "\n",
                encoding="utf-8")
            (ROOT / "SHA256SUMS").write_text(checksum_text(files), encoding="utf-8")
        verify_manifest(files)
    except (VerificationError, OSError, ValueError, KeyError, TypeError) as error:
        print(f"Artifact verification FAILED: {error}", file=sys.stderr)
        return 1
    print(f"Artifact verification passed: {len(files)} payload files plus MANIFEST.json.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
