#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPORT = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1-gate"
    / "acceptance_report.json"
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject Pulsar Phase 1 submission before pilot acceptance."
    )
    parser.add_argument("manifest")
    parser.add_argument("--acceptance-report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    with Path(args.manifest).open("r", encoding="utf-8", newline="") as handle:
        manifest_rows = list(csv.DictReader(handle))
    stages = {
        str(row.get("stage", "")).strip()
        for row in manifest_rows
    }
    if "phase1-screening" not in stages:
        return 0

    report_path = Path(args.acceptance_report)
    if not report_path.is_file():
        raise SystemExit(
            "Pulsar Phase 1 is gated: the published acceptance report is "
            f"missing at {report_path}. Rebuild it with "
            "promote_pulsar_phase1_gate.py before submission."
        )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("format") != "messaging-benchmark.pulsar-phase1-gate.v1":
        raise SystemExit(f"Unexpected Pulsar acceptance report format: {report_path}")
    if report.get("phase1_submission_authorized") is not True:
        reasons = report.get("failure_reasons", [])
        detail = "; ".join(str(reason) for reason in reasons) or "not authorized"
        raise SystemExit(f"Pulsar Phase 1 is gated: {detail}")
    profile_ids = {
        str(row.get("profile_id", "")).strip()
        for row in manifest_rows
    }
    profile = report.get("profile")
    if not isinstance(profile, dict):
        raise SystemExit("Pulsar Phase 1 is gated: accepted profile is missing")
    accepted_profile = str(profile.get("profile_id", ""))
    accepted_checksum = str(profile.get("profile_sha256", ""))
    if not accepted_profile or not accepted_checksum:
        raise SystemExit(
            "Pulsar Phase 1 is gated: accepted profile identity is incomplete"
        )
    if profile_ids != {accepted_profile}:
        raise SystemExit(
            "Pulsar Phase 1 is gated: manifest profile does not match "
            f"accepted profile {accepted_profile!r}"
        )
    config_identities: set[tuple[str, str]] = set()
    for row in manifest_rows:
        config_path = Path(str(row.get("config_path", "")).strip())
        if not config_path.is_absolute():
            config_path = PROJECT_ROOT / config_path
        if not config_path.is_file():
            raise SystemExit(
                f"Pulsar Phase 1 is gated: config does not exist: {config_path}"
            )
        config = json.loads(config_path.read_text(encoding="utf-8"))
        backend = config.get("backend")
        pulsar = backend.get("pulsar") if isinstance(backend, dict) else None
        if not isinstance(pulsar, dict):
            raise SystemExit(
                f"Pulsar Phase 1 is gated: {config_path} has no Pulsar settings"
            )
        config_identities.add(
            (
                str(pulsar.get("profile_id", "")),
                str(pulsar.get("profile_sha256", "")),
            )
        )
    if config_identities != {(accepted_profile, accepted_checksum)}:
        raise SystemExit(
            "Pulsar Phase 1 is gated: case profile ID or checksum differs "
            "from the accepted profile"
        )
    print(f"[pulsar-phase1-gate] accepted: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
