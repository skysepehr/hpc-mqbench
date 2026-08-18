#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.config_loader import load_benchmark_config


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify immutable Pulsar profile")
    parser.add_argument("config")
    parser.add_argument("--project-root", default=".")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.project_root).resolve()
    config = load_benchmark_config(args.config)
    if config.backend_id != "pulsar":
        raise SystemExit("Pulsar profile verification requires backend_id=pulsar")
    settings = config.backend_settings
    profile_id = str(settings["profile_id"])
    path = root / "configs" / "backends" / "pulsar" / "profiles" / f"{profile_id}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    profile_settings = payload.get("settings")
    if not isinstance(profile_settings, dict):
        raise SystemExit(f"Invalid Pulsar profile settings: {path}")
    actual = canonical_sha256(profile_settings)
    recorded = str(payload.get("sha256", "")).lower()
    configured = str(settings.get("profile_sha256", "")).lower()
    if actual != recorded or actual != configured:
        raise SystemExit(
            "Pulsar profile checksum mismatch: "
            f"actual={actual} profile={recorded} config={configured}"
        )
    for key in (
        "service_count",
        "service_mode",
        "managed_ledger",
        "standalone_properties",
        "runtime",
    ):
        if settings.get(key) != profile_settings.get(key):
            raise SystemExit(f"Pulsar profile drift in backend.pulsar.{key}")
    print(
        json.dumps(
            {
                "profile_id": profile_id,
                "profile_sha256": actual,
                "profile_path": str(path),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
