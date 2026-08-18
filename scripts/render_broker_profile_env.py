#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.broker_profile import resolve_profile_for_config
from src.benchmark.config_loader import load_benchmark_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a config's immutable broker profile and print shell exports"
    )
    parser.add_argument("--config", required=True)
    parser.add_argument("--project-root", default=".")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    config_path = Path(args.config).resolve()
    config = load_benchmark_config(config_path)
    profile = resolve_profile_for_config(
        config,
        config_path=config_path,
        project_root=project_root,
    )
    if profile is None:
        return

    values = profile.environment()
    values.update(
        {
            "BROKER_PROFILE_ID": profile.profile_id,
            "BROKER_PROFILE_SHA256": profile.sha256,
            "BROKER_PROFILE_MANIFEST_PATH": str(profile.path),
        }
    )
    for key, value in values.items():
        print(f"{key}={shlex.quote(value)}")


if __name__ == "__main__":
    main()
