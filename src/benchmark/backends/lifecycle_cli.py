from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from src.benchmark.backends import get_backend, list_backends
from src.benchmark.config_loader import load_benchmark_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Messaging backend dispatcher")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list")

    validate = subparsers.add_parser("validate")
    validate.add_argument("backend_id")
    validate.add_argument("config")

    plan = subparsers.add_parser("plan")
    plan.add_argument("backend_id")
    plan.add_argument("config")

    resolve = subparsers.add_parser("resolve")
    resolve.add_argument("backend_id")
    resolve.add_argument("action")
    resolve.add_argument("--project-root", default=".")

    snapshot = subparsers.add_parser("snapshot")
    snapshot.add_argument("backend_id")
    snapshot.add_argument("config")
    snapshot.add_argument("output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "list":
        for backend_id in list_backends():
            print(backend_id)
        return 0

    adapter = get_backend(args.backend_id)
    if args.command == "resolve":
        script = adapter.lifecycle_script(
            args.action,
            Path(args.project_root).resolve(),
        )
        if script is None:
            raise SystemExit(
                f"Backend {adapter.backend_id!r} does not implement "
                f"lifecycle action {args.action!r}"
            )
        if not script.is_file():
            raise SystemExit(f"Lifecycle script not found: {script}")
        print(script)
        return 0

    config = load_benchmark_config(args.config)
    if config.backend_id != adapter.backend_id:
        raise SystemExit(
            f"Config backend_id={config.backend_id!r} does not match "
            f"requested backend {adapter.backend_id!r}"
        )
    adapter.validate_config(config)
    if args.command == "plan":
        plan = adapter.resource_plan(config)
        print(
            json.dumps(
                {
                    "backend_id": adapter.backend_id,
                    "service_nodes": plan.service_nodes,
                    "monitoring_nodes": plan.monitoring_nodes,
                    "producer_ranks": plan.producer_ranks,
                    "consumer_ranks": plan.consumer_ranks,
                    "total_mpi_ranks": plan.total_mpi_ranks,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "validate":
        print(
            json.dumps(
                {
                    "backend_id": adapter.backend_id,
                    "adapter_version": adapter.adapter_version,
                    "schema_version": config.schema_version,
                    "qualification_policy_id": config.qualification_policy_id,
                },
                sort_keys=True,
            )
        )
        return 0

    effective_settings = adapter.effective_settings(config)
    canonical = json.dumps(
        effective_settings,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    payload: dict[str, Any] = {
        "format": "messaging-benchmark.backend-snapshot.v1",
        "identity": adapter.identity(config),
        "effective_settings": effective_settings,
        "effective_settings_sha256": hashlib.sha256(canonical).hexdigest(),
        "monitoring_endpoint_specs": adapter.monitoring_endpoint_specs(config),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
