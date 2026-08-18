#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write a structured post-workload backend health artifact"
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--backend-id", required=True)
    parser.add_argument("--status", choices=("healthy", "failed"), required=True)
    parser.add_argument("--checkpoint", default="post-workload")
    parser.add_argument(
        "--check",
        action="append",
        nargs=3,
        metavar=("NAME", "STATUS", "DETAIL"),
        default=[],
    )
    parser.add_argument("--failure", action="append", default=[])
    parser.add_argument("--signature", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    checks = [
        {"name": name, "status": status, "detail": detail}
        for name, status, detail in args.check
    ]
    if any(check["status"] not in {"pass", "fail"} for check in checks):
        raise SystemExit("Health check status must be 'pass' or 'fail'")
    payload = {
        "format": "messaging-benchmark.backend-health.v1",
        "backend_id": args.backend_id,
        "checkpoint": args.checkpoint,
        "checked_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": args.status,
        "checks": checks,
        "failure_reasons": list(dict.fromkeys(args.failure)),
        "failure_signatures": list(dict.fromkeys(args.signature)),
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
