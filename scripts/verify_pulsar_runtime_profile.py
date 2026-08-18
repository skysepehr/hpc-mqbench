#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any


LEDGER_PROPERTY_MAP = {
    "ensemble_size": "managedLedgerDefaultEnsembleSize",
    "write_quorum": "managedLedgerDefaultWriteQuorum",
    "ack_quorum": "managedLedgerDefaultAckQuorum",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reject Pulsar runtime settings that drift from a profile"
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--standalone-conf", required=True)
    parser.add_argument("--jvm-memory", required=True)
    parser.add_argument("--java-version", required=True)
    parser.add_argument("--pulsar-version", required=True)
    parser.add_argument("--service-node", required=True)
    parser.add_argument("--service-address", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()
    return properties


def parse_java_major(version_text: str) -> int:
    match = re.search(
        r'(?:java|openjdk) version "(?P<major>[0-9]+)(?:[.\-"][^"]*)?"',
        version_text,
        flags=re.IGNORECASE,
    )
    if match is None:
        raise ValueError(f"could not parse Java version: {version_text!r}")
    return int(match.group("major"))


def property_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def main() -> None:
    args = parse_args()
    profile_path = Path(args.profile).resolve()
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    if profile.get("backend_id") != "pulsar":
        raise SystemExit("runtime profile must have backend_id=pulsar")
    settings = profile.get("settings")
    if not isinstance(settings, dict):
        raise SystemExit("Pulsar profile settings must be an object")
    profile_sha256 = canonical_sha256(settings)
    if profile_sha256 != str(profile.get("sha256", "")).lower():
        raise SystemExit("Pulsar profile checksum mismatch")

    runtime = settings.get("runtime")
    ledger = settings.get("managed_ledger")
    standalone = settings.get("standalone_properties")
    if not all(isinstance(value, dict) for value in (runtime, ledger, standalone)):
        raise SystemExit(
            "Pulsar profile requires runtime, managed_ledger, and "
            "standalone_properties objects"
        )

    expected_java_major = int(runtime["java_major"])
    try:
        actual_java_major = parse_java_major(args.java_version)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if actual_java_major != expected_java_major:
        raise SystemExit(
            "Java major version drift: "
            f"runtime={actual_java_major} profile={expected_java_major}"
        )
    if args.jvm_memory != str(runtime["jvm_memory"]):
        raise SystemExit(
            "PULSAR_MEM drift: "
            f"runtime={args.jvm_memory!r} profile={runtime['jvm_memory']!r}"
        )
    expected_product_version = str(runtime["product_version"])
    if expected_product_version not in args.pulsar_version:
        raise SystemExit(
            "Pulsar version drift: "
            f"runtime={args.pulsar_version!r} profile={expected_product_version!r}"
        )

    config_path = Path(args.standalone_conf).resolve()
    properties = parse_properties(config_path)
    expected_properties = {
        property_name: property_value(ledger[setting_name])
        for setting_name, property_name in LEDGER_PROPERTY_MAP.items()
    }
    expected_properties.update(
        {
            str(property_name): property_value(value)
            for property_name, value in standalone.items()
        }
    )
    expected_properties.update(
        {
            "advertisedAddress": args.service_address,
            "bindAddress": "0.0.0.0",
        }
    )
    for property_name, expected in expected_properties.items():
        actual = properties.get(property_name)
        if actual != expected:
            raise SystemExit(
                f"{config_path}: {property_name} drift: "
                f"runtime={actual!r} expected={expected!r}"
            )

    manifest = {
        "format": "messaging-benchmark.pulsar-runtime-manifest.v1",
        "profile_id": profile.get("profile_id"),
        "profile_sha256": profile_sha256,
        "profile_path": str(profile_path),
        "service_mode": settings.get("service_mode"),
        "service_node": args.service_node,
        "service_address": args.service_address,
        "data_root": args.data_root,
        "jvm_memory": args.jvm_memory,
        "expected_java_major_version": expected_java_major,
        "actual_java_major_version": actual_java_major,
        "java_version": args.java_version,
        "pulsar_version": args.pulsar_version,
        "standalone_config": {
            "path": str(config_path),
            "sha256": sha256_file(config_path),
            "profiled_properties": {
                key: properties[key] for key in sorted(expected_properties)
            },
        },
        "command_contract": (
            "pulsar standalone with one BookKeeper, functions worker disabled, "
            "stream storage disabled, per-case wiped RAM-backed storage"
        ),
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
