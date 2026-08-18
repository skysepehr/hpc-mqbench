#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.broker_profile import load_broker_profile


PROPERTY_MAP = {
    "num_network_threads": "num.network.threads",
    "num_io_threads": "num.io.threads",
    "socket_send_buffer_bytes": "socket.send.buffer.bytes",
    "socket_receive_buffer_bytes": "socket.receive.buffer.bytes",
    "socket_request_max_bytes": "socket.request.max.bytes",
    "queued_max_requests": "queued.max.requests",
    "log_segment_bytes": "log.segment.bytes",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reject broker runtime settings that drift from a V2 profile"
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--server-properties", required=True, nargs="+")
    parser.add_argument("--heap-opts", required=True)
    parser.add_argument("--java-version", required=True)
    parser.add_argument("--kafka-version", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def parse_properties(path: Path) -> dict[str, str]:
    properties: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()
    return properties


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_java_major(version_text: str) -> int:
    version_match = re.search(
        r'(?:java|openjdk) version "(?P<major>[0-9]+)(?:[.\-"][^"]*)?"',
        version_text,
        flags=re.IGNORECASE,
    )
    if version_match is None:
        raise ValueError(f"could not parse Java version: {version_text!r}")
    return int(version_match.group("major"))


def main() -> None:
    args = parse_args()
    profile = load_broker_profile(args.profile)
    fixed_semantics = profile.payload.get("fixed_semantics", {})
    expected_java_major = int(fixed_semantics.get("java_major_version", 17))
    try:
        actual_java_major = parse_java_major(args.java_version)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    if actual_java_major != expected_java_major:
        raise SystemExit(
            "Java major version drift: "
            f"runtime={actual_java_major} profile={expected_java_major}"
        )
    if args.heap_opts != str(profile.settings["heap_opts"]):
        raise SystemExit(
            "KAFKA_HEAP_OPTS drift: "
            f"runtime={args.heap_opts!r} profile={profile.settings['heap_opts']!r}"
        )

    server_records = []
    for raw_path in args.server_properties:
        path = Path(raw_path)
        properties = parse_properties(path)
        for setting_name, property_name in PROPERTY_MAP.items():
            actual = properties.get(property_name)
            expected = str(profile.settings[setting_name])
            if actual != expected:
                raise SystemExit(
                    f"{path}: {property_name} drift: runtime={actual!r} "
                    f"profile={expected!r}"
                )
        contract = profile.payload.get("server_properties_contract", {})
        if not isinstance(contract, dict):
            raise SystemExit("profile server_properties_contract must be an object")
        for property_name, expected_value in contract.items():
            actual = properties.get(str(property_name))
            expected = str(expected_value)
            if actual != expected:
                raise SystemExit(
                    f"{path}: {property_name} drift: runtime={actual!r} "
                    f"profile={expected!r}"
                )
        server_records.append(
            {
                "path": str(path),
                "sha256": sha256_file(path),
                "profiled_properties": {
                    property_name: properties[property_name]
                    for property_name in PROPERTY_MAP.values()
                },
                "contract_properties": {
                    property_name: properties[property_name]
                    for property_name in contract
                },
            }
        )

    frozen_runtime = profile.payload.get("frozen_runtime_evidence")
    if isinstance(frozen_runtime, dict):
        expected_java = frozen_runtime.get("java_version")
        expected_kafka = frozen_runtime.get("kafka_version")
        if expected_java and args.java_version != expected_java:
            raise SystemExit(
                "Java runtime drift from frozen evidence: "
                f"runtime={args.java_version!r} frozen={expected_java!r}"
            )
        if expected_kafka and args.kafka_version != expected_kafka:
            raise SystemExit(
                "Kafka runtime drift from frozen evidence: "
                f"runtime={args.kafka_version!r} frozen={expected_kafka!r}"
            )

    payload = {
        "format": "kafka_broker_runtime_manifest.v2",
        "profile_id": profile.profile_id,
        "profile_sha256": profile.sha256,
        "profile_path": str(profile.path),
        "heap_opts": args.heap_opts,
        "jvm_command_contract": (
            "kafka-server-start.sh <generated-server.properties> with "
            "KAFKA_HEAP_OPTS and the JMX exporter javaagent"
        ),
        "expected_java_major_version": expected_java_major,
        "actual_java_major_version": actual_java_major,
        "java_version": args.java_version,
        "kafka_version": args.kafka_version,
        "server_properties": server_records,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
