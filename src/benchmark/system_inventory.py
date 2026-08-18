from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import socket
import subprocess
import time
from pathlib import Path
from typing import Any


def load_system_inventory(output_dir: str | Path) -> dict[str, Any] | None:
    """Load the per-case system inventory artifact when it exists."""
    output_path = Path(output_dir)
    for path in (
        output_path / "runtime" / "system_inventory" / "system_inventory.json",
        output_path / "data" / "system_inventory.json",
        output_path / "system_inventory.json",
    ):
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            return data
    return None


def merge_system_inventory_into_result(
    benchmark_result: dict[str, Any],
    inventory: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach system capability data without changing benchmark metrics."""
    if isinstance(inventory, dict):
        benchmark_result["system_inventory"] = inventory
    else:
        benchmark_result.setdefault(
            "system_inventory",
            {
                "format": "system_inventory.v1",
                "enabled": False,
                "status": "not_collected",
                "nodes": [],
                "network_tests": [],
            },
        )
    return benchmark_result


def parse_iperf3_json(payload: dict[str, Any], requested_seconds: float | None = None) -> dict[str, Any]:
    """Extract the most useful capacity numbers from iperf3 -J output."""
    if not isinstance(payload, dict):
        raise ValueError("iperf3 payload must be a JSON object")

    end = payload.get("end") if isinstance(payload.get("end"), dict) else {}
    start = payload.get("start") if isinstance(payload.get("start"), dict) else {}
    connected = start.get("connected") if isinstance(start.get("connected"), list) else []

    candidates: list[tuple[str, dict[str, Any]]] = []
    for key in ("sum_received", "sum_sent", "sum"):
        value = end.get(key)
        if isinstance(value, dict) and value.get("bits_per_second") is not None:
            candidates.append((key, value))
    if not candidates:
        streams = end.get("streams") if isinstance(end.get("streams"), list) else []
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            receiver = stream.get("receiver")
            if isinstance(receiver, dict) and receiver.get("bits_per_second") is not None:
                candidates.append(("stream_receiver", receiver))
                break

    if not candidates:
        raise ValueError("iperf3 JSON did not include an end-of-test bandwidth summary")

    summary_source, candidate = _select_iperf_summary(candidates, requested_seconds)
    bits_per_second = _as_float(candidate.get("bits_per_second"))
    bytes_transferred = _as_float(candidate.get("bytes"))
    seconds = _as_float(candidate.get("seconds"))
    return {
        "summary_source": summary_source,
        "bits_per_second": bits_per_second,
        "gbit_per_second": bits_per_second / 1_000_000_000.0,
        "megabytes_per_second": bits_per_second / 8_000_000.0,
        "bytes_transferred": bytes_transferred,
        "seconds": seconds,
        "measured_seconds": seconds,
        "parallel_streams": len(connected),
    }


def parse_stream_probe_output(text: str) -> dict[str, Any]:
    """Parse the repo-local STREAM-style probe JSON output."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"STREAM probe output is not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("STREAM probe output must be a JSON object")
    if payload.get("status") != "completed":
        return payload
    payload.setdefault("measurement_scope", "single_process_stream_style")
    payload.setdefault(
        "measurement_note",
        "Single-process RAM bandwidth in GB/s. read/write are pure directional kernels when present; copy/scale/add/triad are STREAM-style kernels. This is not full-node aggregate memory bandwidth.",
    )
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError("STREAM probe output is missing metrics")
    for key in ("copy_gb_s", "scale_gb_s", "add_gb_s", "triad_gb_s"):
        if key not in metrics:
            raise ValueError(f"STREAM probe output is missing {key}")
        metrics[key] = _as_float(metrics[key])
    for key in ("read_gb_s", "write_gb_s"):
        if key in metrics:
            metrics[key] = _as_float(metrics[key])
    if "read_gb_s" in metrics and "write_gb_s" in metrics:
        payload.setdefault(
            "read_write_note",
            "read_gb_s and write_gb_s are pure single-process read-only/write-only loop measurements.",
        )
    else:
        payload.setdefault(
            "read_write_note",
            "This probe output predates explicit read/write kernels; only mixed STREAM-style RAM bandwidth is available.",
        )
    return payload


def collect_local_node(
    node: str,
    role: str,
    service_address: str,
    interface: str,
    ram_probe: str | None,
    ram_array_mb: int,
    ram_timeout_sec: int,
) -> dict[str, Any]:
    """Collect facts and measured RAM bandwidth on the current node."""
    effective_interface = _select_interface(interface)
    record: dict[str, Any] = {
        "node": node,
        "observed_hostname": socket.gethostname(),
        "service_address": service_address,
        "role": role,
        "created_at": _now_iso(),
        "cpu": _collect_cpu(),
        "memory": _collect_memory(),
        "numa": _collect_numa(),
        "network": _collect_network(effective_interface),
    }
    record["memory"]["bandwidth_probe"] = _run_ram_probe(
        ram_probe=ram_probe,
        array_mb=ram_array_mb,
        timeout_sec=ram_timeout_sec,
    )
    return record


def merge_inventory(
    nodes_dir: str | Path,
    iperf_dir: str | Path,
    output: str | Path,
    settings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge per-node and per-path capability records into one artifact."""
    node_records = _load_json_files(Path(nodes_dir))
    iperf_records = _load_json_files(Path(iperf_dir))
    nodes = sorted(
        (record for record in node_records if isinstance(record, dict)),
        key=lambda item: str(item.get("node", "")),
    )
    network_tests = sorted(
        (record for record in iperf_records if isinstance(record, dict)),
        key=lambda item: str(item.get("label", "")),
    )
    probe_summary = _build_probe_summary(nodes, network_tests, settings or {})
    statuses = [str(item.get("status", "completed")) for item in nodes + network_tests]
    if not nodes:
        status = "skipped"
    elif any(value in {"failed", "skipped"} for value in statuses):
        status = "partial"
    else:
        status = "completed"

    payload = {
        "format": "system_inventory.v1",
        "enabled": True,
        "status": status,
        "created_at": _now_iso(),
        "settings": settings or {},
        "probe_summary": probe_summary,
        "nodes": nodes,
        "network_tests": network_tests,
    }
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload


def _build_probe_summary(
    nodes: list[dict[str, Any]],
    network_tests: list[dict[str, Any]],
    settings: dict[str, Any],
) -> dict[str, Any]:
    required_network_labels = ["producer_to_broker", "broker_to_consumer"]
    network_by_label = {str(item.get("label", "")): item for item in network_tests}
    iperf_enabled = bool(settings.get("iperf_enabled", True))
    missing_required = [
        label for label in required_network_labels if iperf_enabled and label not in network_by_label
    ]
    failed_required = [
        label
        for label in required_network_labels
        if iperf_enabled
        and label in network_by_label
        and network_by_label[label].get("status") != "completed"
    ]
    failed_nodes = [
        str(node.get("node", "unknown"))
        for node in nodes
        if node.get("status", "completed") not in {"completed", None}
    ]
    ram_probe_issues = []
    for node in nodes:
        probe = _as_dict(_as_dict(node.get("memory")).get("bandwidth_probe"))
        if probe and probe.get("status") != "completed":
            ram_probe_issues.append(
                {
                    "node": node.get("node", "unknown"),
                    "status": probe.get("status", "unknown"),
                    "reason": probe.get("reason", ""),
                }
            )
    warnings = []
    if missing_required:
        warnings.append("missing required iperf3 path(s): " + ", ".join(missing_required))
    if failed_required:
        warnings.append("required iperf3 path(s) did not complete: " + ", ".join(failed_required))
    if failed_nodes:
        warnings.append("node inventory failed on: " + ", ".join(failed_nodes))
    if ram_probe_issues:
        warnings.append("one or more RAM bandwidth probes were skipped or failed")
    return {
        "node_count": len(nodes),
        "failed_nodes": failed_nodes,
        "network_test_count": len(network_tests),
        "completed_network_tests": [
            str(item.get("label", "unknown"))
            for item in network_tests
            if item.get("status") == "completed"
        ],
        "failed_network_tests": [
            str(item.get("label", "unknown"))
            for item in network_tests
            if item.get("status") == "failed"
        ],
        "skipped_network_tests": [
            str(item.get("label", "unknown"))
            for item in network_tests
            if item.get("status") == "skipped"
        ],
        "required_network_tests": required_network_labels if iperf_enabled else [],
        "missing_required_network_tests": missing_required,
        "failed_required_network_tests": failed_required,
        "required_network_tests_completed": (
            iperf_enabled and not missing_required and not failed_required
        ),
        "ram_probe_issues": ram_probe_issues,
        "failed_required_probe_count": len(missing_required) + len(failed_required) + len(failed_nodes),
        "warnings": warnings,
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def normalize_iperf_record(
    raw_input: str | Path,
    output: str | Path,
    label: str,
    source_node: str,
    target_node: str,
    target_address: str,
    port: int,
    seconds: int,
    parallel: int,
    status: str = "completed",
    reason: str = "",
) -> dict[str, Any]:
    """Write a stable iperf3 summary record from raw JSON or a skip reason."""
    raw_path = Path(raw_input)
    record: dict[str, Any] = {
        "label": label,
        "source_node": source_node,
        "target_node": target_node,
        "target_address": target_address,
        "port": port,
        "requested_seconds": seconds,
        "parallel_streams_requested": parallel,
        "status": status,
        "created_at": _now_iso(),
    }
    if status == "completed":
        try:
            payload = _load_json_object_from_text(raw_path.read_text(encoding="utf-8"))
            record.update(parse_iperf3_json(payload, requested_seconds=float(seconds)))
            record["raw_path"] = raw_path.as_posix()
        except Exception as exc:  # pragma: no cover - exercised by shell path
            record["status"] = "failed"
            record["reason"] = f"could not parse iperf3 output: {type(exc).__name__}: {exc}"
    else:
        record["reason"] = reason or "iperf3 test skipped"

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
    return record


def _load_json_object_from_text(text: str) -> dict[str, Any]:
    """Parse a JSON object, tolerating module-loader lines before iperf3 JSON."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise
        payload = json.loads(text[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("JSON payload is not an object")
    return payload


def _select_iperf_summary(
    candidates: list[tuple[str, dict[str, Any]]],
    requested_seconds: float | None,
) -> tuple[str, dict[str, Any]]:
    if requested_seconds and requested_seconds > 0:
        def score(item: tuple[str, dict[str, Any]]) -> tuple[float, int]:
            name, summary = item
            seconds = _as_float(summary.get("seconds"))
            duration_delta = abs(seconds - requested_seconds) if seconds > 0 else float("inf")
            priority = {"sum_received": 0, "sum_sent": 1, "sum": 2, "stream_receiver": 3}.get(name, 9)
            return (duration_delta, priority)

        return min(candidates, key=score)

    priority = {"sum_received": 0, "sum_sent": 1, "sum": 2, "stream_receiver": 3}
    return min(candidates, key=lambda item: priority.get(item[0], 9))


def _collect_cpu() -> dict[str, Any]:
    lscpu_payload = _run_json_command(["lscpu", "--json"])
    fields = _lscpu_fields(lscpu_payload)
    return {
        "architecture": fields.get("Architecture", platform.machine()),
        "model_name": fields.get("Model name", fields.get("CPU(s) scaling MHz", "unknown")),
        "sockets": _parse_int(fields.get("Socket(s)")),
        "cores_per_socket": _parse_int(fields.get("Core(s) per socket")),
        "threads_per_core": _parse_int(fields.get("Thread(s) per core")),
        "logical_cpus": _parse_int(fields.get("CPU(s)")) or os.cpu_count(),
        "max_mhz": _parse_float(fields.get("CPU max MHz")),
        "min_mhz": _parse_float(fields.get("CPU min MHz")),
        "vendor": fields.get("Vendor ID", "unknown"),
        "raw_lscpu": fields,
    }


def _collect_memory() -> dict[str, Any]:
    meminfo = _parse_meminfo()
    total_kb = _parse_float(meminfo.get("MemTotal")) or 0.0
    return {
        "total_gb": total_kb * 1024.0 / 1_000_000_000.0,
        "meminfo": meminfo,
    }


def _collect_numa() -> dict[str, Any]:
    result = _run_text_command(["numactl", "--hardware"], timeout=10)
    node_count = None
    if result["status"] == "completed":
        match = re.search(r"available:\s+(\d+)\s+nodes", result.get("stdout", ""))
        if match:
            node_count = int(match.group(1))
    return {
        "status": result["status"],
        "node_count": node_count,
        "raw": result.get("stdout", ""),
        "error": result.get("stderr", ""),
    }


def _collect_network(interface: str) -> dict[str, Any]:
    address = _first_line(
        _run_text_command(
            ["bash", "-lc", f"ip -4 -o addr show dev {interface!r} | awk '{{print $4}}' | head -1"],
            timeout=10,
        ).get("stdout", "")
    )
    sysfs_speed_mbit = _read_sysfs_speed_mbit(interface)
    ethtool_speed_mbit = _read_ethtool_speed_mbit(interface)
    speed_mbit = sysfs_speed_mbit or ethtool_speed_mbit
    return {
        "interface": interface,
        "ipv4_cidr": address,
        "link_speed_mbit": speed_mbit,
        "link_speed_gbit": (speed_mbit / 1000.0) if speed_mbit else None,
        "sysfs_speed_mbit": sysfs_speed_mbit,
        "ethtool_speed_mbit": ethtool_speed_mbit,
        "route": _run_text_command(["ip", "route", "show", "dev", interface], timeout=10).get("stdout", ""),
    }


def _run_ram_probe(
    ram_probe: str | None,
    array_mb: int,
    timeout_sec: int,
) -> dict[str, Any]:
    if not ram_probe:
        return {"status": "skipped", "reason": "ram probe path was not provided"}
    probe_path = Path(ram_probe)
    if not probe_path.is_file() or not os.access(probe_path, os.X_OK):
        return {"status": "skipped", "reason": f"ram probe is not executable: {ram_probe}"}
    result = _run_text_command([str(probe_path), str(array_mb)], timeout=timeout_sec)
    if result["status"] != "completed":
        return {
            "status": result["status"],
            "reason": result.get("stderr") or result.get("reason") or "ram probe failed",
            "stdout": result.get("stdout", ""),
        }
    try:
        return parse_stream_probe_output(result.get("stdout", ""))
    except ValueError as exc:
        return {"status": "failed", "reason": str(exc), "stdout": result.get("stdout", "")}


def _select_interface(interface: str) -> str:
    if interface and interface not in {"host", "hostname", "none", "default"}:
        return interface
    result = _run_text_command(
        ["bash", "-lc", "ip route get 1.1.1.1 | awk '{for (i=1;i<=NF;i++) if ($i==\"dev\") {print $(i+1); exit}}'"],
        timeout=10,
    )
    selected = _first_line(result.get("stdout", ""))
    return selected or "unknown"


def _read_sysfs_speed_mbit(interface: str) -> float | None:
    try:
        value = Path("/sys/class/net") / interface / "speed"
        raw = value.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    parsed = _parse_float(raw)
    return parsed if parsed and parsed > 0 else None


def _read_ethtool_speed_mbit(interface: str) -> float | None:
    result = _run_text_command(["ethtool", interface], timeout=10)
    if result["status"] != "completed":
        return None
    match = re.search(r"Speed:\s*([0-9.]+)\s*([GMK]?b/s)", result.get("stdout", ""), re.IGNORECASE)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2).lower()
    if unit.startswith("gb"):
        return value * 1000.0
    if unit.startswith("kb"):
        return value / 1000.0
    return value


def _lscpu_fields(payload: dict[str, Any]) -> dict[str, str]:
    items = payload.get("lscpu") if isinstance(payload, dict) else None
    fields: dict[str, str] = {}
    if not isinstance(items, list):
        return fields
    for item in items:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field", "")).rstrip(":")
        value = str(item.get("data", ""))
        if field:
            fields[field] = value
    return fields


def _parse_meminfo() -> dict[str, str]:
    meminfo: dict[str, str] = {}
    try:
        text = Path("/proc/meminfo").read_text(encoding="utf-8")
    except OSError:
        return meminfo
    for line in text.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        meminfo[key.strip()] = value.strip().split()[0]
    return meminfo


def _run_json_command(command: list[str], timeout: int = 20) -> dict[str, Any]:
    result = _run_text_command(command, timeout=timeout)
    if result["status"] != "completed":
        return {}
    try:
        payload = json.loads(result.get("stdout", ""))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _run_text_command(command: list[str], timeout: int = 20) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        return {"status": "skipped", "reason": str(exc), "stdout": "", "stderr": ""}
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "failed",
            "reason": f"timed out after {timeout} seconds",
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
        }
    return {
        "status": "completed" if proc.returncode == 0 else "failed",
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _load_json_files(directory: Path) -> list[Any]:
    records: list[Any] = []
    if not directory.is_dir():
        return records
    for path in sorted(directory.glob("*.json")):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            records.append({"status": "failed", "path": path.as_posix(), "reason": "could not parse JSON"})
    return records


def _parse_int(value: Any) -> int | None:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    match = re.search(r"-?[0-9]+(?:\.[0-9]+)?", str(value).replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _as_float(value: Any) -> float:
    parsed = _parse_float(value)
    return parsed if parsed is not None else 0.0


def _first_line(text: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _now_iso() -> str:
    return dt.datetime.fromtimestamp(time.time(), tz=dt.timezone.utc).isoformat()


def _main() -> int:
    parser = argparse.ArgumentParser(description="Collect or normalize system inventory artifacts")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect_parser = subparsers.add_parser("collect-node")
    collect_parser.add_argument("--node", required=True)
    collect_parser.add_argument("--role", required=True)
    collect_parser.add_argument("--service-address", required=True)
    collect_parser.add_argument("--interface", default="ib0")
    collect_parser.add_argument("--ram-probe", default="")
    collect_parser.add_argument("--ram-array-mb", type=int, default=512)
    collect_parser.add_argument("--ram-timeout-sec", type=int, default=60)
    collect_parser.add_argument("--output", required=True)

    iperf_parser = subparsers.add_parser("normalize-iperf")
    iperf_parser.add_argument("--raw-input", required=True)
    iperf_parser.add_argument("--output", required=True)
    iperf_parser.add_argument("--label", required=True)
    iperf_parser.add_argument("--source-node", required=True)
    iperf_parser.add_argument("--target-node", required=True)
    iperf_parser.add_argument("--target-address", required=True)
    iperf_parser.add_argument("--port", type=int, required=True)
    iperf_parser.add_argument("--seconds", type=int, required=True)
    iperf_parser.add_argument("--parallel", type=int, required=True)
    iperf_parser.add_argument("--status", default="completed")
    iperf_parser.add_argument("--reason", default="")

    merge_parser = subparsers.add_parser("merge")
    merge_parser.add_argument("--nodes-dir", required=True)
    merge_parser.add_argument("--iperf-dir", required=True)
    merge_parser.add_argument("--output", required=True)
    merge_parser.add_argument("--settings-json", default="{}")

    args = parser.parse_args()

    if args.command == "collect-node":
        record = collect_local_node(
            node=args.node,
            role=args.role,
            service_address=args.service_address,
            interface=args.interface,
            ram_probe=args.ram_probe or None,
            ram_array_mb=args.ram_array_mb,
            ram_timeout_sec=args.ram_timeout_sec,
        )
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
        return 0

    if args.command == "normalize-iperf":
        normalize_iperf_record(
            raw_input=args.raw_input,
            output=args.output,
            label=args.label,
            source_node=args.source_node,
            target_node=args.target_node,
            target_address=args.target_address,
            port=args.port,
            seconds=args.seconds,
            parallel=args.parallel,
            status=args.status,
            reason=args.reason,
        )
        return 0

    if args.command == "merge":
        try:
            settings = json.loads(args.settings_json)
        except json.JSONDecodeError:
            settings = {}
        merge_inventory(
            nodes_dir=args.nodes_dir,
            iperf_dir=args.iperf_dir,
            output=args.output,
            settings=settings if isinstance(settings, dict) else {},
        )
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(_main())
