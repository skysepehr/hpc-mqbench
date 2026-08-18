from __future__ import annotations

import argparse
import csv
import os
import time
from pathlib import Path
from typing import Any


CSV_FIELDS = (
    "timestamp_unix",
    "elapsed_sec",
    "pid",
    "logical_cpu_count",
    "host_logical_cpu_count",
    "allocated_logical_cpu_count",
    "process_cpu_cores",
    "process_cpu_percent_of_one_core",
    "process_cpu_percent_of_node",
    "process_cpu_percent_of_allocated",
    "process_rss_bytes",
    "process_threads",
    "process_open_fds",
    "ib0_rx_bytes_total",
    "ib0_tx_bytes_total",
    "ib0_rx_bytes_per_sec",
    "ib0_tx_bytes_per_sec",
    "tmpfs_total_bytes",
    "tmpfs_used_bytes",
    "tmpfs_used_percent",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect one-second backend-process, network-interface, and "
            "tmpfs evidence"
        )
    )
    parser.add_argument("--pid-file", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--tmpfs-path", required=True)
    parser.add_argument("--interface", default="ib0")
    parser.add_argument("--interval-sec", type=float, default=1.0)
    parser.add_argument("--pid-wait-sec", type=float, default=60.0)
    return parser.parse_args()


def wait_for_pid(path: Path, timeout_sec: float) -> int:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if path.is_file():
            try:
                pid = int(path.read_text(encoding="utf-8").strip())
                os.kill(pid, 0)
                return pid
            except (OSError, ValueError):
                pass
        time.sleep(0.1)
    raise TimeoutError(
        f"Backend process PID not available within {timeout_sec:.1f}s: {path}"
    )


def read_process_ticks(pid: int) -> int:
    fields = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
    return int(fields[13]) + int(fields[14])


def read_status(pid: int) -> dict[str, int]:
    result = {"process_rss_bytes": 0, "process_threads": 0}
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            result["process_rss_bytes"] = int(line.split()[1]) * 1_024
        elif line.startswith("Threads:"):
            result["process_threads"] = int(line.split()[1])
    return result


def read_counter(path: Path) -> int:
    return int(path.read_text(encoding="utf-8").strip())


def filesystem_usage(path: Path) -> tuple[int, int, float]:
    stat = os.statvfs(path)
    total = stat.f_blocks * stat.f_frsize
    available = stat.f_bavail * stat.f_frsize
    used = max(0, total - available)
    percent = (100.0 * used / total) if total > 0 else 0.0
    return total, used, percent


def monitor(args: argparse.Namespace) -> None:
    if args.interval_sec <= 0:
        raise ValueError("interval-sec must be greater than 0")

    pid_file = Path(args.pid_file)
    output = Path(args.output)
    stop_file = Path(args.stop_file)
    tmpfs_path = Path(args.tmpfs_path)
    pid = wait_for_pid(pid_file, args.pid_wait_sec)
    output.parent.mkdir(parents=True, exist_ok=True)

    clock_ticks = os.sysconf("SC_CLK_TCK")
    host_logical_cpus = os.cpu_count() or 1
    try:
        # Normalize to the service process's Slurm/cgroup CPU allowance, not
        # the one-task monitoring step's affinity.
        allocated_logical_cpus = len(os.sched_getaffinity(pid))
    except AttributeError:
        allocated_logical_cpus = host_logical_cpus
    allocated_logical_cpus = max(1, allocated_logical_cpus)
    interface_root = Path("/sys/class/net") / args.interface / "statistics"
    rx_path = interface_root / "rx_bytes"
    tx_path = interface_root / "tx_bytes"
    if not rx_path.is_file() or not tx_path.is_file():
        raise FileNotFoundError(
            f"network interface counters not found for {args.interface}"
        )

    start_monotonic = time.monotonic()
    previous_time = start_monotonic
    previous_ticks = read_process_ticks(pid)
    previous_rx = read_counter(rx_path)
    previous_tx = read_counter(tx_path)

    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        while not stop_file.exists():
            try:
                os.kill(pid, 0)
                now_monotonic = time.monotonic()
                now_unix = time.time()
                ticks = read_process_ticks(pid)
                rx_bytes = read_counter(rx_path)
                tx_bytes = read_counter(tx_path)
                elapsed = max(now_monotonic - previous_time, 1e-9)
                cpu_cores = max(0.0, (ticks - previous_ticks) / clock_ticks / elapsed)
                status = read_status(pid)
                total, used, used_percent = filesystem_usage(tmpfs_path)
                row: dict[str, Any] = {
                    "timestamp_unix": f"{now_unix:.6f}",
                    "elapsed_sec": f"{now_monotonic - start_monotonic:.6f}",
                    "pid": pid,
                    # Kept for compatibility; V2 defines this as CPUs available
                    # to the monitor's Slurm step.
                    "logical_cpu_count": allocated_logical_cpus,
                    "host_logical_cpu_count": host_logical_cpus,
                    "allocated_logical_cpu_count": allocated_logical_cpus,
                    "process_cpu_cores": f"{cpu_cores:.6f}",
                    "process_cpu_percent_of_one_core": f"{100.0 * cpu_cores:.6f}",
                    "process_cpu_percent_of_node": (
                        f"{100.0 * cpu_cores / host_logical_cpus:.6f}"
                    ),
                    "process_cpu_percent_of_allocated": (
                        f"{100.0 * cpu_cores / allocated_logical_cpus:.6f}"
                    ),
                    "process_rss_bytes": status["process_rss_bytes"],
                    "process_threads": status["process_threads"],
                    "process_open_fds": len(list(Path(f"/proc/{pid}/fd").iterdir())),
                    "ib0_rx_bytes_total": rx_bytes,
                    "ib0_tx_bytes_total": tx_bytes,
                    "ib0_rx_bytes_per_sec": (
                        f"{max(0, rx_bytes - previous_rx) / elapsed:.6f}"
                    ),
                    "ib0_tx_bytes_per_sec": (
                        f"{max(0, tx_bytes - previous_tx) / elapsed:.6f}"
                    ),
                    "tmpfs_total_bytes": total,
                    "tmpfs_used_bytes": used,
                    "tmpfs_used_percent": f"{used_percent:.6f}",
                }
                writer.writerow(row)
                handle.flush()
                previous_time = now_monotonic
                previous_ticks = ticks
                previous_rx = rx_bytes
                previous_tx = tx_bytes
            except (FileNotFoundError, ProcessLookupError):
                break
            time.sleep(args.interval_sec)


def main() -> None:
    monitor(parse_args())


if __name__ == "__main__":
    main()
