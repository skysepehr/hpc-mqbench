from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from src.benchmark.local_deps import ensure_repo_local_dependencies

ensure_repo_local_dependencies()

from models.experiment_case import ExperimentCase
from src.benchmark.config_loader import load_experiment_case
from src.benchmark.report_builder import ReportBuilder


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the benchmark entrypoint.

    This entrypoint currently supports one exact case file.
    Later, campaign mode can still call this same entrypoint repeatedly
    for each generated case.
    """
    parser = argparse.ArgumentParser(
        description="Distributed messaging HPC benchmark entrypoint"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the single-case JSON configuration file",
    )
    parser.add_argument(
        "--case-id",
        default="case_001",
        help="Case identifier used for reporting and output paths",
    )
    parser.add_argument(
        "--case-name",
        default="single_case",
        help="Human-readable case name",
    )
    parser.add_argument(
        "--output-dir",
        default="results/case_001",
        help="Directory where benchmark outputs should be written",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Backend service endpoints supplied by the lifecycle adapter",
    )
    parser.add_argument(
        "--report-mode",
        choices=("full", "light", "machine"),
        default=os.environ.get("BENCHMARK_REPORT_MODE", "full"),
        help=(
            "Output mode. 'machine' writes canonical JSON only; 'light' also "
            "writes Markdown; 'full' additionally writes graphs and HTML."
        ),
    )
    return parser.parse_args()


def validate_world_size(case: ExperimentCase, world_size: int) -> None:
    """
    Validate that the actual MPI world size matches the configured rank layout.

    Expected rank layout:
    - rank 0 = controller
    - producer ranks = config.producer_ranks
    - consumer ranks = config.consumer_ranks

    This validation catches mismatches early, before the benchmark begins.
    """
    expected_world_size = case.config.total_mpi_ranks
    if world_size != expected_world_size:
        raise RuntimeError(
            "MPI world size mismatch: "
            f"expected {expected_world_size}, got {world_size}. "
            "Check your MPI launcher and config values."
        )


def load_case_from_args(args: argparse.Namespace) -> ExperimentCase:
    """
    Load one experiment case from the provided command-line arguments.
    """
    case = load_experiment_case(
        path=args.config,
        case_id=args.case_id,
        case_name=args.case_name,
        output_dir=args.output_dir,
    )
    config_path = Path(args.config)
    case.notes["config_path"] = _portable_config_path(config_path)
    case.notes["config_abspath"] = str(config_path.resolve())
    return case


def _portable_config_path(path: Path) -> str:
    """
    Prefer a project-relative config path in reports while retaining the
    absolute path separately for audit/debug use.
    """
    try:
        return path.resolve().relative_to(Path.cwd().resolve()).as_posix()
    except ValueError:
        return str(path)


def load_mpi() -> object:
    """
    Import mpi4py only when benchmark execution actually starts.

    Keeping this lazy lets help/config checks run on systems where MPI cannot
    initialize in the current shell, while real benchmark execution still uses
    MPI exactly as before.
    """
    ensure_repo_local_dependencies()
    from mpi4py import MPI

    return MPI


def main() -> None:
    """
    Main benchmark entrypoint.

    Current behavior:
    1. parse command-line arguments
    2. load one experiment case
    3. initialize MPI
    4. validate MPI world size
    5. run the benchmark controller
    6. on rank 0, write final reports

    Notes
    -----
    - The BenchmarkController internally dispatches producer and consumer work
      according to MPI rank roles.
    - Only rank 0 writes the final benchmark reports.
    """
    args = parse_args()
    rank = 0

    try:
        # Load the benchmark case description.
        case = load_case_from_args(args)

        # Initialize the MPI communicator.
        MPI = load_mpi()
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        world_size = comm.Get_size()

        from src.benchmark.benchmark_controller import BenchmarkController

        # Validate that the launched MPI size matches the config.
        validate_world_size(case, world_size)

        # Create the inner benchmark controller.
        controller = BenchmarkController(
            case=case,
            bootstrap_servers=args.bootstrap_servers,
            output_dir=args.output_dir,
        )

        # Run the benchmark. Only rank 0 will receive the final aggregated result.
        final_result = controller.run()

        # Only rank 0 should enrich and persist the canonical case result.
        if rank == 0:
            report_builder = ReportBuilder(
                output_dir=args.output_dir,
                report_mode=args.report_mode,
            )
            report_builder.build(final_result)

            print(
                f"[rank 0] Benchmark completed successfully. "
                f"Artifacts written to: {Path(args.output_dir)}",
                flush=True,
            )

    except Exception as exc:
        # Print rank-specific errors clearly so failures are easier to debug
        # in distributed runs.
        print(f"[rank {rank}] ERROR: {exc}", file=sys.stderr, flush=True)
        raise


if __name__ == "__main__":
    main()
