#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.workflow import WorkflowError, get_workflow, list_workflows
from src.benchmark.workflow.engine import WorkflowEngine


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run or validate one complete, restartable messaging benchmark "
            "workflow without mixing historical campaigns."
        )
    )
    parser.add_argument("backend_positional", nargs="?", choices=list_workflows())
    parser.add_argument("--backend", choices=list_workflows())
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="generate and validate plans without calling sbatch or polling Slurm",
    )
    mode.add_argument(
        "--run",
        action="store_true",
        help="execute submissions, polling, validation, and machine-result analysis",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="reuse a matching durable state; requires --run-id",
    )
    parser.add_argument(
        "--phase",
        default="all",
        help="all, phase1, validation, v1, v2, or an exact backend stage name",
    )
    parser.add_argument("--run-id", help="safe immutable workflow identifier")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=PROJECT_ROOT / "results" / "workflows",
        help="workflow-state root; never a historical raw-result directory",
    )
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument(
        "--max-wait-seconds",
        type=int,
        default=0,
        help="zero waits without a workflow-level timeout",
    )
    recovery = parser.add_mutually_exclusive_group()
    recovery.add_argument(
        "--retry-failed-batches",
        dest="retry_failed_batches",
        action="store_true",
        help=(
            "enable bounded automatic repair of failed batches and invalid cases "
            "(the default for real runs; retained as a compatibility flag)"
        ),
    )
    recovery.add_argument(
        "--no-auto-repair",
        dest="retry_failed_batches",
        action="store_false",
        help="disable automatic isolated-case and failed-batch repair",
    )
    parser.set_defaults(retry_failed_batches=True)
    parser.add_argument(
        "--max-case-repair-attempts",
        type=int,
        default=2,
        help="maximum immutable repair attempts per submission stage (default: 2)",
    )
    scheduler = parser.add_argument_group(
        "scheduler settings",
        "captured in workflow_state.json and immutable when the run is resumed",
    )
    scheduler.add_argument(
        "--slurm-account",
        help="Slurm account (defaults to SLURM_ACCOUNT, if set)",
    )
    scheduler.add_argument(
        "--slurm-partition",
        help="Slurm partition (defaults to SLURM_PARTITION, if set)",
    )
    scheduler.add_argument(
        "--slurm-qos",
        help="Slurm QoS (defaults to SLURM_QOS, if set)",
    )
    scheduler.add_argument(
        "--slurm-time",
        help="wall time per batch job (defaults to SLURM_TIME or 02:00:00)",
    )
    scheduler.add_argument(
        "--slurm-constraint",
        help="Slurm node constraint (defaults to SLURM_CONSTRAINT, if set)",
    )
    parser.add_argument(
        "--accept-implementation-update",
        action="append",
        default=[],
        metavar="REPO_PATH",
        help=(
            "adopt or update one backend-declared source input while resuming; "
            "may be repeated and never accepts configs or result data"
        ),
    )
    parser.add_argument(
        "--implementation-update-reason",
        default="",
        help="audit reason required with --accept-implementation-update",
    )
    parser.add_argument(
        "--migrate-only",
        action="store_true",
        help="apply the audited implementation update and exit without running stages",
    )
    args = parser.parse_args(argv)
    backend = args.backend or args.backend_positional
    if not backend:
        parser.error("a backend is required, positionally or with --backend")
    if args.backend and args.backend_positional and args.backend != args.backend_positional:
        parser.error("positional backend and --backend disagree")
    args.backend = backend
    if args.resume and not args.run_id:
        parser.error("--resume requires --run-id")
    if args.max_case_repair_attempts < 0:
        parser.error("--max-case-repair-attempts must not be negative")
    if args.accept_implementation_update and not args.resume:
        parser.error("--accept-implementation-update requires --resume")
    if args.accept_implementation_update and not args.implementation_update_reason.strip():
        parser.error(
            "--implementation-update-reason is required with "
            "--accept-implementation-update"
        )
    if args.implementation_update_reason.strip() and not args.accept_implementation_update:
        parser.error(
            "--implementation-update-reason requires "
            "--accept-implementation-update"
        )
    if args.migrate_only and not args.accept_implementation_update:
        parser.error("--migrate-only requires --accept-implementation-update")
    # The documented one-argument command executes the campaign. Automated
    # validation must always pass --dry-run explicitly.
    args.dry_run = bool(args.dry_run)
    args.run = not args.dry_run
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = args.run_id or _default_run_id(args.backend, args.dry_run)
    workflow = get_workflow(args.backend)
    engine = WorkflowEngine(
        project_root=PROJECT_ROOT,
        output_root=args.output_root,
        workflow_id=run_id,
        backend=workflow,
        dry_run=args.dry_run,
        resume=args.resume,
        requested_phase=args.phase,
        poll_seconds=args.poll_seconds,
        max_wait_seconds=args.max_wait_seconds,
        retry_failed_batches=args.retry_failed_batches and not args.dry_run,
        max_case_repair_attempts=args.max_case_repair_attempts,
        execution_setting_overrides={
            key: value
            for key, value in {
                "account": args.slurm_account,
                "partition": args.slurm_partition,
                "qos": args.slurm_qos,
                "time": args.slurm_time,
                "constraint": args.slurm_constraint,
            }.items()
            if value is not None
        },
        accepted_implementation_updates=args.accept_implementation_update,
        implementation_update_reason=args.implementation_update_reason,
    )
    if args.migrate_only:
        state = engine.close()
        print(f"[workflow] backend: {args.backend}")
        print("[workflow] mode: checkpoint migration only")
        print(f"[workflow] status: {state['status']}")
        print(f"[workflow] state: {engine.state_path}")
        return 0
    state = engine.run()
    print(f"[workflow] backend: {args.backend}")
    print(f"[workflow] mode: {state['mode']}")
    print(f"[workflow] status: {state['status']}")
    print(f"[workflow] state: {engine.state_path}")
    print(f"[workflow] machine artifacts: {engine.run_root / 'artifacts'}")
    print("[workflow] optional reporting was not run")
    return 0


def _default_run_id(backend: str, dry_run: bool) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    mode = "dryrun" if dry_run else "run"
    return f"{backend}_{mode}_{timestamp}"


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (WorkflowError, ValueError) as exc:
        print(f"[workflow] ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
