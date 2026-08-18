#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SWEEP_DIR = PROJECT_ROOT / "configs" / "sweeps" / "simultaneous_budgeted"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Split the simultaneous budgeted sweep manifest into CSV batches."
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_SWEEP_DIR / "sweep_manifest.csv"),
        help="Sweep manifest CSV to split.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_SWEEP_DIR / "batches"),
        help="Directory where batch_XXX.csv files are written.",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args(argv)

    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be positive")

    manifest_path = Path(args.manifest)
    output_dir = Path(args.output_dir)
    batches = make_batches(
        manifest_path=manifest_path,
        output_dir=output_dir,
        batch_size=args.batch_size,
    )
    print(f"[sweep-batches] manifest: {_project_relative(manifest_path)}")
    print(f"[sweep-batches] batches: {len(batches)}")
    for path, count in batches:
        print(f"[sweep-batches] {path.name}: {count} configs")
    return 0


def make_batches(
    manifest_path: Path,
    output_dir: Path,
    batch_size: int,
) -> list[tuple[Path, int]]:
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    output_dir.mkdir(parents=True, exist_ok=True)

    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if not rows:
        raise ValueError(f"Manifest has no configs: {manifest_path}")
    required = {"config_id", "config_path", "scenario"}
    missing = sorted(required - set(fieldnames))
    if missing:
        raise ValueError(f"Manifest missing required column(s): {', '.join(missing)}")

    batches: list[tuple[Path, int]] = []
    for batch_index, start in enumerate(range(0, len(rows), batch_size), start=1):
        chunk = rows[start : start + batch_size]
        batch_path = output_dir / f"batch_{batch_index:03d}.csv"
        with batch_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk)
        batches.append((batch_path, len(chunk)))
    return batches


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
