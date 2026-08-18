# Simultaneous Validation Sweep

This directory defines a five-repetition validation experiment from the existing 120-configuration sweep. These files are run inputs only; generating them does not execute benchmarks.

Randomization seed: `20260716`.

- `validation_manifest.csv` lists selected configurations and why they were selected.
- `validation_run_order.csv` contains five randomized blocks; each selected configuration appears exactly once per block.
- Existing generated config JSON files are referenced by path and are not copied or modified.

After repetitions are run, rank configurations by qualified-run frequency first, then median balanced throughput, with variability/backlog/flush/failures as secondary criteria.
