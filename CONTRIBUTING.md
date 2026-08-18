# Contributing

Contributions should preserve the backend-independent measurement contract and
keep product-specific behavior inside adapters, profiles, and lifecycle code.

Before opening a change:

1. Run `./benchmark.sh check`.
2. Run relevant dry-run workflows without submitting Slurm jobs.
3. Keep generated results, reports, runtime archives, and local environments out
   of the commit.
4. Do not include credentials, private infrastructure identifiers, or raw
   operational data.
5. Document any intentional change to timing, record accounting, eligibility,
   qualification, or delivery semantics as a new versioned contract.

New backends must implement the adapter and workflow contracts, use the common
record identity and correctness model, namespace product metrics, and include a
fake or dependency-light test path.
