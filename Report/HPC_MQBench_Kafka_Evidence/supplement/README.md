# Supplemental checks for the paper

These files were derived for the manuscript review from the unchanged retained
campaign evidence. They are **paper artifacts**, not additional outputs produced
by an ordinary benchmark invocation. They contain no new benchmark measurements.

- `policy_sensitivity.csv`: all 113 eligible screening observations reclassified
  under 12 policies, varying one limit at a time. Ratios are recalculated from
  counters. The default is 5% pending deliveries, 10 s flush, and 0.1% failed sends.
- `profile_anchor_comparison.csv`: B0, B3, and B5 on five anchors, using one
  screening observation and two confirmation observations per cell. Each cell
  has three eligible observations; medians include eligible overdriven cases.
  The four maximum-load anchors enter the equal-weight geometric mean of ratios
  to their matching B0 medians. The latency anchor enters only the latency gate.
- `validation_allocations.csv`: all 100 V1/V2 validation observations with their
  manifest block/order, retained batch, qualification, pending ratio, and source
  path. V1 paths explicitly identify jobs 15303484 (blocks 1–3) and 15303485
  (blocks 4–5). V2 has jobs 15324660 and 15324661 in its stage list, but its case
  paths retain batch IDs without job IDs. The V2 per-case job field is therefore
  blank; no exact batch-to-job assignment is invented.
- `review_checks.json`: seed replay, minimum plotted pending ratio, geometric
  means, and the retained profile decision for comparison. Seed 20260728 in the
  inspected generators reproduces all five V1 and V2 orders from the retained
  shortlist. This is a successful replay, not a recovered campaign seed log.
- `qualified_only_ranking.csv`: 20 configuration/stage summaries comparing the
  implemented secondary median (all eligible observations) with a median using
  qualified observations only. Qualified count remains first and all subsequent
  tie-breaks keep their original all-eligible definitions. A zero-qualified case
  has a blank qualified median, which sorts after finite medians within a count
  group. V2's ordering is unchanged. V1 changes cfg_061 from 9 to 6, cfg_107 from
  8 to 7, cfg_007 from 6 to 8, and cfg_049 from 7 to 9. Neither stage leader changes.
- `reviewed_broker_profiles/`: six unchanged JSON files copied from the reviewed
  repository. Their canonical hashes agree with retained tuning rows. See
  `../ARTIFACT_GUIDE.md` for provenance, including the distinction between tuning
  B0 and the frozen B0 profile used in final validation.

The confirmed profile ratios and qualification counts are:

| Anchor | B0 qualified / 3 | B3 rate / B0 | B3 qualified / 3 | B5 rate / B0 | B5 qualified / 3 |
|---|---:|---:|---:|---:|---:|
| cfg_107 | 3 | 0.992880 | 3 | 0.992568 | 3 |
| cfg_061 | 3 | 1.002306 | 3 | 1.001930 | 3 |
| cfg_094 | 3 | 0.994669 | 3 | 0.994179 | 3 |
| cfg_101 | 0 | 1.077089 | 3 | 1.076280 | 3 |
| latency_anchor (excluded from rate geometric mean) | 3 | 1.000104 | 3 | 0.999840 | 3 |

The four-anchor geometric means are 1.0161478521 for B3 and 1.0156566786 for B5,
below 1.03. The latency-anchor p99 ratios are 324/544 and 325/544, respectively.
The qualified-count nonregression gate uses cfg_107 and cfg_061 specifically.
These comparisons describe the retained observations; they do not remove
allocation or time confounding.

The inspected randomization uses Python `random.Random(20260728).shuffle`.
One generator is initialized per stage and advances through five blocks;
each block starts with a fresh copy of the original shortlist. Blocks therefore
have individually shuffled orders; V2 reuses the same sequence of five orders
as V1. The explicit manifest orders, not the seed alone, are the retained record.

From the paper folder, regenerate with
`python3 -B scripts/build_referee_supplement.py`, or check without writing with
`python3 -B scripts/build_referee_supplement.py --check`. `make verify` also checks
these files and compares the profile arithmetic with the retained decision.
