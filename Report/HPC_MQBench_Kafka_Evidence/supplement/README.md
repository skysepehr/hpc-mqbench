# Supplemental data checks

These numerical analyses were derived from unchanged retained campaign evidence.
They are downstream analyses, not extra benchmark observations or additional
outputs of every ordinary benchmark invocation.

| File | Contents |
| --- | --- |
| `policy_sensitivity.csv` | Reclassification of the 113 eligible screening observations under 12 policies, varying one limit at a time. Ratios are recomputed from counters. |
| `profile_anchor_comparison.csv` | Per-anchor comparisons for B0, B3, and B5: one screening and two confirmation observations per cell. The four maximum-load anchors enter the rate geometric mean; the latency anchor enters the latency gate. |
| `validation_allocations.csv` | All 100 validation observations with manifest block/order, retained batch, qualification, pending ratio, source path, and available job identifiers. |
| `qualified_only_ranking.csv` | Twenty configuration/stage rows comparing the implemented all-eligible rate median with a qualified-only median. Qualified count remains the first ranking key; subsequent tie-breaks retain their implemented definitions. A missing qualified median is blank. |
| `review_checks.json` | Seed replay, observed pending-ratio minimum, profile geometric means, and retained profile-selection decisions. |
| `reviewed_broker_profiles/` | Six unchanged reviewed JSON settings. See `../ARTIFACT_GUIDE.md` for canonical hashes and the distinction between tuning B0 and frozen validation B0. |

V1 case paths identify jobs 15303484 for blocks 1-3 and 15303485 for blocks 4-5.
V2 lists jobs 15324660 and 15324661, but their batch association is not retained;
V2 per-case job fields are blank. No job assignment is inferred from list order.

Order replay uses Python `random.Random(20260728).shuffle`, initialized once per
stage and advanced through five blocks, each starting from the original
shortlist. A matching replay does not recover a missing campaign seed log;
the explicit manifest orders remain the retained record.

From the parent evidence folder, check without writing:

```sh
python3 -B scripts/build_referee_supplement.py --check
```

Omit `--check` only to regenerate the derived CSV/JSON outputs intentionally.
The parent `make verify` includes this check and the profile-decision comparisons.
