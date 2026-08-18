#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path
import random


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "phase1"
    / "generated_configs"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "pilots"
    / "batch_runner_10_configs"
)
MANIFEST = OUTPUT_ROOT.parent / "batch_runner_10_manifest.csv"
SEED = 20260820
CONFIG_IDS = (
    "cfg_001",
    "cfg_007",
    "cfg_013",
    "cfg_017",
    "cfg_018",
    "cfg_021",
    "cfg_029",
    "cfg_041",
    "cfg_049",
    "cfg_087",
)
PURPOSES = {
    "cfg_001": "baseline",
    "cfg_007": "producer-rank upper-level OFAT",
    "cfg_013": "consumer-rank upper-level OFAT",
    "cfg_017": "partition-count upper-level OFAT",
    "cfg_018": "small-payload OFAT",
    "cfg_021": "large-payload OFAT",
    "cfg_029": "batching-byte upper-level OFAT",
    "cfg_041": "receiver queue upper-level OFAT",
    "cfg_049": "64-by-64 rank-pair case",
    "cfg_087": "asymmetric seeded mixed case",
}


def main() -> int:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    order = list(CONFIG_IDS)
    random.Random(SEED).shuffle(order)
    rows: list[dict[str, object]] = []
    for position, config_id in enumerate(order, start=1):
        source_path = SOURCE_ROOT / f"{config_id}.json"
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        case_id = f"pulsar-batch10-{config_id.replace('_', '-')}"
        workload = payload["workload"]
        workload.update(
            {
                "warmup_sec": 15,
                "duration_sec": 30,
                "drain_timeout_sec": 60,
            }
        )
        pulsar = payload["backend"]["pulsar"]
        pulsar["topic_name"] = (
            f"persistent://public/default/pulsar-batch10-{config_id.replace('_', '-')}"
        )
        pulsar["subscription_name"] = (
            f"messaging-benchmark-pulsar-batch10-{config_id.replace('_', '-')}"
        )
        payload["campaign"] = {
            "case_id": case_id,
            "campaign_id": "pulsar-batch-runner-acceptance",
            "metadata": {
                "stage": "batch-runner-acceptance",
                "config_id": config_id,
                "source_campaign": "pulsar-phase1-screening",
                "source_config": f"phase1/generated_configs/{config_id}.json",
                "purpose": PURPOSES[config_id],
                "timing_scope": "short runner acceptance; not Phase 1 evidence",
                "seed": SEED,
            },
        }
        output_path = OUTPUT_ROOT / f"{config_id}.json"
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        rows.append(
            {
                "block": 1,
                "order": position,
                "stage": "batch-runner-acceptance",
                "case_id": case_id,
                "config_id": config_id,
                "config_path": output_path.relative_to(PROJECT_ROOT).as_posix(),
                "profile_id": pulsar["profile_id"],
                "purpose": PURPOSES[config_id],
                "seed": SEED,
            }
        )

    with MANIFEST.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[pulsar-batch-acceptance] Wrote {len(rows)} configs to {OUTPUT_ROOT}")
    print(f"[pulsar-batch-acceptance] Manifest: {MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
