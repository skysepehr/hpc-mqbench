#!/usr/bin/env python3
"""Independently verify row evidence, paper claims, and generated artifacts."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import re
import runpy
import subprocess
import sys
from statistics import mean, median, quantiles, stdev


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
AUDIT = DATA / "audit"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def truth(value: str) -> bool:
    return value.strip().lower() == "true"


def num(row: dict[str, str], field: str) -> float:
    return float(row[field])


def integer(row: dict[str, str], field: str) -> int:
    return int(num(row, field))


def close(left: float, right: float, tolerance: float = 1e-6) -> bool:
    return abs(left - right) <= max(tolerance, tolerance * max(abs(left), abs(right)))


def classify(row: dict[str, str]) -> tuple[str, dict[str, float]]:
    attempts = integer(row, "messages_attempted")
    assert attempts > 0
    producer = num(row, "producer_mib_per_sec")
    consumer = num(row, "consumer_mib_per_sec")
    payload = integer(row, "payload_size_bytes")
    assert producer >= 0 and consumer >= 0 and payload > 0
    metrics = {
        "balanced": min(producer, consumer),
        "producer_records": producer * 1_048_576.0 / payload,
        "consumer_records": consumer * 1_048_576.0 / payload,
        "balanced_records": min(producer, consumer) * 1_048_576.0 / payload,
        "pending_ratio": 100.0 * integer(row, "pending_messages_at_flush_start") / attempts,
        "failed_ratio": 100.0 * integer(row, "messages_failed") / attempts,
    }
    correctness = all(
        integer(row, field) == 0
        for field in (
            "records_missing_after_drain",
            "duplicate_records",
            "out_of_order_records",
            "invalid_envelope_records",
            "surplus_records",
        )
    )
    eligible = row["status"] == "completed" and truth(row["latency_valid"]) and correctness
    qualified = (
        eligible
        and metrics["pending_ratio"] <= 5.0
        and num(row, "flush_sec") <= 10.0
        and metrics["failed_ratio"] <= 0.1
    )
    state = "Ineligible" if not eligible else ("Qualified" if qualified else "Overdriven")
    assert close(num(row, "balanced_mib_per_sec"), metrics["balanced"])
    assert close(num(row, "producer_records_per_sec"), metrics["producer_records"])
    assert close(num(row, "consumer_records_per_sec"), metrics["consumer_records"])
    assert close(num(row, "balanced_records_per_sec"), metrics["balanced_records"])
    assert close(num(row, "pending_backlog_percent"), metrics["pending_ratio"])
    assert close(num(row, "failed_send_percent"), metrics["failed_ratio"])
    assert truth(row["eligible"]) == eligible
    assert truth(row["qualified"]) == qualified
    return state, metrics


def verify_phase1(summary: dict) -> None:
    rows = read_csv(AUDIT / "phase1_cases.csv")
    assert len(rows) == 120
    assert len({row["config_id"] for row in rows}) == 120
    groups = Counter(row["manifest_note"].split(":", 1)[0] for row in rows)
    assert groups == {
        "baseline": 1,
        "one-factor": 43,
        "rank-pair": 9,
        "seeded-mixed": 67,
    }

    states = Counter()
    exclusive = Counter()
    marginal = Counter()
    for row in rows:
        state, metrics = classify(row)
        states[state] += 1
        if state == "Overdriven":
            exceeded = []
            if metrics["pending_ratio"] > 5.0:
                exceeded.append("pending_delivery")
                marginal["pending_delivery"] += 1
            if num(row, "flush_sec") > 10.0:
                exceeded.append("flush")
                marginal["flush"] += 1
            if metrics["failed_ratio"] > 0.1:
                exceeded.append("failed_sends")
                marginal["failed_sends"] += 1
            exclusive["+".join(exceeded)] += 1
    assert states == {"Qualified": 99, "Overdriven": 14, "Ineligible": 7}
    assert exclusive == {
        "pending_delivery": 8,
        "pending_delivery+flush": 4,
        "pending_delivery+flush+failed_sends": 2,
    }
    assert marginal == {"pending_delivery": 14, "flush": 6, "failed_sends": 2}

    sensitivity = {}
    eligible_rows = [row for row in rows if classify(row)[0] != "Ineligible"]
    for limit in (1.0, 2.0, 5.0, 10.0):
        count = 0
        for row in eligible_rows:
            _, metrics = classify(row)
            count += (
                metrics["pending_ratio"] <= limit
                and num(row, "flush_sec") <= 10.0
                and metrics["failed_ratio"] <= 0.1
            )
        sensitivity[limit] = count
    assert sensitivity == {1.0: 90, 2.0: 92, 5.0: 99, 10.0: 103}

    phase1 = summary["phase1"]
    assert phase1["rows"] == len(rows)
    assert phase1["unique_configurations"] == 120
    assert phase1["status_counts"] == dict(states)
    assert phase1["recomputation_mismatches"] == {}
    assert phase1["policy_audit"]["overdriven_exclusive_threshold_combinations"] == dict(exclusive)
    assert phase1["policy_audit"]["overdriven_marginal_threshold_counts"] == dict(marginal)


def verify_repeats(summary: dict) -> None:
    expected = {
        "v1_validation": {
            "file": "v1_validation_repeats.csv",
            "qualified_rows": 27,
            "distribution": {"0": 1, "1": 0, "2": 5, "3": 1, "4": 1, "5": 2},
            "median": 2.0,
            "cv": 7.200881007846808,
            "iqr": 312.282396476282,
        },
        "v2_final_validation": {
            "file": "v2_validation_repeats.csv",
            "qualified_rows": 41,
            "distribution": {"0": 1, "1": 0, "2": 0, "3": 1, "4": 2, "5": 6},
            "median": 5.0,
            "cv": 2.3125117971950906,
            "iqr": 69.37492235962623,
        },
    }
    summaries = {row["campaign_id"]: row["audit"] for row in summary["repeated_validation"]}
    for campaign_id, target in expected.items():
        rows = read_csv(AUDIT / target["file"])
        assert len(rows) == 50
        grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
        states = Counter()
        for row in rows:
            state, _ = classify(row)
            states[state] += 1
            grouped[row["config_id"]].append(row)
        assert len(grouped) == 10
        distribution = Counter()
        cvs = []
        iqrs = []
        for config_rows in grouped.values():
            assert sorted(integer(row, "block") for row in config_rows) == [1, 2, 3, 4, 5]
            qualified = sum(classify(row)[0] == "Qualified" for row in config_rows)
            distribution[qualified] += 1
            values = [num(row, "balanced_mib_per_sec") for row in config_rows]
            cvs.append(100.0 * stdev(values) / mean(values))
            quartiles = quantiles(values, n=4, method="inclusive")
            iqrs.append(quartiles[2] - quartiles[0])
        serialized = {str(count): distribution[count] for count in range(6)}
        assert states["Ineligible"] == 0
        assert states["Qualified"] == target["qualified_rows"]
        assert serialized == target["distribution"]
        qualified_counts = [count for count, frequency in distribution.items() for _ in range(frequency)]
        assert median(qualified_counts) == target["median"]
        assert close(median(cvs), target["cv"])
        assert close(median(iqrs), target["iqr"])

        retained = summaries[campaign_id]
        assert retained["complete_five_block_design"] is True
        assert retained["case_rows"] == 50
        assert retained["qualification_count_distribution"] == serialized
        assert retained["row_recomputation_mismatches"] == {}


def verify_validation_blocks() -> None:
    """Recompute the new block-level figure from counters and batch provenance."""
    tex = (ROOT / "hpc_mqbench_paper.tex").read_text(encoding="utf-8")
    figures = re.findall(r"\\begin\{figure\}.*?\\end\{figure\}", tex, flags=re.S)
    figure = next(item for item in figures if r"\label{fig:validation-blocks}" in item)
    plotted = re.findall(r"coordinates\s*\{([^}]+)\}", figure)
    assert len(plotted) == 4
    series = []
    pending_series = []
    for filename in ("v1_validation_repeats.csv", "v2_validation_repeats.csv"):
        rows = read_csv(AUDIT / filename)
        counts = []
        pending = []
        for block in range(1, 6):
            group = [row for row in rows if integer(row, "block") == block]
            assert len(group) == 10 and len({row["config_id"] for row in group}) == 10
            counts.append((block, sum(classify(row)[0] == "Qualified" for row in group)))
            selected = next(row for row in group if row["config_id"] == "cfg_007")
            pending.append((block, round(classify(selected)[1]["pending_ratio"], 3)))
            batches = {re.search(r"/batch_(\d+)", row["source_report"]).group(1)
                       for row in group}
            assert batches == ({"001"} if block <= 3 else {"002"})
        series.append(counts)
        pending_series.append(pending)
    for coordinates, expected in zip(plotted, series + pending_series):
        actual = [(int(x), float(y)) for x, y in re.findall(
            r"\((\d+),([\d.]+)\)", coordinates)]
        assert actual == expected, (actual, expected)
    jobs = read_csv(DATA / "kafka/source/artifacts/slurm_job_ids.csv")
    for stage in ("v1-validation-submit", "v2-final-submit"):
        selected_jobs = [row for row in jobs if row["stage"] == stage]
        assert len({row["job_id"] for row in selected_jobs}) == 2
        assert all(row["status"] == "completed" for row in selected_jobs)


def verify_latency_and_plot_scope() -> None:
    """Check the explained latency grid and that log axes omit no observations."""
    rows = read_csv(AUDIT / "phase1_cases.csv")
    plotted = read_csv(DATA / "kafka/derived/phase1_configuration_outcomes.csv")
    by_id = {row["config_id"]: row for row in rows}
    assert len(plotted) == len(by_id) == 120
    for row in plotted:
        source = by_id[row["config_id"]]
        pending = num(row, "backlog_percent")
        assert 0.01 < pending < 100, "log-axis limit would omit a screening case"
        assert close(pending, num(source, "pending_backlog_percent"))
        assert close(num(row, "balanced_mib_per_sec"), num(source, "balanced_mib_per_sec"))
        assert 0 <= num(row, "balanced_mib_per_sec") < 3575
        if truth(source["eligible"]):
            assert close(num(row, "latency_p99_ms"), num(source, "latency_p99_us") / 1000)
            assert 10 < num(row, "latency_p99_ms") < 70000
    example = by_id["cfg_090"]
    assert num(example, "latency_p99_us") == 19500
    assert integer(example, "latency_sample_count") == 360455

    # The compact artifact has no raw cfg_090 histogram. These probes verify the
    # inspected implementation, not a reconstruction of historical samples.
    implementation = ROOT.parents[1] / "src/benchmark/record_envelope.py"
    if implementation.is_file():
        namespace = runpy.run_path(str(implementation))
        histogram_type = namespace["LatencyHistogram"]
        expected = tuple(
            list(range(1, 1001)) + list(range(1010, 10001, 10))
            + list(range(10100, 100001, 100)) + list(range(101000, 1000001, 1000))
            + list(range(1010000, 10000001, 10000))
            + list(range(10100000, 60000001, 100000))
        )
        assert namespace["default_latency_bucket_upper_bounds_us"]() == expected
        assert expected[expected.index(19500) - 1] == 19400
        low = histogram_type()
        high = histogram_type()
        for _ in range(98):
            assert low.record_ns(1_000_000)
        assert high.record_ns(19_450_000)
        assert high.record_ns(90_000_000_000)
        merged = histogram_type.merged([low.to_dict(), high.to_dict()])
        assert merged.count == 100 and merged.overflow_count == 1
        assert merged.percentile_ns(99) == 19_500_000
        assert merged.percentile_ns(99.9) == 90_000_000_000
        assert not merged.record_ns(-1)
        assert merged.count == 100 and merged.negative_count == 1


def verify_contract(summary: dict) -> None:
    contract = json.loads((DATA / "common_measurement_contract.json").read_text(encoding="utf-8"))
    assert contract["timing"] == {
        "warmup_sec": 15,
        "measurement_sec": 30,
        "drain_timeout_sec": 60,
    }
    assert contract["latency"]["deterministic_sample_every"] == 10
    assert contract["latency"]["record_identity_on_every_record"] is True
    assert contract["producer_delivery"]["backlog_denominator"] == "measurement-period send attempts"
    assert contract["producer_delivery"]["backlog_event"] == "pending producer delivery callbacks at flush start"
    assert contract["consumer_drain"]["poll_during_producer_flush"] is True
    assert contract["qualification_thresholds"] == {
        "backlog_percent_max": 5.0,
        "flush_duration_sec_max": 10.0,
        "failed_send_percent_max": 0.1,
    }
    assert summary["campaign_git_commit"] is None
    assert summary["public_source_revision"] == "bca9a4d18892f5ebff80d474da6ba57a0da749d2"
    phase1 = summary["experimental_setup"]["phase1"]
    assert phase1["virtual_devices_per_producer_rank"] == 50_000
    assert phase1["producer_logical_source_min"] == 800_000
    assert phase1["producer_logical_source_max"] == 3_200_000
    assert phase1["payload_mode"] == "fixed_size"
    assert phase1["send_pattern"] == "steady"
    assert phase1["target_records_per_sec"] is None


def verify_mechanism_test_sources(summary: dict) -> None:
    inventory = {row["mechanism"]: row["source"] for row in summary["mechanism_tests"]}
    assert set(inventory) == {
        "Qualification boundaries",
        "Envelope and ordering",
        "Latency validity",
        "Backend health",
        "Resume and integrity",
    }
    manifest = json.loads((DATA / "source_manifest.json").read_text(encoding="utf-8"))
    manifest_sources = {
        row["source"]: row
        for row in manifest["sources"]
        if row.get("source") in set(inventory.values())
    }
    assert set(manifest_sources) == set(inventory.values())
    for row in manifest_sources.values():
        assert row["bytes"] > 0
        assert re.fullmatch(r"[0-9a-f]{64}", row["sha256"])

    # The checked-in manifest keeps a standalone paper build independent of the
    # parent repository.  When the parent source tree is present, additionally
    # verify both the recorded hashes and the concrete test constructs cited by
    # the implementation-audit table.
    repository = ROOT.parents[1]
    smoke_path = repository / "tests/smoke_test.py"
    workflow_path = repository / "tests/test_reproducible_workflow.py"
    if smoke_path.exists() and workflow_path.exists():
        for relative, row in manifest_sources.items():
            payload = (repository / relative).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == row["sha256"]
        smoke = smoke_path.read_text(encoding="utf-8")
        workflow = workflow_path.read_text(encoding="utf-8")
        for marker in (
            "test_consumer_offset_checks_exclude_warmup_records",
            "test_latency_histogram_merge_and_negative_rejection",
            "pending_messages_at_flush_start",
            "test_backend_health_failure_is_ineligible_and_preserved_in_reports",
        ):
            assert marker in smoke
        for marker in ("checksum drift", "missing artifact"):
            assert marker in workflow


def verify_tex() -> None:
    tex_files = list(ROOT.rglob("*.tex"))
    assert tex_files == [ROOT / "hpc_mqbench_paper.tex"], "paper must have one LaTeX source"
    tex = tex_files[0].read_text(encoding="utf-8")
    assert not re.search(r"\\(?:input|include)\{", tex), "external LaTeX include"
    labels = re.findall(r"\\label\{([^}]+)\}", tex)
    refs = re.findall(r"\\(?:ref|eqref)\{([^}]+)\}", tex)
    assert len(labels) == len(set(labels)), "duplicate LaTeX labels"
    assert not set(refs) - set(labels), "undefined LaTeX labels"
    citations = set()
    for group in re.findall(r"\\cite(?:p|t)?\{([^}]+)\}", tex):
        citations.update(group.split(","))
    bib = (ROOT / "references.bib").read_text()
    bibkey_list = re.findall(r"@\w+\{([^,]+),", bib)
    bibkeys = set(bibkey_list)
    assert len(bibkey_list) == len(bibkeys), "duplicate bibliography keys"
    assert citations == bibkeys, "undefined or unused bibliography entries"
    for section in ("Benchmark Design", "Measurement and Qualification",
                    "Experimental Setup",
                    "Kafka Performance Evaluation", "Conclusion", "Code and Evidence Availability"):
        assert f"\\section{{{section}}}" in tex
    pdf = ROOT / "hpc_mqbench_paper.pdf"
    pdf_text = subprocess.check_output(["pdftotext", str(pdf), "-"], text=True)
    metadata = subprocess.check_output(["pdfinfo", str(pdf)], text=True)
    excluded_name = "pul" + "sar"
    assert excluded_name not in (tex + bib + pdf_text + metadata).lower()
    assert "??" not in pdf_text
    log_path = ROOT / "build/hpc_mqbench_paper.log"
    if log_path.is_file():
        log = log_path.read_text()
        assert "Overfull" not in log
        assert "undefined" not in log.lower()
    else:
        print("Build log absent: checked supplied source/PDF; compile diagnostics require make pdf")


def inline_table(label: str) -> str:
    tex = (ROOT / "hpc_mqbench_paper.tex").read_text(encoding="utf-8")
    tables = re.findall(r"\\begin\{table\}.*?\\end\{table\}", tex, flags=re.S)
    matches = [table for table in tables if f"\\label{{{label}}}" in table]
    assert len(matches) == 1, f"missing or duplicate table: {label}"
    return matches[0]


def verify_kafka_evaluation() -> None:
    """Check every printed numeric result-table cell against retained case rows."""
    tex = inline_table("tab:kafka-final")
    screening = {r["config_id"]: r for r in read_csv(AUDIT / "phase1_cases.csv")}
    repeats = defaultdict(list)
    for row in read_csv(AUDIT / "v2_validation_repeats.csv"):
        repeats[row["config_id"]].append(row)
    table_rows = [line for line in tex.splitlines()
                  if line.startswith(r"\texttt{cfg\_") and " & " in line]
    assert len(table_rows) == 10
    final_ids = []
    for line in table_rows:
        cells = [cell.strip().rstrip("\\").strip() for cell in line.split(" & ")]
        config = "cfg_" + re.search(r"cfg\\_(\d+)", cells[0]).group(1)
        rows = repeats[config]
        assert len(rows) == 5
        final_ids.append(config)
        def med(field): return median(num(r, field) for r in rows)
        vals = [num(r, "balanced_mib_per_sec") for r in rows]
        quartiles = quantiles(vals, n=4, method="inclusive")
        expected = [
            f'{sum(classify(r)[0] == "Qualified" for r in rows)}/5',
            f'{median(vals):,.1f}',
            f'{quartiles[2] - quartiles[0]:,.1f}',
            f'{med("latency_p99_us") / 1000:,.0f}',
            f'{med("pending_backlog_percent"):.3f}',
            f'{med("flush_sec"):.3f}',
        ]
        assert cells[1:] == expected, (config, cells[1:], expected)
    summary_path = ROOT / "data/kafka/source/artifacts/v2/validation_summary.csv"
    ranking = sorted(read_csv(summary_path), key=lambda r: int(r["validation_rank"]))
    assert final_ids == [r["original_config_id"] for r in ranking]
    assert final_ids[0] == "cfg_007"
    winner = screening["cfg_007"]
    baseline = screening["cfg_001"]
    fields = ["producer_ranks", "consumer_ranks", "partitions", "batch_size",
              "linger_ms", "payload_size_bytes", "producer_queue_messages",
              "producer_queue_kbytes", "consumer_fetch_min_bytes",
              "consumer_fetch_wait_max_ms", "consumer_fetch_message_max_bytes"]
    assert [f for f in fields if winner[f] != baseline[f]] == ["producer_ranks"]
    assert integer(winner, "producer_ranks") == 64
    raw_peak = max(screening.values(), key=lambda r: num(r, "balanced_mib_per_sec"))
    assert raw_peak["config_id"] == "cfg_101" and classify(raw_peak)[0] == "Overdriven"
    qualified_peak = max((r for r in screening.values() if classify(r)[0] == "Qualified"),
                         key=lambda r: num(r, "balanced_mib_per_sec"))
    assert qualified_peak["config_id"] == "cfg_107"
    profiles = read_csv(ROOT / "data/kafka/derived/profile_decision.csv")
    assert [r["profile_id"] for r in profiles if truth(r["selected"])] == ["B0"]
    decisions_path = ROOT / "data/kafka/source/analysis/v2/profile-confirmation/kafka_broker_tuning_v2_decisions.json"
    decisions = json.loads(decisions_path.read_text())["profile_selection"]
    for row in decisions["profiles"]:
        assert row["passes_all_rules"] is False
        if row["profile_id"] in ("B3", "B5"):
            assert row["required_measurements_valid"] is True
            assert row["geometric_mean_median_throughput_ratio_to_B0"] < 1.03


def verify_backlog_and_resources() -> None:
    """Check the new sensitivity table, resource table and reported ranges."""
    rows = read_csv(AUDIT / "phase1_cases.csv")
    qualified = [r for r in rows if classify(r)[0] == "Qualified"]
    overloaded = [r for r in rows if classify(r)[0] == "Overdriven"]
    assert f'{median(num(r,"pending_backlog_percent") for r in qualified):.3f}' == "0.320"
    assert f'{median(num(r,"latency_p99_us") for r in qualified)/1000:.0f}' == "259"
    assert f'{median(num(r,"pending_backlog_percent") for r in overloaded):.3f}' == "20.755"
    assert median(num(r,"latency_p99_us") for r in overloaded)/1000 == 19000
    assert max(num(r,"messages_failed") for r in qualified) == 0
    assert f'{max(num(r,"flush_sec") for r in qualified):.3f}' == "2.929"
    assert f'{max(num(r,"flush_sec") for r in overloaded):.3f}' == "25.126"
    assert f'{min(num(r,"flush_sec") for r in overloaded):.3f}' == "2.836"
    eligible = qualified + overloaded
    tex = inline_table("tab:kafka-backlog-sensitivity")
    for limit in (2, 5, 10):
        q = [r for r in eligible if num(r,"pending_backlog_percent") <= limit
             and num(r,"flush_sec") <= 10 and num(r,"failed_send_percent") <= .1]
        best = max(q, key=lambda r: num(r,"balanced_mib_per_sec"))
        cfg = best["config_id"].replace("_",r"\_")
        expected = (f'{limit} & {len(q)} & {len(eligible)-len(q)} & '
                    f'\\texttt{{{cfg}}} & {num(best,"balanced_mib_per_sec"):,.1f}')
        assert expected in tex, expected
    raw = read_csv(ROOT / "data/kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv")
    final = [r for r in raw if r["stage"] == "final_validation"]
    assert len(final) == 50
    network_table = inline_table("tab:network-probes")
    for blocks, count, producer_gbit, consumer_gbit in [
            ((1, 2, 3), 30, "10.134", "9.968"),
            ((4, 5), 20, "10.492", "10.597")]:
        batch_rows = [r for r in final if integer(r, "block") in blocks]
        assert len(batch_rows) == count
        pairs = {
            (num(r, "producer_to_broker_iperf_MBps"),
             num(r, "broker_to_consumer_iperf_MBps"))
            for r in batch_rows
        }
        assert len(pairs) == 1, "batch-shared directional probe values changed"
        producer_mb, consumer_mb = pairs.pop()
        assert f"{producer_mb * 8 / 1000:.3f}" == producer_gbit
        assert f"{consumer_mb * 8 / 1000:.3f}" == consumer_gbit
        expected = (f"{blocks[0]}--{blocks[-1]} & {count} & "
                    f"{producer_gbit} & {consumer_gbit}")
        assert expected in network_table, expected
    assert {integer(r,"sample_count") for r in final} == {29,30}
    for row in final:
        for field in ("kafka_request_queue_size_mean", "kafka_response_queue_size_mean",
                      "kafka_request_queue_size_p95", "kafka_response_queue_size_p95"):
            assert num(row,field) == 0
        assert row["missing_required_jmx_metrics"] in ("", "[]")
    for field, factor, low, high, precision in [
            ("kafka_request_handler_idle_ratio_mean",100,"92.3","96.0",1),
            ("jvm_heap_used_gb_mean",1,"2.43","3.14",2),
            ("jvm_gc_time_rate_ms_per_sec_mean",1,"3.22","4.04",2)]:
        vals = [num(r,field)*factor for r in final]
        assert f'{min(vals):.{precision}f}' == low
        assert f'{max(vals):.{precision}f}' == high
    assert f'{max(num(r,"jvm_heap_used_gb_p95") for r in final):.2f}' == "5.26"
    assert f'{max(num(r,"tmpfs_used_percent_max") for r in final):.2f}' == "56.46"
    table = inline_table("tab:kafka-resources")
    printed = [l for l in table.splitlines() if l.startswith(r"\texttt{cfg\_")]
    assert len(printed) == 6
    for line in printed:
        cells = [c.strip().rstrip("\\").strip() for c in line.split(" & ")]
        config = "cfg_" + re.search(r"cfg\\_(\d+)", cells[0]).group(1)
        cases = [r for r in final if r["anchor_id"] == config]
        assert len(cases) == 5
        def med(field): return median(num(r,field) for r in cases)
        expected = [
            f'{med("cpu_allocated_percent_mean"):.2f}',
            f'{med("kafka_jmx_bytes_in_counter_rate_mean")/1048576:,.0f} / '
            f'{med("kafka_jmx_bytes_out_counter_rate_mean")/1048576:,.0f}',
            f'{100*med("kafka_request_handler_idle_ratio_mean"):.1f}',
            f'{med("jvm_heap_used_gb_mean"):.2f}',
            f'{med("jvm_gc_time_rate_ms_per_sec_mean"):.2f}',
            f'{max(num(r,"tmpfs_used_percent_max") for r in cases):.1f}',
        ]
        assert cells[1:] == expected, (config,cells[1:],expected)


def verify_publication_disclosures() -> None:
    """Recompute added pilot and campaign-history claims from retained evidence."""
    source = DATA / "kafka/source"
    cases = read_csv(source / "analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv")
    counts = Counter(row["stage"] for row in cases)
    assert counts == {"instrumentation_pilot": 6, "rate_calibration": 3,
                      "profile_screening": 30, "profile_confirmation": 30,
                      "final_validation": 50}
    pilot = [row for row in cases if row["stage"] == "instrumentation_pilot"]
    overhead = []
    for block in ("1", "2", "3"):
        pair = [row for row in pilot if row["block"] == block]
        assert len(pair) == 2 and all(truth(row["eligible"]) for row in pair)
        off = next(row for row in pair if not truth(row["latency_enabled"]))
        on = next(row for row in pair if truth(row["latency_enabled"]))
        assert integer(on, "latency_sample_every") == 10
        for row in pair:
            assert (integer(row, "producer_ranks"), integer(row, "consumer_ranks"),
                    integer(row, "payload_size_bytes")) == (16, 64, 2048)
        overhead.append((num(off, "balanced_mibps") - num(on, "balanced_mibps")) /
                        num(off, "balanced_mibps"))
    decision = json.loads((source / "analysis/v2/instrumentation-pilot/kafka_broker_tuning_v2_decisions.json").read_text())["instrumentation"]
    assert close(100 * median(overhead), decision["throughput_overhead_percent"])
    assert f"{100 * median(overhead):.2f}" == "0.41"
    selection = json.loads((source / "artifacts/v1-phase1-verify_result_selection.json").read_text())
    selected = selection["selected_reports"]
    if isinstance(selected, dict):
        selected = list(selected.values())
    assert len(selected) == 120
    assert Counter(row["selected_repair_attempt"] for row in selected) == {0: 119, 3: 1}
    repair = next(row for row in selected if row["selected_repair_attempt"] == 3)
    assert repair["case_id"] == "cfg_103" and repair["eligible"] is False
    executed_repairs = [row for row in selection["repair_events"] if row["status"] == "completed"]
    assert len(executed_repairs) == 2
    assert all(row["case_ids"] == ["cfg_103"] for row in executed_repairs)
    original_screening = 120
    validation_runs = 50 + counts["final_validation"]
    tuning_runs = sum(count for stage, count in counts.items() if stage != "final_validation")
    assert validation_runs == 100 and tuning_runs == 69
    assert original_screening + len(executed_repairs) + validation_runs + tuning_runs == 291
    # The selected 120-row screen contains the last repair. Two executions are
    # outside that dataset: the original cfg_103 run and its earlier repair.
    assert original_screening + len(executed_repairs) - len(selected) == 2
    state = json.loads((source / "artifacts/workflow_state.json").read_text())
    assert any("six clock-only exclusions and one non-clock exclusion" in row["reason"]
               for row in state["implementation_updates"])
    migration = state["input_migrations"][0]
    assert migration["semantic_match_count"] == 120
    assert migration["raw_results_modified"] is False


def verify_referee_supplement() -> None:
    """Recompute supplemental analyses and compare with campaign decisions/code."""
    subprocess.run([sys.executable, "-B", str(ROOT / "scripts/build_referee_supplement.py"), "--check"], check=True)
    rows = read_csv(ROOT / "supplement/policy_sensitivity.csv")
    assert len(rows) == 12
    for row in rows:
        limit = row["varied_limit"]
        if limit == "pending_percent":
            expected = {2: (92, "cfg_096"), 5: (99, "cfg_107"), 10: (103, "cfg_101")}[int(row["pending_limit_percent"])]
        elif limit == "flush_seconds" and num(row, "flush_limit_seconds") == 1:
            expected = (92, "cfg_096")
        else:
            expected = (99, "cfg_107")
        assert (integer(row, "qualified"), row["highest_qualified_config"]) == expected
    policy_table = inline_table("tab:kafka-backlog-sensitivity")
    groups = [("flush_seconds", [1], "1"),
              ("flush_seconds", [3, 5, 10, 30], "3, 5, 10, 30"),
              ("failed_send_percent", [0, .01, .1, 1], "0, 0.01, 0.1, 1")]
    for dimension, limits, displayed in groups:
        field = "flush_limit_seconds" if dimension == "flush_seconds" else "failed_send_limit_percent"
        selected = [r for r in rows if r["varied_limit"] == dimension and num(r, field) in limits]
        assert len(selected) == len(limits)
        for row in selected:
            cfg = row["highest_qualified_config"].replace("_", r"\_")
            expected_line = (f'{displayed} & {row["qualified"]} & {row["overdriven"]} & '
                             f'\\texttt{{{cfg}}} & {num(row, "highest_balanced_endpoint_mibps"):,.1f}')
            assert expected_line in policy_table
    ranking = read_csv(ROOT / "supplement/qualified_only_ranking.csv")
    assert len(ranking) == 20
    changes = {(r["stage"], r["config_id"]): (integer(r, "implemented_rank"), integer(r, "qualified_only_rank"))
               for r in ranking if r["implemented_rank"] != r["qualified_only_rank"]}
    assert changes == {("V1", "cfg_007"): (6, 8), ("V1", "cfg_049"): (7, 9),
                       ("V1", "cfg_107"): (8, 7), ("V1", "cfg_061"): (9, 6)}
    for row in ranking:
        if integer(row, "qualified_count") == 0:
            assert row["qualified_median_mibps"] == ""
        if integer(row, "implemented_rank") == 1:
            assert integer(row, "qualified_only_rank") == 1
    review = json.loads((ROOT / "supplement/review_checks.json").read_text())
    profiles = read_csv(ROOT / "supplement/profile_anchor_comparison.csv")
    retained = review["retained_profile_selection"]
    profile_cases = read_csv(DATA / "kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv")
    profile_table = inline_table("tab:broker-profiles")
    for path in sorted((ROOT / "supplement/reviewed_broker_profiles").glob("B*.json")):
        payload = json.loads(path.read_text())
        declared = payload.pop("profile_sha256")
        calculated = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        assert calculated == declared
        assert any(r["broker_profile_id"] == path.stem and r["broker_profile_sha256"] == declared for r in profile_cases)
        settings = payload["settings"]
        heap_gib = int(re.search(r"-Xmx(\d+)G", settings["heap_opts"], re.I)[1])
        segment = settings["log_segment_bytes"]
        segment_tex = r"$2^{30}$" if segment == 2**30 else r"$2^{31}-1$"
        assert segment in (2**30, 2**31-1)
        expected_line = f'{path.stem} & {settings["num_network_threads"]} & {settings["num_io_threads"]} & {heap_gib} & {segment_tex}'
        assert expected_line in profile_table
    for decision in retained["profiles"]:
        profile = decision["profile_id"]
        if profile not in ("B0", "B3", "B5"):
            continue
        assert close(review["profile_rate_geometric_means"][profile], decision["geometric_mean_median_throughput_ratio_to_B0"])
        group = {row["anchor_id"]: row for row in profiles if row["broker_profile"] == profile}
        assert close(num(group["latency_anchor"], "p99_ratio_to_B0"), decision["latency_p99_ratio_to_B0"])
        for anchor, count in decision["qualified_counts"].items():
            assert integer(group[anchor], "qualified") == count
    tex = (ROOT / "hpc_mqbench_paper.tex").read_text()
    assert "balanced throughput" not in tex.lower()
    assert "20260728" in tex and "0.0157" in tex
    assert r"$2^{30}$" in tex and r"$2^{31}-1$" in tex
    assert "producer-rank candidate" in tex
    source = ROOT.parents[1] / "scripts/analyze_broker_tuning_v2.py"
    if source.is_file():
        code = runpy.run_path(str(source))
        path = DATA / "kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.json"
        cases = json.loads(path.read_text())
        actual = code["final_profile_selection"](cases)
        assert actual == retained, "current selection differs from the retained profile decision"
        changed = json.loads(path.read_text())
        for row in changed:
            for key, value in row.items():
                if "iperf" in key and isinstance(value, (float, int)) and not isinstance(value, bool):
                    row[key] = value * 100
        assert code["final_profile_selection"](changed) == actual
        for value in (None, "invalid", 0, -1, "nan", "inf", 0.001, 1, 10000):
            parsed = code["extract_iperf_capacity"]({"system_inventory": {"network_tests": [
                {"status": "completed", "label": "producer_to_broker", "megabytes_per_second": value}
            ]}})["producer_to_broker_iperf_MBps"]
            assert parsed == (float(value) if value in (0.001, 1, 10000) else None)


def write_checksums() -> None:
    suffixes = {".tex", ".bib", ".py", ".md", ".json", ".csv", ".pdf", ".png"}
    excluded = {
        "SHA256SUMS",
        "hpc_mqbench_paper.aux",
        "hpc_mqbench_paper.bbl",
        "hpc_mqbench_paper.blg",
        "hpc_mqbench_paper.log",
        "hpc_mqbench_paper.out",
        "hpc_mqbench_paper.toc",
        "hpc_mqbench_paper.lof",
        "hpc_mqbench_paper.lot",
    }
    records = []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or any(part.startswith(".") or part in ("build", "__pycache__") for part in path.relative_to(ROOT).parts) or path.name in excluded or (path.suffix not in suffixes and path.name != "Makefile"):
            continue
        records.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(ROOT)}")
    (ROOT / "SHA256SUMS").write_text("\n".join(records) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-checksums", action="store_true",
                        help="Refresh the local manuscript checksum list after successful checks")
    args = parser.parse_args()
    summary = json.loads((DATA / "evidence_summary.json").read_text(encoding="utf-8"))
    verify_contract(summary)
    verify_phase1(summary)
    verify_repeats(summary)
    verify_validation_blocks()
    verify_latency_and_plot_scope()
    verify_mechanism_test_sources(summary)
    verify_tex()
    verify_kafka_evaluation()
    verify_backlog_and_resources()
    verify_publication_disclosures()
    verify_referee_supplement()

    assert (ROOT / "figures/kafka_operating_limits.pdf").is_file()
    for label in ("tab:parameter-space", "tab:kafka-backlog-sensitivity",
                  "tab:kafka-final", "tab:kafka-resources"):
        assert "\t" not in inline_table(label)

    pdf = ROOT / "hpc_mqbench_paper.pdf"
    assert pdf.is_file() and pdf.stat().st_size > 10_000
    if args.write_checksums:
        write_checksums()
    print(
        "Paper verification passed: row-level formulas, states, repeated blocks and figure, "
        "latency estimator and plot scope, mechanism-test inventory, Kafka result/resource tables, "
        "pilot and repair disclosures, referee supplement and profile/probe checks, LaTeX/PDF scope, and manuscript consistency"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
