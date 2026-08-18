#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESULTS_ROOT = PROJECT_ROOT / "results" / "sweeps" / "simultaneous_budgeted"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create a human-facing HTML analysis for a simultaneous budgeted sweep."
    )
    parser.add_argument("--run-dir", help="Specific sweep run directory to analyze.")
    parser.add_argument(
        "--results-root",
        default=str(DEFAULT_RESULTS_ROOT),
        help="Root containing run_*/ directories.",
    )
    args = parser.parse_args(argv)

    run_dir = Path(args.run_dir) if args.run_dir else _latest_run_dir(Path(args.results_root))
    rows = load_summary_rows(run_dir)
    html_text = build_html(run_dir, rows)
    output_path = run_dir / "sweep_summary.html"
    output_path.write_text(html_text, encoding="utf-8")
    print(f"[sweep-analyze] run: {_project_relative(run_dir)}")
    print(f"[sweep-analyze] cases: {len(rows)}")
    print(f"[sweep-analyze] wrote: {_project_relative(output_path)}")
    return 0


def load_summary_rows(run_dir: Path) -> list[dict[str, Any]]:
    csv_path = run_dir / "sweep_summary.csv"
    if not csv_path.is_file():
        raise FileNotFoundError(f"Sweep summary CSV not found: {csv_path}")
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = [dict(row) for row in csv.DictReader(handle)]
    if rows and "primary_rank" in rows[0]:
        rows.sort(key=lambda row: (_float(row.get("primary_rank")), str(row.get("config_id", ""))))
    else:
        rows.sort(
            key=lambda row: (
                _status_order(row),
                -_float(row.get("balanced_app_MBps")),
                _float(row.get("pending_backlog_percent")),
                _float(row.get("flush_sec")),
                _float(row.get("failed_send_percent")),
                str(row.get("config_id", "")),
            )
        )
    return rows


def build_html(run_dir: Path, rows: list[dict[str, Any]]) -> str:
    completed = [row for row in rows if row.get("status") == "completed"]
    qualified = [row for row in completed if _is_qualified(row)]
    best = qualified[0] if qualified else (completed[0] if completed else (rows[0] if rows else {}))
    top_raw = max(completed, key=lambda row: _float(row.get("balanced_app_MBps")), default={})
    top_rows = completed[:15]
    cards = [
        ("Configs", str(len(rows)), "all rows in sweep summary"),
        ("Completed", str(len(completed)), "cases with final_report.json"),
        (
            "Highest qualified",
            f"{_float(best.get('balanced_app_MBps')):.1f} MiB/s" if best else "n/a",
            str(best.get("config_id", "n/a")),
        ),
        (
            "Highest raw upper bound",
            f"{_float(top_raw.get('balanced_app_MBps')):.1f} MiB/s" if top_raw else "n/a",
            str(top_raw.get("config_id", "n/a")),
        ),
    ]
    config_overview = _config_overview(best) if best else ""
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>Kafka Simultaneous Budgeted Sweep Summary</title>",
            "<style>",
            _css(),
            "</style>",
            "</head>",
            "<body>",
            "<main>",
            '<section class="hero">',
            "<p>Kafka MPI Benchmark</p>",
            "<h1>Simultaneous Budgeted Sweep Summary</h1>",
            f'<p class="note">Run directory: <code>{_escape(_project_relative(run_dir))}</code>. Ranking is qualification first, then balanced throughput, lower backlog, lower flush duration, lower failed-send percentage, and configuration ID. Existing <code>*_MBps</code> field names are retained for compatibility, but values use 1,048,576 bytes and are MiB/s.</p>',
            '<div class="cards">' + "".join(_card(*card) for card in cards) + "</div>",
            "</section>",
            '<section id="best-config">',
            "<h2>Best Config At A Glance</h2>",
            config_overview or '<p class="note">No completed config was found.</p>',
            "</section>",
            '<section id="definitions">',
            "<h2>Configuration And Result Definitions</h2>",
            '<p class="note">Short definitions for the sweep knobs and result columns shown in the tables below. The queue and fetch settings come from <code>extra.kafka_producer_config</code> and <code>extra.kafka_consumer_config</code> in each generated JSON config.</p>',
            _definitions_html(),
            "</section>",
            '<section id="plots">',
            "<h2>Ranking Plots</h2>",
            '<div class="plot-grid">',
            _top_bar_svg(top_rows),
            _producer_consumer_scatter_svg(completed),
            "</div>",
            "</section>",
            '<section id="top-ranked">',
            "<h2>Top Primary-Ranked Configs</h2>",
            '<div class="table-wrap">' + _table(top_rows, compact=True) + "</div>",
            "</section>",
            '<section id="all-cases">',
            "<h2>All Sweep Cases</h2>",
            '<p class="note">Every row links to the per-case directory when available. Light mode intentionally skips heavy per-case HTML; use the linked <code>final_report.json</code> and logs for details.</p>',
            '<div class="table-wrap">' + _table(rows, compact=False) + "</div>",
            "</section>",
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def _config_overview(row: dict[str, Any]) -> str:
    keys = [
        ("Config", "config_id"),
        ("Scenario", "scenario"),
        ("Topic", "topic"),
        ("Producer ranks", "producer_ranks"),
        ("Consumer ranks", "consumer_ranks"),
        ("Partitions", "partitions"),
        ("Batch size", "batch_size"),
        ("linger.ms", "linger_ms"),
        ("Payload bytes", "payload_size_bytes"),
        ("Producer queue messages", "producer_queue_messages"),
        ("Producer queue KiB", "producer_queue_kbytes"),
        ("Consumer fetch min bytes", "consumer_fetch_min_bytes"),
        ("Consumer fetch wait ms", "consumer_fetch_wait_max_ms"),
        ("Consumer fetch max bytes", "consumer_fetch_message_max_bytes"),
        ("Balanced app MiB/s", "balanced_app_MBps"),
        ("Balanced records/s", "balanced_records_per_sec"),
        ("Producer delivered MiB/s", "producer_delivered_MBps"),
        ("Consumer received MiB/s", "consumer_received_MBps"),
        ("Broker ingress MiB/s", "broker_ingress_MBps"),
        ("Broker egress MiB/s", "broker_egress_MBps"),
        ("End-to-end p99 us", "latency_p99_us"),
        ("Backlog denominator", "backlog_denominator"),
        ("Qualification", "qualification_status"),
        ("Qualification reason", "qualification_reason"),
        ("Primary rank", "primary_rank"),
        ("Verdict", "throughput_verdict"),
        ("Bottleneck", "primary_bottleneck_conclusion"),
    ]
    body = "".join(
        f"<tr><th>{_escape(label)}</th><td>{_format_cell(row.get(key), key)}</td></tr>"
        for label, key in keys
    )
    return f"<table>{body}</table>"


def _table(rows: list[dict[str, Any]], compact: bool) -> str:
    fields = [
        ("Config", "config_id"),
        ("Status", "status"),
        ("Primary rank", "primary_rank"),
        ("Qualification", "qualification_status"),
        ("Reason", "qualification_reason"),
        ("Balanced MiB/s", "balanced_app_MBps"),
        ("Balanced records/s", "balanced_records_per_sec"),
        ("Producer MiB/s", "producer_delivered_MBps"),
        ("Consumer MiB/s", "consumer_received_MBps"),
        ("JMX ingress MiB/s", "broker_ingress_MBps"),
        ("JMX egress MiB/s", "broker_egress_MBps"),
        ("E2E p99 us", "latency_p99_us"),
        ("Failed %", "failed_send_percent"),
        ("Flush sec", "flush_sec"),
        ("Pending %", "pending_backlog_percent"),
        ("Ranks P/C", "ranks"),
        ("Partitions", "partitions"),
        ("Batch", "batch_size"),
        ("linger", "linger_ms"),
        ("Payload", "payload_size_bytes"),
        ("P queue msgs", "producer_queue_messages"),
        ("P queue KiB", "producer_queue_kbytes"),
        ("C fetch min", "consumer_fetch_min_bytes"),
        ("C fetch wait", "consumer_fetch_wait_max_ms"),
        ("C fetch max", "consumer_fetch_message_max_bytes"),
    ]
    if not compact:
        fields.extend(
            [
                ("Broker CPU peak", "broker_cpu_peak_percent"),
                ("Broker RAM peak GB", "broker_ram_peak_GB"),
                ("Broker RX MB/s", "broker_network_rx_peak_MBps"),
                ("Broker TX MB/s", "broker_network_tx_peak_MBps"),
                ("Verdict", "throughput_verdict"),
                ("Conclusion", "primary_bottleneck_conclusion"),
                ("Case dir", "case_dir"),
            ]
        )
    header = "".join(f"<th>{_escape(label)}</th>" for label, _ in fields)
    body_rows = []
    for row in rows:
        cells = []
        for _, key in fields:
            value = f"{row.get('producer_ranks', '')}/{row.get('consumer_ranks', '')}" if key == "ranks" else row.get(key, "")
            cells.append(f"<td>{_format_cell(value, key, row)}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return "<table><thead><tr>" + header + "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table>"


def _definitions_html() -> str:
    groups = [
        (
            "Configuration",
            [
                ("Producer ranks", "MPI producer workers sending records to Kafka."),
                ("Consumer ranks", "MPI consumer workers reading records from Kafka."),
                ("Partitions", "Kafka topic partitions available for parallel produce and consume work."),
                ("Batch size", "Producer batch.size in bytes; larger batches can improve throughput at the cost of memory and latency."),
                ("linger.ms", "Maximum producer wait before sending a partial batch; higher values can improve batching."),
                ("Payload bytes", "Application payload size per Kafka record."),
                ("Producer queue messages", "librdkafka queue.buffering.max.messages for producer-side buffering."),
                ("Producer queue KiB", "librdkafka queue.buffering.max.kbytes producer buffer limit."),
                ("Consumer fetch min bytes", "Minimum bytes Kafka should collect before answering a consumer fetch."),
                ("Consumer fetch wait ms", "Maximum broker wait for fetch.min.bytes before returning data."),
                ("Consumer fetch max bytes", "Maximum bytes returned per partition fetch response."),
            ],
        ),
        (
            "Results",
            [
                ("Producer MiB/s", "Application payload throughput delivered by producer ranks."),
                ("Consumer MiB/s", "Application payload throughput received by consumer ranks."),
                ("Balanced MiB/s", "min(producer delivered MiB/s, consumer received MiB/s); the simultaneous end-to-end rate."),
                ("Qualification", "Qualified when backlog <= 5%, flush <= 10 s, and failed sends <= 0.1%."),
                ("Primary rank", "Recommendation rank: qualified rows first, then higher balanced MiB/s, then lower backlog, flush, and failures."),
                ("JMX ingress MiB/s", "Broker-side Kafka bytes-in rate converted with 1,048,576 bytes per MiB."),
                ("JMX egress MiB/s", "Broker-side Kafka bytes-out rate converted with 1,048,576 bytes per MiB."),
                ("Failed %", "Share of producer send attempts that failed."),
                ("Flush sec", "Producer flush duration at shutdown; long values indicate backlog pressure."),
                ("Pending %", "Producer delivery callbacks still pending when flush began, divided by measurement-period send attempts for the reusable contract."),
            ],
        ),
    ]
    cards = []
    for title, items in groups:
        rows = "".join(
            f"<tr><th>{_escape(name)}</th><td>{_escape(text)}</td></tr>"
            for name, text in items
        )
        cards.append(
            '<article class="definition-card">'
            f"<h3>{_escape(title)}</h3>"
            f"<table>{rows}</table>"
            "</article>"
        )
    return '<div class="definition-grid">' + "".join(cards) + "</div>"


def _top_bar_svg(rows: list[dict[str, Any]]) -> str:
    width = 880
    row_h = 28
    left = 110
    right = 90
    top = 58
    height = top + row_h * max(1, len(rows)) + 46
    max_value = max((_float(row.get("balanced_app_MBps")) for row in rows), default=1.0) or 1.0
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Top balanced app throughput">',
        "<style>.t{font:700 22px Arial;fill:#17202a}.l{font:13px Arial;fill:#34495e}.v{font:700 12px Arial;fill:#17202a}.g{fill:#1769aa}.track{fill:#eef3f8}</style>",
        '<text class="t" x="20" y="32">Top Balanced App Throughput</text>',
    ]
    for idx, row in enumerate(rows):
        y = top + idx * row_h
        value = _float(row.get("balanced_app_MBps"))
        bar_w = int((value / max_value) * (width - left - right))
        parts.extend(
            [
                f'<text class="l" x="20" y="{y + 17}">{_escape(row.get("config_id", ""))}</text>',
                f'<rect class="track" x="{left}" y="{y}" width="{width-left-right}" height="18" rx="4"/>',
                f'<rect class="g" x="{left}" y="{y}" width="{bar_w}" height="18" rx="4"/>',
                f'<text class="v" x="{left + bar_w + 8}" y="{y + 14}">{value:.1f}</text>',
            ]
        )
    parts.append("</svg>")
    return '<article class="plot">' + "\n".join(parts) + "</article>"


def _producer_consumer_scatter_svg(rows: list[dict[str, Any]]) -> str:
    width = 880
    height = 420
    left = 64
    right = 26
    top = 54
    bottom = 54
    max_value = max(
        [_float(row.get("producer_delivered_MBps")) for row in rows]
        + [_float(row.get("consumer_received_MBps")) for row in rows]
        + [1.0]
    )
    plot_w = width - left - right
    plot_h = height - top - bottom
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Producer versus consumer throughput">',
        "<style>.t{font:700 22px Arial;fill:#17202a}.a{font:12px Arial;fill:#607080}.p{fill:#168a5c;opacity:.78}.diag{stroke:#cbd5e1;stroke-width:2}</style>",
        '<text class="t" x="20" y="32">Producer vs Consumer Throughput</text>',
        f'<line class="diag" x1="{left}" y1="{top + plot_h}" x2="{left + plot_w}" y2="{top}"/>',
        f'<text class="a" x="{left}" y="{height - 18}">producer delivered MiB/s</text>',
        f'<text class="a" x="18" y="{top - 12}">consumer received MiB/s</text>',
    ]
    for row in rows:
        x = left + (_float(row.get("producer_delivered_MBps")) / max_value) * plot_w
        y = top + plot_h - (_float(row.get("consumer_received_MBps")) / max_value) * plot_h
        label = _escape(row.get("config_id", ""))
        parts.append(f'<circle class="p"><title>{label}</title><animate attributeName="r" from="4" to="4" dur="0s" fill="freeze"/></circle>'.replace("<circle", f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4"'))
    parts.append("</svg>")
    return '<article class="plot">' + "\n".join(parts) + "</article>"


def _card(title: str, value: str, caption: str) -> str:
    return (
        "<article>"
        f"<span>{_escape(title)}</span>"
        f"<strong>{_escape(value)}</strong>"
        f"<em>{_escape(caption)}</em>"
        "</article>"
    )


def _format_cell(value: Any, key: str, row: dict[str, Any] | None = None) -> str:
    if key == "case_dir" and value:
        target = _escape(value)
        return f'<a href="{target}/">{target}</a>'
    if key.endswith("MBps") or key == "balanced_app_MBps":
        return _escape(f"{_float(value):.1f}")
    if key.endswith("percent") or key in {"failed_send_percent", "pending_backlog_percent"}:
        return _escape(f"{_float(value):.3f}")
    if key in {"flush_sec", "linger_ms"}:
        return _escape(f"{_float(value):.1f}")
    return _escape(value)


def _is_qualified(row: dict[str, Any]) -> bool:
    if "is_qualified" in row:
        return str(row.get("is_qualified", "")).strip().lower() in {"true", "1", "yes"}
    if row.get("qualification_status"):
        return str(row.get("qualification_status", "")).strip().lower() == "qualified"
    return (
        _float(row.get("pending_backlog_percent")) <= 5.0
        and _float(row.get("flush_sec")) <= 10.0
        and _float(row.get("failed_send_percent")) <= 0.1
    )


def _status_order(row: dict[str, Any]) -> int:
    status = str(row.get("qualification_status", "")).strip().lower()
    if status == "qualified" or (not status and _is_qualified(row)):
        return 0
    if status == "ineligible" or str(row.get("eligible", "")).lower() == "false":
        return 2
    return 1


def _latest_run_dir(results_root: Path) -> Path:
    if not results_root.is_dir():
        raise FileNotFoundError(f"Sweep results root not found: {results_root}")
    candidates = [path for path in results_root.iterdir() if path.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"No sweep run directories found under: {results_root}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _escape(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _css() -> str:
    return """
:root { --ink:#17202a; --muted:#5b677a; --line:#d9e0ea; --panel:#f7f9fc; --accent:#126c8f; }
* { box-sizing: border-box; }
body { margin:0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color:var(--ink); background:#fff; line-height:1.45; }
main { width:min(1600px, calc(100vw - 40px)); margin:0 auto; padding:30px 0 54px; }
section { padding:24px 0; border-bottom:1px solid var(--line); }
.hero p:first-child { color:var(--accent); text-transform:uppercase; font-size:12px; font-weight:700; margin:0 0 8px; }
h1 { margin:0 0 14px; font-size:36px; letter-spacing:0; overflow-wrap:anywhere; }
h2 { margin:0 0 14px; font-size:24px; }
.note { color:var(--muted); max-width:1050px; }
.cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(220px, 1fr)); gap:12px; margin-top:18px; }
article { border:1px solid var(--line); border-radius:8px; background:var(--panel); padding:14px; }
article span, article em { display:block; color:var(--muted); font-style:normal; overflow-wrap:anywhere; }
article strong { display:block; margin:6px 0; font-size:24px; color:var(--accent); overflow-wrap:anywhere; }
.definition-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(420px, 1fr)); gap:14px; }
.definition-card h3 { margin:0 0 10px; font-size:18px; }
.definition-card table { background:#fff; }
.plot-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(420px, 1fr)); gap:14px; }
.plot svg { width:100%; height:auto; display:block; }
.table-wrap { overflow-x:auto; border:1px solid var(--line); border-radius:8px; }
table { width:100%; border-collapse:collapse; font-size:13px; }
th, td { padding:9px 10px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; overflow-wrap:anywhere; }
thead th { background:#eef3f8; white-space:nowrap; }
tbody th { background:#f8fafc; min-width:200px; }
code { background:#eef3f8; padding:1px 4px; border-radius:4px; }
a { color:var(--accent); }
@media (max-width:900px) { main { width:min(100vw - 24px, 1600px); } .plot-grid { grid-template-columns:1fr; } }
""".strip()


if __name__ == "__main__":
    raise SystemExit(main())
