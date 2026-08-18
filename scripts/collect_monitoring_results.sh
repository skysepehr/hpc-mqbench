#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Collect a small Prometheus monitoring snapshot for one benchmark case.
#
# Prometheus keeps the live time-series data while the benchmark is running. This
# script queries the HTTP API before monitoring is stopped and saves two durable
# artifacts inside the case directory:
# - data/monitoring_snapshot.json
# - reports/monitoring_summary.md
#
# The script is intentionally best-effort by default. If Prometheus was disabled
# or unreachable, it records a skipped/partial result instead of hiding the
# benchmark's own outputs.
#
# Required environment variables:
# - CASE_DIR
#
# Optional environment variables:
# - PROMETHEUS_ENDPOINT             (host:port or http://host:port)
# - MONITORING_QUERY_TIMEOUT_SEC    (default: 5)
# - MONITORING_MAX_SAMPLES          (default: 200)
# - MONITORING_FAIL_ON_ERROR        (default: 0)
# - ENABLE_MONITORING_RANGE         (default: 1)
# - MONITORING_RANGE_STEP_SEC       (default: 15)
# - MONITORING_GRACE_SEC            (default: 15)
# - SKIP_MONITORING_GRAPHS          (default: 0)
# - DATA_DIR_NAME                   (default: data)
# - REPORTS_DIR_NAME                (default: reports)
# - BACKEND_ID                      (default: kafka)
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

require_nonempty "CASE_DIR" "${CASE_DIR:-}"

CASE_DIR="$(abspath "$CASE_DIR")"
DATA_DIR_NAME="${DATA_DIR_NAME:-${RESULTS_DIR_NAME:-data}}"
REPORTS_DIR_NAME="${REPORTS_DIR_NAME:-reports}"

CASE_DATA_DIR="$CASE_DIR/$DATA_DIR_NAME"
CASE_REPORTS_DIR="$CASE_DIR/$REPORTS_DIR_NAME"
MONITORING_RUNTIME_DIR="$CASE_DIR/runtime/monitoring"
PROMETHEUS_ENDPOINT_FILE="$MONITORING_RUNTIME_DIR/prometheus_endpoint.txt"

ensure_dir "$CASE_DATA_DIR"
ensure_dir "$CASE_REPORTS_DIR"

PROMETHEUS_ENDPOINT="${PROMETHEUS_ENDPOINT:-}"
if [[ -z "$PROMETHEUS_ENDPOINT" && -f "$PROMETHEUS_ENDPOINT_FILE" ]]; then
    PROMETHEUS_ENDPOINT="$(<"$PROMETHEUS_ENDPOINT_FILE")"
fi

export PROMETHEUS_ENDPOINT
export MONITORING_QUERY_TIMEOUT_SEC="${MONITORING_QUERY_TIMEOUT_SEC:-5}"
export MONITORING_MAX_SAMPLES="${MONITORING_MAX_SAMPLES:-200}"
export MONITORING_FAIL_ON_ERROR="${MONITORING_FAIL_ON_ERROR:-0}"
export ENABLE_MONITORING_RANGE="${ENABLE_MONITORING_RANGE:-1}"
export MONITORING_RANGE_STEP_SEC="${MONITORING_RANGE_STEP_SEC:-15}"
export MONITORING_GRACE_SEC="${MONITORING_GRACE_SEC:-15}"
export SKIP_MONITORING_GRAPHS="${SKIP_MONITORING_GRAPHS:-0}"
export BACKEND_ID="${BACKEND_ID:-kafka}"
export MONITORING_SNAPSHOT_FILE="$CASE_DATA_DIR/monitoring_snapshot.json"
export MONITORING_SUMMARY_FILE="$CASE_REPORTS_DIR/monitoring_summary.md"
export MONITORING_RANGE_DIR="$CASE_DIR/monitoring"
export MONITORING_RANGE_SUMMARY_FILE="$MONITORING_RANGE_DIR/monitoring_summary.json"
export PYTHONPATH="$(cd "$SCRIPT_DIR/.." && pwd):${PYTHONPATH:-}"

log_info "Collecting monitoring snapshot"
log_info "Monitoring snapshot file: $MONITORING_SNAPSHOT_FILE"
log_info "Monitoring summary file: $MONITORING_SUMMARY_FILE"

python3 - <<'PY'
import datetime as _dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


SNAPSHOT_FILE = Path(os.environ["MONITORING_SNAPSHOT_FILE"])
SUMMARY_FILE = Path(os.environ["MONITORING_SUMMARY_FILE"])
ENDPOINT = os.environ.get("PROMETHEUS_ENDPOINT", "").strip()
TIMEOUT = float(os.environ.get("MONITORING_QUERY_TIMEOUT_SEC", "5"))
MAX_SAMPLES = int(os.environ.get("MONITORING_MAX_SAMPLES", "200"))
FAIL_ON_ERROR = os.environ.get("MONITORING_FAIL_ON_ERROR", "0") == "1"
BACKEND_ID = os.environ.get("BACKEND_ID", "kafka").strip().lower()
MACHINE_ONLY = os.environ.get("BENCHMARK_REPORT_MODE", "full").strip().lower() == "machine"


def now_iso():
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def normalize_endpoint(endpoint):
    if not endpoint:
        return ""
    if endpoint.startswith(("http://", "https://")):
        return endpoint.rstrip("/")
    return f"http://{endpoint.rstrip('/')}"


def request_json(base_url, path, params=None):
    query = urllib.parse.urlencode(params or {})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def query_prometheus(base_url, query):
    return request_json(base_url, "/api/v1/query", {"query": query})


def simplify_targets(targets_payload):
    data = targets_payload.get("data", {})
    active_targets = data.get("activeTargets", [])
    dropped_targets = data.get("droppedTargets", [])
    simplified = []
    health_counts = {}

    for target in active_targets:
        health = target.get("health", "unknown")
        health_counts[health] = health_counts.get(health, 0) + 1
        simplified.append(
            {
                "scrape_url": target.get("scrapeUrl"),
                "health": health,
                "last_error": target.get("lastError", ""),
                "labels": target.get("labels", {}),
                "discovered_labels": target.get("discoveredLabels", {}),
            }
        )

    return {
        "active_count": len(active_targets),
        "dropped_count": len(dropped_targets),
        "health_counts": health_counts,
        "active_targets": simplified,
    }


def trim_query_result(query_payload):
    data = query_payload.get("data", {})
    result = data.get("result", [])
    trimmed_result = result[:MAX_SAMPLES]
    return {
        "result_type": data.get("resultType"),
        "sample_count": len(result),
        "truncated": len(result) > MAX_SAMPLES,
        "result": trimmed_result,
    }


def format_value_preview(query_payload):
    """
    Return a compact human-readable preview for instant-vector query values.

    The full Prometheus payload stays in monitoring_snapshot.json. The preview
    makes the Markdown report useful at a glance, especially for Kafka broker
    throughput rates.
    """
    data = query_payload.get("data", {})
    result = data.get("result", []) if isinstance(data, dict) else []
    if not isinstance(result, list) or not result:
        return ""

    previews = []
    for series in result[:5]:
        if not isinstance(series, dict):
            continue
        metric = series.get("metric", {})
        value = series.get("value")
        if not isinstance(value, list) or len(value) < 2:
            continue

        label_parts = []
        if isinstance(metric, dict):
            for key in ("job", "instance", "topic", "partition", "device", "mode"):
                if metric.get(key):
                    label_parts.append(f"{key}={metric[key]}")
        label = ",".join(label_parts)
        display_value = _format_number(value[1])
        previews.append(f"{label}: {display_value}" if label else display_value)

    if len(result) > 5:
        previews.append(f"... {len(result) - 5} more")
    return "; ".join(previews)


def _format_number(value):
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(numeric) >= 1000:
        return f"{numeric:,.3f}"
    return f"{numeric:.6g}"


def write_markdown(snapshot):
    if MACHINE_ONLY:
        return
    def pipe_safe(value):
        return str(value).replace("|", "\\|")

    lines = [
        "# Monitoring Snapshot",
        "",
        f"- Status: `{snapshot['status']}`",
        f"- Collected at: `{snapshot['collected_at']}`",
        f"- Prometheus endpoint: `{snapshot.get('endpoint') or 'not available'}`",
        "",
    ]

    if snapshot["status"] == "skipped":
        lines.extend(
            [
                "Prometheus was not configured for this case, so no monitoring snapshot was collected.",
                "",
            ]
        )
    else:
        targets = snapshot.get("targets", {})
        lines.extend(
            [
                "## Targets",
                "",
                f"- Active targets: `{targets.get('active_count', 0)}`",
                f"- Dropped targets: `{targets.get('dropped_count', 0)}`",
                f"- Health counts: `{json.dumps(targets.get('health_counts', {}), sort_keys=True)}`",
                "",
                "## Query Coverage",
                "",
                "| Metric group | Query | Samples | Value preview | Status |",
                "|---|---|---:|---|---|",
            ]
        )

        for query in snapshot.get("queries", []):
            lines.append(
                "| {name} | `{query_text}` | {count} | {value_preview} | {status} |".format(
                    name=pipe_safe(query["name"]),
                    query_text=pipe_safe(query["query"]),
                    count=query.get("sample_count", 0),
                    value_preview=pipe_safe(query.get("value_preview", "")),
                    status=pipe_safe(query["status"]),
                )
            )

        lines.append("")

    errors = snapshot.get("errors", [])
    if errors:
        lines.extend(["## Collection Notes", ""])
        for error in errors:
            lines.append(f"- {error}")
        lines.append("")

    lines.extend(
        [
            "The complete raw API snapshot is stored in `data/monitoring_snapshot.json`.",
            "",
        ]
    )

    SUMMARY_FILE.write_text("\n".join(lines), encoding="utf-8")


QUERIES = [
    {
        "name": "scrape target health",
        "query": "up",
        "description": "Prometheus target health by job and instance.",
    },
    {
        "name": "scrape samples",
        "query": "scrape_samples_scraped",
        "description": "Number of samples scraped from each target.",
    },
    {
        "name": "scrape duration",
        "query": "scrape_duration_seconds",
        "description": "Duration of each scrape.",
    },
    {
        "name": "Kafka messages in",
        "query": "kafka_server_brokertopicmetrics_messagesinpersec_total",
        "description": "Kafka message ingress counter exposed through JMX exporter when available.",
    },
    {
        "name": "Kafka broker messages in rate",
        "query": "sum(rate(kafka_server_brokertopicmetrics_messagesinpersec_total[1m]))",
        "description": "Kafka broker message ingress rate derived from the JMX counter.",
    },
    {
        "name": "Kafka broker messages in one-minute rate",
        "query": "sum(kafka_server_brokertopicmetrics_messagesinpersec_oneminuterate)",
        "description": "Kafka broker smoothed message ingress rate reported by Kafka.",
    },
    {
        "name": "Kafka bytes in",
        "query": "kafka_server_brokertopicmetrics_bytesinpersec_total",
        "description": "Kafka byte ingress counter exposed through JMX exporter when available.",
    },
    {
        "name": "Kafka broker bytes in rate",
        "query": "sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_total[1m]))",
        "description": "Kafka broker byte ingress throughput derived from the JMX counter.",
    },
    {
        "name": "Kafka broker bytes in one-minute rate",
        "query": "sum(kafka_server_brokertopicmetrics_bytesinpersec_oneminuterate)",
        "description": "Kafka broker smoothed byte ingress throughput reported by Kafka.",
    },
    {
        "name": "Kafka bytes out",
        "query": "kafka_server_brokertopicmetrics_bytesoutpersec_total",
        "description": "Kafka byte egress counter exposed through JMX exporter when available.",
    },
    {
        "name": "Kafka broker bytes out rate",
        "query": "sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[1m]))",
        "description": "Kafka broker byte egress throughput derived from the JMX counter.",
    },
    {
        "name": "Kafka broker combined bytes in plus out rate",
        "query": "sum(rate(kafka_server_brokertopicmetrics_bytesinpersec_total[1m])) + sum(rate(kafka_server_brokertopicmetrics_bytesoutpersec_total[1m]))",
        "description": "Kafka broker combined byte ingress plus egress throughput during simultaneous read/write workloads.",
    },
    {
        "name": "Kafka broker bytes out one-minute rate",
        "query": "sum(kafka_server_brokertopicmetrics_bytesoutpersec_oneminuterate)",
        "description": "Kafka broker smoothed byte egress throughput reported by Kafka.",
    },
    {
        "name": "Kafka under-replicated partitions",
        "query": "kafka_server_replicamanager_underreplicatedpartitions",
        "description": "Under-replicated partitions exposed through JMX exporter.",
    },
    {
        "name": "Kafka active controller count",
        "query": "kafka_controller_kafkacontroller_activecontrollercount",
        "description": "Active controller count exposed through JMX exporter.",
    },
    {
        "name": "Kafka exporter broker count",
        "query": "kafka_brokers",
        "description": "Broker count reported by kafka_exporter when enabled.",
    },
    {
        "name": "Kafka exporter topic partitions",
        "query": "kafka_topic_partitions",
        "description": "Topic partition counts reported by kafka_exporter when enabled.",
    },
    {
        "name": "Kafka exporter consumer group lag",
        "query": "kafka_consumergroup_lag or kafka_consumergroup_lag_sum or kafka_consumergroup_group_lag or kafka_consumergroup_group_lag_sum",
        "description": "Consumer-group lag reported by kafka_exporter when enabled.",
    },
    {
        "name": "JVM memory",
        "query": "jvm_memory_heap_used_bytes",
        "description": "Kafka broker JVM heap and non-heap memory usage when exposed.",
    },
    {
        "name": "JVM threads",
        "query": "jvm_threads_threadcount",
        "description": "Kafka broker JVM thread count when exposed.",
    },
    {
        "name": "process CPU",
        "query": "process_cpu_seconds_total",
        "description": "Exporter or broker process CPU counter when exposed.",
    },
    {
        "name": "process RSS",
        "query": "process_resident_memory_bytes",
        "description": "Resident memory for monitored processes when exposed.",
    },
    {
        "name": "node memory available",
        "query": "node_memory_MemAvailable_bytes",
        "description": "Available memory on broker nodes from node_exporter.",
    },
    {
        "name": "node memory total",
        "query": "node_memory_MemTotal_bytes",
        "description": "Total memory on broker nodes from node_exporter.",
    },
    {
        "name": "node load",
        "query": "node_load1",
        "description": "One-minute load average from node_exporter.",
    },
    {
        "name": "node CPU rate",
        "query": "rate(node_cpu_seconds_total[1m])",
        "description": "Per-mode CPU rate over the final minute.",
    },
    {
        "name": "node network receive",
        "query": "rate(node_network_receive_bytes_total[1m])",
        "description": "Receive throughput by network device over the final minute.",
    },
    {
        "name": "node network transmit",
        "query": "rate(node_network_transmit_bytes_total[1m])",
        "description": "Transmit throughput by network device over the final minute.",
    },
]

if BACKEND_ID == "pulsar":
    common_query_names = {
        "scrape target health",
        "scrape samples",
        "scrape duration",
        "process CPU",
        "process RSS",
        "node memory available",
        "node memory total",
        "node load",
        "node CPU rate",
        "node network receive",
        "node network transmit",
    }
    QUERIES = [entry for entry in QUERIES if entry["name"] in common_query_names]
    QUERIES.extend(
        [
            {
                "name": "Pulsar messages in rate",
                "query": "sum(pulsar_broker_rate_in) or sum(pulsar_rate_in)",
                "description": "Pulsar broker message ingress rate.",
            },
            {
                "name": "Pulsar messages out rate",
                "query": "sum(pulsar_broker_rate_out) or sum(pulsar_rate_out)",
                "description": "Pulsar broker message egress rate.",
            },
            {
                "name": "Pulsar bytes in rate",
                "query": "sum(pulsar_broker_throughput_in) or sum(pulsar_throughput_in)",
                "description": "Pulsar broker byte ingress rate.",
            },
            {
                "name": "Pulsar bytes out rate",
                "query": "sum(pulsar_broker_throughput_out) or sum(pulsar_throughput_out)",
                "description": "Pulsar broker byte egress rate.",
            },
            {
                "name": "Pulsar message backlog",
                "query": "sum(pulsar_broker_msg_backlog) or sum(pulsar_msg_backlog)",
                "description": "Backlog across Pulsar subscriptions.",
            },
            {
                "name": "Pulsar producer count",
                "query": "sum(pulsar_broker_producers_count) or sum(pulsar_producers_count)",
                "description": "Connected Pulsar producers.",
            },
            {
                "name": "Pulsar consumer count",
                "query": "sum(pulsar_broker_consumers_count) or sum(pulsar_consumers_count)",
                "description": "Connected Pulsar consumers.",
            },
            {
                "name": "Pulsar JVM heap used",
                "query": "sum(jvm_memory_bytes_used{area=\"heap\"})",
                "description": "Pulsar JVM heap consumption when exported.",
            },
            {
                "name": "Pulsar JVM GC pauses",
                "query": "sum(rate(jvm_gc_collection_seconds_sum[30s]))",
                "description": "Pulsar JVM garbage-collection time rate.",
            },
            {
                "name": "Pulsar managed-ledger direct pool allocated",
                "query": "sum(pulsar_ml_cache_pool_allocated)",
                "description": "Allocated direct-arena memory used by managed ledgers.",
            },
            {
                "name": "Pulsar managed-ledger direct pool used",
                "query": "sum(pulsar_ml_cache_pool_used)",
                "description": "Used direct-arena memory reported by managed ledgers.",
            },
            {
                "name": "Pulsar process RSS",
                "query": "max(process_resident_memory_bytes)",
                "description": "Resident memory of the Pulsar standalone JVM process.",
            },
            {
                "name": "BookKeeper journal queue",
                "query": "sum(bookie_journal_JOURNAL_QUEUE_SIZE)",
                "description": "Requests pending in the colocated BookKeeper journal queue.",
            },
            {
                "name": "BookKeeper force-write queue",
                "query": "sum(bookie_journal_JOURNAL_FORCE_WRITE_QUEUE_SIZE)",
                "description": "Fsync requests pending in the BookKeeper force-write queue.",
            },
        ]
    )


endpoint = normalize_endpoint(ENDPOINT)
snapshot = {
    "status": "skipped" if not endpoint else "completed",
    "collected_at": now_iso(),
    "endpoint": endpoint,
    "backend_id": BACKEND_ID,
    "timeout_sec": TIMEOUT,
    "max_samples_per_query": MAX_SAMPLES,
    "targets": {},
    "queries": [],
    "errors": [],
}

if not endpoint:
    snapshot["errors"].append(
        "No Prometheus endpoint was provided and runtime/monitoring/prometheus_endpoint.txt was missing."
    )
else:
    try:
        targets_payload = request_json(endpoint, "/api/v1/targets")
        snapshot["targets"] = simplify_targets(targets_payload)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as exc:
        snapshot["status"] = "partial"
        snapshot["errors"].append(f"Failed to query Prometheus targets: {exc}")

    for entry in QUERIES:
        started = time.monotonic()
        item = {
            "name": entry["name"],
            "query": entry["query"],
            "description": entry["description"],
            "status": "completed",
            "duration_sec": 0.0,
            "result_type": None,
            "sample_count": 0,
            "truncated": False,
            "result": [],
        }
        try:
            payload = query_prometheus(endpoint, entry["query"])
            if payload.get("status") != "success":
                raise RuntimeError(payload.get("error", "Prometheus returned a non-success status"))
            item.update(trim_query_result(payload))
            item["value_preview"] = format_value_preview(payload)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, RuntimeError) as exc:
            item["status"] = "error"
            item["error"] = str(exc)
            snapshot["status"] = "partial"
            snapshot["errors"].append(f"Query failed for {entry['name']}: {exc}")
        finally:
            item["duration_sec"] = round(time.monotonic() - started, 3)
            snapshot["queries"].append(item)

SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
write_markdown(snapshot)

if FAIL_ON_ERROR and snapshot["status"] != "completed":
    sys.exit(1)
PY

if [[ "$ENABLE_MONITORING_RANGE" == "1" && -n "$PROMETHEUS_ENDPOINT" ]]; then
    if [[ -z "${BENCHMARK_START_UNIX:-}" || -z "${BENCHMARK_END_UNIX:-}" ]]; then
        WINDOW_FILE=""
        if [[ -f "$CASE_DIR/runtime/monitoring_window.json" ]]; then
            WINDOW_FILE="$CASE_DIR/runtime/monitoring_window.json"
        elif [[ -f "$CASE_DIR/runtime/benchmark_window.json" ]]; then
            WINDOW_FILE="$CASE_DIR/runtime/benchmark_window.json"
        fi
        if [[ -n "$WINDOW_FILE" ]]; then
            read -r BENCHMARK_START_UNIX BENCHMARK_END_UNIX < <(
                python3 - "$WINDOW_FILE" <<'PY'
import json
import sys
from pathlib import Path

window = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(window.get("start_unix", ""), window.get("end_unix", ""))
PY
            )
            export BENCHMARK_START_UNIX
            export BENCHMARK_END_UNIX
        fi
    fi

    if [[ -n "${BENCHMARK_START_UNIX:-}" && -n "${BENCHMARK_END_UNIX:-}" ]]; then
        read -r RANGE_START_UNIX RANGE_END_UNIX < <(
            python3 - "$BENCHMARK_START_UNIX" "$BENCHMARK_END_UNIX" "$MONITORING_GRACE_SEC" <<'PY'
import sys

start = float(sys.argv[1])
end = float(sys.argv[2])
grace = float(sys.argv[3])
print(f"{start:.3f}", f"{end + grace:.3f}")
PY
        )

        graph_args=()
        if [[ "$SKIP_MONITORING_GRAPHS" == "1" ]]; then
            graph_args+=(--skip-graphs)
        fi
        if [[ -f "$CASE_DIR/runtime/node_roles.json" ]]; then
            graph_args+=(--node-roles "$CASE_DIR/runtime/node_roles.json")
        fi
        if [[ -f "$CASE_DIR/runtime/benchmark_events.json" ]]; then
            graph_args+=(--events "$CASE_DIR/runtime/benchmark_events.json")
        fi

        log_info "Collecting Prometheus range monitoring bundle"
        log_info "Monitoring range directory: $MONITORING_RANGE_DIR"
        python3 -m src.benchmark.monitoring_client \
            --backend-id "$BACKEND_ID" \
            --prometheus-url "$PROMETHEUS_ENDPOINT" \
            --output-dir "$MONITORING_RANGE_DIR" \
            --start "$RANGE_START_UNIX" \
            --end "$RANGE_END_UNIX" \
            --step-sec "$MONITORING_RANGE_STEP_SEC" \
            --artifact-prefix monitoring \
            "${graph_args[@]}" \
            || log_warn "Prometheus range monitoring bundle collection failed"
    else
        log_warn "Benchmark time window is unavailable; skipping range monitoring bundle"
    fi
fi

log_info "Monitoring snapshot collection completed"

MERGE_MONITORING_FILE="$MONITORING_SNAPSHOT_FILE"
if [[ -f "$MONITORING_RANGE_SUMMARY_FILE" ]]; then
    MERGE_MONITORING_FILE="$MONITORING_RANGE_SUMMARY_FILE"
fi

python3 -m src.benchmark.monitoring_summary \
    --output-dir "$CASE_DIR" \
    --snapshot "$MERGE_MONITORING_FILE" \
    || log_warn "Could not merge monitoring snapshot into final reports"
