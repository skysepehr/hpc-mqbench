#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Install monitoring tools from vendored archives.
#
# This script does not download anything. It unpacks the archives already stored
# under tools/archives and creates stable *-current symlinks used by local/HPC
# scripts. kafka_exporter is a metrics exporter, not the Kafka broker.
# -----------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

INSTALL_ROOT="${INSTALL_ROOT:-$PROJECT_ROOT/tools}"
ARCHIVE_DIR="${ARCHIVE_DIR:-$INSTALL_ROOT/archives}"

PROM_ARCHIVE="$ARCHIVE_DIR/prometheus-3.11.2.linux-amd64.tar.gz"
NODE_ARCHIVE="$ARCHIVE_DIR/node_exporter-1.11.1.linux-amd64.tar.gz"
KAFKA_EXPORTER_ARCHIVE="$ARCHIVE_DIR/kafka_exporter-1.9.0.linux-amd64.tar.gz"
JMX_JAR="$INSTALL_ROOT/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar"

require_command tar

require_file "$PROM_ARCHIVE"
require_file "$NODE_ARCHIVE"
require_file "$KAFKA_EXPORTER_ARCHIVE"
require_file "$JMX_JAR"

ensure_dir "$INSTALL_ROOT"

extract_if_missing() {
    local archive="${1:?archive required}"
    local expected_dir="${2:?expected dir required}"

    if [[ -d "$expected_dir" ]]; then
        log_info "Already extracted: $expected_dir"
        return
    fi

    log_info "Extracting: $archive"
    tar -xzf "$archive" -C "$INSTALL_ROOT"
}

PROM_DIR="$INSTALL_ROOT/prometheus-3.11.2.linux-amd64"
NODE_DIR="$INSTALL_ROOT/node_exporter-1.11.1.linux-amd64"
KAFKA_EXPORTER_DIR="$INSTALL_ROOT/kafka_exporter-1.9.0.linux-amd64"

extract_if_missing "$PROM_ARCHIVE" "$PROM_DIR"
extract_if_missing "$NODE_ARCHIVE" "$NODE_DIR"
extract_if_missing "$KAFKA_EXPORTER_ARCHIVE" "$KAFKA_EXPORTER_DIR"

(
    cd "$INSTALL_ROOT"
    ln -sfn "$(basename "$PROM_DIR")" prometheus-current
    ln -sfn "$(basename "$NODE_DIR")" node_exporter-current
    ln -sfn "$(basename "$KAFKA_EXPORTER_DIR")" kafka_exporter-current
)

require_file "$INSTALL_ROOT/prometheus-current/prometheus"
require_file "$INSTALL_ROOT/node_exporter-current/node_exporter"
require_file "$INSTALL_ROOT/kafka_exporter-current/kafka_exporter"
require_file "$JMX_JAR"

log_info "Prometheus available at: $INSTALL_ROOT/prometheus-current"
log_info "node_exporter available at: $INSTALL_ROOT/node_exporter-current"
log_info "kafka_exporter available at: $INSTALL_ROOT/kafka_exporter-current"
log_info "JMX exporter jar available at: $JMX_JAR"
