#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Install Kafka locally from the vendored archive.
#
# This script never downloads, never uses sudo, and never installs globally. It
# only extracts tools/archives/kafka_2.13-4.2.0.tgz into .local/kafka-dist and
# updates .local/kafka-current.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

KAFKA_ARCHIVE="${LOCAL_KAFKA_ARCHIVE:-$PROJECT_ROOT/tools/archives/kafka_2.13-4.2.0.tgz}"
LOCAL_KAFKA_DIST_DIR="$PROJECT_ROOT/.local/kafka-dist"
LOCAL_KAFKA_CURRENT="$PROJECT_ROOT/.local/kafka-current"
EXPECTED_DIST_NAME="kafka_2.13-4.2.0"
EXPECTED_DIST_DIR="$LOCAL_KAFKA_DIST_DIR/$EXPECTED_DIST_NAME"

die() {
    printf '[install-local-kafka] ERROR: %s\n' "$*" >&2
    exit 1
}

log() {
    printf '[install-local-kafka] %s\n' "$*"
}

is_valid_kafka_home() {
    local kafka_home="${1:-}"
    [[ -n "$kafka_home" ]] || return 1
    [[ -f "$kafka_home/bin/kafka-storage.sh" ]] || return 1
    [[ -f "$kafka_home/bin/kafka-server-start.sh" ]] || return 1
    [[ -f "$kafka_home/bin/kafka-server-stop.sh" ]] || return 1
    [[ -f "$kafka_home/bin/kafka-topics.sh" ]] || return 1
    [[ -f "$kafka_home/bin/kafka-broker-api-versions.sh" ]] || return 1
}

warn_if_java_looks_old() {
    local version_text major
    version_text="$(java -version 2>&1 | awk -F '"' '/version/ {print $2; exit}')"
    major="$(printf '%s\n' "$version_text" | awk -F. '{ if ($1 == "1") print $2; else print $1 }')"

    if [[ "$major" =~ ^[0-9]+$ ]] && (( major < 17 )); then
        log "WARNING: Java appears older than 17 (${version_text}). Kafka 4.x expects Java 17+."
    fi
}

if is_valid_kafka_home "${KAFKA_HOME:-}"; then
    log "KAFKA_HOME is already set and valid: $KAFKA_HOME"
    log "You can use this Kafka installation for local development."
    exit 0
fi

command -v tar >/dev/null 2>&1 || die "tar is required"
command -v gzip >/dev/null 2>&1 || die "gzip is required"
command -v java >/dev/null 2>&1 || die "Java is required. Install Java 17+ before using local Kafka."

log "Java runtime:"
java -version
warn_if_java_looks_old

[[ -f "$KAFKA_ARCHIVE" ]] || die "Kafka archive not found: $KAFKA_ARCHIVE"

mkdir -p "$LOCAL_KAFKA_DIST_DIR"

if [[ ! -d "$EXPECTED_DIST_DIR" ]]; then
    log "Extracting Kafka archive: $KAFKA_ARCHIVE"
    tar -xzf "$KAFKA_ARCHIVE" -C "$LOCAL_KAFKA_DIST_DIR"
else
    log "Kafka distribution already exists: $EXPECTED_DIST_DIR"
fi

(
    cd "$(dirname "$LOCAL_KAFKA_CURRENT")"
    ln -sfn "kafka-dist/$EXPECTED_DIST_NAME" "$(basename "$LOCAL_KAFKA_CURRENT")"
)

if ! is_valid_kafka_home "$LOCAL_KAFKA_CURRENT"; then
    die "Extracted Kafka home is missing expected scripts: $LOCAL_KAFKA_CURRENT"
fi

log "Kafka archive: $KAFKA_ARCHIVE"
log "Installed Kafka home: $LOCAL_KAFKA_CURRENT"
log "Suggested export:"
printf 'export KAFKA_HOME="$PWD/.local/kafka-current"\n'
