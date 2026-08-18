#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${PULSAR_VERSION:-5.0.0-M1}"
ARCHIVE="${PULSAR_ARCHIVE:-$PROJECT_ROOT/tools/archives/apache-pulsar-${VERSION}-bin.tar.gz}"
EXPECTED_SHA512="${PULSAR_ARCHIVE_SHA512:-51da3ea56788c0a65844824a4cd2c54e4df513e453a1fe3e8444b98a0cccb91085c8b0af04606bd6e5570293b76f355d40b2f3607d6a8139f0aab70de1486855}"
INSTALL_DIR="${PULSAR_INSTALL_DIR:-$PROJECT_ROOT/.local/apache-pulsar-${VERSION}}"
CURRENT_LINK="$PROJECT_ROOT/.local/pulsar-current"

die() {
    printf '[install-local-pulsar] ERROR: %s\n' "$*" >&2
    exit 1
}

[[ -f "$ARCHIVE" ]] || die "archive not found: $ARCHIVE"
command -v sha512sum >/dev/null 2>&1 || die "sha512sum is required"
command -v tar >/dev/null 2>&1 || die "tar is required"

actual_sha512="$(sha512sum "$ARCHIVE" | awk '{print $1}')"
[[ "$actual_sha512" == "$EXPECTED_SHA512" ]] || \
    die "archive SHA-512 mismatch for $ARCHIVE"

if [[ ! -x "$INSTALL_DIR/bin/pulsar" ]]; then
    extract_root="$PROJECT_ROOT/.local/pulsar-extract.$$"
    rm -rf "$extract_root"
    mkdir -p "$extract_root" "$(dirname "$INSTALL_DIR")"
    tar -xzf "$ARCHIVE" -C "$extract_root"
    extracted_dir="$extract_root/apache-pulsar-${VERSION}"
    [[ -x "$extracted_dir/bin/pulsar" ]] || \
        die "archive did not contain apache-pulsar-${VERSION}/bin/pulsar"
    rm -rf "$INSTALL_DIR"
    mv "$extracted_dir" "$INSTALL_DIR"
    rm -rf "$extract_root"
fi

ln -sfn "$INSTALL_DIR" "$CURRENT_LINK"
printf '[install-local-pulsar] Pulsar %s installed at %s\n' "$VERSION" "$INSTALL_DIR"
printf '[install-local-pulsar] PULSAR_HOME=%s\n' "$CURRENT_LINK"
