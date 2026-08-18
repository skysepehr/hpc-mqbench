#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${TEMURIN_JDK21_VERSION:-21.0.12+8}"
ARCHIVE_NAME="${TEMURIN_JDK21_ARCHIVE_NAME:-OpenJDK21U-jdk_x64_linux_hotspot_21.0.12_8.tar.gz}"
ARCHIVE="${TEMURIN_JDK21_ARCHIVE:-$PROJECT_ROOT/tools/archives/$ARCHIVE_NAME}"
EXPECTED_SHA256="${TEMURIN_JDK21_ARCHIVE_SHA256:-e4446ff06a276155697597cc0f1b15da004ff083f4964a35271ecee567177370}"
INSTALL_DIR="${TEMURIN_JDK21_INSTALL_DIR:-$PROJECT_ROOT/.local/temurin-$VERSION}"
CURRENT_LINK="$PROJECT_ROOT/.local/jdk21-current"

die() {
    printf '[install-local-jdk21] ERROR: %s\n' "$*" >&2
    exit 1
}

[[ -f "$ARCHIVE" ]] || die "archive not found: $ARCHIVE"
command -v sha256sum >/dev/null 2>&1 || die "sha256sum is required"
command -v tar >/dev/null 2>&1 || die "tar is required"

actual_sha256="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
[[ "$actual_sha256" == "$EXPECTED_SHA256" ]] || \
    die "archive SHA-256 mismatch for $ARCHIVE"

if [[ ! -x "$INSTALL_DIR/bin/java" ]]; then
    extract_root="$PROJECT_ROOT/.local/jdk21-extract.$$"
    rm -rf "$extract_root"
    mkdir -p "$extract_root" "$(dirname "$INSTALL_DIR")"
    tar -xzf "$ARCHIVE" -C "$extract_root"
    mapfile -t extracted_javas < <(find "$extract_root" -mindepth 2 -maxdepth 3 -type f -path '*/bin/java')
    if (( ${#extracted_javas[@]} != 1 )); then
        rm -rf "$extract_root"
        die "archive must contain exactly one JDK bin/java"
    fi
    extracted_dir="$(dirname "$(dirname "${extracted_javas[0]}")")"
    rm -rf "$INSTALL_DIR"
    mv "$extracted_dir" "$INSTALL_DIR"
    rm -rf "$extract_root"
fi

ln -sfn "$INSTALL_DIR" "$CURRENT_LINK"
printf '[install-local-jdk21] Temurin JDK %s installed at %s\n' "$VERSION" "$INSTALL_DIR"
printf '[install-local-jdk21] JAVA_HOME=%s\n' "$CURRENT_LINK"
