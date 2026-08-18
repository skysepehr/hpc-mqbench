#!/usr/bin/env python3
"""Create and verify a code-only repository snapshot for external sharing."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]

ROOT_FILES = (
    ".gitattributes",
    ".gitignore",
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "SECURITY.md",
    "benchmark.sh",
    "requirements-pulsar.txt",
    "requirements.txt",
    "run_all.sh",
    "sitecustomize.py",
)

SOURCE_TREES = (
    "configs",
    "models",
    "monitoring",
    "schemas",
    "scripts",
    "src",
    "tests",
)

EXTRA_FILES = (
    "tools/README.md",
    "tools/stream_probe.c",
    "tools/archives/pulsar-checksums.txt",
    "tools/archives/temurin-jdk21-checksums.txt",
)

FORBIDDEN_TOP_LEVEL = {
    ".git",
    ".local",
    "Report",
    "analysis_deps",
    "analysis_inputs",
    "logs",
    "results",
}

FORBIDDEN_SUFFIXES = {
    ".7z",
    ".class",
    ".gz",
    ".jar",
    ".key",
    ".p12",
    ".pem",
    ".pfx",
    ".png",
    ".pdf",
    ".tar",
    ".tgz",
    ".whl",
    ".zip",
}

SUSPICIOUS_NAMES = {
    ".env",
    "authorized_keys",
    "credentials.json",
    "id_ed25519",
    "id_rsa",
    "known_hosts",
    "secrets.json",
}

MAX_FILE_BYTES = 5 * 1024 * 1024

CONTENT_RULES = (
    (
        "private-key material",
        re.compile(
            r"-----BEGIN (?:OPENSSH|RSA|DSA|EC|PGP|[A-Z0-9 ]*PRIVATE) KEY-----"
        ),
    ),
    ("AWS access key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    (
        "GitHub access token",
        re.compile(r"\b(?:github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})\b"),
    ),
    ("Slack access token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")),
    ("Google API key", re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b")),
    ("OpenAI-style API key", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
    (
        "literal user home path",
        re.compile(r"/(?:home/[A-Za-z0-9._-]+|user/[A-Za-z0-9._-]+/u[0-9]+)(?:/|\b)"),
    ),
    (
        "literal GWDG home path",
        re.compile(r"/mnt/vast-nhr/home/[A-Za-z0-9._-]+/u[0-9]+(?:/|\b)"),
    ),
    ("literal GWDG user ID", re.compile(r"\bu[0-9]{5,}\b")),
    (
        "literal Slurm allocation account",
        re.compile(r"\bnhr_[a-z0-9][a-z0-9_-]*_[0-9]{3,}\b", re.IGNORECASE),
    ),
    ("internal benchmark IPv4 address", re.compile(r"\b10\.246\.[0-9]{1,3}\.[0-9]{1,3}\b")),
)


class ReleaseAuditError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create an allowlisted source-only snapshot, or audit an existing snapshot."
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path, help="new snapshot directory")
    mode.add_argument("--audit-only", type=Path, help="existing snapshot directory")
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iter_source_files() -> Iterable[tuple[Path, Path]]:
    for relative_name in ROOT_FILES + EXTRA_FILES:
        source = PROJECT_ROOT / relative_name
        if not source.is_file():
            raise ReleaseAuditError(f"required source file is missing: {relative_name}")
        yield source, Path(relative_name)

    for tree_name in SOURCE_TREES:
        tree = PROJECT_ROOT / tree_name
        if not tree.is_dir():
            raise ReleaseAuditError(f"required source tree is missing: {tree_name}")
        for source in sorted(tree.rglob("*")):
            if not source.is_file():
                continue
            relative = source.relative_to(PROJECT_ROOT)
            if "__pycache__" in relative.parts or source.suffix in {".pyc", ".pyo"}:
                continue
            yield source, relative


def copy_snapshot(destination: Path) -> None:
    destination = destination.resolve()
    if destination == PROJECT_ROOT or PROJECT_ROOT in destination.parents:
        allowed_inside = destination.name in {
            "github-source",
            "public-source",
            "source-release",
        }
        if not allowed_inside:
            raise ReleaseAuditError(
                "output inside the working repository must be named source-release, "
                "public-source, or github-source"
            )
    if destination.exists():
        raise ReleaseAuditError(
            f"output already exists; choose a new empty path: {destination}"
        )

    destination.mkdir(parents=True)
    copied: set[Path] = set()
    for source, relative in iter_source_files():
        if relative in copied:
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        copied.add(relative)

    write_manifest(destination)
    write_checksums(destination)


def snapshot_files(root: Path, *, include_checksums: bool = True) -> list[Path]:
    files: list[Path] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if (
            not include_checksums
            and path.parent == root
            and path.name in {"SHA256SUMS", "SOURCE_MANIFEST.json"}
        ):
            continue
        files.append(path)
    return files


def write_manifest(root: Path) -> None:
    rows = []
    for path in snapshot_files(root, include_checksums=False):
        rows.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    payload = {
        "schema_version": "messaging-benchmark.source-release.v1",
        "policy": "source-only; no Git history, results, reports, credentials, or runtime binaries",
        "file_count": len(rows),
        "files": rows,
    }
    (root / "SOURCE_MANIFEST.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_checksums(root: Path) -> None:
    checksum_path = root / "SHA256SUMS"
    files = [path for path in snapshot_files(root) if path != checksum_path]
    checksum_path.write_text(
        "".join(
            f"{sha256(path)}  {path.relative_to(root).as_posix()}\n" for path in files
        ),
        encoding="utf-8",
    )


def audit_snapshot(root: Path) -> dict[str, int]:
    root = root.resolve()
    if not root.is_dir():
        raise ReleaseAuditError(f"snapshot directory does not exist: {root}")

    failures: list[str] = []
    for name in sorted(FORBIDDEN_TOP_LEVEL):
        if (root / name).exists():
            failures.append(f"forbidden top-level path is present: {name}")

    files = snapshot_files(root)
    total_bytes = 0
    for path in files:
        relative = path.relative_to(root).as_posix()
        total_bytes += path.stat().st_size
        if path.is_symlink():
            failures.append(f"symbolic link is not allowed: {relative}")
            continue
        if path.stat().st_size > MAX_FILE_BYTES:
            failures.append(
                f"file exceeds {MAX_FILE_BYTES} bytes: {relative} ({path.stat().st_size})"
            )
        if path.suffix.lower() in FORBIDDEN_SUFFIXES:
            failures.append(f"binary/generated suffix is not allowed: {relative}")
        lower_name = path.name.lower()
        if lower_name in SUSPICIOUS_NAMES or lower_name.startswith(
            ("id_rsa", "id_ed25519", ".env.")
        ):
            failures.append(f"credential-like filename is not allowed: {relative}")
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            failures.append(f"non-text file is not allowed: {relative}")
            continue
        for description, pattern in CONTENT_RULES:
            if pattern.search(text):
                failures.append(f"{description} detected in {relative}")

    failures.extend(validate_manifest(root))
    failures.extend(validate_checksums(root))
    if failures:
        raise ReleaseAuditError("\n".join(sorted(set(failures))))

    return {"files": len(files), "bytes": total_bytes}


def validate_manifest(root: Path) -> list[str]:
    manifest_path = root / "SOURCE_MANIFEST.json"
    if not manifest_path.is_file():
        return ["SOURCE_MANIFEST.json is missing"]
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        return [f"SOURCE_MANIFEST.json is invalid: {exc}"]

    expected_files = snapshot_files(root, include_checksums=False)
    expected = {
        path.relative_to(root).as_posix(): (path.stat().st_size, sha256(path))
        for path in expected_files
    }
    observed_rows = payload.get("files")
    if not isinstance(observed_rows, list):
        return ["SOURCE_MANIFEST.json files must be a list"]
    observed: dict[str, tuple[int, str]] = {}
    for row in observed_rows:
        if not isinstance(row, dict):
            return ["SOURCE_MANIFEST.json contains a non-object file row"]
        try:
            observed[str(row["path"])] = (int(row["bytes"]), str(row["sha256"]))
        except (KeyError, TypeError, ValueError):
            return ["SOURCE_MANIFEST.json contains an invalid file row"]
    if observed != expected:
        return ["SOURCE_MANIFEST.json does not match snapshot files"]
    if payload.get("file_count") != len(expected):
        return ["SOURCE_MANIFEST.json file_count is incorrect"]
    return []


def validate_checksums(root: Path) -> list[str]:
    checksum_path = root / "SHA256SUMS"
    if not checksum_path.is_file():
        return ["SHA256SUMS is missing"]
    failures: list[str] = []
    observed: dict[str, str] = {}
    for line_number, line in enumerate(
        checksum_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if not line:
            continue
        try:
            digest, relative = line.split("  ", 1)
        except ValueError:
            failures.append(f"SHA256SUMS line {line_number} is malformed")
            continue
        observed[relative] = digest
    expected_paths = [
        path for path in snapshot_files(root) if path != checksum_path
    ]
    expected = {
        path.relative_to(root).as_posix(): sha256(path) for path in expected_paths
    }
    if observed != expected:
        failures.append("SHA256SUMS does not match snapshot files")
    return failures


def main() -> int:
    args = parse_args()
    try:
        if args.output is not None:
            target = args.output
            copy_snapshot(target)
        else:
            target = args.audit_only
        summary = audit_snapshot(target)
    except ReleaseAuditError as exc:
        print(f"[source-release] ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"[source-release] audit passed: {summary['files']} files, {summary['bytes']} bytes")
    print(f"[source-release] snapshot: {Path(target).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
