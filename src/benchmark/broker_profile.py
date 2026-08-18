from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig


PROFILE_FORMAT = "kafka_broker_profile.v2"
PROFILE_SETTING_ENV = {
    "heap_opts": "KAFKA_HEAP_OPTS",
    "num_network_threads": "KAFKA_NUM_NETWORK_THREADS",
    "num_io_threads": "KAFKA_NUM_IO_THREADS",
    "socket_send_buffer_bytes": "KAFKA_SOCKET_SEND_BUFFER_BYTES",
    "socket_receive_buffer_bytes": "KAFKA_SOCKET_RECEIVE_BUFFER_BYTES",
    "socket_request_max_bytes": "KAFKA_SOCKET_REQUEST_MAX_BYTES",
    "queued_max_requests": "KAFKA_QUEUED_MAX_REQUESTS",
    "log_segment_bytes": "KAFKA_LOG_SEGMENT_BYTES",
}
PROFILE_SETTING_EXTRA = {
    "heap_opts": "kafka_heap_opts",
    "num_network_threads": "kafka_num_network_threads",
    "num_io_threads": "kafka_num_io_threads",
    "socket_send_buffer_bytes": "kafka_socket_send_buffer_bytes",
    "socket_receive_buffer_bytes": "kafka_socket_receive_buffer_bytes",
    "socket_request_max_bytes": "kafka_socket_request_max_bytes",
    "queued_max_requests": "kafka_queued_max_requests",
    "log_segment_bytes": "kafka_log_segment_bytes",
}


@dataclass(frozen=True, slots=True)
class BrokerProfile:
    path: Path
    profile_id: str
    sha256: str
    payload: dict[str, Any]

    @property
    def settings(self) -> dict[str, Any]:
        settings = self.payload.get("settings", {})
        return settings if isinstance(settings, dict) else {}

    def environment(self) -> dict[str, str]:
        return {
            env_name: str(self.settings[setting_name])
            for setting_name, env_name in PROFILE_SETTING_ENV.items()
        }


def canonical_profile_payload(payload: dict[str, Any]) -> bytes:
    canonical = dict(payload)
    canonical.pop("profile_sha256", None)
    return json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def profile_sha256(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_profile_payload(payload)).hexdigest()


def load_broker_profile(path: str | Path) -> BrokerProfile:
    profile_path = Path(path).resolve()
    raw = json.loads(profile_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{profile_path}: broker profile must be a JSON object")
    if raw.get("format") != PROFILE_FORMAT:
        raise ValueError(
            f"{profile_path}: expected format={PROFILE_FORMAT!r}, "
            f"got {raw.get('format')!r}"
        )
    profile_id = str(raw.get("profile_id", "")).strip()
    if not profile_id:
        raise ValueError(f"{profile_path}: profile_id must not be empty")
    settings = raw.get("settings")
    if not isinstance(settings, dict):
        raise ValueError(f"{profile_path}: settings must be an object")
    missing = sorted(set(PROFILE_SETTING_ENV) - set(settings))
    if missing:
        raise ValueError(
            f"{profile_path}: missing broker profile settings: {', '.join(missing)}"
        )

    calculated = profile_sha256(raw)
    declared = str(raw.get("profile_sha256", "")).strip().lower()
    if declared != calculated:
        raise ValueError(
            f"{profile_path}: profile hash mismatch; declared={declared or '<missing>'} "
            f"calculated={calculated}"
        )
    return BrokerProfile(
        path=profile_path,
        profile_id=profile_id,
        sha256=calculated,
        payload=raw,
    )


def resolve_profile_for_config(
    config: BenchmarkConfig,
    *,
    config_path: str | Path,
    project_root: str | Path,
) -> BrokerProfile | None:
    if config.broker_profile_id is None:
        return None

    extra = config.extra if isinstance(config.extra, dict) else {}
    raw_path = extra.get("broker_profile_path")
    if not raw_path:
        raise ValueError(
            "config with broker_profile_id requires extra.broker_profile_path"
        )

    profile_path = Path(str(raw_path))
    if not profile_path.is_absolute():
        profile_path = Path(project_root) / profile_path
    profile = load_broker_profile(profile_path)

    if profile.profile_id != config.broker_profile_id:
        raise ValueError(
            f"{config_path}: broker profile ID mismatch: config="
            f"{config.broker_profile_id} manifest={profile.profile_id}"
        )
    if profile.sha256 != config.broker_profile_sha256:
        raise ValueError(
            f"{config_path}: broker profile hash mismatch: config="
            f"{config.broker_profile_sha256} manifest={profile.sha256}"
        )

    for setting_name, extra_name in PROFILE_SETTING_EXTRA.items():
        if extra_name not in extra:
            continue
        configured = extra[extra_name]
        manifested = profile.settings[setting_name]
        if str(configured) != str(manifested):
            raise ValueError(
                f"{config_path}: extra.{extra_name}={configured!r} conflicts "
                f"with profile {profile.profile_id} value {manifested!r}"
            )
    return profile


def write_broker_profile(path: str | Path, payload: dict[str, Any]) -> BrokerProfile:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    complete = dict(payload)
    complete["profile_sha256"] = profile_sha256(complete)
    output_path.write_text(
        json.dumps(complete, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return load_broker_profile(output_path)
