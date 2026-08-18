from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from models.benchmark_config import BenchmarkConfig


@dataclass(slots=True)
class GeneratedPayload:
    """
    Container for one generated message.

    We keep both the Python dictionary and the serialized bytes because:
    - the dictionary is useful for debugging and testing
    - the bytes are what the Kafka producer will actually send
    """

    payload_dict: dict[str, Any]
    payload_bytes: bytes
    payload_size_bytes: int
    device_id: str
    sequence_number: int


class PayloadGenerator:
    """
    Generate synthetic IoT-style payloads for benchmark producers.

    This class is responsible only for building payloads.
    It does not send anything to Kafka.

    Supported payload modes:
    - compact
    - standard
    - enriched
    - fixed_size

    Each producer rank should create one PayloadGenerator instance and use it
    repeatedly inside its send loop.
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        rank: int,
        device_index_start: int = 0,
    ) -> None:
        """
        Parameters
        ----------
        config:
            One validated benchmark configuration.

        rank:
            MPI rank of the current producer. Used for device naming and IDs.

        device_index_start:
            First virtual device index assigned to this producer rank.
        """
        self.config = config
        self.rank = rank
        self.device_index_start = device_index_start

        # We keep a reusable base template for speed and clarity.
        self._base_template = self._build_base_template()

    def generate(self, message_index: int) -> GeneratedPayload:
        """
        Generate one payload for the given message index.

        Parameters
        ----------
        message_index:
            Zero-based counter of how many messages this producer has created.

        Returns
        -------
        GeneratedPayload
            Structured result containing the dictionary, encoded bytes,
            size, device_id, and sequence number.
        """
        # Choose a virtual device for this message.
        device_id, sequence_number = self._select_device_and_sequence(message_index)

        # Build the payload according to the configured mode.
        if self.config.payload_mode == "compact":
            payload = self._build_compact_payload(device_id, sequence_number)
        elif self.config.payload_mode == "standard":
            payload = self._build_standard_payload(device_id, sequence_number)
        elif self.config.payload_mode == "enriched":
            payload = self._build_enriched_payload(device_id, sequence_number)
        elif self.config.payload_mode == "fixed_size":
            payload = self._build_fixed_size_base_payload(device_id, sequence_number)
        else:
            raise ValueError(f"Unsupported payload_mode: {self.config.payload_mode}")

        # Serialize the payload first.
        payload_bytes = self._serialize_payload(payload)

        # If the mode requires a fixed size, pad until the target is reached.
        if self.config.payload_mode == "fixed_size":
            payload, payload_bytes = self._pad_payload_to_target_size(payload)

        return GeneratedPayload(
            payload_dict=payload,
            payload_bytes=payload_bytes,
            payload_size_bytes=len(payload_bytes),
            device_id=device_id,
            sequence_number=sequence_number,
        )

    def _build_base_template(self) -> dict[str, Any]:
        """
        Build the base payload structure shared by richer payload modes.

        This is used as a template and then copied for each generated message.
        """
        return {
            "event_id": "evt-00000000",
            "device_id": "sensor-000000",
            "timestamp": "1970-01-01T00:00:00Z",
            "seq": 0,
            "metrics": {
                "temperature": 21.4,
                "humidity": 48.2,
                "pressure": 1012.4,
            },
            "status": "OK",
        }

    def _select_device_and_sequence(self, message_index: int) -> tuple[str, int]:
        """
        Select which virtual device this message belongs to.

        We use a simple round-robin assignment across the producer's virtual
        device range. This is predictable and cheap.

        Returns
        -------
        tuple[str, int]
            (device_id, sequence_number)
        """
        # Map the message to one virtual device within this rank's device range.
        local_device_offset = message_index % self.config.virtual_devices_per_rank
        global_device_index = self.device_index_start + local_device_offset

        # Use the same message index as the sequence number for now.
        # This keeps the first implementation simple.
        sequence_number = message_index

        device_id = f"sensor-{global_device_index:06d}"
        return device_id, sequence_number

    def _current_timestamp(self) -> str:
        """
        Return the current UTC timestamp in ISO-8601 format.
        """
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _build_compact_payload(
        self,
        device_id: str,
        sequence_number: int,
    ) -> dict[str, Any]:
        """
        Build a compact payload for high-message-rate tests.

        This mode keeps the payload small and simple.
        """
        return {
            "device_id": device_id,
            "timestamp": self._current_timestamp(),
            "seq": sequence_number,
            "value": 21.4,
            "status": "OK",
        }

    def _build_standard_payload(
        self,
        device_id: str,
        sequence_number: int,
    ) -> dict[str, Any]:
        """
        Build the standard IoT payload.

        We copy the base template and then update only the fields that should
        change per message.
        """
        payload = copy.deepcopy(self._base_template)
        payload["event_id"] = f"evt-{self.rank}-{sequence_number}"
        payload["device_id"] = device_id
        payload["timestamp"] = self._current_timestamp()
        payload["seq"] = sequence_number

        # Add a tiny deterministic metric variation so messages are not all identical.
        payload["metrics"]["temperature"] = 20.0 + ((sequence_number % 50) / 10.0)
        payload["metrics"]["humidity"] = 45.0 + ((sequence_number % 30) / 10.0)
        payload["metrics"]["pressure"] = 1000.0 + ((sequence_number % 100) / 10.0)

        return payload

    def _build_enriched_payload(
        self,
        device_id: str,
        sequence_number: int,
    ) -> dict[str, Any]:
        """
        Build a richer payload with extra metadata and diagnostics.

        This is useful when testing larger and more realistic telemetry events.
        """
        payload = self._build_standard_payload(device_id, sequence_number)

        payload["site_id"] = f"site-{self.rank:03d}"
        payload["gateway_id"] = f"gw-{self.rank:03d}"
        payload["firmware_version"] = "1.0.0"
        payload["tags"] = ["iot", "sensor", "benchmark"]
        payload["diagnostics"] = {
            "battery_health": 95,
            "error_count": 0,
            "uptime_sec": 3600 + sequence_number,
        }

        return payload

    def _build_fixed_size_base_payload(
        self,
        device_id: str,
        sequence_number: int,
    ) -> dict[str, Any]:
        """
        Build the smallest useful payload base for fixed-size tests.

        Standard payloads are realistic, but they can already exceed small target
        sizes such as 100 bytes. This compact base leaves room for deterministic
        padding while still preserving the device and sequence fields.
        """
        standard_payload = self._build_standard_payload(device_id, sequence_number)
        standard_size = len(self._serialize_payload(standard_payload))
        if standard_size <= self.config.payload_size_bytes:
            return standard_payload

        return {
            "device_id": device_id,
            "seq": sequence_number,
            "padding": "",
        }

    def _serialize_payload(self, payload: dict[str, Any]) -> bytes:
        """
        Serialize the payload dictionary to UTF-8 JSON bytes.

        We use compact JSON separators to avoid unnecessary spaces, which gives
        a more accurate size for benchmark payload control.
        """
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )

    def _pad_payload_to_target_size(
        self,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], bytes]:
        """
        Pad the payload until the serialized byte size reaches the configured target.

        Notes
        -----
        - Exact byte-perfect padding is harder with JSON because the added field
          also changes the serialized structure itself.
        - This method computes the current size, estimates the needed padding,
          writes a padding field, and then trims if necessary.
        """
        target_size = self.config.payload_size_bytes

        # Serialize the payload before padding.
        current_bytes = self._serialize_payload(payload)
        current_size = len(current_bytes)

        if current_size >= target_size:
            if current_size == target_size:
                return payload, current_bytes
            raise ValueError(
                "fixed_size payload target is too small for the base JSON structure "
                f"({target_size} < {current_size} bytes)"
            )

        # Estimate how many padding characters are needed.
        # We subtract a small safety margin because the JSON field itself
        # adds extra characters beyond the raw padding string.
        estimated_padding_length = max(0, target_size - current_size - 32)

        payload["padding"] = "x" * estimated_padding_length
        padded_bytes = self._serialize_payload(payload)

        # If we overshot the target, trim the padding carefully.
        if len(padded_bytes) > target_size:
            overshoot = len(padded_bytes) - target_size
            current_padding = payload["padding"]
            if overshoot < len(current_padding):
                payload["padding"] = current_padding[:-overshoot]
            else:
                payload["padding"] = ""

            padded_bytes = self._serialize_payload(payload)

        # If we are still slightly under target, add the exact missing amount.
        if len(padded_bytes) < target_size:
            missing = target_size - len(padded_bytes)
            payload["padding"] = payload.get("padding", "") + ("x" * missing)
            padded_bytes = self._serialize_payload(payload)

            # Final trim if JSON overhead pushed us over again.
            if len(padded_bytes) > target_size:
                overshoot = len(padded_bytes) - target_size
                payload["padding"] = payload["padding"][:-overshoot]
                padded_bytes = self._serialize_payload(payload)

        return payload, padded_bytes
