from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PhaseSummary:
    """
    Summary of one benchmark phase.

    Examples of phases:
    - ingress_ramp
    - egress_only
    - simultaneous
    - consume_and_process
    """

    name: str
    description: str = ""
    duration_sec: float = 0.0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the phase summary to a JSON-friendly dictionary.
        """
        return {
            "name": self.name,
            "description": self.description,
            "duration_sec": self.duration_sec,
            "notes": self.notes,
        }


@dataclass(slots=True)
class ReportModel:
    """
    Top-level report data model for one benchmark case.

    This is meant to hold the information that will later be written into:
    - final_report.json
    - final_report.md

    It keeps reporting data organized and separate from execution logic.
    """

    case_id: str
    case_name: str
    campaign_id: str | None = None
    status: str = "completed"

    # Small config snapshot or important selected config values.
    config_snapshot: dict[str, Any] = field(default_factory=dict)

    # Aggregated metrics or benchmark result dictionary.
    metrics_summary: dict[str, Any] = field(default_factory=dict)

    # Optional benchmark phases.
    phases: list[PhaseSummary] = field(default_factory=list)

    # Optional notes or observations.
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the report model to a JSON-friendly dictionary.
        """
        return {
            "case_id": self.case_id,
            "case_name": self.case_name,
            "campaign_id": self.campaign_id,
            "status": self.status,
            "config_snapshot": self.config_snapshot,
            "metrics_summary": self.metrics_summary,
            "phases": [phase.to_dict() for phase in self.phases],
            "notes": self.notes,
        }