from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from models.benchmark_config import BenchmarkConfig


@dataclass(slots=True)
class ExperimentCase:
    """
    Represents one exact benchmark case.

    A campaign can contain multiple ExperimentCase objects. A single-case run
    contains exactly one.
    """

    case_id: str
    case_name: str
    config: BenchmarkConfig
    output_dir: Path

    campaign_id: Optional[str] = None
    status: str = "pending"
    notes: dict[str, Any] = field(default_factory=dict)

    def ensure_output_dir(self) -> None:
        """
        Create the output directory if it does not already exist.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the case into a plain dictionary for serialization or reporting.
        """
        return {
            "case_id": self.case_id,
            "case_name": self.case_name,
            "campaign_id": self.campaign_id,
            "status": self.status,
            "output_dir": self._portable_output_dir(),
            "config": self.config.to_dict(),
            "notes": self.notes,
        }

    def _portable_output_dir(self) -> str:
        """
        Prefer project-relative output paths when possible.

        Local and Slurm runners execute from the repository root, so this keeps
        generated reports useful after copying them to another machine. External
        output directories remain absolute because there is no safe relative
        anchor for them.
        """
        try:
            return self.output_dir.resolve().relative_to(Path.cwd().resolve()).as_posix()
        except ValueError:
            return str(self.output_dir)
