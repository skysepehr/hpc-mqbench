from __future__ import annotations

from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from models.experiment_case import ExperimentCase
from src.benchmark.backends import get_backend
from src.benchmark.core.config_schema import normalize_case_config


def build_single_case(
    raw_config: dict[str, Any],
    case_id: str = "case_001",
    case_name: str = "single_case",
    output_dir: str | Path = "results/case_001",
) -> ExperimentCase:
    """
    Build one normalized ExperimentCase from a raw single-case JSON object.

    The shell layer already reads a few config values, but the Python benchmark
    still needs one authoritative place that validates and attaches case metadata
    before MPI ranks start doing work.
    """
    if not isinstance(raw_config, dict):
        raise ValueError("raw_config must be a dictionary")

    normalized_config = normalize_case_config(raw_config)
    mode = str(normalized_config.get("mode", "single")).strip()
    if mode != "single":
        raise ValueError(f"build_single_case requires mode='single', got {mode!r}")

    normalized_config["mode"] = "single"

    # Keep case metadata inside the config snapshot as well as the ExperimentCase.
    normalized_config.setdefault("case_id", case_id)

    config = BenchmarkConfig(**benchmark_config_input_dict(normalized_config))
    get_backend(config.backend_id).validate_config(config)

    return ExperimentCase(
        case_id=case_id,
        case_name=case_name,
        campaign_id=config.campaign_id,
        config=config,
        output_dir=Path(output_dir),
    )
