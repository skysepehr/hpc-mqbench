from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from models.experiment_case import ExperimentCase
from src.benchmark.backends import get_backend
from src.benchmark.case_factory import build_single_case
from src.benchmark.core.config_schema import normalize_case_config


def _read_json_file(path: Path) -> dict[str, Any]:
    """
    Read and parse one JSON file.

    Parameters
    ----------
    path:
        Path to the JSON file.

    Returns
    -------
    dict[str, Any]
        Parsed top-level JSON object.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    ValueError
        If the top-level JSON value is not an object/dictionary.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("Top-level JSON config must be an object/dictionary")

    return data


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load one raw JSON configuration file.

    This is the lowest-level loader and returns the parsed dictionary
    exactly as represented in the JSON file.

    Parameters
    ----------
    path:
        Path to the JSON config file.

    Returns
    -------
    dict[str, Any]
        Raw configuration dictionary.
    """
    config_path = Path(path)
    return _read_json_file(config_path)


def load_benchmark_config(path: str | Path) -> BenchmarkConfig:
    """
    Load one JSON config file and convert it into a BenchmarkConfig object.

    Use this when you want:
    - config validation
    - normalized values
    - a strongly-typed config object
    """
    raw_config = load_config(path)
    normalized = normalize_case_config(raw_config)
    config = BenchmarkConfig(**benchmark_config_input_dict(normalized))
    get_backend(config.backend_id).validate_config(config)
    return config


def load_experiment_case(
    path: str | Path,
    case_id: str = "case_001",
    case_name: str = "single_case",
    output_dir: str | Path = "results/case_001",
) -> ExperimentCase:
    """
    Load one JSON config file and wrap it into an ExperimentCase object.

    This is the most convenient loader for single-case runs because it creates:
    - one BenchmarkConfig
    - one ExperimentCase
    - one output directory path

    Parameters
    ----------
    path:
        Path to the single-case JSON config.

    case_id:
        Unique case identifier.

    case_name:
        Human-readable case name.

    output_dir:
        Case output directory.

    Returns
    -------
    ExperimentCase
        Fully prepared single benchmark case.
    """
    raw_config = load_config(path)
    return build_single_case(
        raw_config=raw_config,
        case_id=case_id,
        case_name=case_name,
        output_dir=output_dir,
    )
