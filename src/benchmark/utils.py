from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_dir(path: str | Path) -> Path:
    """
    Ensure that a directory exists and return it as a Path object.

    Parameters
    ----------
    path:
        Directory path as string or Path.

    Returns
    -------
    Path
        The normalized Path object for the directory.
    """
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_json_file(path: str | Path, data: dict[str, Any]) -> None:
    """
    Write a dictionary to a JSON file with readable formatting.

    Parameters
    ----------
    path:
        Output file path.

    data:
        Dictionary to serialize.
    """
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


def read_json_file(path: str | Path) -> dict[str, Any]:
    """
    Read a JSON file and return the parsed dictionary.

    Parameters
    ----------
    path:
        Input file path.

    Returns
    -------
    dict[str, Any]
        Parsed JSON data.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.

    ValueError
        If the top-level JSON object is not a dictionary.
    """
    input_path = Path(path)

    if not input_path.exists():
        raise FileNotFoundError(f"JSON file not found: {input_path}")

    with input_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("Expected top-level JSON object to be a dictionary")

    return data


def write_text_file(path: str | Path, text: str) -> None:
    """
    Write text to a file, creating parent directories when needed.

    Parameters
    ----------
    path:
        Output file path.

    text:
        Text content to write.
    """
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(text)