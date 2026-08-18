from __future__ import annotations

import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def ensure_repo_local_dependencies() -> None:
    """
    Make repo-local Python/native dependencies visible to benchmark modules.

    This is intentionally lightweight and safe to call before optional imports.
    It lets local runs use packages built under .local/ without requiring a user
    shell profile edit or a global Python package installation.
    """
    if os.environ.get("KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH", "0") == "1":
        return

    for path in reversed(repo_local_python_paths(PROJECT_ROOT)):
        _prepend_path(path)
    _preload_librdkafka(PROJECT_ROOT / ".local" / "librdkafka" / "lib")
    _set_matplotlib_cache(PROJECT_ROOT / ".local" / "matplotlib")


def repo_local_python_paths(project_root: Path = PROJECT_ROOT) -> list[Path]:
    """
    Return repo-local Python paths in runtime precedence order.

    The compute-node path is only added when it appears to contain mpi4py. That
    lets Slurm runs prefer a compute-built MPI binding while local development
    keeps using the shared .local/python path.
    """
    if os.environ.get("KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH", "0") == "1":
        return []

    paths: list[Path] = []
    hpc_python = project_root / ".local" / "python-hpc"
    if (hpc_python / "mpi4py").is_dir() or (hpc_python / "mpi4py.py").is_file():
        paths.append(hpc_python)

    shared_python = project_root / ".local" / "python"
    if shared_python.is_dir():
        paths.append(shared_python)

    return paths


def _prepend_path(path: Path) -> None:
    if not path.is_dir():
        return

    resolved = str(path.resolve())
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


def _preload_librdkafka(lib_dir: Path) -> None:
    library = lib_dir / "librdkafka.so"
    if not library.is_file():
        library = lib_dir / "librdkafka.so.1"
    if not library.is_file():
        return

    try:
        import ctypes

        ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
    except Exception:
        # Importers will raise a clearer dependency error if this preload fails.
        return


def _set_matplotlib_cache(cache_dir: Path) -> None:
    """
    Keep matplotlib cache/config files inside the repository-local runtime area.
    """
    if os.environ.get("MPLCONFIGDIR"):
        return

    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        return

    os.environ["MPLCONFIGDIR"] = str(cache_dir)
