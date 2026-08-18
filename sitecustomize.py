import os
import sys
from pathlib import Path
from typing import List


def _prepend_existing_paths(paths: List[Path]) -> None:
    """
    Put repo-local Python dependency directories at the front of sys.path.

    Paths are provided in desired precedence order. Existing entries are removed
    by resolved path first so a symlinked project path such as /user/... cannot
    leave the same directory earlier in sys.path as /mnt/....
    """
    for path in reversed(paths):
        if not path.is_dir():
            continue

        resolved_path = path.resolve()
        resolved = str(resolved_path)
        for existing in list(sys.path):
            if not existing:
                continue
            try:
                if Path(existing).resolve() == resolved_path:
                    sys.path.remove(existing)
            except OSError:
                continue

        sys.path.insert(0, resolved)


if os.environ.get("KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH", "0") != "1":
    PROJECT_ROOT = Path(__file__).resolve().parent
    python_paths = []  # type: List[Path]

    for extra_path in os.environ.get("KAFKA_HPC_EXTRA_PYTHONPATH", "").split(os.pathsep):
        if extra_path:
            python_paths.append(Path(extra_path))

    compute_python = PROJECT_ROOT / ".local" / "python-hpc"
    if (compute_python / "mpi4py").is_dir() or (compute_python / "mpi4py.py").is_file():
        python_paths.append(compute_python)

    python_paths.append(PROJECT_ROOT / ".local" / "python")
    _prepend_existing_paths(python_paths)

    # Preload repo-local librdkafka when it exists. This helps the compiled
    # confluent_kafka extension resolve librdkafka without requiring a global
    # system package or a user shell profile edit.
    local_librdkafka = PROJECT_ROOT / ".local" / "librdkafka" / "lib" / "librdkafka.so"
    if local_librdkafka.is_file():
        try:
            import ctypes

            ctypes.CDLL(str(local_librdkafka), mode=ctypes.RTLD_GLOBAL)
        except Exception:
            pass
