"""Backend adapter registry and contracts."""

from src.benchmark.backends.base import BackendAdapter
from src.benchmark.backends.registry import (
    get_backend,
    list_backends,
    register_backend,
)

__all__ = [
    "BackendAdapter",
    "get_backend",
    "list_backends",
    "register_backend",
]
