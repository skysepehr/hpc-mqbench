from __future__ import annotations

from src.benchmark.backends.base import BackendAdapter


_BACKENDS: dict[str, BackendAdapter] = {}
_BUILTINS_LOADED = False


def register_backend(adapter: BackendAdapter, *, replace: bool = False) -> None:
    backend_id = str(adapter.backend_id).strip()
    if not backend_id:
        raise ValueError("Backend adapter backend_id must not be empty")
    if backend_id in _BACKENDS and not replace:
        raise ValueError(f"Backend adapter already registered: {backend_id}")
    _BACKENDS[backend_id] = adapter


def get_backend(backend_id: str) -> BackendAdapter:
    _load_builtins()
    normalized = str(backend_id).strip()
    try:
        return _BACKENDS[normalized]
    except KeyError as exc:
        available = ", ".join(sorted(_BACKENDS)) or "none"
        raise ValueError(
            f"Unknown backend_id {normalized!r}; available backends: {available}"
        ) from exc


def list_backends() -> tuple[str, ...]:
    _load_builtins()
    return tuple(sorted(_BACKENDS))


def _load_builtins() -> None:
    global _BUILTINS_LOADED
    if _BUILTINS_LOADED:
        return
    from src.benchmark.backends.kafka.adapter import KafkaBackendAdapter
    from src.benchmark.backends.pulsar.adapter import PulsarBackendAdapter

    register_backend(KafkaBackendAdapter())
    register_backend(PulsarBackendAdapter())
    _BUILTINS_LOADED = True
