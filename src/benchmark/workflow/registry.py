from __future__ import annotations

from src.benchmark.workflow.base import BackendWorkflow


_WORKFLOWS: dict[str, BackendWorkflow] = {}
_BUILTINS_LOADED = False


def register_workflow(workflow: BackendWorkflow, *, replace: bool = False) -> None:
    backend_id = str(workflow.backend_id).strip()
    if not backend_id:
        raise ValueError("Workflow backend_id must not be empty")
    if backend_id in _WORKFLOWS and not replace:
        raise ValueError(f"Workflow already registered: {backend_id}")
    _WORKFLOWS[backend_id] = workflow


def get_workflow(backend_id: str) -> BackendWorkflow:
    _load_builtins()
    normalized = str(backend_id).strip()
    try:
        return _WORKFLOWS[normalized]
    except KeyError as exc:
        available = ", ".join(sorted(_WORKFLOWS)) or "none"
        raise ValueError(
            f"No reproducible workflow for {normalized!r}; available: {available}"
        ) from exc


def list_workflows() -> tuple[str, ...]:
    _load_builtins()
    return tuple(sorted(_WORKFLOWS))


def _load_builtins() -> None:
    global _BUILTINS_LOADED
    if _BUILTINS_LOADED:
        return
    from src.benchmark.backends.kafka.workflow import KafkaReproducibleWorkflow
    from src.benchmark.backends.pulsar.workflow import PulsarReproducibleWorkflow

    register_workflow(KafkaReproducibleWorkflow())
    register_workflow(PulsarReproducibleWorkflow())
    _BUILTINS_LOADED = True
