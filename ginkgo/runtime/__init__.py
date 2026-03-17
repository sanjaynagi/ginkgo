"""Execution engine internals for Ginkgo."""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "CacheStore": ("ginkgo.runtime.cache", "CacheStore"),
    "CodecError": ("ginkgo.runtime.value_codec", "CodecError"),
    "CompositeBackend": ("ginkgo.runtime.backend", "CompositeBackend"),
    "LocalBackend": ("ginkgo.runtime.backend", "LocalBackend"),
    "MISSING": ("ginkgo.runtime.cache", "MISSING"),
    "RunProvenanceRecorder": ("ginkgo.runtime.provenance", "RunProvenanceRecorder"),
    "SchedulableTask": ("ginkgo.runtime.scheduler", "SchedulableTask"),
    "TaskBackend": ("ginkgo.runtime.backend", "TaskBackend"),
    "decode_value": ("ginkgo.runtime.value_codec", "decode_value"),
    "encode_value": ("ginkgo.runtime.value_codec", "encode_value"),
    "ensure_serializable": ("ginkgo.runtime.value_codec", "ensure_serializable"),
    "evaluate": ("ginkgo.runtime.evaluator", "evaluate"),
    "hash_value_bytes": ("ginkgo.runtime.value_codec", "hash_value_bytes"),
    "latest_run_dir": ("ginkgo.runtime.provenance", "latest_run_dir"),
    "load_manifest": ("ginkgo.runtime.provenance", "load_manifest"),
    "load_module": ("ginkgo.runtime.module_loader", "load_module"),
    "load_module_from_path": ("ginkgo.runtime.module_loader", "load_module_from_path"),
    "module_name_for_path": ("ginkgo.runtime.module_loader", "module_name_for_path"),
    "run_task": ("ginkgo.runtime.worker", "run_task"),
    "select_dispatch_subset": ("ginkgo.runtime.scheduler", "select_dispatch_subset"),
    "summarise_value": ("ginkgo.runtime.value_codec", "summarise_value"),
}


def __getattr__(name: str):
    """Resolve runtime exports lazily to avoid importing optional deps eagerly."""
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    module = import_module(module_name)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return the names exposed by this package."""
    return sorted(list(globals().keys()) + list(_EXPORTS.keys()))


__all__ = sorted(_EXPORTS.keys())
