"""Object-store resolution for remote URI schemes.

:func:`resolve_backend` looks the scheme up in the plugin registry. The
config helpers are for store plugins, each of which reads its provider settings
from the environment and the ``[remote]`` config table.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ginkgo.config import load_runtime_config
from ginkgo.plugins.api import ObjectStoreContext, ObjectStoreType
from ginkgo.plugins.registry import plugin_registry
from ginkgo.remote.backend import ObjectStore


def resolve_backend(
    scheme: str,
    *,
    region: str | None = None,
) -> ObjectStore:
    """Return a storage backend for the given URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"s3"``, ``"oci"`` or ``"gs"``.
    region : str | None
        Region override. Falls back to environment variables or provider
        defaults.

    Returns
    -------
    ObjectStore

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides a store for the scheme.
    """
    store_type = plugin_registry().get(ObjectStoreType, scheme)
    config = load_runtime_config(project_root=Path.cwd())
    return store_type.build(ObjectStoreContext(config=config, region=region))


def config_table(mapping: Any, key: str) -> Mapping[str, Any]:
    """Return ``mapping[key]`` when it is a table, else an empty mapping."""
    value = mapping.get(key) if isinstance(mapping, Mapping) else None
    return value if isinstance(value, Mapping) else {}


def config_string(mapping: Mapping[str, Any], key: str) -> str | None:
    """Return ``mapping[key]`` as a string, or ``None`` when it is unset."""
    value = mapping.get(key)
    if value is None:
        return None
    return str(value)


def first_defined(*values: str | None) -> str | None:
    """Return the first non-empty value, so an environment variable can override config."""
    for value in values:
        if value:
            return value
    return None
