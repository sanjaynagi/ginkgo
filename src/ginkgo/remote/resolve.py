"""Object-store resolution for remote URI schemes.

:func:`resolve_backend` looks the scheme up in the plugin registry. The
``build_*_store`` functions are core's own object stores, each reading its
provider settings from the environment and the ``[remote]`` config table.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ginkgo.config import load_runtime_config
from ginkgo.plugins.api import ObjectStoreContext, ObjectStoreType
from ginkgo.plugins.registry import plugin_registry
from ginkgo.remote.backend import ObjectStore
from ginkgo.remote.fsspec_backends import (
    GCSFileSystemBackend,
    OCIFileSystemBackend,
    S3FileSystemBackend,
)


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


def build_s3_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the S3 store from ``AWS_REGION`` or ``[remote] region``."""
    remote = _table(context.config, "remote")
    return S3FileSystemBackend(
        region=context.region
        or _first_defined(
            os.environ.get("AWS_REGION"),
            os.environ.get("AWS_DEFAULT_REGION"),
            _config_string(remote, "region"),
        )
    )


def build_oci_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the OCI store from ``GINKGO_REMOTE_OCI_*``/``OCI_*`` or ``[remote.oci]``."""
    remote = _table(context.config, "remote")
    oci = _table(remote, "oci")
    return OCIFileSystemBackend(
        config_path=_first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_CONFIG"),
            os.environ.get("OCI_CONFIG_FILE"),
            _config_string(oci, "config"),
            "~/.oci/config",
        ),
        profile=_first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_PROFILE"),
            os.environ.get("OCI_CONFIG_PROFILE"),
            _config_string(oci, "profile"),
        ),
        region=context.region
        or _first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_REGION"),
            os.environ.get("OCI_REGION"),
            _config_string(oci, "region"),
            _config_string(remote, "region"),
        ),
    )


def build_gcs_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the GCS store from ``GINKGO_REMOTE_GCS_PROJECT`` or ``[remote.gcs]``.

    GCS buckets are not regional, so the context's region is unused.
    """
    gcs = _table(_table(context.config, "remote"), "gcs")
    return GCSFileSystemBackend(
        project=_first_defined(
            os.environ.get("GINKGO_REMOTE_GCS_PROJECT"),
            os.environ.get("GOOGLE_CLOUD_PROJECT"),
            _config_string(gcs, "project"),
        )
    )


def _table(mapping: Any, key: str) -> Mapping[str, Any]:
    """Return ``mapping[key]`` when it is a table, else an empty mapping."""
    value = mapping.get(key) if isinstance(mapping, Mapping) else None
    return value if isinstance(value, Mapping) else {}


def _config_string(mapping: Mapping[str, Any], key: str) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    return str(value)


def _first_defined(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None
