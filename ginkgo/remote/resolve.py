"""Backend resolution for remote storage schemes."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ginkgo.config import load_runtime_config
from ginkgo.remote.backend import RemoteStorageBackend
from ginkgo.remote.fsspec_backends import OCIFileSystemBackend, S3FileSystemBackend


def resolve_backend(
    scheme: str,
    *,
    region: str | None = None,
) -> RemoteStorageBackend:
    """Return a storage backend for the given URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme (``"s3"`` or ``"oci"``).
    region : str | None
        Region override.  Falls back to environment variables or provider
        defaults.

    Returns
    -------
    RemoteStorageBackend

    Raises
    ------
    ValueError
        If the scheme is unsupported.
    """
    settings = _load_remote_settings(project_root=Path.cwd())

    if scheme == "s3":
        return S3FileSystemBackend(region=region or settings.get("s3_region"))

    if scheme == "oci":
        return OCIFileSystemBackend(
            config_path=settings.get("oci_config_path"),
            profile=settings.get("oci_profile"),
            region=region or settings.get("oci_region"),
        )

    raise ValueError(f"Unsupported remote scheme: {scheme!r}")


def _load_remote_settings(*, project_root: Path) -> dict[str, str]:
    config = load_runtime_config(project_root=project_root)
    remote_config = config.get("remote", {})
    oci_config = remote_config.get("oci", {}) if isinstance(remote_config, dict) else {}

    return {
        "s3_region": _first_defined(
            os.environ.get("AWS_REGION"),
            os.environ.get("AWS_DEFAULT_REGION"),
            _config_string(remote_config, "region"),
        ),
        "oci_config_path": _first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_CONFIG"),
            os.environ.get("OCI_CONFIG_FILE"),
            _config_string(oci_config, "config"),
            "~/.oci/config",
        ),
        "oci_profile": _first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_PROFILE"),
            os.environ.get("OCI_CONFIG_PROFILE"),
            _config_string(oci_config, "profile"),
        ),
        "oci_region": _first_defined(
            os.environ.get("GINKGO_REMOTE_OCI_REGION"),
            os.environ.get("OCI_REGION"),
            _config_string(oci_config, "region"),
            _config_string(remote_config, "region"),
        ),
    }


def _config_string(mapping: Any, key: str) -> str | None:
    if not isinstance(mapping, dict):
        return None
    value = mapping.get(key)
    if value is None:
        return None
    return str(value)


def _first_defined(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None
