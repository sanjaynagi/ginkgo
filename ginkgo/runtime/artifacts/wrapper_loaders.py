"""Loader registry for wrapped asset artifacts.

Each wrapped asset kind (``table``, ``array``, ``fig``, ``text``) has a
dedicated loader that rehydrates the stored artifact bytes into a Python
value. Callers (the CLI ``asset show`` path, future programmatic readers)
use the :func:`load` dispatcher keyed on asset namespace.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from ginkgo.core.asset import AssetVersion
from ginkgo.runtime.artifacts.artifact_store import ArtifactStore


def load(
    *,
    artifact_store: ArtifactStore,
    version: AssetVersion,
) -> Any:
    """Rehydrate a wrapped asset version into a live Python value.

    Parameters
    ----------
    artifact_store : ArtifactStore
        Store used to fetch the immutable artifact bytes.
    version : AssetVersion
        Version record to rehydrate.

    Returns
    -------
    Any
        Kind-specific materialised value (pandas DataFrame, numpy array,
        raw bytes plus format for figures, or the decoded string for text).
    """
    namespace = version.key.namespace
    if namespace == "table":
        return load_table(artifact_store=artifact_store, version=version)
    if namespace == "array":
        return load_array(artifact_store=artifact_store, version=version)
    if namespace == "fig":
        return load_fig(artifact_store=artifact_store, version=version)
    if namespace == "text":
        return load_text(artifact_store=artifact_store, version=version)
    raise ValueError(f"no loader registered for asset namespace {namespace!r}")


def load_table(*, artifact_store: ArtifactStore, version: AssetVersion) -> Any:
    """Load a ``table`` asset version as a pandas DataFrame."""
    import pandas as pd

    data = artifact_store.read_bytes(artifact_id=version.artifact_id)
    return pd.read_parquet(io.BytesIO(data))


def load_array(*, artifact_store: ArtifactStore, version: AssetVersion) -> Any:
    """Load an ``array`` asset version as a numpy array.

    Returns a numpy array for both ``.npy`` fallback blobs and zipped zarr
    stores. Callers that need the live zarr handle can re-open the store
    themselves via :meth:`ArtifactStore.artifact_path`.
    """
    import numpy as np

    data = artifact_store.read_bytes(artifact_id=version.artifact_id)
    sub_kind = version.metadata.get("sub_kind")

    # Heuristic: ``.npy`` blobs are the only non-zarr path we write.
    # Try numpy first when the stored form is numpy, otherwise open via zarr.
    if sub_kind == "numpy" and _looks_like_npy(data):
        return np.load(io.BytesIO(data), allow_pickle=False)

    import zarr  # type: ignore[import-not-found]

    path = artifact_store.artifact_path(artifact_id=version.artifact_id)
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        root = zarr.open(store, mode="r")
        return np.asarray(root[...]) if hasattr(root, "shape") else np.asarray(root)
    finally:
        store.close()


def load_fig(*, artifact_store: ArtifactStore, version: AssetVersion) -> tuple[bytes, str]:
    """Load a ``fig`` asset version as ``(bytes, source_format)``."""
    data = artifact_store.read_bytes(artifact_id=version.artifact_id)
    source_format = str(version.metadata.get("source_format", "png"))
    return data, source_format


def load_text(*, artifact_store: ArtifactStore, version: AssetVersion) -> str:
    """Load a ``text`` asset version as a decoded UTF-8 string."""
    data = artifact_store.read_bytes(artifact_id=version.artifact_id)
    return data.decode("utf-8")


def _looks_like_npy(data: bytes) -> bool:
    """Return whether *data* starts with the ``.npy`` magic marker."""
    return len(data) >= 6 and data[:6] == b"\x93NUMPY"


# Kept for callers that prefer Path-based access.
def artifact_path_for(*, artifact_store: ArtifactStore, version: AssetVersion) -> Path:
    """Return the absolute filesystem path for an asset version's artifact."""
    return artifact_store.artifact_path(artifact_id=version.artifact_id)
