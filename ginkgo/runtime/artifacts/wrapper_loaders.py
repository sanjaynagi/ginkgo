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

from ginkgo.core.asset import AssetRef, AssetVersion
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
    return _dispatch(
        namespace=version.key.namespace,
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
        metadata=dict(version.metadata),
    )


def load_from_ref(
    *,
    artifact_store: ArtifactStore,
    asset_ref: AssetRef,
) -> Any:
    """Rehydrate a wrapped asset reference into a live Python value.

    Parameters
    ----------
    artifact_store : ArtifactStore
        Store used to fetch the immutable artifact bytes.
    asset_ref : AssetRef
        Resolved reference produced by the asset registrar.

    Returns
    -------
    Any
        Kind-specific materialised value. See :func:`load` for the
        per-namespace return types.
    """
    return _dispatch(
        namespace=asset_ref.kind,
        artifact_store=artifact_store,
        artifact_id=asset_ref.artifact_id,
        metadata=dict(asset_ref.metadata),
    )


def _dispatch(
    *,
    namespace: str,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    """Dispatch to the per-kind loader by namespace."""
    if namespace == "table":
        return _load_table_bytes(artifact_store=artifact_store, artifact_id=artifact_id)
    if namespace == "array":
        return _load_array_bytes(
            artifact_store=artifact_store,
            artifact_id=artifact_id,
            metadata=metadata,
        )
    if namespace == "fig":
        return _load_fig_bytes(
            artifact_store=artifact_store,
            artifact_id=artifact_id,
            metadata=metadata,
        )
    if namespace == "text":
        return _load_text_bytes(artifact_store=artifact_store, artifact_id=artifact_id)
    if namespace == "model":
        return _load_model_bytes(
            artifact_store=artifact_store,
            artifact_id=artifact_id,
            metadata=metadata,
        )
    raise ValueError(f"no loader registered for asset namespace {namespace!r}")


def load_table(*, artifact_store: ArtifactStore, version: AssetVersion) -> Any:
    """Load a ``table`` asset version as a pandas DataFrame."""
    return _load_table_bytes(artifact_store=artifact_store, artifact_id=version.artifact_id)


def load_array(*, artifact_store: ArtifactStore, version: AssetVersion) -> Any:
    """Load an ``array`` asset version as a numpy array.

    Returns a numpy array for both ``.npy`` fallback blobs and zipped zarr
    stores. Callers that need the live zarr handle can re-open the store
    themselves via :meth:`ArtifactStore.artifact_path`.
    """
    return _load_array_bytes(
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
        metadata=dict(version.metadata),
    )


def load_fig(*, artifact_store: ArtifactStore, version: AssetVersion) -> tuple[bytes, str]:
    """Load a ``fig`` asset version as ``(bytes, source_format)``."""
    return _load_fig_bytes(
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
        metadata=dict(version.metadata),
    )


def load_text(*, artifact_store: ArtifactStore, version: AssetVersion) -> str:
    """Load a ``text`` asset version as a decoded UTF-8 string."""
    return _load_text_bytes(artifact_store=artifact_store, artifact_id=version.artifact_id)


def load_model(*, artifact_store: ArtifactStore, version: AssetVersion) -> Any:
    """Load a ``model`` asset version as the original trained-model object."""
    return _load_model_bytes(
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
        metadata=dict(version.metadata),
    )


def _load_table_bytes(*, artifact_store: ArtifactStore, artifact_id: str) -> Any:
    import pandas as pd

    data = artifact_store.read_bytes(artifact_id=artifact_id)
    return pd.read_parquet(io.BytesIO(data))


def _load_array_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    import numpy as np

    data = artifact_store.read_bytes(artifact_id=artifact_id)
    sub_kind = metadata.get("sub_kind")

    if sub_kind == "numpy" and _looks_like_npy(data):
        return np.load(io.BytesIO(data), allow_pickle=False)

    import zarr  # type: ignore[import-not-found]

    path = artifact_store.artifact_path(artifact_id=artifact_id)
    store = zarr.storage.ZipStore(str(path), mode="r")
    try:
        root = zarr.open(store, mode="r")
        return np.asarray(root[...]) if hasattr(root, "shape") else np.asarray(root)
    finally:
        store.close()


def _load_fig_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> tuple[bytes, str]:
    data = artifact_store.read_bytes(artifact_id=artifact_id)
    source_format = str(metadata.get("source_format", "png"))
    return data, source_format


def _load_text_bytes(*, artifact_store: ArtifactStore, artifact_id: str) -> str:
    data = artifact_store.read_bytes(artifact_id=artifact_id)
    return data.decode("utf-8")


def _load_model_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    sub_kind = metadata.get("sub_kind")
    data = artifact_store.read_bytes(artifact_id=artifact_id)

    if sub_kind in {"sklearn", "xgboost", "lightgbm"}:
        import joblib  # type: ignore[import-not-found]

        return joblib.load(io.BytesIO(data))

    if sub_kind == "pytorch":
        import torch  # type: ignore[import-not-found]

        # ``weights_only=False`` is required for full-object reloads, which
        # is the symmetry we chose with sklearn/keras. The user accepts the
        # pickle contract at save time.
        return torch.load(io.BytesIO(data), weights_only=False)

    if sub_kind == "keras":
        import tempfile

        import keras  # type: ignore[import-not-found]

        with tempfile.NamedTemporaryFile(suffix=".keras", delete=False) as handle:
            handle.write(data)
            tmp_path = Path(handle.name)
        try:
            return keras.models.load_model(str(tmp_path))
        finally:
            tmp_path.unlink(missing_ok=True)

    raise ValueError(f"unknown model sub_kind {sub_kind!r}")


def _looks_like_npy(data: bytes) -> bool:
    """Return whether *data* starts with the ``.npy`` magic marker."""
    return len(data) >= 6 and data[:6] == b"\x93NUMPY"


# Kept for callers that prefer Path-based access.
def artifact_path_for(*, artifact_store: ArtifactStore, version: AssetVersion) -> Path:
    """Return the absolute filesystem path for an asset version's artifact."""
    return artifact_store.artifact_path(artifact_id=version.artifact_id)
