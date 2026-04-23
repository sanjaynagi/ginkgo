"""Loader registry for semantic asset artifacts.

Each asset kind (``table``, ``array``, ``fig``, ``text``, ``model``)
registers a loader in :data:`~ginkgo.runtime.artifacts.asset_kinds.ASSET_KINDS`
that rehydrates the stored bytes back into a live Python value. Callers
(the CLI ``asset show`` path, the evaluator's ``_rehydrate_wrapped_refs``
pass, any future programmatic reader) dispatch through :func:`load_from_ref`
or :func:`load_from_version`.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

from ginkgo.core.asset import AssetRef, AssetVersion
from ginkgo.runtime.artifacts.artifact_store import ArtifactStore


def load_from_ref(
    *,
    artifact_store: ArtifactStore,
    asset_ref: AssetRef,
) -> Any:
    """Rehydrate an asset reference into a live Python value.

    Parameters
    ----------
    artifact_store : ArtifactStore
        Store used to fetch the immutable artifact bytes.
    asset_ref : AssetRef
        Resolved reference produced by the asset registrar.

    Returns
    -------
    Any
        Kind-specific materialised value.
    """
    return _dispatch(
        kind=asset_ref.kind,
        artifact_store=artifact_store,
        artifact_id=asset_ref.artifact_id,
        metadata=dict(asset_ref.metadata),
    )


def load_from_version(
    *,
    artifact_store: ArtifactStore,
    version: AssetVersion,
) -> Any:
    """Rehydrate an asset version into a live Python value."""
    return _dispatch(
        kind=version.key.namespace,
        artifact_store=artifact_store,
        artifact_id=version.artifact_id,
        metadata=dict(version.metadata),
    )


def _dispatch(
    *,
    kind: str,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    """Dispatch to the per-kind loader registered in the kind registry."""
    # Late import breaks the cycle asset_kinds -> asset_loaders.
    from ginkgo.runtime.artifacts.asset_kinds import get_kind_spec

    spec = get_kind_spec(kind)
    if spec.loader is None:
        raise ValueError(f"no loader registered for asset kind {kind!r}")
    return spec.loader(
        artifact_store=artifact_store,
        artifact_id=artifact_id,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Per-kind loaders
# ---------------------------------------------------------------------------


def load_table_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any] | None = None,  # noqa: ARG001 — signature symmetry
) -> Any:
    """Load a ``table`` asset as a pandas DataFrame."""
    import pandas as pd

    data = artifact_store.read_bytes(artifact_id=artifact_id)
    return pd.read_parquet(io.BytesIO(data))


def load_array_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    """Load an ``array`` asset as a numpy array.

    Returns a numpy array for both ``.npy`` fallback blobs and zipped
    zarr stores. Callers that need the live zarr handle can re-open the
    store themselves via :meth:`ArtifactStore.artifact_path`.
    """
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


def load_fig_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> tuple[bytes, str]:
    """Load a ``fig`` asset as ``(bytes, source_format)``."""
    data = artifact_store.read_bytes(artifact_id=artifact_id)
    source_format = str(metadata.get("source_format", "png"))
    return data, source_format


def load_text_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any] | None = None,  # noqa: ARG001 — signature symmetry
) -> str:
    """Load a ``text`` asset as a decoded UTF-8 string."""
    data = artifact_store.read_bytes(artifact_id=artifact_id)
    return data.decode("utf-8")


def load_model_bytes(
    *,
    artifact_store: ArtifactStore,
    artifact_id: str,
    metadata: dict[str, Any],
) -> Any:
    """Load a ``model`` asset as the original trained-model object."""
    import tempfile as _tempfile

    sub_kind = metadata.get("sub_kind")
    data = artifact_store.read_bytes(artifact_id=artifact_id)

    if sub_kind in {"sklearn", "xgboost", "lightgbm"}:
        try:
            import joblib  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                f"Loading a model asset with framework={sub_kind!r} requires the 'joblib' package."
            ) from exc

        return joblib.load(io.BytesIO(data))

    if sub_kind == "pytorch":
        try:
            import torch  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "Loading a model asset with framework='pytorch' requires the 'torch' package."
            ) from exc

        # ``weights_only=False`` is required for full-object reloads, which
        # is the symmetry we chose with sklearn/keras. The user accepts the
        # pickle contract at save time.
        return torch.load(io.BytesIO(data), weights_only=False)

    if sub_kind == "keras":
        try:
            import keras  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "Loading a model asset with framework='keras' requires the 'keras' package."
            ) from exc

        with _tempfile.NamedTemporaryFile(suffix=".keras", delete=False) as handle:
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


def artifact_path_for(*, artifact_store: ArtifactStore, version: AssetVersion) -> Path:
    """Return the absolute filesystem path for an asset version's artifact."""
    return artifact_store.artifact_path(artifact_id=version.artifact_id)
