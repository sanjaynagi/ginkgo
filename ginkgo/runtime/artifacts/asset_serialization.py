"""Serialisation backends for semantic asset kinds.

Each kind's ``serialize_*`` returns a :class:`SerializedAsset` with the
raw bytes, file extension, and kind-specific metadata. Optional backends
(polars, pyarrow, duckdb, xarray, zarr, dask, matplotlib, plotly, bokeh,
joblib, torch, keras) are imported lazily so tasks without the relevant
dependency simply cannot produce that sub-kind.

The asset registrar drives dispatch through :data:`ASSET_KINDS` rather
than calling these functions directly.
"""

from __future__ import annotations

import io
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.core.asset import AssetResult


class AssetSerializationError(RuntimeError):
    """Raised when an :class:`AssetResult` payload cannot be serialised.

    Parameters
    ----------
    kind : str
        The asset kind (``table`` / ``array`` / ``fig`` / ``text`` /
        ``model``).
    index : int
        Per-kind positional index of the offending result.
    name : str | None
        Explicit name, if the result had one.
    message : str
        Underlying error message.
    """

    def __init__(
        self,
        *,
        kind: str,
        index: int,
        name: str | None,
        message: str,
    ) -> None:
        identifier = f"name={name!r}" if name else f"index={index}"
        super().__init__(f"failed to serialise {kind} asset ({identifier}): {message}")
        self.kind = kind
        self.index = index
        self.name = name


@dataclass(kw_only=True)
class SerializedAsset:
    """Output of one asset serialisation pass.

    Parameters
    ----------
    data : bytes
        Raw bytes written to the artifact store for the main payload.
    extension : str
        File extension (without leading dot) used when storing bytes.
    metadata : dict[str, Any]
        Kind-specific metadata persisted on the asset version.
    """

    data: bytes
    extension: str
    metadata: dict[str, Any] = field(default_factory=dict)


def serialize_asset(
    *,
    result: AssetResult,
    index: int,
) -> SerializedAsset:
    """Serialise one :class:`AssetResult` via the kind registry.

    Parameters
    ----------
    result : AssetResult
        Sentinel returned by a task body (non-``file`` kind).
    index : int
        Per-kind positional index of the result (used in error
        messages).

    Returns
    -------
    SerializedAsset
    """
    # Late import breaks the cycle asset_kinds -> asset_serialization.
    from ginkgo.runtime.artifacts.asset_kinds import get_kind_spec

    spec = get_kind_spec(result.kind)
    if spec.serializer is None:
        raise AssetSerializationError(
            kind=result.kind,
            index=index,
            name=result.name,
            message=f"kind {result.kind!r} has no registered serializer",
        )

    try:
        return spec.serializer(result)
    except AssetSerializationError:
        raise
    except Exception as exc:
        raise AssetSerializationError(
            kind=result.kind,
            index=index,
            name=result.name,
            message=f"{type(exc).__name__}: {exc}",
        ) from exc


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------


def serialize_table(result: AssetResult) -> SerializedAsset:
    """Materialise a table asset to Parquet bytes."""
    frame = _materialise_table_to_pandas(result)

    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    parquet_bytes = buffer.getvalue()

    schema = [
        {"name": str(col), "dtype": str(dtype)}
        for col, dtype in zip(frame.columns, frame.dtypes, strict=True)
    ]
    metadata: dict[str, Any] = {
        "sub_kind": result.sub_kind,
        "schema": schema,
        "row_count": int(len(frame.index)),
        "byte_size": len(parquet_bytes),
    }
    metadata.update(result.metadata)
    return SerializedAsset(
        data=parquet_bytes,
        extension="parquet",
        metadata=metadata,
    )


def _materialise_table_to_pandas(result: AssetResult) -> Any:
    """Convert any supported table payload to a pandas DataFrame."""
    import pandas as pd

    payload = result.payload
    sub_kind = result.sub_kind

    if sub_kind == "pandas":
        return payload

    if sub_kind == "polars":
        if hasattr(payload, "collect"):
            payload = payload.collect()
        return payload.to_pandas()

    if sub_kind == "pyarrow":
        if hasattr(payload, "to_table"):
            payload = payload.to_table()
        return payload.to_pandas()

    if sub_kind == "duckdb":
        return payload.df()

    if sub_kind in {"csv", "tsv"}:
        sep = "," if sub_kind == "csv" else "\t"
        return pd.read_csv(Path(str(payload)), sep=sep)

    raise ValueError(f"unknown table sub_kind {sub_kind!r}")


# ---------------------------------------------------------------------------
# Array
# ---------------------------------------------------------------------------


def serialize_array(result: AssetResult) -> SerializedAsset:
    """Materialise an array asset into a zarr zip store or ``.npy`` blob.

    When the ``zarr`` package is installed the array is written to a
    zipped zarr store (preserving chunking for dask/xarray/zarr
    payloads). When ``zarr`` is unavailable, numpy arrays fall back to
    ``.npy`` and non-numpy backends raise a structured error asking the
    user to install ``zarr``.
    """
    try:
        import zarr  # type: ignore[import-not-found]

        zarr_available = True
    except ImportError:
        zarr = None  # type: ignore[assignment]
        zarr_available = False

    sub_kind = result.sub_kind
    if not zarr_available and sub_kind != "numpy":
        raise AssetSerializationError(
            kind="array",
            index=-1,
            name=result.name,
            message=(
                f"array({sub_kind}) requires the 'zarr' package. "
                "Install with 'pip install zarr' or 'pixi add zarr'."
            ),
        )

    if not zarr_available:
        return _serialize_numpy_to_npy(result)

    return _serialize_array_to_zarr(result=result, zarr_module=zarr)


def _serialize_numpy_to_npy(result: AssetResult) -> SerializedAsset:
    """Fallback path: write a numpy array as a ``.npy`` blob."""
    import numpy as np

    payload = result.payload
    if not isinstance(payload, np.ndarray):
        raise TypeError(f"numpy fallback cannot handle payload of type {type(payload).__name__}")

    buffer = io.BytesIO()
    np.save(buffer, payload, allow_pickle=False)
    data = buffer.getvalue()

    metadata: dict[str, Any] = {
        "sub_kind": result.sub_kind,
        "shape": list(payload.shape),
        "dtype": str(payload.dtype),
        "chunks": None,
        "coordinates": None,
        "byte_size": len(data),
    }
    metadata.update(result.metadata)
    return SerializedAsset(data=data, extension="npy", metadata=metadata)


def _serialize_array_to_zarr(*, result: AssetResult, zarr_module: Any) -> SerializedAsset:
    """Write any supported array payload to a zipped zarr store."""
    shape, dtype_str, chunks, coordinates = _array_properties(result)

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "array.zarr.zip"
        store = zarr_module.storage.ZipStore(str(zip_path), mode="w")
        try:
            _write_payload_to_zarr_store(result=result, store=store, zarr_module=zarr_module)
        finally:
            store.close()
        data = zip_path.read_bytes()

    metadata: dict[str, Any] = {
        "sub_kind": result.sub_kind,
        "shape": shape,
        "dtype": dtype_str,
        "chunks": chunks,
        "coordinates": coordinates,
        "byte_size": len(data),
    }
    metadata.update(result.metadata)
    return SerializedAsset(data=data, extension="zarr.zip", metadata=metadata)


def _array_properties(
    result: AssetResult,
) -> tuple[list[int], str, list[int] | None, dict[str, list[str]] | None]:
    """Return ``(shape, dtype, chunks, coordinates)`` for an array payload.

    Computed without triggering dask computation when possible.
    """
    import numpy as np

    payload = result.payload
    sub_kind = result.sub_kind

    if sub_kind == "numpy":
        return list(payload.shape), str(payload.dtype), None, None

    if sub_kind == "dask":
        chunks_meta = getattr(payload, "chunks", None)
        chunks_summary: list[int] | None = None
        if chunks_meta:
            chunks_summary = [int(c[0]) for c in chunks_meta]
        return list(payload.shape), str(payload.dtype), chunks_summary, None

    if sub_kind == "xarray":
        data_obj = payload
        if hasattr(data_obj, "to_array") and not hasattr(data_obj, "values"):
            data_obj = data_obj.to_array()
        coords: dict[str, list[str]] = {}
        for name, coord in getattr(data_obj, "coords", {}).items():
            try:
                sample = [str(v) for v in list(np.asarray(coord.values))[:8]]
            except Exception:
                sample = []
            coords[str(name)] = sample
        shape = list(data_obj.shape)
        dtype_str = str(data_obj.dtype)
        chunks_attr = getattr(data_obj, "chunks", None)
        chunks_summary = None
        if chunks_attr:
            try:
                chunks_summary = [int(c[0]) for c in chunks_attr]
            except (TypeError, IndexError):
                chunks_summary = None
        return shape, dtype_str, chunks_summary, coords or None

    if sub_kind == "zarr":
        chunks_attr = getattr(payload, "chunks", None)
        chunks_summary = list(chunks_attr) if chunks_attr else None
        return list(payload.shape), str(payload.dtype), chunks_summary, None

    raise ValueError(f"unknown array sub_kind {sub_kind!r}")


def _write_payload_to_zarr_store(*, result: AssetResult, store: Any, zarr_module: Any) -> None:
    """Materialise an array payload into the given zarr store."""
    import numpy as np

    payload = result.payload
    sub_kind = result.sub_kind

    if sub_kind == "zarr":
        zarr_module.copy_store(payload.store, store) if hasattr(
            zarr_module, "copy_store"
        ) else _copy_zarr_via_array(source=payload, store=store, zarr_module=zarr_module)
        return

    if sub_kind == "dask":
        payload.to_zarr(store, component="array", overwrite=True)
        return

    if sub_kind == "xarray":
        payload.to_zarr(store=store, mode="w")
        return

    if sub_kind == "numpy":
        arr = np.asarray(payload)
        z = zarr_module.open(store, mode="w", shape=arr.shape, dtype=arr.dtype)
        z[...] = arr
        return

    raise ValueError(f"unknown array sub_kind {sub_kind!r}")


def _copy_zarr_via_array(*, source: Any, store: Any, zarr_module: Any) -> None:
    """Fallback passthrough when ``zarr.copy_store`` is unavailable."""
    import numpy as np

    arr = np.asarray(source[...])
    chunks = getattr(source, "chunks", None)
    z = zarr_module.open(
        store,
        mode="w",
        shape=arr.shape,
        dtype=arr.dtype,
        chunks=chunks,
    )
    z[...] = arr


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------


def serialize_fig(result: AssetResult) -> SerializedAsset:
    """Serialise a figure asset to its native format."""
    sub_kind = result.sub_kind
    payload = result.payload

    if sub_kind in {"png", "svg", "html"}:
        source_path = Path(str(payload))
        if not source_path.is_file():
            raise FileNotFoundError(f"figure path does not exist: {source_path}")
        data = source_path.read_bytes()
        extension = sub_kind
    elif sub_kind == "matplotlib":
        buffer = io.BytesIO()
        payload.savefig(buffer, format="png", bbox_inches="tight")
        data = buffer.getvalue()
        extension = "png"
    elif sub_kind == "plotly":
        html = payload.to_html(include_plotlyjs="cdn", full_html=True)
        data = html.encode("utf-8")
        extension = "html"
    elif sub_kind == "bokeh":
        from bokeh.embed import file_html  # type: ignore[import-not-found]
        from bokeh.resources import CDN  # type: ignore[import-not-found]

        html = file_html(payload, CDN, "ginkgo-figure")
        data = html.encode("utf-8")
        extension = "html"
    else:
        raise ValueError(f"unknown fig sub_kind {sub_kind!r}")

    dimensions: dict[str, int] | None = None
    if extension == "png":
        width, height = _png_dimensions(data)
        if width and height:
            dimensions = {"width": width, "height": height}

    metadata: dict[str, Any] = {
        "sub_kind": sub_kind,
        "source_format": extension,
        "byte_size": len(data),
        "dimensions": dimensions,
    }
    metadata.update(result.metadata)
    return SerializedAsset(data=data, extension=extension, metadata=metadata)


def _png_dimensions(data: bytes) -> tuple[int | None, int | None]:
    """Return ``(width, height)`` for a PNG blob, or ``(None, None)``."""
    if len(data) < 24 or data[:8] != b"\x89PNG\r\n\x1a\n":
        return None, None
    width = int.from_bytes(data[16:20], "big")
    height = int.from_bytes(data[20:24], "big")
    return width, height


# ---------------------------------------------------------------------------
# Text
# ---------------------------------------------------------------------------


_TEXT_EXTENSION_BY_FORMAT = {"plain": "txt", "markdown": "md", "json": "json"}


def serialize_text(result: AssetResult) -> SerializedAsset:
    """Serialise a text asset as raw UTF-8 bytes."""
    payload = result.payload
    text_format = str(result.kind_fields.get("format", "plain"))

    if isinstance(payload, Path):
        body = payload.read_text(encoding="utf-8")
    else:
        # Factory normalised dicts into JSON strings; str flows through
        # directly without re-probing the filesystem.
        body = str(payload)

    data = body.encode("utf-8")

    if not body:
        line_count = 0
    elif body.endswith("\n"):
        line_count = body.count("\n")
    else:
        line_count = body.count("\n") + 1

    metadata: dict[str, Any] = {
        "sub_kind": result.sub_kind,
        "format": text_format,
        "byte_size": len(data),
        "line_count": line_count,
    }
    metadata.update(result.metadata)
    return SerializedAsset(
        data=data,
        extension=_TEXT_EXTENSION_BY_FORMAT[text_format],
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


_MODEL_EXTENSION_BY_SUB_KIND = {
    "sklearn": "joblib",
    "xgboost": "joblib",
    "lightgbm": "joblib",
    "pytorch": "pt",
    "keras": "keras",
}


def serialize_model(result: AssetResult) -> SerializedAsset:
    """Serialise a trained-model payload as a full-object blob.

    Joblib covers the sklearn family (sklearn itself plus the sklearn
    wrappers in xgboost/lightgbm); PyTorch uses ``torch.save`` with the
    full model object so reloading does not require the original class
    to be re-defined in the same import path; Keras writes the native
    ``.keras`` archive via a temporary file.
    """
    sub_kind = result.sub_kind
    payload = result.payload
    metrics = dict(result.kind_fields.get("metrics", {}))

    if sub_kind in {"sklearn", "xgboost", "lightgbm"}:
        try:
            import joblib  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                f"model() with framework={sub_kind!r} requires the 'joblib' package."
            ) from exc

        buffer = io.BytesIO()
        joblib.dump(payload, buffer)
        data = buffer.getvalue()
    elif sub_kind == "pytorch":
        try:
            import torch  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "model() with framework='pytorch' requires the 'torch' package."
            ) from exc

        buffer = io.BytesIO()
        torch.save(payload, buffer)
        data = buffer.getvalue()
    elif sub_kind == "keras":
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "model.keras"
            payload.save(str(target))
            data = target.read_bytes()
    else:
        raise ValueError(f"unknown model sub_kind {sub_kind!r}")

    metadata: dict[str, Any] = {
        "sub_kind": sub_kind,
        "framework": sub_kind,
        "metrics": metrics,
        "byte_size": len(data),
    }
    metadata.update(result.metadata)
    return SerializedAsset(
        data=data,
        extension=_MODEL_EXTENSION_BY_SUB_KIND[sub_kind],
        metadata=metadata,
    )


__all__ = [
    "AssetSerializationError",
    "SerializedAsset",
    "serialize_asset",
    "serialize_array",
    "serialize_fig",
    "serialize_model",
    "serialize_table",
    "serialize_text",
]
