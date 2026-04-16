"""Serialisation backends for special asset wrappers.

Each wrapper kind (``table``, ``array``, ``fig``, ``text``) has one
``serialize_*`` function returning a :class:`SerializedWrapper` with the raw
bytes, file extension, and kind-specific metadata. Optional backends
(polars, pyarrow, duckdb, xarray, zarr, dask, matplotlib, plotly, bokeh)
are imported lazily so tasks without the relevant dependency simply cannot
produce that sub-kind.
"""

from __future__ import annotations

import io
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ginkgo.core.wrappers import (
    ArrayResult,
    FigureResult,
    ModelResult,
    TableResult,
    TextResult,
    WrappedResult,
)


class WrapperSerializationError(RuntimeError):
    """Raised when a wrapped payload cannot be serialised.

    Parameters
    ----------
    wrapper_kind : str
        The wrapper kind (``table`` / ``array`` / ``fig`` / ``text``).
    wrapper_index : int
        Per-kind positional index of the offending wrapper.
    wrapper_name : str | None
        Explicit name, if the wrapper had one.
    message : str
        Underlying error message.
    """

    def __init__(
        self,
        *,
        wrapper_kind: str,
        wrapper_index: int,
        wrapper_name: str | None,
        message: str,
    ) -> None:
        identifier = f"name={wrapper_name!r}" if wrapper_name else f"index={wrapper_index}"
        super().__init__(f"failed to serialise {wrapper_kind} wrapper ({identifier}): {message}")
        self.wrapper_kind = wrapper_kind
        self.wrapper_index = wrapper_index
        self.wrapper_name = wrapper_name


@dataclass(kw_only=True)
class SerializedWrapper:
    """Output of one wrapper serialisation pass.

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


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


def serialize_wrapper(
    *,
    wrapper: WrappedResult,
    wrapper_index: int,
) -> SerializedWrapper:
    """Serialise one wrapper payload.

    Parameters
    ----------
    wrapper : WrappedResult
        The sentinel returned by a task body.
    wrapper_index : int
        Per-kind positional index of the wrapper (used in error messages).

    Returns
    -------
    SerializedWrapper
    """
    try:
        if isinstance(wrapper, TableResult):
            return _serialize_table(wrapper)
        if isinstance(wrapper, ArrayResult):
            return _serialize_array(wrapper)
        if isinstance(wrapper, FigureResult):
            return _serialize_fig(wrapper)
        if isinstance(wrapper, TextResult):
            return _serialize_text(wrapper)
        if isinstance(wrapper, ModelResult):
            return _serialize_model(wrapper)
    except WrapperSerializationError:
        raise
    except Exception as exc:
        raise WrapperSerializationError(
            wrapper_kind=wrapper.kind,
            wrapper_index=wrapper_index,
            wrapper_name=wrapper.name,
            message=f"{type(exc).__name__}: {exc}",
        ) from exc

    raise WrapperSerializationError(
        wrapper_kind=getattr(wrapper, "kind", "unknown"),
        wrapper_index=wrapper_index,
        wrapper_name=getattr(wrapper, "name", None),
        message=f"unknown wrapper type {type(wrapper).__name__}",
    )


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------


def _serialize_table(wrapper: TableResult) -> SerializedWrapper:
    """Materialise a table wrapper to Parquet bytes."""
    frame = _materialise_table_to_pandas(wrapper)

    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    parquet_bytes = buffer.getvalue()

    schema = [
        {"name": str(col), "dtype": str(dtype)}
        for col, dtype in zip(frame.columns, frame.dtypes, strict=True)
    ]
    metadata: dict[str, Any] = {
        "sub_kind": wrapper.sub_kind,
        "schema": schema,
        "row_count": int(len(frame.index)),
        "byte_size": len(parquet_bytes),
    }
    metadata.update(wrapper.metadata)
    return SerializedWrapper(
        data=parquet_bytes,
        extension="parquet",
        metadata=metadata,
    )


def _materialise_table_to_pandas(wrapper: TableResult) -> Any:
    """Convert any supported table payload to a pandas DataFrame."""
    import pandas as pd

    payload = wrapper.payload
    sub_kind = wrapper.sub_kind

    if sub_kind == "pandas":
        return payload

    if sub_kind == "polars":
        # Handle both eager and lazy polars frames without forcing user collect.
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


def _serialize_array(wrapper: ArrayResult) -> SerializedWrapper:
    """Materialise an array wrapper into a zarr zip store or ``.npy`` blob.

    When the ``zarr`` package is installed the array is written to a zipped
    zarr store (preserving chunking for dask/xarray/zarr payloads). When
    ``zarr`` is unavailable, numpy arrays fall back to ``.npy`` and
    non-numpy backends raise a structured error asking the user to install
    ``zarr``.
    """
    try:
        import zarr  # type: ignore[import-not-found]

        zarr_available = True
    except ImportError:
        zarr = None  # type: ignore[assignment]
        zarr_available = False

    sub_kind = wrapper.sub_kind
    if not zarr_available and sub_kind != "numpy":
        raise WrapperSerializationError(
            wrapper_kind="array",
            wrapper_index=-1,
            wrapper_name=wrapper.name,
            message=(
                f"array({sub_kind}) requires the 'zarr' package. "
                "Install with 'pip install zarr' or 'pixi add zarr'."
            ),
        )

    if not zarr_available:
        return _serialize_numpy_to_npy(wrapper)

    return _serialize_array_to_zarr(wrapper=wrapper, zarr_module=zarr)


def _serialize_numpy_to_npy(wrapper: ArrayResult) -> SerializedWrapper:
    """Fallback path: write a numpy array as a ``.npy`` blob."""
    import numpy as np

    payload = wrapper.payload
    if not isinstance(payload, np.ndarray):
        # Detection should have caught this earlier; guard defensively.
        raise TypeError(f"numpy fallback cannot handle payload of type {type(payload).__name__}")

    buffer = io.BytesIO()
    np.save(buffer, payload, allow_pickle=False)
    data = buffer.getvalue()

    metadata: dict[str, Any] = {
        "sub_kind": wrapper.sub_kind,
        "shape": list(payload.shape),
        "dtype": str(payload.dtype),
        "chunks": None,
        "coordinates": None,
        "byte_size": len(data),
    }
    metadata.update(wrapper.metadata)
    return SerializedWrapper(data=data, extension="npy", metadata=metadata)


def _serialize_array_to_zarr(*, wrapper: ArrayResult, zarr_module: Any) -> SerializedWrapper:
    """Write any supported array payload to a zipped zarr store."""
    shape, dtype_str, chunks, coordinates = _array_properties(wrapper)

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "array.zarr.zip"
        store = zarr_module.storage.ZipStore(str(zip_path), mode="w")
        try:
            _write_payload_to_zarr_store(wrapper=wrapper, store=store, zarr_module=zarr_module)
        finally:
            store.close()
        data = zip_path.read_bytes()

    metadata: dict[str, Any] = {
        "sub_kind": wrapper.sub_kind,
        "shape": shape,
        "dtype": dtype_str,
        "chunks": chunks,
        "coordinates": coordinates,
        "byte_size": len(data),
    }
    metadata.update(wrapper.metadata)
    return SerializedWrapper(data=data, extension="zarr.zip", metadata=metadata)


def _array_properties(
    wrapper: ArrayResult,
) -> tuple[list[int], str, list[int] | None, dict[str, list[str]] | None]:
    """Return ``(shape, dtype, chunks, coordinates)`` for an array payload.

    Computed without triggering dask computation when possible.
    """
    import numpy as np

    payload = wrapper.payload
    sub_kind = wrapper.sub_kind

    if sub_kind == "numpy":
        return list(payload.shape), str(payload.dtype), None, None

    if sub_kind == "dask":
        chunks_meta = getattr(payload, "chunks", None)
        # dask chunks is a tuple of tuples per axis; summarise as first-axis chunks.
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


def _write_payload_to_zarr_store(*, wrapper: ArrayResult, store: Any, zarr_module: Any) -> None:
    """Materialise a wrapped array payload into the given zarr store."""
    import numpy as np

    payload = wrapper.payload
    sub_kind = wrapper.sub_kind

    if sub_kind == "zarr":
        # Passthrough: copy the existing store contents to preserve chunking.
        zarr_module.copy_store(payload.store, store) if hasattr(
            zarr_module, "copy_store"
        ) else _copy_zarr_via_array(source=payload, store=store, zarr_module=zarr_module)
        return

    if sub_kind == "dask":
        # dask can write straight to zarr, triggering compute at this point.
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


def _serialize_fig(wrapper: FigureResult) -> SerializedWrapper:
    """Serialise a figure wrapper to its native format."""
    sub_kind = wrapper.sub_kind
    payload = wrapper.payload

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
    metadata.update(wrapper.metadata)
    return SerializedWrapper(data=data, extension=extension, metadata=metadata)


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


def _serialize_text(wrapper: TextResult) -> SerializedWrapper:
    """Serialise a text wrapper as raw UTF-8 bytes."""
    payload = wrapper.payload

    if isinstance(payload, Path):
        body = payload.read_text(encoding="utf-8")
    else:
        # The factory normalised dicts into JSON strings; str flows through
        # directly without re-probing the filesystem.
        body = str(payload)

    data = body.encode("utf-8")

    # Count lines: every "\n" plus one final non-empty, non-terminated line.
    if not body:
        line_count = 0
    elif body.endswith("\n"):
        line_count = body.count("\n")
    else:
        line_count = body.count("\n") + 1

    metadata: dict[str, Any] = {
        "sub_kind": wrapper.sub_kind,
        "format": wrapper.text_format,
        "byte_size": len(data),
        "line_count": line_count,
    }
    metadata.update(wrapper.metadata)
    return SerializedWrapper(
        data=data,
        extension=_TEXT_EXTENSION_BY_FORMAT[wrapper.text_format],
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


def _serialize_model(wrapper: ModelResult) -> SerializedWrapper:
    """Serialise a trained-model payload as a full-object blob.

    Joblib covers the sklearn family (sklearn itself plus the sklearn
    wrappers in xgboost/lightgbm); PyTorch uses ``torch.save`` with the
    full model object so reloading does not require the original class
    to be re-defined in the same import path; Keras writes the native
    ``.keras`` archive via a temporary file.
    """
    sub_kind = wrapper.sub_kind
    payload = wrapper.payload

    if sub_kind in {"sklearn", "xgboost", "lightgbm"}:
        try:
            import joblib  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                f"model() with framework={sub_kind!r} requires the 'joblib' package. "
                "Install with: pip install joblib scikit-learn"
            ) from exc

        buffer = io.BytesIO()
        joblib.dump(payload, buffer)
        data = buffer.getvalue()
    elif sub_kind == "pytorch":
        try:
            import torch  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                "model() with framework='pytorch' requires the 'torch' package. "
                "Install with: pip install torch"
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
        "metrics": dict(wrapper.metrics),
        "byte_size": len(data),
    }
    metadata.update(wrapper.metadata)
    return SerializedWrapper(
        data=data,
        extension=_MODEL_EXTENSION_BY_SUB_KIND[sub_kind],
        metadata=metadata,
    )


# Re-exported for tests and loader registry callers.
__all__ = [
    "SerializedWrapper",
    "WrapperSerializationError",
    "serialize_wrapper",
]
