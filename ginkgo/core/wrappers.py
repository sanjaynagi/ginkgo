"""Special asset wrapper sentinels (``table`` / ``array`` / ``fig`` / ``text``).

These wrappers let task authors tag selected return values so the evaluator
materialises them as immutable asset artifacts with kind-specific metadata
and previews. They follow the same sentinel pattern as :class:`ShellExpr`:
the user calls a factory (``table(df, name="features")``) inside a task body,
the task returns the sentinel, and the evaluator replaces it with a resolved
:class:`~ginkgo.core.asset.AssetRef` after serialising the payload.

Wrappers are intentionally **detection-only** at construction time: the
sub-kind (``pandas``, ``polars``, ``matplotlib``, ...) is probed here so the
evaluator can dispatch to the right serialiser without re-importing optional
backends. No materialisation happens until the evaluator processes the
returned sentinel.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# ---------------------------------------------------------------------------
# Sub-kind detection helpers
# ---------------------------------------------------------------------------


def _module_root(value: Any) -> str:
    """Return the top-level module name for *value*'s class."""
    return type(value).__module__.split(".", 1)[0]


# ---------------------------------------------------------------------------
# Sentinel dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class _WrappedResult:
    """Base class for wrapped asset return values.

    Parameters
    ----------
    payload : Any
        The user-provided value (DataFrame, ndarray, Figure, ...).
    name : str | None
        Optional explicit local asset name.
    sub_kind : str
        Detected backend name (e.g. ``"pandas"``, ``"numpy"``).
    metadata : dict[str, Any]
        Optional user-defined metadata passed through to the asset version.
    """

    payload: Any
    name: str | None
    sub_kind: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class TableResult(_WrappedResult):
    """Sentinel for tabular asset outputs.

    Produced by :func:`ginkgo.table`. The kind name is ``"table"``.
    """

    kind: str = "table"


@dataclass(frozen=True, kw_only=True)
class ArrayResult(_WrappedResult):
    """Sentinel for n-dimensional array asset outputs.

    Produced by :func:`ginkgo.array`. The kind name is ``"array"``.
    """

    kind: str = "array"


@dataclass(frozen=True, kw_only=True)
class FigureResult(_WrappedResult):
    """Sentinel for figure/plot asset outputs.

    Produced by :func:`ginkgo.fig`. The kind name is ``"fig"``.
    """

    kind: str = "fig"


@dataclass(frozen=True, kw_only=True)
class TextResult(_WrappedResult):
    """Sentinel for textual/structured document asset outputs.

    Produced by :func:`ginkgo.text`. The kind name is ``"text"``.

    Parameters
    ----------
    text_format : {"plain", "markdown", "json"}
        Document format used for storage and rendering.
    """

    kind: str = "text"
    text_format: Literal["plain", "markdown", "json"] = "plain"


WrappedResult = TableResult | ArrayResult | FigureResult | TextResult


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------


def _detect_table_sub_kind(payload: Any) -> str:
    """Detect the backend sub-kind for a table payload."""
    if isinstance(payload, (str, Path)):
        suffix = Path(payload).suffix.lower()
        if suffix == ".csv":
            return "csv"
        if suffix == ".tsv":
            return "tsv"
        raise TypeError(f"table() path input must end with .csv or .tsv, got {str(payload)!r}")

    # Check pandas first (required dependency so the isinstance check is cheap).
    import pandas as pd

    if isinstance(payload, pd.DataFrame):
        return "pandas"

    root = _module_root(payload)
    if root == "polars":
        # Covers both DataFrame and LazyFrame.
        return "polars"
    if root == "pyarrow":
        # Covers Table, RecordBatch, and dataset handles.
        return "pyarrow"
    if root == "duckdb":
        return "duckdb"

    raise TypeError(
        f"table() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_array_sub_kind(payload: Any) -> str:
    """Detect the backend sub-kind for an array payload."""
    import numpy as np

    if isinstance(payload, np.ndarray):
        return "numpy"

    root = _module_root(payload)
    if root == "xarray":
        return "xarray"
    if root == "zarr":
        return "zarr"
    if root == "dask":
        return "dask"

    raise TypeError(
        f"array() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_fig_sub_kind(payload: Any) -> str:
    """Detect the backend sub-kind for a figure payload."""
    if isinstance(payload, (str, Path)):
        suffix = Path(payload).suffix.lower()
        if suffix == ".png":
            return "png"
        if suffix == ".svg":
            return "svg"
        if suffix in {".html", ".htm"}:
            return "html"
        raise TypeError(f"fig() path input must end with .png/.svg/.html, got {str(payload)!r}")

    root = _module_root(payload)
    if root == "matplotlib":
        return "matplotlib"
    if root == "plotly":
        return "plotly"
    if root == "bokeh":
        return "bokeh"

    raise TypeError(
        f"fig() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_text_sub_kind(
    payload: Any, *, text_format: Literal["plain", "markdown", "json"] | None
) -> tuple[str, Literal["plain", "markdown", "json"]]:
    """Detect the sub-kind and resolved format for a text payload.

    Rules
    -----
    - ``dict`` → ``json`` (format must be ``"json"`` if given).
    - ``Path`` → inferred from suffix when format omitted (``.md``→markdown,
      ``.json``→json, else plain). No filesystem lookup.
    - ``str`` → treated as inline content. Defaults to ``plain`` when no
      explicit format is given; no path probing.
    """
    if isinstance(payload, dict):
        resolved = text_format if text_format is not None else "json"
        if resolved != "json":
            raise ValueError(
                f"text() dict payload requires format='json', got format={resolved!r}"
            )
        return "json", "json"

    if isinstance(payload, Path):
        suffix = payload.suffix.lower()
        if text_format is not None:
            resolved = text_format
        elif suffix == ".md":
            resolved = "markdown"
        elif suffix == ".json":
            resolved = "json"
        else:
            resolved = "plain"
        return resolved, resolved

    if isinstance(payload, str):
        resolved = text_format if text_format is not None else "plain"
        return resolved, resolved

    raise TypeError(
        f"text() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def table(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> TableResult:
    """Wrap a tabular value as an asset return.

    Parameters
    ----------
    payload : Any
        The tabular value. Supports pandas DataFrame, polars
        DataFrame/LazyFrame, pyarrow Table/Dataset, DuckDB relation,
        or a path to a CSV/TSV file.
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    TableResult
    """
    sub_kind = _detect_table_sub_kind(payload)
    return TableResult(
        payload=payload,
        name=name,
        sub_kind=sub_kind,
        metadata=dict(metadata or {}),
    )


def array(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> ArrayResult:
    """Wrap an n-dimensional array value as an asset return.

    Parameters
    ----------
    payload : Any
        The array value. Supports numpy ndarray, xarray DataArray/Dataset,
        zarr array/group, and dask array.
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    ArrayResult
    """
    sub_kind = _detect_array_sub_kind(payload)
    return ArrayResult(
        payload=payload,
        name=name,
        sub_kind=sub_kind,
        metadata=dict(metadata or {}),
    )


def fig(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> FigureResult:
    """Wrap a figure or plot value as an asset return.

    Parameters
    ----------
    payload : Any
        The figure value. Supports matplotlib Figure, plotly Figure,
        bokeh Figure, or a path to an existing PNG/SVG/HTML file.
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    FigureResult
    """
    sub_kind = _detect_fig_sub_kind(payload)
    return FigureResult(
        payload=payload,
        name=name,
        sub_kind=sub_kind,
        metadata=dict(metadata or {}),
    )


def text(
    payload: Any,
    *,
    name: str | None = None,
    format: Literal["plain", "markdown", "json"] | None = None,
    metadata: dict[str, Any] | None = None,
) -> TextResult:
    """Wrap a text or structured document value as an asset return.

    Parameters
    ----------
    payload : Any
        The document value. Supports strings, dicts, or paths. Dicts are
        serialised as JSON; strings are stored as the requested format.
    name : str | None
        Optional explicit local asset name.
    format : {"plain", "markdown", "json"} | None
        Document format. Auto-detected from the payload when omitted.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    TextResult
    """
    if format is not None and format not in {"plain", "markdown", "json"}:
        raise ValueError(
            f"text() format must be one of 'plain', 'markdown', 'json', got {format!r}"
        )
    sub_kind, resolved_format = _detect_text_sub_kind(payload, text_format=format)

    # Normalise dict payloads to canonical JSON strings so serialisation is
    # trivially deterministic downstream.
    if isinstance(payload, dict):
        normalised: Any = json.dumps(payload, indent=2, sort_keys=True, default=str)
    else:
        normalised = payload

    return TextResult(
        payload=normalised,
        name=name,
        sub_kind=sub_kind,
        text_format=resolved_format,
        metadata=dict(metadata or {}),
    )
