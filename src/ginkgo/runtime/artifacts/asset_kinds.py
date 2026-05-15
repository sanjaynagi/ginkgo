"""Kind registry for the unified asset model.

Each asset kind registers a single :class:`AssetKindSpec` entry carrying
its construction-time detection, serialisation, and loading behaviour.
``asset()`` in :mod:`ginkgo.core.asset` dispatches to ``detect`` at
construction time; the asset registrar dispatches to ``serializer`` at
task completion; the rehydration path and the ``ginkgo asset show`` /
``ginkgo models`` CLI use ``loader`` to read registered bytes back into
live Python values.

Adding a new asset kind is a pure-registry change: register one entry in
:data:`ASSET_KINDS` (plus an optional one-line shorthand factory in
:mod:`ginkgo.core.asset`) and every dispatch site picks it up.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ginkgo.runtime.artifacts import asset_serialization as _ser
from ginkgo.runtime.artifacts import asset_loaders as _load


# ---------------------------------------------------------------------------
# Per-kind detect callables
# ---------------------------------------------------------------------------
#
# Each ``detect(payload, **kind_fields) -> (payload, sub_kind, kind_fields)``
# performs the construction-time sub-kind probing previously done inside
# the old wrapper factories. ``payload`` may be transformed (e.g. dicts
# normalised to JSON text). ``kind_fields`` is the bag carrying
# kind-specific construction-time fields forwarded to the serialiser.


def _module_root(value: Any) -> str:
    """Return the top-level module name for *value*'s class."""
    return type(value).__module__.split(".", 1)[0]


def _detect_file(
    payload: Any,
) -> tuple[Any, str | None, dict[str, Any]]:
    """Validate and pass through a ``file`` payload."""
    from pathlib import Path

    if not isinstance(payload, (str, Path)):
        raise TypeError(
            f"asset(kind='file') expects a path-like value, got {type(payload).__name__!r}"
        )
    return payload, None, {}


def _detect_table(
    payload: Any,
) -> tuple[Any, str, dict[str, Any]]:
    """Detect the backend sub-kind for a table payload."""
    from pathlib import Path

    if isinstance(payload, (str, Path)):
        suffix = Path(payload).suffix.lower()
        if suffix == ".csv":
            return payload, "csv", {}
        if suffix == ".tsv":
            return payload, "tsv", {}
        raise TypeError(f"table() path input must end with .csv or .tsv, got {str(payload)!r}")

    import pandas as pd

    if isinstance(payload, pd.DataFrame):
        return payload, "pandas", {}

    root = _module_root(payload)
    if root == "polars":
        # Covers both DataFrame and LazyFrame.
        return payload, "polars", {}
    if root == "pyarrow":
        # Covers Table, RecordBatch, and dataset handles.
        return payload, "pyarrow", {}
    if root == "duckdb":
        return payload, "duckdb", {}

    raise TypeError(
        f"table() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_array(
    payload: Any,
) -> tuple[Any, str, dict[str, Any]]:
    """Detect the backend sub-kind for an array payload."""
    import numpy as np

    if isinstance(payload, np.ndarray):
        return payload, "numpy", {}

    root = _module_root(payload)
    if root == "xarray":
        return payload, "xarray", {}
    if root == "zarr":
        return payload, "zarr", {}
    if root == "dask":
        return payload, "dask", {}

    raise TypeError(
        f"array() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_fig(
    payload: Any,
) -> tuple[Any, str, dict[str, Any]]:
    """Detect the backend sub-kind for a figure payload."""
    from pathlib import Path

    if isinstance(payload, (str, Path)):
        suffix = Path(payload).suffix.lower()
        if suffix == ".png":
            return payload, "png", {}
        if suffix == ".svg":
            return payload, "svg", {}
        if suffix in {".html", ".htm"}:
            return payload, "html", {}
        raise TypeError(f"fig() path input must end with .png/.svg/.html, got {str(payload)!r}")

    root = _module_root(payload)
    if root == "matplotlib":
        return payload, "matplotlib", {}
    if root == "plotly":
        return payload, "plotly", {}
    if root == "bokeh":
        return payload, "bokeh", {}

    raise TypeError(
        f"fig() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


def _detect_text(
    payload: Any,
    *,
    format: str | None = None,
) -> tuple[Any, str, dict[str, Any]]:
    """Detect sub-kind + resolved format for a text payload.

    Rules
    -----
    - ``dict`` → ``json`` (format must be ``"json"`` if given).
    - ``Path`` → inferred from suffix when format omitted (``.md`` →
      markdown, ``.json`` → json, else plain). No filesystem lookup.
    - ``str`` → treated as inline content. Defaults to ``plain`` when no
      explicit format is given.
    """
    import json as _json
    from pathlib import Path

    if format is not None and format not in {"plain", "markdown", "json"}:
        raise ValueError(
            f"text() format must be one of 'plain', 'markdown', 'json', got {format!r}"
        )

    if isinstance(payload, dict):
        resolved = format if format is not None else "json"
        if resolved != "json":
            raise ValueError(
                f"text() dict payload requires format='json', got format={resolved!r}"
            )
        # Normalise the dict into canonical JSON so serialisation is
        # trivially deterministic downstream.
        normalised = _json.dumps(payload, indent=2, sort_keys=True, default=str)
        return normalised, "json", {"format": "json"}

    if isinstance(payload, Path):
        suffix = payload.suffix.lower()
        if format is not None:
            resolved = format
        elif suffix == ".md":
            resolved = "markdown"
        elif suffix == ".json":
            resolved = "json"
        else:
            resolved = "plain"
        return payload, resolved, {"format": resolved}

    if isinstance(payload, str):
        resolved = format if format is not None else "plain"
        return payload, resolved, {"format": resolved}

    raise TypeError(
        f"text() does not support payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}"
    )


_MODEL_MODULE_ROOTS: dict[str, str] = {
    "sklearn": "sklearn",
    "xgboost": "xgboost",
    "lightgbm": "lightgbm",
    "torch": "pytorch",
    "keras": "keras",
    "tensorflow": "keras",
}
_MODEL_FRAMEWORKS: frozenset[str] = frozenset(_MODEL_MODULE_ROOTS.values())


def _detect_model(
    payload: Any,
    *,
    framework: str | None = None,
    metrics: dict[str, float] | None = None,
) -> tuple[Any, str, dict[str, Any]]:
    """Detect the framework sub-kind for a model payload.

    Uses the top-level module of the payload's class. Scikit-learn-style
    wrappers in other libraries (``xgboost.sklearn.XGBClassifier``,
    ``lightgbm.sklearn.LGBMClassifier``) resolve to their owning package
    rather than ``sklearn``, which keeps serialisation consistent with
    the library that produced them.
    """
    if framework is not None:
        if framework not in _MODEL_FRAMEWORKS:
            raise ValueError(
                f"model() framework must be one of {sorted(_MODEL_FRAMEWORKS)}, got {framework!r}"
            )
        sub_kind = framework
    else:
        root = _module_root(payload)
        try:
            sub_kind = _MODEL_MODULE_ROOTS[root]
        except KeyError as exc:
            raise TypeError(
                f"model() does not support payload of type "
                f"{type(payload).__module__}.{type(payload).__name__}"
            ) from exc

    return payload, sub_kind, {"framework": sub_kind, "metrics": dict(metrics or {})}


# ---------------------------------------------------------------------------
# Spec dataclass and registry
# ---------------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class AssetKindSpec:
    """Per-kind dispatch table entry.

    Parameters
    ----------
    kind : str
        Asset kind identifier.
    detect : Callable
        Construction-time probe returning ``(payload, sub_kind,
        kind_fields)``. Called by :func:`ginkgo.core.asset.asset`.
    serializer : Callable[[AssetResult, int], SerializedAsset] | None
        Serialiser writing bytes for the artifact store. ``None`` for
        ``file`` assets, whose content is copied directly from a
        source path by the registrar.
    loader : Callable | None
        Rehydration function used by the CLI and evaluator. ``None`` for
        kinds without an on-disk loader (``file``).
    rehydrate_on_receive : bool
        Whether the evaluator should auto-rehydrate this kind's
        ``AssetRef`` into a live Python value when passed as a task
        argument. ``False`` for ``file`` (file coercion path handles it)
        and ``fig`` (binary payloads rarely consumed as live objects).
    default_name_strategy : str
        Either ``"task_name"`` (use the task function name as the
        default when no explicit ``name`` is supplied — ``file``) or
        ``"kind_index"`` (use ``<task>.<kind>[<index>]`` — all other
        kinds).
    """

    kind: str
    detect: Callable[..., tuple[Any, str | None, dict[str, Any]]]
    serializer: Callable[..., Any] | None
    loader: Callable[..., Any] | None
    rehydrate_on_receive: bool
    default_name_strategy: str


ASSET_KINDS: dict[str, AssetKindSpec] = {
    "file": AssetKindSpec(
        kind="file",
        detect=_detect_file,
        serializer=None,
        loader=None,
        rehydrate_on_receive=False,
        default_name_strategy="task_name",
    ),
    "table": AssetKindSpec(
        kind="table",
        detect=_detect_table,
        serializer=_ser.serialize_table,
        loader=_load.load_table_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
    ),
    "array": AssetKindSpec(
        kind="array",
        detect=_detect_array,
        serializer=_ser.serialize_array,
        loader=_load.load_array_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
    ),
    "fig": AssetKindSpec(
        kind="fig",
        detect=_detect_fig,
        serializer=_ser.serialize_fig,
        loader=_load.load_fig_bytes,
        rehydrate_on_receive=False,
        default_name_strategy="kind_index",
    ),
    "text": AssetKindSpec(
        kind="text",
        detect=_detect_text,
        serializer=_ser.serialize_text,
        loader=_load.load_text_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
    ),
    "model": AssetKindSpec(
        kind="model",
        detect=_detect_model,
        serializer=_ser.serialize_model,
        loader=_load.load_model_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
    ),
}


def get_kind_spec(kind: str) -> AssetKindSpec:
    """Return the :class:`AssetKindSpec` registered for *kind*."""
    try:
        return ASSET_KINDS[kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported asset kind: {kind!r}") from exc


REHYDRATABLE_KINDS: frozenset[str] = frozenset(
    spec.kind for spec in ASSET_KINDS.values() if spec.rehydrate_on_receive
)

WRAPPER_KINDS: frozenset[str] = frozenset(kind for kind in ASSET_KINDS if kind != "file")
