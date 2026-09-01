"""Kind registry for the unified asset model.

Each asset kind registers a single :class:`AssetKindSpec` entry carrying
its construction-time detection, serialisation, and loading behaviour.
``asset()`` in :mod:`ginkgo.core.asset` dispatches to ``detect`` at
construction time; the asset registrar dispatches to ``serializer`` at
task completion; the rehydration path and the ``ginkgo asset show`` /
``ginkgo models`` CLI use ``loader`` to read registered bytes back into
live Python values.

Adding a new asset kind means extending the canonical ``AssetKind`` Literal
in :mod:`ginkgo.core.asset` and registering one entry in
:data:`ASSET_KINDS` (plus an optional one-line shorthand factory in
:mod:`ginkgo.core.asset`); every dispatch site picks it up.  The registry
keys are validated against the canonical kind list at import time, so
adding a kind to one home without the other fails immediately.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable

from ginkgo.core.asset import ASSET_KIND_NAMES
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
_GENERIC_MODEL_SUB_KIND = "pickle"
_NATIVE_MODEL_FRAMEWORKS: frozenset[str] = frozenset(_MODEL_MODULE_ROOTS.values())
_MODEL_FRAMEWORKS: frozenset[str] = _NATIVE_MODEL_FRAMEWORKS | {_GENERIC_MODEL_SUB_KIND}


def _unloadable_pickle_class(payload: Any) -> type | None:
    """Return the class of a payload pickle could store but nothing could load.

    Pickle records an instance by its class's ``__module__`` and qualified
    name, and unpickling re-imports that module. Two module names never
    survive the trip: ``__main__``, which resolves to whichever script the
    reading process happens to be running, and the synthetic
    ``ginkgo_user_*`` name a single-file flow is loaded under, which no other
    process — nor the same file at another path — can import at all. Pickling
    such a payload succeeds and produces bytes that raise
    ``ModuleNotFoundError`` on every later read.

    Checks the payload's own class and, one level down, the classes inside a
    dict's values or a list/tuple/set's items: a dict of plain floats is fine,
    while a dict holding one flow-defined estimator is as broken as the
    estimator alone. Deeper nesting is not walked — a general object-graph
    walk would cost more than the failure mode it buys.

    Parameters
    ----------
    payload : Any
        The payload about to be stored under the generic ``pickle`` sub-kind.

    Returns
    -------
    type | None
        The offending class, or ``None`` when every class checked lives in an
        importable module.
    """
    from ginkgo.runtime.module_loader import USER_MODULE_PREFIX

    candidates: list[Any] = [payload]
    if isinstance(payload, dict):
        candidates.extend(payload.values())
    elif isinstance(payload, (list, tuple, set, frozenset)):
        candidates.extend(payload)

    for candidate in candidates:
        cls = type(candidate)
        module = cls.__module__
        if module == "__main__" or module.startswith(USER_MODULE_PREFIX):
            return cls
    return None


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

    Anything else — a dict of weights, a statsmodels result, a
    hand-rolled estimator — falls back to the generic ``pickle``
    sub-kind, so the model kind is not closed to the frameworks that
    happen to have a native serialiser. The fallback pickles the payload
    here rather than at serialisation time so an unpicklable payload
    fails at the ``model()`` call site, where the traceback points at
    the user's own code.

    Two payloads are refused there rather than stored: a ``str`` or
    ``os.PathLike``, which would otherwise become a pickled *path* posing
    as a model in ``ginkgo models`` (``file(path)`` stores a model file),
    and a value whose class no reader could import — see
    :func:`_unloadable_pickle_class`.
    """
    if framework is not None:
        if framework not in _MODEL_FRAMEWORKS:
            raise ValueError(
                f"model() framework must be one of {sorted(_MODEL_FRAMEWORKS)}, got {framework!r}"
            )
        sub_kind = framework
    else:
        root = _module_root(payload)
        sub_kind = _MODEL_MODULE_ROOTS.get(root, _GENERIC_MODEL_SUB_KIND)

    if sub_kind == _GENERIC_MODEL_SUB_KIND:
        import pickle

        if isinstance(payload, (str, os.PathLike)):
            raise TypeError(
                f"model() does not accept a path; got "
                f"{type(payload).__module__}.{type(payload).__name__} {str(payload)!r}. "
                f"model() takes the trained model object itself. To store a model "
                f"file that already exists on disk, use file(path) instead."
            )

        try:
            pickle.dumps(payload)
        except Exception as exc:
            raise TypeError(
                f"model() cannot store payload of type "
                f"{type(payload).__module__}.{type(payload).__name__}: it is not a "
                f"{', '.join(sorted(_NATIVE_MODEL_FRAMEWORKS))} model and is not "
                f"picklable ({type(exc).__name__}: {exc}). Store it as "
                f"text(json.dumps(...)) or file(path) instead."
            ) from exc

        unloadable = _unloadable_pickle_class(payload)
        if unloadable is not None:
            raise TypeError(
                f"model() cannot store a payload whose class is defined in "
                f"{unloadable.__module__!r}: pickle records "
                f"{unloadable.__qualname__} by that module name, which no other "
                f"process can import, so the stored model would fail to load. "
                f"Move {unloadable.__qualname__} into an importable module beside "
                f"the flow and import it there — a package-style workflow layout "
                f"gives your classes real importable names. A payload built only "
                f"from library and built-in types (a dict of weights, a "
                f"scikit-learn estimator) has no such problem."
            )

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
    artifact_encoding : str | None
        Name of the encoding this kind's serialiser writes, or ``None``
        when the artifact holds the payload's own bytes. ``None`` means
        the stored artifact is readable as the file it appears to be, so
        its path may bind a ``file`` parameter; a named encoding means
        the artifact is Ginkgo's rendering of a Python object and a task
        reading it as a file would read a serialized blob. Every kind
        must declare which side it is on.
    """

    kind: str
    detect: Callable[..., tuple[Any, str | None, dict[str, Any]]]
    serializer: Callable[..., Any] | None
    loader: Callable[..., Any] | None
    rehydrate_on_receive: bool
    default_name_strategy: str
    artifact_encoding: str | None


ASSET_KINDS: dict[str, AssetKindSpec] = {
    "file": AssetKindSpec(
        kind="file",
        detect=_detect_file,
        serializer=None,
        loader=None,
        rehydrate_on_receive=False,
        default_name_strategy="task_name",
        # The registrar copies the source path's bytes verbatim.
        artifact_encoding=None,
    ),
    "table": AssetKindSpec(
        kind="table",
        detect=_detect_table,
        serializer=_ser.serialize_table,
        loader=_load.load_table_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
        artifact_encoding="Parquet",
    ),
    "array": AssetKindSpec(
        kind="array",
        detect=_detect_array,
        serializer=_ser.serialize_array,
        loader=_load.load_array_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
        artifact_encoding="a zipped zarr store or .npy blob",
    ),
    "fig": AssetKindSpec(
        kind="fig",
        detect=_detect_fig,
        serializer=_ser.serialize_fig,
        loader=_load.load_fig_bytes,
        rehydrate_on_receive=False,
        default_name_strategy="kind_index",
        # ``serialize_fig`` writes the native image or HTML bytes: a source
        # path is read through unchanged, and a figure object is rendered to
        # the format a viewer expects.
        artifact_encoding=None,
    ),
    "text": AssetKindSpec(
        kind="text",
        detect=_detect_text,
        serializer=_ser.serialize_text,
        loader=_load.load_text_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
        # ``serialize_text`` writes raw UTF-8.
        artifact_encoding=None,
    ),
    "model": AssetKindSpec(
        kind="model",
        detect=_detect_model,
        serializer=_ser.serialize_model,
        loader=_load.load_model_bytes,
        rehydrate_on_receive=True,
        default_name_strategy="kind_index",
        artifact_encoding="a framework-specific serialized model",
    ),
}

# The registry must stay in lockstep with the canonical kind list in
# ginkgo.core.asset; adding a kind to one without the other fails at import.
if frozenset(ASSET_KINDS) != frozenset(ASSET_KIND_NAMES):
    raise RuntimeError(
        f"ASSET_KINDS registry keys {sorted(ASSET_KINDS)} do not match the "
        f"canonical asset kinds {sorted(ASSET_KIND_NAMES)} defined in ginkgo.core.asset"
    )


def get_kind_spec(kind: str) -> AssetKindSpec:
    """Return the :class:`AssetKindSpec` registered for *kind*."""
    try:
        return ASSET_KINDS[kind]
    except KeyError as exc:
        raise ValueError(f"Unsupported asset kind: {kind!r}") from exc


def artifact_encoding_for(kind: str) -> str | None:
    """Return the name of Ginkgo's encoding for *kind*'s artifact.

    ``None`` means the artifact holds the payload's own bytes — a ``file``
    copied verbatim, a ``fig``'s native PNG/SVG/HTML, a ``text`` asset's raw
    UTF-8 — so its path reads as the file it appears to be. A returned name
    means the bytes are Ginkgo's rendering of a Python object, so code that
    reads the path as a file reads a serialized blob instead.

    Parameters
    ----------
    kind : str
        Asset kind identifier.

    Returns
    -------
    str | None
        Encoding name, or ``None`` for native bytes. An unregistered kind
        counts as encoded, since nothing vouches for its bytes.
    """
    spec = ASSET_KINDS.get(kind)
    if spec is None:
        return "an unrecognised encoding"
    return spec.artifact_encoding


NATIVE_ARTIFACT_KINDS: frozenset[str] = frozenset(
    spec.kind for spec in ASSET_KINDS.values() if spec.artifact_encoding is None
)

REHYDRATABLE_KINDS: frozenset[str] = frozenset(
    spec.kind for spec in ASSET_KINDS.values() if spec.rehydrate_on_receive
)

WRAPPER_KINDS: frozenset[str] = frozenset(kind for kind in ASSET_KINDS if kind != "file")

# Sub-kinds whose detect callable accepted a ``str`` path rather than an
# in-memory value. Only ``table`` and ``fig`` accept a bare ``str`` as a path;
# ``text`` treats ``str`` as inline content, and ``array`` / ``model`` reject
# both ``str`` and ``os.PathLike`` outright, so neither has a path-backed form.
_PATH_SUB_KINDS: dict[str, frozenset[str]] = {
    "table": frozenset({"csv", "tsv"}),
    "fig": frozenset({"png", "svg", "html"}),
}


def is_path_backed_payload(*, kind: str, sub_kind: str | None, payload: Any) -> bool:
    """Return whether a wrapped payload names a file rather than holding a value.

    Parameters
    ----------
    kind : str
        The registered asset kind.
    sub_kind : str | None
        The sub-kind resolved by the kind's detect callable.
    payload : Any
        The construction-time payload carried by the ``AssetResult``.

    Returns
    -------
    bool
        ``True`` when the payload is a path the serializer reads from disk,
        ``False`` when it is the in-memory value itself. Note that
        ``sub_kind`` alone cannot decide this for ``text``, where a ``Path``
        and an inline ``str`` share the same sub-kinds.
    """
    if isinstance(payload, os.PathLike):
        return True
    return sub_kind is not None and sub_kind in _PATH_SUB_KINDS.get(kind, frozenset())
