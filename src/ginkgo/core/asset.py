"""Asset identity and task-return wrappers.

Defines a narrow asset model layered over the existing artifact store.
Tasks return an :class:`AssetResult` sentinel via ``asset(...)`` (or one of
the shorthand factories :func:`table`, :func:`array`, :func:`fig`,
:func:`text`, :func:`model`) and the evaluator replaces it with an
:class:`AssetRef` once the payload has been registered in the catalog.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from ginkgo.core.types import file
from ginkgo.runtime.caching.hashing import hash_str


AssetKind = Literal["file", "table", "array", "fig", "text", "model"]

_VALID_KINDS: frozenset[str] = frozenset({"file", "table", "array", "fig", "text", "model"})


@dataclass(frozen=True, kw_only=True)
class AssetKey:
    """Stable logical identifier for one asset.

    Parameters
    ----------
    namespace : str
        Asset namespace (the asset's ``kind``).
    name : str
        Human-readable logical asset name within the namespace.
    """

    namespace: str
    name: str

    def to_dict(self) -> dict[str, str]:
        """Return a JSON/YAML-safe mapping."""
        return {"namespace": self.namespace, "name": self.name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AssetKey:
        """Build an asset key from serialized metadata.

        Parameters
        ----------
        data : dict[str, Any]
            Serialized key payload.

        Returns
        -------
        AssetKey
        """
        return cls(namespace=str(data["namespace"]), name=str(data["name"]))

    def __str__(self) -> str:
        """Render the canonical ``namespace:name`` string."""
        return f"{self.namespace}:{self.name}"


@dataclass(frozen=True, kw_only=True)
class AssetVersion:
    """Immutable metadata for one asset materialization.

    Parameters
    ----------
    key : AssetKey
        Logical identity of the asset.
    version_id : str
        Immutable version identifier.
    kind : str
        Physical asset kind.
    artifact_id : str
        Backing content-addressed artifact identifier.
    content_hash : str
        Content hash of the stored asset bytes.
    run_id : str
        Run identifier that produced this version.
    producer_task : str
        Fully-qualified producing task name.
    created_at : str
        ISO-8601 creation timestamp.
    metadata : dict[str, Any]
        User-supplied asset metadata.
    """

    key: AssetKey
    version_id: str
    kind: str
    artifact_id: str
    content_hash: str
    run_id: str
    producer_task: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a YAML-safe mapping."""
        data = asdict(self)
        data["key"] = self.key.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AssetVersion:
        """Build an asset version from serialized metadata.

        Parameters
        ----------
        data : dict[str, Any]
            Serialized version payload.

        Returns
        -------
        AssetVersion
        """
        return cls(
            key=AssetKey.from_dict(data["key"]),
            version_id=str(data["version_id"]),
            kind=str(data["kind"]),
            artifact_id=str(data["artifact_id"]),
            content_hash=str(data["content_hash"]),
            run_id=str(data["run_id"]),
            producer_task=str(data["producer_task"]),
            created_at=str(data["created_at"]),
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True, kw_only=True)
class AssetResult:
    """Task-return sentinel produced by :func:`asset` and its shorthands.

    An :class:`AssetResult` tags a task output for registration as an
    immutable asset. The evaluator consumes the sentinel, serialises its
    payload, and replaces it with a resolved :class:`AssetRef`.

    Parameters
    ----------
    payload : Any
        The user-provided value. For ``file``-kind assets this is a path
        (``str`` / :class:`~pathlib.Path`). For semantic kinds this is the
        live object (``pandas.DataFrame`` / ``numpy.ndarray`` / figure /
        text body / trained model) or, for the path-backed sub-kinds
        (``csv``/``tsv``/``png``/``svg``/``html`` and ``Path`` for text),
        a filesystem path.
    kind : str
        Asset kind. One of ``file``/``table``/``array``/``fig``/``text``
        /``model``. ``file`` is the fallback kind for bytes whose semantic
        shape Ginkgo does not track; the other kinds are semantically
        typed and drive kind-specific serialization, preview rendering,
        and rehydration behaviour.
    sub_kind : str | None
        Backend sub-kind detected at construction time (e.g.
        ``"pandas"`` / ``"matplotlib"`` / ``"sklearn"``). ``None`` for
        ``file`` assets.
    name : str | None
        Optional explicit local asset name. When omitted, the evaluator
        assigns a name based on the producing task function.
    metadata : dict[str, Any]
        Optional user-supplied metadata stored on the asset version.
    kind_fields : dict[str, Any]
        Internal bag carrying kind-specific construction-time fields
        (e.g. ``"format"`` for ``text`` / ``"framework"`` and
        ``"metrics"`` for ``model``). Populated by the factory from its
        typed keyword arguments; readers should use the well-known keys
        defined in the corresponding kind spec.
    """

    payload: Any
    kind: AssetKind = "file"
    sub_kind: str | None = None
    name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    kind_fields: dict[str, Any] = field(default_factory=dict)

    @property
    def path(self) -> Path:
        """Return the wrapped path as a :class:`Path`.

        Only valid for ``file`` assets and the path-backed sub-kinds
        (``csv``/``tsv``/``png``/``svg``/``html`` and ``Path`` text).
        Raises :class:`TypeError` for in-memory payloads.
        """
        payload = self.payload
        if isinstance(payload, (str, Path)):
            return Path(payload)
        raise TypeError(
            f"AssetResult(kind={self.kind!r}).path is only valid for path-backed "
            f"payloads; got in-memory {type(payload).__name__}."
        )


@dataclass(frozen=True, kw_only=True)
class AssetRef:
    """Resolved reference passed to downstream tasks.

    Parameters
    ----------
    key : AssetKey
        Logical asset identity.
    version_id : str
        Immutable version identifier.
    kind : str
        Physical asset kind.
    artifact_id : str
        Backing artifact identifier.
    content_hash : str
        Content hash of the stored bytes.
    artifact_path : str
        Absolute filesystem path to the immutable stored artifact.
    metadata : dict[str, Any]
        Asset metadata copied from the registered version.
    """

    key: AssetKey
    version_id: str
    kind: str
    artifact_id: str
    content_hash: str
    artifact_path: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def namespace(self) -> str:
        """Return the asset namespace."""
        return self.key.namespace

    @property
    def name(self) -> str:
        """Return the asset name."""
        return self.key.name

    def load(self) -> str:
        """Return the stored artifact path.

        Returns
        -------
        str
            Absolute path to the immutable artifact content.
        """
        return self.artifact_path

    def as_file(self) -> file:
        """Return the artifact path as a ``ginkgo.file`` marker."""
        return file(self.artifact_path)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON/YAML-safe mapping."""
        return {
            "key": self.key.to_dict(),
            "version_id": self.version_id,
            "kind": self.kind,
            "artifact_id": self.artifact_id,
            "content_hash": self.content_hash,
            "artifact_path": self.artifact_path,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AssetRef:
        """Build a reference from serialized metadata.

        Parameters
        ----------
        data : dict[str, Any]
            Serialized asset reference payload.

        Returns
        -------
        AssetRef
        """
        return cls(
            key=AssetKey.from_dict(data["key"]),
            version_id=str(data["version_id"]),
            kind=str(data["kind"]),
            artifact_id=str(data["artifact_id"]),
            content_hash=str(data["content_hash"]),
            artifact_path=str(data["artifact_path"]),
            metadata=dict(data.get("metadata", {})),
        )


def asset(
    payload: Any,
    *,
    kind: AssetKind = "file",
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
    **kind_fields: Any,
) -> AssetResult:
    """Wrap a task output for registration as an asset.

    Canonical constructor for every asset kind. ``asset(df, kind="table")``
    and :func:`table` produce identical :class:`AssetResult` values; the
    semantic factories are shorthand around this function.

    Parameters
    ----------
    payload : Any
        The value to register. For ``file`` assets this must be a
        path-like value. For the semantic kinds, this is the live
        object (DataFrame / ndarray / figure / text body / model) or a
        path to a sub-kind-specific file format.
    kind : str
        Asset kind. Defaults to ``"file"``. Must be one of the
        registered kinds (``file``/``table``/``array``/``fig``/``text``
        /``model``).
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-supplied metadata persisted on the asset version.
    **kind_fields : Any
        Kind-specific construction-time fields. For ``text`` this
        accepts ``format``; for ``model`` this accepts ``framework`` and
        ``metrics``. Other kinds accept no extra fields.

    Returns
    -------
    AssetResult
        Sentinel consumed by the evaluator after task execution.
    """
    if kind not in _VALID_KINDS:
        raise ValueError(f"asset() kind must be one of {sorted(_VALID_KINDS)}, got {kind!r}")

    # Late import breaks the import cycle: the registry imports AssetResult
    # from this module, but asset() needs to dispatch into the registry at
    # call time.
    from ginkgo.runtime.artifacts.asset_kinds import ASSET_KINDS

    spec = ASSET_KINDS[kind]
    normalised_payload, sub_kind, extra_kind_fields = spec.detect(payload, **kind_fields)
    return AssetResult(
        payload=normalised_payload,
        kind=kind,
        sub_kind=sub_kind,
        name=name,
        metadata=dict(metadata or {}),
        kind_fields=extra_kind_fields,
    )


def table(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
    """Wrap a tabular value as an asset return.

    Parameters
    ----------
    payload : Any
        The tabular value. Supports pandas DataFrame, polars
        DataFrame/LazyFrame, pyarrow Table/Dataset, DuckDB relation, or
        a path to a CSV/TSV file.
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    AssetResult
    """
    return asset(payload, kind="table", name=name, metadata=metadata)


def array(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
    """Wrap an n-dimensional array value as an asset return.

    Parameters
    ----------
    payload : Any
        The array value. Supports numpy ndarray, xarray
        DataArray/Dataset, zarr array/group, and dask array.
    name : str | None
        Optional explicit local asset name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata stored with the asset version.

    Returns
    -------
    AssetResult
    """
    return asset(payload, kind="array", name=name, metadata=metadata)


def fig(
    payload: Any,
    *,
    name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
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
    AssetResult
    """
    return asset(payload, kind="fig", name=name, metadata=metadata)


def text(
    payload: Any,
    *,
    name: str | None = None,
    format: Literal["plain", "markdown", "json"] | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
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
    AssetResult
    """
    return asset(payload, kind="text", name=name, metadata=metadata, format=format)


def model(
    payload: Any,
    *,
    name: str | None = None,
    framework: str | None = None,
    metrics: dict[str, float] | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
    """Wrap a trained model as an asset return.

    Parameters
    ----------
    payload : Any
        The trained model object. Supports scikit-learn estimators,
        XGBoost and LightGBM sklearn-wrapped models, PyTorch
        ``nn.Module`` instances, and Keras/TensorFlow models.
    name : str | None
        Optional explicit local asset name.
    framework : str | None
        Optional explicit framework override, bypassing module-based
        detection. Must be one of ``"sklearn"``, ``"xgboost"``,
        ``"lightgbm"``, ``"pytorch"``, ``"keras"``.
    metrics : dict[str, float] | None
        Optional scalar metrics captured at training time. Stored as a
        first-class field on the asset version for ``ginkgo models`` and
        UI rendering.
    metadata : dict[str, Any] | None
        Optional free-form metadata stored on the asset version.

    Returns
    -------
    AssetResult
    """
    return asset(
        payload,
        kind="model",
        name=name,
        metadata=metadata,
        framework=framework,
        metrics=metrics,
    )


def make_asset_version_id(
    *,
    key: AssetKey,
    content_hash: str,
    run_id: str,
) -> str:
    """Return a stable asset version identifier.

    Parameters
    ----------
    key : AssetKey
        Logical asset identity.
    content_hash : str
        Content hash of the stored bytes.
    run_id : str
        Producing run identifier.

    Returns
    -------
    str
        Hex-encoded version identifier.
    """
    payload = {
        "asset": key.to_dict(),
        "content_hash": content_hash,
        "run_id": run_id,
    }
    return hash_str(repr(payload))


def make_asset_version(
    *,
    key: AssetKey,
    kind: str,
    artifact_id: str,
    content_hash: str,
    run_id: str,
    producer_task: str,
    metadata: dict[str, Any] | None = None,
) -> AssetVersion:
    """Build an immutable asset version record.

    Parameters
    ----------
    key : AssetKey
        Logical asset identity.
    kind : str
        Physical asset kind.
    artifact_id : str
        Backing artifact identifier.
    content_hash : str
        Hash of the stored bytes.
    run_id : str
        Producing run identifier.
    producer_task : str
        Fully-qualified task name.
    metadata : dict[str, Any] | None
        Optional user-defined metadata.

    Returns
    -------
    AssetVersion
    """
    return AssetVersion(
        key=key,
        version_id=make_asset_version_id(key=key, content_hash=content_hash, run_id=run_id),
        kind=kind,
        artifact_id=artifact_id,
        content_hash=content_hash,
        run_id=run_id,
        producer_task=producer_task,
        created_at=datetime.now(UTC).isoformat(),
        metadata=dict(metadata or {}),
    )


def asset_ref_from_version(*, version: AssetVersion, artifact_path: str | Path) -> AssetRef:
    """Create an :class:`AssetRef` from one registered version.

    Parameters
    ----------
    version : AssetVersion
        Registered asset version.
    artifact_path : str | Path
        Absolute immutable artifact path.

    Returns
    -------
    AssetRef
    """
    return AssetRef(
        key=version.key,
        version_id=version.version_id,
        kind=version.kind,
        artifact_id=version.artifact_id,
        content_hash=version.content_hash,
        artifact_path=str(artifact_path),
        metadata=dict(version.metadata),
    )


def collect_asset_refs(value: Any) -> list[AssetRef]:
    """Collect nested asset references from a Python value.

    Parameters
    ----------
    value : Any
        Arbitrary nested value.

    Returns
    -------
    list[AssetRef]
        Flat list of discovered asset references.
    """
    if isinstance(value, AssetRef):
        return [value]
    if isinstance(value, list | tuple):
        refs: list[AssetRef] = []
        for item in value:
            refs.extend(collect_asset_refs(item))
        return refs
    if isinstance(value, dict):
        refs: list[AssetRef] = []
        for item in value.values():
            refs.extend(collect_asset_refs(item))
        return refs
    return []
