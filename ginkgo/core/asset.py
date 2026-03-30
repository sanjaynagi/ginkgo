"""Asset identity and task-return wrappers.

Phase 7 introduces a narrow file-asset model layered over the existing
artifact store. Tasks return an :class:`AssetResult` sentinel via
``asset(...)`` and the evaluator replaces it with an :class:`AssetRef` once
the file has been registered in the catalog.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ginkgo.core.types import file
from ginkgo.runtime.hashing import hash_str


@dataclass(frozen=True, kw_only=True)
class AssetKey:
    """Stable logical identifier for one asset.

    Parameters
    ----------
    namespace : str
        Asset namespace. Phase 7 uses ``"file"`` only.
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
        Physical asset kind. Phase 7 supports ``"file"`` only.
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
    """Task-return sentinel produced by :func:`asset`.

    Parameters
    ----------
    value : str | Path
        Path-like value to register as an asset.
    name : str | None
        Optional logical asset name. When omitted, the evaluator uses the
        producing task function name.
    kind : str
        Asset kind. Phase 7 supports ``"file"`` only.
    metadata : dict[str, Any]
        Optional user-defined metadata stored with the version.
    """

    value: str | Path
    name: str | None = None
    kind: str = "file"
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def path(self) -> Path:
        """Return the wrapped path as a :class:`Path`."""
        return Path(self.value)


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
        Physical asset kind. Phase 7 supports ``"file"`` only.
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
    value: str | Path,
    *,
    name: str | None = None,
    kind: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AssetResult:
    """Wrap a task output so the evaluator registers it as an asset.

    Parameters
    ----------
    value : str | Path
        Path-like task output to register.
    name : str | None
        Optional logical asset name.
    kind : str | None
        Optional explicit asset kind. Only ``"file"`` is supported in
        Phase 7.
    metadata : dict[str, Any] | None
        Optional version metadata to persist in the catalog.

    Returns
    -------
    AssetResult
        Sentinel consumed by the evaluator after task execution.
    """
    resolved_kind = "file" if kind is None else kind
    if resolved_kind != "file":
        raise ValueError(f"asset() only supports kind='file' in Phase 7, got {resolved_kind!r}")
    if not isinstance(value, (str, Path)):
        raise TypeError(f"asset() expects a path-like value, got {type(value).__name__!r}")
    return AssetResult(
        value=value,
        name=name,
        kind=resolved_kind,
        metadata=dict(metadata or {}),
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
