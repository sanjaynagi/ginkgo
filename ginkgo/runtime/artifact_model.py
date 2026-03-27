"""Data model for content-addressed artifacts.

Defines the immutable reference types and metadata records used by the
artifact store.  All artifact identity flows through these types rather
than bare strings.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, kw_only=True)
class BlobRef:
    """Reference to a single content-addressed blob.

    Parameters
    ----------
    digest_algorithm : str
        Hash algorithm used (e.g. ``"blake3"``).
    digest_hex : str
        Hex-encoded content digest.
    size : int
        Byte count of the stored blob.
    extension : str
        Original file extension including the leading dot (e.g. ``".csv"``),
        or empty string if the source had no extension.
    """

    digest_algorithm: str
    digest_hex: str
    size: int
    extension: str


@dataclass(frozen=True, kw_only=True)
class TreeEntry:
    """One file entry in a tree manifest.

    Parameters
    ----------
    relative_path : str
        POSIX-style relative path within the directory.
    blob_digest : str
        Hex digest of the file content.
    size : int
        Byte count of the file.
    mode : int
        Original file permission mode.
    """

    relative_path: str
    blob_digest: str
    size: int
    mode: int


@dataclass(frozen=True, kw_only=True)
class TreeRef:
    """Reference to a directory manifest.

    Parameters
    ----------
    digest_algorithm : str
        Hash algorithm used for the manifest digest.
    digest_hex : str
        Hex digest of the serialized manifest content.
    entries : tuple[TreeEntry, ...]
        Ordered file entries that compose the directory.
    """

    digest_algorithm: str
    digest_hex: str
    entries: tuple[TreeEntry, ...]


@dataclass(frozen=True, kw_only=True)
class ArtifactRecord:
    """Metadata record persisted alongside stored content.

    Parameters
    ----------
    artifact_id : str
        Unique artifact identifier (content digest for managed artifacts).
    kind : str
        ``"blob"`` or ``"tree"``.
    digest_algorithm : str
        Hash algorithm used.
    digest_hex : str
        Hex-encoded content digest.
    extension : str
        Original file extension for blobs, empty string for trees.
    size : int
        Total byte count.
    created_at : str
        ISO-8601 creation timestamp.
    storage_backend : str
        Storage backend identifier (``"local"`` for now).
    """

    artifact_id: str
    kind: str
    digest_algorithm: str
    digest_hex: str
    extension: str
    size: int
    created_at: str
    storage_backend: str
    remote_uri: str | None = None

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Returns
        -------
        str
            JSON representation with sorted keys.
        """
        return json.dumps(asdict(self), indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> ArtifactRecord:
        """Deserialize from a JSON string.

        Parameters
        ----------
        data : str
            JSON string produced by :meth:`to_json`.

        Returns
        -------
        ArtifactRecord
        """
        return cls(**json.loads(data))

    @classmethod
    def from_path(cls, path: Path) -> ArtifactRecord:
        """Load from a JSON file on disk.

        Parameters
        ----------
        path : Path
            Path to the JSON file.

        Returns
        -------
        ArtifactRecord
        """
        return cls.from_json(path.read_text(encoding="utf-8"))


def serialize_tree_manifest(ref: TreeRef) -> str:
    """Serialize a tree manifest to JSON.

    Parameters
    ----------
    ref : TreeRef
        Tree reference to serialize.

    Returns
    -------
    str
        JSON string with sorted keys.
    """
    payload = {
        "digest_algorithm": ref.digest_algorithm,
        "digest_hex": ref.digest_hex,
        "entries": [asdict(entry) for entry in ref.entries],
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def deserialize_tree_manifest(data: str) -> TreeRef:
    """Deserialize a tree manifest from JSON.

    Parameters
    ----------
    data : str
        JSON string produced by :func:`serialize_tree_manifest`.

    Returns
    -------
    TreeRef
    """
    payload = json.loads(data)
    entries = tuple(TreeEntry(**entry) for entry in payload["entries"])
    return TreeRef(
        digest_algorithm=payload["digest_algorithm"],
        digest_hex=payload["digest_hex"],
        entries=entries,
    )
