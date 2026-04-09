"""Content-addressed artifact storage for Ginkgo.

The ``ArtifactStore`` protocol defines the contract for storing and retrieving
binary artifacts.  ``LocalArtifactStore`` is the default implementation that
stores artifacts on the local filesystem under ``.ginkgo/artifacts/``.

Storage layout::

    .ginkgo/artifacts/
      blobs/<digest>              # raw file bytes, read-only
      trees/<tree_digest>.json    # directory manifest
      refs/<artifact_id>.json     # artifact metadata record
"""

from __future__ import annotations

import shutil
import stat
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

from ginkgo.runtime.artifacts.artifact_model import (
    ArtifactRecord,
    TreeEntry,
    TreeRef,
    deserialize_tree_manifest,
    serialize_tree_manifest,
)
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.runtime.caching.hashing import hash_bytes, hash_file
from ginkgo.runtime.caching.materialization_log import MaterializationLog


DIGEST_ALGORITHM = "blake3"


@runtime_checkable
class ArtifactStore(Protocol):
    """Protocol for content-addressed artifact storage."""

    def store(self, *, src_path: Path) -> ArtifactRecord:
        """Copy bytes into the store and return an artifact record.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.

        Returns
        -------
        ArtifactRecord
            Metadata record for the stored artifact.
        """
        ...

    def retrieve(self, *, artifact_id: str, dest_path: Path) -> None:
        """Materialise an artifact at *dest_path* as a symlink.

        Parameters
        ----------
        artifact_id : str
            The artifact ID returned by :meth:`store`.
        dest_path : Path
            Location where the symlink should be created.
        """
        ...

    def restore(self, *, artifact_id: str, dest_path: Path) -> None:
        """Materialise an artifact at *dest_path* as writable content.

        Parameters
        ----------
        artifact_id : str
            The artifact ID returned by :meth:`store`.
        dest_path : Path
            Location where the writable file or directory should be restored.
        """
        ...

    def matches(self, *, artifact_id: str, path: Path) -> bool:
        """Return whether *path* matches the stored artifact content.

        Parameters
        ----------
        artifact_id : str
            The artifact ID returned by :meth:`store`.
        path : Path
            Existing working-tree path to compare.

        Returns
        -------
        bool
        """
        ...

    def exists(self, *, artifact_id: str) -> bool:
        """Return whether an artifact exists in the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to check.

        Returns
        -------
        bool
        """
        ...

    def delete(self, *, artifact_id: str) -> None:
        """Remove an artifact from the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to remove.
        """
        ...

    def artifact_path(self, *, artifact_id: str) -> Path:
        """Return the absolute filesystem path for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        Path
        """
        ...

    def store_bytes(self, *, data: bytes, extension: str) -> ArtifactRecord:
        """Store raw bytes and return an artifact record.

        Parameters
        ----------
        data : bytes
            Raw bytes to store.
        extension : str
            File extension (without leading dot).

        Returns
        -------
        ArtifactRecord
        """
        ...

    def read_bytes(self, *, artifact_id: str) -> bytes:
        """Read raw bytes for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        bytes
        """
        ...


class LocalArtifactStore:
    """Local filesystem artifact store using blob/tree CAS layout.

    Parameters
    ----------
    root : Path
        Root directory for artifact storage.  Defaults to
        ``.ginkgo/artifacts`` under the current working directory.
    """

    def __init__(
        self,
        *,
        root: Path | None = None,
        hash_memo: HashMemo | None = None,
        materialization_log: MaterializationLog | None = None,
    ) -> None:
        self._root = root if root is not None else Path.cwd() / ".ginkgo" / "artifacts"
        self._blobs_dir = self._root / "blobs"
        self._trees_dir = self._root / "trees"
        self._refs_dir = self._root / "refs"
        self._hash_memo = hash_memo
        self._materialization_log = materialization_log
        for directory in (self._blobs_dir, self._trees_dir, self._refs_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def store(self, *, src_path: Path) -> ArtifactRecord:
        """Copy a file or directory into the store.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.

        Returns
        -------
        ArtifactRecord
            Metadata record for the stored artifact.
        """
        if src_path.is_dir():
            record = self._store_directory(src_path)
        else:
            record = self._store_file(src_path)
        self._record_materialization(path=src_path, artifact_id=record.artifact_id)
        return record

    def retrieve(self, *, artifact_id: str, dest_path: Path) -> None:
        """Create a symlink at *dest_path* pointing to the stored artifact.

        For blob artifacts, creates a symlink to the blob file.  For tree
        artifacts, reconstructs the directory by creating symlinks from each
        manifest entry to its corresponding blob.

        Parameters
        ----------
        artifact_id : str
            Artifact ID returned by :meth:`store`.
        dest_path : Path
            Target symlink location.
        """
        ref_path = self._refs_dir / f"{artifact_id}.json"
        if not ref_path.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")

        record = ArtifactRecord.from_path(ref_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Clean up any existing path at dest.
        _remove_dest(dest_path)

        if record.kind == "blob":
            blob_path = self._blobs_dir / record.digest_hex
            dest_path.symlink_to(blob_path)
        else:
            self._retrieve_tree(artifact_id=artifact_id, dest_path=dest_path)

    def restore(self, *, artifact_id: str, dest_path: Path) -> None:
        """Restore an artifact at *dest_path* as regular writable content."""
        record = self._load_record(artifact_id=artifact_id)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove any prior materialization before restoring fresh content.
        _remove_dest(dest_path)

        if record.kind == "blob":
            blob_path = self._blobs_dir / record.digest_hex
            shutil.copy2(blob_path, dest_path)
            dest_path.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
            self._record_materialization(path=dest_path, artifact_id=artifact_id)
            return

        self._restore_tree(record=record, dest_path=dest_path)
        self._record_materialization(path=dest_path, artifact_id=artifact_id)

    def matches(self, *, artifact_id: str, path: Path) -> bool:
        """Return whether *path* matches the stored artifact content."""
        if not path.exists():
            return False

        record = self._load_record(artifact_id=artifact_id)
        if record.kind == "blob":
            if not path.is_file():
                return False
            # Stat-gated fast path: reliable for files because any content
            # change updates the file's own mtime.  Not used for directories
            # because a directory's mtime only changes on add/remove, not on
            # child content modification.
            if self._materialization_log is not None and self._materialization_log.check(
                path=path, artifact_id=artifact_id
            ):
                return True
            return self._hash_file(path) == record.digest_hex

        if not path.is_dir():
            return False
        return self._tree_digest_for_path(path) == record.digest_hex

    def exists(self, *, artifact_id: str) -> bool:
        """Check whether an artifact exists in the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to check.

        Returns
        -------
        bool
        """
        return (self._refs_dir / f"{artifact_id}.json").exists()

    def delete(self, *, artifact_id: str) -> None:
        """Remove an artifact from the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to remove.
        """
        ref_path = self._refs_dir / f"{artifact_id}.json"
        if not ref_path.exists():
            return

        record = ArtifactRecord.from_path(ref_path)

        if record.kind == "tree":
            # Remove tree manifest.
            tree_path = self._trees_dir / f"{record.digest_hex}.json"
            if tree_path.exists():
                tree_path.unlink()

        # Remove blob(s).  For trees, only remove blobs not referenced
        # by other artifacts.  For simplicity in the local case we remove
        # the blob unconditionally -- orphaned blob cleanup can be added
        # later if needed.
        if record.kind == "blob":
            blob_path = self._blobs_dir / record.digest_hex
            if blob_path.exists():
                blob_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
                blob_path.unlink()

        ref_path.unlink()

    def artifact_path(self, *, artifact_id: str) -> Path:
        """Return the absolute path for an artifact's primary content.

        For blobs, returns the blob file path.  For trees, returns the
        blob directory (callers should use :meth:`retrieve` instead for
        tree artifacts).

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        Path
        """
        ref_path = self._refs_dir / f"{artifact_id}.json"
        if not ref_path.exists():
            return self._blobs_dir / artifact_id

        record = ArtifactRecord.from_path(ref_path)
        if record.kind == "blob":
            return self._blobs_dir / record.digest_hex
        return self._trees_dir / f"{record.digest_hex}.json"

    def store_bytes(self, *, data: bytes, extension: str) -> ArtifactRecord:
        """Store raw bytes, returning an artifact record.

        Parameters
        ----------
        data : bytes
            Raw bytes to store.
        extension : str
            File extension (without leading dot).

        Returns
        -------
        ArtifactRecord
        """
        digest = hash_bytes(data)
        blob_path = self._blobs_dir / digest

        if not blob_path.exists():
            blob_path.write_bytes(data)
            blob_path.chmod(_READ_ONLY_FILE)

        ext = f".{extension}" if extension else ""
        record = ArtifactRecord(
            artifact_id=digest,
            kind="blob",
            digest_algorithm=DIGEST_ALGORITHM,
            digest_hex=digest,
            extension=ext,
            size=len(data),
            created_at=_now_iso(),
            storage_backend="local",
        )
        self._write_ref(record)
        return record

    def read_bytes(self, *, artifact_id: str) -> bytes:
        """Read raw bytes for an artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID.

        Returns
        -------
        bytes
        """
        ref_path = self._refs_dir / f"{artifact_id}.json"
        if ref_path.exists():
            record = ArtifactRecord.from_path(ref_path)
            blob_path = self._blobs_dir / record.digest_hex
        else:
            blob_path = self._blobs_dir / artifact_id

        if not blob_path.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")
        return blob_path.read_bytes()

    # -- internal helpers --------------------------------------------------

    def _store_file(self, src_path: Path) -> ArtifactRecord:
        """Store a single file as a blob."""
        digest = self._hash_file(src_path)
        blob_path = self._blobs_dir / digest

        if not blob_path.exists():
            shutil.copy2(src_path, blob_path)
            blob_path.chmod(_READ_ONLY_FILE)

        size = blob_path.stat().st_size
        ext = src_path.suffix  # includes leading dot

        record = ArtifactRecord(
            artifact_id=digest,
            kind="blob",
            digest_algorithm=DIGEST_ALGORITHM,
            digest_hex=digest,
            extension=ext,
            size=size,
            created_at=_now_iso(),
            storage_backend="local",
        )
        self._write_ref(record)
        return record

    def _store_directory(self, src_path: Path) -> ArtifactRecord:
        """Store a directory as individual blobs plus a tree manifest."""
        tree_ref, total_size = self._build_tree_ref(src_path)

        real_src = src_path.resolve()

        # Store the blob content for each manifest entry.
        for entry in tree_ref.entries:
            child = real_src / Path(entry.relative_path)
            # Store the blob.
            blob_path = self._blobs_dir / entry.blob_digest
            if not blob_path.exists():
                shutil.copy2(child, blob_path)
                blob_path.chmod(_READ_ONLY_FILE)

        tree_path = self._trees_dir / f"{tree_ref.digest_hex}.json"
        manifest_json = serialize_tree_manifest(tree_ref)
        tree_path.write_text(manifest_json, encoding="utf-8")

        record = ArtifactRecord(
            artifact_id=tree_ref.digest_hex,
            kind="tree",
            digest_algorithm=DIGEST_ALGORITHM,
            digest_hex=tree_ref.digest_hex,
            extension="",
            size=total_size,
            created_at=_now_iso(),
            storage_backend="local",
        )
        self._write_ref(record)
        return record

    def _retrieve_tree(self, *, artifact_id: str, dest_path: Path) -> None:
        """Reconstruct a directory from its tree manifest."""
        record = self._load_record(artifact_id=artifact_id)
        tree_ref = self._load_tree_ref(record=record)

        dest_path.mkdir(parents=True, exist_ok=True)

        for entry in tree_ref.entries:
            entry_dest = dest_path / entry.relative_path
            entry_dest.parent.mkdir(parents=True, exist_ok=True)
            blob_path = self._blobs_dir / entry.blob_digest

            # Symlink each file to its blob.
            if entry_dest.is_symlink() or entry_dest.exists():
                entry_dest.unlink()
            entry_dest.symlink_to(blob_path)

    def _restore_tree(self, *, record: ArtifactRecord, dest_path: Path) -> None:
        """Reconstruct a directory from its tree manifest as writable files."""
        tree_ref = self._load_tree_ref(record=record)
        dest_path.mkdir(parents=True, exist_ok=True)

        for entry in tree_ref.entries:
            entry_dest = dest_path / entry.relative_path
            entry_dest.parent.mkdir(parents=True, exist_ok=True)
            blob_path = self._blobs_dir / entry.blob_digest
            shutil.copy2(blob_path, entry_dest)
            entry_dest.chmod(entry.mode)

    def _build_tree_ref(self, src_path: Path) -> tuple[TreeRef, int]:
        """Return the manifest representation for a directory."""
        real_src = src_path.resolve()
        entries: list[TreeEntry] = []
        total_size = 0

        # Walk files in sorted order for deterministic manifests.
        for child in sorted(real_src.rglob("*"), key=lambda p: str(p.relative_to(real_src))):
            if child.is_dir():
                continue

            rel = child.relative_to(real_src).as_posix()
            digest = self._hash_file(child)
            file_size = child.stat().st_size
            file_mode = child.stat().st_mode & 0o777
            entries.append(
                TreeEntry(
                    relative_path=rel,
                    blob_digest=digest,
                    size=file_size,
                    mode=file_mode,
                )
            )
            total_size += file_size

        placeholder = TreeRef(
            digest_algorithm=DIGEST_ALGORITHM,
            digest_hex="",
            entries=tuple(entries),
        )
        tree_digest = hash_bytes(serialize_tree_manifest(placeholder).encode("utf-8"))
        return (
            TreeRef(
                digest_algorithm=DIGEST_ALGORITHM,
                digest_hex=tree_digest,
                entries=tuple(entries),
            ),
            total_size,
        )

    def _hash_file(self, path: Path) -> str:
        """Hash a file, using run-scoped memoization when available."""
        if self._hash_memo is not None:
            return self._hash_memo.hash_file(path)
        return hash_file(path)

    def _record_materialization(self, *, path: Path, artifact_id: str) -> None:
        """Record stat metadata for a materialized artifact path."""
        if self._materialization_log is not None:
            self._materialization_log.record(path=path, artifact_id=artifact_id)

    def _tree_digest_for_path(self, path: Path) -> str:
        """Return the manifest digest for a directory path."""
        tree_ref, _ = self._build_tree_ref(path)
        return tree_ref.digest_hex

    def _load_record(self, *, artifact_id: str) -> ArtifactRecord:
        """Load one artifact record or raise if it does not exist."""
        ref_path = self._refs_dir / f"{artifact_id}.json"
        if not ref_path.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")
        return ArtifactRecord.from_path(ref_path)

    def _load_tree_ref(self, *, record: ArtifactRecord) -> TreeRef:
        """Load the tree manifest for one directory artifact."""
        tree_path = self._trees_dir / f"{record.digest_hex}.json"
        if not tree_path.exists():
            raise FileNotFoundError(f"Tree manifest not found: {record.digest_hex}")
        return deserialize_tree_manifest(tree_path.read_text(encoding="utf-8"))

    def _write_ref(self, record: ArtifactRecord) -> None:
        """Write an artifact metadata record to the refs directory."""
        ref_path = self._refs_dir / f"{record.artifact_id}.json"
        ref_path.write_text(record.to_json(), encoding="utf-8")


# -- module-level helpers --------------------------------------------------

_READ_ONLY_FILE = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH  # 0o444


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _remove_dest(dest_path: Path) -> None:
    """Remove an existing file, symlink, or directory at *dest_path*."""
    if dest_path.is_symlink():
        dest_path.unlink()
    elif dest_path.is_dir():
        shutil.rmtree(dest_path)
    elif dest_path.exists():
        dest_path.unlink()


def _make_writable_recursive(path: Path) -> None:
    """Restore write permissions on a read-only directory tree before deletion."""
    for child in path.rglob("*"):
        if child.is_dir():
            child.chmod(stat.S_IRWXU)
        else:
            child.chmod(stat.S_IRUSR | stat.S_IWUSR)
    path.chmod(stat.S_IRWXU)
