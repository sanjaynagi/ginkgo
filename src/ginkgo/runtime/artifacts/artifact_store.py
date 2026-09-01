"""Content-addressed artifact storage for Ginkgo.

The ``ArtifactStore`` protocol defines the contract for storing and retrieving
binary artifacts.  ``LocalArtifactStore`` is the default implementation that
stores artifacts on the local filesystem under ``.ginkgo/artifacts/``.

Storage layout::

    .ginkgo/artifacts/
      blobs/<digest><ext>         # raw file bytes, read-only
      trees/<tree_digest>.json    # directory manifest

A recorded blob keeps its original file extension in its store filename, so a
logged command shows what the bytes are and a consumer that dispatches on
``Path.suffix`` (ImageMagick, a viewer) is not surprised by a bare digest
(#231). A tree's member blobs are named by digest alone: they are reached
through the manifest and materialise under their real relative paths, so the
extension would inform nobody.

The metadata record for each artifact — kind, size, extension, where it was
published — is a row in the ``artifacts`` table rather than a file beside the
bytes, so one index answers "what is in the store" and a garbage collector can
join it against the cache and asset tables in one query.
"""

from __future__ import annotations


import shutil
import stat
from pathlib import Path
from typing import Protocol, runtime_checkable

from ginkgo.runtime.artifacts.artifact_model import (
    ArtifactRecord,
    TreeEntry,
    TreeRef,
    deserialize_tree_manifest,
    serialize_tree_manifest,
)
from ginkgo.runtime.artifacts.fs_share import share_bytes
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.core.hashing import hash_bytes, hash_file
from ginkgo.formatting import now_iso
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import WorkspaceLayout


DIGEST_ALGORITHM = "blake3"

# Longer than any real file extension; the digest already spends 64 of the
# filename's 255 bytes, so an unbounded suffix could make store() fail.
_MAX_EXTENSION_LEN = 32


@runtime_checkable
class ArtifactStore(Protocol):
    """Protocol for content-addressed artifact storage."""

    def store(
        self,
        *,
        src_path: Path,
        src_is_readonly: bool = False,
    ) -> ArtifactRecord:
        """Copy bytes into the store and return an artifact record.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.
        src_is_readonly : bool
            When ``True`` the caller guarantees the source will not be
            mutated. The store may then hardlink instead of copying.
            Leave ``False`` for user-produced paths (task outputs).

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
    hash_memo : HashMemo | None
        Shared content-hash memo, so a file hashed elsewhere in the run is
        not read again here.
    index : CacheIndex
        The database rows recording what the store holds. Required, and never
        opened here: a read path passes a reader, a remote worker passes an
        in-memory index, and neither should have a database created for it as
        the side effect of constructing a store.
    """

    def __init__(
        self,
        *,
        index: CacheIndex,
        root: Path | None = None,
        hash_memo: HashMemo | None = None,
    ) -> None:
        self._root = root if root is not None else WorkspaceLayout.for_cwd().artifacts
        self._blobs_dir = self._root / "blobs"
        self._trees_dir = self._root / "trees"
        self._hash_memo = hash_memo
        self._index = index
        for directory in (self._blobs_dir, self._trees_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def store(
        self,
        *,
        src_path: Path,
        src_is_readonly: bool = False,
    ) -> ArtifactRecord:
        """Copy a file or directory into the store.

        Parameters
        ----------
        src_path : Path
            Source file or directory to store.
        src_is_readonly : bool
            When ``True`` the caller guarantees the source will not be
            mutated, allowing the store to hardlink in preference to a
            full copy when reflink is unavailable. Leave ``False`` for
            user-produced paths.

        Returns
        -------
        ArtifactRecord
            Metadata record for the stored artifact.
        """
        if src_path.is_dir():
            record = self._store_directory(src_path, src_is_readonly=src_is_readonly)
        else:
            record = self._store_file(src_path, src_is_readonly=src_is_readonly)
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
        record = self._load_record(artifact_id=artifact_id)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Clean up any existing path at dest.
        _remove_dest(dest_path)

        if record.kind == "blob":
            dest_path.symlink_to(self._blob_path(record))
        else:
            self._retrieve_tree(artifact_id=artifact_id, dest_path=dest_path)

    def restore(self, *, artifact_id: str, dest_path: Path) -> None:
        """Restore an artifact at *dest_path* as regular writable content."""
        record = self._load_record(artifact_id=artifact_id)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove any prior materialization before restoring fresh content.
        _remove_dest(dest_path)

        if record.kind == "blob":
            shutil.copy2(self._blob_path(record), dest_path)
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
            if self._index.materialization_matches(path=path, artifact_id=artifact_id):
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
        return self._index.artifact(artifact_id) is not None

    def load_record(self, *, artifact_id: str) -> ArtifactRecord | None:
        """Return the stored metadata record for one artifact.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to look up.

        Returns
        -------
        ArtifactRecord | None
            The record, or ``None`` when the artifact is not in the store.
        """
        return self._index.artifact(artifact_id)

    def list_artifact_ids(self) -> list[str]:
        """Return the IDs of every artifact currently in the store.

        Returns
        -------
        list[str]
            Sorted artifact IDs.
        """
        return self._index.artifact_ids()

    def delete(self, *, artifact_id: str) -> None:
        """Remove an artifact from the store.

        Parameters
        ----------
        artifact_id : str
            The artifact ID to remove.
        """
        record = self._index.artifact(artifact_id)
        if record is None:
            return

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
            blob_path = self._blob_path(record)
            if blob_path.exists():
                blob_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
                blob_path.unlink()

        self._index.forget_artifact(artifact_id)

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
        record = self._index.artifact(artifact_id)
        if record is None:
            return self._blobs_dir / artifact_id

        if record.kind == "blob":
            return self._blob_path(record)
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
        ext = f".{extension}" if extension else ""
        record = self._existing_blob_record(digest=digest, extension=ext, size=len(data))
        blob_path = self._blob_path(record)

        if not blob_path.exists():
            if not self._share_from_sibling(digest=digest, dst=blob_path):
                blob_path.write_bytes(data)
            blob_path.chmod(_READ_ONLY_FILE)

        self.put_record(record)
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
        record = self._index.artifact(artifact_id)
        blob_path = (
            self._blob_path(record) if record is not None else self._blobs_dir / artifact_id
        )

        if not blob_path.exists():
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")
        return blob_path.read_bytes()

    # -- internal helpers --------------------------------------------------

    def _store_file(
        self,
        src_path: Path,
        *,
        src_is_readonly: bool = False,
    ) -> ArtifactRecord:
        """Store a single file as a blob."""
        digest = self._hash_file(src_path)
        record = self._existing_blob_record(
            digest=digest, extension=src_path.suffix, size=src_path.stat().st_size
        )
        blob_path = self._blob_path(record)

        if not blob_path.exists():
            # chmod on a hardlinked blob also flips the source's mode;
            # that is intentional when ``src_is_readonly`` is set — the
            # caller has promised immutability — and enforces the
            # store's read-only invariant on the shared inode.
            if not self._share_from_sibling(digest=digest, dst=blob_path):
                share_bytes(
                    src=src_path,
                    dst=blob_path,
                    allow_hardlink=src_is_readonly,
                )
            blob_path.chmod(_READ_ONLY_FILE)

        self.put_record(record)
        return record

    def _store_directory(
        self,
        src_path: Path,
        *,
        src_is_readonly: bool = False,
    ) -> ArtifactRecord:
        """Store a directory as individual blobs plus a tree manifest."""
        tree_ref, total_size = self._build_tree_ref(src_path)

        real_src = src_path.resolve()

        # Store the blob content for each manifest entry.
        for entry in tree_ref.entries:
            child = real_src / Path(entry.relative_path)
            # Store the blob.
            blob_path = self._blobs_dir / entry.blob_digest
            if not blob_path.exists():
                if not self._share_from_sibling(digest=entry.blob_digest, dst=blob_path):
                    share_bytes(src=child, dst=blob_path, allow_hardlink=src_is_readonly)
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
            created_at=now_iso(),
            storage_backend="local",
        )
        self.put_record(record)
        return record

    def _blob_path(self, record: ArtifactRecord) -> Path:
        """Return the store path for a blob record: its digest plus extension."""
        return self._blobs_dir / f"{record.digest_hex}{record.extension}"

    def _share_from_sibling(self, *, digest: str, dst: Path) -> bool:
        """Populate *dst* from another store filename the same bytes live under.

        A recorded blob (``<digest><ext>``) and a tree member (``<digest>``)
        name the same content differently, so storing one after the other
        would otherwise write the bytes to disk twice. Blobs are immutable,
        so the copies can share an inode via hardlink instead.
        """
        candidates = [self._blobs_dir / digest]
        existing = self._index.artifact(digest)
        if existing is not None and existing.kind == "blob":
            candidates.append(self._blob_path(existing))
        for src in candidates:
            if src != dst and src.exists():
                share_bytes(src=src, dst=dst, allow_hardlink=True)
                return True
        return False

    def _existing_blob_record(self, *, digest: str, extension: str, size: int) -> ArtifactRecord:
        """Return the record a blob store should write bytes against.

        The store is content-addressed, so the digest is the identity; the
        extension is only the filename's label. When the same bytes arrive
        twice under two names, the first record wins — re-labelling would
        strand the earlier file as an orphan the integrity check then flags.
        The check is in-process only: two processes storing the same bytes
        under different names can still each write their own file, and the
        later record wins.
        """
        existing = self._index.artifact(digest)
        if existing is not None and existing.kind == "blob":
            return existing
        if len(extension) > _MAX_EXTENSION_LEN:
            # A "suffix" this long is not an extension, and the digest
            # already spends 64 of the filename's 255 bytes.
            extension = ""
        return ArtifactRecord(
            artifact_id=digest,
            kind="blob",
            digest_algorithm=DIGEST_ALGORITHM,
            digest_hex=digest,
            extension=extension,
            size=size,
            created_at=now_iso(),
            storage_backend="local",
        )

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
        self._index.record_materialization(path=path, artifact_id=artifact_id)

    def _tree_digest_for_path(self, path: Path) -> str:
        """Return the manifest digest for a directory path."""
        tree_ref, _ = self._build_tree_ref(path)
        return tree_ref.digest_hex

    def _load_record(self, *, artifact_id: str) -> ArtifactRecord:
        """Load one artifact record or raise if it does not exist."""
        record = self.load_record(artifact_id=artifact_id)
        if record is None:
            raise FileNotFoundError(f"Artifact not found in store: {artifact_id}")
        return record

    def _load_tree_ref(self, *, record: ArtifactRecord) -> TreeRef:
        """Load the tree manifest for one directory artifact."""
        tree_path = self._trees_dir / f"{record.digest_hex}.json"
        if not tree_path.exists():
            raise FileNotFoundError(f"Tree manifest not found: {record.digest_hex}")
        return deserialize_tree_manifest(tree_path.read_text(encoding="utf-8"))

    def put_record(self, record: ArtifactRecord) -> None:
        """Record an artifact this store now holds.

        Public because a remote store that downloaded the bytes into this
        store's directories has to say so; local writes go through
        :meth:`store` and :meth:`store_bytes`.
        """
        self._index.record_artifact(record)

    def integrity_problems(self) -> list[str]:
        """Return the ways the artifact rows and the bytes on disk disagree.

        Both directions: a row whose blob or tree manifest is gone names an
        artifact nothing can restore, and a file in ``blobs/`` or ``trees/``
        with no row is bytes nothing can find — the digest that would name them
        only exists in the row. A tree's member blobs are reachable through its
        manifest rather than through a row of their own, so they count as known.
        """
        problems: list[str] = []
        known: set[Path] = set()
        for artifact_id in self.list_artifact_ids():
            record = self._index.artifact(artifact_id)
            path = self.artifact_path(artifact_id=artifact_id)
            known.add(path)
            if not path.exists():
                problems.append(f"artifact {artifact_id} has a row but no bytes")
                continue
            if record is not None and record.kind != "blob":
                tree = self._load_tree_ref(record=record)
                known.update(self._blobs_dir / entry.blob_digest for entry in tree.entries)

        for directory in (self._blobs_dir, self._trees_dir):
            if not directory.is_dir():
                continue
            problems += [
                f"artifact file {directory.name}/{entry.name} has no row (orphan)"
                for entry in sorted(directory.iterdir())
                if entry.is_file() and entry not in known
            ]
        return problems

    def materialized_artifact_id(self, *, path: Path) -> str | None:
        """Return the artifact *path* holds, if it still has the recorded stat.

        The counterpart of the materialization :meth:`store` records: it lets a
        later run recognise an unchanged input without re-hashing it.
        """
        return self._index.materialized_artifact_id(path=path)


# -- module-level helpers --------------------------------------------------

_READ_ONLY_FILE = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH  # 0o444


def _remove_dest(dest_path: Path) -> None:
    """Remove an existing file, symlink, or directory at *dest_path*."""
    if dest_path.is_symlink():
        dest_path.unlink()
    elif dest_path.is_dir():
        shutil.rmtree(dest_path)
    elif dest_path.exists():
        dest_path.unlink()


def make_writable_recursive(path: Path) -> None:
    """Restore write permissions on a read-only directory tree before deletion."""
    for child in path.rglob("*"):
        if child.is_dir():
            child.chmod(stat.S_IRWXU)
        else:
            child.chmod(stat.S_IRUSR | stat.S_IWUSR)
    path.chmod(stat.S_IRWXU)
