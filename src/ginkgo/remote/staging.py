"""Local staging cache for remote artifacts.

Downloads remote files into a content-addressed local cache so that tasks
receive normal filesystem paths. ETag-based freshness checks avoid redundant
downloads on subsequent runs.

The bytes live on disk; what was staged, and from where, is a row in the
ledger's ``staging_entries`` table — one home for the fact, queryable by
``ginkgo db check`` alongside every other index.

Layout::

    .ginkgo/staging/
      blobs/<digest>              # cached file bytes
      folders/<folder_digest>/    # cached prefix downloads
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from ginkgo.config import load_runtime_config
from ginkgo.core.hashing import hash_file, hash_str
from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef
from ginkgo.formatting import now_iso
from ginkgo.remote.backend import ObjectStore, RemoteObjectMeta
from ginkgo.remote.resolve import resolve_backend
from ginkgo.store.direct_index import DirectIndex
from ginkgo.store.protocol import ProjectionOp
from ginkgo.store.sqlite import MEMORY
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = ["StagingCache", "StagingEntry", "StagingIndex"]


@dataclass(frozen=True, kw_only=True)
class StagingEntry:
    """What the staging cache knows about one staged remote URI.

    Parameters
    ----------
    uri : str
        Original remote URI.
    digest : str
        BLAKE3 content digest of the staged file, or the manifest digest of a
        staged folder.
    etag : str | None
        Provider ETag at time of download; ``None`` for a folder.
    version_id : str | None
        Provider version ID at time of download.
    size : int
        Total size in bytes of what was staged.
    staged_at : str
        ISO-8601 timestamp of when it was staged.
    blob_path : str
        Path within the staging cache, relative to its root.
    """

    uri: str
    digest: str
    etag: str | None
    version_id: str | None
    size: int
    staged_at: str
    blob_path: str


class StagingIndex(DirectIndex):
    """The ``staging_entries`` table: what has been downloaded, and from where.

    One row per remote URI. The row is the only record — the bytes beside it
    are content-addressed and carry no identity of their own, so without it a
    second run could not tell a stale download from a fresh one.
    """

    def entry(self, *, uri: str) -> StagingEntry | None:
        """Return the recorded entry for *uri*, or ``None`` if it is not staged."""
        rows = self._query(
            "SELECT uri, digest, etag, version_id, size, staged_at, blob_path "
            "FROM staging_entries WHERE uri = ?",
            (uri,),
        )
        return _entry_from_row(rows[0]) if rows else None

    def entries(self) -> list[StagingEntry]:
        """Return every recorded entry, oldest first."""
        rows = self._query(
            "SELECT uri, digest, etag, version_id, size, staged_at, blob_path "
            "FROM staging_entries ORDER BY staged_at"
        )
        return [_entry_from_row(row) for row in rows]

    def record(self, entry: StagingEntry) -> None:
        """Record a freshly staged URI, replacing whatever was there before."""
        self._write(
            ProjectionOp(
                sql="INSERT INTO staging_entries "
                "(uri, digest, etag, version_id, size, staged_at, blob_path, last_used_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (uri) DO UPDATE SET "
                "digest=excluded.digest, etag=excluded.etag, version_id=excluded.version_id, "
                "size=excluded.size, staged_at=excluded.staged_at, "
                "blob_path=excluded.blob_path, last_used_at=excluded.last_used_at",
                params=(
                    entry.uri,
                    entry.digest,
                    entry.etag,
                    entry.version_id,
                    entry.size,
                    entry.staged_at,
                    entry.blob_path,
                    entry.staged_at,
                ),
            )
        )

    def unused_since(self, *, before: str) -> list[StagingEntry]:
        """Return the entries not used since *before*, an ISO-8601 timestamp."""
        rows = self._query(
            "SELECT uri, digest, etag, version_id, size, staged_at, blob_path "
            "FROM staging_entries WHERE last_used_at < ? ORDER BY last_used_at",
            (before,),
        )
        return [_entry_from_row(row) for row in rows]

    def forget(self, *, uris: Sequence[str]) -> None:
        """Drop the rows for *uris*. The bytes are the caller's to remove."""
        self._write(
            *(
                ProjectionOp(sql="DELETE FROM staging_entries WHERE uri = ?", params=(uri,))
                for uri in uris
            )
        )

    def record_use(self, *, uri: str) -> None:
        """Note that a staged URI was reused rather than re-downloaded."""
        self._write(
            ProjectionOp(
                sql="UPDATE staging_entries SET last_used_at = ? WHERE uri = ?",
                params=(now_iso(), uri),
            )
        )


class StagingCache:
    """Content-addressed local cache for remote file downloads.

    The database is opened on first use, never on construction: a workspace
    that stages nothing gets no database out of building one of these.

    Parameters
    ----------
    root : Path | None
        Root directory for the staged bytes. Defaults to ``.ginkgo/staging``
        under the current working directory, or to whatever
        ``GINKGO_STAGING_ROOT`` or ``[remote] staging_root`` names.
    db_path : Path | None
        The ledger holding ``staging_entries``. Defaults to the database beside
        an explicitly given *root*, and otherwise to this workspace's — the
        staged bytes may be configured to live elsewhere, but the rows that
        index them belong to the workspace. Pass
        :data:`~ginkgo.store.sqlite.MEMORY` where there is no workspace to
        write to: a remote worker has the inputs it staged but not the
        database that indexes them, and a row written in a pod's scratch
        directory is one nothing will ever read.
    """

    def __init__(self, *, root: Path | None = None, db_path: Path | None = None) -> None:
        self._root = root if root is not None else _default_staging_root()
        self._blobs_dir = self._root / "blobs"
        self._folders_dir = self._root / "folders"
        if db_path is not None:
            self._db_path = db_path
        elif root is not None:
            self._db_path = WorkspaceLayout.sibling_of(root).db
        else:
            self._db_path = WorkspaceLayout.for_cwd().db
        self._index: StagingIndex | None = None

    @property
    def index(self) -> StagingIndex:
        """The staging table, opened for writing on first use.

        Neither the database nor the directories the bytes go in are made
        until something is actually staged.
        """
        if self._index is None:
            for directory in (self._blobs_dir, self._folders_dir):
                directory.mkdir(parents=True, exist_ok=True)
            self._index = (
                StagingIndex.in_memory()
                if self._db_path == MEMORY
                else StagingIndex.open(path=self._db_path)
            )
        return self._index

    def close(self) -> None:
        """Release the database connection, if one was ever opened."""
        if self._index is not None:
            self._index.close()
            self._index = None

    def stage_file(
        self,
        *,
        ref: RemoteFileRef,
        backend: ObjectStore | None = None,
    ) -> Path:
        """Stage a remote file and return the local path.

        If the file is already cached and the remote ETag has not changed,
        returns the cached path without re-downloading.

        Parameters
        ----------
        ref : RemoteFileRef
            Remote file reference.
        backend : ObjectStore | None
            Storage backend to use.  Resolved from the ref's scheme if
            ``None``.

        Returns
        -------
        Path
            Local path to the staged file.
        """
        backend = backend or resolve_backend(ref.scheme)

        existing = self.index.entry(uri=ref.uri)
        if existing is not None:
            blob_path = self._blobs_dir / existing.digest
            if blob_path.exists() and not self._needs_refresh(
                existing=existing, ref=ref, backend=backend
            ):
                self.index.record_use(uri=ref.uri)
                return blob_path

        return self._download_and_cache(ref=ref, backend=backend)

    def stage_folder(
        self,
        *,
        ref: RemoteFolderRef,
        backend: ObjectStore | None = None,
    ) -> Path:
        """Stage a remote folder (prefix) and return the local directory path.

        Downloads all objects under the prefix into a local directory that
        mirrors the remote key structure.

        Parameters
        ----------
        ref : RemoteFolderRef
            Remote folder reference.
        backend : ObjectStore | None
            Storage backend to use.

        Returns
        -------
        Path
            Local directory path containing the staged files.
        """
        backend = backend or resolve_backend(ref.scheme)

        objects = backend.list_prefix(bucket=ref.bucket, prefix=ref.key)
        folder_digest = _folder_manifest_digest(uri=ref.uri, objects=objects)
        folder_dir = self._folders_dir / folder_digest

        if folder_dir.exists():
            self.index.record_use(uri=ref.uri)
            return folder_dir

        temp_dir = self._folders_dir / f".tmp-{folder_digest}"
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            for obj in objects:
                # Derive relative path within the folder.
                relative = obj.uri.split(ref.key, 1)[-1]
                if not relative:
                    continue

                dest = temp_dir / relative
                dest.parent.mkdir(parents=True, exist_ok=True)

                # Extract the object key from the URI.
                obj_key = obj.uri.split(f"{ref.bucket}/", 1)[-1]
                backend.download(bucket=ref.bucket, key=obj_key, dest_path=dest)

            temp_dir.rename(folder_dir)
        except Exception:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
            raise

        self.index.record(
            StagingEntry(
                uri=ref.uri,
                digest=folder_digest,
                etag=None,
                version_id=ref.version_id,
                size=sum(obj.size or 0 for obj in objects),
                staged_at=now_iso(),
                blob_path=f"folders/{folder_digest}",
            )
        )
        return folder_dir

    def prune(self, *, before: str, dry_run: bool = False) -> tuple[int, int]:
        """Delete staged downloads not used since *before*.

        The staged bytes are the largest thing under ``.ginkgo/`` and the only
        store with no eviction of its own; ``last_used_at`` is what makes one
        possible. Losing an entry costs a re-download, never a wrong answer.

        A blob shared by two URIs — the same bytes fetched from two places — is
        removed only when the last row naming it goes, so pruning one URI never
        strands the other.

        Parameters
        ----------
        before : str
            ISO-8601 cutoff. Entries last used before it go.
        dry_run : bool, optional
            Measure without deleting.

        Returns
        -------
        tuple[int, int]
            The number of entries and the number of bytes removed, or that
            would be.
        """
        if not self._db_path.is_file():
            return 0, 0
        with StagingIndex.for_reading(self._db_path) as index:
            stale = index.unused_since(before=before)
            still_used = {entry.blob_path for entry in index.entries() if entry not in stale}
        if not stale:
            return 0, 0

        freed = 0
        for entry in stale:
            if entry.blob_path in still_used:
                continue
            target = self._root / entry.blob_path
            freed += _tree_size(target)
            if not dry_run:
                _remove(target)
        if not dry_run:
            self.index.forget(uris=[entry.uri for entry in stale])
        return len(stale), freed

    def integrity_problems(self) -> list[str]:
        """Return the staged URIs whose bytes are no longer on disk.

        A row without its bytes is a URI that will be re-downloaded — harmless,
        but it means the cache is smaller than the index says it is.
        """
        if self._db_path == MEMORY:
            return []
        with StagingIndex.for_reading(self._db_path) as index:
            entries = index.entries()
        return [
            f"staged {entry.uri} has a row but no bytes at {entry.blob_path}"
            for entry in entries
            if not (self._root / entry.blob_path).exists()
        ]

    def lookup(self, *, uri: str) -> StagingEntry | None:
        """Look up the staging entry for a URI without downloading.

        A read path: it opens the database read-only, and answers ``None`` for
        a workspace that has never staged anything rather than creating one.

        Parameters
        ----------
        uri : str
            Remote URI.

        Returns
        -------
        StagingEntry | None
            Recorded entry, or ``None`` if not staged.
        """
        if self._db_path == MEMORY:
            return self.index.entry(uri=uri)
        with StagingIndex.for_reading(self._db_path) as index:
            return index.entry(uri=uri)

    def _needs_refresh(
        self,
        *,
        existing: StagingEntry,
        ref: RemoteRef,
        backend: ObjectStore,
    ) -> bool:
        """Check whether a cached entry needs re-downloading."""
        # Pinned version — always fresh.
        if ref.version_id is not None and existing.version_id == ref.version_id:
            return False

        # Check remote ETag.
        try:
            remote_meta = backend.head(bucket=ref.bucket, key=ref.key)
        except Exception:
            # If HEAD fails, assume we need a refresh.
            return True

        if remote_meta.etag and existing.etag and remote_meta.etag == existing.etag:
            return False

        return True

    def _download_and_cache(
        self,
        *,
        ref: RemoteFileRef,
        backend: ObjectStore,
    ) -> Path:
        """Download a remote file and store it in the staging cache."""
        with tempfile.NamedTemporaryFile(
            prefix="ginkgo-stage-", dir=self._blobs_dir, delete=False
        ) as handle:
            temp_path = Path(handle.name)
        try:
            meta = backend.download(
                bucket=ref.bucket,
                key=ref.key,
                dest_path=temp_path,
            )

            digest = hash_file(temp_path)

            # Move to content-addressed location.
            blob_path = self._blobs_dir / digest
            if not blob_path.exists():
                temp_path.rename(blob_path)
            else:
                temp_path.unlink()

            self.index.record(
                StagingEntry(
                    uri=ref.uri,
                    digest=digest,
                    etag=meta.etag,
                    version_id=meta.version_id or ref.version_id,
                    size=meta.size,
                    staged_at=now_iso(),
                    blob_path=f"blobs/{digest}",
                )
            )
            return blob_path

        except Exception:
            # Clean up temp file on failure.
            if temp_path.exists():
                temp_path.unlink()
            raise


def _entry_from_row(row: Any) -> StagingEntry:
    """Build a :class:`StagingEntry` from one ``staging_entries`` row."""
    return StagingEntry(
        uri=str(row["uri"]),
        digest=str(row["digest"]),
        etag=row["etag"],
        version_id=row["version_id"],
        size=int(row["size"] or 0),
        staged_at=str(row["staged_at"]),
        blob_path=str(row["blob_path"]),
    )


def _tree_size(path: Path) -> int:
    """Return the bytes *path* occupies, counting a directory's contents."""
    if path.is_file():
        return path.stat().st_size
    if not path.is_dir():
        return 0
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def _remove(path: Path) -> None:
    """Delete a staged file or folder, tolerating one already gone."""
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _folder_manifest_digest(
    *,
    uri: str,
    objects: list[RemoteObjectMeta],
) -> str:
    manifest = [
        {
            "uri": obj.uri,
            "size": obj.size,
            "etag": obj.etag,
            "version_id": obj.version_id,
        }
        for obj in sorted(objects, key=lambda item: item.uri)
    ]
    return hash_str(json.dumps({"uri": uri, "objects": manifest}, sort_keys=True))


def _default_staging_root() -> Path:
    env_root = os.environ.get("GINKGO_STAGING_ROOT")
    if env_root:
        return Path(env_root).expanduser()

    config = load_runtime_config(project_root=Path.cwd())
    remote_config = config.get("remote", {})
    if isinstance(remote_config, dict):
        staging_root = remote_config.get("staging_root")
        if staging_root:
            return Path(str(staging_root)).expanduser()

    return WorkspaceLayout.for_cwd().staging
