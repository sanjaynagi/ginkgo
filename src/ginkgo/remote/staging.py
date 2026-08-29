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

from ginkgo.config import load_runtime_config
from ginkgo.core.hashing import hash_file, hash_str
from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef
from ginkgo.formatting import now_iso
from ginkgo.remote.backend import ObjectStore, RemoteObjectMeta
from ginkgo.remote.resolve import resolve_backend
from ginkgo.store.direct_index import DirectIndex
from ginkgo.store.protocol import ProjectionOp
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
        if not rows:
            return None
        row = rows[0]
        return StagingEntry(
            uri=str(row["uri"]),
            digest=str(row["digest"]),
            etag=row["etag"],
            version_id=row["version_id"],
            size=int(row["size"] or 0),
            staged_at=str(row["staged_at"]),
            blob_path=str(row["blob_path"]),
        )

    def entries(self) -> list[StagingEntry]:
        """Return every recorded entry, oldest first."""
        rows = self._query(
            "SELECT uri, digest, etag, version_id, size, staged_at, blob_path "
            "FROM staging_entries ORDER BY staged_at"
        )
        return [
            StagingEntry(
                uri=str(row["uri"]),
                digest=str(row["digest"]),
                etag=row["etag"],
                version_id=row["version_id"],
                size=int(row["size"] or 0),
                staged_at=str(row["staged_at"]),
                blob_path=str(row["blob_path"]),
            )
            for row in rows
        ]

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
        under the current working directory.
    db_path : Path | None
        The ledger holding ``staging_entries``. Defaults to the database
        belonging to the workspace *root* sits in.
    """

    def __init__(self, *, root: Path | None = None, db_path: Path | None = None) -> None:
        self._root = root if root is not None else _default_staging_root()
        self._blobs_dir = self._root / "blobs"
        self._folders_dir = self._root / "folders"
        for directory in (self._blobs_dir, self._folders_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._db_path = (
            db_path if db_path is not None else WorkspaceLayout.sibling_of(self._root).db
        )
        self._index: StagingIndex | None = None

    @property
    def index(self) -> StagingIndex:
        """The staging table, opened for writing on first use."""
        if self._index is None:
            self._index = StagingIndex.open(path=self._db_path)
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
