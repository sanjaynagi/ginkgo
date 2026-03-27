"""Local staging cache for remote artifacts.

Downloads remote files into a content-addressed local cache so that tasks
receive normal filesystem paths.  ETag-based freshness checks avoid
redundant downloads on subsequent runs.

Layout::

    .ginkgo/staging/
      blobs/<digest>                # cached file bytes
      metadata/<uri_hash>.json      # freshness and identity metadata
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil

from ginkgo.config import load_runtime_config
from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef
from ginkgo.remote.backend import RemoteObjectMeta, RemoteStorageBackend
from ginkgo.remote.resolve import resolve_backend
from ginkgo.runtime.hashing import hash_file, hash_str


@dataclass(frozen=True, kw_only=True)
class StagingEntry:
    """Metadata for a staged remote object.

    Parameters
    ----------
    uri : str
        Original remote URI.
    digest : str
        BLAKE3 content digest of the staged file.
    etag : str | None
        Provider ETag at time of download.
    version_id : str | None
        Provider version ID at time of download.
    size : int
        File size in bytes.
    staged_at : str
        ISO-8601 timestamp of when the file was staged.
    blob_path : str
        Relative path to the blob within the staging cache.
    """

    uri: str
    digest: str
    etag: str | None
    version_id: str | None
    size: int
    staged_at: str
    blob_path: str

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(asdict(self), indent=2, sort_keys=True)

    @classmethod
    def from_json(cls, data: str) -> StagingEntry:
        """Deserialize from JSON."""
        return cls(**json.loads(data))


class StagingCache:
    """Content-addressed local cache for remote file downloads.

    Parameters
    ----------
    root : Path | None
        Root directory for the staging cache.  Defaults to
        ``.ginkgo/staging`` under the current working directory.
    """

    def __init__(self, *, root: Path | None = None) -> None:
        self._root = root if root is not None else _default_staging_root()
        self._blobs_dir = self._root / "blobs"
        self._metadata_dir = self._root / "metadata"
        self._folders_dir = self._root / "folders"
        for directory in (self._blobs_dir, self._metadata_dir, self._folders_dir):
            directory.mkdir(parents=True, exist_ok=True)

    def stage_file(
        self,
        *,
        ref: RemoteFileRef,
        backend: RemoteStorageBackend | None = None,
    ) -> Path:
        """Stage a remote file and return the local path.

        If the file is already cached and the remote ETag has not changed,
        returns the cached path without re-downloading.

        Parameters
        ----------
        ref : RemoteFileRef
            Remote file reference.
        backend : RemoteStorageBackend | None
            Storage backend to use.  Resolved from the ref's scheme if
            ``None``.

        Returns
        -------
        Path
            Local path to the staged file.
        """
        backend = backend or resolve_backend(ref.scheme)
        uri_key = _uri_hash(ref.uri)
        metadata_path = self._metadata_dir / f"{uri_key}.json"

        # Check for a cached entry.
        existing = self._load_entry(metadata_path)
        if existing is not None:
            blob_path = self._blobs_dir / existing.digest
            if blob_path.exists():
                # Check freshness via ETag if available.
                if not self._needs_refresh(existing=existing, ref=ref, backend=backend):
                    return blob_path

        # Download the file to a temp location, then move into the cache.
        return self._download_and_cache(
            ref=ref,
            backend=backend,
            uri_key=uri_key,
            metadata_path=metadata_path,
        )

    def stage_folder(
        self,
        *,
        ref: RemoteFolderRef,
        backend: RemoteStorageBackend | None = None,
    ) -> Path:
        """Stage a remote folder (prefix) and return the local directory path.

        Downloads all objects under the prefix into a local directory that
        mirrors the remote key structure.

        Parameters
        ----------
        ref : RemoteFolderRef
            Remote folder reference.
        backend : RemoteStorageBackend | None
            Storage backend to use.

        Returns
        -------
        Path
            Local directory path containing the staged files.
        """
        backend = backend or resolve_backend(ref.scheme)
        uri_key = _uri_hash(ref.uri)

        objects = backend.list_prefix(bucket=ref.bucket, prefix=ref.key)
        folder_digest = _folder_manifest_digest(uri=ref.uri, objects=objects)
        folder_dir = self._folders_dir / folder_digest

        if folder_dir.exists():
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

        metadata_path = self._metadata_dir / f"{uri_key}.json"
        metadata_path.write_text(
            json.dumps(
                {
                    "uri": ref.uri,
                    "folder_digest": folder_digest,
                    "object_count": len(objects),
                    "staged_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        return folder_dir

    def lookup(self, *, uri: str) -> StagingEntry | None:
        """Look up the staging entry for a URI without downloading.

        Parameters
        ----------
        uri : str
            Remote URI.

        Returns
        -------
        StagingEntry | None
            Cached entry, or ``None`` if not staged.
        """
        uri_key = _uri_hash(uri)
        metadata_path = self._metadata_dir / f"{uri_key}.json"
        return self._load_entry(metadata_path)

    def _needs_refresh(
        self,
        *,
        existing: StagingEntry,
        ref: RemoteRef,
        backend: RemoteStorageBackend,
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
        backend: RemoteStorageBackend,
        uri_key: str,
        metadata_path: Path,
    ) -> Path:
        """Download a remote file and store it in the staging cache."""
        import tempfile

        # Download to a temp file first.
        temp_path = Path(tempfile.mktemp(prefix="ginkgo-stage-", dir=str(self._blobs_dir)))
        try:
            meta = backend.download(
                bucket=ref.bucket,
                key=ref.key,
                dest_path=temp_path,
            )

            # Compute content digest.
            digest = hash_file(temp_path)

            # Move to content-addressed location.
            blob_path = self._blobs_dir / digest
            if not blob_path.exists():
                temp_path.rename(blob_path)
            else:
                temp_path.unlink()

            # Write metadata.
            entry = StagingEntry(
                uri=ref.uri,
                digest=digest,
                etag=meta.etag,
                version_id=meta.version_id or ref.version_id,
                size=meta.size,
                staged_at=datetime.now(timezone.utc).isoformat(),
                blob_path=f"blobs/{digest}",
            )
            metadata_path.write_text(entry.to_json(), encoding="utf-8")

            return blob_path

        except Exception:
            # Clean up temp file on failure.
            if temp_path.exists():
                temp_path.unlink()
            raise

    def _load_entry(self, metadata_path: Path) -> StagingEntry | None:
        """Load a staging entry from disk."""
        if not metadata_path.exists():
            return None
        try:
            return StagingEntry.from_json(metadata_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, TypeError, KeyError):
            return None


def _uri_hash(uri: str) -> str:
    """Return a stable hash of a URI for use as a cache key."""
    return hash_str(uri)


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

    return Path.cwd() / ".ginkgo" / "staging"
