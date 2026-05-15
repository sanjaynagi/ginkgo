"""Remote storage backend protocol and metadata types."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, kw_only=True)
class RemoteObjectMeta:
    """Metadata returned by remote storage operations.

    Parameters
    ----------
    uri : str
        Full URI of the remote object.
    size : int
        Object size in bytes.
    etag : str | None
        Provider-assigned ETag for freshness checks.
    digest : str | None
        Content digest if known (e.g. BLAKE3 after download).
    version_id : str | None
        Object version identifier for immutable pinning.
    """

    uri: str
    size: int
    etag: str | None = None
    digest: str | None = None
    version_id: str | None = None


@runtime_checkable
class RemoteStorageBackend(Protocol):
    """Protocol for remote object storage operations."""

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        """Return metadata for an object without downloading it.

        Parameters
        ----------
        bucket : str
            Bucket name (S3) or ``namespace/bucket`` (OCI).
        key : str
            Object key.

        Returns
        -------
        RemoteObjectMeta
        """
        ...

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        """Download an object to a local path.

        Parameters
        ----------
        bucket : str
            Bucket name.
        key : str
            Object key.
        dest_path : Path
            Local destination path.

        Returns
        -------
        RemoteObjectMeta
            Includes size and ETag from the download response.
        """
        ...

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        """Upload a local file to remote storage.

        Parameters
        ----------
        src_path : Path
            Local file to upload.
        bucket : str
            Destination bucket.
        key : str
            Destination key.

        Returns
        -------
        RemoteObjectMeta
        """
        ...

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        """List objects under a key prefix.

        Parameters
        ----------
        bucket : str
            Bucket name.
        prefix : str
            Key prefix to list.

        Returns
        -------
        list[RemoteObjectMeta]
        """
        ...
