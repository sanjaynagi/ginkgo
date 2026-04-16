"""Remote storage backends built on filesystem adapters."""

from __future__ import annotations

import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

from ginkgo.remote.backend import RemoteObjectMeta

logger = logging.getLogger(__name__)

_AUTH_EXCEPTIONS: tuple[str, ...] = (
    "RefreshError",
    "DefaultCredentialsError",
    "TransportError",
)

_OPERATION_TIMEOUT_SECONDS = 600


def _is_auth_error(exc: Exception) -> bool:
    """Return True if the exception looks like a credential / auth failure."""
    if getattr(exc, "status", None) == 401:
        return True
    if type(exc).__name__ in _AUTH_EXCEPTIONS:
        return True
    for cause in (exc.__cause__, exc.__context__):
        if cause is not None and type(cause).__name__ in _AUTH_EXCEPTIONS:
            return True
    return False


def _run_with_timeout(fn: Any, *, timeout: float = _OPERATION_TIMEOUT_SECONDS) -> Any:
    """Run *fn* in a thread and raise ``TimeoutError`` if it takes too long."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        future: Future[Any] = pool.submit(fn)
        return future.result(timeout=timeout)


class FsspecBackend:
    """Base class for fsspec-based remote storage backends.

    Subclasses must implement ``_get_filesystem()`` and set ``self._scheme``
    to the URI scheme they handle (e.g. ``"s3"``, ``"oci"``, ``"gs"``).

    Auth errors (expired tokens, revoked credentials) trigger a single
    filesystem rebuild and retry, mirroring the ``_RefreshingApi`` pattern
    used for the Kubernetes API client.

    Parameters
    ----------
    scheme : str
        URI scheme for this backend (used in metadata construction).
    operation_timeout : float
        Per-operation timeout in seconds for upload / download / head calls.
    """

    def __init__(
        self,
        *,
        scheme: str,
        operation_timeout: float = _OPERATION_TIMEOUT_SECONDS,
    ) -> None:
        self._scheme = scheme
        self._filesystem: Any | None = None
        self._operation_timeout = operation_timeout

    def _get_filesystem(self) -> Any:
        """Return the fsspec filesystem instance (lazy-initialized)."""
        raise NotImplementedError

    def _rebuild_filesystem(self) -> Any:
        """Discard the cached filesystem and build a fresh one."""
        self._filesystem = None
        return self._get_filesystem()

    def _call_with_retry(self, fn: Any) -> Any:
        """Execute *fn*, retrying once with a fresh filesystem on auth error."""
        try:
            return _run_with_timeout(fn, timeout=self._operation_timeout)
        except TimeoutError:
            logger.warning("fsspec operation timed out — rebuilding filesystem and retrying")
            self._rebuild_filesystem()
            return _run_with_timeout(fn, timeout=self._operation_timeout)
        except Exception as exc:
            if not _is_auth_error(exc):
                raise
            logger.warning("fsspec auth error (%s) — rebuilding filesystem and retrying", exc)
            self._rebuild_filesystem()
            return _run_with_timeout(fn, timeout=self._operation_timeout)

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        """Return metadata for a remote object without downloading it."""
        path = _join_remote_path(bucket=bucket, key=key)

        def _do() -> RemoteObjectMeta:
            fs = self._get_filesystem()
            info = fs.info(path)
            return _remote_meta_from_info(scheme=self._scheme, bucket=bucket, key=key, info=info)

        return self._call_with_retry(_do)

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        """Download a remote object to a local path."""
        path = _join_remote_path(bucket=bucket, key=key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        def _do() -> None:
            fs = self._get_filesystem()
            with fs.open(path, "rb") as source, dest_path.open("wb") as destination:
                while True:
                    chunk = source.read(65536)
                    if not chunk:
                        break
                    destination.write(chunk)

        self._call_with_retry(_do)
        return self.head(bucket=bucket, key=key)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        """Upload a local file to a remote path."""
        path = _join_remote_path(bucket=bucket, key=key)

        def _do() -> None:
            fs = self._get_filesystem()
            with src_path.open("rb") as source, fs.open(path, "wb") as destination:
                while True:
                    chunk = source.read(65536)
                    if not chunk:
                        break
                    destination.write(chunk)

        self._call_with_retry(_do)
        return self.head(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        """List all objects under a remote prefix."""
        base = _join_remote_path(bucket=bucket, key=prefix)

        def _do() -> list[RemoteObjectMeta]:
            fs = self._get_filesystem()
            found = fs.find(base, detail=True)
            return _list_results_to_meta(scheme=self._scheme, results=found)

        return self._call_with_retry(_do)


class S3FileSystemBackend(FsspecBackend):
    """S3 backend implemented with ``s3fs``.

    Parameters
    ----------
    region : str | None
        AWS region override.
    """

    def __init__(self, *, region: str | None = None) -> None:
        super().__init__(scheme="s3")
        self._region = region

    def _get_filesystem(self) -> Any:
        if self._filesystem is not None:
            return self._filesystem

        try:
            import s3fs
        except ImportError as exc:
            raise ImportError(
                "s3fs is required for S3 remote storage support. Install it with: pip install s3fs"
            ) from exc

        client_kwargs: dict[str, Any] = {}
        if self._region is not None:
            client_kwargs["region_name"] = self._region

        kwargs: dict[str, Any] = {}
        if client_kwargs:
            kwargs["client_kwargs"] = client_kwargs

        self._filesystem = s3fs.S3FileSystem(**kwargs)
        return self._filesystem


class OCIFileSystemBackend(FsspecBackend):
    """OCI Object Storage backend implemented with ``ocifs``.

    Parameters
    ----------
    config_path : str | None
        Path to the OCI config file.
    profile : str | None
        OCI config profile name.
    region : str | None
        OCI region override.
    """

    def __init__(
        self,
        *,
        config_path: str | None = None,
        profile: str | None = None,
        region: str | None = None,
    ) -> None:
        super().__init__(scheme="oci")
        self._config_path = config_path
        self._profile = profile
        self._region = region

    def _get_filesystem(self) -> Any:
        if self._filesystem is not None:
            return self._filesystem

        try:
            import ocifs
        except ImportError as exc:
            raise ImportError(
                "ocifs is required for OCI remote storage support. "
                "Install it with: pip install ocifs"
            ) from exc

        kwargs: dict[str, Any] = {}
        if self._config_path is not None:
            kwargs["config"] = os.path.expanduser(self._config_path)
        if self._profile is not None:
            kwargs["profile"] = self._profile
        if self._region is not None:
            kwargs["region"] = self._region

        self._filesystem = ocifs.OCIFileSystem(**kwargs)
        return self._filesystem


class GCSFileSystemBackend(FsspecBackend):
    """Google Cloud Storage backend implemented with ``gcsfs``.

    Parameters
    ----------
    project : str | None
        GCP project ID override.
    """

    def __init__(self, *, project: str | None = None) -> None:
        super().__init__(scheme="gs")
        self._project = project

    def _get_filesystem(self) -> Any:
        if self._filesystem is not None:
            return self._filesystem

        try:
            import gcsfs
        except ImportError as exc:
            raise ImportError(
                "gcsfs is required for GCS remote storage support. "
                "Install it with: pip install gcsfs or pip install ginkgo[cloud]"
            ) from exc

        kwargs: dict[str, Any] = {}
        if self._project is not None:
            kwargs["project"] = self._project

        self._filesystem = gcsfs.GCSFileSystem(**kwargs)
        return self._filesystem


def _join_remote_path(*, bucket: str, key: str) -> str:
    key = key.lstrip("/")
    if not key:
        return bucket
    return f"{bucket}/{key}"


def _remote_meta_from_info(
    *,
    scheme: str,
    bucket: str,
    key: str,
    info: dict[str, Any],
) -> RemoteObjectMeta:
    return RemoteObjectMeta(
        uri=f"{scheme}://{bucket}/{key}",
        size=_extract_size(info),
        etag=_extract_metadata_value(info, "etag", "ETag"),
        version_id=_extract_metadata_value(info, "version_id", "VersionId", "versionId"),
    )


def _list_results_to_meta(
    *,
    scheme: str,
    results: dict[str, dict[str, Any]] | list[str],
) -> list[RemoteObjectMeta]:
    if isinstance(results, dict):
        items = results.items()
    else:
        items = ((path, {}) for path in results)

    remote_objects: list[RemoteObjectMeta] = []
    for path, info in items:
        normalized = path.lstrip("/")
        bucket, _, key = normalized.partition("/")
        if not key:
            continue
        remote_objects.append(
            RemoteObjectMeta(
                uri=f"{scheme}://{bucket}/{key}",
                size=_extract_size(info),
                etag=_extract_metadata_value(info, "etag", "ETag"),
                version_id=_extract_metadata_value(
                    info,
                    "version_id",
                    "VersionId",
                    "versionId",
                ),
            )
        )
    return remote_objects


def _extract_size(info: dict[str, Any]) -> int:
    for key in ("size", "Size", "ContentLength"):
        value = info.get(key)
        if value is not None:
            return int(value)
    return 0


def _extract_metadata_value(info: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = info.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip('"')
            return stripped or None
        return str(value)
    return None
