"""Remote storage backends built on filesystem adapters."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ginkgo.remote.backend import RemoteObjectMeta


class S3FileSystemBackend:
    """S3 backend implemented with ``s3fs``."""

    def __init__(self, *, region: str | None = None) -> None:
        self._region = region
        self._filesystem: Any | None = None

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

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        info = fs.info(path)
        return _remote_meta_from_info(
            scheme="s3",
            bucket=bucket,
            key=key,
            info=info,
        )

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with fs.open(path, "rb") as source, dest_path.open("wb") as destination:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                destination.write(chunk)
        return self.head(bucket=bucket, key=key)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        with src_path.open("rb") as source, fs.open(path, "wb") as destination:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                destination.write(chunk)
        return self.head(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        fs = self._get_filesystem()
        base = _join_remote_path(bucket=bucket, key=prefix)
        found = fs.find(base, detail=True)
        return _list_results_to_meta(
            scheme="s3",
            results=found,
        )


class OCIFileSystemBackend:
    """OCI Object Storage backend implemented with ``ocifs``."""

    def __init__(
        self,
        *,
        config_path: str | None = None,
        profile: str | None = None,
        region: str | None = None,
    ) -> None:
        self._config_path = config_path
        self._profile = profile
        self._region = region
        self._filesystem: Any | None = None

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

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        info = fs.info(path)
        return _remote_meta_from_info(
            scheme="oci",
            bucket=bucket,
            key=key,
            info=info,
        )

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with fs.open(path, "rb") as source, dest_path.open("wb") as destination:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                destination.write(chunk)
        return self.head(bucket=bucket, key=key)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        fs = self._get_filesystem()
        path = _join_remote_path(bucket=bucket, key=key)
        with src_path.open("rb") as source, fs.open(path, "wb") as destination:
            while True:
                chunk = source.read(65536)
                if not chunk:
                    break
                destination.write(chunk)
        return self.head(bucket=bucket, key=key)

    def list_prefix(self, *, bucket: str, prefix: str) -> list[RemoteObjectMeta]:
        fs = self._get_filesystem()
        base = _join_remote_path(bucket=bucket, key=prefix)
        found = fs.find(base, detail=True)
        return _list_results_to_meta(
            scheme="oci",
            results=found,
        )


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
