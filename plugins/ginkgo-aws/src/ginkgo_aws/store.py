"""The ``s3://`` object store, on ``s3fs``."""

from __future__ import annotations

import os
from typing import Any

from ginkgo.plugins.api import ObjectStoreContext
from ginkgo.remote.backend import ObjectStore
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import config_string, config_table, first_defined


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

        # Imported on first use: the plugin module loads on every CLI run.
        import s3fs

        client_kwargs: dict[str, Any] = {}
        if self._region is not None:
            client_kwargs["region_name"] = self._region

        # botocore pools 10 connections and makes the rest wait, which would
        # cap concurrent staging below ``staging_jobs``. Connections open
        # lazily, so a larger pool costs nothing until it is used.
        kwargs: dict[str, Any] = {"config_kwargs": {"max_pool_connections": 64}}
        if client_kwargs:
            kwargs["client_kwargs"] = client_kwargs

        self._filesystem = s3fs.S3FileSystem(**kwargs)
        return self._filesystem


def build_s3_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the S3 store from ``AWS_REGION`` or ``[remote] region``."""
    remote = config_table(context.config, "remote")
    return S3FileSystemBackend(
        region=context.region
        or first_defined(
            os.environ.get("AWS_REGION"),
            os.environ.get("AWS_DEFAULT_REGION"),
            config_string(remote, "region"),
        )
    )
