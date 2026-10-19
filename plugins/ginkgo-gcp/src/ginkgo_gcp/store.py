"""The ``gs://`` object store, on ``gcsfs``."""

from __future__ import annotations

import os
from typing import Any

from ginkgo.plugins.api import ObjectStoreContext
from ginkgo.remote.backend import ObjectStore
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import config_string, config_table, first_defined


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

        # Imported on first use: the plugin module loads on every CLI run.
        import gcsfs

        kwargs: dict[str, Any] = {}
        if self._project is not None:
            kwargs["project"] = self._project

        self._filesystem = gcsfs.GCSFileSystem(**kwargs)
        return self._filesystem


def build_gcs_store(context: ObjectStoreContext) -> ObjectStore:
    """Build the GCS store from ``GINKGO_REMOTE_GCS_PROJECT`` or ``[remote.gcs]``.

    GCS buckets are not regional, so the context's region is unused.
    """
    gcs = config_table(config_table(context.config, "remote"), "gcs")
    return GCSFileSystemBackend(
        project=first_defined(
            os.environ.get("GINKGO_REMOTE_GCS_PROJECT"),
            os.environ.get("GOOGLE_CLOUD_PROJECT"),
            config_string(gcs, "project"),
        )
    )
