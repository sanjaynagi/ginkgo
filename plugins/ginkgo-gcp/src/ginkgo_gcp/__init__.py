"""ginkgo-gcp: Google Cloud integrations.

The ``gs://`` object store, the gcsfuse FUSE driver and the ``batch`` executor
type.
"""

from __future__ import annotations

from ginkgo.core.remote import parse_bucket_key_uri
from ginkgo.plugins.api import ExecutorType, MountDriverType, ObjectStoreType, Plugin
from ginkgo_gcp.batch import build_gcp_batch_executor
from ginkgo_gcp.gcsfuse import GcsFuseDriver
from ginkgo_gcp.store import build_gcs_store

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-gcp",
    contributions=(
        ObjectStoreType(scheme="gs", parse_uri=parse_bucket_key_uri, build=build_gcs_store),
        MountDriverType(scheme="gs", build=GcsFuseDriver),
        ExecutorType(
            name="batch",
            label="GCP Batch",
            required_settings=("project", "image"),
            build=build_gcp_batch_executor,
        ),
    ),
)
