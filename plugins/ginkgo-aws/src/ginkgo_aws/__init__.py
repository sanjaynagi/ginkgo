"""ginkgo-aws: the ``s3://`` object store and the mountpoint-s3 FUSE driver."""

from __future__ import annotations

from ginkgo.core.remote import parse_bucket_key_uri
from ginkgo.plugins.api import MountDriverType, ObjectStoreType, Plugin
from ginkgo_aws.mountpoint import MountpointS3Driver
from ginkgo_aws.store import build_s3_store

__all__ = ["PLUGIN"]

PLUGIN = Plugin(
    name="ginkgo-aws",
    contributions=(
        ObjectStoreType(scheme="s3", parse_uri=parse_bucket_key_uri, build=build_s3_store),
        MountDriverType(scheme="s3", build=MountpointS3Driver),
    ),
)
