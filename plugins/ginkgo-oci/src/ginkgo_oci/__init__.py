"""ginkgo-oci: the ``oci://`` object store and the rclone FUSE driver."""

from __future__ import annotations

from ginkgo.plugins.api import MountDriverType, ObjectStoreType, Plugin
from ginkgo_oci.rclone import RcloneDriver
from ginkgo_oci.store import build_oci_store, filesystem, parse_oci_uri

__all__ = ["PLUGIN", "filesystem"]

PLUGIN = Plugin(
    name="ginkgo-oci",
    contributions=(
        ObjectStoreType(scheme="oci", parse_uri=parse_oci_uri, build=build_oci_store),
        MountDriverType(scheme="oci", build=RcloneDriver),
    ),
)
