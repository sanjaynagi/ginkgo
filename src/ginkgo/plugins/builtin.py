"""Core's own plugin: the implementations ginkgo ships with.

Registered through the same registry as any installed plugin, so core has one
dispatch path rather than built-ins plus extensions. The registry loads it
directly, ahead of the entry-point group, so it never depends on package
metadata being installed.
"""

from __future__ import annotations

from ginkgo.plugins.api import (
    EventSubscriberType,
    ExecutorType,
    MountDriverType,
    ObjectStoreType,
    Plugin,
    SecretBackendType,
)
from ginkgo.remote.access.drivers.gcsfuse import GcsFuseDriver
from ginkgo.remote.access.drivers.rclone import RcloneDriver
from ginkgo.remote.access.drivers.s3 import MountpointS3Driver
from ginkgo.remote.gcp_batch import build_gcp_batch_executor
from ginkgo.remote.kubernetes import build_kubernetes_executor
from ginkgo.remote.resolve import build_gcs_store, build_oci_store, build_s3_store
from ginkgo.runtime.environment.secrets import build_env_backends
from ginkgo.runtime.notifications.notifications import build_notification_service

__all__ = ["BUILTIN"]

BUILTIN = Plugin(
    name="ginkgo",
    contributions=(
        ExecutorType(
            name="k8s",
            label="Kubernetes",
            required_settings=("image",),
            build=build_kubernetes_executor,
        ),
        ExecutorType(
            name="batch",
            label="GCP Batch",
            required_settings=("project", "image"),
            build=build_gcp_batch_executor,
        ),
        ObjectStoreType(scheme="s3", build=build_s3_store),
        ObjectStoreType(scheme="oci", build=build_oci_store),
        ObjectStoreType(scheme="gs", build=build_gcs_store),
        MountDriverType(scheme="gs", build=GcsFuseDriver),
        MountDriverType(scheme="s3", build=MountpointS3Driver),
        MountDriverType(scheme="oci", build=RcloneDriver),
        SecretBackendType(name="env", build=build_env_backends),
        EventSubscriberType(name="slack", build=build_notification_service),
    ),
)
"""Everything core contributes, in the order the registry indexes it."""
