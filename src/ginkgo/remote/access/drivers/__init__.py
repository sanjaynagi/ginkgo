"""FUSE-style mount drivers for streaming remote inputs.

Each driver exposes a uniform :class:`MountDriver` protocol: ``mount``,
``unmount``, and ``health_check``. Drivers are contributed by plugins as
:class:`~ginkgo.plugins.api.MountDriverType`, one per URI scheme: gcsfuse
(ginkgo-gcp), mountpoint-s3 (ginkgo-aws) and rclone (ginkgo-oci).
"""

from __future__ import annotations

from ginkgo.remote.access.drivers.base import (
    DriverUnavailableError,
    MountDriver,
    MountFailedError,
    resolve_driver,
)

__all__ = [
    "DriverUnavailableError",
    "MountDriver",
    "MountFailedError",
    "resolve_driver",
]
