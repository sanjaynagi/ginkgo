"""FUSE-style mount drivers for streaming remote inputs.

Each driver exposes a uniform :class:`MountDriver` protocol: ``mount``,
``unmount``, and ``health_check``. Driver selection is dispatched off
the remote URI scheme, mirroring
:func:`ginkgo.remote.resolve.resolve_backend`.

Current drivers:

- ``gs`` → :mod:`ginkgo.remote.access.drivers.gcsfuse`
- ``s3`` → :mod:`ginkgo.remote.access.drivers.s3` (``mountpoint-s3``)
- ``oci`` → :mod:`ginkgo.remote.access.drivers.rclone`
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
