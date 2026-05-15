"""Base driver protocol and dispatch for FUSE mount backends."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol


class MountFailedError(RuntimeError):
    """Raised when mounting a prefix fails after retries."""


class DriverUnavailableError(RuntimeError):
    """Raised when the driver binary is missing or unusable."""


@dataclass(kw_only=True, frozen=True)
class MountSpec:
    """Parameters needed to establish a mount.

    Parameters
    ----------
    scheme : str
        Remote scheme (``gs``/``s3``/``oci``).
    bucket : str
        Bucket / container authority.
    mount_point : Path
        Local directory where the mount will appear.
    cache_dir : Path | None
        Driver read-through cache directory.
    cache_max_bytes : int | None
        Upper bound on the read-through cache size.
    read_only : bool
        Whether the mount should be read-only.
    extra_args : tuple[str, ...]
        Additional driver-specific command-line arguments.
    """

    scheme: str
    bucket: str
    mount_point: Path
    cache_dir: Path | None = None
    cache_max_bytes: int | None = None
    read_only: bool = True
    extra_args: tuple[str, ...] = ()


class MountDriver(Protocol):
    """FUSE driver interface.

    Implementations wrap a driver binary (``gcsfuse``,
    ``mount-s3``/``mountpoint-s3``, ``rclone``) via :mod:`subprocess`.
    """

    name: str

    def health_check(self) -> None:
        """Raise :class:`DriverUnavailableError` when unusable."""
        ...

    def mount(self, *, spec: MountSpec) -> int:
        """Mount a bucket at ``spec.mount_point``. Return the driver PID."""
        ...

    def unmount(self, *, mount_point: Path) -> None:
        """Unmount a path mounted by this driver."""
        ...


_DRIVER_FACTORIES: dict[str, Callable[[], MountDriver]] = {}


def register_driver(scheme: str, factory: Callable[[], MountDriver]) -> None:
    """Register a driver factory for ``scheme``."""
    _DRIVER_FACTORIES[scheme] = factory


def resolve_driver(*, scheme: str) -> MountDriver:
    """Return a driver instance for ``scheme`` or raise.

    Factories are registered lazily by their module so they are not
    imported when streaming is not in use.
    """
    if scheme not in _DRIVER_FACTORIES:
        _bootstrap_defaults()
    factory = _DRIVER_FACTORIES.get(scheme)
    if factory is None:
        raise DriverUnavailableError(f"No FUSE driver registered for scheme {scheme!r}")
    return factory()


def _bootstrap_defaults() -> None:
    """Register the bundled drivers on first use."""
    from ginkgo.remote.access.drivers.gcsfuse import GcsFuseDriver
    from ginkgo.remote.access.drivers.rclone import RcloneDriver
    from ginkgo.remote.access.drivers.s3 import MountpointS3Driver

    register_driver("gs", GcsFuseDriver)
    register_driver("s3", MountpointS3Driver)
    register_driver("oci", RcloneDriver)
