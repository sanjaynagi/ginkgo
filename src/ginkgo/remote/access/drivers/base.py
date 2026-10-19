"""Base driver protocol and dispatch for FUSE mount backends."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ginkgo.errors import GinkgoError
from ginkgo.plugins.api import MountDriverType
from ginkgo.plugins.registry import PluginNotFoundError, plugin_registry


class MountFailedError(GinkgoError, RuntimeError):
    """Raised when mounting a prefix fails after retries."""


class DriverUnavailableError(GinkgoError, RuntimeError):
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


def resolve_driver(*, scheme: str) -> MountDriver:
    """Return a driver instance for ``scheme`` from the plugin registry.

    Raises
    ------
    DriverUnavailableError
        If no installed plugin provides a driver for the scheme. Callers treat
        this like a driver that failed its health check and fall back to
        staging.
    """
    try:
        driver_type = plugin_registry().get(MountDriverType, scheme)
    except PluginNotFoundError as exc:
        raise DriverUnavailableError(str(exc)) from exc
    return driver_type.build()


def unmount_fuse(*, mount_point: Path) -> None:
    """Unmount a FUSE mount, for any driver: gcsfuse, mountpoint-s3 or rclone.

    Tries ``fusermount -u`` first (Linux, preferred), then falls back to
    ``umount``. Any failure is re-raised as :class:`MountFailedError`.
    """
    last_err = "no unmount command available on PATH"
    for cmd in (
        ["fusermount", "-u", str(mount_point)],
        ["umount", str(mount_point)],
    ):
        if shutil.which(cmd[0]) is None:
            continue
        result = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode == 0:
            return
        last_err = result.stderr.decode(errors="replace").strip() or f"rc={result.returncode}"
    raise MountFailedError(f"Failed to unmount {mount_point}: {last_err}")
