"""FUSE-mounted remote-input access.

Mounts each unique ``(scheme, bucket)`` pair referenced by a task once,
then yields per-input paths inside the mount. Mounts are torn down in
``release`` / ``close``.

This module is Linux-only at runtime — the drivers it dispatches to
(``gcsfuse``, ``mountpoint-s3``, ``rclone mount``) do not support macOS
in Ginkgo's supported deployment topology. Instantiation on any
platform is allowed so that the unit tests can patch
:meth:`MountedAccess._mount_bucket` on Mac; ``materialize_*`` will
raise :class:`DriverUnavailableError` when the underlying driver is
missing.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef
from ginkgo.remote.access.drivers import (
    DriverUnavailableError,
    MountDriver,
    MountFailedError,
    resolve_driver,
)
from ginkgo.remote.access.drivers.base import MountSpec
from ginkgo.remote.access.protocol import AccessStats, PerInputStats


@dataclass(kw_only=True)
class _ActiveMount:
    """Internal record of one established mount."""

    scheme: str
    bucket: str
    mount_point: Path
    driver: MountDriver


class MountedAccess:
    """Stream remote inputs through a FUSE driver.

    Parameters
    ----------
    mount_root : Path | None
        Root directory under which per-bucket mount points are created.
        Defaults to ``$GINKGO_FUSE_ROOT`` or ``/tmp/ginkgo-fuse``.
    cache_root : Path | None
        Root directory for the driver read-through cache. Defaults to
        ``$GINKGO_FUSE_CACHE`` or ``<mount_root>/cache``.
    cache_max_bytes : int | None
        Upper bound on the read-through cache size. ``None`` lets the
        driver pick its default.
    driver_factory : callable | None
        Override for driver resolution. Tests inject a stub factory.
    """

    def __init__(
        self,
        *,
        mount_root: Path | None = None,
        cache_root: Path | None = None,
        cache_max_bytes: int | None = None,
        driver_factory: object | None = None,
    ) -> None:
        self._mount_root = mount_root if mount_root is not None else _default_mount_root()
        self._cache_root = cache_root if cache_root is not None else (self._mount_root / "cache")
        self._cache_max_bytes = cache_max_bytes
        self._driver_factory = driver_factory
        self._stats = AccessStats(policy="fuse")
        self._mounts: dict[tuple[str, str], _ActiveMount] = {}

    # -------- strategy API ------------------------------------------------

    def materialize_file(self, *, ref: RemoteFileRef) -> Path:
        """Return a local path inside the per-bucket mount for ``ref``."""
        mount = self._ensure_mount(ref=ref)
        path = mount.mount_point / ref.key
        self._record_open(uri=ref.uri)
        return path

    def materialize_folder(self, *, ref: RemoteFolderRef) -> Path:
        """Return a local directory path inside the per-bucket mount."""
        mount = self._ensure_mount(ref=ref)
        path = mount.mount_point / ref.key.rstrip("/")
        self._record_open(uri=ref.uri)
        return path

    def release(self, *, paths: Iterable[Path]) -> None:  # noqa: ARG002
        """Paths are valid until :meth:`close` tears down the mount."""
        return

    def close(self) -> None:
        """Tear down every active mount. Safe to call multiple times."""
        if not self._mounts:
            return
        unmount_started = time.perf_counter()
        errors: list[str] = []
        for (scheme, bucket), mount in list(self._mounts.items()):
            try:
                mount.driver.unmount(mount_point=mount.mount_point)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{scheme}://{bucket}: {exc}")
            finally:
                self._mounts.pop((scheme, bucket), None)
        self._stats.unmount_seconds += time.perf_counter() - unmount_started
        if errors:
            # Recording without raising: teardown failures should not mask
            # task success.
            self._stats.fallback_reason = (
                (self._stats.fallback_reason or "") + " unmount_errors: " + "; ".join(errors)
            ).strip()

    def stats(self) -> AccessStats:
        """Return recorded mount statistics."""
        return self._stats

    # -------- internals ---------------------------------------------------

    def _ensure_mount(self, *, ref: RemoteRef) -> _ActiveMount:
        """Return (or create) the mount for the ref's bucket."""
        key = (ref.scheme, ref.bucket)
        if key in self._mounts:
            return self._mounts[key]

        driver = self._resolve_driver(scheme=ref.scheme)
        driver.health_check()

        mount_point = self._mount_point_for(ref=ref)
        cache_dir = self._cache_root / ref.scheme / _safe_component(ref.bucket)
        spec = MountSpec(
            scheme=ref.scheme,
            bucket=ref.bucket,
            mount_point=mount_point,
            cache_dir=cache_dir,
            cache_max_bytes=self._cache_max_bytes,
            read_only=True,
        )

        mount_started = time.perf_counter()
        try:
            driver.mount(spec=spec)
        except (DriverUnavailableError, MountFailedError):
            raise
        self._stats.mount_seconds += time.perf_counter() - mount_started

        record = _ActiveMount(
            scheme=ref.scheme,
            bucket=ref.bucket,
            mount_point=mount_point,
            driver=driver,
        )
        self._mounts[key] = record
        return record

    def _resolve_driver(self, *, scheme: str) -> MountDriver:
        """Resolve a driver instance, honouring the optional factory override."""
        if self._driver_factory is not None:
            return self._driver_factory(scheme)  # type: ignore[operator]
        return resolve_driver(scheme=scheme)

    def _mount_point_for(self, *, ref: RemoteRef) -> Path:
        """Return the mount-point directory for a ref's bucket."""
        return self._mount_root / ref.scheme / _safe_component(ref.bucket)

    def _record_open(self, *, uri: str) -> None:
        """Seed the per-input stats entry on first access."""
        self._stats.per_input.setdefault(uri, PerInputStats(uri=uri))


def _default_mount_root() -> Path:
    """Return the default FUSE mount root.

    Honours ``$GINKGO_FUSE_ROOT`` when set. Otherwise prefers a local
    NVMe / SSD mount (``/mnt/disks/ssd`` on GCE Local SSD, ``/mnt/nvme``
    on AWS, ``/mnt/local-ssd`` for generic cases) when one is present —
    that is where gcsfuse's read-through cache earns its keep. Falls
    back to ``/tmp/ginkgo-fuse`` (worker) or ``.ginkgo/fuse`` (local
    driver) when no fast-local mount is available.
    """
    env = os.environ.get("GINKGO_FUSE_ROOT")
    if env:
        return Path(env).expanduser()
    for candidate in ("/mnt/disks/ssd", "/mnt/nvme", "/mnt/local-ssd"):
        candidate_path = Path(candidate)
        if candidate_path.is_dir() and os.access(candidate_path, os.W_OK):
            return candidate_path / "ginkgo-fuse"
    if Path("/tmp").exists():
        return Path("/tmp/ginkgo-fuse")
    return Path.cwd() / ".ginkgo" / "fuse"


def _safe_component(value: str) -> str:
    """Collapse a bucket name into a filesystem-safe single path component."""
    safe = value.replace("/", "_").replace("@", "_at_")
    return safe or "_"
