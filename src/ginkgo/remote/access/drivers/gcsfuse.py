"""``gcsfuse`` driver wrapper.

Mounts a GCS bucket via the open-source ``gcsfuse`` binary. Read-only
by construction; per-file read-through cache is enabled when a cache
directory is provided.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ginkgo.remote.access.drivers.base import (
    DriverUnavailableError,
    MountFailedError,
    MountSpec,
)


class GcsFuseDriver:
    """Thin subprocess wrapper around the ``gcsfuse`` binary."""

    name = "gcsfuse"

    def __init__(self, *, binary: str = "gcsfuse") -> None:
        self._binary = binary

    def health_check(self) -> None:
        """Verify ``gcsfuse`` is on ``$PATH``."""
        if shutil.which(self._binary) is None:
            raise DriverUnavailableError(
                f"{self._binary!r} binary not found on PATH. "
                "Install gcsfuse or use the worker-fuse image."
            )

    def mount(self, *, spec: MountSpec) -> int:
        """Mount ``spec.bucket`` at ``spec.mount_point``.

        The flag set below is tuned for data-intensive pipelines typical
        of Ginkgo workloads (many concurrent range reads on large
        objects) rather than the gcsfuse defaults, which favour
        low-memory footprints. Callers can override any flag by passing
        ``extra_args`` — the last occurrence wins in gcsfuse's CLI.
        """
        spec.mount_point.mkdir(parents=True, exist_ok=True)
        cmd = [
            self._binary,
            "--implicit-dirs",
            "-o",
            "ro" if spec.read_only else "rw",
            # Metadata caches: reduce round-trips on stat() / open() / readdir().
            # Both default to 60s in gcsfuse 2.x — set explicitly so the
            # flag surface is visible when debugging.
            "--stat-cache-ttl",
            "60s",
            "--type-cache-ttl",
            "60s",
        ]
        if spec.cache_dir is not None:
            spec.cache_dir.mkdir(parents=True, exist_ok=True)
            cmd += [
                "--cache-dir",
                str(spec.cache_dir),
                # Populate the file cache on range reads, not just full reads.
                "--file-cache-cache-file-for-range-read",
                # Fetch the file into cache via multiple concurrent ranges
                # rather than a single stream. Big win on large objects
                # when bandwidth per stream is the bottleneck.
                "--file-cache-enable-parallel-downloads",
                "--file-cache-parallel-downloads-per-file",
                "16",
                "--file-cache-download-chunk-size-mb",
                "100",
            ]
            if spec.cache_max_bytes is not None:
                cmd += [
                    "--file-cache-max-size-mb",
                    str(max(1, spec.cache_max_bytes // (1024 * 1024))),
                ]
        cmd += list(spec.extra_args)
        cmd += [spec.bucket, str(spec.mount_point)]

        try:
            result = subprocess.run(
                cmd,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError as exc:
            raise DriverUnavailableError(str(exc)) from exc

        if result.returncode != 0:
            raise MountFailedError(
                "gcsfuse failed: "
                f"rc={result.returncode} stderr={result.stderr.decode(errors='replace')}"
            )
        return 0  # gcsfuse daemonises; PID tracking is via the mount point.

    def unmount(self, *, mount_point: Path) -> None:
        """Unmount via ``fusermount -u`` (Linux) or ``umount`` (fallback)."""
        _generic_unmount(mount_point=mount_point)


def _generic_unmount(*, mount_point: Path) -> None:
    """Best-effort unmount compatible with gcsfuse / mountpoint-s3 / rclone.

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
