"""``rclone mount`` driver wrapper.

Used as the fallback for schemes without a first-class driver (currently
``oci``). rclone's per-remote configuration is the user's responsibility;
we assume a remote named ``<scheme>`` is configured.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from ginkgo.remote.access.drivers.base import (
    DriverUnavailableError,
    MountFailedError,
    MountSpec,
)


class RcloneDriver:
    """Thin subprocess wrapper around ``rclone mount``."""

    name = "rclone"

    def __init__(self, *, binary: str = "rclone", remote: str | None = None) -> None:
        self._binary = binary
        self._remote = remote

    def health_check(self) -> None:
        """Verify ``rclone`` is on PATH."""
        if shutil.which(self._binary) is None:
            raise DriverUnavailableError(
                f"{self._binary!r} binary not found on PATH. "
                "Install rclone or use the worker-fuse image."
            )

    def mount(self, *, spec: MountSpec) -> int:
        """Mount ``<remote>:<bucket>`` at ``spec.mount_point``."""
        spec.mount_point.mkdir(parents=True, exist_ok=True)
        remote_name = self._remote or spec.scheme

        # OCI refs carry the namespace in the bucket component
        # ("bucket@namespace"), but rclone's oracleobjectstorage backend
        # takes the namespace from the remote's config and rejects "@" in
        # bucket names. Mount only the bucket component; the configured
        # remote must point at the matching namespace.
        bucket = spec.bucket.split("@", 1)[0]

        cmd = [
            self._binary,
            "mount",
            f"{remote_name}:{bucket}",
            str(spec.mount_point),
            "--daemon",
        ]
        if spec.read_only:
            cmd.append("--read-only")
        if spec.cache_dir is not None:
            spec.cache_dir.mkdir(parents=True, exist_ok=True)
            cmd += [
                "--cache-dir",
                str(spec.cache_dir),
                "--vfs-cache-mode",
                "full",
            ]
            if spec.cache_max_bytes is not None:
                cmd += [
                    "--vfs-cache-max-size",
                    f"{max(1, spec.cache_max_bytes // (1024 * 1024))}M",
                ]
        cmd += list(spec.extra_args)

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
                "rclone mount failed: "
                f"rc={result.returncode} stderr={result.stderr.decode(errors='replace')}"
            )

        # The daemon exits 0 even when the remote is misconfigured (bad
        # bucket, bad credentials) and reads then fail with EIO. Probe the
        # mount with one directory listing so a broken mount surfaces as
        # MountFailedError, letting hydration fall back to staging.
        try:
            os.listdir(spec.mount_point)
        except OSError as exc:
            self.unmount(mount_point=spec.mount_point)
            raise MountFailedError(
                f"rclone mount at {spec.mount_point} is not readable: {exc}"
            ) from exc
        return 0

    def unmount(self, *, mount_point: Path) -> None:
        """Unmount via the generic helper."""
        from ginkgo.remote.access.drivers.gcsfuse import _generic_unmount

        _generic_unmount(mount_point=mount_point)
