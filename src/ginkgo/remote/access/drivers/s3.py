"""``mountpoint-s3`` (AWS) driver wrapper."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from ginkgo.remote.access.drivers.base import (
    DriverUnavailableError,
    MountFailedError,
    MountSpec,
)


# AWS packages the binary as ``mount-s3`` in most distributions but the
# project itself is called ``mountpoint-s3``; accept either.
_CANDIDATE_BINARIES = ("mount-s3", "mountpoint-s3")


class MountpointS3Driver:
    """Thin subprocess wrapper around AWS mountpoint-s3."""

    name = "mountpoint-s3"

    def __init__(self, *, binary: str | None = None) -> None:
        self._binary = binary or _find_binary()

    def health_check(self) -> None:
        """Verify the mountpoint-s3 binary is available."""
        if self._binary is None or shutil.which(self._binary) is None:
            raise DriverUnavailableError(
                "mountpoint-s3 binary not found on PATH. "
                "Install it from https://s3.amazonaws.com/mountpoint-s3-release/ "
                "or use the worker-fuse image."
            )

    def mount(self, *, spec: MountSpec) -> int:
        """Mount ``spec.bucket`` at ``spec.mount_point``."""
        if self._binary is None:
            raise DriverUnavailableError("mountpoint-s3 binary not found")

        spec.mount_point.mkdir(parents=True, exist_ok=True)
        cmd = [self._binary]
        if spec.read_only:
            cmd.append("--read-only")
        if spec.cache_dir is not None:
            spec.cache_dir.mkdir(parents=True, exist_ok=True)
            cmd += ["--cache", str(spec.cache_dir)]
            if spec.cache_max_bytes is not None:
                cmd += [
                    "--max-cache-size",
                    str(max(1, spec.cache_max_bytes // (1024 * 1024 * 1024))),
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
                "mountpoint-s3 failed: "
                f"rc={result.returncode} stderr={result.stderr.decode(errors='replace')}"
            )
        return 0

    def unmount(self, *, mount_point: Path) -> None:
        """Unmount via ``fusermount -u`` (Linux) or ``umount`` (fallback)."""
        from ginkgo.remote.access.drivers.gcsfuse import _generic_unmount

        _generic_unmount(mount_point=mount_point)


def _find_binary() -> str | None:
    """Return the first available mountpoint-s3 binary name, or ``None``."""
    for name in _CANDIDATE_BINARIES:
        if shutil.which(name) is not None:
            return name
    return None
