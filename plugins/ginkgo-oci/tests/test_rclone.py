"""The rclone FUSE driver ginkgo-oci uses for oci:// mounts."""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo_oci.rclone as rclone_mod
from ginkgo.remote.access.drivers.base import MountFailedError, MountSpec


class TestRcloneDriver:
    def _spec(self, tmp_path: Path, *, bucket: str) -> MountSpec:
        return MountSpec(
            scheme="oci",
            bucket=bucket,
            mount_point=tmp_path / "mount",
            cache_dir=None,
            cache_max_bytes=None,
            read_only=True,
        )

    def test_mount_strips_oci_namespace_from_bucket(self, tmp_path: Path, monkeypatch) -> None:
        commands: list[list[str]] = []

        def _fake_run(cmd, **kwargs):  # noqa: ARG001
            commands.append(cmd)

            class _Result:
                returncode = 0
                stderr = b""

            return _Result()

        monkeypatch.setattr(rclone_mod.subprocess, "run", _fake_run)
        driver = rclone_mod.RcloneDriver()
        driver.mount(spec=self._spec(tmp_path, bucket="my-bucket@my-namespace"))
        assert commands[0][2] == "oci:my-bucket"

    def test_mount_probe_failure_raises_and_unmounts(self, tmp_path: Path, monkeypatch) -> None:
        def _fake_run(cmd, **kwargs):  # noqa: ARG001
            class _Result:
                returncode = 0
                stderr = b""

            return _Result()

        def _fail_listdir(path):  # noqa: ARG001
            raise OSError(5, "Input/output error")

        unmounted: list[Path] = []
        monkeypatch.setattr(rclone_mod.subprocess, "run", _fake_run)
        monkeypatch.setattr(rclone_mod.os, "listdir", _fail_listdir)
        driver = rclone_mod.RcloneDriver()
        monkeypatch.setattr(
            driver, "unmount", lambda *, mount_point: unmounted.append(mount_point)
        )
        with pytest.raises(MountFailedError, match="not readable"):
            driver.mount(spec=self._spec(tmp_path, bucket="my-bucket@my-namespace"))
        assert unmounted == [tmp_path / "mount"]
