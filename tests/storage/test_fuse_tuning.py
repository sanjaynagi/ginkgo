"""Tests for the gcsfuse performance defaults and fallback-warning wiring.

The gcsfuse flag tests live with the driver, in ginkgo-gcp. These cover:

2. ``_default_mount_root`` prefers a known NVMe/SSD mount when one is
   present — that is where gcsfuse's read-through cache earns its keep.
3. The evaluator emits a user-visible ``TaskNotice`` when a worker
   reports ``fallback_reason`` on its access stats, so a silent
   fuse→stage downgrade can't hide behind successful task completion.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from ginkgo.remote.access.mounted import _default_mount_root
from ginkgo.runtime.advisories import access_fallback_notice


class TestDefaultMountRoot:
    """NVMe-aware selection of the FUSE mount root."""

    def test_env_override_wins(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_FUSE_ROOT", str(tmp_path / "custom"))
        assert _default_mount_root() == tmp_path / "custom"

    def test_prefers_nvme_when_writable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GINKGO_FUSE_ROOT", raising=False)
        fake_nvme = tmp_path / "fake-ssd"
        fake_nvme.mkdir()

        # Pretend /mnt/disks/ssd exists and is writable by mapping the
        # first probed candidate to our tmp directory.
        real_is_dir = Path.is_dir
        real_access = os.access

        def fake_is_dir(self: Path) -> bool:
            if str(self) == "/mnt/disks/ssd":
                return True
            return real_is_dir(self)

        def fake_access(path, mode) -> bool:
            if str(path) == "/mnt/disks/ssd":
                return True
            return real_access(path, mode)

        with patch.object(Path, "is_dir", fake_is_dir), patch("os.access", fake_access):
            result = _default_mount_root()
        assert result == Path("/mnt/disks/ssd/ginkgo-fuse")

    def test_falls_back_to_tmp_when_no_nvme(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GINKGO_FUSE_ROOT", raising=False)

        def no_nvme(self: Path) -> bool:
            return False

        with patch.object(Path, "is_dir", no_nvme):
            # /tmp branch only fires if /tmp exists; simulate no nvme but real /tmp.
            with patch("pathlib.Path.exists", return_value=True):
                result = _default_mount_root()
        assert result == Path("/tmp/ginkgo-fuse")


class TestFallbackNotice:
    """A FUSE mount that fell back to staging is reported."""

    def test_notice_names_the_fallback_reason(self) -> None:
        notice = access_fallback_notice(
            {"policy": "fuse", "fallback_reason": "gcsfuse: /dev/fuse not accessible"}
        )

        assert notice is not None
        assert "fell back to staging" in notice
        assert "/dev/fuse" in notice

    def test_no_notice_without_a_fallback_reason(self) -> None:
        assert access_fallback_notice({"policy": "fuse", "fallback_reason": None}) is None
