"""Where FUSE mounts go, and the notice a fallback to staging leaves.

The gcsfuse flag tests live with the driver, in ginkgo-gcp.
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from ginkgo.remote.access.mounted import _default_mount_root
from ginkgo.runtime.advisories import access_fallback_notice

_NVME = "/mnt/disks/ssd"


def test_the_environment_override_wins(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GINKGO_FUSE_ROOT", str(tmp_path / "custom"))
    assert _default_mount_root() == tmp_path / "custom"


@pytest.mark.parametrize(
    ("nvme_present", "expected"),
    [(True, Path(_NVME) / "ginkgo-fuse"), (False, Path("/tmp/ginkgo-fuse"))],
    ids=["nvme", "tmp"],
)
def test_a_writable_nvme_mount_is_preferred_to_tmp(
    monkeypatch: pytest.MonkeyPatch, nvme_present: bool, expected: Path
) -> None:
    """A local SSD is where gcsfuse's read-through cache earns its keep."""
    monkeypatch.delenv("GINKGO_FUSE_ROOT", raising=False)
    real_is_dir, real_access = Path.is_dir, os.access

    def is_dir(self: Path) -> bool:
        return nvme_present if str(self) == _NVME else real_is_dir(self) and nvme_present

    def access(path, mode) -> bool:
        return nvme_present if str(path) == _NVME else real_access(path, mode)

    with patch.object(Path, "is_dir", is_dir), patch("os.access", access):
        assert _default_mount_root() == expected


def test_a_fallback_to_staging_is_noticed_with_its_reason() -> None:
    notice = access_fallback_notice(
        {"policy": "fuse", "fallback_reason": "gcsfuse: /dev/fuse not accessible"}
    )

    assert notice is not None
    assert "fell back to staging" in notice and "/dev/fuse" in notice
    assert access_fallback_notice({"policy": "fuse", "fallback_reason": None}) is None
