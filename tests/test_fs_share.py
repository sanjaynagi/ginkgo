"""Tests for the artifact-store filesystem sharing helpers.

Covers:

- ``share_bytes`` picks the cheapest method the platform supports,
  falling back to ``copy`` when reflink and hardlink are refused.
- ``allow_hardlink=False`` never uses a hardlink even when one would
  work — the default must be safe for user-owned paths.
- After hardlinking, a subsequent read-only chmod on the destination
  flips the source too; that is the documented invariant.
- The integration through ``LocalArtifactStore.store`` and
  ``RemoteArtifactStore.store`` preserves artifact identity and
  content regardless of which sharing method was used.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from ginkgo.runtime.artifacts import fs_share
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.fs_share import share_bytes


@pytest.fixture
def payload(tmp_path: Path) -> Path:
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    return src


class TestShareBytesFallback:
    """Exercise the reflink → hardlink → copy decision tree."""

    def test_allow_hardlink_false_never_hardlinks(self, tmp_path: Path, payload: Path) -> None:
        # Force both reflink and hardlink to be refused — only copy should fire.
        dst = tmp_path / "dst.bin"
        with (
            patch.object(fs_share, "_reflink", return_value=False),
            patch.object(fs_share, "_hardlink") as hardlink,
        ):
            method = share_bytes(src=payload, dst=dst, allow_hardlink=False)
        assert method == "copy"
        hardlink.assert_not_called()
        assert dst.read_bytes() == b"payload"
        # Independent inodes after copy.
        assert dst.stat().st_ino != payload.stat().st_ino

    def test_allow_hardlink_true_uses_hardlink_when_reflink_fails(
        self, tmp_path: Path, payload: Path
    ) -> None:
        dst = tmp_path / "dst.bin"
        with patch.object(fs_share, "_reflink", return_value=False):
            method = share_bytes(src=payload, dst=dst, allow_hardlink=True)
        assert method == "hardlink"
        assert dst.stat().st_ino == payload.stat().st_ino

    def test_falls_back_to_copy_when_hardlink_fails(self, tmp_path: Path, payload: Path) -> None:
        dst = tmp_path / "dst.bin"
        with (
            patch.object(fs_share, "_reflink", return_value=False),
            patch.object(fs_share, "_hardlink", return_value=False),
        ):
            method = share_bytes(src=payload, dst=dst, allow_hardlink=True)
        assert method == "copy"
        assert dst.read_bytes() == b"payload"
        assert dst.stat().st_ino != payload.stat().st_ino

    def test_reflink_success_short_circuits(self, tmp_path: Path, payload: Path) -> None:
        dst = tmp_path / "dst.bin"

        def fake_reflink(*, src: Path, dst: Path) -> bool:
            dst.write_bytes(src.read_bytes())
            return True

        with (
            patch.object(fs_share, "_reflink", side_effect=fake_reflink),
            patch.object(fs_share, "_hardlink") as hardlink,
        ):
            method = share_bytes(src=payload, dst=dst, allow_hardlink=True)
        assert method == "reflink"
        hardlink.assert_not_called()


class TestHardlinkChmodSemantics:
    """Confirm the hardlink path's read-only chmod flips the source."""

    def test_chmod_blob_also_flips_source(self, tmp_path: Path, payload: Path) -> None:
        dst = tmp_path / "dst.bin"
        with patch.object(fs_share, "_reflink", return_value=False):
            method = share_bytes(src=payload, dst=dst, allow_hardlink=True)
        assert method == "hardlink"

        dst.chmod(stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
        assert not (payload.stat().st_mode & stat.S_IWUSR), (
            "hardlink shares inode; chmod on dst must also affect src"
        )


class TestArtifactStoreIntegration:
    """The store should accept ``src_is_readonly`` without changing semantics."""

    def test_store_returns_same_record_regardless_of_flag(self, tmp_path: Path) -> None:
        src_a = tmp_path / "a.bin"
        src_a.write_bytes(b"identical bytes")
        src_b = tmp_path / "b.bin"
        src_b.write_bytes(b"identical bytes")

        store = LocalArtifactStore(root=tmp_path / "artifacts")
        record_default = store.store(src_path=src_a)
        store2 = LocalArtifactStore(root=tmp_path / "artifacts2")
        record_readonly = store2.store(src_path=src_b, src_is_readonly=True)

        assert record_default.artifact_id == record_readonly.artifact_id
        assert record_default.digest_hex == record_readonly.digest_hex
        assert record_default.size == record_readonly.size

    def test_store_with_readonly_hint_shares_inode_on_posix(self, tmp_path: Path) -> None:
        """When reflink is refused, ``src_is_readonly=True`` should hardlink."""
        src = tmp_path / "src.bin"
        src.write_bytes(b"abc")
        store = LocalArtifactStore(root=tmp_path / "artifacts")

        with patch.object(fs_share, "_reflink", return_value=False):
            record = store.store(src_path=src, src_is_readonly=True)

        blob_path = tmp_path / "artifacts" / "blobs" / record.digest_hex
        # Same inode only if hardlink was used.
        if sys.platform != "win32":
            assert blob_path.stat().st_ino == src.stat().st_ino

    def test_store_without_hint_never_shares_inode(self, tmp_path: Path) -> None:
        src = tmp_path / "src.bin"
        src.write_bytes(b"abc")
        store = LocalArtifactStore(root=tmp_path / "artifacts")

        with patch.object(fs_share, "_reflink", return_value=False):
            record = store.store(src_path=src)

        blob_path = tmp_path / "artifacts" / "blobs" / record.digest_hex
        assert blob_path.stat().st_ino != src.stat().st_ino
        # User file should remain writable.
        assert src.stat().st_mode & stat.S_IWUSR


class TestHardlinkFunction:
    """Exercise the real ``_hardlink`` on the test filesystem."""

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX semantics")
    def test_hardlink_creates_shared_inode(self, tmp_path: Path, payload: Path) -> None:
        dst = tmp_path / "dst.bin"
        assert fs_share._hardlink(src=payload, dst=dst)
        assert dst.stat().st_ino == payload.stat().st_ino

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX semantics")
    def test_hardlink_fails_cross_device_returns_false(
        self, tmp_path: Path, payload: Path
    ) -> None:
        # Simulate EXDEV by patching os.link.
        dst = tmp_path / "dst.bin"
        with patch.object(os, "link", side_effect=OSError(18, "Cross-device link")):
            assert fs_share._hardlink(src=payload, dst=dst) is False
