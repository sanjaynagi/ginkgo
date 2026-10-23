"""``share_bytes``: reflink, else hardlink when the caller allows it, else copy.

Reflink support depends on the filesystem, so the fallback cases refuse it
explicitly; hardlinks are never used unless the caller says the source is
read-only, so the store's chmod cannot bleed back into a user's file.
"""

from __future__ import annotations

import errno
import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest

from ginkgo.runtime.artifacts import fs_share
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.fs_share import share_bytes
from ginkgo.runtime.caching.index import CacheIndex


@pytest.fixture
def payload(tmp_path: Path) -> Path:
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    return src


def test_whatever_the_filesystem_offers_the_bytes_arrive(tmp_path: Path, payload: Path) -> None:
    dst = tmp_path / "dst.bin"

    assert share_bytes(src=payload, dst=dst, allow_hardlink=True) in {"reflink", "hardlink"}
    assert dst.read_bytes() == b"payload"


@pytest.mark.parametrize(
    ("allow_hardlink", "hardlink_works", "expected"),
    [(False, True, "copy"), (True, True, "hardlink"), (True, False, "copy")],
    ids=["hardlink-not-allowed", "hardlink-allowed", "hardlink-refused"],
)
def test_without_reflink_the_fallback_follows_the_caller_s_permission(
    tmp_path: Path, payload: Path, allow_hardlink: bool, hardlink_works: bool, expected: str
) -> None:
    dst = tmp_path / "dst.bin"

    def cross_device(src, dst):
        raise OSError(errno.EXDEV, "Cross-device link")

    with (
        patch.object(fs_share, "_reflink", return_value=False),
        patch.object(os, "link", os.link if hardlink_works else cross_device),
    ):
        assert share_bytes(src=payload, dst=dst, allow_hardlink=allow_hardlink) == expected

    assert dst.read_bytes() == b"payload"
    assert (dst.stat().st_ino == payload.stat().st_ino) is (expected == "hardlink")


@pytest.mark.parametrize("readonly_hint", [False, True])
def test_the_store_hardlinks_only_a_source_declared_read_only(
    tmp_path: Path, readonly_hint: bool
) -> None:
    src = tmp_path / "src.bin"
    src.write_bytes(b"abc")
    store = LocalArtifactStore(root=tmp_path / "artifacts", index=CacheIndex.in_memory())

    with patch.object(fs_share, "_reflink", return_value=False):
        record = store.store(src_path=src, src_is_readonly=readonly_hint)

    blob = store.artifact_path(artifact_id=record.artifact_id)
    assert blob.read_bytes() == b"abc"
    assert (blob.stat().st_ino == src.stat().st_ino) is readonly_hint
    if not readonly_hint:
        assert src.stat().st_mode & stat.S_IWUSR
