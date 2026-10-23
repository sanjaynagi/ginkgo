"""Tests for the ``.ginkgo/`` directory layout value object.

The layout's job is to keep the directory convention in one place, so the
tests that matter pin the names it hands out and check that real call sites
still land where they did before it existed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.runtime.artifacts.remote_arg_transfer import _is_managed_cas_blob
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import DIRECTORY_NAME, WorkspaceLayout


def test_construction(tmp_path):
    # Compared via resolve() because macOS reports /private/var for /var.
    assert WorkspaceLayout.for_cwd().root.resolve() == (tmp_path / DIRECTORY_NAME).resolve()
    assert WorkspaceLayout.relative().root == Path(DIRECTORY_NAME)
    assert not WorkspaceLayout.relative().runs.is_absolute()
    assert WorkspaceLayout(root=tmp_path) == WorkspaceLayout(root=tmp_path)
    assert len({WorkspaceLayout(root=tmp_path), WorkspaceLayout(root=tmp_path)}) == 1


def test_sibling_of_round_trips_every_directory(tmp_path):
    layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)

    for path in (layout.runs, layout.cache, layout.artifacts):
        assert WorkspaceLayout.sibling_of(path) == layout
    # Store roots are caller-supplied, so this must not require .ginkgo.
    assert WorkspaceLayout.sibling_of(tmp_path / "scratch").artifacts == tmp_path / "artifacts"


def test_every_concern_has_its_own_stable_directory(tmp_path, monkeypatch):
    monkeypatch.delenv("GINKGO_DB", raising=False)
    layout = WorkspaceLayout(root=tmp_path / DIRECTORY_NAME)
    names = ("runs", "cache", "artifacts", "staging", "fuse", "notebooks", "reports")

    assert [getattr(layout, name) for name in names] == [layout.root / name for name in names]
    assert layout.db == layout.root / "ginkgo.db"
    # Handing out paths creates nothing.
    assert not layout.root.exists()


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ("{tmp}/local/ledger.db", "{tmp}/local/ledger.db"),
        # An unset-looking variable must not resolve the database to the cwd.
        ("", "{tmp}/.ginkgo/ginkgo.db"),
    ],
)
def test_ginkgo_db_relocates_the_database(tmp_path, monkeypatch, override, expected):
    monkeypatch.setenv("GINKGO_DB", override.format(tmp=tmp_path))

    assert WorkspaceLayout(root=tmp_path / DIRECTORY_NAME).db == Path(
        expected.format(tmp=tmp_path)
    )


def test_cache_store_resolves_its_directories_through_the_layout(tmp_path):
    store = CacheStore(root=tmp_path / DIRECTORY_NAME / "cache", index=CacheIndex.in_memory())
    default = CacheStore(index=CacheIndex.in_memory())

    assert store._artifact_store._root == tmp_path / DIRECTORY_NAME / "artifacts"
    assert default._root.resolve() == (tmp_path / DIRECTORY_NAME / "cache").resolve()


@pytest.mark.parametrize(
    ("path", "managed"),
    [
        (f"/work/{DIRECTORY_NAME}/staging/blobs/abc123/data.bin", True),
        (f"/work/{DIRECTORY_NAME}/artifacts/blobs/abc123/data.bin", True),
        ("/work/other/blobs/abc/data.bin", False),
        (f"/work/{DIRECTORY_NAME}/cache/abc/data.bin", False),
    ],
)
def test_managed_blob_detection(path: str, managed: bool):
    assert _is_managed_cas_blob(path=Path(path)) is managed
