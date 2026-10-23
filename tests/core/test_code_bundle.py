"""Tests for code bundle creation, publishing, and extraction."""

from __future__ import annotations

import shutil
import tarfile
from pathlib import Path

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.code_bundle import (
    _DEFAULT_EXCLUDES,
    _should_exclude,
    create_code_bundle,
    download_and_extract,
    publish_code_bundle,
)


class _DirectoryStore:
    """An object store whose buckets are directories under ``root``."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.uploads = 0

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def head(self, *, bucket: str, key: str) -> RemoteObjectMeta:
        path = self._path(bucket, key)
        if not path.is_file():
            raise FileNotFoundError(key)
        return RemoteObjectMeta(uri=f"dir://{bucket}/{key}", size=path.stat().st_size)

    def upload(self, *, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
        self.uploads += 1
        self._path(bucket, key).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, self._path(bucket, key))
        return self.head(bucket=bucket, key=key)

    def download(self, *, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
        shutil.copy2(self._path(bucket, key), dest_path)
        return self.head(bucket=bucket, key=key)


@pytest.fixture
def package(tmp_path: Path) -> Path:
    """A minimal package, with a ``__pycache__`` the bundle must leave out."""
    pkg = tmp_path / "my_workflow"
    (pkg / "sub").mkdir(parents=True)
    (pkg / "__pycache__").mkdir()
    for name, text in {
        "__init__.py": "",
        "tasks.py": "def hello(): return 42\n",
        "sub/__init__.py": "",
        "sub/helpers.py": "def h(): pass\n",
    }.items():
        (pkg / name).write_text(text, encoding="utf-8")
    (pkg / "__pycache__" / "tasks.cpython-312.pyc").write_bytes(b"\x00")
    return pkg


@pytest.mark.parametrize(
    ("name", "excluded"),
    [
        ("my_workflow/__pycache__/foo.pyc", True),
        ("my_workflow/tasks.pyc", True),
        ("my_workflow/.git/config", True),
        ("my_workflow/foo.egg-info/PKG-INFO", True),
        ("my_workflow/tasks.py", False),
        ("my_workflow/sub/helpers.py", False),
    ],
)
def test_default_excludes(name: str, excluded: bool) -> None:
    assert _should_exclude(tarfile.TarInfo(name=name), excludes=_DEFAULT_EXCLUDES) is excluded


def test_a_gitignore_match_is_excluded() -> None:
    class _Spec:
        def match_file(self, path: str) -> bool:
            return path == "my_workflow/secret.txt"

    def excluded(name: str) -> bool:
        info = tarfile.TarInfo(name=name)
        return _should_exclude(info, excludes=_DEFAULT_EXCLUDES, gitignore_spec=_Spec())

    assert excluded("my_workflow/secret.txt")
    assert not excluded("my_workflow/tasks.py")


def test_a_bundle_publishes_once_and_extracts_the_source(tmp_path: Path, package: Path) -> None:
    tarball_path, digest = create_code_bundle(package_path=package)
    store = _DirectoryStore(tmp_path / "remote")
    try:
        with tarfile.open(tarball_path, "r:gz") as tar:
            names = tar.getnames()
        assert len(digest) == 64  # SHA-256 hex
        assert {"my_workflow/tasks.py", "my_workflow/sub/helpers.py"} <= set(names)
        assert not [name for name in names if "__pycache__" in name]

        publish = {"backend": store, "bucket": "b", "prefix": "artifacts/", "digest": digest}
        key = publish_code_bundle(bundle_path=tarball_path, **publish)
        assert key == f"artifacts/code-bundles/{digest}.tar.gz"
        assert publish_code_bundle(bundle_path=tarball_path, **publish) == key
        assert store.uploads == 1, "a published bundle was uploaded again"
    finally:
        tarball_path.unlink()

    dest_dir = tmp_path / "extracted"
    assert download_and_extract(backend=store, bucket="b", key=key, dest_dir=dest_dir) == dest_dir
    assert (dest_dir / "my_workflow" / "tasks.py").read_text() == "def hello(): return 42\n"
    assert not (dest_dir / "code-bundle.tar.gz").exists()


def test_missing_package_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Package directory not found"):
        create_code_bundle(package_path=tmp_path / "nonexistent")
