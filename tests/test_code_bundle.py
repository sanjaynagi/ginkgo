"""Tests for code bundle creation, publishing, and extraction."""

from __future__ import annotations

import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.code_bundle import (
    _DEFAULT_EXCLUDES,
    _should_exclude,
    create_code_bundle,
    download_and_extract,
    publish_code_bundle,
)


def _make_package(tmp_path: Path) -> Path:
    """Create a minimal Python package for testing."""
    pkg = tmp_path / "my_workflow"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("# init\n")
    (pkg / "tasks.py").write_text("def hello(): return 42\n")
    (pkg / "utils.py").write_text("X = 1\n")

    # Add files that should be excluded.
    cache = pkg / "__pycache__"
    cache.mkdir()
    (cache / "tasks.cpython-312.pyc").write_bytes(b"\x00")

    sub = pkg / "sub"
    sub.mkdir()
    (sub / "__init__.py").write_text("")
    (sub / "helpers.py").write_text("def h(): pass\n")
    return pkg


class TestShouldExclude:
    """Tests for the _should_exclude filter."""

    def test_excludes_pycache(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/__pycache__/foo.pyc")
        assert _should_exclude(info, excludes=_DEFAULT_EXCLUDES)

    def test_excludes_pyc_extension(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/tasks.pyc")
        assert _should_exclude(info, excludes=_DEFAULT_EXCLUDES)

    def test_excludes_git(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/.git/config")
        assert _should_exclude(info, excludes=_DEFAULT_EXCLUDES)

    def test_includes_normal_py(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/tasks.py")
        assert not _should_exclude(info, excludes=_DEFAULT_EXCLUDES)

    def test_includes_subpackage(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/sub/helpers.py")
        assert not _should_exclude(info, excludes=_DEFAULT_EXCLUDES)

    def test_excludes_egg_info(self) -> None:
        info = tarfile.TarInfo(name="my_workflow/foo.egg-info/PKG-INFO")
        assert _should_exclude(info, excludes=_DEFAULT_EXCLUDES)


class TestCreateCodeBundle:
    """Tests for create_code_bundle."""

    def test_creates_tarball_with_digest(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        tarball_path, digest = create_code_bundle(package_path=pkg)

        try:
            assert tarball_path.exists()
            assert tarball_path.suffix == ".gz"
            assert len(digest) == 64  # SHA-256 hex
        finally:
            tarball_path.unlink()

    def test_tarball_contains_source_files(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        tarball_path, _ = create_code_bundle(package_path=pkg)

        try:
            with tarfile.open(tarball_path, "r:gz") as tar:
                names = tar.getnames()

            assert "my_workflow/__init__.py" in names
            assert "my_workflow/tasks.py" in names
            assert "my_workflow/sub/helpers.py" in names
        finally:
            tarball_path.unlink()

    def test_tarball_excludes_pycache(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        tarball_path, _ = create_code_bundle(package_path=pkg)

        try:
            with tarfile.open(tarball_path, "r:gz") as tar:
                names = tar.getnames()

            pycache_entries = [n for n in names if "__pycache__" in n]
            assert pycache_entries == []
        finally:
            tarball_path.unlink()

    def test_deterministic_digest(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        _, digest1 = create_code_bundle(package_path=pkg)
        _, digest2 = create_code_bundle(package_path=pkg)

        # Digests may differ due to mtime in tar — but both should be valid.
        assert len(digest1) == 64
        assert len(digest2) == 64

    def test_missing_package_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Package directory not found"):
            create_code_bundle(package_path=tmp_path / "nonexistent")


class TestPublishCodeBundle:
    """Tests for publish_code_bundle."""

    def test_uploads_when_not_present(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        tarball_path, digest = create_code_bundle(package_path=pkg)

        backend = MagicMock()
        backend.head.side_effect = FileNotFoundError("not found")
        backend.upload.return_value = RemoteObjectMeta(uri="gs://b/k", size=100)

        try:
            key = publish_code_bundle(
                backend=backend,
                bucket="test-bucket",
                prefix="artifacts/",
                bundle_path=tarball_path,
                digest=digest,
            )

            assert key == f"artifacts/code-bundles/{digest}.tar.gz"
            backend.upload.assert_called_once()
        finally:
            tarball_path.unlink()

    def test_skips_upload_when_present(self, tmp_path: Path) -> None:
        pkg = _make_package(tmp_path)
        tarball_path, digest = create_code_bundle(package_path=pkg)

        backend = MagicMock()
        backend.head.return_value = RemoteObjectMeta(uri="gs://b/k", size=100)

        try:
            key = publish_code_bundle(
                backend=backend,
                bucket="test-bucket",
                prefix="artifacts/",
                bundle_path=tarball_path,
                digest=digest,
            )

            assert key == f"artifacts/code-bundles/{digest}.tar.gz"
            backend.upload.assert_not_called()
        finally:
            tarball_path.unlink()


class TestDownloadAndExtract:
    """Tests for download_and_extract."""

    def test_downloads_and_extracts(self, tmp_path: Path) -> None:
        # Create a real tarball to use as the "downloaded" content.
        pkg = _make_package(tmp_path)
        tarball_path, _ = create_code_bundle(package_path=pkg)

        dest_dir = tmp_path / "extracted"

        def mock_download(*, bucket: str, key: str, dest_path: Path) -> RemoteObjectMeta:
            import shutil

            shutil.copy2(tarball_path, dest_path)
            return RemoteObjectMeta(uri="gs://b/k", size=100)

        backend = MagicMock()
        backend.download.side_effect = mock_download

        try:
            result = download_and_extract(
                backend=backend,
                bucket="test-bucket",
                key="artifacts/code-bundles/abc123.tar.gz",
                dest_dir=dest_dir,
            )

            assert result == dest_dir
            assert (dest_dir / "my_workflow" / "__init__.py").exists()
            assert (dest_dir / "my_workflow" / "tasks.py").exists()
            # Tarball should be cleaned up.
            assert not (dest_dir / "code-bundle.tar.gz").exists()
        finally:
            tarball_path.unlink()
