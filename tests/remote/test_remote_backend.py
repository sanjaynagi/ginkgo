"""Unit tests for filesystem-backed remote storage backends."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.fsspec_backends import OCIFileSystemBackend, S3FileSystemBackend
from ginkgo.remote.resolve import resolve_backend


@pytest.fixture()
def mock_filesystem() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def s3_backend(mock_filesystem: MagicMock) -> S3FileSystemBackend:
    backend = S3FileSystemBackend(region="eu-west-1")
    backend._filesystem = mock_filesystem
    return backend


@pytest.fixture()
def oci_backend(mock_filesystem: MagicMock) -> OCIFileSystemBackend:
    backend = OCIFileSystemBackend(config_path="~/.oci/config", profile="uk")
    backend._filesystem = mock_filesystem
    return backend


class TestHead:
    def test_s3_returns_metadata(
        self, s3_backend: S3FileSystemBackend, mock_filesystem: MagicMock
    ) -> None:
        mock_filesystem.info.return_value = {
            "size": 1024,
            "ETag": '"abc123"',
            "VersionId": "v1",
        }

        meta = s3_backend.head(bucket="my-bucket", key="data/file.csv")

        assert isinstance(meta, RemoteObjectMeta)
        assert meta.uri == "s3://my-bucket/data/file.csv"
        assert meta.size == 1024
        assert meta.etag == "abc123"
        assert meta.version_id == "v1"
        mock_filesystem.info.assert_called_once_with("my-bucket/data/file.csv")

    def test_oci_returns_metadata(
        self, oci_backend: OCIFileSystemBackend, mock_filesystem: MagicMock
    ) -> None:
        mock_filesystem.info.return_value = {
            "size": 33,
            "etag": "etag-1",
        }

        meta = oci_backend.head(bucket="bucket@namespace", key="path/data.fastq.gz")

        assert meta.uri == "oci://bucket@namespace/path/data.fastq.gz"
        assert meta.size == 33
        assert meta.etag == "etag-1"


class TestDownload:
    def test_s3_downloads_to_path(
        self,
        s3_backend: S3FileSystemBackend,
        mock_filesystem: MagicMock,
        tmp_path: Path,
    ) -> None:
        source = MagicMock()
        source.read.side_effect = [b"hello ", b"world", b""]
        mock_filesystem.open.return_value.__enter__.return_value = source
        mock_filesystem.info.return_value = {"size": 11, "ETag": '"etag123"'}

        dest = tmp_path / "downloaded.txt"
        meta = s3_backend.download(bucket="bkt", key="key.txt", dest_path=dest)

        assert dest.read_bytes() == b"hello world"
        assert meta.size == 11
        assert meta.etag == "etag123"


class TestUpload:
    def test_oci_uploads_file(
        self,
        oci_backend: OCIFileSystemBackend,
        mock_filesystem: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "upload_me.txt"
        src.write_text("payload", encoding="utf-8")

        destination = MagicMock()
        mock_filesystem.open.return_value.__enter__.return_value = destination
        mock_filesystem.info.return_value = {"size": 7, "etag": "uploaded-etag"}

        meta = oci_backend.upload(
            src_path=src,
            bucket="bucket@namespace",
            key="dest/file.txt",
        )

        assert meta.etag == "uploaded-etag"
        mock_filesystem.open.assert_called_once_with("bucket@namespace/dest/file.txt", "wb")


class TestListPrefix:
    def test_lists_s3_objects(
        self,
        s3_backend: S3FileSystemBackend,
        mock_filesystem: MagicMock,
    ) -> None:
        mock_filesystem.find.return_value = {
            "bkt/prefix/a.txt": {"size": 10, "ETag": '"e1"'},
            "bkt/prefix/b.txt": {"size": 20, "ETag": '"e2"'},
        }

        results = s3_backend.list_prefix(bucket="bkt", prefix="prefix/")

        assert len(results) == 2
        assert results[0].uri == "s3://bkt/prefix/a.txt"
        assert results[1].size == 20

    def test_lists_oci_objects(
        self,
        oci_backend: OCIFileSystemBackend,
        mock_filesystem: MagicMock,
    ) -> None:
        mock_filesystem.find.return_value = {
            "bucket@ns/prefix/a.txt": {"size": 10, "etag": "e1"},
            "bucket@ns/prefix/sub/b.txt": {"size": 20, "etag": "e2"},
        }

        results = oci_backend.list_prefix(bucket="bucket@ns", prefix="prefix/")

        assert len(results) == 2
        assert results[0].uri == "oci://bucket@ns/prefix/a.txt"
        assert results[1].uri == "oci://bucket@ns/prefix/sub/b.txt"


class TestResolveBackend:
    def test_s3_scheme(self) -> None:
        backend = resolve_backend("s3")
        assert isinstance(backend, S3FileSystemBackend)

    def test_oci_scheme_uses_env_profile(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_OCI_PROFILE", "uk")

        backend = resolve_backend("oci")
        assert isinstance(backend, OCIFileSystemBackend)
        assert backend._profile == "uk"

    def test_unsupported_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            resolve_backend("ftp")


class TestLazyImports:
    def test_missing_s3fs_raises_clear_error(self) -> None:
        backend = S3FileSystemBackend()
        backend._filesystem = None

        with patch.dict("sys.modules", {"s3fs": None}):
            with pytest.raises(ImportError, match="s3fs is required"):
                backend._get_filesystem()

    def test_missing_ocifs_raises_clear_error(self) -> None:
        backend = OCIFileSystemBackend()
        backend._filesystem = None

        with patch.dict("sys.modules", {"ocifs": None}):
            with pytest.raises(ImportError, match="ocifs is required"):
                backend._get_filesystem()
