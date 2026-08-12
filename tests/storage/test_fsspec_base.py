"""Tests for the fsspec backend base class and GCS backend."""

from unittest.mock import MagicMock, patch

import pytest

from ginkgo.remote.fsspec_backends import (
    FsspecBackend,
    GCSFileSystemBackend,
    OCIFileSystemBackend,
    S3FileSystemBackend,
)


class TestFsspecBackendHierarchy:
    """Verify the inheritance hierarchy after base class extraction."""

    def test_s3_is_subclass(self) -> None:
        assert issubclass(S3FileSystemBackend, FsspecBackend)

    def test_oci_is_subclass(self) -> None:
        assert issubclass(OCIFileSystemBackend, FsspecBackend)

    def test_gcs_is_subclass(self) -> None:
        assert issubclass(GCSFileSystemBackend, FsspecBackend)

    def test_s3_scheme(self) -> None:
        backend = S3FileSystemBackend(region="us-west-2")
        assert backend._scheme == "s3"

    def test_oci_scheme(self) -> None:
        backend = OCIFileSystemBackend()
        assert backend._scheme == "oci"

    def test_gcs_scheme(self) -> None:
        backend = GCSFileSystemBackend()
        assert backend._scheme == "gs"


class TestGCSFileSystemBackend:
    """Tests for the GCS backend with a mocked filesystem."""

    def _make_backend(self) -> GCSFileSystemBackend:
        backend = GCSFileSystemBackend(project="test-project")
        mock_fs = MagicMock()
        backend._filesystem = mock_fs
        return backend

    def test_head_returns_metadata(self) -> None:
        backend = self._make_backend()
        backend._filesystem.info.return_value = {
            "size": 1024,
            "etag": '"abc123"',
        }

        meta = backend.head(bucket="my-bucket", key="data/file.csv")

        assert meta.uri == "gs://my-bucket/data/file.csv"
        assert meta.size == 1024
        assert meta.etag == "abc123"

    def test_download_writes_to_path(self, tmp_path) -> None:
        backend = self._make_backend()
        content = b"hello world"
        mock_file = MagicMock()
        mock_file.read.side_effect = [content, b""]
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        backend._filesystem.open.return_value = mock_file
        backend._filesystem.info.return_value = {"size": len(content)}

        dest = tmp_path / "output.csv"
        meta = backend.download(bucket="my-bucket", key="data/file.csv", dest_path=dest)

        assert meta.size == len(content)
        backend._filesystem.open.assert_called_once()

    def test_upload_reads_from_path(self, tmp_path) -> None:
        backend = self._make_backend()
        src = tmp_path / "input.csv"
        src.write_bytes(b"data")

        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        backend._filesystem.open.return_value = mock_file
        backend._filesystem.info.return_value = {"size": 4}

        meta = backend.upload(src_path=src, bucket="my-bucket", key="data/input.csv")

        assert meta.size == 4

    def test_list_prefix(self) -> None:
        backend = self._make_backend()
        backend._filesystem.find.return_value = {
            "my-bucket/prefix/a.csv": {"size": 100, "etag": '"e1"'},
            "my-bucket/prefix/b.csv": {"size": 200, "etag": '"e2"'},
        }

        results = backend.list_prefix(bucket="my-bucket", prefix="prefix/")

        assert len(results) == 2
        assert results[0].uri == "gs://my-bucket/prefix/a.csv"
        assert results[1].uri == "gs://my-bucket/prefix/b.csv"

    def test_missing_gcsfs_raises(self) -> None:
        backend = GCSFileSystemBackend()
        with patch.dict("sys.modules", {"gcsfs": None}):
            with pytest.raises(ImportError, match="gcsfs is required"):
                backend._get_filesystem()
