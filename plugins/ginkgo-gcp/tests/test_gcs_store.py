"""The gs:// object store contributed by ginkgo-gcp."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ginkgo.core.remote import is_remote_uri, remote_file, remote_folder
from ginkgo.plugins.api import ObjectStoreContext
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import resolve_backend
from ginkgo_gcp.store import GCSFileSystemBackend, build_gcs_store


class TestGCSUriParsing:
    """gs:// URIs parse as bucket and key."""

    def test_is_remote_uri_gs(self) -> None:
        assert is_remote_uri("gs://my-bucket/key")

    def test_remote_file_gs(self) -> None:
        ref = remote_file("gs://my-bucket/data/file.csv")
        assert ref.scheme == "gs"
        assert ref.bucket == "my-bucket"
        assert ref.key == "data/file.csv"
        assert ref.namespace is None
        assert ref.version_id is None

    def test_remote_file_gs_with_version(self) -> None:
        ref = remote_file("gs://my-bucket/data/file.csv", version_id="v123")
        assert ref.version_id == "v123"

    def test_remote_folder_gs(self) -> None:
        ref = remote_folder("gs://my-bucket/data/prefix/")
        assert ref.scheme == "gs"
        assert ref.bucket == "my-bucket"
        assert ref.key == "data/prefix/"

    def test_gs_missing_bucket_raises(self) -> None:
        with pytest.raises(ValueError, match="gs:// URI missing bucket"):
            remote_file("gs:///key")

    def test_gs_missing_key_raises(self) -> None:
        with pytest.raises(ValueError, match="gs:// URI missing key"):
            remote_file("gs://my-bucket")

    def test_gs_missing_key_trailing_slash(self) -> None:
        with pytest.raises(ValueError, match="gs:// URI missing key"):
            remote_file("gs://my-bucket/")


class TestBuildGCSStore:
    def test_the_installed_plugin_resolves_gs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_GCS_PROJECT", "test-project")

        backend = resolve_backend("gs")

        assert isinstance(backend, GCSFileSystemBackend)
        assert isinstance(backend, FsspecBackend)
        assert backend._scheme == "gs"
        assert backend._project == "test-project"

    def test_project_comes_from_the_remote_gcs_table(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("GINKGO_REMOTE_GCS_PROJECT", raising=False)
        monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)

        backend = build_gcs_store(
            ObjectStoreContext(config={"remote": {"gcs": {"project": "p1"}}})
        )

        assert isinstance(backend, GCSFileSystemBackend)
        assert backend._project == "p1"


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
