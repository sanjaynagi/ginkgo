"""Tests for GCS URI parsing and remote backend resolution."""

import pytest

from ginkgo.plugins.registry import PluginNotFoundError
from ginkgo.core.remote import is_remote_uri, remote_file, remote_folder


class TestGCSUriParsing:
    """Tests for gs:// URI support in core/remote.py."""

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
        with pytest.raises(ValueError, match="GCS URI missing bucket"):
            remote_file("gs:///key")

    def test_gs_missing_key_raises(self) -> None:
        with pytest.raises(ValueError, match="GCS URI missing key"):
            remote_file("gs://my-bucket")

    def test_gs_missing_key_trailing_slash(self) -> None:
        with pytest.raises(ValueError, match="GCS URI missing key"):
            remote_file("gs://my-bucket/")


class TestResolveBackendGCS:
    """Tests for resolve_backend with gs:// scheme."""

    def test_resolve_gs_returns_gcs_backend(self, monkeypatch) -> None:
        from ginkgo.remote.resolve import resolve_backend

        monkeypatch.setenv("GINKGO_REMOTE_GCS_PROJECT", "test-project")

        backend = resolve_backend("gs")
        from ginkgo.remote.fsspec_backends import GCSFileSystemBackend

        assert isinstance(backend, GCSFileSystemBackend)
        assert backend._project == "test-project"

    def test_gcs_project_comes_from_the_remote_gcs_table(self, monkeypatch) -> None:
        from ginkgo.plugins.api import ObjectStoreContext
        from ginkgo.remote.resolve import build_gcs_store

        monkeypatch.delenv("GINKGO_REMOTE_GCS_PROJECT", raising=False)
        monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)

        backend = build_gcs_store(
            ObjectStoreContext(config={"remote": {"gcs": {"project": "p1"}}})
        )

        assert backend._project == "p1"

    def test_resolve_unsupported_raises(self) -> None:
        from ginkgo.remote.resolve import resolve_backend

        with pytest.raises(PluginNotFoundError, match="No installed plugin provides object store"):
            resolve_backend("ftp")
