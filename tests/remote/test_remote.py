"""Unit tests for remote reference types and URI parsing."""

from __future__ import annotations

import pytest

from ginkgo.plugins.registry import PluginNotFoundError
from ginkgo.core.remote import (
    RemoteFileRef,
    RemoteFolderRef,
    is_remote_uri,
    remote_file,
    remote_folder,
)


class TestRemoteFile:
    def test_s3_uri(self) -> None:
        ref = remote_file("s3://my-bucket/data/sample.txt")
        assert isinstance(ref, RemoteFileRef)
        assert ref.scheme == "s3"
        assert ref.bucket == "my-bucket"
        assert ref.key == "data/sample.txt"
        assert ref.uri == "s3://my-bucket/data/sample.txt"
        assert ref.version_id is None

    def test_s3_uri_with_version(self) -> None:
        ref = remote_file("s3://bucket/key.csv", version_id="abc123")
        assert ref.version_id == "abc123"

    def test_unsupported_scheme_raises(self) -> None:
        with pytest.raises(PluginNotFoundError, match="object store for scheme 'ftp'"):
            remote_file("ftp://host/file.txt")

    def test_a_first_party_scheme_names_the_plugin_to_install(self) -> None:
        with pytest.raises(PluginNotFoundError, match="Install ginkgo-oci to add it"):
            remote_file("oci://ns/bkt/key")

    def test_a_uri_without_a_scheme_raises(self) -> None:
        with pytest.raises(ValueError, match="scheme://"):
            remote_file("bucket/key")

    def test_s3_missing_bucket_raises(self) -> None:
        with pytest.raises(ValueError, match="missing bucket"):
            remote_file("s3:///key")

    def test_s3_missing_key_raises(self) -> None:
        with pytest.raises(ValueError, match="missing key"):
            remote_file("s3://bucket")

    def test_immutable(self) -> None:
        ref = remote_file("s3://b/k.txt")
        with pytest.raises(AttributeError):
            ref.uri = "s3://other/key"  # type: ignore[misc]


class TestRemoteFolder:
    def test_s3_prefix(self) -> None:
        ref = remote_folder("s3://bucket/prefix/subdir/")
        assert isinstance(ref, RemoteFolderRef)
        assert ref.scheme == "s3"
        assert ref.bucket == "bucket"
        assert ref.key == "prefix/subdir/"


class TestIsRemoteUri:
    def test_s3(self) -> None:
        assert is_remote_uri("s3://bucket/key") is True

    def test_a_first_party_scheme_without_its_plugin(self) -> None:
        # Still a remote URI, so using it fails naming ginkgo-oci rather than
        # being mistaken for a local path.
        assert is_remote_uri("oci://ns/bkt/key") is True

    def test_local_path(self) -> None:
        assert is_remote_uri("/tmp/file.txt") is False

    def test_unsupported_scheme(self) -> None:
        assert is_remote_uri("ftp://host/file") is False

    def test_empty_string(self) -> None:
        assert is_remote_uri("") is False

    def test_not_a_string(self) -> None:
        assert is_remote_uri(42) is False  # type: ignore[arg-type]


class TestExportedFromGinkgo:
    def test_remote_file_importable(self) -> None:
        from ginkgo import remote_file as rf

        ref = rf("s3://b/k.txt")
        assert ref.scheme == "s3"

    def test_remote_folder_importable(self) -> None:
        from ginkgo import remote_folder as rf

        ref = rf("s3://b/prefix/")
        assert ref.scheme == "s3"
