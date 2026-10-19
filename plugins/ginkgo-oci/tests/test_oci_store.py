"""The oci:// object store contributed by ginkgo-oci, and its URI layout."""

from __future__ import annotations

import pytest

from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, remote_file, remote_folder
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import resolve_backend
from ginkgo_oci.store import OCIFileSystemBackend


class TestOCIUris:
    def test_oci_uri(self) -> None:
        ref = remote_file("oci://mynamespace/mybucket/path/to/object.csv")
        assert isinstance(ref, RemoteFileRef)
        assert ref.scheme == "oci"
        assert ref.bucket == "mybucket@mynamespace"
        assert ref.namespace == "mynamespace"
        assert ref.key == "path/to/object.csv"

    def test_oci_uri_nested_key(self) -> None:
        ref = remote_file("oci://ns/bkt/a/b/c.txt")
        assert ref.bucket == "bkt@ns"
        assert ref.namespace == "ns"
        assert ref.key == "a/b/c.txt"

    def test_oci_bucket_at_namespace_uri(self) -> None:
        ref = remote_file("oci://mybucket@mynamespace/path/to/object.csv")
        assert ref.bucket == "mybucket@mynamespace"
        assert ref.namespace == "mynamespace"
        assert ref.key == "path/to/object.csv"

    def test_oci_missing_namespace_raises(self) -> None:
        with pytest.raises(ValueError, match="missing namespace"):
            remote_file("oci:///bucket/key")

    def test_oci_missing_key_raises(self) -> None:
        with pytest.raises(ValueError, match="oci://namespace/bucket/key"):
            remote_file("oci://namespace/bucket")

    def test_oci_prefix(self) -> None:
        ref = remote_folder("oci://ns/bkt/prefix/")
        assert isinstance(ref, RemoteFolderRef)
        assert ref.bucket == "bkt@ns"
        assert ref.key == "prefix/"


class TestBuildOCIStore:
    def test_profile_comes_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_OCI_PROFILE", "uk")

        backend = resolve_backend("oci")

        assert isinstance(backend, OCIFileSystemBackend)
        assert isinstance(backend, FsspecBackend)
        assert backend._scheme == "oci"
        assert backend._profile == "uk"
