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


class TestOCIAuth:
    def _built_kwargs(self, monkeypatch: pytest.MonkeyPatch) -> dict:
        """Return the keyword arguments the store passes to ``OCIFileSystem``."""
        import ocifs

        captured: dict = {}
        monkeypatch.setattr(ocifs, "OCIFileSystem", lambda **kwargs: captured.update(kwargs))
        resolve_backend("oci").filesystem()
        return captured

    def test_workload_identity_signs_without_a_config_file(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_OCI_AUTH", "oke_workload_identity")
        monkeypatch.setenv("GINKGO_REMOTE_OCI_REGION", "uk-london-1")
        monkeypatch.setenv("GINKGO_REMOTE_OCI_PROFILE", "ignored")

        assert self._built_kwargs(monkeypatch) == {
            "iam_type": "oke_principal",
            "region": "uk-london-1",
        }

    def test_api_key_is_the_default_and_reads_the_config_file(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_OCI_CONFIG", "/etc/oci/config")
        monkeypatch.setenv("GINKGO_REMOTE_OCI_PROFILE", "uk")

        assert self._built_kwargs(monkeypatch) == {
            "iam_type": "api_key",
            "config": "/etc/oci/config",
            "profile": "uk",
        }

    def test_auth_can_come_from_config(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / "ginkgo.toml").write_text(
            '[remote.oci]\nauth = "instance_principal"\n', encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)

        assert self._built_kwargs(monkeypatch)["iam_type"] == "instance_principal"

    def test_an_unknown_auth_is_refused(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GINKGO_REMOTE_OCI_AUTH", "password")

        with pytest.raises(ValueError, match="Unknown OCI auth 'password'"):
            resolve_backend("oci")

    def test_filesystem_returns_the_configured_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import ocifs

        from ginkgo_oci import filesystem

        sentinel = object()
        monkeypatch.setattr(ocifs, "OCIFileSystem", lambda **kwargs: sentinel)

        assert filesystem() is sentinel
