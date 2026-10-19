"""Unit tests for filesystem-backed remote storage backends."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ginkgo.core.remote import parse_bucket_key_uri
from ginkgo.plugins.api import ObjectStoreContext, ObjectStoreType, Plugin
from ginkgo.plugins.builtin import BUILTIN
from ginkgo.plugins.registry import PluginNotFoundError, PluginRegistry, use_plugin_registry
from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.fsspec_backends import FsspecBackend
from ginkgo.remote.resolve import resolve_backend


@pytest.fixture()
def mock_filesystem() -> MagicMock:
    return MagicMock()


class _MockedBackend(FsspecBackend):
    """An fsspec store over a mocked filesystem.

    The S3 and OCI fixtures differ only in scheme and in how their provider
    spells metadata keys, which is what the shared base has to handle.
    """

    def __init__(self, *, scheme: str, filesystem: MagicMock) -> None:
        super().__init__(scheme=scheme)
        self._filesystem = filesystem

    def _get_filesystem(self) -> MagicMock:
        return self._filesystem


@pytest.fixture()
def s3_backend(mock_filesystem: MagicMock) -> FsspecBackend:
    return _MockedBackend(scheme="s3", filesystem=mock_filesystem)


@pytest.fixture()
def oci_backend(mock_filesystem: MagicMock) -> FsspecBackend:
    return _MockedBackend(scheme="oci", filesystem=mock_filesystem)


class TestHead:
    def test_s3_returns_metadata(
        self, s3_backend: FsspecBackend, mock_filesystem: MagicMock
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
        self, oci_backend: FsspecBackend, mock_filesystem: MagicMock
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
        s3_backend: FsspecBackend,
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
        oci_backend: FsspecBackend,
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
        s3_backend: FsspecBackend,
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
        oci_backend: FsspecBackend,
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
    def test_the_scheme_selects_the_plugin_store(self) -> None:
        built: list[ObjectStoreContext] = []

        def build(context: ObjectStoreContext) -> FsspecBackend:
            built.append(context)
            return _MockedBackend(scheme="mem", filesystem=MagicMock())

        plugin = Plugin(
            name="mem",
            contributions=(
                ObjectStoreType(scheme="mem", parse_uri=parse_bucket_key_uri, build=build),
            ),
        )
        with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, plugin))):
            backend = resolve_backend("mem", region="eu-west-1")

        assert isinstance(backend, _MockedBackend)
        (context,) = built
        assert context.region == "eu-west-1"

    def test_unsupported_scheme_raises(self) -> None:
        with pytest.raises(PluginNotFoundError, match="No installed plugin provides object store"):
            resolve_backend("ftp")

    def test_a_first_party_scheme_names_its_plugin(self) -> None:
        with use_plugin_registry(PluginRegistry(plugins=(BUILTIN,))):
            with pytest.raises(PluginNotFoundError, match="Install ginkgo-oci to add it"):
                resolve_backend("oci")
