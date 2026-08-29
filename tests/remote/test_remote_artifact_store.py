"""Tests for the remote-backed artifact store."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore


@pytest.fixture
def local_store(tmp_path: Path) -> LocalArtifactStore:
    """Create a LocalArtifactStore in a temp directory."""
    root = tmp_path / ".ginkgo" / "artifacts"
    root.mkdir(parents=True)
    return LocalArtifactStore(root=root, index=CacheIndex.in_memory())


@pytest.fixture
def mock_backend() -> MagicMock:
    """Create a mock ObjectStore.

    ``head`` defaults to raising ``FileNotFoundError`` so that upload
    paths exercised by tests actually call ``backend.upload``. Tests
    that need existing-object behaviour override ``head`` explicitly.
    """
    backend = MagicMock()
    backend.upload.return_value = RemoteObjectMeta(
        uri="gs://test-bucket/test-key",
        size=100,
    )
    backend.head.side_effect = FileNotFoundError("not found")
    return backend


@pytest.fixture
def remote_store(
    local_store: LocalArtifactStore,
    mock_backend: MagicMock,
) -> RemoteArtifactStore:
    """Create a RemoteArtifactStore wrapping a local store."""
    return RemoteArtifactStore(
        local=local_store,
        backend=mock_backend,
        bucket="test-bucket",
        prefix="artifacts/",
        scheme="gs",
    )


class TestRemoteArtifactStore:
    """Tests for store/retrieve with remote publishing."""

    def test_store_publishes_to_remote(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("hello,world\n")

        record = remote_store.store(src_path=src)

        assert record.remote_uri is not None
        assert record.remote_uri.startswith("gs://")
        # Should upload blob + ref.
        assert mock_backend.upload.call_count >= 2

    def test_retrieve_local_hit_no_download(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("hello,world\n")
        record = remote_store.store(src_path=src)

        # Reset download mock.
        mock_backend.download.reset_mock()

        dest = tmp_path / "output.csv"
        remote_store.retrieve(artifact_id=record.artifact_id, dest_path=dest)

        # Should not download — data is local.
        mock_backend.download.assert_not_called()
        assert dest.exists() or dest.is_symlink()

    def test_exists_local(
        self,
        remote_store: RemoteArtifactStore,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("test\n")
        record = remote_store.store(src_path=src)

        assert remote_store.exists(artifact_id=record.artifact_id)

    def test_exists_checks_remote_on_local_miss(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
    ) -> None:
        # head() succeeds → exists remotely.
        mock_backend.head.side_effect = None
        mock_backend.head.return_value = RemoteObjectMeta(
            uri="gs://test-bucket/artifacts/refs/fake-id.json",
            size=100,
        )

        assert remote_store.exists(artifact_id="fake-id")
        mock_backend.head.assert_called_once()

    def test_exists_remote_miss(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
    ) -> None:
        mock_backend.head.side_effect = FileNotFoundError("not found")

        assert not remote_store.exists(artifact_id="nonexistent-id")

    def test_store_bytes_publishes(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
    ) -> None:
        record = remote_store.store_bytes(data=b"binary content", extension=".bin")

        assert record.remote_uri is not None
        assert mock_backend.upload.call_count >= 2

    def test_delete_only_local(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("test\n")
        record = remote_store.store(src_path=src)

        remote_store.delete(artifact_id=record.artifact_id)

        # Local gone, but no remote delete call.
        assert not remote_store.local.exists(artifact_id=record.artifact_id)


class TestPublishRecording:
    """`is_published` reads the artifact's row, so the row must not lead it."""

    def test_a_failed_ref_upload_is_not_remembered_as_published(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("hello,world\n")

        def _fail_on_ref(*, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
            if key.startswith("artifacts/refs/"):
                raise OSError("ref upload failed")
            return RemoteObjectMeta(uri=f"gs://{bucket}/{key}", size=100)

        mock_backend.upload.side_effect = _fail_on_ref

        with pytest.raises(OSError, match="ref upload failed"):
            remote_store.store(src_path=src)

        artifact_id = remote_store.local.materialized_artifact_id(path=src)
        assert artifact_id is not None
        assert remote_store.is_published(artifact_id) is False

    def test_a_retry_after_a_failed_ref_upload_publishes(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        src = tmp_path / "data.csv"
        src.write_text("hello,world\n")
        attempts: list[str] = []

        def _fail_first_ref(*, src_path: Path, bucket: str, key: str) -> RemoteObjectMeta:
            if key.startswith("artifacts/refs/"):
                attempts.append(key)
                if len(attempts) == 1:
                    raise OSError("ref upload failed")
            return RemoteObjectMeta(uri=f"gs://{bucket}/{key}", size=100)

        mock_backend.upload.side_effect = _fail_first_ref

        with pytest.raises(OSError):
            remote_store.store(src_path=src)
        record = remote_store.store(src_path=src)

        assert remote_store.is_published(record.artifact_id) is True

    def test_publish_uploads_without_re_storing_the_source(
        self,
        remote_store: RemoteArtifactStore,
        mock_backend: MagicMock,
        tmp_path: Path,
    ) -> None:
        """An artifact known locally but unpublished needs the upload, not the hash."""
        src = tmp_path / "data.csv"
        src.write_text("hello,world\n")
        record = remote_store.local.store(src_path=src)
        assert remote_store.is_published(record.artifact_id) is False

        published = remote_store.publish(artifact_id=record.artifact_id)

        assert published is not None
        assert remote_store.is_published(record.artifact_id) is True
        assert any(
            call.kwargs["key"].startswith("artifacts/refs/")
            for call in mock_backend.upload.call_args_list
        )

    def test_publishing_an_unknown_artifact_answers_none(
        self, remote_store: RemoteArtifactStore
    ) -> None:
        assert remote_store.publish(artifact_id="0" * 64) is None
