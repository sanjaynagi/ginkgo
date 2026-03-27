"""Unit tests for the remote artifact publisher."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from ginkgo.remote.backend import RemoteObjectMeta
from ginkgo.remote.publisher import RemotePublisher
from ginkgo.runtime.artifact_model import (
    ArtifactRecord,
    TreeEntry,
    TreeRef,
    serialize_tree_manifest,
)


def _make_record(
    *,
    kind: str = "blob",
    digest_hex: str = "abc123",
    artifact_id: str = "abc123",
    remote_uri: str | None = None,
) -> ArtifactRecord:
    return ArtifactRecord(
        artifact_id=artifact_id,
        kind=kind,
        digest_algorithm="blake3",
        digest_hex=digest_hex,
        extension=".csv",
        size=100,
        created_at="2026-01-01T00:00:00Z",
        storage_backend="local",
        remote_uri=remote_uri,
    )


def _make_publisher(tmp_path: Path) -> tuple[RemotePublisher, MagicMock]:
    blobs_dir = tmp_path / "blobs"
    trees_dir = tmp_path / "trees"
    refs_dir = tmp_path / "refs"
    for d in (blobs_dir, trees_dir, refs_dir):
        d.mkdir()

    backend = MagicMock()
    backend.upload.return_value = RemoteObjectMeta(uri="s3://bkt/key", size=100)

    publisher = RemotePublisher(
        backend=backend,
        bucket="test-bucket",
        prefix="artifacts/",
        local_blobs_dir=blobs_dir,
        local_trees_dir=trees_dir,
        local_refs_dir=refs_dir,
    )
    return publisher, backend


class TestPublishBlob:
    def test_uploads_blob_and_sets_remote_uri(self, tmp_path) -> None:
        publisher, backend = _make_publisher(tmp_path)
        blob_path = tmp_path / "blobs" / "abc123"
        blob_path.write_bytes(b"data")

        record = _make_record()
        result = publisher.publish(record=record)

        assert result.remote_uri == "s3://test-bucket/artifacts/blobs/abc123"
        backend.upload.assert_called_once_with(
            src_path=blob_path, bucket="test-bucket", key="artifacts/blobs/abc123"
        )

    def test_updates_ref_file(self, tmp_path) -> None:
        publisher, _ = _make_publisher(tmp_path)
        blob_path = tmp_path / "blobs" / "abc123"
        blob_path.write_bytes(b"data")

        # Create a ref file.
        ref_path = tmp_path / "refs" / "abc123.json"
        record = _make_record()
        ref_path.write_text(record.to_json(), encoding="utf-8")

        result = publisher.publish(record=record)

        # Ref file should now contain remote_uri.
        updated_ref = json.loads(ref_path.read_text(encoding="utf-8"))
        assert updated_ref["remote_uri"] == result.remote_uri

    def test_skips_already_published(self, tmp_path) -> None:
        publisher, backend = _make_publisher(tmp_path)
        record = _make_record(remote_uri="s3://already/published")

        result = publisher.publish(record=record)

        assert result is record
        backend.upload.assert_not_called()


class TestPublishTree:
    def test_uploads_blobs_and_manifest(self, tmp_path) -> None:
        publisher, backend = _make_publisher(tmp_path)

        # Create tree manifest and constituent blobs.
        tree_ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="tree_digest",
            entries=(
                TreeEntry(relative_path="a.txt", blob_digest="blob_a", size=5, mode=0o644),
                TreeEntry(relative_path="b.txt", blob_digest="blob_b", size=3, mode=0o644),
            ),
        )
        manifest_path = tmp_path / "trees" / "tree_digest.json"
        manifest_path.write_text(serialize_tree_manifest(tree_ref), encoding="utf-8")
        (tmp_path / "blobs" / "blob_a").write_bytes(b"aaaaa")
        (tmp_path / "blobs" / "blob_b").write_bytes(b"bbb")

        record = _make_record(kind="tree", digest_hex="tree_digest", artifact_id="tree_digest")
        result = publisher.publish(record=record)

        assert result.remote_uri == "s3://test-bucket/artifacts/trees/tree_digest.json"
        # Two blob uploads + one manifest upload.
        assert backend.upload.call_count == 3

    def test_updates_ref_file_for_tree(self, tmp_path) -> None:
        publisher, _ = _make_publisher(tmp_path)

        tree_ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="tree_digest",
            entries=(),
        )
        manifest_path = tmp_path / "trees" / "tree_digest.json"
        manifest_path.write_text(serialize_tree_manifest(tree_ref), encoding="utf-8")

        ref_path = tmp_path / "refs" / "tree_id.json"
        record = _make_record(kind="tree", digest_hex="tree_digest", artifact_id="tree_id")
        ref_path.write_text(record.to_json(), encoding="utf-8")

        result = publisher.publish(record=record)

        updated_ref = json.loads(ref_path.read_text(encoding="utf-8"))
        assert updated_ref["remote_uri"] == result.remote_uri

    def test_skips_missing_blobs(self, tmp_path) -> None:
        """Missing local blobs are silently skipped during tree publish."""
        publisher, backend = _make_publisher(tmp_path)

        tree_ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="tree_digest",
            entries=(
                TreeEntry(relative_path="exists.txt", blob_digest="blob_yes", size=3, mode=0o644),
                TreeEntry(relative_path="gone.txt", blob_digest="blob_no", size=3, mode=0o644),
            ),
        )
        manifest_path = tmp_path / "trees" / "tree_digest.json"
        manifest_path.write_text(serialize_tree_manifest(tree_ref), encoding="utf-8")
        (tmp_path / "blobs" / "blob_yes").write_bytes(b"yes")
        # blob_no intentionally missing.

        record = _make_record(kind="tree", digest_hex="tree_digest", artifact_id="tree_digest")
        publisher.publish(record=record)

        # One blob upload + one manifest upload (missing blob skipped).
        assert backend.upload.call_count == 2

    def test_missing_manifest_still_sets_uri(self, tmp_path) -> None:
        """If the tree manifest file is missing, remote_uri is still set."""
        publisher, backend = _make_publisher(tmp_path)

        record = _make_record(kind="tree", digest_hex="no_manifest", artifact_id="no_manifest")
        result = publisher.publish(record=record)

        assert result.remote_uri is not None
        backend.upload.assert_not_called()
