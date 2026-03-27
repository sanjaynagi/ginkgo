"""Unit tests for artifact model types."""

from __future__ import annotations

import json

from ginkgo.runtime.artifact_model import (
    ArtifactRecord,
    BlobRef,
    TreeEntry,
    TreeRef,
    deserialize_tree_manifest,
    serialize_tree_manifest,
)


class TestBlobRef:
    def test_immutable(self) -> None:
        ref = BlobRef(
            digest_algorithm="blake3",
            digest_hex="abc123",
            size=1024,
            extension=".csv",
        )
        assert ref.digest_hex == "abc123"
        assert ref.extension == ".csv"

    def test_no_extension(self) -> None:
        ref = BlobRef(
            digest_algorithm="blake3",
            digest_hex="abc123",
            size=0,
            extension="",
        )
        assert ref.extension == ""


class TestTreeEntry:
    def test_fields(self) -> None:
        entry = TreeEntry(
            relative_path="data/sample.csv",
            blob_digest="def456",
            size=512,
            mode=0o644,
        )
        assert entry.relative_path == "data/sample.csv"
        assert entry.mode == 0o644


class TestTreeRef:
    def test_entries_are_tuple(self) -> None:
        ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="tree_digest",
            entries=(
                TreeEntry(
                    relative_path="a.txt",
                    blob_digest="aaa",
                    size=10,
                    mode=0o644,
                ),
            ),
        )
        assert isinstance(ref.entries, tuple)
        assert len(ref.entries) == 1


class TestArtifactRecord:
    def _make_record(self) -> ArtifactRecord:
        return ArtifactRecord(
            artifact_id="abc123",
            kind="blob",
            digest_algorithm="blake3",
            digest_hex="abc123",
            extension=".csv",
            size=1024,
            created_at="2026-01-01T00:00:00Z",
            storage_backend="local",
        )

    def test_json_round_trip(self) -> None:
        record = self._make_record()
        serialized = record.to_json()
        restored = ArtifactRecord.from_json(serialized)
        assert restored == record

    def test_json_is_valid(self) -> None:
        record = self._make_record()
        parsed = json.loads(record.to_json())
        assert parsed["kind"] == "blob"
        assert parsed["artifact_id"] == "abc123"

    def test_from_path(self, tmp_path: object) -> None:
        from pathlib import Path

        record = self._make_record()
        path = Path(str(tmp_path)) / "ref.json"
        path.write_text(record.to_json(), encoding="utf-8")
        restored = ArtifactRecord.from_path(path)
        assert restored == record


class TestTreeManifestSerialization:
    def test_round_trip(self) -> None:
        ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="tree_abc",
            entries=(
                TreeEntry(
                    relative_path="a.txt",
                    blob_digest="aaa",
                    size=10,
                    mode=0o644,
                ),
                TreeEntry(
                    relative_path="sub/b.txt",
                    blob_digest="bbb",
                    size=20,
                    mode=0o644,
                ),
            ),
        )
        serialized = serialize_tree_manifest(ref)
        restored = deserialize_tree_manifest(serialized)
        assert restored == ref

    def test_empty_tree(self) -> None:
        ref = TreeRef(
            digest_algorithm="blake3",
            digest_hex="empty",
            entries=(),
        )
        serialized = serialize_tree_manifest(ref)
        restored = deserialize_tree_manifest(serialized)
        assert restored == ref
        assert len(restored.entries) == 0
