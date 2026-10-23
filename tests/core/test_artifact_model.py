"""Serialisation round trips for the artifact model types."""

from __future__ import annotations

import json

import pytest

from ginkgo.runtime.artifacts.artifact_model import (
    ArtifactRecord,
    TreeEntry,
    TreeRef,
    deserialize_tree_manifest,
    serialize_tree_manifest,
)


def test_artifact_record_json_round_trip() -> None:
    record = ArtifactRecord(
        artifact_id="abc123",
        kind="blob",
        digest_algorithm="blake3",
        digest_hex="abc123",
        extension=".csv",
        size=1024,
        created_at="2026-01-01T00:00:00Z",
        storage_backend="local",
    )

    assert ArtifactRecord.from_json(record.to_json()) == record
    assert json.loads(record.to_json())["kind"] == "blob"


@pytest.mark.parametrize(
    "entries",
    [
        (),
        (
            TreeEntry(relative_path="a.txt", blob_digest="aaa", size=10, mode=0o644),
            TreeEntry(relative_path="sub/b.txt", blob_digest="bbb", size=20, mode=0o755),
        ),
    ],
    ids=["empty", "nested"],
)
def test_tree_manifest_round_trip(entries) -> None:
    ref = TreeRef(digest_algorithm="blake3", digest_hex="tree", entries=entries)

    assert deserialize_tree_manifest(serialize_tree_manifest(ref)) == ref
