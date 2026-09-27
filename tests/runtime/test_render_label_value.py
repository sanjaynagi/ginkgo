"""Compact display labels for mapped-task arguments."""

from __future__ import annotations

from pathlib import Path

from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.runtime.task_runners.notebook import render_label_value


def test_an_asset_input_is_labelled_by_its_logical_name() -> None:
    # An asset input reaches the label as its ref, since the payload is only
    # loaded on a cache miss; the ref's repr would be truncated noise.
    ref = AssetRef(
        key=AssetKey(namespace="model", name="rf_model"),
        version_id="v1",
        kind="model",
        artifact_id="abc123",
        content_hash="def456",
        artifact_path="/blobs/abc123",
    )

    assert render_label_value(ref) == "rf_model"


def test_a_path_is_labelled_by_its_file_name() -> None:
    assert render_label_value(Path("results/sample_a.tsv")) == "sample_a.tsv"
