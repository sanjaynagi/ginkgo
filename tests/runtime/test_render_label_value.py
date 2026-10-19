"""Compact display labels for mapped-task arguments."""

from __future__ import annotations

from pathlib import Path

from ginkgo.runtime.display_labels import render_label_value


def test_a_path_is_labelled_by_its_file_name() -> None:
    assert render_label_value(Path("results/sample_a.tsv")) == "sample_a.tsv"
