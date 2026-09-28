"""Unit tests for the path index behind inferred ``Out[...]`` edges (#307)."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, file, task, untracked
from ginkgo.runtime.edge_inference import (
    ConsumedPath,
    DuplicateOutputPathError,
    PathIndex,
    ProducedPath,
    collect_consumed_paths,
)


def _produced(node_id: int, path: str, kind: str = "file") -> ProducedPath:
    return ProducedPath(
        node_id=node_id, task_name=f"t{node_id}", param="out", path=path, kind=kind
    )


def _consumed(node_id: int, path: str, kind: str | None = None) -> ConsumedPath:
    return ConsumedPath(
        node_id=node_id, task_name=f"t{node_id}", param="src", path=path, kind=kind
    )


class TestPathIndex:
    def test_exact_match(self) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, "/w/results/a.txt")])

        found = index.producers_for(_consumed(2, "/w/results/a.txt"))

        assert [entry.node_id for entry in found] == [1]

    def test_consumer_inside_a_produced_folder(self) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, "/w/qc", kind="folder")])

        found = index.producers_for(_consumed(2, "/w/qc/report.html"))

        assert [entry.node_id for entry in found] == [1]

    def test_folder_consumer_containing_a_produced_file(self) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, "/w/results/a.txt")])

        assert index.producers_for(_consumed(2, "/w/results", kind="folder"))
        # An untyped string naming the directory is not a folder read.
        assert not index.producers_for(_consumed(2, "/w/results"))

    def test_a_node_never_matches_itself(self) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, "/w/a.txt")])

        assert index.producers_for(_consumed(1, "/w/a.txt")) == []

    def test_sibling_prefix_is_not_containment(self) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, "/w/qc", kind="folder")])

        assert index.producers_for(_consumed(2, "/w/qc2/x.txt")) == []

    def test_consumers_for_finds_earlier_consumers(self) -> None:
        index = PathIndex()
        index.add_consumed([_consumed(2, "/w/qc/report.html"), _consumed(3, "/w", kind="folder")])

        found = index.consumers_for(_produced(1, "/w/qc", kind="folder"))

        assert sorted(entry.node_id for entry in found) == [2, 3]

    @pytest.mark.parametrize(
        ("first", "second"),
        [
            (("/w/a.txt", "file"), ("/w/a.txt", "file")),
            (("/w/qc", "folder"), ("/w/qc/x.txt", "file")),
            (("/w/qc/x.txt", "file"), ("/w/qc", "folder")),
        ],
    )
    def test_overlapping_outputs_of_two_nodes_are_refused(self, first, second) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, first[0], kind=first[1])])

        with pytest.raises(DuplicateOutputPathError):
            index.add_produced([_produced(2, second[0], kind=second[1])])

    def test_one_node_may_declare_nested_outputs(self) -> None:
        index = PathIndex()
        index.add_produced(
            [_produced(1, "/w/qc", kind="folder"), _produced(1, "/w/qc/summary.txt")]
        )

    def test_a_wide_fan_out_indexes_quickly(self) -> None:
        index = PathIndex()
        produced = [_produced(i, f"/w/results/a/{i}.txt") for i in range(20_000)]
        index.add_produced(produced)

        edges = [
            index.producers_for(_consumed(20_000 + i, f"/w/results/a/{i}.txt"))
            for i in range(20_000)
        ]

        assert all(len(found) == 1 for found in edges)


@task()
def _logs_to(*, log_path: untracked, logs: list[untracked], dest: Out[file]) -> None:
    Path(dest).write_text("x", encoding="utf-8")


def test_an_untracked_parameter_is_not_a_consumer() -> None:
    consumed = collect_consumed_paths(
        node_id=1,
        task_name="logs_to",
        task_def=_logs_to,
        args={"log_path": "/w/run.log", "logs": ["/w/a.log"], "dest": "/w/out.txt"},
    )

    assert consumed == []
