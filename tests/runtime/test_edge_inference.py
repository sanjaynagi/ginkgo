"""Unit tests for the path index behind inferred ``Out[...]`` edges (#307)."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, file, task, untracked
from ginkgo.runtime.edge_inference import (
    ConsumedPath,
    DuplicateOutputPathError,
    OutputOverwritesInputError,
    PathIndex,
    ProducedPath,
    collect_consumed_paths,
    reject_self_overwrite,
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
    @pytest.mark.parametrize(
        ("produced", "consumed", "found"),
        [
            (("/w/results/a.txt", "file"), (2, "/w/results/a.txt", None), True),
            (("/w/qc", "folder"), (2, "/w/qc/report.html", None), True),
            (("/w/results/a.txt", "file"), (2, "/w/results", "folder"), True),
            # An untyped string naming the directory is not a folder read.
            (("/w/results/a.txt", "file"), (2, "/w/results", None), False),
            (("/w/a.txt", "file"), (1, "/w/a.txt", None), False),  # never itself
            (("/w/qc", "folder"), (2, "/w/qc2/x.txt", None), False),  # sibling prefix
        ],
    )
    def test_producers_for(self, produced, consumed, found: bool) -> None:
        index = PathIndex()
        index.add_produced([_produced(1, produced[0], kind=produced[1])])

        assert [e.node_id for e in index.producers_for(_consumed(*consumed))] == (
            [1] if found else []
        )

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


class TestRejectSelfOverwrite:
    """#334 — a node may not declare an ``Out[...]`` path over its own input."""

    @pytest.mark.parametrize(
        ("consumed", "produced"),
        [
            (("/w/in.txt", "file"), ("/w/in.txt", "file")),
            (("/w/qc/in.txt", "file"), ("/w/qc", "folder")),
            (("/w/data", "folder"), ("/w/data/out.txt", "file")),
            (("/w/data", "folder"), ("/w/data", "folder")),
        ],
    )
    def test_overlapping_input_and_output_are_refused(self, consumed, produced) -> None:
        with pytest.raises(OutputOverwritesInputError, match="t1"):
            reject_self_overwrite(
                produced=[_produced(1, produced[0], kind=produced[1])],
                consumed=[_consumed(1, consumed[0], kind=consumed[1])],
            )

    def test_message_names_both_parameters_and_the_path(self) -> None:
        with pytest.raises(OutputOverwritesInputError) as excinfo:
            reject_self_overwrite(
                produced=[_produced(1, "/w/in.txt")],
                consumed=[_consumed(1, "/w/in.txt", kind="file")],
            )

        message = str(excinfo.value)
        assert "'out'" in message
        assert "'src'" in message
        assert "/w/in.txt" in message

    def test_disjoint_paths_are_accepted(self) -> None:
        reject_self_overwrite(
            produced=[_produced(1, "/w/qc", kind="folder")],
            consumed=[_consumed(1, "/w/qc2/x.txt", kind="file")],
        )

    def test_an_untyped_string_is_not_an_input(self) -> None:
        """A plain ``str`` may be a label that happens to match, so it is not refused."""
        reject_self_overwrite(
            produced=[_produced(1, "/w/S1", kind="folder")],
            consumed=[_consumed(1, "/w/S1")],
        )


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
