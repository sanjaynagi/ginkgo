"""Tests for silent failures at flow/graph boundaries.

Covers two defects: a path crossing a task boundary as ``str`` contributes only
its path string to the downstream cache key (#121), and a task call unreachable
from the flow return value is dropped from the graph (#122).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import evaluate, file, flow, task, tmp_dir
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.diagnostics import UNREACHABLE_CALL_CODE, unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import TaskNotice
from ginkgo.runtime.task_validation import is_untracked_path_value
from tests.conftest import EventCollector


@task()
def write_rows_str(*, rows: int, output_path: str) -> str:
    """Write ``rows`` lines and return the path as a plain ``str``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(str(index) for index in range(rows)) + "\n", encoding="utf-8")
    return output_path


@task()
def summarise_str(*, coords: str, output_path: str) -> str:
    """Summarise a path received as a plain ``str``."""
    count = len(Path(coords).read_text(encoding="utf-8").strip().split("\n"))
    Path(output_path).write_text(f"rows,{count}\n", encoding="utf-8")
    return output_path


@task()
def write_rows_file(*, rows: int, output_path: str) -> file:
    """Write ``rows`` lines and return the path as a ``file``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(str(index) for index in range(rows)) + "\n", encoding="utf-8")
    return file(output_path)


@task()
def summarise_file(*, coords: file, output_path: str) -> str:
    """Summarise a path received as a ``file``."""
    count = len(Path(coords).read_text(encoding="utf-8").strip().split("\n"))
    Path(output_path).write_text(f"rows,{count}\n", encoding="utf-8")
    return output_path


@task()
def summarise_many_str(*, coords: list[str], output_path: str) -> str:
    """Summarise several paths received inside a ``list[str]``."""
    total = sum(len(Path(path).read_text(encoding="utf-8").strip().split("\n")) for path in coords)
    Path(output_path).write_text(f"rows,{total}\n", encoding="utf-8")
    return output_path


@task()
def summarise_many_file(*, coords: list[file], output_path: str) -> str:
    """Summarise several paths received inside a ``list[file]``."""
    total = sum(len(Path(path).read_text(encoding="utf-8").strip().split("\n")) for path in coords)
    Path(output_path).write_text(f"rows,{total}\n", encoding="utf-8")
    return output_path


@task()
def summarise_mapping_str(*, coords: dict[str, str], output_path: str) -> str:
    """Summarise paths received as the values of a ``dict[str, str]``."""
    total = sum(
        len(Path(path).read_text(encoding="utf-8").strip().split("\n")) for path in coords.values()
    )
    Path(output_path).write_text(f"rows,{total}\n", encoding="utf-8")
    return output_path


@task()
def produce_file_asset(*, output_path: str) -> object:
    """Return a file asset, which reaches a consumer as an ``AssetRef``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("0\n1\n", encoding="utf-8")
    return ginkgo.asset(target)


@task()
def receive_as_str(*, incoming: str, output_path: str) -> str:
    """Receive an upstream value through a plain ``str`` parameter."""
    Path(output_path).write_text(str(incoming), encoding="utf-8")
    return output_path


@task()
def make_label(*, text: str) -> str:
    return text.upper()


@task()
def join_labels(*, left: str, right: str) -> str:
    return f"{left}-{right}"


def _notices(collector: EventCollector) -> list[str]:
    return [event.message for event in collector.events if isinstance(event, TaskNotice)]


class TestUntrackedPathBoundary:
    """#121 — a ``str`` path boundary is cached on identity, not content."""

    def test_str_boundary_warns_and_names_both_ends(self, event_collector: EventCollector) -> None:
        coords = write_rows_str(rows=3, output_path="rows.csv")
        evaluate(
            summarise_str(coords=coords, output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1
        assert "write_rows_str" in messages[0]
        assert "coords" in messages[0]

    def test_file_boundary_is_silent(self, event_collector: EventCollector) -> None:
        coords = write_rows_file(rows=3, output_path="rows.csv")
        evaluate(
            summarise_file(coords=coords, output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_literal_path_argument_is_not_warned_about(
        self, event_collector: EventCollector
    ) -> None:
        Path("rows.csv").write_text("0\n1\n", encoding="utf-8")

        evaluate(
            summarise_str(coords="rows.csv", output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_str_boundary_serves_stale_downstream_output(self) -> None:
        """The defect the warning exists to flag: content change, cache hit."""
        evaluate(
            summarise_str(
                coords=write_rows_str(rows=3, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"

        evaluate(
            summarise_str(
                coords=write_rows_str(rows=5, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )
        assert len(Path("rows.csv").read_text(encoding="utf-8").strip().split("\n")) == 5
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"

    def test_file_boundary_invalidates_downstream(self) -> None:
        evaluate(
            summarise_file(
                coords=write_rows_file(rows=3, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )
        evaluate(
            summarise_file(
                coords=write_rows_file(rows=5, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )

        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,5\n"


class TestUntrackedPathsInsideContainers:
    """The fan-in shape: expressions nested inside a list argument."""

    def test_paths_inside_a_list_argument_are_checked(
        self, event_collector: EventCollector
    ) -> None:
        evaluate(
            summarise_many_str(
                coords=[
                    write_rows_str(rows=2, output_path="a.csv"),
                    write_rows_str(rows=3, output_path="b.csv"),
                ],
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1, messages
        assert "write_rows_str" in messages[0]
        assert "coords" in messages[0]

    def test_paths_inside_a_list_of_file_are_silent(self, event_collector: EventCollector) -> None:
        evaluate(
            summarise_many_file(
                coords=[
                    write_rows_file(rows=2, output_path="a.csv"),
                    write_rows_file(rows=3, output_path="b.csv"),
                ],
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_paths_from_a_fan_out_are_checked(self, event_collector: EventCollector) -> None:
        evaluate(
            summarise_many_str(
                coords=write_rows_str(rows=2).map(output_path=["a.csv", "b.csv"]),
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1, messages
        assert "write_rows_str" in messages[0]

    def test_paths_inside_a_dict_argument_are_checked(
        self, event_collector: EventCollector
    ) -> None:
        evaluate(
            summarise_mapping_str(
                coords={"first": write_rows_str(rows=2, output_path="a.csv")},
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1, messages
        assert "write_rows_str" in messages[0]


class TestAssetRefBoundary:
    """An ``AssetRef`` is version-keyed, so its boundary is already tracked."""

    def test_asset_ref_reaching_a_str_parameter_is_not_warned_about(
        self, event_collector: EventCollector
    ) -> None:
        evaluate(
            receive_as_str(
                incoming=produce_file_asset(output_path="rows.csv"),
                output_path="summary.txt",
            ),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_the_predicate_excludes_asset_refs(self) -> None:
        """Pinned directly: a file-kind ref under a ``str`` annotation.

        An ``AssetRef`` is not path-like, so it never reaches the existence
        probe. This pins the outcome rather than the mechanism, so it still
        holds if ``AssetRef`` ever becomes ``os.PathLike``.
        """
        ref = AssetRef(
            key=AssetKey(namespace="ns", name="rows"),
            version_id="v1",
            kind="file",
            artifact_id="artifact-1",
            content_hash="hash-1",
            artifact_path=Path("rows.csv"),
        )

        assert is_untracked_path_value(annotation=str, value=ref) is False


class TestIsUntrackedPathValue:
    """The predicate behind the warning, over the annotation table in #121."""

    @pytest.fixture(autouse=True)
    def existing_path(self) -> Path:
        target = Path("present.csv")
        target.write_text("x\n", encoding="utf-8")
        return target

    @pytest.mark.parametrize(
        ("annotation", "value", "expected"),
        [
            (file, "present.csv", False),
            (file | None, "present.csv", False),
            (list[file], "present.csv", False),
            (tmp_dir, "present.csv", False),
            (str, file("present.csv"), False),
            (str, "present.csv", True),
            (str | None, "present.csv", True),
            (Path, Path("present.csv"), True),
            (Any, "present.csv", True),
            (str, "absent.csv", False),
            (str, "not a path at all", False),
            (int, 3, False),
            (str, "s3://bucket/key.csv", False),
        ],
    )
    def test_predicate(self, annotation: Any, value: Any, expected: bool) -> None:
        assert is_untracked_path_value(annotation=annotation, value=value) is expected


def _validated_evaluator(build: Any) -> ConcurrentEvaluator:
    """Run a flow body under a construction recorder and validate the graph."""
    with record_constructed_calls() as constructed_calls:
        expr = build()
    evaluator = ConcurrentEvaluator(constructed_calls=tuple(constructed_calls))
    evaluator.build_and_validate(expr)
    return evaluator


class TestUnreachableCalls:
    """#122 — calls not reachable from the flow return value are dropped."""

    def test_bare_call_is_dropped_from_the_graph_and_reported(self) -> None:
        @flow
        def main():
            kept = make_label(text="kept")
            make_label(text="dropped")
            return kept

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 1
        assert [call.label for call in evaluator.unreachable_calls] == ["make_label()"]

    def test_returned_calls_are_all_reachable(self) -> None:
        @flow
        def main():
            return join_labels(
                left=make_label(text="a"),
                right=make_label(text="b"),
            )

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 3
        assert evaluator.unreachable_calls == []

    def test_calls_returned_inside_a_tuple_are_reachable(self) -> None:
        @flow
        def main():
            return make_label(text="a"), make_label(text="b")

        assert _validated_evaluator(main).unreachable_calls == []

    def test_dropped_producer_is_reported_when_a_literal_replaces_it(self) -> None:
        """Case 2 of #122: a literal path in place of the upstream expression."""
        Path("rows.csv").write_text("0\n", encoding="utf-8")

        @flow
        def main():
            write_rows_str(rows=3, output_path="rows.csv")
            return summarise_str(coords="rows.csv", output_path="summary.csv")

        evaluator = _validated_evaluator(main)

        assert [call.label for call in evaluator.unreachable_calls] == ["write_rows_str()"]

    def test_fan_out_branches_are_reported_as_one_call(self) -> None:
        @flow
        def main():
            make_label().map(text=["a", "b", "c"])
            return make_label(text="kept")

        evaluator = _validated_evaluator(main)

        assert [call.label for call in evaluator.unreachable_calls] == ["make_label() × 3"]

    def test_chained_map_does_not_report_superseded_branches(self) -> None:
        @flow
        def main():
            return join_labels().map(left=["a", "b"]).map(right=["x", "y"])

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 4
        assert evaluator.unreachable_calls == []

    def test_an_empty_fan_out_is_not_reported(self) -> None:
        """No branches were built, so no call was dropped."""

        @flow
        def main():
            make_label().map(text=[])
            return join_labels(left="a", right="b")

        evaluator = _validated_evaluator(main)

        assert evaluator.unreachable_calls == []

    def test_an_empty_fan_out_returned_by_the_flow_is_not_reported(self) -> None:
        @flow
        def main():
            return make_label().map(text=[])

        assert _validated_evaluator(main).unreachable_calls == []

    def test_no_recorder_means_no_reporting(self) -> None:
        """Expressions built outside a recorder never look unreachable."""
        evaluator = ConcurrentEvaluator()
        evaluator.build_and_validate(make_label(text="a"))

        assert evaluator.unreachable_calls == []

    def test_diagnostics_are_warnings_that_name_the_call(self) -> None:
        @flow
        def main():
            make_label(text="dropped")
            return join_labels(left="a", right="b")

        evaluator = _validated_evaluator(main)
        diagnostics = unreachable_call_diagnostics(calls=evaluator.unreachable_calls)

        assert len(diagnostics) == 1
        assert diagnostics[0].severity == "warning"
        assert diagnostics[0].code == UNREACHABLE_CALL_CODE
        assert "make_label()" in diagnostics[0].message
        assert diagnostics[0].location.endswith("make_label")

    def test_dry_run_plan_lists_dropped_calls(self) -> None:
        @flow
        def main():
            make_label(text="dropped")
            return make_label(text="kept")

        evaluator = _validated_evaluator(main)
        plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")

        assert plan.task_count == 1
        assert plan.dropped_labels == ("make_label()",)
