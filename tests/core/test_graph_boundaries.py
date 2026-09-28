"""Tests for silent failures at flow/graph boundaries.

Covers two defects: a task call unreachable from the flow return value is
dropped from the graph (#122), and — since #307 phase 2 — what happens at a
``str``-typed path boundary between two tasks. A root/boundary path naming an
existing *file* is now content-hashed by default whatever its annotation
(closing #121/#281): editing it, or the upstream task producing a different
one, invalidates the consumer, and the #121 cross-task notice no longer fires
for it. A boundary path naming an existing *directory* is not auto-hashed
(hashing an entire tree as a side effect of a plain ``str`` would be a
surprise) and keeps the narrowed notice, now suggesting ``folder``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import evaluate, file, flow, folder, task, tmp_dir, untracked
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.diagnostics import UNREACHABLE_CALL_CODE, unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import TaskNotice
from ginkgo.runtime.task_validation import (
    is_content_trackable_path_value,
    is_untracked_directory_value,
    is_untracked_path_value,
)
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
def write_dir_str(*, tag: str, output_dir: str) -> str:
    """Write a file inside a directory and return the directory as a plain ``str``."""
    target = Path(output_dir)
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{tag}.txt").write_text(tag, encoding="utf-8")
    return output_dir


@task()
def summarise_dir_str(*, readings: str, output_path: untracked) -> str:
    """Summarise a directory received as a plain ``str``."""
    total = len(list(Path(readings).iterdir()))
    Path(output_path).write_text(f"entries,{total}\n", encoding="utf-8")
    return output_path


@task()
def write_rows_untracked(*, rows: int, output_path: str) -> str:
    """Write ``rows`` lines and return the path as a plain ``str``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(str(index) for index in range(rows)) + "\n", encoding="utf-8")
    return output_path


@task()
def summarise_untracked(*, coords: untracked, output_path: untracked) -> str:
    """Summarise a path deliberately annotated ``untracked``."""
    count = len(Path(coords).read_text(encoding="utf-8").strip().split("\n"))
    Path(output_path).write_text(f"rows,{count}\n", encoding="utf-8")
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


class TestFilePathBoundaryIsTracked:
    """#121/#281 closed — a ``str`` boundary naming an existing *file* is
    content-hashed by default now, so it no longer warns and no longer goes
    stale."""

    def test_str_boundary_is_silent(self, event_collector: EventCollector) -> None:
        coords = write_rows_str(rows=3, output_path="rows.csv")
        evaluate(
            summarise_str(coords=coords, output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

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

    def test_str_boundary_no_longer_serves_stale_downstream_output(self) -> None:
        """The #121 repro: the upstream task returns a path as ``str`` and the
        consumer receives it through a plain ``str`` parameter. Before #307
        phase 2 this stayed cached on the path string; now the content
        change re-runs the consumer."""
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
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,5\n"

    def test_str_boundary_invalidates_on_a_literal_root_input_edit(self) -> None:
        """#281's own repro: a literal root input, not routed through the
        graph at all — editing the file directly must still invalidate."""
        Path("rows.csv").write_text("0\n1\n2\n", encoding="utf-8")
        evaluate(summarise_str(coords="rows.csv", output_path="summary.csv"))
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"

        Path("rows.csv").write_text("0\n1\n2\n3\n", encoding="utf-8")
        evaluate(summarise_str(coords="rows.csv", output_path="summary.csv"))
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,4\n"

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


class TestUntrackedDirectoryBoundary:
    """The narrowed #121 case: a directory is never auto-hashed, so a ``str``
    boundary naming one still warns — now suggesting ``folder``."""

    def test_str_boundary_to_a_directory_warns_and_names_both_ends(
        self, event_collector: EventCollector
    ) -> None:
        readings = write_dir_str(tag="a", output_dir="readings")
        evaluate(
            summarise_dir_str(readings=readings, output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1, messages
        assert "write_dir_str" in messages[0]
        assert "readings" in messages[0]
        assert "folder" in messages[0]

    def test_str_boundary_to_a_directory_serves_stale_downstream_output(self) -> None:
        evaluate(
            summarise_dir_str(
                readings=write_dir_str(tag="a", output_dir="readings"),
                output_path="summary.csv",
            )
        )
        assert Path("summary.csv").read_text(encoding="utf-8") == "entries,1\n"

        evaluate(
            summarise_dir_str(
                readings=write_dir_str(tag="b", output_dir="readings"),
                output_path="summary.csv",
            )
        )
        # A second file was added to the same directory, but the consumer's
        # cache key is still the directory's path string only.
        assert Path("summary.csv").read_text(encoding="utf-8") == "entries,1\n"


class TestExplicitlyUntrackedBoundary:
    """``untracked`` opts a path out of content tracking deliberately, and
    never warns — the #121 notice exists to flag a *silent* trap, not a
    declared one."""

    def test_untracked_boundary_is_silent(self, event_collector: EventCollector) -> None:
        coords = write_rows_untracked(rows=3, output_path="rows.csv")
        evaluate(
            summarise_untracked(coords=coords, output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_untracked_boundary_does_not_invalidate_on_content_change(self) -> None:
        evaluate(
            summarise_untracked(
                coords=write_rows_untracked(rows=3, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"

        evaluate(
            summarise_untracked(
                coords=write_rows_untracked(rows=5, output_path="rows.csv"),
                output_path="summary.csv",
            )
        )
        assert len(Path("rows.csv").read_text(encoding="utf-8").strip().split("\n")) == 5
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"


class TestUntrackedPathsInsideContainers:
    """The fan-in shape: expressions nested inside a list argument."""

    def test_paths_inside_a_list_argument_are_silent(
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

        assert _notices(event_collector) == []

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

    def test_paths_from_a_fan_out_are_silent(self, event_collector: EventCollector) -> None:
        evaluate(
            summarise_many_str(
                coords=write_rows_str(rows=2).map(output_path=["a.csv", "b.csv"]),
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []

    def test_paths_inside_a_dict_argument_are_silent(
        self, event_collector: EventCollector
    ) -> None:
        evaluate(
            summarise_mapping_str(
                coords={"first": write_rows_str(rows=2, output_path="a.csv")},
                output_path="summary.csv",
            ),
            event_bus=event_collector.bus,
        )

        assert _notices(event_collector) == []


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
    """The predicate behind the warning and the ``"path"`` label, over the
    annotation table in #121/#307. Since #307 phase 2, a plain ``str``
    naming an existing *file* that reads as a path (a separator or an
    extension) is content-trackable, not untracked — see
    ``TestIsContentTrackablePathValue`` below for that half of the table.
    What remains untracked-by-content here is a directory, and a bare word
    with neither a separator nor an extension.
    """

    @pytest.fixture(autouse=True)
    def existing_paths(self) -> Path:
        Path("present.csv").write_text("x\n", encoding="utf-8")
        Path("present").write_text("x\n", encoding="utf-8")
        Path("present_dir").mkdir()
        return Path("present.csv")

    @pytest.mark.parametrize(
        ("annotation", "value", "expected"),
        [
            (file, "present.csv", False),
            (file | None, "present.csv", False),
            (list[file], "present.csv", False),
            (tmp_dir, "present.csv", False),
            (untracked, "present.csv", False),
            (str, file("present.csv"), False),
            (str, untracked("present.csv"), False),
            # A separator/extension existing file is content-trackable now,
            # not "untracked" — the #121/#281 fix.
            (str, "present.csv", False),
            (str | None, "present.csv", False),
            # A bare word does not read as a path, so it stays untracked.
            (str, "present", True),
            # A directory is never auto-hashed, so it stays untracked too.
            (str, "present_dir", True),
            (str | None, "present_dir", True),
            (Path, Path("present_dir"), True),
            (Any, "present_dir", True),
            (str, "absent.csv", False),
            (str, "not a path at all", False),
            (int, 3, False),
            (str, "s3://bucket/key.csv", False),
            # A rehydrated ``text`` asset arrives as its contents.
            (str, "# Title\n\n" + "x" * 400, False),
            (str, "has a \x00 in it", False),
        ],
    )
    def test_predicate(self, annotation: Any, value: Any, expected: bool) -> None:
        assert is_untracked_path_value(annotation=annotation, value=value) is expected


class TestIsContentTrackablePathValue:
    """The #307 phase 2 eligibility rule: separator-or-extension, existing,
    a regular file."""

    @pytest.fixture(autouse=True)
    def existing_paths(self) -> None:
        Path("present.csv").write_text("x\n", encoding="utf-8")
        Path("present").write_text("x\n", encoding="utf-8")
        Path("present_dir").mkdir()

    @pytest.mark.parametrize(
        ("annotation", "value", "expected"),
        [
            (str, "present.csv", True),
            (str | None, "present.csv", True),
            (Any, "present.csv", True),
            (str, "present", False),
            (str, "present_dir", False),
            (str, "absent.csv", False),
            (file, "present.csv", False),
            (tmp_dir, "present.csv", False),
            (untracked, "present.csv", False),
            (str, untracked("present.csv"), False),
            (str, "s3://bucket/key.csv", False),
        ],
    )
    def test_predicate(self, annotation: Any, value: Any, expected: bool) -> None:
        assert is_content_trackable_path_value(annotation=annotation, value=value) is expected


class TestIsUntrackedDirectoryValue:
    """The predicate behind the narrowed evaluator notice."""

    @pytest.fixture(autouse=True)
    def existing_paths(self) -> None:
        Path("present.csv").write_text("x\n", encoding="utf-8")
        Path("present_dir").mkdir()

    def test_directory_is_flagged(self) -> None:
        assert is_untracked_directory_value(annotation=str, value="present_dir") is True

    def test_file_is_no_longer_flagged(self) -> None:
        assert is_untracked_directory_value(annotation=str, value="present.csv") is False

    def test_folder_annotation_is_not_flagged(self) -> None:
        assert is_untracked_directory_value(annotation=folder, value="present_dir") is False


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
