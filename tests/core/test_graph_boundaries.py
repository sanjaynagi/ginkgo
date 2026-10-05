"""Tests for silent failures at flow/graph boundaries.

Covers three defects: a task call unreachable from the flow return value is
dropped from the graph (#122); what happens at a ``str``-typed path boundary
between two tasks — since #307 phase 2 a path naming an existing *file* is
content-hashed whatever its annotation (closing #121/#281), while one naming a
*directory* keeps the narrowed notice suggesting ``folder``; and a literal path
shared between two tasks, which now gets an inferred dependency edge when it
matches an ``Out[...]`` path (closing #280 — see ``TestOutPathEdgeInference``).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import Out, evaluate, file, flow, folder, task, tmp_dir, untracked
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.diagnostics import UNREACHABLE_CALL_CODE, unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.edge_inference import DuplicateOutputPathError, OutputOverwritesInputError
from ginkgo.runtime.evaluator import ConcurrentEvaluator, CycleError, IncompleteCallError
from ginkgo.runtime.events import GraphNodeRegistered, TaskNotice
from ginkgo.runtime.task_validation import (
    is_content_trackable_path_value,
    is_untracked_directory_value,
    is_untracked_path_value,
)
from tests.conftest import EventCollector


@task()
def write_rows_str(*, rows: int, output_path: Out[file]) -> str:
    """Write ``rows`` lines and return the path as a plain ``str``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(str(index) for index in range(rows)) + "\n", encoding="utf-8")
    return output_path


@task()
def summarise_str(*, coords: str, output_path: Out[file]) -> str:
    """Summarise a path received as a plain ``str``."""
    count = len(Path(coords).read_text(encoding="utf-8").strip().split("\n"))
    Path(output_path).write_text(f"rows,{count}\n", encoding="utf-8")
    return output_path


@task()
def write_rows_file(*, rows: int, output_path: Out[file]) -> file:
    """Write ``rows`` lines and return the path as a ``file``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(str(index) for index in range(rows)) + "\n", encoding="utf-8")
    return file(output_path)


@task()
def summarise_file(*, coords: file, output_path: Out[file]) -> str:
    """Summarise a path received as a ``file``."""
    count = len(Path(coords).read_text(encoding="utf-8").strip().split("\n"))
    Path(output_path).write_text(f"rows,{count}\n", encoding="utf-8")
    return output_path


@task()
def summarise_many_str(*, coords: list[str], output_path: Out[file]) -> str:
    """Summarise several paths received inside a ``list[str]``."""
    total = sum(len(Path(path).read_text(encoding="utf-8").strip().split("\n")) for path in coords)
    Path(output_path).write_text(f"rows,{total}\n", encoding="utf-8")
    return output_path


@task()
def summarise_many_file(*, coords: list[file], output_path: Out[file]) -> str:
    """Summarise several paths received inside a ``list[file]``."""
    total = sum(len(Path(path).read_text(encoding="utf-8").strip().split("\n")) for path in coords)
    Path(output_path).write_text(f"rows,{total}\n", encoding="utf-8")
    return output_path


@task()
def summarise_mapping_str(*, coords: dict[str, str], output_path: Out[file]) -> str:
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
def write_rows_untracked(*, rows: int, output_path: Out[file]) -> str:
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
def produce_file_asset(*, output_path: Out[file]) -> object:
    """Return a file asset, which reaches a consumer as an ``AssetRef``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("0\n1\n", encoding="utf-8")
    return ginkgo.asset(target)


@task()
def receive_as_str(*, incoming: str, output_path: Out[file]) -> str:
    """Receive an upstream value through a plain ``str`` parameter."""
    Path(output_path).write_text(str(incoming), encoding="utf-8")
    return output_path


@task()
def make_label(*, text: str) -> str:
    return text.upper()


@task()
def join_labels(*, left: str, right: str) -> str:
    return f"{left}-{right}"


@task()
def append_to_log(*, n: int, log_path: str) -> int:
    """A side-channel log passed as a plain ``str``: written, never returned."""
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(f"ran {n}\n")
    return n * 2


@task()
def append_to_untracked_log(*, n: int, log_path: untracked) -> int:
    """The same side-channel log, declared ``untracked``."""
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(f"ran {n}\n")
    return n * 2


@task()
def pass_through(*, path: str) -> file:
    """Returns the input it only read, as a validator or normaliser might."""
    with open("pass_through_runs.txt", "a", encoding="utf-8") as handle:
        handle.write("ran\n")
    return file(path)


def _notices(collector: EventCollector) -> list[str]:
    return [event.message for event in collector.events if isinstance(event, TaskNotice)]


class TestWrittenStrInputsAreFlagged:
    """A file a task writes through a plain ``str`` path is flagged.

    Plain ``str`` path inputs are content-hashed (#281), so a task that writes
    one invalidates its own cache entry and re-runs every time. That is
    reported with a notice naming the fix: ``Out[file]`` or ``untracked``.
    """

    def test_a_task_appending_to_a_str_log_is_flagged_and_reruns(
        self, event_collector: EventCollector
    ) -> None:
        for _ in range(2):
            evaluate(append_to_log(n=3, log_path="run.log"), event_bus=event_collector.bus)

        assert Path("run.log").read_text(encoding="utf-8") == "ran 3\nran 3\n"
        notices = _notices(event_collector)
        assert notices and all("Out[file]" in notice for notice in notices)
        assert "run.log" in notices[0]

    def test_an_untracked_log_is_not_flagged_and_caches(
        self, event_collector: EventCollector
    ) -> None:
        for _ in range(3):
            evaluate(
                append_to_untracked_log(n=3, log_path="run.log"), event_bus=event_collector.bus
            )

        assert Path("run.log").read_text(encoding="utf-8") == "ran 3\n"
        assert _notices(event_collector) == []

    def test_a_returned_input_the_task_only_read_stays_content_tracked(self) -> None:
        """Returning an input path does not make it look like an output."""
        Path("in.txt").write_text("original\n", encoding="utf-8")
        evaluate(pass_through(path="in.txt"))
        evaluate(pass_through(path="in.txt"))
        Path("in.txt").write_text("changed\n", encoding="utf-8")
        evaluate(pass_through(path="in.txt"))

        assert Path("pass_through_runs.txt").read_text(encoding="utf-8") == "ran\nran\n"

    def test_a_file_the_task_only_reads_is_not_flagged(
        self, event_collector: EventCollector
    ) -> None:
        Path("rows.csv").write_text("0\n1\n", encoding="utf-8")
        evaluate(
            summarise_str(coords="rows.csv", output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert not any("rows.csv" in notice for notice in _notices(event_collector))


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


class TestPartialCalls:
    """#332 — a call missing a required argument is a ``PartialCall``, not a task."""

    def test_returned_partial_call_is_a_build_error(self) -> None:
        @flow
        def main():
            return join_labels(left="a")

        with pytest.raises(IncompleteCallError, match=r"join_labels\(\) .*right"):
            _validated_evaluator(main)

    def test_returned_partial_call_fails_evaluate(self) -> None:
        with pytest.raises(IncompleteCallError, match=r"join_labels\(\) .*right"):
            evaluate(join_labels(left="a"))

    def test_partial_call_inside_a_returned_container_is_a_build_error(self) -> None:
        @flow
        def main():
            return [make_label(text="a"), make_label()]

        with pytest.raises(IncompleteCallError, match=r"make_label\(\) .*text"):
            _validated_evaluator(main)

    def test_consumed_partial_call_is_a_build_error_naming_the_consumer(self) -> None:
        @flow
        def main():
            return join_labels(left=make_label(), right="b")

        with pytest.raises(IncompleteCallError) as excinfo:
            _validated_evaluator(main)

        message = str(excinfo.value)
        assert "make_label()" in message
        assert "text" in message
        assert "join_labels()" in message

    def test_discarded_partial_call_is_reported_as_dropped(self) -> None:
        @flow
        def main():
            make_label()
            return make_label(text="kept")

        evaluator = _validated_evaluator(main)
        diagnostics = unreachable_call_diagnostics(calls=evaluator.unreachable_calls)

        assert len(evaluator.task_nodes) == 1
        assert [call.label for call in evaluator.unreachable_calls] == ["make_label()"]
        assert len(diagnostics) == 1
        assert diagnostics[0].severity == "warning"
        assert diagnostics[0].code == UNREACHABLE_CALL_CODE
        assert "missing required argument" in diagnostics[0].message
        assert "text" in diagnostics[0].message

    def test_map_completes_a_partial_call(self) -> None:
        @flow
        def main():
            return join_labels(left="a").map(right=["x", "y"])

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 2
        assert evaluator.unreachable_calls == []

    def test_product_map_completes_a_partial_call(self) -> None:
        @flow
        def main():
            return join_labels().product_map(left=["a", "b"], right=["x", "y"])

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 4
        assert evaluator.unreachable_calls == []

    def test_partial_call_mapped_twice_is_not_reported(self) -> None:
        @flow
        def main():
            partial = join_labels(left="a")
            return partial.map(right=["x"]), partial.map(right=["y"])

        evaluator = _validated_evaluator(main)

        assert len(evaluator.task_nodes) == 2
        assert evaluator.unreachable_calls == []


# --------------------------------------------------------------------------
# #280 / #307 phase 2 — dependency edges inferred from Out[...] paths
# --------------------------------------------------------------------------


@task()
def aggregate(*, rows: int, output_path: Out[file]) -> None:
    """Write ``output_path`` slowly, line by line, flushing after each."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for index in range(rows):
            handle.write(f"{index}\n")
            handle.flush()
            time.sleep(0.05)


@task()
def relay(*, src: str, dest: Out[file]) -> None:
    """Copy ``src`` to ``dest`` — one link in a chain joined only by paths."""
    Path(dest).write_text(Path(src).read_text(encoding="utf-8"), encoding="utf-8")


@task()
def report(*, csv_path: str) -> int:
    """Read ``csv_path`` and return its line count.

    Raises if the path does not exist yet — the tell for #280: without the
    inferred edge this task can run before ``aggregate`` has written
    anything.
    """
    return len(Path(csv_path).read_text(encoding="utf-8").strip().split("\n"))


@task()
def make_qc_dir(*, qc_dir: Out[folder]) -> None:
    Path(qc_dir).mkdir(parents=True, exist_ok=True)
    (Path(qc_dir) / "report.txt").write_text("ok\n", encoding="utf-8")


@task()
def read_file_in_dir(*, report_path: str) -> str:
    return Path(report_path).read_text(encoding="utf-8")


@task()
def write_many_into_dir(*, out_dir: Out[folder]) -> None:
    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    (target / "a.txt").write_text("a\n", encoding="utf-8")


@task()
def read_whole_dir(*, input_dir: folder) -> list[str]:
    return sorted(p.name for p in Path(input_dir).iterdir())


@task()
def touch_output(*, output_path: Out[file]) -> None:
    Path(output_path).write_text("x\n", encoding="utf-8")


@task()
def read_own_output(*, output_path: Out[file], also_read: str) -> str:
    Path(output_path).write_text("x\n", encoding="utf-8")
    return also_read


@task()
def overwrite_input(*, src: file, out: Out[file]) -> None:
    Path(out).write_text("FILTERED\n", encoding="utf-8")


@task()
def write_into_input_dir(*, input_dir: folder, out: Out[file]) -> None:
    Path(out).write_text(str(len(list(Path(input_dir).iterdir()))), encoding="utf-8")


@task()
def side_use(*, value: object) -> object:
    """Consume ``value`` for no other reason than to keep it reachable."""
    return value


@task()
def build_and_link(*, rows: int) -> object:
    """Dynamically register a producer/consumer pair sharing a literal path."""
    agg = aggregate(rows=rows, output_path="dyn.csv")
    rep = report(csv_path="dyn.csv")
    return agg, rep


@task()
def write_one(*, n: int, out: Out[file]) -> int:
    Path(out).write_text(f"{n}\n", encoding="utf-8")
    return n


@task()
def spawn_into_folder(*, d: folder) -> object:
    """Receive a folder and return children writing inside it."""
    return write_one(n=1, out=str(Path(d) / "a.txt")), write_one(n=2, out=str(Path(d) / "b.txt"))


@task()
def slow_gate(*, seconds: float) -> int:
    time.sleep(seconds)
    return 0


@task()
def report_after(*, csv_path: str, gate: object) -> int:
    return len(Path(csv_path).read_text(encoding="utf-8").strip().split("\n"))


@task()
def spawn_producer(*, path: str, gate: object) -> object:
    """Dynamically register an ``Out`` producer for a literal path."""
    return aggregate(rows=3, output_path=path)


def _node_for(evaluator: ConcurrentEvaluator, *, param: str, value: Any) -> Any:
    matches = [
        node for node in evaluator.task_nodes.values() if node.expr.args.get(param) == value
    ]
    assert len(matches) == 1, f"expected exactly one node with {param}={value!r}: {matches}"
    return matches[0]


class TestOutPathEdgeInference:
    """#280/#307 phase 2 — a literal path matching ``Out[...]`` gets an edge."""

    def test_literal_path_matching_out_creates_dependency_edge(self) -> None:
        @flow
        def main():
            agg = aggregate(rows=5, output_path="agg.csv")
            rep = report(csv_path="agg.csv")
            return agg, rep

        evaluator = _validated_evaluator(main)
        agg_node = _node_for(evaluator, param="output_path", value="agg.csv")
        rep_node = _node_for(evaluator, param="csv_path", value="agg.csv")

        assert agg_node.node_id in rep_node.dependency_ids
        assert agg_node.node_id in rep_node.inferred_dependency_ids
        assert evaluator.unreachable_calls == []

    def test_dry_run_shows_two_waves(self) -> None:
        @flow
        def main():
            return aggregate(rows=3, output_path="agg.csv"), report(csv_path="agg.csv")

        evaluator = _validated_evaluator(main)
        plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")

        assert plan.wave_count == 2

    def test_real_run_orders_producer_before_consumer(self) -> None:
        """The #280 repro: without the edge, ``report`` races ``aggregate``."""
        _, count = evaluate(
            (
                aggregate(rows=5, output_path="agg.csv"),
                report(csv_path="agg.csv"),
            )
        )
        assert count == 5

    def test_producer_reachable_only_via_a_separate_use_still_gets_the_edge(self) -> None:
        """Producer's Expr is not returned directly, but stays in the graph."""

        @flow
        def main():
            agg = aggregate(rows=4, output_path="agg.csv")
            # `agg` reaches the graph only through this unrelated use, not by
            # being returned itself — its Out[...] path must still be found.
            kept = side_use(value=agg)
            return kept, report(csv_path="agg.csv")

        evaluator = _validated_evaluator(main)
        agg_node = _node_for(evaluator, param="output_path", value="agg.csv")
        rep_node = _node_for(evaluator, param="csv_path", value="agg.csv")

        assert evaluator.unreachable_calls == []
        assert agg_node.node_id in rep_node.dependency_ids

    def test_map_fan_out_gets_per_branch_edges(self) -> None:
        @flow
        def main():
            aggs = aggregate(rows=2).map(output_path=["a.csv", "b.csv"])
            reps = report().map(csv_path=["a.csv", "b.csv"])
            return aggs, reps

        evaluator = _validated_evaluator(main)
        agg_a = _node_for(evaluator, param="output_path", value="a.csv")
        agg_b = _node_for(evaluator, param="output_path", value="b.csv")
        rep_a = _node_for(evaluator, param="csv_path", value="a.csv")
        rep_b = _node_for(evaluator, param="csv_path", value="b.csv")

        assert rep_a.dependency_ids == frozenset({agg_a.node_id})
        assert rep_b.dependency_ids == frozenset({agg_b.node_id})

    def test_consumer_path_inside_produced_out_folder_gets_the_edge(self) -> None:
        @flow
        def main():
            qc = make_qc_dir(qc_dir="qc")
            rep = read_file_in_dir(report_path="qc/report.txt")
            return qc, rep

        evaluator = _validated_evaluator(main)
        qc_node = _node_for(evaluator, param="qc_dir", value="qc")
        rep_node = _node_for(evaluator, param="report_path", value="qc/report.txt")

        assert qc_node.node_id in rep_node.dependency_ids

    def test_folder_consumer_containing_a_produced_file_gets_the_edge(self) -> None:
        @flow
        def main():
            prod = write_many_into_dir(out_dir="stage")
            rep = read_whole_dir(input_dir="stage")
            return prod, rep

        evaluator = _validated_evaluator(main)
        prod_node = _node_for(evaluator, param="out_dir", value="stage")
        rep_node = _node_for(evaluator, param="input_dir", value="stage")

        assert prod_node.node_id in rep_node.dependency_ids

    def test_duplicate_out_paths_raise_before_anything_runs(self) -> None:
        @flow
        def main():
            first = touch_output(output_path="dup.txt")
            second = touch_output(output_path="dup.txt")
            return first, second

        with pytest.raises(DuplicateOutputPathError, match="dup.txt"):
            _validated_evaluator(main)

    def test_a_long_chain_of_inferred_edges_does_not_hit_the_recursion_limit(self) -> None:
        """The cycle check walks the graph iteratively."""

        @flow
        def main():
            return [relay(src=f"chain/{i}.txt", dest=f"chain/{i + 1}.txt") for i in range(3000)]

        evaluator = _validated_evaluator(main)
        plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")

        assert plan.wave_count == 3000

    def test_inferred_edges_forming_a_cycle_raise_cycle_error(self) -> None:
        @task()
        def write_a_read_b(*, a_path: Out[file], b_path: str) -> None:
            Path(a_path).write_text(Path(b_path).read_text(encoding="utf-8"), encoding="utf-8")

        @task()
        def write_b_read_a(*, b_path: Out[file], a_path: str) -> None:
            Path(b_path).write_text(Path(a_path).read_text(encoding="utf-8"), encoding="utf-8")

        @flow
        def main():
            first = write_a_read_b(a_path="a.txt", b_path="b.txt")
            second = write_b_read_a(b_path="b.txt", a_path="a.txt")
            return first, second

        with pytest.raises(CycleError, match="Detected cycle in workflow graph"):
            _validated_evaluator(main)

    def test_overwriting_your_own_file_input_is_refused_before_it_runs(self) -> None:
        """#334: the task would destroy its raw input and then cache the result."""
        Path("in.txt").write_text("RAW\n", encoding="utf-8")

        @flow
        def main():
            return overwrite_input(src="in.txt", out="in.txt")

        with pytest.raises(OutputOverwritesInputError, match="in.txt"):
            _validated_evaluator(main)
        with pytest.raises(OutputOverwritesInputError):
            evaluate(main())
        assert Path("in.txt").read_text(encoding="utf-8") == "RAW\n"

    def test_writing_inside_your_own_folder_input_is_refused(self) -> None:
        Path("data").mkdir()

        @flow
        def main():
            return write_into_input_dir(input_dir="data", out="data/summary.txt")

        with pytest.raises(OutputOverwritesInputError, match="input_dir"):
            _validated_evaluator(main)

    def test_reading_your_own_declared_output_is_not_a_self_dependency(self) -> None:
        @flow
        def main():
            return read_own_output(output_path="self.txt", also_read="self.txt")

        evaluator = _validated_evaluator(main)
        node = next(iter(evaluator.task_nodes.values()))

        assert node.dependency_ids == frozenset()
        assert node.inferred_dependency_ids == frozenset()

    def test_graph_node_registered_event_carries_the_inferred_edge(
        self, event_collector: EventCollector
    ) -> None:
        evaluate(
            (
                aggregate(rows=2, output_path="agg.csv"),
                report(csv_path="agg.csv"),
            ),
            event_bus=event_collector.bus,
        )

        registered = [
            event for event in event_collector.events if isinstance(event, GraphNodeRegistered)
        ]
        agg_event = next(event for event in registered if "aggregate" in event.task_name)
        rep_event = next(event for event in registered if "report" in event.task_name)

        assert agg_event.task_id in rep_event.dependency_ids
        assert agg_event.task_id in rep_event.inferred_dependency_ids

    def test_no_out_params_means_no_behaviour_change(self) -> None:
        """A graph with no ``Out[...]`` parameters is untouched by inference."""

        @flow
        def main():
            return join_labels(left=make_label(text="a"), right=make_label(text="b"))

        evaluator = _validated_evaluator(main)
        for node in evaluator.task_nodes.values():
            assert node.inferred_dependency_ids == frozenset()

    def test_file_annotated_consumer_waits_for_the_out_file_producer(self) -> None:
        """The reverse direction of #280: existence validation must wait too.

        ``summarise_file`` requires its ``coords`` argument to already exist
        (``file`` is validated at prepare time, right before dispatch). Without
        the inferred edge this task could be prepared — and its existence
        check run — before ``aggregate`` has written anything.
        """

        @flow
        def main():
            return (
                aggregate(rows=3, output_path="agg.csv"),
                summarise_file(coords="agg.csv", output_path="summary.csv"),
            )

        evaluate(main())
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"

    def test_dry_run_does_not_reject_a_not_yet_existing_out_path(self) -> None:
        """Static validation must not reject a `file` input this graph produces."""

        @flow
        def main():
            return (
                aggregate(rows=3, output_path="agg.csv"),
                summarise_file(coords="agg.csv", output_path="summary.csv"),
            )

        # Would raise FileNotFoundError before #307 phase 2's dry-run skip,
        # since "agg.csv" does not exist on disk yet at validation time.
        evaluator = _validated_evaluator(main)
        assert len(evaluator.task_nodes) == 2

    def test_dynamic_expansion_infers_edges_among_newly_registered_nodes(self) -> None:
        """A producer/consumer pair built at runtime (dynamic graph expansion)."""
        _, count = evaluate(build_and_link(rows=4))
        assert count == 4

    def test_expanding_task_may_spawn_children_writing_inside_its_folder(
        self, tmp_path: Path
    ) -> None:
        """The expanding task is itself a consumer of its children's paths."""
        results_dir = tmp_path / "res"
        results_dir.mkdir()
        assert evaluate(spawn_into_folder(d=str(results_dir))) == (1, 2)
        assert (results_dir / "b.txt").read_text(encoding="utf-8") == "2\n"

    def test_dynamic_producer_retroactively_gates_a_pending_consumer(self) -> None:
        """A reader registered earlier waits for a producer registered later."""

        @flow
        def main():
            gate = slow_gate(seconds=1.0)
            spawned = spawn_producer(path="late.csv", gate=0)
            return spawned, report_after(csv_path="late.csv", gate=gate)

        _, count = evaluate(main())
        assert count == 3

    def test_dynamic_producer_for_an_already_read_path_raises(self) -> None:
        Path("late.csv").write_text("old\n", encoding="utf-8")

        @flow
        def main():
            count = report_after(csv_path="late.csv", gate=0)
            return spawn_producer(path="late.csv", gate=count)

        with pytest.raises(RuntimeError, match="already read that path"):
            evaluate(main())
