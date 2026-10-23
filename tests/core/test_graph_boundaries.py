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

from ginkgo import Out, evaluate, file, flow, folder, task, tmp_dir, untracked
from ginkgo.core.expr import ConstructedCall, record_constructed_calls
from ginkgo.runtime.diagnostics import UNREACHABLE_CALL_CODE, unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.edge_inference import DuplicateOutputPathError, OutputOverwritesInputError
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.graph import CycleError, IncompleteCallError, TaskNode
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


def _write_rows(path: str, rows: int) -> None:
    Path(path).write_text("".join(f"{index}\n" for index in range(rows)), encoding="utf-8")


def test_only_a_written_str_input_is_flagged_and_reruns(event_collector: EventCollector) -> None:
    """A file a task writes through a plain ``str`` path is flagged.

    Plain ``str`` path inputs are content-hashed (#281), so a task that writes
    one invalidates its own cache entry and re-runs every time; the notice
    names the fix (``Out[file]`` or ``untracked``). A path the task only reads,
    or returns, stays content-tracked and silent.
    """
    Path("in.txt").write_text("original\n", encoding="utf-8")
    _write_rows("rows.csv", 2)

    def run() -> None:
        evaluate(
            (
                append_to_log(n=3, log_path="run.log"),
                append_to_untracked_log(n=3, log_path="untracked.log"),
                pass_through(path="in.txt"),
                summarise_str(coords="rows.csv", output_path="summary.csv"),
            ),
            event_bus=event_collector.bus,
        )

    run()
    run()
    Path("in.txt").write_text("changed\n", encoding="utf-8")
    run()

    assert Path("run.log").read_text(encoding="utf-8") == "ran 3\n" * 3
    assert Path("untracked.log").read_text(encoding="utf-8") == "ran 3\n"
    assert Path("pass_through_runs.txt").read_text(encoding="utf-8") == "ran\nran\n"
    notices = _notices(event_collector)
    assert notices and all("Out[file]" in notice and "run.log" in notice for notice in notices)


def test_tracked_path_boundaries_rerun_and_only_a_str_directory_warns(
    event_collector: EventCollector,
) -> None:
    """What a path handed between two tasks invalidates (#121, #281, #307).

    A ``str`` or ``file`` boundary naming an existing *file* is content-hashed,
    so a change re-runs the consumer, silently. A *directory* behind a ``str``
    is never auto-hashed: it serves stale output and is the one shape that
    warns, suggesting ``folder``. ``untracked`` opts out deliberately and
    silently.
    """

    def run(*, rows: int, tag: str) -> None:
        evaluate(
            (
                summarise_str(
                    coords=write_rows_str(rows=rows, output_path="str.csv"),
                    output_path="str_summary.csv",
                ),
                summarise_file(
                    coords=write_rows_file(rows=rows, output_path="file.csv"),
                    output_path="file_summary.csv",
                ),
                summarise_untracked(
                    coords=write_rows_untracked(rows=rows, output_path="untracked.csv"),
                    output_path="untracked_summary.csv",
                ),
                summarise_str(coords="literal.csv", output_path="literal_summary.csv"),
                summarise_dir_str(
                    readings=write_dir_str(tag=tag, output_dir="readings"),
                    output_path="dir_summary.csv",
                ),
            ),
            event_bus=event_collector.bus,
        )

    _write_rows("literal.csv", 3)
    run(rows=3, tag="a")
    _write_rows("literal.csv", 4)
    run(rows=5, tag="b")

    def summary(name: str) -> str:
        return Path(f"{name}_summary.csv").read_text(encoding="utf-8")

    assert summary("str") == "rows,5\n"  # the #121 repro
    assert summary("file") == "rows,5\n"
    assert summary("literal") == "rows,4\n"  # the #281 repro: a literal root input
    assert summary("untracked") == "rows,3\n"
    # A second file landed in the directory, but the key is still its path string.
    assert summary("dir") == "entries,1\n"

    notices = _notices(event_collector)
    assert notices
    for message in notices:
        assert "write_dir_str" in message and "readings" in message and "folder" in message


def test_paths_nested_in_containers_are_silent(event_collector: EventCollector) -> None:
    evaluate(
        (
            summarise_many_str(
                coords=[
                    write_rows_str(rows=2, output_path="a.csv"),
                    write_rows_str(rows=3, output_path="b.csv"),
                ],
                output_path="list_summary.csv",
            ),
            summarise_many_file(
                coords=[
                    write_rows_file(rows=2, output_path="c.csv"),
                    write_rows_file(rows=3, output_path="d.csv"),
                ],
                output_path="file_list_summary.csv",
            ),
            summarise_many_str(
                coords=write_rows_str(rows=2).map(output_path=["e.csv", "f.csv"]),
                output_path="fan_out_summary.csv",
            ),
            summarise_mapping_str(
                coords={"first": write_rows_str(rows=2, output_path="g.csv")},
                output_path="dict_summary.csv",
            ),
        ),
        event_bus=event_collector.bus,
    )

    assert _notices(event_collector) == []


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
            # Long text that cannot name a path at all.
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


@pytest.mark.parametrize(
    ("annotation", "value", "expected"),
    [(str, "present_dir", True), (str, "present.csv", False), (folder, "present_dir", False)],
)
def test_is_untracked_directory_value(annotation: Any, value: str, expected: bool) -> None:
    """The predicate behind the narrowed evaluator notice."""
    Path("present.csv").write_text("x\n", encoding="utf-8")
    Path("present_dir").mkdir()
    assert is_untracked_directory_value(annotation=annotation, value=value) is expected


def _validated_evaluator(build: Any) -> tuple[ConcurrentEvaluator, list[ConstructedCall]]:
    """Run a flow body under a construction recorder and validate the graph.

    Returns the evaluator and the constructed calls its graph never reached.
    """
    with record_constructed_calls() as constructed_calls:
        expr = build()
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(expr)
    evaluator.validate_graph()
    return evaluator, evaluator.graph.unreachable_calls(constructed_calls)


def _drop_one_label():
    kept = make_label(text="kept")
    make_label(text="dropped")
    return kept


def _drop_a_literal_replaced_producer():
    """Case 2 of #122: a literal path in place of the upstream expression."""
    write_rows_str(rows=3, output_path="rows.csv")
    return summarise_str(coords="rows.csv", output_path="summary.csv")


def _drop_a_fan_out():
    make_label().map(text=["a", "b", "c"])
    return make_label(text="kept")


def _drop_an_empty_fan_out():
    make_label().map(text=[])
    return join_labels(left="a", right="b")


def _map_one_partial_twice():
    partial = join_labels(left="a")
    return partial.map(right=["x"]), partial.map(right=["y"])


class TestUnreachableCalls:
    """#122 — calls not reachable from the flow return value are dropped."""

    @pytest.mark.parametrize(
        ("build", "task_count", "dropped"),
        [
            (_drop_one_label, 1, ["make_label()"]),
            (_drop_a_literal_replaced_producer, 1, ["write_rows_str()"]),
            (_drop_a_fan_out, 1, ["make_label() × 3"]),
            # No branches were built, so no call was dropped.
            (_drop_an_empty_fan_out, 1, []),
            (lambda: make_label().map(text=[]), 0, []),
            (lambda: join_labels(left=make_label(text="a"), right=make_label(text="b")), 3, []),
            (lambda: (make_label(text="a"), make_label(text="b")), 2, []),
            (lambda: join_labels().map(left=["a", "b"]).map(right=["x", "y"]), 4, []),
            # #332: .map() and .product_map() complete a partial call.
            (lambda: join_labels(left="a").map(right=["x", "y"]), 2, []),
            (lambda: join_labels().product_map(left=["a", "b"], right=["x", "y"]), 4, []),
            (_map_one_partial_twice, 2, []),
        ],
    )
    def test_reachability(self, build: Any, task_count: int, dropped: list[str]) -> None:
        Path("rows.csv").write_text("0\n", encoding="utf-8")

        evaluator, unreachable = _validated_evaluator(build)

        assert len(evaluator.task_nodes) == task_count
        assert [call.label for call in unreachable] == dropped

    def test_dropped_calls_are_warnings_and_appear_in_the_dry_run_plan(self) -> None:
        def main():
            make_label(text="dropped")
            join_labels(left="a")
            return make_label(text="kept")

        evaluator, unreachable = _validated_evaluator(main)
        diagnostics = unreachable_call_diagnostics(calls=unreachable)
        plan = build_dry_run_plan(
            evaluator=evaluator, workflow_label="workflow.py", unreachable_calls=unreachable
        )

        assert {d.severity for d in diagnostics} == {"warning"}
        assert {d.code for d in diagnostics} == {UNREACHABLE_CALL_CODE}
        assert diagnostics[0].location.endswith("make_label")
        assert any("missing required argument" in d.message for d in diagnostics)
        assert plan.task_count == 1
        assert set(plan.dropped_labels) == {
            "make_label()",
            "join_labels() (missing required argument(s): right)",
        }


class TestPartialCalls:
    """#332 — a call missing a required argument is a ``PartialCall``, not a task."""

    @pytest.mark.parametrize(
        ("build", "match"),
        [
            (lambda: join_labels(left="a"), r"join_labels\(\) .*right"),
            (lambda: [make_label(text="a"), make_label()], r"make_label\(\) .*text"),
            # Consumed: the message names the consumer too.
            (
                lambda: join_labels(left=make_label(), right="b"),
                r"make_label\(\)(.|\n)*text(.|\n)*join_labels\(\)",
            ),
        ],
    )
    def test_a_returned_or_consumed_partial_call_is_a_build_error(
        self, build: Any, match: str
    ) -> None:
        with pytest.raises(IncompleteCallError, match=match):
            _validated_evaluator(build)

    def test_returned_partial_call_fails_evaluate(self) -> None:
        with pytest.raises(IncompleteCallError, match=r"join_labels\(\) .*right"):
            evaluate(join_labels(left="a"))


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


def _node_for(evaluator: ConcurrentEvaluator, *, param: str, value: Any) -> TaskNode:
    matches = [
        node for node in evaluator.graph.nodes.values() if node.expr.args.get(param) == value
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

        evaluator, unreachable = _validated_evaluator(main)
        agg_node = _node_for(evaluator, param="output_path", value="agg.csv")
        rep_node = _node_for(evaluator, param="csv_path", value="agg.csv")

        assert agg_node.node_id in rep_node.dependency_ids
        assert agg_node.node_id in rep_node.inferred_dependency_ids
        assert unreachable == []
        plan = build_dry_run_plan(
            evaluator=evaluator, workflow_label="workflow.py", unreachable_calls=unreachable
        )
        assert plan.wave_count == 2

    def test_real_run_orders_producers_before_consumers(
        self, event_collector: EventCollector
    ) -> None:
        """The #280 repro: without the edge, ``report`` races ``aggregate``.

        ``summarise_file`` is the reverse direction: its ``file`` argument is
        validated for existence right before dispatch, which must wait too.
        """
        _, count, _ = evaluate(
            (
                aggregate(rows=5, output_path="agg.csv"),
                report(csv_path="agg.csv"),
                summarise_file(coords="agg.csv", output_path="summary.csv"),
            ),
            event_bus=event_collector.bus,
        )

        assert count == 5
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,5\n"
        registered = [
            event for event in event_collector.events if isinstance(event, GraphNodeRegistered)
        ]
        agg_event = next(event for event in registered if "aggregate" in event.task_name)
        rep_event = next(event for event in registered if "report" in event.task_name)
        assert agg_event.task_id in rep_event.inferred_dependency_ids

    def test_producer_reachable_only_via_a_separate_use_still_gets_the_edge(self) -> None:
        """Producer's Expr is not returned directly, but stays in the graph."""

        @flow
        def main():
            agg = aggregate(rows=4, output_path="agg.csv")
            # `agg` reaches the graph only through this unrelated use, not by
            # being returned itself — its Out[...] path must still be found.
            kept = side_use(value=agg)
            return kept, report(csv_path="agg.csv")

        evaluator, unreachable = _validated_evaluator(main)
        agg_node = _node_for(evaluator, param="output_path", value="agg.csv")
        rep_node = _node_for(evaluator, param="csv_path", value="agg.csv")

        assert unreachable == []
        assert agg_node.node_id in rep_node.dependency_ids

    def test_map_fan_out_gets_per_branch_edges(self) -> None:
        @flow
        def main():
            aggs = aggregate(rows=2).map(output_path=["a.csv", "b.csv"])
            reps = report().map(csv_path=["a.csv", "b.csv"])
            return aggs, reps

        evaluator, unreachable = _validated_evaluator(main)
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

        evaluator, unreachable = _validated_evaluator(main)
        qc_node = _node_for(evaluator, param="qc_dir", value="qc")
        rep_node = _node_for(evaluator, param="report_path", value="qc/report.txt")

        assert qc_node.node_id in rep_node.dependency_ids

    def test_folder_consumer_containing_a_produced_file_gets_the_edge(self) -> None:
        @flow
        def main():
            prod = write_many_into_dir(out_dir="stage")
            rep = read_whole_dir(input_dir="stage")
            return prod, rep

        evaluator, unreachable = _validated_evaluator(main)
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

        evaluator, unreachable = _validated_evaluator(main)
        plan = build_dry_run_plan(
            evaluator=evaluator, workflow_label="workflow.py", unreachable_calls=unreachable
        )

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

        evaluator, unreachable = _validated_evaluator(main)
        node = next(iter(evaluator.graph.nodes.values()))

        assert node.dependency_ids == frozenset()
        assert node.inferred_dependency_ids == frozenset()

    def test_no_out_params_means_no_behaviour_change(self) -> None:
        """A graph with no ``Out[...]`` parameters is untouched by inference."""

        @flow
        def main():
            return join_labels(left=make_label(text="a"), right=make_label(text="b"))

        evaluator, unreachable = _validated_evaluator(main)
        for node in evaluator.graph.nodes.values():
            assert node.inferred_dependency_ids == frozenset()

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
        evaluator, unreachable = _validated_evaluator(main)
        assert len(evaluator.task_nodes) == 2

    def test_dynamic_expansion_infers_edges_among_newly_registered_nodes(self) -> None:
        """A producer/consumer pair built at runtime, and an expanding task whose
        children write inside its folder: the expanding task is itself a consumer
        of its children's paths."""
        Path("res").mkdir()

        (_, count), spawned = evaluate((build_and_link(rows=4), spawn_into_folder(d="res")))

        assert count == 4
        assert spawned == (1, 2)
        assert Path("res/b.txt").read_text(encoding="utf-8") == "2\n"

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
