"""Tests for silent failures at flow/graph boundaries.

Covers four defects, all of them a path or a call that crosses a boundary
without the graph noticing. A path crossing a task boundary as ``str``
contributes only its path string to the downstream cache key (#121), and a task
call unreachable from the flow return value is dropped from the graph (#122).

The two later issues share one root cause with those: a literal path argument
is a value with no provenance, so it buys neither an edge nor a content-hashed
key. Where no task produces it, it is an untracked input and the consumer
serves stale results in silence (#281). Where a task does produce it, nothing
orders the two, so they race and the corrupt result is cached (#280).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import evaluate, file, flow, task, tmp_dir
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.cli.commands.init import write_starter_project
from ginkgo.config import config_session
from ginkgo.core.expr import record_constructed_calls
from ginkgo.core.flow import discover_flow
from ginkgo.runtime.diagnostics import (
    SHARED_LITERAL_PATH_CODE,
    UNREACHABLE_CALL_CODE,
    shared_literal_path_diagnostics,
    unreachable_call_diagnostics,
)
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator, UndeclaredPathDependencyError
from ginkgo.runtime.events import TaskNotice
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.path_hazards import (
    ancestor_ids,
    are_ordered,
    literal_path_arguments,
    shared_literal_path_findings,
)
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


def _literal_path_sites(nodes: Any) -> dict[str, set[int]]:
    """Map each path-shaped literal in a graph to the nodes it is passed to."""
    sites: dict[str, set[int]] = {}
    for node_id, node in nodes.items():
        for argument in node.expr.args.values():
            for path in literal_path_arguments(argument):
                sites.setdefault(path, set()).add(node_id)
    return sites


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

    def test_literal_input_path_warns(self, event_collector: EventCollector) -> None:
        """#281 — a literal path no task produces is an untracked input.

        The shape every pipeline starts with: a raw input handed to the first
        task. It has no producer expression, so the notice #121 added could
        never fire for it, and editing the file on disk served a stale result
        in silence.
        """
        Path("rows.csv").write_text("0\n1\n", encoding="utf-8")

        evaluate(
            summarise_str(coords="rows.csv", output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        messages = _notices(event_collector)
        assert len(messages) == 1
        assert "rows.csv" in messages[0]
        assert "coords" in messages[0]

    def test_literal_output_path_is_silent(self, event_collector: EventCollector) -> None:
        """A path the task itself writes is an output, not an untracked input.

        The starter template's dominant idiom. It exists on disk from the
        second run onward, so existence alone cannot tell it apart from an
        input — being a task's declared output is what does.
        """
        Path("rows.csv").write_text("0\n1\n", encoding="utf-8")
        evaluate(summarise_str(coords="rows.csv", output_path="summary.csv"))
        assert Path("summary.csv").is_file()

        evaluate(
            summarise_str(coords="rows.csv", output_path="summary.csv"),
            event_bus=event_collector.bus,
        )

        assert not [message for message in _notices(event_collector) if "output_path" in message]

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
            # A rehydrated ``text`` asset arrives as its contents.
            (str, "# Title\n\n" + "x" * 400, False),
            (str, "has a \x00 in it", False),
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


AGG_PATH = "agg.csv"


@task()
def write_agg_str(*, rows: int, output_path: str) -> str:
    """Write a file slowly, returning its path as a plain ``str``."""
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        handle.write("head\n")
        handle.flush()
        time.sleep(0.4)
        handle.write("\n".join(str(index) for index in range(rows)) + "\n")
    return output_path


@task()
def write_agg_body_path(*, rows: int) -> file:
    """Write the module-constant path, taking no path argument at all.

    The shape #280 was filed as. The path this task writes is invisible to the
    graph until the task has run, so no static check can reach it — which is
    why the post-hoc detector has to exist alongside the pre-flight one.
    """
    target = Path(AGG_PATH)
    with target.open("w", encoding="utf-8") as handle:
        handle.write("head\n")
        handle.flush()
        time.sleep(0.4)
        handle.write("\n".join(str(index) for index in range(rows)) + "\n")
    return file(AGG_PATH)


@task()
def read_agg(*, csv_path: str) -> str:
    """Read whatever is at ``csv_path`` right now, missing file included."""
    target = Path(csv_path)
    return target.read_text(encoding="utf-8") if target.is_file() else "<<MISSING>>"


@task()
def read_agg_file(*, csv_path: file) -> str:
    """Read a path annotated ``file``, which validation requires to exist."""
    return Path(csv_path).read_text(encoding="utf-8")


class TestUndeclaredPathDependency:
    """#280 — a literal path between tasks buys no edge, so they race."""

    def test_race_fails_the_run_and_names_both_tasks(self) -> None:
        """The producer stays in the graph, so #122 cannot fire for it."""

        @flow
        def main():
            produced = write_agg_body_path(rows=3)
            consumed = read_agg(csv_path=AGG_PATH)
            return [produced, consumed]

        with pytest.raises(UndeclaredPathDependencyError) as excinfo:
            evaluate(main())

        message = str(excinfo.value)
        assert "read_agg" in message
        assert "write_agg_body_path" in message
        assert AGG_PATH in message

    def test_raced_consumer_is_not_left_cached(self) -> None:
        """The corrupt result must not survive as a permanent cache hit."""

        @flow
        def main():
            return [write_agg_body_path(rows=3), read_agg(csv_path=AGG_PATH)]

        for _ in range(2):
            with pytest.raises(UndeclaredPathDependencyError):
                evaluate(main())

        # A second run that found the consumer cached would resolve without
        # re-reading, and so would raise nothing at all.

    def test_file_annotation_does_not_close_the_race(self) -> None:
        """#280's stale-file bypass: `file` is an existence check, not an edge.

        Annotating the parameter ``file`` refuses the first run outright,
        because the file is not there yet. On every later run it *is* there,
        the existence check passes, and the race returns in full — which is
        what turns always-wrong into intermittently-wrong.
        """
        Path(AGG_PATH).write_text("STALE\n", encoding="utf-8")

        @flow
        def main():
            return [write_agg_body_path(rows=3), read_agg_file(csv_path=AGG_PATH)]

        with pytest.raises(UndeclaredPathDependencyError) as excinfo:
            evaluate(main())

        assert "read_agg_file" in str(excinfo.value)

    def test_ordered_producer_and_consumer_are_silent(self) -> None:
        """Passing the returned value, not the path string, is the fix."""

        @flow
        def main():
            return summarise_file(
                coords=write_rows_file(rows=3, output_path="rows.csv"),
                output_path="summary.csv",
            )

        evaluate(main())
        assert Path("summary.csv").read_text(encoding="utf-8") == "rows,3\n"


class TestSharedLiteralPath:
    """#280's statically decidable subset: one literal path, two unordered tasks."""

    def test_unordered_pair_is_reported(self) -> None:
        @flow
        def main():
            return [
                write_agg_str(rows=3, output_path=AGG_PATH),
                read_agg(csv_path=AGG_PATH),
            ]

        findings = shared_literal_path_findings(_validated_evaluator(main).task_nodes)

        assert len(findings) == 1
        assert findings[0].path == AGG_PATH
        assert {findings[0].first_parameter, findings[0].second_parameter} == {
            "output_path",
            "csv_path",
        }

    def test_transitively_ordered_pair_is_silent(self) -> None:
        """Reachability, not adjacency — the starter template's shape.

        ``write_summary`` reads seed paths its ``write_seed_card`` nodes wrote,
        connected only through the chain between them. A direct-edge test would
        call that a race.
        """

        @flow
        def main():
            written = write_rows_str(rows=3, output_path="rows.csv")
            relayed = make_label(text=written)
            return summarise_str(coords="rows.csv", output_path=relayed)

        assert shared_literal_path_findings(_validated_evaluator(main).task_nodes) == []

    def test_shared_non_path_argument_is_not_a_finding(self) -> None:
        """A label two tasks share is not a file two tasks fight over."""

        @flow
        def main():
            return join_labels(
                left=make_label(text="alpha"),
                right=make_label(text="alpha"),
            )

        assert shared_literal_path_findings(_validated_evaluator(main).task_nodes) == []

    def test_doctor_reports_it_as_a_warning(self) -> None:
        @flow
        def main():
            return [
                write_agg_str(rows=3, output_path=AGG_PATH),
                read_agg(csv_path=AGG_PATH),
            ]

        diagnostics = shared_literal_path_diagnostics(nodes=_validated_evaluator(main).task_nodes)

        assert len(diagnostics) == 1
        assert diagnostics[0].severity == "warning"
        assert diagnostics[0].code == SHARED_LITERAL_PATH_CODE
        # The wording carries the weight severity does not.
        assert "at the same time" in diagnostics[0].message
        assert AGG_PATH in diagnostics[0].message

    def test_dry_run_plan_reports_it(self) -> None:
        @flow
        def main():
            return [
                write_agg_str(rows=3, output_path=AGG_PATH),
                read_agg(csv_path=AGG_PATH),
            ]

        plan = build_dry_run_plan(
            evaluator=_validated_evaluator(main), workflow_label="workflow.py"
        )

        assert plan.wave_count == 1, "the two tasks share a wave — that is the race"
        assert any(AGG_PATH in diagnostic.message for diagnostic in plan.diagnostics)


class TestAssetLogicalFilename:
    """#289 — an asset knows where its producer wrote it, not just its blob."""

    def test_source_path_survives_a_cache_hit(self) -> None:
        """Cold and warm must agree, or a warm rerun loses the produced path.

        The detector reads produced paths from completed nodes, and a cache
        hit completes a node without running it. If the ref came back without
        its declared path, every output path in a warm run would look like a
        path no task produced.
        """
        cold = evaluate(produce_file_asset(output_path="rows.csv"))
        assert isinstance(cold, AssetRef)
        assert cold.source_path == "rows.csv"
        assert cold.filename == "rows.csv"

        warm = evaluate(produce_file_asset(output_path="rows.csv"))
        assert isinstance(warm, AssetRef)
        assert warm.source_path == "rows.csv"

    def test_artifact_path_is_not_the_logical_name(self) -> None:
        """The distinction the issue is about: blob path versus filename."""
        ref = evaluate(produce_file_asset(output_path="nested/rows.csv"))

        assert isinstance(ref, AssetRef)
        assert "artifacts" in ref.artifact_path
        assert ref.filename == "rows.csv"
        assert ref.source_path == "nested/rows.csv"

    def test_in_memory_payload_has_no_declared_path(self) -> None:
        """A table built from a DataFrame never had a path to declare."""
        ref = AssetRef(
            key=AssetKey(namespace="table", name="t"),
            version_id="v",
            kind="table",
            artifact_id="a",
            content_hash="h",
            artifact_path="/store/blobs/a.parquet",
        )

        assert ref.source_path is None
        assert ref.filename is None

    def test_round_trips_through_serialization(self) -> None:
        ref = evaluate(produce_file_asset(output_path="rows.csv"))
        assert isinstance(ref, AssetRef)

        assert AssetRef.from_dict(ref.to_dict()).source_path == "rows.csv"

    def test_entry_written_before_the_field_still_loads(self) -> None:
        """An older cache entry has no ``source_path`` key at all."""
        payload = {
            "key": {"namespace": "file", "name": "old"},
            "version_id": "v",
            "kind": "file",
            "artifact_id": "a",
            "content_hash": "h",
            "artifact_path": "/store/blobs/a.csv",
        }

        assert AssetRef.from_dict(payload).source_path is None


class TestStarterTemplateStaysSilent:
    """The property most likely to regress: no findings on the shipped scaffold.

    Every check here has to be silent on what ``ginkgo init`` gives a user.
    The template's dominant idiom is an output path passed as a literal
    ``str`` argument, which exists on disk from the second run onward — so a
    naive "literal path that exists" rule would fire on all of it.
    """

    @pytest.fixture
    def scaffold_nodes(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
        """Build the real ``ginkgo init`` graph, without running it."""
        root = write_starter_project(root=tmp_path / "starter")
        monkeypatch.chdir(root)

        with config_session(
            override_paths=[], param_config={}, cli_extras=(), require_params=False
        ):
            module = load_module_from_path(Path("workflow/flow.py"))
            with record_constructed_calls() as constructed_calls:
                expr = discover_flow(module)()
        evaluator = ConcurrentEvaluator(constructed_calls=tuple(constructed_calls))
        evaluator.build_and_validate(expr)
        return evaluator.task_nodes

    def test_shares_paths_but_always_in_order(self, scaffold_nodes: Any) -> None:
        """The template does share literal paths — that is what makes it a test.

        ``write_summary`` reads the seed paths ``write_seed_card`` wrote. If
        no path were shared, silence would prove nothing.
        """
        shared = [
            path
            for path, node_ids in _literal_path_sites(scaffold_nodes).items()
            if len(node_ids) > 1
        ]

        assert shared, "template no longer shares a literal path; this test is now vacuous"

    def test_static_check_is_silent(self, scaffold_nodes: Any) -> None:
        assert shared_literal_path_findings(scaffold_nodes) == []

    def test_doctor_is_silent(self, scaffold_nodes: Any) -> None:
        assert shared_literal_path_diagnostics(nodes=scaffold_nodes) == []

    def test_all_nodes_sharing_a_path_are_ordered(self, scaffold_nodes: Any) -> None:
        """Spelled out as the property, not just the absence of findings.

        Silence could also come from the walk finding no literal paths at
        all. This says what actually holds: wherever the template hands one
        path to more than one task, a dependency path runs between them.
        """
        ancestors = ancestor_ids(scaffold_nodes)
        for path, node_ids in _literal_path_sites(scaffold_nodes).items():
            ordered = sorted(node_ids)
            for index, left in enumerate(ordered):
                for right in ordered[index + 1 :]:
                    assert are_ordered(ancestors=ancestors, left=left, right=right), (
                        f"{path} reaches two tasks with nothing ordering them"
                    )


class TestStarterTemplateShapeAtRuntime:
    """The template's idiom, reduced to something a test can actually run.

    Running the real scaffold needs Pixi environments and Docker, so its
    runtime verdict is out of reach here. This mirrors the shape that matters:
    an ``asset()``-returning producer handed its output path as a literal
    ``str``, and a consumer reading that same literal, ordered behind it.

    It is the interaction most likely to break — the producer's declared path
    reaches the detector only through ``AssetRef.source_path``, so losing that
    field turns every template output path into an untracked input.
    """

    def test_no_notice_cold_or_warm(self, event_collector: EventCollector) -> None:
        def build():
            produced = produce_file_asset(output_path="rows.csv")
            return receive_as_str(incoming=produced, output_path="summary.csv")

        evaluate(build())
        evaluate(build(), event_bus=event_collector.bus)

        assert [message for message in _notices(event_collector) if "rows.csv" in message] == []
