"""Tests for silent failures at flow/graph boundaries.

Covers three defects: a path crossing a task boundary as ``str`` contributes
only its path string to the downstream cache key (#121), a task call
unreachable from the flow return value is dropped from the graph (#122), and
a literal path shared between two tasks creates no dependency edge, so a
producer and consumer race in the same wave (#280, closed by #307 phase 2 —
see ``TestOutPathEdgeInference`` below).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pytest

import ginkgo
from ginkgo import Out, evaluate, file, flow, folder, task, tmp_dir
from ginkgo.core.asset import AssetKey, AssetRef
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.diagnostics import UNREACHABLE_CALL_CODE, unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.edge_inference import DuplicateOutputPathError
from ginkgo.runtime.evaluator import ConcurrentEvaluator, CycleError
from ginkgo.runtime.events import GraphNodeRegistered, TaskNotice
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
def side_use(*, value: object) -> object:
    """Consume ``value`` for no other reason than to keep it reachable."""
    return value


@task()
def build_and_link(*, rows: int) -> object:
    """Dynamically register a producer/consumer pair sharing a literal path."""
    agg = aggregate(rows=rows, output_path="dyn.csv")
    rep = report(csv_path="dyn.csv")
    return agg, rep


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
