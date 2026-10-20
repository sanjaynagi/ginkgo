"""Tests for the static task graph, built without an evaluator."""

from __future__ import annotations

import pytest

from ginkgo import Out, file, task
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.graph import CycleError, IncompleteCallError, TaskGraph


@task()
def label(*, text: str) -> str:
    return text


@task()
def join(*, left: str, right: str) -> str:
    return left + right


@task()
def write(*, output_path: Out[file]) -> file:
    return file(output_path)


@task()
def read(*, path: file) -> str:
    return path


@task()
def read_then_write(*, path: file, output_path: Out[file]) -> file:
    return file(output_path)


class TestRegistration:
    def test_nodes_take_ids_in_walk_order_with_declared_edges(self) -> None:
        graph = TaskGraph.build(join(left=label(text="a"), right=label(text="b")))

        assert [node.task_def.name.rsplit(".", 1)[-1] for node in graph.nodes.values()] == [
            "join",
            "label",
            "label",
        ]
        assert graph.nodes[0].dependency_ids == {1, 2}

    def test_a_repeated_call_label_takes_an_ordinal(self) -> None:
        graph = TaskGraph.build([label(text="a"), label(text="b")])

        assert [node.label for node in graph.nodes.values()] == ["label", "label[2]"]

    def test_a_shared_expression_is_one_node(self) -> None:
        shared = label(text="a")
        graph = TaskGraph.build(join(left=shared, right=shared))

        assert len(graph.nodes) == 2

    def test_a_partial_call_is_rejected(self) -> None:
        with pytest.raises(IncompleteCallError, match="right"):
            TaskGraph.build(join(left="a"))


class TestInferredEdges:
    def test_a_reader_of_a_written_path_depends_on_the_writer(self, tmp_path) -> None:
        path = str(tmp_path / "out.txt")
        graph = TaskGraph.build([read(path=path), write(output_path=path)])

        reader, writer = graph.nodes[0], graph.nodes[1]
        assert reader.dependency_ids == {writer.node_id}
        assert reader.inferred_dependency_ids == {writer.node_id}

    def test_an_inferred_edge_closing_a_cycle_is_rejected(self, tmp_path) -> None:
        a, b = str(tmp_path / "a.txt"), str(tmp_path / "b.txt")
        with pytest.raises(CycleError):
            TaskGraph.build(
                [
                    read_then_write(path=a, output_path=b),
                    read_then_write(path=b, output_path=a),
                ]
            )

    def test_a_new_writer_cannot_feed_a_reader_already_started(self, tmp_path) -> None:
        path = str(tmp_path / "out.txt")
        graph = TaskGraph.build(read(path=path))

        with pytest.raises(RuntimeError, match="already read that path"):
            graph.register(write(output_path=path), is_pending=lambda node_id: False)

    def test_a_new_writer_feeds_a_pending_reader(self, tmp_path) -> None:
        path = str(tmp_path / "out.txt")
        graph = TaskGraph.build(read(path=path))

        registration = graph.register(write(output_path=path))

        assert registration.new_node_ids == [1]
        assert graph.nodes[0].inferred_dependency_ids == {1}


class TestUnreachableCalls:
    def test_a_call_the_value_never_reaches_is_unreachable(self) -> None:
        with record_constructed_calls() as calls:
            label(text="dropped")
            kept = label(text="kept")
        graph = TaskGraph.build(kept)

        assert [call.label for call in graph.unreachable_calls(calls)] == ["label()"]
