"""Tests for the static task graph, built without an evaluator."""

from __future__ import annotations

import pytest

from ginkgo import Out, file, folder, task
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


@task()
def write_after(*, up: str, output_path: Out[file]) -> file:
    return file(output_path)


@task()
def split(*, split_dir: Out[folder]) -> list:
    return []


class TestRegistration:
    def test_a_caller_takes_a_lower_id_than_its_arguments(self) -> None:
        graph = TaskGraph.build(join(left=label(text="a"), right=label(text="b")))

        assert graph.nodes[0].label == "join"
        assert graph.nodes[0].dependency_ids == {1, 2}

    def test_nodes_are_held_after_their_dependencies(self) -> None:
        # A scheduler pass over the nodes then resolves a cached chain at once.
        graph = TaskGraph.build(join(left=label(text="a"), right=label(text="b")))

        assert list(graph.nodes) == [1, 2, 0]

    def test_a_repeated_call_label_takes_an_ordinal(self) -> None:
        graph = TaskGraph.build(join(left=label(text="a"), right=label(text="b")))

        assert [graph.nodes[node_id].label for node_id in (1, 2)] == ["label", "label[2]"]

    def test_a_chain_s_ordinals_follow_the_order_it_runs(self) -> None:
        """SC5-1: the first call of a chain to run has the bare label."""
        graph = TaskGraph.build(label(text=label(text=label(text="a"))))

        by_id = sorted(graph.nodes.values(), key=lambda node: node.node_id)
        # Ids run root-first; the root runs last and takes the last ordinal.
        assert [node.label for node in by_id] == ["label[3]", "label[2]", "label"]

    def test_calls_listed_together_are_labelled_by_the_argument_that_differs(self) -> None:
        graph = TaskGraph.build([join(left="x", right=r) for r in ("a", "b")])

        assert [node.label for node in graph.nodes.values()] == ["join[a]", "join[b]"]

    def test_listed_calls_differing_only_by_upstream_calls_inherit_their_labels(self) -> None:
        texts = [label(text=t) for t in ("a", "b")]
        graph = TaskGraph.build([texts, [join(left=t, right="z") for t in texts]])

        assert sorted(node.label for node in graph.nodes.values()) == [
            "join[a]",
            "join[b]",
            "label[a]",
            "label[b]",
        ]

    def test_identical_listed_calls_take_ordinals(self) -> None:
        graph = TaskGraph.build([label(text="a"), label(text="a")])

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

        with pytest.raises(RuntimeError, match="state='running'"):
            graph.register(write(output_path=path), node_state=lambda node_id: "running")
        # The writer stays registered, so a caller can still give it run state.
        assert graph.new_node_ids == [1]

    def test_a_new_writer_feeds_a_pending_reader(self, tmp_path) -> None:
        path = str(tmp_path / "out.txt")
        graph = TaskGraph.build(read(path=path))

        registration = graph.register(write(output_path=path))

        assert registration.new_node_ids == [1]
        assert graph.nodes[0].inferred_dependency_ids == {1}


class TestDynamicExpansionEdges:
    """G2: a dynamic expansion's children against the task that returned them."""

    def test_children_reading_inside_the_parents_output_folder_do_not_wait_on_it(
        self, tmp_path
    ) -> None:
        split_dir = str(tmp_path / "chunks")
        graph = TaskGraph.build(split(split_dir=split_dir))

        registration = graph.register(
            [read(path=f"{split_dir}/chunk{i}.txt") for i in range(2)], expanding_node_id=0
        )

        for node_id in registration.new_node_ids:
            assert graph.nodes[node_id].dependency_ids == frozenset()

    def test_a_cycle_through_a_dynamic_expansion_names_it(self, tmp_path) -> None:
        path = str(tmp_path / "out.txt")
        # write_after (node 0) waits on label (node 1), which then returns a
        # reader of write_after's output: label would wait on its own dependent.
        graph = TaskGraph.build(write_after(up=label(text="p"), output_path=path))

        with pytest.raises(CycleError, match=r"read \(returned by label\)"):
            graph.register(read(path=path), expanding_node_id=1)


class TestUnreachableCalls:
    def test_a_call_the_value_never_reaches_is_unreachable(self) -> None:
        with record_constructed_calls() as calls:
            label(text="dropped")
            kept = label(text="kept")
        graph = TaskGraph.build(kept)

        assert [call.label for call in graph.unreachable_calls(calls)] == ["label()"]
