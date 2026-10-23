"""Tests for the expressions a dynamic task returns."""

from __future__ import annotations

from pathlib import Path

from ginkgo import Out, evaluate, file, folder, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from tests.conftest import EventCollector


@task()
def child(*, name: str, out: Out[file]) -> file:
    Path(out).write_text(name, encoding="utf-8")
    return file(out)


@task()
def merge(*, parts: list[file], merged: Out[file]) -> file:
    Path(merged).write_text("".join(Path(p).read_text() for p in parts), encoding="utf-8")
    return file(merged)


@task()
def merged_and_parts(*, k: int) -> list:
    calls = [child(name=f"c{i}", out=f"out/{i}.txt") for i in range(k)]
    return [merge(parts=calls, merged="out/merged.txt"), calls]


def test_a_returned_template_can_use_one_child_twice() -> None:
    collector = EventCollector()

    merged, parts = evaluate(merged_and_parts(k=2), event_bus=collector.bus)

    assert Path(merged).read_text() == "c0c1"
    assert parts == ["out/0.txt", "out/1.txt"]
    assert len(collector.started()) == 4  # parent, merge, and each child once


@task()
def score(*, name: str) -> str:
    return name.upper()


@task()
def score_all(*, names: list[str]) -> list:
    return [score(name=name) for name in names]


def _started_labels(collector: EventCollector) -> list[str]:
    return sorted(event.display_label or "" for event in collector.started())


def test_returned_calls_are_labelled_and_rerun_by_the_argument_that_differs() -> None:
    first, rerun = EventCollector(), EventCollector()

    evaluate(score_all(names=["ann", "bo"]), event_bus=first.bus)
    ConcurrentEvaluator(rerun=frozenset({"score[bo]"}), event_bus=rerun.bus).evaluate(
        score_all(names=["ann", "bo"])
    )

    assert _started_labels(first) == ["", "score[ann]", "score[bo]"]
    assert _started_labels(rerun) == ["score[bo]"]


@task()
def read_chunk(*, chunk: file) -> str:
    return Path(chunk).read_text(encoding="utf-8")


@task()
def split(*, n: int, chunks: Out[folder]) -> list:
    Path(chunks).mkdir()
    for i in range(n):
        (Path(chunks) / f"{i}.txt").write_text(str(i), encoding="utf-8")
    return [read_chunk(chunk=str(path)) for path in sorted(Path(chunks).iterdir())]


def test_a_cached_dynamic_parent_restores_its_out_folder() -> None:
    evaluate(split(n=3, chunks="chunks"))
    evaluate(split(n=1, chunks="chunks"))

    # A hit on the n=3 entry: the folder holds the n=1 run's one chunk until
    # the hit restores the three its children read.
    assert evaluate(split(n=3, chunks="chunks")) == ["0", "1", "2"]
