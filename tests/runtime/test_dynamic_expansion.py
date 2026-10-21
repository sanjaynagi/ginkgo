"""Tests for the expressions a dynamic task returns."""

from __future__ import annotations

from pathlib import Path

from ginkgo import Out, evaluate, file, task
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
