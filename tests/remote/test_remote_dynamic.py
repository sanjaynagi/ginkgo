"""Tests for a remote task that returns task expressions (a dynamic task)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from ginkgo import file, task
from tests.conftest import EventCollector
from tests.remote_fakes import evaluate_remotely


pytestmark = pytest.mark.usefixtures("remote_round_trip")


@task()
def double(x: int) -> int:
    return x * 2


@task()
def total(xs: list[int]) -> int:
    return sum(xs)


@task()
def read_text(path: file) -> str:
    return Path(path).read_text(encoding="utf-8")


@task()
def expand(x: int):
    return double(x=x)


@task()
def expand_annotated(x: int) -> int:
    return double(x=x)


@task()
def expand_total_and_parts(n: int) -> list:
    calls = [double(x=i) for i in range(n)]
    return [total(xs=calls), calls]


@task()
def write_then_read(text: str):
    # The driver's temporary directory is hidden from the pod, and this one
    # is removed once the job ends, so the child reads what the store holds.
    path = Path(tempfile.mkdtemp()) / "written.txt"
    path.write_text(text, encoding="utf-8")
    return read_text(path=file(path))


def test_a_returned_expression_expands_on_the_driver() -> None:
    assert evaluate_remotely(expand(x=3)) == 6


def test_the_expanded_value_meets_the_return_annotation() -> None:
    assert evaluate_remotely(expand_annotated(x=3)) == 6


def test_a_list_of_expressions_registers_each_call_once() -> None:
    collector = EventCollector()

    result = evaluate_remotely(expand_total_and_parts(n=3), event_bus=collector.bus)

    assert result == [6, [0, 2, 4]]
    assert len(collector.started()) == 5  # the parent, total, and each double once


def test_a_child_reads_a_file_the_remote_task_wrote() -> None:
    assert evaluate_remotely(write_then_read(text="from the pod")) == "from the pod"
