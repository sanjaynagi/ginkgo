"""#343 — the dry-run plan answers what it can know.

A consumer wired to a cached producer through ``.output["name"]`` has every
input it needs, so the probe must determine its status rather than report a
probe failure.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo
from ginkgo import Out, file, task
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@task()
def make(n: int, data: Out[file], log: Out[file]) -> int:
    """Write two declared outputs."""
    Path(data).parent.mkdir(parents=True, exist_ok=True)
    Path(data).write_text(str(n))
    Path(log).write_text("ok")
    return n


@task()
def use(p: file) -> int:
    """Read one file."""
    return len(Path(p).read_text())


@task()
def collect(logs: list[file]) -> int:
    """Read a list of files."""
    return sum(len(Path(p).read_text()) for p in logs)


def _single_flow():
    made = make(n=1, data="out/d.txt", log="out/l.txt")
    return use(p=made.output["data"])


def _mapped_flow():
    runs = make().map(
        n=[1, 2],
        data=["out/d1.txt", "out/d2.txt"],
        log=["out/l1.txt", "out/l2.txt"],
    )
    return collect(logs=runs.output["log"])


def _plan_for(build):
    """Build the dry-run plan for a flow body's expression."""
    with record_constructed_calls() as calls:
        expr = build()
    evaluator = ConcurrentEvaluator(constructed_calls=tuple(calls))
    evaluator.build_and_validate(expr)
    return build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")


def _statuses(plan) -> dict[str, str]:
    return {task.base_name: task.cache_status for wave in plan.waves for task in wave.tasks}


class TestNamedOutputConsumers:
    def test_consumer_of_a_cached_producer_is_probed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(_single_flow())

        plan = _plan_for(_single_flow)

        assert plan.probe_failures == ()
        assert _statuses(plan) == {"make": "cached", "use": "cached"}

    def test_consumer_of_a_cached_mapped_producer_is_probed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(_mapped_flow())

        plan = _plan_for(_mapped_flow)

        assert plan.probe_failures == ()
        assert _statuses(plan) == {"make": "cached", "collect": "cached"}

    def test_consumer_of_a_cached_producer_with_new_inputs_will_run(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(make(n=1, data="out/d.txt", log="out/l.txt"))

        plan = _plan_for(_single_flow)

        assert plan.probe_failures == ()
        assert _statuses(plan) == {"make": "cached", "use": "will_run"}
