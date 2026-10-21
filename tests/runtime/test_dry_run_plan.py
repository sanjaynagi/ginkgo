"""#343 — the dry-run plan answers what it can know.

A consumer wired to a cached producer through ``.output["name"]`` has every
input it needs, so the probe must determine its status rather than report a
probe failure. The resource summary must not present summed thread
declarations as a core count the run would actually use.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

import ginkgo
from ginkgo import Out, file, shell, task
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
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


@task(threads=4)
def busy(i: int) -> int:
    """Declare more threads than one core."""
    return i


@task("shell")
def make_shell(out: Out[file]):
    """Write one declared output from a shell command."""
    return shell(cmd=f"mkdir -p out && echo made > {out}")


@task()
def fan_out(n: int) -> list[int]:
    """Return one ``busy`` call per item: a dynamic fan-out."""
    return [busy(i=i) for i in range(n)]


def _shell_flow():
    return use(p=make_shell(out="out/s.txt"))


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
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(expr)
    evaluator.validate_graph()
    return build_dry_run_plan(
        evaluator=evaluator,
        workflow_label="workflow.py",
        unreachable_calls=evaluator.graph.unreachable_calls(calls),
    )


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

    def test_a_rewritten_upstream_output_does_not_predict_a_rerun(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The run restores a cached shell output before the consumer reads it,
        # so the consumer is a cache hit whatever the file now holds.
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(_shell_flow())
        Path("out/s.txt").write_text("rewritten")

        plan = _plan_for(_shell_flow)

        assert _statuses(plan) == {"make_shell": "cached", "use": "cached"}

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


class TestResourceSummaryLabel:
    def test_peak_threads_are_labelled_as_requested_threads(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Summed declarations are demand, not cores the run will occupy."""
        monkeypatch.chdir(tmp_path)
        plan = _plan_for(lambda: busy().map(i=[1, 2, 3]))

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=False)
        text = console.export_text()

        assert "12 threads requested at peak (wave 1)" in text
        assert "cores peak" not in text

    def test_tasks_declaring_no_threads_are_counted_as_tasks_at_one_thread_each(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        for name in ("a.txt", "b.txt", "c.txt"):
            Path(name).write_text(name)
        plan = _plan_for(lambda: use().map(p=["a.txt", "b.txt", "c.txt"]))

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=False)
        text = console.export_text()

        assert "up to 3 tasks at once, one thread each (wave 1)" in text
        assert "threads requested" not in text


class TestDynamicFanOut:
    def test_a_task_that_returns_task_calls_is_named_as_adding_tasks_at_run_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        plan = _plan_for(lambda: fan_out(n=3))

        console = Console(record=True, width=160)
        render_dry_run_plan(plan=plan, console=console, verbose=False)
        text = console.export_text()

        assert plan.fan_out_labels == ("fan_out",)
        assert "fan_out calls tasks in its body" in text

    def test_a_task_that_calls_no_task_is_not_named(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)

        assert _plan_for(_single_flow).fan_out_labels == ()
