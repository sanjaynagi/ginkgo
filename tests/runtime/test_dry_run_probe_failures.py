"""#294 — a dry-run probe failure is reported, not swallowed.

``[unknown]`` is the honest answer in several cases, so a probe that fails and
silently returns ``[unknown]`` is indistinguishable from one that worked. Every
unexpected probe error is recorded against the task it was probing; the errors
that genuinely mean "cannot know without executing" stay quiet.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

import ginkgo
from ginkgo import file, task
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@task()
def produce_file(output_path: str) -> file:
    """Write a plain file output."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("payload")
    return output_path


@task()
def consume_path(src: file) -> str:
    """A valid file consumer."""
    return Path(src).read_text()


def _plan_for(build):
    """Build the dry-run plan for a flow body's expression."""
    with record_constructed_calls() as calls:
        expr = build()
    evaluator = ConcurrentEvaluator(constructed_calls=tuple(calls))
    evaluator.build_and_validate(expr)
    return build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")


class TestProbeFailuresAreReported:
    def test_an_unexpected_argument_resolution_error_is_recorded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A defect in argument resolution names itself instead of hiding."""
        monkeypatch.chdir(tmp_path)

        def boom(self, *, node):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(ConcurrentEvaluator, "resolve_probe_args", boom)

        plan = _plan_for(lambda: produce_file(output_path="results/out.txt"))

        assert len(plan.probe_failures) == 1
        failure = plan.probe_failures[0]
        assert failure.label.startswith("produce_file")
        assert failure.task_name.endswith("produce_file")
        assert failure.stage == "resolve_args"
        assert failure.exception_type == "RuntimeError"
        assert failure.message == "resolver exploded"

    def test_an_unexpected_cache_key_error_is_recorded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The second probe step reports on the same terms as the first."""
        monkeypatch.chdir(tmp_path)

        def boom(self, **kwargs):
            raise KeyError("missing_input")

        monkeypatch.setattr(CacheStore, "build_cache_key", boom)

        plan = _plan_for(lambda: produce_file(output_path="results/out.txt"))

        assert len(plan.probe_failures) == 1
        failure = plan.probe_failures[0]
        assert failure.stage == "cache_key"
        assert failure.exception_type == "KeyError"
        assert "missing_input" in failure.message

    def test_a_probe_failure_still_degrades_to_unknown(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dry run of a workflow that would run fine must not crash."""
        monkeypatch.chdir(tmp_path)

        def boom(self, *, node):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(ConcurrentEvaluator, "resolve_probe_args", boom)

        plan = _plan_for(lambda: produce_file(output_path="results/out.txt"))

        statuses = {
            task.base_name: task.cache_status for wave in plan.waves for task in wave.tasks
        }
        assert statuses["produce_file"] == "unknown"

    def test_a_probe_failure_is_not_a_workflow_problem(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Probe failures stay out of ``diagnostics``, which fail the command."""
        monkeypatch.chdir(tmp_path)

        def boom(self, *, node):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(ConcurrentEvaluator, "resolve_probe_args", boom)

        plan = _plan_for(lambda: produce_file(output_path="results/out.txt"))

        assert plan.diagnostics == ()
        assert plan.probe_failures != ()

    def test_an_honest_unknown_is_not_reported_as_a_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A deleted-but-restorable output is a "cannot know", not a defect."""
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(consume_path(src=produce_file(output_path="results/out.txt")))
        Path("results/out.txt").unlink()

        plan = _plan_for(lambda: consume_path(src=produce_file(output_path="results/out.txt")))

        assert plan.probe_failures == ()

    def test_a_clean_probe_records_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(consume_path(src=produce_file(output_path="results/out.txt")))

        plan = _plan_for(lambda: consume_path(src=produce_file(output_path="results/out.txt")))

        assert plan.probe_failures == ()


class TestProbeFailureRendering:
    def _plan_with_failure(self, monkeypatch: pytest.MonkeyPatch):
        def boom(self, *, node):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(ConcurrentEvaluator, "resolve_probe_args", boom)
        return _plan_for(lambda: produce_file(output_path="results/out.txt"))

    def test_the_default_output_says_how_many_probes_failed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without --verbose the user still learns the probe gave up."""
        monkeypatch.chdir(tmp_path)
        plan = self._plan_with_failure(monkeypatch)

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=False)
        text = console.export_text()

        assert "Cache status could not be determined for 1 task" in text
        assert "resolver exploded" not in text
        assert "Problems" not in text

    def test_verbose_names_the_task_and_the_exception(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        plan = self._plan_with_failure(monkeypatch)

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=True)
        text = console.export_text()

        assert "produce_file" in text
        assert "RuntimeError: resolver exploded" in text
        assert "resolve_args" in text

    def test_a_clean_plan_prints_no_probe_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        plan = _plan_for(lambda: produce_file(output_path="results/out.txt"))

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=True)

        assert "Cache status could not be determined" not in console.export_text()
