"""#232 — a warm-cache dry run validates probe-resolved arguments.

Once an upstream task is cached, the probe holds the real ``AssetRef`` its
consumer would receive, so the run's own input validation can already prove a
kind/path mismatch. The plan collects each refusal as a diagnostic instead of
reporting the doomed task as a plain ``[will run]``.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from rich.console import Console

import ginkgo
from ginkgo import file, table, task
from ginkgo.cli.app import main as cli_main
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@task()
def produce_summary(output_path: str) -> object:
    """Write a CSV and return it as a ``table`` asset."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north", "south"], "count": [10, 20]}).to_csv(out, index=False)
    return table(out, name="summary")


@task()
def consume_as_file(summary: file) -> str:
    """A bare ``file`` parameter fed a table asset — always invalid."""
    return str(summary)


@task()
def consume_as_object(summary: object) -> str:
    """The valid consumer for the same asset."""
    return str(type(summary))


@task(kind="script")
def consume_in_script(summary: file, output_path: str) -> file:
    """A script consumer with the same invalid binding (#199's case)."""
    return ginkgo.script(path="analyse.py", output=output_path)


def _plan_for(build):
    """Build the dry-run plan for a flow body's expression."""
    with record_constructed_calls() as calls:
        expr = build()
    evaluator = ConcurrentEvaluator(constructed_calls=tuple(calls))
    evaluator.build_and_validate(expr)
    return build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")


class TestWarmCacheDryRunValidation:
    def test_a_cached_table_bound_to_file_is_a_diagnostic(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(produce_summary(output_path="results/summary.csv"))

        plan = _plan_for(
            lambda: consume_as_file(summary=produce_summary(output_path="results/summary.csv"))
        )

        assert len(plan.diagnostics) == 1
        diagnostic = plan.diagnostics[0]
        assert diagnostic.label.startswith("consume_as_file")
        assert "annotated `file` but is a `table` asset" in diagnostic.message
        statuses = {
            task.base_name: task.cache_status for wave in plan.waves for task in wave.tasks
        }
        assert statuses["produce_summary"] == "cached"
        assert statuses["consume_as_file"] == "will_run"

    def test_a_script_consumer_is_checked_before_its_source_hash_stops_the_probe(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Driver kinds probe as ``unknown``, but their inputs still validate."""
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(produce_summary(output_path="results/summary.csv"))

        plan = _plan_for(
            lambda: consume_in_script(
                summary=produce_summary(output_path="results/summary.csv"),
                output_path="out.txt",
            )
        )

        assert len(plan.diagnostics) == 1
        assert plan.diagnostics[0].label.startswith("consume_in_script")

    def test_a_cold_cache_proves_nothing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without the cached upstream, the consumer stays unknown — no claim."""
        monkeypatch.chdir(tmp_path)

        plan = _plan_for(
            lambda: consume_as_file(summary=produce_summary(output_path="results/summary.csv"))
        )

        assert plan.diagnostics == ()
        statuses = {
            task.base_name: task.cache_status for wave in plan.waves for task in wave.tasks
        }
        assert statuses["consume_as_file"] == "unknown"

    def test_a_valid_consumer_stays_clean(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(produce_summary(output_path="results/summary.csv"))

        plan = _plan_for(
            lambda: consume_as_object(summary=produce_summary(output_path="results/summary.csv"))
        )

        assert plan.diagnostics == ()

    def test_renderer_prints_a_problems_section(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        ginkgo.evaluate(produce_summary(output_path="results/summary.csv"))
        plan = _plan_for(
            lambda: consume_as_file(summary=produce_summary(output_path="results/summary.csv"))
        )

        console = Console(record=True, width=120)
        render_dry_run_plan(plan=plan, console=console, verbose=False)
        text = console.export_text()

        assert "Problems — this run would fail" in text
        assert "consume_as_file" in text
        assert "table" in text


SWITCHED_WORKFLOW = """
from pathlib import Path

import pandas as pd
from ginkgo import file, flow, param, table, task

mode = param("mode", default="produce")


@task()
def produce_summary(output_path: str) -> object:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north"], "count": [1]}).to_csv(out, index=False)
    return table(out, name="summary")


@task()
def consume_as_file(summary: file) -> str:
    return str(summary)


@flow
def main():
    summary = produce_summary(output_path="results/summary.csv")
    if mode == "produce":
        return summary
    return consume_as_file(summary=summary)
"""


class TestCliExitCode:
    def test_dry_run_exits_nonzero_on_a_provable_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """A preflight that proves the run would fail says so to the shell."""
        monkeypatch.chdir(tmp_path)
        Path("wf.py").write_text(SWITCHED_WORKFLOW.strip() + "\n", encoding="utf-8")

        assert cli_main(["run", "wf.py"]) == 0

        status = cli_main(["run", "wf.py", "--dry-run", "--mode", "full"])

        out = capsys.readouterr().out
        assert status == 1, out
        assert "Problems — this run would fail" in out

    def test_dry_run_exits_zero_on_a_cold_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        Path("wf.py").write_text(SWITCHED_WORKFLOW.strip() + "\n", encoding="utf-8")

        assert cli_main(["run", "wf.py", "--dry-run", "--mode", "full"]) == 0
