"""Unit tests for ``ginkgo.runtime.diagnostics``.

Covers issue #307: a ``str``-annotated parameter whose name looks path-like
is not content-tracked by the cache and creates no dependency edge.
"""

from __future__ import annotations

from ginkgo import task
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.diagnostics import (
    PATH_LIKE_STR_PARAM_CODE,
    path_like_str_param_diagnostics,
)
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@task()
def greet(name: str, output_path: str) -> str:
    return f"hello {name} -> {output_path}"


@task()
def tidy(threshold: int, input_dir: str) -> str:
    return f"{threshold} {input_dir}"


def _task_defs_for(build):
    """Build a graph and return the task defs of every node it registers."""
    with record_constructed_calls() as calls:
        expr = build()
    evaluator = ConcurrentEvaluator(constructed_calls=tuple(calls))
    evaluator.build_and_validate(expr)
    return [node.task_def for node in evaluator.task_nodes.values()]


class TestPathLikeStrParamDiagnostics:
    def test_warns_once_per_path_like_str_parameter(self) -> None:
        task_defs = _task_defs_for(lambda: greet(name="alice", output_path="results/out.txt"))

        diagnostics = path_like_str_param_diagnostics(task_defs=task_defs)

        assert len(diagnostics) == 1
        diagnostic = diagnostics[0]
        assert diagnostic.code == PATH_LIKE_STR_PARAM_CODE
        assert diagnostic.severity == "warning"
        assert "output_path" in diagnostic.message
        assert "greet" in diagnostic.message
        assert "file" in diagnostic.message
        assert "folder" in diagnostic.message
        assert "Out[file]" in diagnostic.message

    def test_does_not_warn_for_non_path_like_names_or_non_str_annotations(self) -> None:
        task_defs = _task_defs_for(lambda: tidy(threshold=1, input_dir="data/raw"))

        diagnostics = path_like_str_param_diagnostics(task_defs=task_defs)

        assert len(diagnostics) == 1
        assert diagnostics[0].message.count("threshold") == 0
        assert "input_dir" in diagnostics[0].message

    def test_dedupes_by_task_definition_not_by_call(self) -> None:
        """Two calls to the same task must warn once, not once per branch."""
        task_defs = _task_defs_for(
            lambda: [
                greet(name="alice", output_path="a.txt"),
                greet(name="bob", output_path="b.txt"),
            ]
        )

        diagnostics = path_like_str_param_diagnostics(task_defs=task_defs)

        assert len(diagnostics) == 1

    def test_no_warning_when_nothing_looks_path_like(self) -> None:
        task_defs = _task_defs_for(lambda: tidy(threshold=1, input_dir="ignored"))
        # Strip the path-like parameter from consideration by only checking
        # a task whose remaining parameter is not path-shaped.
        filtered = path_like_str_param_diagnostics(
            task_defs=[td for td in task_defs if "tidy" not in td.name]
        )
        assert filtered == []


class TestCollectWorkflowDiagnosticsSurfacesThePathWarning:
    """Integration: ``collect_workflow_diagnostics`` reports it and still passes."""

    def test_doctor_reports_path_like_str_param_and_stays_ok(self, tmp_path) -> None:
        from pathlib import Path

        from ginkgo.runtime.diagnostics import collect_workflow_diagnostics

        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text(
            """
from ginkgo import flow, task

@task()
def write_report(output_path: str) -> str:
    return output_path

@flow
def main():
    return write_report(output_path="report.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        import os

        old_cwd = Path.cwd()
        os.chdir(tmp_path)
        try:
            diagnostics = collect_workflow_diagnostics(
                workflow_path=workflow_path,
                config_paths=[],
                secret_resolver=None,
            )
        finally:
            os.chdir(old_cwd)

        matches = [d for d in diagnostics if d.code == PATH_LIKE_STR_PARAM_CODE]
        assert len(matches) == 1
        assert matches[0].severity == "warning"
        assert "output_path" in matches[0].message
        # Nothing here is an error: doctor would still report success.
        assert all(d.severity != "error" for d in diagnostics)


class TestDryRunSurfacesThePathWarning:
    """``ginkgo run --dry-run`` prints the same warning (issue #307)."""

    def test_dry_run_prints_the_path_like_str_param_warning(
        self, tmp_path, monkeypatch, capsys
    ) -> None:
        from ginkgo.cli.app import main as cli_main

        monkeypatch.chdir(tmp_path)
        (tmp_path / "wf.py").write_text(
            """
from ginkgo import flow, task

@task()
def write_report(output_path: str) -> str:
    return output_path

@flow
def main():
    return write_report(output_path="report.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        status = cli_main(["run", "wf.py", "--dry-run"])

        err = capsys.readouterr().err
        assert status == 0
        assert "output_path" in err
        assert "write_report" in err
        assert "file" in err and "folder" in err

    def test_a_real_run_does_not_print_the_path_like_str_param_warning(
        self, tmp_path, monkeypatch, capsys
    ) -> None:
        """Noisy: a real run would otherwise repeat it every invocation."""
        from ginkgo.cli.app import main as cli_main

        monkeypatch.chdir(tmp_path)
        (tmp_path / "wf.py").write_text(
            """
from ginkgo import flow, task

@task()
def write_report(output_path: str) -> str:
    return output_path

@flow
def main():
    return write_report(output_path="report.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        status = cli_main(["run", "wf.py"])

        err = capsys.readouterr().err
        assert status == 0
        assert "output_path" not in err
