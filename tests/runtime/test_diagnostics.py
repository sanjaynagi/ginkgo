"""Unit tests for ``ginkgo.runtime.diagnostics``.

Covers issue #307: a ``str``-annotated parameter whose name looks path-like
is not content-tracked by the cache and creates no dependency edge. Also covers
the warning for a driver task whose body can never return its directive.
"""

from __future__ import annotations

from ginkgo import Out, file, shell, task, untracked
import pytest

from ginkgo.runtime.diagnostics import (
    MISSING_DIRECTIVE_CODE,
    PATH_LIKE_STR_PARAM_CODE,
    SPACED_COMMAND_PATH_CODE,
    _diagnostic_code_and_suggestion,
    missing_directive_diagnostics,
    path_like_str_param_diagnostics,
    spaced_command_path_diagnostics,
    task_call_diagnostics,
)
from ginkgo.runtime.graph import TaskGraph
from ginkgo.runtime.placement import Placement
from ginkgo.runtime.task_validation import TaskValidator
from tests.conftest import run_cli


@task()
def greet(name: str, output_path: str) -> str:
    return f"hello {name} -> {output_path}"


@task()
def tidy(threshold: int, input_dir: str) -> str:
    return f"{threshold} {input_dir}"


@task()
def deliberate(output_path: untracked) -> str:
    return output_path


@task()
def produce(name: str) -> str:
    return f"{name}.txt"


@task()
def write_named(name: str, out: Out[file]) -> file:
    return out


@task(kind="shell")
def copy_named(src: file, out: Out[file]):
    return shell(cmd=f"cp {src} {out}")


def _nodes_for(build):
    """Build a graph and return every node it registers."""
    expr = build()
    graph = TaskGraph.build(expr)
    graph.validate(validator=TaskValidator(), placement=Placement())
    return list(graph.nodes.values())


class TestSpacedCommandPathDiagnostics:
    """ED5-8: an upstream path with a space, bound for a shell command."""

    def test_warns_before_the_run_reaches_the_shell_task(self) -> None:
        nodes = _nodes_for(
            lambda: copy_named(src=write_named(name="a", out="o/sp ace.txt"), out="o/b.txt")
        )

        [diagnostic] = spaced_command_path_diagnostics(nodes=nodes)

        assert diagnostic.code == SPACED_COMMAND_PATH_CODE
        assert "copy_named is a shell task reading the result of write_named" in diagnostic.message
        assert "'o/sp ace.txt'" in diagnostic.message

    def test_no_warning_without_a_space(self) -> None:
        nodes = _nodes_for(
            lambda: copy_named(src=write_named(name="a", out="o/space.txt"), out="o/b.txt")
        )

        assert spaced_command_path_diagnostics(nodes=nodes) == []


class TestPathLikeStrParamDiagnostics:
    def test_warns_once_per_path_like_str_parameter(self) -> None:
        nodes = _nodes_for(lambda: greet(name="alice", output_path="results/out.txt"))

        [diagnostic] = path_like_str_param_diagnostics(nodes=nodes)

        assert diagnostic.code == PATH_LIKE_STR_PARAM_CODE
        assert diagnostic.severity == "warning"
        for phrase in ("output_path", "greet", "Out[file]", "folder"):
            assert phrase in diagnostic.message

    def test_names_only_the_path_like_parameter(self) -> None:
        nodes = _nodes_for(lambda: tidy(threshold=1, input_dir="data/raw"))

        [diagnostic] = path_like_str_param_diagnostics(nodes=nodes)

        assert "input_dir" in diagnostic.message
        assert "threshold" not in diagnostic.message

    @pytest.mark.parametrize(
        ("build", "count"),
        [
            # Two calls to the same task warn once, not once per branch.
            (lambda: [greet(name="a", output_path="a.txt"), greet(name="b", output_path="b")], 1),
            # The producer's annotation decides tracking, so the advice would be wrong.
            (lambda: greet(name="alice", output_path=produce(name="a")), 0),
            (
                lambda: [
                    greet(name="alice", output_path=produce(name="a")),
                    greet(name="bob", output_path="b.txt"),
                ],
                1,
            ),
            # ``untracked`` is a deliberate opt-out (#307 phase 2), not the trap.
            (lambda: deliberate(output_path="results/out.txt"), 0),
        ],
        ids=["dedupe-by-task", "upstream-result", "any-literal", "untracked"],
    )
    def test_warning_count(self, build, count: int) -> None:
        assert len(path_like_str_param_diagnostics(nodes=_nodes_for(build))) == count


_PATH_LIKE_WORKFLOW = """
from ginkgo import flow, task

@task()
def write_report(output_path: str) -> str:
    return output_path

@flow
def main():
    return write_report(output_path="report.txt")
"""


def test_doctor_reports_path_like_str_param_and_stays_ok(tmp_path) -> None:
    from ginkgo.runtime.diagnostics import collect_workflow_diagnostics

    workflow_path = tmp_path / "workflow.py"
    workflow_path.write_text(_PATH_LIKE_WORKFLOW, encoding="utf-8")

    diagnostics = collect_workflow_diagnostics(
        workflow_path=workflow_path, config_paths=[], secret_resolver=None
    ).diagnostics

    [match] = [d for d in diagnostics if d.code == PATH_LIKE_STR_PARAM_CODE]
    assert match.severity == "warning"
    assert "output_path" in match.message
    # Nothing here is an error: doctor would still report success.
    assert all(d.severity != "error" for d in diagnostics)


def test_only_a_dry_run_prints_the_path_like_str_param_warning(tmp_path) -> None:
    """``run --dry-run`` prints it (issue #307); a real run would repeat it every time."""
    (tmp_path / "wf.py").write_text(_PATH_LIKE_WORKFLOW, encoding="utf-8")

    dry = run_cli("run", "wf.py", "--dry-run")
    assert dry.returncode == 0
    assert "write_report" in dry.stderr and "output_path" in dry.stderr
    # Escaped for Rich, which would otherwise swallow `[file]` as a tag.
    assert "Out[file]" in dry.stderr and "Out[folder]" in dry.stderr
    # A single-file workflow's synthetic module name is not shown.
    assert "ginkgo_user_" not in dry.stderr

    real = run_cli("run", "wf.py")
    assert real.returncode == 0
    assert "output_path" not in real.stderr


@task("shell")
def echo_as_string(name: str) -> str:
    if not name:
        return "true"
    return f"echo {name}"


@task("shell")
def echo_as_shell(name: str) -> str:
    return shell(cmd=f"echo {name}")


@task("shell")
def echo_through_a_name(name: str) -> str:
    directive = shell(cmd=f"echo {name}")
    return directive


class TestMissingDirectiveDiagnostics:
    """F8: a driver task that can never return its directive is flagged early."""

    def test_warns_when_every_return_is_a_plain_value(self) -> None:
        diagnostics = missing_directive_diagnostics(task_defs=[echo_as_string, echo_as_string])

        assert len(diagnostics) == 1
        assert diagnostics[0].code == MISSING_DIRECTIVE_CODE
        assert diagnostics[0].severity == "warning"
        assert '@task("shell")' in diagnostics[0].message
        assert "shell(...)" in diagnostics[0].message

    def test_never_warns_when_a_return_might_be_the_directive(self) -> None:
        task_defs = [echo_as_shell, echo_through_a_name, greet]

        assert missing_directive_diagnostics(task_defs=task_defs) == []

    def test_dry_run_prints_it(self, tmp_path) -> None:
        (tmp_path / "wf.py").write_text(
            """
from ginkgo import flow, task

@task("shell")
def count_lines(path: str) -> str:
    return f"wc -l {path}"

@flow
def main():
    return count_lines(path="x")
""",
            encoding="utf-8",
        )

        result = run_cli("run", "wf.py", "--dry-run")

        assert result.returncode == 0
        assert 'count_lines is declared @task("shell")' in result.stderr


@pytest.mark.parametrize(
    ("exc", "code"),
    [
        (FileNotFoundError("gone"), "file_not_found_error"),
        (OSError("disk"), "os_error"),
        (ValueError("bad"), "invalid_value"),
        (RuntimeError("Missing secrets: TOKEN"), "missing_secret"),
    ],
)
def test_every_diagnostic_code_is_snake_case(exc: Exception, code: str) -> None:
    assert _diagnostic_code_and_suggestion(exc=exc, message=str(exc))[0] == code


def _with_an_empty_cell():
    """Return a task whose closure cell is never filled."""

    @task()
    def reads_later(x: int) -> int:
        return later(x)

    return reads_later
    later = greet  # noqa: F841 - never reached, so the cell stays empty


def test_task_call_check_skips_tasks_it_cannot_read() -> None:
    """An empty closure cell or a lambda task must not abort doctor or run."""
    lambda_task = task()(lambda x: greet(x))
    assert task_call_diagnostics(task_defs=[_with_an_empty_cell(), lambda_task]) == []
