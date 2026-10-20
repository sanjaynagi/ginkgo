"""Marimo notebook tasks, run end to end with marimo."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, file, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.rundir import RunDir, make_run_id
from ginkgo.runtime.events import EventBus, TaskAnnotated
from ginkgo_notebook import notebook
from ginkgo_notebook.runner import NotebookTaskError

_NOTEBOOK = """
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo

    args = mo.cli_args()
    return (args,)


@app.cell
def _(args):
    from pathlib import Path

    if args.get("fail"):
        raise ValueError("deliberate failure in a cell")
    if args.get("output-path"):
        Path(args.get("output-path")).write_text(str(args.get("sample-id")), encoding="utf-8")
    print("sample", args.get("sample-id"))
    return


if __name__ == "__main__":
    app.run()
"""


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The cache and rendered pages live under the working directory.
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def notebook_path(tmp_path: Path) -> Path:
    path = tmp_path / "report.py"
    path.write_text(_NOTEBOOK, encoding="utf-8")
    return path


@task("notebook")
def render(notebook_path: str, sample_id: str) -> file:
    return notebook(notebook_path)


@task("notebook")
def render_failing(notebook_path: str, fail: bool) -> file:
    return notebook(notebook_path)


@task("notebook")
def render_with_output(notebook_path: str, sample_id: str, output_path: Out[file]) -> file:
    return notebook(notebook_path, output=output_path)


def _evaluate(expr: object, *, root: Path) -> tuple[object, RunDir, list[TaskAnnotated]]:
    run_dir = RunDir.create(run_id=make_run_id(workflow_path=root / "flow.py"), root=root / "runs")
    bus = EventBus()
    annotated: list[TaskAnnotated] = []
    bus.subscribe(
        lambda event: annotated.append(event) if isinstance(event, TaskAnnotated) else None
    )
    result = ConcurrentEvaluator(run_dir=run_dir, event_bus=bus, jobs=1, cores=1).evaluate(expr)
    return result, run_dir, annotated


def _rendered(annotated: list[TaskAnnotated]) -> dict[str, object]:
    fields: dict[str, object] = {}
    for event in annotated:
        fields.update(event.fields)
    return fields


def test_a_notebook_runs_with_the_task_arguments_and_returns_its_page(
    tmp_path: Path, notebook_path: Path
) -> None:
    result, run_dir, annotated = _evaluate(
        render(notebook_path=str(notebook_path), sample_id="s1"), root=tmp_path
    )

    html = run_dir.path / "notebooks" / "task_0000.html"
    assert Path(str(result)) == html
    assert "sample" in html.read_text(encoding="utf-8")
    assert _rendered(annotated) == {
        "rendered_html": "notebooks/task_0000.html",
        "rendered_html_run_id": run_dir.run_id,
    }


def test_a_cache_hit_points_at_the_page_the_producing_run_rendered(
    tmp_path: Path, notebook_path: Path
) -> None:
    expr = render(notebook_path=str(notebook_path), sample_id="s1")
    _, first_run, _ = _evaluate(expr, root=tmp_path)

    _, _, annotated = _evaluate(expr, root=tmp_path)

    replayed = _rendered(annotated)
    assert replayed["rendered_html"] == str(
        (first_run.path / "notebooks" / "task_0000.html").resolve()
    )
    assert replayed["rendered_html_run_id"] == first_run.run_id


def test_editing_the_notebook_reruns_the_task(tmp_path: Path, notebook_path: Path) -> None:
    expr = render(notebook_path=str(notebook_path), sample_id="s1")
    _evaluate(expr, root=tmp_path)

    notebook_path.write_text(_NOTEBOOK + "\n# edited\n", encoding="utf-8")
    _, second_run, annotated = _evaluate(
        render(notebook_path=str(notebook_path), sample_id="s1"), root=tmp_path
    )

    assert _rendered(annotated)["rendered_html_run_id"] == second_run.run_id


def test_a_failing_cell_fails_the_task_and_points_at_the_page(
    tmp_path: Path, notebook_path: Path
) -> None:
    with pytest.raises(NotebookTaskError, match="shows the failing cell"):
        _evaluate(render_failing(notebook_path=str(notebook_path), fail=True), root=tmp_path)


def test_a_declared_output_is_the_result(tmp_path: Path, notebook_path: Path) -> None:
    output = tmp_path / "results" / "sample.txt"

    result, _, _ = _evaluate(
        render_with_output(
            notebook_path=str(notebook_path), sample_id="s7", output_path=str(output)
        ),
        root=tmp_path,
    )

    assert Path(str(result)) == output
    assert output.read_text(encoding="utf-8") == "s7"


def test_notebook_runs_marimo_notebooks_only(tmp_path: Path) -> None:
    ipynb = tmp_path / "report.ipynb"
    ipynb.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="marimo notebooks, which are .py files"):
        notebook(ipynb)


def test_the_users_notebook_is_left_byte_identical(tmp_path: Path, notebook_path: Path) -> None:
    before = notebook_path.read_bytes()

    _evaluate(render(notebook_path=str(notebook_path), sample_id="s1"), root=tmp_path)

    assert notebook_path.read_bytes() == before


def test_a_notebook_with_a_syntax_error_is_refused(tmp_path: Path) -> None:
    broken = tmp_path / "broken.py"
    broken.write_text("import marimo\napp = marimo.App(\n", encoding="utf-8")

    with pytest.raises(SyntaxError):
        notebook(broken)


def test_a_missing_notebook_is_refused(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Notebook source not found"):
        notebook(tmp_path / "missing.py")
