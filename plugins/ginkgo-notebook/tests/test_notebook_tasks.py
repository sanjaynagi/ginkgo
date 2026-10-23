"""Marimo notebook tasks, run end to end with marimo."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, file, task
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.rundir import RunDir, make_run_id
from ginkgo.runtime.events import EventBus, TaskAnnotated
from ginkgo.runtime.task_runners.shell import classify_failure
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


@task("notebook")
def render_jupyter() -> file:
    return notebook("report.ipynb")


def _evaluate(expr: object, *, root: Path) -> tuple[object, RunDir, dict[str, object]]:
    """Run *expr* once; return its result, run directory and annotated fields."""
    run_dir = RunDir.create(run_id=make_run_id(workflow_path=root / "flow.py"), root=root / "runs")
    bus = EventBus()
    fields: dict[str, object] = {}
    bus.subscribe(lambda event: isinstance(event, TaskAnnotated) and fields.update(event.fields))
    result = ConcurrentEvaluator(run_dir=run_dir, event_bus=bus, jobs=1, cores=1).evaluate(expr)
    return result, run_dir, fields


def test_a_notebook_renders_its_page_and_leaves_the_source_untouched(
    tmp_path: Path, notebook_path: Path
) -> None:
    before = notebook_path.read_bytes()

    result, run_dir, fields = _evaluate(
        render(notebook_path=str(notebook_path), sample_id="s1"), root=tmp_path
    )

    html = run_dir.path / "notebooks" / "task_0000.html"
    assert Path(str(result)) == html
    assert "sample" in html.read_text(encoding="utf-8")
    assert fields == {
        "rendered_html": "notebooks/task_0000.html",
        "rendered_html_run_id": run_dir.run_id,
    }
    assert notebook_path.read_bytes() == before


def test_a_cache_hit_points_at_the_producing_page_until_the_notebook_is_edited(
    tmp_path: Path, notebook_path: Path
) -> None:
    expr = render(notebook_path=str(notebook_path), sample_id="s1")
    _, first_run, _ = _evaluate(expr, root=tmp_path)

    _, _, replayed = _evaluate(expr, root=tmp_path)
    assert replayed["rendered_html"] == str(
        (first_run.path / "notebooks" / "task_0000.html").resolve()
    )
    assert replayed["rendered_html_run_id"] == first_run.run_id

    notebook_path.write_text(_NOTEBOOK + "\n# edited\n", encoding="utf-8")
    _, third_run, rerun = _evaluate(
        render(notebook_path=str(notebook_path), sample_id="s1"), root=tmp_path
    )
    assert rerun["rendered_html_run_id"] == third_run.run_id


def test_a_failing_cell_fails_the_task_and_points_at_the_page(
    tmp_path: Path, notebook_path: Path
) -> None:
    with pytest.raises(NotebookTaskError, match="shows the failing cell") as raised:
        _evaluate(render_failing(notebook_path=str(notebook_path), fail=True), root=tmp_path)

    assert classify_failure(exc=raised.value)["kind"] == "user_code_error"


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


@pytest.mark.parametrize(
    ("name", "source", "error", "match"),
    [
        ("report.ipynb", "{}", ValueError, "marimo notebooks, which are .py files"),
        ("broken.py", "import marimo\napp = marimo.App(\n", SyntaxError, None),
        ("missing.py", None, FileNotFoundError, "Notebook source not found"),
    ],
)
def test_notebook_refuses_what_it_cannot_run(
    tmp_path: Path, name: str, source: str | None, error: type[Exception], match: str | None
) -> None:
    if source is not None:
        (tmp_path / name).write_text(source, encoding="utf-8")

    with pytest.raises(error, match=match):
        notebook(tmp_path / name)


def test_a_dry_run_reports_a_jupyter_notebook(tmp_path: Path) -> None:
    (tmp_path / "report.ipynb").write_text("{}", encoding="utf-8")
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(render_jupyter())
    evaluator.validate_graph()

    plan = build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")

    [diagnostic] = plan.diagnostics
    assert "Jupyter .ipynb notebooks are not supported" in diagnostic.message
