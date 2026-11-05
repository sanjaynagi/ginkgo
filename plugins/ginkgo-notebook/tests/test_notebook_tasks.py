"""Marimo notebook tasks, run end to end with marimo."""

from __future__ import annotations

import subprocess
from dataclasses import fields, replace
from pathlib import Path

import pytest

from ginkgo import Out, file, task
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.remote_executor import RemoteExecutor
from ginkgo.runtime.run_summary import TaskSummary
from ginkgo.runtime.rundir import RunDir, make_run_id
from ginkgo.runtime.events import EventBus, TaskAnnotated
from ginkgo.runtime.task_runners.shell import classify_failure
from ginkgo_notebook import notebook
from ginkgo_notebook.runner import NotebookTaskError
from tests.remote_fakes import ContainerRoundTripExecutor, RemoteRoundTrip, set_up_round_trip

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


IMAGE = "docker://python:3.12"


@task("notebook", env=IMAGE)
def render_in_an_image(notebook_path: str, sample_id: str) -> file:
    return notebook(notebook_path)


def _replayed_page(recorded: dict[str, object], *, run_dir: RunDir) -> Path | None:
    """Resolve the page *recorded* names, as a summary of the run in *run_dir* does."""
    unrecorded = {field.name: None for field in fields(TaskSummary)}
    summary = TaskSummary(**{**unrecorded, **recorded})
    return summary.rendered_html_absolute(run_dir=run_dir.path)


def _evaluate(
    expr: object,
    *,
    root: Path,
    executor: RemoteExecutor | None = None,
    code: dict[str, str] | None = None,
) -> tuple[object, RunDir, dict[str, object]]:
    """Run *expr* once; return its result, run directory and annotated fields.

    With *executor*, every task runs on it, its code shipped as *code* says.
    """
    run_dir = RunDir.create(run_id=make_run_id(workflow_path=root / "flow.py"), root=root / "runs")
    bus = EventBus()
    fields: dict[str, object] = {}
    bus.subscribe(lambda event: isinstance(event, TaskAnnotated) and fields.update(event.fields))
    remote: dict[str, ExecutorRegistry] = {}
    if executor is not None:
        registry = ExecutorRegistry.for_executor(executor, name="remote")
        if code is not None:
            registry.specs["remote"] = replace(registry.specs["remote"], code=code)
        remote["executor_registry"] = registry
    result = ConcurrentEvaluator(
        run_dir=run_dir, event_bus=bus, jobs=1, cores=1, **remote
    ).evaluate(expr)
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
    assert not (notebook_path.parent / "__marimo__").exists()


def test_a_cache_hit_points_at_the_producing_page_until_the_notebook_is_edited(
    tmp_path: Path, notebook_path: Path
) -> None:
    expr = render(notebook_path=str(notebook_path), sample_id="s1")
    _, first_run, _ = _evaluate(expr, root=tmp_path)

    _, second_run, replayed = _evaluate(expr, root=tmp_path)
    # Relative to the run that rendered it, so the record holds no absolute path.
    assert replayed == {
        "rendered_html": "notebooks/task_0000.html",
        "rendered_html_run_id": first_run.run_id,
    }
    assert (
        _replayed_page(replayed, run_dir=second_run)
        == (first_run.path / "notebooks" / "task_0000.html").resolve()
    )

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


@pytest.fixture
def remote_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> RemoteRoundTrip:
    return set_up_round_trip(root=tmp_path, monkeypatch=monkeypatch)


class TestRemoteNotebook:
    """A notebook on a remote executor renders in the pod, and its page comes to the driver.

    The pod is the in-process round trip of ``tests/remote_fakes.py``. It
    renders in a working directory of its own, so a page in the driver's run
    directory came through the fake object store.
    """

    def test_the_page_comes_back_to_the_run(
        self, tmp_path: Path, notebook_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        executor = ContainerRoundTripExecutor()

        result, run_dir, fields = _evaluate(
            render(notebook_path=str(notebook_path), sample_id="s1"),
            root=tmp_path,
            executor=executor,
        )

        assert executor.images == [None]
        html = run_dir.path / "notebooks" / "task_0000.html"
        assert "sample" in html.read_text(encoding="utf-8")
        assert fields == {
            "rendered_html": "notebooks/task_0000.html",
            "rendered_html_run_id": run_dir.run_id,
        }
        assert Path(str(result)).read_bytes() == html.read_bytes()

    def test_a_read_only_working_directory_is_enough(
        self, tmp_path: Path, notebook_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        # The page goes on the worker's scratch, like everything else it writes.
        workdir = tmp_path / "image-workdir"
        workdir.mkdir(mode=0o555)
        executor = ContainerRoundTripExecutor()
        executor.workdir = workdir

        _, run_dir, fields = _evaluate(
            render(notebook_path=str(notebook_path), sample_id="s1"),
            root=tmp_path,
            executor=executor,
        )

        assert (run_dir.path / fields["rendered_html"]).is_file()

    def test_a_declared_output_and_the_page_both_come_back(
        self, tmp_path: Path, notebook_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        output = tmp_path / "results" / "sample.txt"
        executor = ContainerRoundTripExecutor()

        result, run_dir, fields = _evaluate(
            render_with_output(
                notebook_path=str(notebook_path), sample_id="s7", output_path=str(output)
            ),
            root=tmp_path,
            executor=executor,
        )

        assert executor.images == [None]
        assert Path(str(result)) == output
        assert output.read_text(encoding="utf-8") == "s7"
        assert (run_dir.path / fields["rendered_html"]).is_file()

    def test_a_cache_hit_points_at_the_page_the_remote_run_rendered(
        self, tmp_path: Path, notebook_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        expr = render(notebook_path=str(notebook_path), sample_id="s1")
        _, first_run, rendered = _evaluate(
            expr, root=tmp_path, executor=ContainerRoundTripExecutor()
        )

        _, second_run, replayed = _evaluate(
            expr, root=tmp_path, executor=ContainerRoundTripExecutor()
        )

        assert replayed == rendered
        page = _replayed_page(replayed, run_dir=second_run)
        assert page == (first_run.path / "notebooks" / "task_0000.html").resolve()
        assert page.is_file()

    def test_a_failing_cell_names_the_page_in_the_run(
        self, tmp_path: Path, notebook_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        run_dirs = tmp_path / "runs"
        executor = ContainerRoundTripExecutor()

        with pytest.raises(NotebookTaskError, match="shows the failing cell") as raised:
            _evaluate(
                render_failing(notebook_path=str(notebook_path), fail=True),
                root=tmp_path,
                executor=executor,
            )

        assert executor.images == [None]
        [html] = run_dirs.glob("*/notebooks/task_0000.html")
        assert f"The rendered notebook at {html} shows the failing cell" in str(raised.value)
        # Nor anywhere else: the pod's run directory is one per attempt.
        assert "task_0000_attempt_1" not in str(raised.value)

    def test_a_notebook_in_an_image_keeps_the_page_it_rendered(
        self, tmp_path: Path, remote_round_trip: RemoteRoundTrip
    ) -> None:
        # This host's python stands in for the image's, as its sh does.
        if subprocess.run(["python", "-c", "import marimo"], check=False).returncode != 0:
            pytest.skip("the python on PATH cannot import marimo")
        (tmp_path / "wf").mkdir()
        (tmp_path / "wf" / "report.py").write_text(_NOTEBOOK, encoding="utf-8")
        executor = ContainerRoundTripExecutor()

        # Relative to the project, which the code bundle stands in for.
        _, run_dir, fields = _evaluate(
            render_in_an_image(notebook_path="wf/report.py", sample_id="s1"),
            root=tmp_path,
            executor=executor,
            code={"mode": "sync", "package": "wf"},
        )

        assert executor.images == [IMAGE.removeprefix("docker://")]
        assert "sample" in (run_dir.path / fields["rendered_html"]).read_text(encoding="utf-8")
