"""Warning about mo.cli_args() keys a notebook reads but its task does not pass."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import file, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.events import EventBus, TaskNotice
from ginkgo.runtime.rundir import RunDir, make_run_id
from ginkgo_notebook import notebook
from ginkgo_notebook.cli_args import read_cli_arg_keys, unpassed_cli_arg_notices

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
    print(args.get("summaries"), args.get("sample-id"))
    return


if __name__ == "__main__":
    app.run()
"""


def test_every_literal_key_shape_is_read() -> None:
    source = (
        "import marimo as mo\n"
        "args = mo.cli_args()\n"
        "args.get('a'); args.get_all('b'); args['c']; 'd' in args\n"
        "mo.cli_args().get('e'); args.get(name)\n"
    )

    assert read_cli_arg_keys(source) == {"a", "b", "c", "d", "e"}


def test_an_underscored_key_names_the_flag_the_task_passes() -> None:
    notices = unpassed_cli_arg_notices(
        source="import marimo as mo\nargs = mo.cli_args()\nargs.get('sample_id')\n",
        task_name="render",
        passed=["sample_id"],
    )

    assert notices == [
        "The notebook reads mo.cli_args() key 'sample_id', which render does not pass, "
        "so it reads None; the task passes it as 'sample-id'"
    ]


@task("notebook")
def render(notebook_path: str, sample_id: str, summary: str) -> file:
    return notebook(notebook_path)


def test_a_run_warns_about_a_key_the_task_does_not_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "report.py"
    path.write_text(_NOTEBOOK, encoding="utf-8")
    run_dir = RunDir.create(run_id=make_run_id(workflow_path=tmp_path / "flow.py"), root=tmp_path)
    bus = EventBus()
    notices: list[str] = []
    bus.subscribe(lambda event: isinstance(event, TaskNotice) and notices.append(event.message))

    ConcurrentEvaluator(run_dir=run_dir, event_bus=bus, jobs=1, cores=1).evaluate(
        render(notebook_path=str(path), sample_id="s1", summary="x")
    )

    assert len(notices) == 1
    assert "'summaries'" in notices[0]
    assert "did you mean 'summary'?" in notices[0]


def test_doctor_and_dry_run_warn_before_the_notebook_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from ginkgo.cli.app import main as cli_main

    monkeypatch.chdir(tmp_path)
    (tmp_path / "report.py").write_text(_NOTEBOOK, encoding="utf-8")
    (tmp_path / "wf.py").write_text(
        """
from ginkgo import file, flow, task
from ginkgo_notebook import notebook

@task("notebook")
def render(sample_id: str, summary: str) -> file:
    return notebook("report.py")

@flow
def main():
    return render().map(sample_id=["s1", "s2"], summary=["x", "y"])
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert cli_main(["doctor", "wf.py", "--json"]) == 0
    doctor_out = capsys.readouterr().out
    assert doctor_out.count("did you mean 'summary'?") == 1
    assert cli_main(["run", "wf.py", "--dry-run"]) == 0
    assert "did you mean 'summary'?" in capsys.readouterr().err
    assert not (tmp_path / ".ginkgo" / "notebooks").exists()
