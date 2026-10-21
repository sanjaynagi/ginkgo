"""NB5-1: a project file a notebook names in a literal is invisible to its task's key."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo_notebook.static_notices import literal_path_notices

_NOTEBOOK = """
import marimo

app = marimo.App()


@app.cell
def _():
    import marimo as mo

    mo.image("results/north.png")
    mo.image(mo.cli_args().get("south"))
    return
"""


def test_a_literal_project_file_is_flagged(tmp_path: Path) -> None:
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "north.png").write_bytes(b"png")

    notices = literal_path_notices(
        source=_NOTEBOOK, task_name="overview", root=tmp_path, declared=[]
    )

    assert len(notices) == 1
    assert "'results/north.png'" in notices[0]
    assert "`file` argument" in notices[0]


def test_a_declared_output_or_missing_file_is_not_flagged(tmp_path: Path) -> None:
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "north.png").write_bytes(b"png")

    assert (
        literal_path_notices(
            source=_NOTEBOOK, task_name="overview", root=tmp_path, declared=["results/north.png"]
        )
        == []
    )
    assert (
        literal_path_notices(
            source=_NOTEBOOK, task_name="overview", root=tmp_path / "empty", declared=[]
        )
        == []
    )


def test_a_default_for_a_missing_argument_is_not_flagged(tmp_path: Path) -> None:
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "north.png").write_bytes(b"png")
    source = """
import marimo as mo
args = mo.cli_args()
first = args.get("north") or "results/north.png"
second = args.get("south", "results/north.png")
"""

    assert (
        literal_path_notices(source=source, task_name="overview", root=tmp_path, declared=[]) == []
    )


def test_doctor_reports_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from ginkgo.cli.app import main as cli_main

    monkeypatch.chdir(tmp_path)
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "north.png").write_bytes(b"png")
    (tmp_path / "results" / "south.png").write_bytes(b"png")
    (tmp_path / "overview.py").write_text(_NOTEBOOK, encoding="utf-8")
    (tmp_path / "wf.py").write_text(
        """
from ginkgo import file, flow, task
from ginkgo_notebook import notebook

@task("notebook")
def overview(south: file) -> file:
    return notebook("overview.py")

@flow
def main():
    return overview(south="results/south.png")
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert cli_main(["doctor", "wf.py", "--json"]) == 0
    assert "The notebook names the project path 'results/north.png'" in capsys.readouterr().out
