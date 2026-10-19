"""The notebook section of the run banner.

Regression cover for issue #202 part 2: a cached rerun counted an earlier
run's notebook under "Notebooks materialised" and re-reported its export
failure as if this run had just produced it.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

from rich.console import Console
from rich.spinner import Spinner

from ginkgo.cli.renderers.models import CliNotebookSummary, CliRunSummary
from ginkgo.cli.renderers.run import _RunEventState, _RunLayoutRenderer


def _renderer(tmp_path: Path) -> _RunLayoutRenderer:
    return _RunLayoutRenderer(
        console=Console(file=StringIO(), width=200, force_terminal=False),
        summary=CliRunSummary(run_id="run_2", mode="default", run_dir=tmp_path, cores=1),
        resources=None,
        state=_RunEventState(),
        activity_spinner=Spinner("dots"),
        time_spinner=Spinner("dots"),
    )


def _notebook(tmp_path: Path, **overrides: object) -> CliNotebookSummary:
    fields: dict[str, object] = {
        "task_label": "render_overview_notebook",
        "html_path": tmp_path / "notebooks" / "task_0014.html",
        "render_status": "succeeded",
    }
    fields.update(overrides)
    return CliNotebookSummary(**fields)  # type: ignore[arg-type]


def test_fresh_notebooks_are_counted_as_materialised(tmp_path: Path) -> None:
    text = _renderer(tmp_path).render_notebooks([_notebook(tmp_path)]).plain

    assert "Notebooks materialised (1)" in text
    assert "↺" not in text


def test_fresh_export_failure_is_reported(tmp_path: Path) -> None:
    text = (
        _renderer(tmp_path).render_notebooks([_notebook(tmp_path, render_status="failed")]).plain
    )

    assert "Notebooks materialised (1)" in text
    assert "⚠ 1 HTML export failed" in text


def test_replayed_notebook_is_not_counted_as_materialised(tmp_path: Path) -> None:
    text = (
        _renderer(tmp_path)
        .render_notebooks([_notebook(tmp_path, replayed_from_run_id="run_1")])
        .plain
    )

    assert "Notebooks materialised (0)" in text
    assert "↺ 1 from an earlier run" in text
    assert "↺ from run run_1" in text


def test_replayed_export_failure_is_not_reported_as_new(tmp_path: Path) -> None:
    """The row keeps its warning; the header no longer counts it as this run's."""
    text = (
        _renderer(tmp_path)
        .render_notebooks(
            [_notebook(tmp_path, render_status="failed", replayed_from_run_id="run_1")]
        )
        .plain
    )

    assert "Notebooks materialised (0)" in text
    assert "⚠ 1 HTML export failed" not in text
    assert "↺ from run run_1" in text
    assert "⚠ HTML export failed" in text


def test_fresh_and_replayed_notebooks_are_counted_apart(tmp_path: Path) -> None:
    text = (
        _renderer(tmp_path)
        .render_notebooks(
            [
                _notebook(tmp_path, task_label="fresh"),
                _notebook(tmp_path, task_label="from_run_1", replayed_from_run_id="run_1"),
                _notebook(tmp_path, task_label="from_run_0", replayed_from_run_id="run_0"),
            ]
        )
        .plain
    )

    assert "Notebooks materialised (1)" in text
    assert "↺ 2 from an earlier runs" not in text
    assert "↺ 2 from earlier runs" in text
