"""The rendered-pages section of the run banner.

A cached rerun lists the page an earlier run rendered, marked as reused, and
does not count it as this run's work (regression cover for issue #202 part 2).
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path

from rich.console import Console
from rich.spinner import Spinner

from ginkgo.cli.renderers.models import CliRenderedPage, CliRunSummary
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


def _page(tmp_path: Path, *, replayed_from_run_id: str | None = None) -> CliRenderedPage:
    return CliRenderedPage(
        task_label="render_overview_notebook",
        html_path=tmp_path / "notebooks" / "task_0014.html",
        replayed_from_run_id=replayed_from_run_id,
    )


def test_a_page_this_run_rendered_is_counted(tmp_path: Path) -> None:
    text = _renderer(tmp_path).render_pages([_page(tmp_path)]).plain

    assert "Rendered pages (1)" in text
    assert "render_overview_notebook" in text
    assert "↺" not in text


def test_a_reused_page_is_listed_but_not_counted(tmp_path: Path) -> None:
    text = _renderer(tmp_path).render_pages([_page(tmp_path, replayed_from_run_id="run_1")]).plain

    assert "Rendered pages (0)" in text
    assert "↺ 1 from an earlier run" in text
    assert "↺ from run run_1" in text
