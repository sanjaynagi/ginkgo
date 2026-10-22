"""The run banner's end-of-run sections: rendered pages and the resource footer.

A cached rerun lists the page an earlier run rendered, marked as reused so it
does not read as this run's work (regression cover for issue #202 part 2). The
count is of the pages listed, so it never reads 0 above a list of pages.
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


def test_a_reused_page_is_counted_and_marked_as_reused(tmp_path: Path) -> None:
    text = _renderer(tmp_path).render_pages([_page(tmp_path, replayed_from_run_id="run_1")]).plain

    assert "Rendered pages (1)  ↺ all reused from an earlier run" in text
    assert "↺ from run run_1" in text


def test_a_mix_of_pages_says_how_many_were_reused(tmp_path: Path) -> None:
    pages = [_page(tmp_path), _page(tmp_path, replayed_from_run_id="run_1")]

    text = _renderer(tmp_path).render_pages(pages).plain

    assert "Rendered pages (2)  ↺ 1 reused from an earlier run" in text


def test_the_resource_footer_says_its_rss_is_a_process_tree_total(tmp_path: Path) -> None:
    footer = _renderer(tmp_path).render_resource_footer(
        {
            "average": {"cpu_percent": 50.0, "rss_bytes": 1024},
            "peak": {"cpu_percent": 90.0, "rss_bytes": 2048, "process_count": 5},
        }
    )

    assert footer is not None
    assert "(process-tree total, up to 5 processes)" in footer.plain
