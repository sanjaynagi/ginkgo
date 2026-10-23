"""``ginkgo runs show`` lists the pages a run's tasks rendered."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ginkgo.cli.app import main
from ginkgo.runtime.events import GraphNodeRegistered, TaskAnnotated, TaskCompleted
from tests.conftest import Ledger


def _run_with_a_rendered_page(*, root: Path) -> Ledger:
    ledger = Ledger.start(root=root, run_id="run-page")
    ledger.bus.emit(
        GraphNodeRegistered(
            run_id="run-page", task_id="task_0000", node_id=0, task_name="flow.report"
        )
    )
    page = ledger.path / "notebooks" / "task_0000.html"
    page.parent.mkdir(parents=True)
    page.write_text("<html></html>", encoding="utf-8")
    ledger.bus.emit(
        TaskAnnotated(
            run_id="run-page",
            task_id="task_0000",
            task_name="flow.report",
            fields={
                "rendered_html": "notebooks/task_0000.html",
                "rendered_html_run_id": "run-page",
            },
        )
    )
    ledger.bus.emit(
        TaskCompleted(run_id="run-page", task_id="task_0000", task_name="flow.report", attempt=1)
    )
    ledger.finish()
    ledger.close()
    return ledger


def test_runs_show_lists_the_rendered_page(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    ledger = _run_with_a_rendered_page(root=tmp_path)

    assert main(["runs", "show", "run-page"]) == 0
    out = capsys.readouterr().out
    assert "Rendered pages" in out
    assert str((ledger.path / "notebooks" / "task_0000.html").resolve()) in out.replace("\n", "")

    assert main(["runs", "show", "run-page", "--json"]) == 0
    (task,) = json.loads(capsys.readouterr().out)["tasks"]
    assert task["rendered_html"] == "notebooks/task_0000.html"
    assert task["rendered_html_run_id"] == "run-page"
