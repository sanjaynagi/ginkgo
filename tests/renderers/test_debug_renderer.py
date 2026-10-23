"""``ginkgo debug``'s failure panel: a headline, not a doubled-up traceback (#297)."""

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console

from ginkgo.cli.renderers.debug import render_debug_failure_panel
from ginkgo.cli.renderers.models import FailureDetails

_TRACEBACK = ["Traceback (most recent call last):", "ValueError: bad value"]


@pytest.mark.parametrize(
    ("error", "log_tail", "shown", "hidden"),
    [
        # The traceback is the log tail, printed below the panel, not repeated in it.
        ("\n".join(_TRACEBACK), _TRACEBACK, "ValueError: bad value", _TRACEBACK[0]),
        ("bad input", [], "bad input", None),
    ],
    ids=["multi-line-repeating-the-log-tail", "single-line"],
)
def test_the_panel_shows_the_error_s_headline(error, log_tail, shown, hidden) -> None:
    console = Console(file=StringIO(), width=120, force_terminal=False)
    console.print(
        render_debug_failure_panel(
            FailureDetails(
                task_label="task_a",
                exit_code=1,
                log_path=None,
                log_tail=log_tail,
                error=error,
                task_kind="shell",
            )
        )
    )
    text = console.file.getvalue()

    assert shown in text
    assert hidden is None or hidden not in text
