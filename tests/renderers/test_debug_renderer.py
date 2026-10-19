"""``ginkgo debug``'s failure panel: a headline, not a doubled-up traceback (#297)."""

from __future__ import annotations

from io import StringIO

from rich.console import Console

from ginkgo.cli.renderers.debug import render_debug_failure_panel
from ginkgo.cli.renderers.models import FailureDetails


def _rendered(details: FailureDetails) -> str:
    console = Console(file=StringIO(), width=120, force_terminal=False)
    console.print(render_debug_failure_panel(details))
    return console.file.getvalue()


def test_a_notebook_failures_error_row_is_a_headline_not_the_full_traceback() -> None:
    traceback_text = (
        "Notebook task survey failed during execute with exit code 1: papermill ...\n"
        'Exception encountered at "In [7]":\n'
        "KeyError                                 Traceback (most recent call last)\n"
        "KeyError: 'slope_per_year'"
    )
    details = FailureDetails(
        task_label="survey",
        exit_code=1,
        log_path=None,
        log_tail=traceback_text.splitlines(),
        error=traceback_text,
        task_kind="notebook",
    )

    text = _rendered(details)

    assert "papermill failed executing cell 7: KeyError: 'slope_per_year'" in text
    assert text.count("Traceback (most recent call last)") == 1


def test_a_multiline_error_that_repeats_the_log_tail_is_reduced_to_its_last_line() -> None:
    details = FailureDetails(
        task_label="task_a",
        exit_code=1,
        log_path=None,
        log_tail=["Traceback (most recent call last):", "ValueError: bad value"],
        error="Traceback (most recent call last):\nValueError: bad value",
        task_kind="shell",
    )

    text = _rendered(details)

    assert "ValueError: bad value" in text
    assert text.count("Traceback (most recent call last):") == 1


def test_a_single_line_error_is_shown_as_is() -> None:
    details = FailureDetails(
        task_label="task_a",
        exit_code=1,
        log_path=None,
        log_tail=[],
        error="bad input",
    )

    text = _rendered(details)

    assert "bad input" in text
