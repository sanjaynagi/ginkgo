"""How the CLI reports a failure that reached the top level."""

from __future__ import annotations

import argparse
import os
from typing import IO

from rich.text import Text
from rich.traceback import Traceback

from ginkgo.cli.common import console
from ginkgo.errors import GinkgoError, failure_location

__all__ = [
    "INTERRUPT_EXIT_CODE",
    "report_failure",
    "report_interrupt",
    "traceback_requested",
]

_TRACEBACK_ENV_VAR = "GINKGO_TRACEBACK"
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})

#: Exit status conventionally used for termination by SIGINT (128 + 2).
INTERRUPT_EXIT_CODE = 130


def traceback_requested(args: argparse.Namespace | None = None) -> bool:
    """Whether the user asked to see full tracebacks.

    Either ``GINKGO_TRACEBACK=1`` in the environment or ``--verbose`` on the
    command line turns them on. The failure's location is reported either way;
    this only controls the frames beneath it.
    """
    if os.environ.get(_TRACEBACK_ENV_VAR, "").strip().lower() in _TRUE_VALUES:
        return True
    return bool(getattr(args, "verbose", False))


def report_failure(
    *,
    exc: BaseException,
    stream: IO[str],
    show_traceback: bool,
) -> int:
    """Print *exc* to *stream* and return the exit status the CLI should use.

    A :class:`~ginkgo.errors.GinkgoError` — or any failure raised with no user
    code on the stack, which means one of ginkgo's own checks tripped — prints
    as a single ``✖ <message>`` line. Anything else is a bug in the workflow or
    in ginkgo, so the file and line of the innermost user frame follow the
    message, and the traceback follows that when *show_traceback* is set.
    """
    rich_console = console(stream)
    rich_console.print(Text("✖ ", style="bold red"), Text(str(exc)), sep="")

    location = None if isinstance(exc, GinkgoError) else failure_location(exc)
    if location is None:
        return 1

    # A wrapped path is a path the user cannot click or copy, so the location
    # line is printed whole even when it is wider than the terminal.
    rich_console.print(
        Text(f"  {type(exc).__name__} at {location}", style="dim"),
        soft_wrap=True,
    )
    if show_traceback:
        rich_console.print(
            Traceback.from_exception(
                type(exc),
                exc,
                exc.__traceback__,
                show_locals=False,
            )
        )
    else:
        rich_console.print(
            Text(
                f"  Re-run with {_TRACEBACK_ENV_VAR}=1 (or --verbose) for the full traceback.",
                style="dim",
            )
        )
    return 1


def report_interrupt(*, stream: IO[str]) -> int:
    """Print the notice for a Ctrl-C and return the conventional exit status."""
    console(stream).print(Text("⨯ Interrupted", style="yellow"))
    return INTERRUPT_EXIT_CODE
