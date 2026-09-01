"""How the CLI reports a failure that reached the top level."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import IO

from rich.text import Text
from rich.traceback import Traceback

from ginkgo.cli.common import console
from ginkgo.envs.interpreter import explain_import_failure
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
    message.

    *show_traceback* is the user overriding that judgement, so it always wins:
    a traceback that was asked for is printed for every failure, including the
    ones whose default report is a bare message. The hint that advertises the
    escape hatch is only printed alongside a location, so that ginkgo's own
    one-line messages stay one line.

    A ``ModuleNotFoundError`` inside a project that declares its environment
    carries the interpreter-mismatch explanation as well: on its own, that
    message sends the reader to the manifest, which is the one thing that is
    already right.
    """
    rich_console = console(stream)
    rich_console.print(Text("✖ ", style="bold red"), Text(str(exc)), sep="")

    # A workflow module imports in the CLI's own interpreter, so a missing
    # module here is as likely to mean the wrong interpreter as a missing
    # dependency. Say which when the project declares an environment. Printed
    # unwrapped for the same reason as the location line below: the hint names
    # an interpreter and a manifest path, and a wrapped path cannot be copied.
    if isinstance(exc, ModuleNotFoundError):
        finding = explain_import_failure(message=str(exc), project_root=Path.cwd())
        if finding is not None:
            rich_console.print(
                Text("\n".join(finding.hint_lines), style="yellow"),
                soft_wrap=True,
            )

    location = None if isinstance(exc, GinkgoError) else failure_location(exc)
    if location is not None:
        # A wrapped path is a path the user cannot click or copy, so the
        # location line is printed whole even when wider than the terminal.
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
    elif location is not None:
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
