"""Shared CLI constants and utilities."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from rich import box
from rich.console import Console
from rich.table import Table

from ginkgo import query
from ginkgo.query import Query
from ginkgo.workspace_layout import WorkspaceLayout

_LAYOUT = WorkspaceLayout.relative()
RUNS_ROOT = _LAYOUT.runs
CACHE_ROOT = _LAYOUT.cache
RunMode = Literal["default", "verbose", "agent", "agent_verbose"]


def console(output_stream, *, width: int | None = None) -> Console:
    """Build the Rich console used by the CLI."""
    return Console(
        file=output_stream,
        highlight=False,
        soft_wrap=False,
        force_terminal=getattr(output_stream, "isatty", lambda: False)(),
        width=width,
    )


def stdout_console(*, piped_width: int = 160) -> Console:
    """Return the console a read-only command prints to.

    A terminal sets its own width; anything else — a pipe, a file, a test —
    gets a fixed one, so a table's columns do not depend on where the output
    went. Every command that prints a table asks for its console here, which
    is why they all wrap the same way.

    Parameters
    ----------
    piped_width : int, optional
        The width to assume when stdout is not a terminal. Raise it for output
        with more columns than a default terminal fits.
    """
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    return Console(
        file=sys.stdout,
        highlight=False,
        soft_wrap=False,
        force_terminal=is_tty,
        width=None if is_tty else piped_width,
    )


def new_table(*columns: str) -> Table:
    """Return an empty table in ginkgo's one list style.

    Written down once so ``cache ls``, ``runs ls``, ``history`` and the rest
    cannot drift into looking like different programs. Columns given here take
    the default treatment; a column wanting alignment or wrapping of its own is
    added by the caller with :meth:`~rich.table.Table.add_column`.
    """
    built = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    for column in columns:
        built.add_column(column)
    return built


def resolve_run_id(reader: Query, run_id: str | None) -> str:
    """Return the run to act on: the one named, or the most recent.

    Parameters
    ----------
    reader : Query
        An open read-only view of the ledger.
    run_id : str | None
        The run the user named, or ``None`` for the latest.

    Returns
    -------
    str
        A run id the store has a row for.

    Raises
    ------
    FileNotFoundError
        If the named run is unknown, or the workspace has no runs at all.
    """
    if run_id is not None:
        if not reader.store.query("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)):
            raise FileNotFoundError(f"Run not found: {run_id}")
        return run_id

    latest = reader.latest_run_id()
    if latest is None:
        raise FileNotFoundError(f"No runs recorded in {_LAYOUT.db}")
    return latest


@contextmanager
def open_run(run_id: str | None) -> Iterator[tuple[Query, str]]:
    """Open the ledger and resolve which run the command is about.

    Parameters
    ----------
    run_id : str | None
        The run the user named, or ``None`` for the most recent.

    Yields
    ------
    tuple[Query, str]
        The open read-only view and the resolved run id.

    Raises
    ------
    FileNotFoundError
        If the run is unknown, or the workspace has recorded no runs at all.
        A named run reads as missing whether the ledger lacks the row or the
        workspace lacks the ledger — from where the user stands those are the
        same thing.
    """
    with query.open(missing_ok=True) as reader:
        yield reader, resolve_run_id(reader, run_id)
