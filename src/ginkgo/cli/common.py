"""Shared CLI constants and utilities."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal

from rich.console import Console

from ginkgo import query
from ginkgo.query import Query
from ginkgo.workspace_layout import WorkspaceLayout

_LAYOUT = WorkspaceLayout.relative()
RUNS_ROOT = _LAYOUT.runs
CACHE_ROOT = _LAYOUT.cache
ASSETS_ROOT = _LAYOUT.assets
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


def resolve_run_id(query: Query, run_id: str | None) -> str:
    """Return the run to act on: the one named, or the most recent.

    Parameters
    ----------
    query : Query
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
        if not query.store.query("SELECT 1 FROM runs WHERE run_id = ?", (run_id,)):
            raise FileNotFoundError(f"Run not found: {run_id}")
        return run_id

    latest = query.latest_run_id()
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
    try:
        store = query.open()
    except FileNotFoundError:
        if run_id is not None:
            raise FileNotFoundError(f"Run not found: {run_id}") from None
        raise
    with store:
        yield store, resolve_run_id(store, run_id)
