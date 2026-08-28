"""Read the provenance ledger.

The public way to ask ginkgo what has happened. Every CLI read path goes
through here, so there is one place that knows the schema and one place to
change when it moves. Readers open the database read-only, which means they
work while a run is writing and can never migrate it out from under one.

Phase 1 ships the run surface the CLI needs. ``task_history``, ``explain_rerun``,
``cache_stats``, ``lineage``, ``why`` and ``sql`` arrive with the phases that
give them a caller.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from ginkgo.runtime.run_summary import RunSummary
from ginkgo.store.protocol import ProvenanceStore
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = ["EventRow", "Query", "RunRow", "open"]


@dataclass(frozen=True, kw_only=True)
class RunRow:
    """One row of the run index."""

    run_id: str
    workflow: str | None
    status: str
    started_at: str | None
    finished_at: str | None
    parent_run_id: str | None


@dataclass(frozen=True, kw_only=True)
class EventRow:
    """One ledger event."""

    seq: int
    run_id: str
    ts: str
    type: str
    task_id: str | None
    payload: dict[str, Any]


class Query:
    """A read-only view of one workspace's ledger.

    Construct through :func:`open`. Closing it releases the connection; it is
    also a context manager.
    """

    def __init__(self, store: ProvenanceStore, *, layout: WorkspaceLayout) -> None:
        self._store = store
        self._layout = layout

    @property
    def store(self) -> ProvenanceStore:
        """The underlying store, for callers that need raw SQL."""
        return self._store

    def runs(
        self,
        *,
        workflow: str | None = None,
        status: str | None = None,
        since: str | None = None,
        limit: int = 50,
    ) -> list[RunRow]:
        """Return runs, newest first.

        Parameters
        ----------
        workflow : str | None, optional
            Match runs whose workflow path ends with this.
        status : str | None, optional
            Match one run status.
        since : str | None, optional
            Only runs started at or after this ISO timestamp.
        limit : int, optional
            Most rows to return.

        Returns
        -------
        list[RunRow]
        """
        clauses: list[str] = []
        params: list[Any] = []
        if workflow is not None:
            clauses.append("workflow LIKE ?")
            params.append(f"%{workflow}")
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        if since is not None:
            clauses.append("started_at >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._store.query(
            "SELECT run_id, workflow, status, started_at, finished_at, parent_run_id "
            f"FROM runs {where} ORDER BY started_at DESC, run_id DESC LIMIT ?",  # noqa: S608
            (*params, limit),
        )
        return [
            RunRow(
                run_id=row["run_id"],
                workflow=row["workflow"],
                status=row["status"],
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                parent_run_id=row["parent_run_id"],
            )
            for row in rows
        ]

    def latest_run_id(self) -> str | None:
        """Return the most recent run's id, or ``None`` when there is none."""
        rows = self.runs(limit=1)
        return rows[0].run_id if rows else None

    def run(self, run_id: str) -> RunSummary:
        """Return the full read model for one run.

        Raises
        ------
        KeyError
            If the store has no such run.
        """
        return RunSummary.load(self._store, run_id, runs_root=self._layout.runs)

    def task_status_counts(self, run_id: str) -> dict[str, int]:
        """Return how many of a run's tasks are in each status."""
        rows = self._store.query(
            "SELECT status, count(*) AS n FROM tasks WHERE run_id = ? GROUP BY status",
            (run_id,),
        )
        return {row["status"]: int(row["n"]) for row in rows}

    def events(
        self,
        run_id: str,
        *,
        after_seq: int = 0,
        types: Sequence[str] | None = None,
    ) -> Iterator[EventRow]:
        """Yield a run's ledger events in the order they happened."""
        params: list[Any] = [run_id, after_seq]
        filter_sql = ""
        if types:
            filter_sql = f" AND type IN ({', '.join('?' for _ in types)})"
            params.extend(types)
        rows = self._store.query(
            "SELECT seq, run_id, ts, type, task_id, payload FROM events "
            f"WHERE run_id = ? AND seq > ?{filter_sql} ORDER BY seq",  # noqa: S608
            params,
        )
        for row in rows:
            yield EventRow(
                seq=row["seq"],
                run_id=row["run_id"],
                ts=row["ts"],
                type=row["type"],
                task_id=row["task_id"],
                payload=json.loads(row["payload"]),
            )

    def close(self) -> None:
        """Release the connection."""
        self._store.close()

    def __enter__(self) -> Query:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def open(  # noqa: A001 - the module's verb; callers write ginkgo.query.open(...)
    layout: WorkspaceLayout | None = None,
    *,
    readonly: bool = True,
) -> Query:
    """Open a workspace's ledger for reading.

    Parameters
    ----------
    layout : WorkspaceLayout | None, optional
        The workspace. Defaults to the current directory's.
    readonly : bool, optional
        Keep this ``True`` unless you are the run that owns the write lock.

    Returns
    -------
    Query

    Raises
    ------
    FileNotFoundError
        If the workspace has no ledger yet, which is to say no runs.
    StoreError
        If the database exists but cannot be read.
    """
    layout = layout if layout is not None else WorkspaceLayout.relative()
    path = Path(layout.db)
    if readonly and not path.is_file():
        raise FileNotFoundError(f"No runs found: there is no provenance database at {path}")
    return Query(open_store(path, readonly=readonly), layout=layout)
