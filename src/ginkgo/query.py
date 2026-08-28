"""Read the provenance ledger.

The public way to ask ginkgo what has happened. Every CLI read path goes
through here, so there is one place that knows the schema and one place to
change when it moves. Readers open the database read-only, which means they
work while a run is writing and can never migrate it out from under one.

Phase 1 shipped the run surface and Phase 2 the cache surface. ``task_history``,
``lineage``, ``why`` and ``sql`` arrive with the phases that give them a
caller.
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

__all__ = ["CacheEntryRow", "CacheStats", "EventRow", "Query", "RunRow", "open"]


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
class CacheEntryRow:
    """One cache entry, as the cache commands display it."""

    cache_key: str
    function: str
    size_bytes: int
    created_at: str | None
    hit_count: int
    last_hit_at: str | None


@dataclass(frozen=True, kw_only=True)
class CacheStats:
    """What the cache holds, in aggregate."""

    entries: int
    total_bytes: int
    never_hit: int
    never_hit_bytes: int
    hit_histogram: dict[int, int]
    top_functions: list[tuple[str, int, int]]


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

    # -- cache ---------------------------------------------------------------

    def cache_entries(self, *, function: str | None = None) -> list[CacheEntryRow]:
        """Return cache entries, newest first.

        Parameters
        ----------
        function : str | None, optional
            Only entries written by this task.

        Returns
        -------
        list[CacheEntryRow]
        """
        where = "WHERE function = ?" if function is not None else ""
        params = (function,) if function is not None else ()
        rows = self._store.query(
            "SELECT cache_key, function, size_bytes, created_at, hit_count, last_hit_at "
            f"FROM cache_entries {where} ORDER BY created_at DESC, cache_key",  # noqa: S608
            params,
        )
        return [
            CacheEntryRow(
                cache_key=str(row["cache_key"]),
                function=str(row["function"]),
                size_bytes=int(row["size_bytes"] or 0),
                created_at=row["created_at"],
                hit_count=int(row["hit_count"] or 0),
                last_hit_at=row["last_hit_at"],
            )
            for row in rows
        ]

    def cache_key_components(self, cache_key: str) -> dict[str, Any]:
        """Return the labelled components of one entry's cache key."""
        rows = self._store.query(
            "SELECT component, value FROM cache_key_components WHERE cache_key = ?",
            (cache_key,),
        )
        return {
            str(row["component"]): json.loads(row["value"]) if row["value"] is not None else None
            for row in rows
        }

    def cache_stats(self) -> CacheStats:
        """Return entry counts, bytes and hit statistics for the whole cache."""
        totals = self._store.query(
            "SELECT count(*) AS entries, coalesce(sum(size_bytes), 0) AS total_bytes, "
            "coalesce(sum(hit_count = 0), 0) AS never_hit, "
            "coalesce(sum(CASE WHEN hit_count = 0 THEN size_bytes ELSE 0 END), 0) "
            "AS never_hit_bytes FROM cache_entries"
        )[0]
        histogram = self._store.query(
            "SELECT hit_count, count(*) AS n FROM cache_entries GROUP BY hit_count "
            "ORDER BY hit_count"
        )
        functions = self._store.query(
            "SELECT function, count(*) AS n, coalesce(sum(size_bytes), 0) AS bytes "
            "FROM cache_entries GROUP BY function ORDER BY bytes DESC, function LIMIT 10"
        )
        return CacheStats(
            entries=int(totals["entries"]),
            total_bytes=int(totals["total_bytes"]),
            never_hit=int(totals["never_hit"]),
            never_hit_bytes=int(totals["never_hit_bytes"]),
            hit_histogram={int(row["hit_count"]): int(row["n"]) for row in histogram},
            top_functions=[
                (str(row["function"]), int(row["n"]), int(row["bytes"])) for row in functions
            ],
        )

    def previous_cache_key(self, *, run_id: str, task_id: str) -> tuple[str, str] | None:
        """Return the key this task's node last used, and how it was found.

        The comparison ``ginkgo cache explain`` wants is against the same node
        of the same workflow in the run before this one — the entry this run
        superseded. Where that node is new, the newest earlier entry written by
        the same function is the closest thing to a predecessor, and the caller
        is told which of the two it got so it does not read a fan-out sibling
        as a history (issue #223).

        Parameters
        ----------
        run_id : str
            The run being explained.
        task_id : str
            The task within it.

        Returns
        -------
        tuple[str, str] | None
            ``(cache_key, strategy)`` where strategy is ``"same_node"`` or
            ``"newest_by_function"``, or ``None`` when nothing earlier exists.
        """
        rows = self._store.query(
            "SELECT t.name, t.display_label, t.cache_key, r.workflow, r.started_at "
            "FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            "WHERE t.run_id = ? AND t.task_id = ?",
            (run_id, task_id),
        )
        if not rows:
            return None
        task = rows[0]
        same_node = self._store.query(
            "SELECT t.cache_key FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            "WHERE r.workflow = ? AND r.started_at < ? AND t.cache_key IS NOT NULL "
            "AND t.cache_key != ? AND coalesce(t.display_label, t.name) = ? "
            "ORDER BY r.started_at DESC LIMIT 1",
            (
                task["workflow"],
                task["started_at"],
                task["cache_key"],
                task["display_label"] or task["name"],
            ),
        )
        if same_node:
            return str(same_node[0]["cache_key"]), "same_node"

        sibling = self._store.query(
            "SELECT cache_key FROM cache_entries WHERE function = ? AND cache_key != ? "
            "AND created_at < coalesce("
            "  (SELECT created_at FROM cache_entries WHERE cache_key = ?), created_at"
            ") ORDER BY created_at DESC LIMIT 1",
            (task["name"], task["cache_key"], task["cache_key"]),
        )
        if sibling:
            return str(sibling[0]["cache_key"]), "newest_by_function"
        return None

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
