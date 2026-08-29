"""Read the provenance ledger.

The public way to ask ginkgo what has happened. Every CLI read path goes
through here, so there is one place that knows the schema and one place to
change when it moves. Readers open the database read-only, which means they
work while a run is writing and can never migrate it out from under one.

Every method returns either a core type ginkgo already models — a
:class:`~ginkgo.runtime.run_summary.RunSummary`, an
:class:`~ginkgo.core.asset.AssetVersion` — or a frozen row dataclass declared
here for a shape that has no model elsewhere.

The database schema is versioned but **not stable**. :meth:`Query.sql` hands
out raw SQL over the tables described in ``docs/architecture/store.md``, and
those tables change between ginkgo versions without a deprecation period; a
query written against them may need rewriting after an upgrade. The methods on
:class:`Query` are the surface that is kept working.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from ginkgo.core.asset import AssetKey, AssetVersion
from ginkgo.formatting import duration_seconds, parse_timestamp
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.caching.cache import key_components
from ginkgo.runtime.caching.index import ENTRY_COLUMNS, CacheEntry
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.store.errors import StoreError
from ginkgo.store.protocol import ProvenanceStore
from ginkgo.store.sqlite import MEMORY, open_store
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = [
    "CacheEntryRow",
    "CacheStats",
    "EventRow",
    "LineageGraph",
    "Provenance",
    "Query",
    "RerunExplanation",
    "RunRow",
    "SqlResult",
    "TaskRow",
    "open",
]

SQL_ROW_LIMIT = 1000
"""Rows :meth:`Query.sql` returns when the caller names no limit of its own."""

_READ_VERBS = frozenset({"SELECT", "WITH", "VALUES", "EXPLAIN"})
"""The statements :meth:`Query.sql` will run. Everything else is refused."""

_BODY_VERBS = frozenset({"SELECT", "VALUES", "INSERT", "UPDATE", "DELETE", "REPLACE"})
"""The verbs that can follow a ``WITH`` clause's common table expressions."""


@dataclass(frozen=True, kw_only=True)
class RunRow:
    """One row of the run index.

    The listing shape, for ``ginkgo runs ls``. One run in full is a
    :class:`~ginkgo.runtime.run_summary.RunSummary` from :meth:`Query.run`.

    Attributes
    ----------
    run_id : str
        The run's id.
    workflow : str | None
        Path of the workflow module it ran.
    status : str
        ``running``, ``succeeded``, ``failed``, or ``interrupted``.
    started_at, finished_at : str | None
        ISO-8601 UTC timestamps; ``finished_at`` is ``None`` while it runs.
    duration_s : float | None
        Wall-clock seconds, once the run has finished.
    parent_run_id : str | None
        The run that launched this one as a sub-workflow, if any.
    """

    run_id: str
    workflow: str | None
    status: str
    started_at: str | None
    finished_at: str | None
    duration_s: float | None
    parent_run_id: str | None

    def to_payload(self) -> dict[str, Any]:
        """Return the row as JSON-ready data."""
        return {
            "run_id": self.run_id,
            "workflow": self.workflow,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": self.duration_s,
            "parent_run_id": self.parent_run_id,
        }


@dataclass(frozen=True, kw_only=True)
class TaskRow:
    """One run of one task, as ``ginkgo history`` lists them.

    A task's history crosses runs, which is what makes this neither a
    :class:`~ginkgo.runtime.run_summary.TaskSummary` (one task within one run,
    with its inputs, outputs and logs) nor a :class:`RunRow`.

    Attributes
    ----------
    run_id, task_id : str
        Where this execution happened.
    name : str
        The task's fully qualified name.
    display_label : str | None
        The label the run rendered, when fan-out gave it one.
    status : str
        ``succeeded``, ``failed``, ``cached``, ``skipped``, …
    cached : bool
        Whether the output was served from the cache rather than computed.
    cache_key : str | None
        The entry it hit or wrote.
    started_at, finished_at : str | None
        ISO-8601 UTC timestamps.
    duration_s : float | None
        Wall-clock seconds, when both timestamps are present.
    attempts : int
        How many times it was tried, retries included.
    """

    run_id: str
    task_id: str
    name: str
    display_label: str | None
    status: str
    cached: bool
    cache_key: str | None
    started_at: str | None
    finished_at: str | None
    duration_s: float | None
    attempts: int

    def to_payload(self) -> dict[str, Any]:
        """Return the row as JSON-ready data."""
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "name": self.name,
            "display_label": self.display_label,
            "status": self.status,
            "cached": self.cached,
            "cache_key": self.cache_key,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": self.duration_s,
            "attempts": self.attempts,
        }


@dataclass(frozen=True, kw_only=True)
class SqlResult:
    """What one :meth:`Query.sql` statement selected.

    Attributes
    ----------
    columns : tuple[str, ...]
        Column names, in the order the statement selected them. Present even
        when no row came back, so a CSV export still has a header.
    rows : list[sqlite3.Row]
        The rows, addressable by index or by column name.
    truncated : bool
        Whether the row limit cut the result short.
    limit : int
        The cap that was applied, so a truncated result says what to raise.
    """

    columns: tuple[str, ...]
    rows: list[sqlite3.Row]
    truncated: bool
    limit: int

    def to_payload(self) -> dict[str, Any]:
        """Return the whole result as JSON-ready data.

        An envelope rather than a bare list of rows: a caller that reads only
        the rows cannot tell a complete answer from a truncated one, and that
        is the difference that matters. Repeated column names are suffixed by
        :func:`_unique_columns`, because a mapping cannot hold two of them.
        """
        keys = _unique_columns(self.columns)
        return {
            "columns": list(keys),
            "rows": [dict(zip(keys, tuple(row), strict=True)) for row in self.rows],
            "truncated": self.truncated,
            "limit": self.limit,
        }


@dataclass(frozen=True, kw_only=True)
class CacheEntryRow:
    """One cache entry, as the cache commands display it.

    Attributes
    ----------
    cache_key : str
        The entry's content-addressed key.
    function : str
        Fully qualified name of the task that wrote it.
    size_bytes : int
        Bytes the entry's stored output occupies.
    created_at, last_hit_at : str | None
        ISO-8601 UTC timestamps; ``last_hit_at`` is ``None`` until a run hits it.
    hit_count : int
        Runs that have served from this entry.
    """

    cache_key: str
    function: str
    size_bytes: int
    created_at: str | None
    hit_count: int
    last_hit_at: str | None


@dataclass(frozen=True, kw_only=True)
class CacheStats:
    """What the cache holds, in aggregate.

    Attributes
    ----------
    entries : int
        Entries in the index.
    total_bytes : int
        Bytes across all of them.
    never_hit : int
        Entries no run has ever served from.
    never_hit_bytes : int
        Bytes those entries hold — what pruning them would free.
    hit_histogram : dict[int, int]
        Entry count by hit count.
    top_functions : list[tuple[str, int, int]]
        The ten largest tasks as ``(function, entries, bytes)``, biggest first.
    """

    entries: int
    total_bytes: int
    never_hit: int
    never_hit_bytes: int
    hit_histogram: dict[int, int]
    top_functions: list[tuple[str, int, int]]

    @classmethod
    def empty(cls) -> CacheStats:
        """Return the stats of a workspace with no cache — and no database."""
        return cls(
            entries=0,
            total_bytes=0,
            never_hit=0,
            never_hit_bytes=0,
            hit_histogram={},
            top_functions=[],
        )


@dataclass(frozen=True, kw_only=True)
class RerunExplanation:
    """Why one task ran again — or did not.

    Attributes
    ----------
    reason : str
        The summary code: ``all_inputs_match``, ``no_entry_for_key``,
        ``no_prior_entry``, or the first of *details*.
    details : list[str]
        Every summary code the component diff implies.
    compared_with : dict[str, str] | None
        The entry compared against and how it was found — ``cache_key`` and a
        ``strategy`` of ``same_node`` or ``newest_by_function`` — or ``None``
        when there was nothing to compare with.
    components : list[dict[str, Any]]
        One entry per cache-key component that differs.
    """

    task_id: str | None
    task_name: str
    display_label: str | None
    cache_key: str | None
    reason: str
    details: list[str]
    compared_with: dict[str, str] | None = None
    components: list[dict[str, Any]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        """Return the JSON shape ``ginkgo cache explain`` prints."""
        payload: dict[str, Any] = {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "display_label": self.display_label,
            "cache_key": self.cache_key,
            "reason": self.reason,
        }
        if self.compared_with is None and not self.components:
            return payload
        return payload | {
            "compared_with": self.compared_with,
            "details": self.details,
            "components": self.components,
        }


@dataclass(frozen=True, kw_only=True)
class EventRow:
    """One ledger event.

    The ledger is the same event stream ``ginkgo run --agent-output`` prints, so
    *payload* is exactly the JSON object that run printed for this event.

    Attributes
    ----------
    seq : int
        The event's position in the ledger. Ascending, and unique across runs.
    run_id : str
        The run that emitted it.
    ts : str
        ISO-8601 UTC timestamp.
    type : str
        The event's discriminator, e.g. ``"task_completed"``.
    task_id : str | None
        The task it concerns, or ``None`` for a run-level event.
    payload : dict[str, Any]
        The whole event as data.
    """

    seq: int
    run_id: str
    ts: str
    type: str
    task_id: str | None
    payload: dict[str, Any]


@dataclass(frozen=True, kw_only=True)
class LineageGraph:
    """One asset version and the versions it reaches, in one direction.

    Nodes are :class:`~ginkgo.core.asset.AssetVersion` — the catalog's model of
    a version, not a second one — keyed by version id. Every edge points from a
    parent to the child derived from it, whichever direction the walk went.

    Parameters
    ----------
    root : AssetVersion
        The version the walk started from.
    direction : str
        ``"upstream"`` for what the root was built from, ``"downstream"`` for
        what was built from it.
    versions : dict[str, AssetVersion]
        Every version reached, including the root, by version id.
    edges : tuple[tuple[str, str], ...]
        ``(parent_version_id, child_version_id)`` pairs, sorted.
    """

    root: AssetVersion
    direction: str
    versions: dict[str, AssetVersion]
    edges: tuple[tuple[str, str], ...]

    def neighbours(self, version_id: str) -> list[str]:
        """Return the versions one step from *version_id*, in the walk's direction."""
        if self.direction == "downstream":
            return [child for parent, child in self.edges if parent == version_id]
        return [parent for parent, child in self.edges if child == version_id]

    def to_payload(self) -> dict[str, Any]:
        """Return the graph as JSON-ready data."""
        return {
            "root": self.root.to_dict(),
            "direction": self.direction,
            "versions": {
                version_id: version.to_dict()
                for version_id, version in sorted(self.versions.items())
            },
            "edges": [{"parent": parent, "child": child} for parent, child in self.edges],
        }


@dataclass(frozen=True, kw_only=True)
class Provenance:
    """Where one artifact came from.

    Parameters
    ----------
    artifact_id : str
        The artifact asked about.
    path : str | None
        The materialized path it was found through, when the question was a path.
    run_id, task_id, task_name : str | None
        The task that produced it.
    cache_key : str | None
        The cache entry holding it, if it came out of the cache.
    asset_key, version_id : str | None
        The asset version it backs, if it is a catalogued asset.
    inputs : tuple[dict[str, Any], ...]
        The producing task's recorded inputs, one mapping per parameter.
    """

    artifact_id: str
    path: str | None = None
    run_id: str | None = None
    task_id: str | None = None
    task_name: str | None = None
    cache_key: str | None = None
    asset_key: str | None = None
    version_id: str | None = None
    inputs: tuple[dict[str, Any], ...] = ()

    def to_payload(self) -> dict[str, Any]:
        """Return the provenance as JSON-ready data."""
        return {
            "artifact_id": self.artifact_id,
            "path": self.path,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "task_name": self.task_name,
            "cache_key": self.cache_key,
            "asset_key": self.asset_key,
            "version_id": self.version_id,
            "inputs": [dict(entry) for entry in self.inputs],
        }


class Query:
    """A read-only view of one workspace's ledger.

    Construct through :func:`open`. Closing it releases the connection; it is
    also a context manager.
    """

    def __init__(self, store: ProvenanceStore, *, layout: WorkspaceLayout) -> None:
        self._store = store
        self._layout = layout
        # One catalog over this reader's connection, sharing its lifetime: a
        # fresh one per access would mean a fresh lock per access, which
        # guards nothing.
        self._catalog = AssetStore(store=store, owns_store=False)

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
            Only runs started at or after this ISO-8601 timestamp. Compared as
            text against the stored timestamps, so a date alone is a valid
            bound; anything ISO-8601 cannot parse is refused rather than
            silently matching nothing.
        limit : int, optional
            Most rows to return.

        Returns
        -------
        list[RunRow]

        Raises
        ------
        ValueError
            If *since* is not an ISO-8601 timestamp.
        """
        if since is not None and parse_timestamp(since) is None:
            raise ValueError(
                f"{since!r} is not an ISO-8601 timestamp. Give a date (2026-08-01) or a "
                "date and time (2026-08-01T09:30)."
            )
        clauses: list[str] = []
        params: list[Any] = []
        if workflow is not None:
            clauses.append("workflow LIKE ? ESCAPE '\\'")
            params.append(_like_suffix(workflow))
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
                duration_s=duration_seconds(row["started_at"], row["finished_at"]),
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

    def task_history(self, name: str, *, limit: int = 20) -> list[TaskRow]:
        """Return every run of one task, newest first.

        The task is matched on its display label as well as its name, so the
        label ``ginkgo run`` printed for a fanned-out branch is what a user can
        ask about afterwards.

        Parameters
        ----------
        name : str
            The task's name, its base name, or the display label of one branch.
        limit : int, optional
            Most rows to return.

        Returns
        -------
        list[TaskRow]
            Empty when no run has a task by that name.
        """
        # Ordered by the run rather than by the task: a cached task never
        # started, so its own timestamp is null and would sort it out of the
        # history it belongs to.
        rows = self._store.query(
            "SELECT t.run_id, t.task_id, t.name, t.display_label, t.status, t.cached, "
            "t.cache_key, t.started_at, t.finished_at, t.attempts "
            "FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            "WHERE t.name = ? OR t.display_label = ? OR t.name LIKE ? ESCAPE '\\' "
            "ORDER BY r.started_at DESC, t.run_id DESC, t.task_id LIMIT ?",
            (name, name, _like_suffix(f".{name}"), limit),
        )
        return [
            TaskRow(
                run_id=str(row["run_id"]),
                task_id=str(row["task_id"]),
                name=str(row["name"]),
                display_label=row["display_label"],
                status=str(row["status"]),
                cached=bool(row["cached"]),
                cache_key=row["cache_key"],
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                duration_s=duration_seconds(row["started_at"], row["finished_at"]),
                attempts=int(row["attempts"] or 0),
            )
            for row in rows
        ]

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

    def _cache_key_components(self, cache_key: str) -> dict[str, Any]:
        """Return the labelled components of one entry's cache key.

        Derived from the entry's own row by the same function that labels the
        payload the key is hashed from, so the two cannot drift and no second
        table has to be kept in step.
        """
        rows = self._store.query(
            f"SELECT {', '.join(ENTRY_COLUMNS)} FROM cache_entries WHERE cache_key = ?",  # noqa: S608
            (cache_key,),
        )
        if not rows:
            return {}
        return key_components(CacheEntry.from_row(rows[0]).as_meta())

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

    def _previous_cache_key(self, *, run_id: str, task_id: str) -> tuple[str, str] | None:
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

    def explain_rerun(self, run_id: str, task_id: str) -> RerunExplanation:
        """Explain why one task of a run executed rather than serving from cache.

        Parameters
        ----------
        run_id : str
            The run being explained.
        task_id : str
            The task within it.

        Returns
        -------
        RerunExplanation
            The moved cache-key components, with the entry they were compared
            against; or a bare reason where no comparison was possible.

        Raises
        ------
        KeyError
            If the run has no such task.
        """
        rows = self._store.query(
            "SELECT name, display_label, cache_key, status FROM tasks "
            "WHERE run_id = ? AND task_id = ?",
            (run_id, task_id),
        )
        if not rows:
            raise KeyError(f"{run_id} has no task {task_id}")
        task = rows[0]
        cache_key = task["cache_key"]
        identity: dict[str, Any] = {
            "task_id": task_id,
            "task_name": str(task["name"]),
            "display_label": task["display_label"],
            "cache_key": cache_key,
        }
        if task["status"] == "cached":
            return RerunExplanation(**identity, reason="all_inputs_match", details=[])

        current = self._cache_key_components(cache_key) if isinstance(cache_key, str) else {}
        if not current:
            return RerunExplanation(**identity, reason="no_entry_for_key", details=[])

        prior = self._previous_cache_key(run_id=run_id, task_id=task_id)
        if prior is None:
            return RerunExplanation(**identity, reason="no_prior_entry", details=[])

        prior_key, strategy = prior
        components = _diff_key_components(
            current=current, prior=self._cache_key_components(prior_key)
        )
        details = _coarse_reasons(components)
        return RerunExplanation(
            **identity,
            reason=details[0],
            details=details,
            compared_with={"cache_key": prior_key, "strategy": strategy},
            components=components,
        )

    # -- raw SQL -------------------------------------------------------------

    def sql(
        self, query: str, params: Sequence[Any] = (), *, limit: int = SQL_ROW_LIMIT
    ) -> SqlResult:
        """Run one read-only statement against the ledger.

        For the question no method here answers. The tables are described in
        ``docs/architecture/store.md``; they are versioned but not stable, so a
        query written against them may need rewriting after an upgrade.

        Three things are refused, so that a mistake is reported rather than
        performed: a statement that is not a read, more than one statement, and
        more rows than *limit*. The row cap is applied while fetching from the
        cursor rather than by wrapping the statement in a ``LIMIT``, so the SQL
        that runs is the SQL the caller wrote and any error names their text
        rather than ginkgo's rewrite of it.

        Parameters
        ----------
        query : str
            One ``SELECT`` (or ``WITH``, ``VALUES``, ``EXPLAIN``). A trailing
            semicolon is allowed; a second statement is not.
        params : Sequence[Any], optional
            Values for the statement's ``?`` placeholders. Use these rather
            than formatting values into *query*.
        limit : int, optional
            Most rows to return. :data:`SQL_ROW_LIMIT` by default.

        Returns
        -------
        SqlResult
            The column names, the rows, and whether the limit cut them short.

        Raises
        ------
        StoreError
            If the statement writes, is not one statement, or the database
            rejects it — an unknown table or column, or a syntax error.
        """
        statement = query.strip().rstrip(";").strip()
        refusal = _refusal(statement)
        if refusal is not None:
            raise StoreError(refusal)
        try:
            columns, rows = self._store.select_with_columns(statement, params, limit=limit)
        except sqlite3.ProgrammingError as exc:
            raise StoreError(
                "That is more than one statement. `ginkgo query` runs one; give it "
                "the SELECT you want and drop the rest."
            ) from exc
        except sqlite3.Error as exc:
            raise StoreError(
                f"SQLite rejected the query: {exc}. The table and column names are "
                "listed in the provenance store documentation."
            ) from exc
        return SqlResult(
            columns=columns,
            rows=rows[:limit],
            truncated=len(rows) > limit,
            limit=limit,
        )

    # -- assets and lineage --------------------------------------------------

    @property
    def catalog(self) -> AssetStore:
        """The asset catalog over this reader's connection."""
        return self._catalog

    def lineage(
        self,
        asset_key: str,
        version_id: str | None = None,
        *,
        direction: str = "upstream",
        depth: int | None = None,
    ) -> LineageGraph:
        """Walk the ``derived_from`` edges around one asset version.

        Parameters
        ----------
        asset_key : str
            ``'<kind>:<name>'``, as ``ginkgo asset ls`` prints it.
        version_id : str | None
            A version id or alias. The latest version when omitted.
        direction : str
            ``"upstream"`` for what this version was built from,
            ``"downstream"`` for what was built from it.
        depth : int | None
            Stop after this many hops. Unlimited when omitted.

        Returns
        -------
        LineageGraph

        Raises
        ------
        FileNotFoundError
            If the asset or version is unknown.
        ValueError
            If *direction* is neither ``"upstream"`` nor ``"downstream"``.
        """
        if direction not in {"upstream", "downstream"}:
            raise ValueError(f"direction must be 'upstream' or 'downstream', got {direction!r}")
        catalog = self.catalog
        root = catalog.resolve_version(key=AssetKey.parse(asset_key), selector=version_id)

        versions: dict[str, AssetVersion] = {root.version_id: root}
        edges: set[tuple[str, str]] = set()
        frontier = [root.version_id]
        hops = 0
        while frontier and (depth is None or hops < depth):
            next_frontier: list[str] = []
            for current in frontier:
                step = (
                    catalog.children_of(current)
                    if direction == "downstream"
                    else catalog.parents_of(current)
                )
                for other in step:
                    edges.add((current, other) if direction == "downstream" else (other, current))
                    if other in versions:
                        continue
                    version = catalog.version_by_id(other)
                    if version is None:
                        continue
                    versions[other] = version
                    next_frontier.append(other)
            frontier = next_frontier
            hops += 1
        return LineageGraph(
            root=root,
            direction=direction,
            versions=versions,
            edges=tuple(sorted(edges)),
        )

    def why(self, path_or_artifact_id: str) -> Provenance:
        """Return what produced an artifact, named by path or by id.

        A path is resolved through ``materializations`` — the stat log of every
        artifact ginkgo has written to a working directory — and then answered
        exactly as an artifact id is.

        Parameters
        ----------
        path_or_artifact_id : str
            A file path ginkgo materialized, or an artifact id.

        Returns
        -------
        Provenance

        Raises
        ------
        FileNotFoundError
            If nothing in the ledger matches.
        """
        artifact_id, path = self._resolve_artifact(path_or_artifact_id)
        output = self._store.query(
            "SELECT run_id, task_id, asset_key, asset_version_id FROM task_outputs "
            "WHERE artifact_id = ? ORDER BY run_id DESC LIMIT 1",
            (artifact_id,),
        )
        asset = self._store.query(
            "SELECT asset_key, version_id, run_id, task_id, cache_key FROM asset_versions "
            "WHERE artifact_id = ? ORDER BY created_at DESC LIMIT 1",
            (artifact_id,),
        )
        cached = self._store.query(
            "SELECT cache_key FROM cache_artifacts WHERE artifact_id = ? LIMIT 1",
            (artifact_id,),
        )
        run_id = _first(output, "run_id") or _first(asset, "run_id")
        task_id = _first(output, "task_id") or _first(asset, "task_id")
        cache_key = _first(cached, "cache_key") or _first(asset, "cache_key")
        if run_id is None and cache_key is not None:
            produced_by = self._store.query(
                "SELECT run_id, task_id FROM tasks WHERE cache_key = ? "
                "ORDER BY started_at LIMIT 1",
                (cache_key,),
            )
            run_id = _first(produced_by, "run_id")
            task_id = _first(produced_by, "task_id")
        return Provenance(
            artifact_id=artifact_id,
            path=path,
            run_id=run_id,
            task_id=task_id,
            task_name=self._task_name(run_id=run_id, task_id=task_id),
            cache_key=cache_key,
            asset_key=_first(asset, "asset_key") or _first(output, "asset_key"),
            version_id=_first(asset, "version_id") or _first(output, "asset_version_id"),
            inputs=self._task_inputs(run_id=run_id, task_id=task_id),
        )

    def _resolve_artifact(self, path_or_artifact_id: str) -> tuple[str, str | None]:
        """Return the artifact id *path_or_artifact_id* names, and the path used."""
        resolved = str(Path(path_or_artifact_id).expanduser().resolve())
        rows = self._store.query(
            "SELECT artifact_id FROM materializations WHERE path = ?", (resolved,)
        )
        if rows:
            return str(rows[0]["artifact_id"]), resolved
        known = self._store.query(
            "SELECT artifact_id FROM artifacts WHERE artifact_id = ?", (path_or_artifact_id,)
        )
        if known:
            return path_or_artifact_id, None
        raise FileNotFoundError(
            f"Nothing in the ledger produced {path_or_artifact_id!r}: it is neither an "
            "artifact id nor a path ginkgo has materialized."
        )

    def _task_name(self, *, run_id: str | None, task_id: str | None) -> str | None:
        """Return the display label or name of one task."""
        if run_id is None or task_id is None:
            return None
        rows = self._store.query(
            "SELECT coalesce(display_label, name) AS label FROM tasks "
            "WHERE run_id = ? AND task_id = ?",
            (run_id, task_id),
        )
        return str(rows[0]["label"]) if rows else None

    def _task_inputs(
        self, *, run_id: str | None, task_id: str | None
    ) -> tuple[dict[str, Any], ...]:
        """Return the recorded inputs of one task, one mapping per parameter."""
        if run_id is None or task_id is None:
            return ()
        rows = self._store.query(
            "SELECT param, value_type, value_summary, digest, artifact_id, asset_key, "
            "asset_version_id, remote_uri FROM task_inputs WHERE run_id = ? AND task_id = ? "
            "ORDER BY param, position",
            (run_id, task_id),
        )
        return tuple(dict(row) for row in rows)

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
    missing_ok: bool = False,
) -> Query:
    """Open a workspace's ledger for reading.

    Parameters
    ----------
    layout : WorkspaceLayout | None, optional
        The workspace. Defaults to the current directory's.
    readonly : bool, optional
        Keep this ``True`` unless you are the run that owns the write lock.
    missing_ok : bool, optional
        Read an empty ledger instead of raising when the workspace has none.
        What a listing wants: a workspace nobody has run anything in holds no
        assets and no runs, and that is an answer rather than a failure. The
        empty ledger is in memory, so a read path still never creates a file.

    Returns
    -------
    Query

    Raises
    ------
    FileNotFoundError
        If the workspace has no ledger yet — which is to say no runs — and
        *missing_ok* is False.
    StoreError
        If the database exists but cannot be read.
    """
    layout = layout if layout is not None else WorkspaceLayout.relative()
    path = Path(layout.db)
    if readonly and not path.is_file():
        if not missing_ok:
            raise FileNotFoundError(f"No runs found: there is no provenance database at {path}")
        # Write-mode, because the schema has to exist before anything can be
        # selected from it — then closed to writes, so an empty workspace
        # refuses the same statements a populated one does.
        empty = open_store(MEMORY)
        empty.restrict_to_reads()
        return Query(empty, layout=layout)
    return Query(open_store(path, readonly=readonly), layout=layout)


def _diff_key_components(
    *, current: dict[str, Any], prior: dict[str, Any]
) -> list[dict[str, Any]]:
    """Return the cache-key components that differ between two entries."""
    differences: list[dict[str, Any]] = []
    for name in sorted(set(current) | set(prior)):
        # A component absent from one side is a parameter that came or went.
        if name not in current:
            differences.append({"component": name, "status": "removed", "prior": prior[name]})
        elif name not in prior:
            differences.append({"component": name, "status": "added", "current": current[name]})
        elif current[name] != prior[name]:
            differences.append(
                {
                    "component": name,
                    "status": "changed",
                    "current": current[name],
                    "prior": prior[name],
                }
            )
    return differences


def _coarse_reasons(components: list[dict[str, Any]]) -> list[str]:
    """Return the summary reason codes implied by a component diff.

    A moved component no code covers falls back to ``cache_key_changed``; the
    component list names it either way.
    """
    moved = {str(component["component"]) for component in components}
    reasons = []
    if moved & {"source_hash", "extra_source_hash"}:
        reasons.append("source_hash_changed")
    if "version" in moved:
        reasons.append("version_bump")
    if moved & {"env", "env_hash.pixi_lock"}:
        reasons.append("env_changed")
    if any(name.startswith("inputs") for name in moved):
        reasons.append("input_changed")
    return reasons or ["cache_key_changed"]


def _top_level_words(statement: str) -> Iterator[str]:
    """Yield the upper-cased bare words of *statement* outside parentheses.

    Enough of a scanner to tell a statement's verbs from its data: string
    literals, quoted identifiers and comments are skipped whole, so a row whose
    value is ``'delete'`` is never mistaken for a verb, and anything nested in
    parentheses is skipped because a common table expression's own ``SELECT``
    is not the statement's verb.
    """
    closing = {"'": "'", '"': '"', "`": "`", "[": "]"}
    index = 0
    depth = 0
    while index < len(statement):
        char = statement[index]
        if char in closing:
            end = statement.find(closing[char], index + 1)
            index = len(statement) if end == -1 else end + 1
            continue
        if statement.startswith("--", index):
            end = statement.find("\n", index)
            index = len(statement) if end == -1 else end + 1
            continue
        if statement.startswith("/*", index):
            end = statement.find("*/", index + 2)
            index = len(statement) if end == -1 else end + 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char.isalpha() or char == "_":
            end = index
            while end < len(statement) and (statement[end].isalnum() or statement[end] == "_"):
                end += 1
            if depth == 0:
                yield statement[index:end].upper()
            index = end
            continue
        index += 1


def _refusal(statement: str) -> str | None:
    """Return why :meth:`Query.sql` will not run *statement*, or ``None``.

    The message is the whole report, so it names the verb the user wrote rather
    than describing the rule in the abstract.
    """
    words = list(_top_level_words(statement))
    if not words:
        return "No SQL to run."
    verb = words[0]
    if verb not in _READ_VERBS:
        return (
            f"{verb} is not a read. `ginkgo query` runs one SELECT (or WITH, VALUES, "
            "EXPLAIN) against a read-only connection; nothing may change the ledger."
        )
    if verb != "WITH":
        return None
    # A WITH clause is only as read-only as the statement it introduces:
    # `WITH t AS (SELECT 1) DELETE FROM runs` leads with a verb this allows.
    body = next((word for word in words[1:] if word in _BODY_VERBS), None)
    if body is None or body in {"SELECT", "VALUES"}:
        return None
    return (
        f"That WITH clause ends in {body}, which is not a read. `ginkgo query` runs "
        "one SELECT against a read-only connection; nothing may change the ledger."
    )


def _unique_columns(columns: Sequence[str]) -> tuple[str, ...]:
    """Return *columns* with repeats suffixed, so each names one value.

    ``SELECT r.run_id, t.run_id`` selects two columns of the same name. A
    mapping keyed on them would keep only the last, so the second becomes
    ``run_id_2`` — the position is what tells them apart, and the position is
    what the suffix carries.
    """
    seen: dict[str, int] = {}
    unique: list[str] = []
    for column in columns:
        seen[column] = seen.get(column, 0) + 1
        unique.append(column if seen[column] == 1 else f"{column}_{seen[column]}")
    return tuple(unique)


def _like_suffix(value: str) -> str:
    """Return the LIKE pattern matching anything ending in *value*.

    ``%`` and ``_`` are escaped, so a user asking for a task literally named
    ``%`` is answered with that task rather than with every row in the table.
    The caller must pair this with ``ESCAPE '\\'``.
    """
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}"


def _first(rows: list[Any], column: str) -> str | None:
    """Return one column of the first row, or ``None`` when there is none."""
    if not rows:
        return None
    value = rows[0][column]
    return str(value) if value is not None else None
