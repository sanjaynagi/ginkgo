"""Read the provenance ledger.

The public way to ask ginkgo what has happened. Every CLI read path goes
through here, so there is one place that knows the schema and one place to
change when it moves. Readers open the database read-only, which means they
work while a run is writing and can never change it out from under one.

Every method returns either a core type ginkgo already models — a
:class:`~ginkgo.runtime.run_summary.RunSummary` — or a frozen row dataclass
declared here for a shape that has no model elsewhere.

The database schema is versioned but **not stable**. :meth:`Query.sql` hands
out raw SQL over the tables :meth:`Query.schema` names — they are also described
in ``docs/architecture/store.md`` — and those tables change between ginkgo versions without a deprecation period; a
query written against them may need rewriting after an upgrade. The methods on
:class:`Query` are the surface that is kept working.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from ginkgo.core.resources import memory_gib
from ginkgo.formatting import duration_seconds, parse_timestamp
from ginkgo.runtime.caching.cache import coarse_reasons, diff_key_components, key_components
from ginkgo.runtime.caching.index import ENTRY_COLUMNS, PRIOR_ENTRY_WINDOW, CacheEntry
from ginkgo.runtime.module_loader import task_base_name
from ginkgo.runtime.run_summary import RunSummary
from ginkgo.runtime.rundir import runs_without_ledger_warning
from ginkgo.store.errors import StoreError, UninitializedStoreError
from ginkgo.store.protocol import ProvenanceStore, event_fields
from ginkgo.store.sqlite import MEMORY, open_store
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = [
    "CacheEntryRow",
    "CacheStats",
    "EventRow",
    "Provenance",
    "Query",
    "RUN_STATUSES",
    "RerunExplanation",
    "RunRow",
    "SqlResult",
    "TaskRow",
    "open",
]

SQL_ROW_LIMIT = 1000
"""Rows :meth:`Query.sql` returns when the caller names no limit of its own."""

RUN_STATUSES = ("running", "succeeded", "failed", "cancelled")
"""Every status the ledger records for a run, for filters to validate against."""

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
        ``running``, ``succeeded``, ``failed``, or ``cancelled``.
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
        The task's qualified name, as the ledger records it.
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
    peak_rss_bytes : int | None
        Highest resident set size measured across the task's attempts, or
        ``None`` when nothing was measured — a cache hit, a task that never
        ran, or a run predating the measurement.
    cpu_seconds : float | None
        CPU time summed over the task's attempts and subprocesses.
    declared_threads : int | None
        Threads the task declared, after site overrides.
    declared_memory_gb : float | None
        Memory the task declared, after site overrides and before any retry
        escalation, in GiB as written: ``512Mi`` is ``0.5``. This is the
        number a user would edit.
    effective_memory_gb : float | None
        Whole GiB the last attempt actually ran against: the declaration
        rounded up by the scheduler, or more when ``memory_retry_multiplier``
        escalated a retry.
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
    peak_rss_bytes: int | None = None
    cpu_seconds: float | None = None
    declared_threads: int | None = None
    declared_memory_gb: float | None = None
    effective_memory_gb: float | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return the row as JSON-ready data."""
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "task_name": task_base_name(self.name),
            "qualified_name": self.name,
            "display_label": self.display_label,
            "status": self.status,
            "cached": self.cached,
            "cache_key": self.cache_key,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": self.duration_s,
            "attempts": self.attempts,
            "peak_rss_bytes": self.peak_rss_bytes,
            "cpu_seconds": self.cpu_seconds,
            "declared_threads": self.declared_threads,
            "declared_memory_gb": self.declared_memory_gb,
            "effective_memory_gb": self.effective_memory_gb,
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


_ENTRY_ARTIFACT_BYTES = (
    "coalesce((SELECT sum(a.size) FROM artifacts a WHERE a.artifact_id IN ("
    "  SELECT ca.artifact_id FROM cache_artifacts ca WHERE ca.cache_key = c.cache_key)"
    "  AND coalesce(a.storage_backend, 'local') = 'local'), 0)"
)
"""SQL for the local artifact bytes one cache entry ``c`` names, each counted once."""

_SIZED_ENTRIES = (
    "WITH sized AS (SELECT c.function, c.hit_count, "
    f"coalesce(c.size_bytes, 0) + {_ENTRY_ARTIFACT_BYTES} AS bytes FROM cache_entries c)"
)
"""A ``sized`` CTE: every cache entry with the bytes it accounts for."""


@dataclass(frozen=True, kw_only=True)
class CacheEntryRow:
    """One cache entry, as the cache commands display it.

    Attributes
    ----------
    cache_key : str
        The entry's content-addressed key.
    function : str
        Qualified name of the task that wrote it, as the ledger records it.
    output_bytes : int
        Bytes the entry's own stored output occupies.
    artifact_bytes : int
        Bytes of the local artifacts the entry names: the files and folders
        it restores. An artifact two entries share counts toward both.
    created_at, last_hit_at : str | None
        ISO-8601 UTC timestamps; ``last_hit_at`` is ``None`` until a run hits it.
    hit_count : int
        Runs that have served from this entry.
    display_label : str | None
        The fan-out label of the newest recorded task with this key, such as
        ``clean[north]``; ``None`` when that task had none.
    """

    cache_key: str
    function: str
    output_bytes: int
    artifact_bytes: int
    created_at: str | None
    hit_count: int
    last_hit_at: str | None
    display_label: str | None

    @property
    def size_bytes(self) -> int:
        """Bytes the entry accounts for: its output and the artifacts it names."""
        return self.output_bytes + self.artifact_bytes

    def to_payload(self) -> dict[str, Any]:
        """Return the entry as ``ginkgo cache ls --json`` prints it."""
        return {
            "cache_key": self.cache_key,
            "task_name": task_base_name(self.function),
            "qualified_name": self.function,
            "display_label": self.display_label,
            "size_bytes": self.size_bytes,
            "output_bytes": self.output_bytes,
            "artifact_bytes": self.artifact_bytes,
            "created_at": self.created_at,
            "hit_count": self.hit_count,
            "last_hit_at": self.last_hit_at,
        }


@dataclass(frozen=True, kw_only=True)
class CacheStats:
    """What the cache holds, in aggregate.

    Attributes
    ----------
    entries : int
        Entries in the index.
    total_bytes : int
        Bytes across all of them, each entry counting its output and the
        artifacts it names (see :attr:`CacheEntryRow.size_bytes`).
    artifact_bytes : int
        Bytes in the local artifact store, counted once each, including
        bytes no entry names.
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
    artifact_bytes: int
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
            artifact_bytes=0,
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
    task_name : str
        The task's base name; *display_label* names the branch.
    reason : str
        The summary code: ``all_inputs_match``, ``rerun_requested`` (forced
        by ``--rerun``), ``no_entry_for_key`` (the node ran under this key
        before and its entry is gone), ``prior_entry_removed`` (the key moved
        but the entry it moved from is gone), ``no_prior_entry``,
        ``task_moved``, or the first of *details*. A node with no earlier
        entry of its own is ``no_prior_entry`` even when a sibling's entry
        was compared against.
    details : list[str]
        Every summary code the component diff implies, in the order source,
        version, environment, inputs. A source change where the task's own
        text is unchanged is ``module_source_changed``: something else in its
        module or its local imports moved.
    compared_with : dict[str, str] | None
        The entry compared against and how it was found — ``cache_key`` and a
        ``strategy`` of ``same_node`` (this node's previous entry) or
        ``most_similar_by_function`` (the earlier entry of the same task that
        differs from this one least, for a node that has none) — or ``None`` when there was nothing to compare
        with.
    components : list[dict[str, Any]]
        One entry per cache-key component that differs. A changed input whose
        values are short scalars also carries ``prior_value`` and ``current_value``.
    moved_from : str | None
        With reason ``task_moved``: the qualified name (``module.task``) an
        earlier run of the same workflow knew this task by. A task's cache
        identity includes its module and name, so moving or renaming it
        starts it without a cache entry.
    input_labels : dict[str, str]
        Each input parameter's cache-tracking label (``content``, ``version``,
        ``path``, ``value``, ``output``, ``untracked`` — see
        ``ginkgo.runtime.task_validation.label_input_value``), read from this
        task's own ``task_inputs`` row. Empty for a task run before this label
        existed; ``ginkgo cache explain`` shows nothing for those rather than
        guessing.
    """

    task_id: str | None
    task_name: str
    display_label: str | None
    cache_key: str | None
    reason: str
    details: list[str]
    compared_with: dict[str, str] | None = None
    components: list[dict[str, Any]] = field(default_factory=list)
    moved_from: str | None = None
    input_labels: dict[str, str] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Return the JSON shape ``ginkgo cache explain`` prints."""
        payload: dict[str, Any] = {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "display_label": self.display_label,
            "cache_key": self.cache_key,
            "reason": self.reason,
        }
        if self.input_labels:
            payload["input_labels"] = self.input_labels
        if self.moved_from is not None:
            payload["moved_from"] = self.moved_from
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
class Provenance:
    """Where one artifact came from.

    Parameters
    ----------
    artifact_id : str
        The artifact asked about.
    path : str | None
        The path asked about, when the question was a path.
    containing_output : str | None
        The recorded output folder holding *path*, when *path* has no record of
        its own; the rest describes that folder.
    run_id, task_id, task_name : str | None
        The task that produced it.
    cache_key : str | None
        The cache entry holding it, if it came out of the cache.
    inputs : tuple[dict[str, Any], ...]
        The producing task's recorded inputs, one mapping per parameter.
    """

    artifact_id: str
    path: str | None = None
    containing_output: str | None = None
    run_id: str | None = None
    task_id: str | None = None
    task_name: str | None = None
    cache_key: str | None = None
    inputs: tuple[dict[str, Any], ...] = ()

    def to_payload(self) -> dict[str, Any]:
        """Return the provenance as JSON-ready data."""
        return {
            "artifact_id": self.artifact_id,
            "path": self.path,
            "containing_output": self.containing_output,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "task_name": self.task_name,
            "cache_key": self.cache_key,
            "inputs": [dict(entry) for entry in self.inputs],
        }


@dataclass(frozen=True, kw_only=True)
class _PriorEntry:
    """The earlier cache key ``ginkgo cache explain`` compares a task against.

    Attributes
    ----------
    cache_key : str
        The earlier key.
    strategy : str
        ``same_node`` (this node's previous run) or ``most_similar_by_function``
        (the closest entry of the same task, for a node with no history).
    task_name : str
        The qualified name the earlier run knew the task by.
    """

    cache_key: str
    strategy: str
    task_name: str


def _own_source_unchanged(*, current: CacheEntry, prior: CacheEntry) -> bool:
    """Return whether two entries record the same task text.

    Their source hashes can still differ: each also covers the task's module
    and local imports, so an edit to a helper or constant beside it moves it.
    """
    current_own = (current.extra or {}).get("own_source_hash")
    return current_own is not None and current_own == (prior.extra or {}).get("own_source_hash")


def _add_scalar_values(
    components: list[dict[str, Any]], *, current: CacheEntry, prior: CacheEntry
) -> None:
    """Give each changed input whose two values are short scalars those values.

    A digest says only that an input moved; ``10 → 3`` says how. Files, folders,
    redacted secrets and long strings keep their digests alone.
    """
    prior_inputs = prior["inputs"] or {}
    current_inputs = current["inputs"] or {}
    for item in components:
        name = str(item["component"]).removeprefix("inputs.")
        if item["status"] != "changed" or name == item["component"]:
            continue
        prior_value, current_value = prior_inputs.get(name), current_inputs.get(name)
        if _is_short_scalar(item["prior"], prior_value) and _is_short_scalar(
            item["current"], current_value
        ):
            item.update(prior_value=prior_value, current_value=current_value)


def _is_short_scalar(record: Any, value: Any) -> bool:
    """Return whether an input hashed as *record* reads inline as *value*.

    The hash record's type, not the value, decides: a ``str`` naming a file
    is hashed by its contents (#307), so its path is not what changed.
    """
    return (
        isinstance(record, dict)
        and record.get("type") in {"bool", "int", "float", "str"}
        and isinstance(value, (bool, int, float, str))
        and not (isinstance(value, str) and len(value) > 40)
    )


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

    def measured_executions(self, cache_keys: Iterable[str]) -> dict[str, TaskRow]:
        """Return, per cache key, the newest execution that produced and measured it.

        A cache hit measures nothing: the result it serves was made, and its
        memory measured, by an earlier run. This finds that run.

        Parameters
        ----------
        cache_keys : Iterable[str]
            The cache keys to look up.

        Returns
        -------
        dict[str, TaskRow]
            Keyed by cache key; a key no measured execution produced is absent.
        """
        keys = sorted(set(cache_keys))
        if not keys:
            return {}
        placeholders = ", ".join("?" for _ in keys)
        rows = self._store.query(
            f"SELECT {_TASK_COLUMNS} "
            "FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            f"WHERE t.cache_key IN ({placeholders}) AND t.cached = 0 "
            "AND t.status = 'succeeded' AND t.peak_rss_bytes IS NOT NULL "
            "ORDER BY r.started_at DESC, t.run_id DESC",
            tuple(keys),
        )
        found: dict[str, TaskRow] = {}
        for row in rows:
            found.setdefault(str(row["cache_key"]), _task_row(row))
        return found

    def task_history(
        self, name: str, *, limit: int | None = 20, include_unstarted: bool = False
    ) -> list[TaskRow]:
        """Return every execution of one task in its most recent runs, newest first.

        The task is matched on its base name, its qualified name, or the
        display label of one branch, so the label ``ginkgo run`` printed for
        a fanned-out branch is what a user can ask about afterwards. A
        fan-out puts several rows in one run; *limit* counts runs, so a run's
        branches are never split across the cut.

        Parameters
        ----------
        name : str
            The task's base name, its qualified name, or the display label of
            one branch.
        limit : int | None, optional
            Most runs to return rows from; ``None`` for the task's whole
            history, which is what a distribution over it needs.
        include_unstarted : bool, optional
            Also return runs in which the task never started, such as one
            skipped after an upstream failure. Left out by default: they hold
            no history of the task.

        Returns
        -------
        list[TaskRow]
            Empty when no run has a task by that name.
        """
        # Ordered by the run rather than by the task: a cached task never
        # started, so its own timestamp is null and would sort it out of the
        # history it belongs to.
        rows = self._store.query(
            f"SELECT {_TASK_COLUMNS} "
            "FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            f"WHERE ({_TASK_MATCH}){'' if include_unstarted else _STARTED_ONLY} "
            "ORDER BY r.started_at DESC, t.run_id DESC, t.task_id",
            (name, name, _like_suffix(f".{name}")),
        )
        history = [_task_row(row) for row in rows]
        if limit is None:
            return history
        kept = set(list(dict.fromkeys(row.run_id for row in history))[:limit])
        return [row for row in history if row.run_id in kept]

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
            "SELECT seq, run_id, ts, type, v, task_id, attempt, cache_key, payload FROM events "
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
                payload=event_fields(
                    run_id=row["run_id"],
                    ts=row["ts"],
                    type=row["type"],
                    v=row["v"],
                    task_id=row["task_id"],
                    attempt=row["attempt"],
                    cache_key=row["cache_key"],
                    payload=row["payload"],
                ),
            )

    # -- cache ---------------------------------------------------------------

    def cache_entries(self, *, task: str | None = None) -> list[CacheEntryRow]:
        """Return cache entries, newest first.

        Parameters
        ----------
        task : str | None, optional
            Only entries written by this task, named as ``history`` takes it:
            its base name, its qualified name, or the display label of the
            branch that recorded the entry.

        Returns
        -------
        list[CacheEntryRow]
        """
        where = ""
        params: tuple[str, ...] = ()
        if task is not None:
            where = (
                "WHERE c.function = ? OR c.function LIKE ? ESCAPE '\\' OR c.cache_key IN ("
                "  SELECT cache_key FROM tasks WHERE display_label = ?)"
            )
            params = (task, _like_suffix(f".{task}"), task)
        # The entry does not record which branch wrote it, so the label comes
        # from the newest task the ledger recorded with the same key.
        rows = self._store.query(
            "SELECT c.cache_key, c.function, c.size_bytes, c.created_at, c.hit_count, "
            f"c.last_hit_at, {_ENTRY_ARTIFACT_BYTES} AS artifact_bytes, ("
            "  SELECT t.display_label FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            "  WHERE t.cache_key = c.cache_key ORDER BY r.started_at DESC LIMIT 1"
            ") AS display_label "
            f"FROM cache_entries c {where} ORDER BY c.created_at DESC, c.cache_key",  # noqa: S608
            params,
        )
        return [
            CacheEntryRow(
                cache_key=str(row["cache_key"]),
                function=str(row["function"]),
                output_bytes=int(row["size_bytes"] or 0),
                artifact_bytes=int(row["artifact_bytes"]),
                created_at=row["created_at"],
                hit_count=int(row["hit_count"] or 0),
                last_hit_at=row["last_hit_at"],
                display_label=row["display_label"],
            )
            for row in rows
        ]

    def _cache_entry(self, cache_key: str) -> CacheEntry | None:
        """Return one entry's row, or ``None`` once it has been removed."""
        rows = self._store.query(
            f"SELECT {', '.join(ENTRY_COLUMNS)} FROM cache_entries WHERE cache_key = ?",  # noqa: S608
            (cache_key,),
        )
        return CacheEntry.from_row(rows[0]) if rows else None

    def _cache_key_components(self, cache_key: str) -> dict[str, Any]:
        """Return the labelled components of one entry's cache key.

        Derived from the entry's own row by the same function that labels the
        payload the key is hashed from, so the two cannot drift and no second
        table has to be kept in step.
        """
        entry = self._cache_entry(cache_key)
        return {} if entry is None else key_components(entry.as_meta())

    def cache_stats(self) -> CacheStats:
        """Return entry counts, bytes and hit statistics for the whole cache."""
        totals = self._store.query(
            f"{_SIZED_ENTRIES} SELECT count(*) AS entries, "  # noqa: S608
            "coalesce(sum(bytes), 0) AS total_bytes, "
            "coalesce(sum(hit_count = 0), 0) AS never_hit, "
            "coalesce(sum(CASE WHEN hit_count = 0 THEN bytes ELSE 0 END), 0) "
            "AS never_hit_bytes FROM sized"
        )[0]
        histogram = self._store.query(
            "SELECT hit_count, count(*) AS n FROM cache_entries GROUP BY hit_count "
            "ORDER BY hit_count"
        )
        functions = self._store.query(
            f"{_SIZED_ENTRIES} SELECT function, count(*) AS n, "  # noqa: S608
            "coalesce(sum(bytes), 0) AS bytes "
            "FROM sized GROUP BY function ORDER BY bytes DESC, function LIMIT 10"
        )
        artifacts = self._store.query(
            "SELECT coalesce(sum(size), 0) AS artifact_bytes FROM artifacts "
            "WHERE coalesce(storage_backend, 'local') = 'local'"
        )[0]
        return CacheStats(
            entries=int(totals["entries"]),
            total_bytes=int(totals["total_bytes"]),
            artifact_bytes=int(artifacts["artifact_bytes"]),
            never_hit=int(totals["never_hit"]),
            never_hit_bytes=int(totals["never_hit_bytes"]),
            hit_histogram={int(row["hit_count"]): int(row["n"]) for row in histogram},
            top_functions=[
                (str(row["function"]), int(row["n"]), int(row["bytes"])) for row in functions
            ],
        )

    def _previous_cache_key(
        self, *, run_id: str, task_id: str, current: dict[str, Any]
    ) -> _PriorEntry | None:
        """Return the key this task's node last used, and how it was found.

        The comparison ``ginkgo cache explain`` wants is against the same node
        of the same workflow in the latest run before this one. That key can
        be this run's own: the node ran under it before, and the entry it left
        is gone. Where the node is new, the closest thing to a predecessor is
        whichever recent earlier entry of the same function differs from
        *current* least, and the caller is told which of the two it got so it
        does not read a fan-out sibling as a history (issue #223).

        Parameters
        ----------
        run_id : str
            The run being explained.
        task_id : str
            The task within it.
        current : dict[str, Any]
            The task's own cache-key components.

        Returns
        -------
        _PriorEntry | None
            The earlier key, or ``None`` when nothing earlier exists.
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
            "SELECT t.name, t.cache_key FROM tasks t JOIN runs r ON r.run_id = t.run_id "
            "WHERE r.workflow = ? AND r.started_at < ? AND t.cache_key IS NOT NULL "
            "AND coalesce(t.display_label, t.name) = ? "
            "ORDER BY r.started_at DESC LIMIT 1",
            (task["workflow"], task["started_at"], task["display_label"] or task["name"]),
        )
        if same_node:
            return _PriorEntry(
                cache_key=str(same_node[0]["cache_key"]),
                strategy="same_node",
                task_name=str(same_node[0]["name"]),
            )

        siblings = self._store.query(
            "SELECT cache_key FROM cache_entries WHERE function = ? AND cache_key != ? "
            "AND created_at < coalesce("
            "  (SELECT created_at FROM cache_entries WHERE cache_key = ?), created_at"
            ") ORDER BY created_at DESC LIMIT ?",
            (task["name"], task["cache_key"], task["cache_key"], PRIOR_ENTRY_WINDOW),
        )
        if not siblings:
            return None
        # min() keeps the first of equals, and the entries come newest first.
        closest = min(
            (str(row["cache_key"]) for row in siblings),
            key=lambda key: len(
                diff_key_components(current=current, prior=self._cache_key_components(key))
            ),
        )
        return _PriorEntry(
            cache_key=closest, strategy="most_similar_by_function", task_name=str(task["name"])
        )

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
            "task_name": task_base_name(str(task["name"])),
            "display_label": task["display_label"],
            "cache_key": cache_key,
            "input_labels": self._task_input_labels(run_id=run_id, task_id=task_id),
        }
        if task["status"] == "cached":
            return RerunExplanation(**identity, reason="all_inputs_match", details=[])
        # Asked before any key is compared: a forced task never looked.
        if self._rerun_requested(run_id=run_id, task_id=task_id):
            return RerunExplanation(**identity, reason="rerun_requested", details=[])

        current_entry = self._cache_entry(cache_key) if isinstance(cache_key, str) else None
        if current_entry is None:
            return RerunExplanation(**identity, reason="no_entry_for_key", details=[])
        current = key_components(current_entry.as_meta())

        prior = self._previous_cache_key(run_id=run_id, task_id=task_id, current=current)
        if prior is None:
            moved_from = self._moved_from(run_id=run_id, task_id=task_id)
            return RerunExplanation(
                **identity,
                reason="no_prior_entry" if moved_from is None else "task_moved",
                details=[],
                moved_from=moved_from,
            )
        compared_with = {"cache_key": prior.cache_key, "strategy": prior.strategy}
        if prior.cache_key == cache_key:
            # The node ran under this very key before, so nothing it is keyed
            # on changed: the entry that run left was removed (`cache prune`,
            # `cache clear`), or never written because the run failed.
            return RerunExplanation(**identity, reason="no_entry_for_key", details=[])
        prior_entry = self._cache_entry(prior.cache_key)
        if prior_entry is None:
            # Diffing against nothing would call every component "added".
            return RerunExplanation(
                **identity, reason="prior_entry_removed", details=[], compared_with=compared_with
            )

        components = diff_key_components(
            current=current, prior=key_components(prior_entry.as_meta())
        )
        _add_scalar_values(components, current=current_entry, prior=prior_entry)
        details = coarse_reasons(components)
        if _own_source_unchanged(current=current_entry, prior=prior_entry):
            details = [
                "module_source_changed" if code == "source_hash_changed" else code
                for code in details
            ]
        moved_from = (
            prior.task_name
            if prior.strategy == "same_node" and prior.task_name != task["name"]
            else None
        )
        if moved_from is not None:
            reason = "task_moved"
        elif prior.strategy == "same_node":
            reason = details[0]
        else:
            # A sibling's entry is the nearest thing to compare with, not a
            # predecessor: this node has no earlier entry, so nothing of its
            # own changed. The sibling diff stays as detail.
            reason = "no_prior_entry"
        return RerunExplanation(
            **identity,
            reason=reason,
            details=details,
            compared_with=compared_with,
            components=components,
            moved_from=moved_from,
        )

    def _rerun_requested(self, *, run_id: str, task_id: str) -> bool:
        """Return whether ``--rerun`` forced this task past its cache entry.

        Read from the task's cache-miss event, so it is unknown — ``False`` —
        once ``ginkgo db prune`` has dropped that run's events.
        """
        rows = self._store.query(
            "SELECT payload FROM events WHERE run_id = ? AND task_id = ? "
            "AND type = 'task_cache_miss'",
            (run_id, task_id),
        )
        return any(json.loads(row["payload"]).get("rerun_requested") for row in rows)

    def _moved_from(self, *, run_id: str, task_id: str) -> str | None:
        """Return the qualified name an earlier run of this workflow gave this task.

        Only asked once no earlier entry of the same node exists. A task of the
        same base name under another module, in the latest earlier run of the
        same workflow that had one, is this task before it moved. Failing
        that, a rename: the same node of the latest earlier run, under a name
        this run no longer has and with the same parameters.
        """
        rows = self._store.query(
            "SELECT t.name, r.workflow, r.started_at FROM tasks t "
            "JOIN runs r ON r.run_id = t.run_id WHERE t.run_id = ? AND t.task_id = ?",
            (run_id, task_id),
        )
        if not rows:
            return None
        name = str(rows[0]["name"])
        base_name = task_base_name(name)
        earlier = self._store.query(
            "SELECT DISTINCT t.name, r.started_at FROM tasks t "
            "JOIN runs r ON r.run_id = t.run_id "
            "WHERE r.workflow = ? AND r.started_at < ? AND t.name != ? "
            "ORDER BY r.started_at DESC",
            (rows[0]["workflow"], rows[0]["started_at"], name),
        )
        moved = next(
            (str(row["name"]) for row in earlier if task_base_name(str(row["name"])) == base_name),
            None,
        )
        if moved is not None:
            return moved

        previous_run = self._store.query(
            "SELECT run_id FROM runs WHERE workflow = ? AND started_at < ? "
            "ORDER BY started_at DESC LIMIT 1",
            (rows[0]["workflow"], rows[0]["started_at"]),
        )
        if not previous_run:
            return None
        earlier_run_id = str(previous_run[0]["run_id"])
        same_node = self._store.query(
            "SELECT name FROM tasks WHERE run_id = ? AND task_id = ? "
            "AND name NOT IN (SELECT name FROM tasks WHERE run_id = ?)",
            (earlier_run_id, task_id, run_id),
        )
        if not same_node:
            return None
        params = "SELECT param FROM task_inputs WHERE run_id = ? AND task_id = ? ORDER BY param"
        current_params = [row["param"] for row in self._store.query(params, (run_id, task_id))]
        earlier_params = [
            row["param"] for row in self._store.query(params, (earlier_run_id, task_id))
        ]
        return str(same_node[0]["name"]) if current_params == earlier_params else None

    def _task_input_labels(self, *, run_id: str, task_id: str) -> dict[str, str]:
        """Return one task's per-parameter cache-tracking labels.

        Read from ``task_inputs.tracking``, one row per parameter. A run recorded before the column existed reads back
        ``None`` for every row, so the result is empty rather than a dict of
        placeholders — ``ginkgo cache explain`` shows nothing for those,
        never a guess.
        """
        rows = self._store.query(
            "SELECT param, tracking FROM task_inputs WHERE run_id = ? AND task_id = ?",
            (run_id, task_id),
        )
        return {
            str(row["param"]): str(row["tracking"]) for row in rows if row["tracking"] is not None
        }

    # -- raw SQL -------------------------------------------------------------

    def sql(
        self, query: str, params: Sequence[Any] = (), *, limit: int = SQL_ROW_LIMIT
    ) -> SqlResult:
        """Run one read-only statement against the ledger.

        For the question no method here answers. :meth:`schema` names the tables
        and their columns; they are versioned but not stable, so a query written
        against them may need rewriting after an upgrade.

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
            hint = self._schema_hint(statement)
            raise StoreError(f"SQLite rejected the query: {exc}. {hint}") from exc
        return SqlResult(
            columns=columns,
            rows=rows[:limit],
            truncated=len(rows) > limit,
            limit=limit,
        )

    def schema(self) -> dict[str, tuple[str, ...]]:
        """Return the ledger's tables, each with its columns in declared order.

        The answer to "what can I select?", read out of the database in front of
        the caller rather than out of prose that can fall behind it. What
        ``ginkgo query --schema`` prints, and what a rejected statement quotes
        back at whoever wrote it.

        Returns
        -------
        dict[str, tuple[str, ...]]
            Table name to column names. Tables in alphabetical order; SQLite's
            own ``sqlite_*`` tables are left out, since no user query wants them.
        """
        rows = self._store.query(
            "SELECT m.name AS table_name, c.name AS column_name "
            "FROM sqlite_master AS m JOIN pragma_table_info(m.name) AS c "
            "WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
            "ORDER BY m.name, c.cid"
        )
        tables: dict[str, list[str]] = {}
        for row in rows:
            tables.setdefault(row["table_name"], []).append(row["column_name"])
        return {table: tuple(columns) for table, columns in tables.items()}

    def _schema_hint(self, statement: str) -> str:
        """Return the sentence a rejected *statement* is answered with.

        The user is standing at their terminal, not in the documentation, so the
        columns come to them. A statement that names a table ginkgo has gets that
        table's columns; one that names none — a table typo, a syntax error —
        gets the list of tables to choose from.
        """
        schema = self.schema()
        named = [table for table in schema if _mentions(statement, table)]
        if named:
            listed = " ".join(f"{table} has {', '.join(schema[table])}." for table in named)
            return f"{listed} `ginkgo query --schema` lists every table."
        return f"The tables are {', '.join(schema)}. `ginkgo query --schema` lists their columns."

    def why(self, path_or_artifact_id: str) -> Provenance:
        """Return what produced an artifact, named by path or by id.

        A path is resolved through ``materializations`` — the stat log of every
        artifact ginkgo has written to a working directory — to its artifact,
        and then to the task whose output row names that artifact. Artifact ids
        are content digests, so two outputs with identical bytes share one; a
        path narrows the match to the output written there. Among matching
        rows the task that wrote the output beats a dynamic parent whose result
        merely passes it on, an execution beats a cache-hit replay, and a later
        run an earlier one.

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
        artifact_id, path, containing_output = self._resolve_artifact(path_or_artifact_id)
        output_path = containing_output or path
        producers = [
            row
            for row in self._store.query(
                "SELECT o.run_id, o.task_id, o.path, t.cache_key FROM task_outputs o "
                "JOIN tasks t ON t.run_id = o.run_id AND t.task_id = o.task_id "
                "WHERE o.artifact_id = ? ORDER BY EXISTS ("
                "  SELECT 1 FROM edges e WHERE e.run_id = o.run_id AND e.dst_kind = 'task'"
                "  AND e.dst_id = o.task_id AND e.edge = 'dynamic_depends_on'"
                ") ASC, t.cached ASC, o.run_id DESC",
                (artifact_id,),
            )
            if output_path is None or self._workspace_path(row["path"]) == output_path
        ]
        run_id = _first(producers, "run_id")
        task_id = _first(producers, "task_id")
        cache_key = _first(producers, "cache_key")
        if run_id is None:
            cached = self._store.query(
                "SELECT cache_key FROM cache_artifacts WHERE artifact_id = ? LIMIT 1",
                (artifact_id,),
            )
            cache_key = _first(cached, "cache_key")
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
            containing_output=containing_output,
            run_id=run_id,
            task_id=task_id,
            task_name=self._task_name(run_id=run_id, task_id=task_id),
            cache_key=cache_key,
            inputs=self._task_inputs(run_id=run_id, task_id=task_id),
        )

    def _resolve_artifact(self, path_or_artifact_id: str) -> tuple[str, str | None, str | None]:
        """Return the artifact id *path_or_artifact_id* names, the path, and its folder.

        Only an output folder's root is materialized, so an existing path with
        no record of its own resolves to the nearest recorded output folder
        holding it, returned third; otherwise the third item is ``None``.
        """
        resolved = self._workspace_path(path_or_artifact_id)
        rows = self._store.query(
            "SELECT artifact_id FROM materializations WHERE path = ?", (resolved,)
        )
        if rows:
            return str(rows[0]["artifact_id"]), resolved, None
        if Path(resolved).exists():
            for parent in Path(resolved).parents:
                rows = self._store.query(
                    "SELECT m.artifact_id FROM materializations m JOIN task_outputs o "
                    "ON o.artifact_id = m.artifact_id "
                    "WHERE m.path = ? AND o.value_type = 'folder' LIMIT 1",
                    (str(parent),),
                )
                if rows:
                    return str(rows[0]["artifact_id"]), resolved, str(parent)
        known = self._store.query(
            "SELECT artifact_id FROM artifacts WHERE artifact_id = ?", (path_or_artifact_id,)
        )
        if known:
            return path_or_artifact_id, None, None
        # A ledger lost or replaced since the run that wrote the path knows
        # nothing of it, whatever the task's annotations were.
        ledger_gap = runs_without_ledger_warning(layout=self._layout)
        if ledger_gap is not None:
            raise FileNotFoundError(
                f"Nothing in the ledger produced {path_or_artifact_id!r}, and {ledger_gap}. "
                "A path written by one of those runs cannot be traced; run the workflow "
                "again to record it."
            )
        raise FileNotFoundError(
            f"Nothing in the ledger produced {path_or_artifact_id!r}: it is neither an "
            "artifact id nor a path ginkgo has materialized. A path a task returns as a "
            "plain `str` is not tracked; annotate the return `-> file` (or `-> folder`), "
            "or write it through an `Out[file]` parameter, and ginkgo records it."
        )

    def _workspace_path(self, path: str) -> str:
        """Return *path* resolved against the project root, not the caller's cwd.

        Output paths are recorded relative to the project root, the directory
        holding ``.ginkgo/``; a library caller may be standing anywhere.
        """
        return str((self._layout.root.parent / Path(path).expanduser()).resolve())

    def _task_name(self, *, run_id: str | None, task_id: str | None) -> str | None:
        """Return the name one task is shown under: its display label or base name."""
        if run_id is None or task_id is None:
            return None
        rows = self._store.query(
            "SELECT display_label, name FROM tasks WHERE run_id = ? AND task_id = ?",
            (run_id, task_id),
        )
        if not rows:
            return None
        return rows[0]["display_label"] or task_base_name(str(rows[0]["name"]))

    def _task_inputs(
        self, *, run_id: str | None, task_id: str | None
    ) -> tuple[dict[str, Any], ...]:
        """Return the recorded inputs of one task, one mapping per parameter."""
        if run_id is None or task_id is None:
            return ()
        rows = self._store.query(
            "SELECT param, value_type, value_summary, digest, remote_uri, tracking "
            "FROM task_inputs "
            "WHERE run_id = ? AND task_id = ? ORDER BY param",
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
        runs, and that is an answer rather than a failure. So is a ledger a
        first run has created but not yet given a schema. The
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
        return _empty_query(layout)
    try:
        return Query(open_store(path, readonly=readonly), layout=layout)
    except UninitializedStoreError:
        # A first run has created the file but not yet its schema.
        if not missing_ok:
            raise
        return _empty_query(layout)


def _empty_query(layout: WorkspaceLayout) -> Query:
    """A ``Query`` over an empty in-memory ledger, for a workspace with no runs."""
    # Write-mode, because the schema has to exist before anything can be
    # selected from it — then closed to writes, so an empty workspace
    # refuses the same statements a populated one does.
    empty = open_store(MEMORY)
    empty.restrict_to_reads()
    return Query(empty, layout=layout)


def _mentions(statement: str, table: str) -> bool:
    """Return whether *statement* names *table* as a whole identifier.

    A substring test would find ``runs`` inside ``FROM task_runs`` and answer
    about a table the statement never named. Underscores count as word
    characters, so the boundaries hold either way round.
    """
    return re.search(rf"\b{re.escape(table)}\b", statement, re.IGNORECASE) is not None


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


_TASK_COLUMNS = (
    "t.run_id, t.task_id, t.name, t.display_label, t.status, t.cached, "
    "t.cache_key, t.started_at, t.finished_at, t.attempts, "
    "t.peak_rss_bytes, t.cpu_seconds, t.declared_threads, "
    "t.declared_memory_gb, t.effective_memory_gb, "
    "json_extract(t.resource_usage, '$.declared.memory') AS declared_memory"
)
"""The projection behind :class:`TaskRow`, shared by every reader of it."""

_TASK_MATCH = "t.name = ? OR t.display_label = ? OR t.name LIKE ? ESCAPE '\\'"
"""Matches a task by name, by base name, or by one fan-out branch's label."""

_STARTED_ONLY = " AND NOT (t.status IN ('pending', 'skipped') AND t.started_at IS NULL)"
"""Drops tasks a run never started: one skipped when its run stopped, or one
still waiting in a run in progress. A cache hit never starts either, but it
holds a result, so it stays; so does a task waiting to retry."""


def _task_row(row: Any) -> TaskRow:
    """Build a :class:`TaskRow` from one ``_TASK_COLUMNS`` result row."""
    return TaskRow(
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
        peak_rss_bytes=row["peak_rss_bytes"],
        cpu_seconds=row["cpu_seconds"],
        declared_threads=row["declared_threads"],
        declared_memory_gb=_declared_memory_gb(row),
        effective_memory_gb=row["effective_memory_gb"],
    )


def _declared_memory_gb(row: Any) -> float | None:
    """Return the memory a task declared, in GiB as written.

    The ``declared_memory_gb`` column holds the whole GiB the scheduler rounds
    a declaration up to, so ``512Mi`` reads as 1 there. Sizing a declaration
    from a measured peak needs the figure the user wrote, which the recorded
    resource usage keeps.
    """
    written = row["declared_memory"]
    if isinstance(written, str):
        return memory_gib(written)
    return row["declared_memory_gb"]


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
