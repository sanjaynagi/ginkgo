"""The one read model for a run, built from the store's projections.

Every presenter — ``runs show``, ``debug``, the end-of-run console summary,
and plugin presenters such as ``ginkgo report`` and Slack notifications —
formats this and nothing else (issue #79). It is
loaded from the ledger's projection tables, so a run is visible the moment it
has rows, and the run directory is needed only for the bytes it holds: logs and
rendered pages.
"""

from __future__ import annotations

import logging
import re
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ginkgo.formatting import duration_seconds, parse_timestamp
from ginkgo.store.jsonio import loads
from ginkgo.runtime.events import task_id_for_node
from ginkgo.runtime.rundir import combined_log_tail
from ginkgo.store.protocol import ProvenanceStore
from ginkgo.workspace_layout import WorkspaceLayout

logger = logging.getLogger(__name__)

# Statuses that mark a task as finished; shared by every consumer that
# needs to decide terminality (see TaskSummary.is_terminal and the CLI
# renderers).
TERMINAL_STATUSES = frozenset({"cached", "succeeded", "failed", "skipped"})

DONE_STATUSES = frozenset({"cached", "succeeded"})
"""The terminal statuses of a task that produced a result."""

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
"""A terminal colour or cursor code, stripped from log lines meant for a program."""


def _base_name(value: Any) -> str:
    """Return the final dotted segment of a task identifier."""
    if not isinstance(value, str) or not value:
        return "unknown"
    return value.rsplit(".", 1)[-1]


def _path_base_name(value: Any) -> str:
    """Return the basename for a path-like string."""
    if not isinstance(value, str) or not value:
        return "unknown"
    return Path(value).name


@dataclass(frozen=True, kw_only=True)
class TaskSummary:
    """One task of a run, as the projections recorded it."""

    task_key: str
    node_id: int | None
    name: str
    base_name: str
    display_label: str | None
    status: str
    cached: bool
    started_at: datetime | None
    finished_at: datetime | None
    duration_s: float | None
    exit_code: int | None
    error: str | None
    failure: dict[str, Any] | None
    env: str | None
    """The environment the task declared; ``None`` means the local one."""
    kind: str | None
    cache_key: str | None
    attempts: int
    max_attempts: int | None
    stdout_log: str | None
    stderr_log: str | None
    execution_backend: str | None
    remote_job_id: str | None
    sub_run_id: str | None
    rendered_html: str | None
    """A page the task rendered for people to read, such as a notebook's HTML.

    Any task kind may record one; relative to the run directory, or absolute
    when a cache hit points at the page an earlier run rendered."""
    rendered_html_run_id: str | None
    """The run that rendered ``rendered_html``: not this run on a cache hit."""
    dependency_ids: tuple[int, ...]
    dynamic_dependency_ids: tuple[int, ...]
    inputs: dict[str, Any] | None
    input_trackings: dict[str, str] | None
    """Each input parameter's cache-tracking label (``content``, ``version``,
    ``path``, ``value``, ``output``, ``untracked``), when recorded (issue
    #307 phase 1). ``None`` for a run recorded before the label existed."""
    outputs: tuple[dict[str, Any], ...]
    resource_usage: dict[str, Any] | None
    skipped_because: dict[str, Any] | None
    """Why this task was skipped, and the failure the skip is attributed to."""
    ignored: bool
    """Whether the run's failure policy let this task's failure pass."""
    attempt_history: tuple[dict[str, Any], ...] = ()
    """One entry per attempt, oldest first: its ``attempt`` number, ``status``,
    ``started_at``/``finished_at``, ``exit_code`` and ``failure``."""
    timings: dict[str, float] = field(default_factory=dict)

    def is_terminal(self) -> bool:
        """Return True when the task reached a terminal status."""
        return self.status in TERMINAL_STATUSES

    @property
    def failure_kind(self) -> str | None:
        """Return the diagnosed failure category, when the task recorded one."""
        if isinstance(self.failure, dict):
            kind = self.failure.get("kind")
            if isinstance(kind, str):
                return kind
        return None

    @property
    def label(self) -> str:
        """Return the name the task is shown under: its fan-out label or base name."""
        return self.display_label or self.base_name

    @property
    def kind_label(self) -> str:
        """Return a display label for the task kind (``"task"`` fallback)."""
        return self.kind or "task"

    @property
    def started(self) -> bool:
        """Whether the task started an attempt of its own.

        The one question behind both labels below, and the one that separates
        the two kinds of skipped task: one the run never reached, and one that
        ran its body and then waited on an expansion that failed.
        """
        return self.started_at is not None

    @property
    def cache_label(self) -> str:
        """Return ``"hit"``, ``"miss"``, or ``"—"`` for the cache outcome.

        Anything that started missed: a hit completes a task where it stands,
        so a task that went on to run had already missed. A task the run never
        started never asked, and claims neither.
        """
        if self.cached or self.status == "cached":
            return "hit"
        if self.status in {"succeeded", "failed"} or self.started:
            return "miss"
        return "—"

    @property
    def attempts_label(self) -> str:
        """Return ``"N"``, ``"N of M allowed"``, or ``"—"`` when nothing was attempted.

        ``M`` is the attempts the task's retries allow, so a task that
        succeeded first time with a retry to spare reads ``1 of 2 allowed``.
        """
        if self.attempts == 0 or self.status == "cached":
            return "—"
        if self.status == "skipped" and not self.started:
            return "—"
        # ``attempts`` is the ledger's 1-based count of attempts made.
        if self.max_attempts is not None and self.max_attempts > 1:
            return f"{self.attempts} of {self.max_attempts} allowed"
        return f"{self.attempts}"

    def skip_cause(self, *, labels: Mapping[str, str]) -> str | None:
        """Say why a skipped task never produced a result; ``None`` when it was not skipped.

        Parameters
        ----------
        labels : Mapping[str, str]
            The label to name each task of the run by, keyed by ``task_key``,
            so the blocking task is named the way the skipped one is.
        """
        if self.status != "skipped":
            return None
        blocker = self.skipped_because or {}
        blocker_name = blocker.get("task_name")
        blocker_label = labels.get(str(blocker.get("task_id"))) or (
            _base_name(blocker_name)
            if isinstance(blocker_name, str) and blocker_name
            else "a failed task"
        )
        reason = blocker.get("reason")
        if reason == "interrupted":
            return "not started: the run was interrupted"
        if reason == "run_stopped":
            if blocker.get("task_id"):
                return f"not started: the run stopped when {blocker_label} failed"
            return "not started: the run stopped"
        return f"blocked by {blocker_label}"

    def rendered_html_absolute(self, *, run_dir: Path) -> Path | None:
        """Resolve ``rendered_html`` against the run directory.

        Cache hits replay an absolute path; a freshly rendered page is stored
        relative to ``run_dir``. ``Path /`` handles both.
        """
        if self.rendered_html is None:
            return None
        return (run_dir / self.rendered_html).resolve()


@dataclass(frozen=True, kw_only=True)
class RunSummary:
    """Aggregate view of one run.

    Parameters
    ----------
    run_id : str
        Identifier for the run.
    workflow : str | None
        Workflow path string as recorded when the run started.
    workflow_label : str
        Display name for the workflow (its basename).
    status : str
        Run status: ``"succeeded"``, ``"failed"``, ``"cancelled"``,
        ``"running"``, or ``"unknown"``. ``"cancelled"`` is a run an
        interrupt ended.
    started_at : datetime | None
        Run start timestamp.
    finished_at : datetime | None
        Run end timestamp.
    duration_s : float | None
        Wall-clock duration in seconds when both timestamps exist.
    run_dir : Path
        The run's directory, which holds its logs, rendered pages and snapshot.
    error : str | None
        The failure that ended the run, when one did.
    jobs, cores, memory : int | None
        The budgets the run was given.
    ginkgo_version : str | None
        The ginkgo that ran it.
    parent_run_id, parent_task_id : str | None
        The run and task that called this one, when it is a sub-workflow.
    resources : dict[str, Any]
        Resource summary sampled during the run.
    params : dict[str, Any]
        Workflow parameters the run resolved.
    param_sources : dict[str, str]
        Where each parameter's value came from: cli, config, or default.
    timings : dict[str, float]
        Run-level phase timings, in seconds.
    tasks : tuple[TaskSummary, ...]
        Tasks ordered by ``node_id`` ascending.
    notices : tuple[str, ...]
        Run-level problems recorded as ``run_notice`` events, in the order
        they happened, such as a notification that was not delivered.
    """

    run_id: str
    workflow: str | None
    workflow_label: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    duration_s: float | None
    run_dir: Path
    error: str | None
    jobs: int | None
    cores: int | None
    memory: int | None
    ginkgo_version: str | None
    parent_run_id: str | None
    parent_task_id: str | None
    resources: dict[str, Any]
    params: dict[str, Any]
    param_sources: dict[str, str]
    timings: dict[str, float]
    tasks: tuple[TaskSummary, ...]
    notices: tuple[str, ...] = ()

    @classmethod
    def load(
        cls,
        store: ProvenanceStore,
        run_id: str,
        *,
        runs_root: Path | None = None,
    ) -> RunSummary:
        """Load one run from the store.

        Parameters
        ----------
        store : ProvenanceStore
            An open store; a read-only one is enough.
        run_id : str
            The run to load.
        runs_root : Path | None, optional
            Where run directories live. Defaults to the current workspace's.

        Returns
        -------
        RunSummary

        Raises
        ------
        KeyError
            If the store has no such run.
        """
        rows = store.query("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        if not rows:
            raise KeyError(run_id)
        run = dict(rows[0])
        root = runs_root if runs_root is not None else WorkspaceLayout.relative().runs

        tasks = _load_tasks(store=store, run_id=run_id)
        started_at = parse_timestamp(run.get("started_at"))
        finished_at = parse_timestamp(run.get("finished_at"))
        workflow = run.get("workflow") if isinstance(run.get("workflow"), str) else None

        return cls(
            run_id=run_id,
            workflow=workflow,
            workflow_label=_path_base_name(workflow),
            status=str(run.get("status") or "unknown"),
            started_at=started_at,
            finished_at=finished_at,
            duration_s=duration_seconds(started_at, finished_at),
            run_dir=root / run_id,
            error=_text(run.get("error")),
            jobs=run.get("jobs"),
            cores=run.get("cores"),
            memory=run.get("memory"),
            ginkgo_version=_text(run.get("ginkgo_version")),
            parent_run_id=_text(run.get("parent_run_id")),
            parent_task_id=_text(run.get("parent_task_id")),
            resources=_mapping(run.get("resources")),
            params=_mapping(run.get("params")),
            param_sources=_mapping(run.get("param_sources")),
            timings=_mapping(run.get("timings")),
            tasks=tasks,
            notices=_load_notices(store=store, run_id=run_id),
        )

    def to_payload(self) -> dict[str, Any]:
        """Return the run as a JSON-serialisable mapping.

        The one home for the run's serialised form: ``ginkgo runs show --json``
        prints this as JSON and the run directory keeps it as YAML, so the file
        and the command cannot disagree about what a run was.

        A task's ``status`` carries ``"skipped"`` for one left resultless
        without failing itself, and such a task also carries
        ``skipped_because``: the skip's ``reason`` (``upstream_failed``,
        ``run_stopped`` or ``interrupted``) and the ``task_id``/``task_name``
        of the failure it is attributed to, null when no task caused it. A
        failure the run's policy let pass carries ``ignored: true``. A task
        that made more than one attempt carries ``attempt_history``, one
        entry per attempt with its status and failure. These keys are
        present only where they mean something.

        Returns
        -------
        dict[str, Any]
        """
        tasks: list[dict[str, Any]] = []
        expansions: list[dict[str, Any]] = []
        for task in self.tasks:
            dynamic = [task_id_for_node(node) for node in task.dynamic_dependency_ids]
            if dynamic:
                expansions.append(
                    {"parent_task_id": task.task_key, "dynamic_dependency_ids": dynamic}
                )
            row: dict[str, Any] = {
                "task_id": task.task_key,
                "task_name": task.base_name,
                "display_label": task.display_label,
                "status": task.status,
                "attempts": task.attempts,
                "max_attempts": task.max_attempts,
                "cache_key": task.cache_key,
                "cached": task.cached,
                "exit_code": task.exit_code,
                "env": task.env,
                "kind": task.kind,
                "dependency_ids": [task_id_for_node(node) for node in task.dependency_ids],
                "dynamic_dependency_ids": dynamic,
                "inputs": task.inputs,
                "failure": task.failure,
                "outputs": list(task.outputs),
                "stdout_log": task.stdout_log,
                "stderr_log": task.stderr_log,
                "started_at": _iso(task.started_at),
                "finished_at": _iso(task.finished_at),
                "timings": task.timings,
            }
            # Present only where they mean something, so a local run's record
            # is not padded with nulls about remote execution it never did.
            for key, value in (
                ("remote_job_id", task.remote_job_id),
                ("execution_backend", task.execution_backend),
                ("resource_usage", task.resource_usage),
                ("sub_run_id", task.sub_run_id),
                ("rendered_html", task.rendered_html),
                ("rendered_html_run_id", task.rendered_html_run_id),
                ("skipped_because", task.skipped_because),
                ("ignored", True if task.ignored else None),
                (
                    "attempt_history",
                    list(task.attempt_history) if len(task.attempt_history) > 1 else None,
                ),
            ):
                if value is not None:
                    row[key] = value
            tasks.append(row)

        return {
            "run_id": self.run_id,
            "workflow": self.workflow,
            "status": self.status,
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "error": self.error,
            "ginkgo_version": self.ginkgo_version,
            "jobs": self.jobs,
            "cores": self.cores,
            "memory": self.memory,
            "parent_run_id": self.parent_run_id,
            "parent_task_id": self.parent_task_id,
            "params": self.params,
            "param_sources": self.param_sources,
            "resources": self.resources,
            "timings": self.timings,
            "tasks": tasks,
            "dynamic_expansions": expansions,
            "notices": list(self.notices),
        }

    def outcome_payload(self) -> dict[str, Any]:
        """Return what an agent needs from a run: its status, counts, failures and outputs.

        The compact sibling of :meth:`to_payload`, and the body of both the
        ``run_summary`` event ``ginkgo run --agent-output`` ends with and
        ``ginkgo runs status``/``wait``. Read from the ledger, so it is just as
        valid for a run still going: its status is then ``running``.
        """
        labels = {task.task_key: task.label for task in self.tasks}
        counts = self.task_counts()
        return {
            "run_id": self.run_id,
            "status": self.status,
            "run_dir": str(self.run_dir),
            "counts": {
                "total": self.task_count,
                "done": sum(1 for task in self.tasks if task.is_terminal()),
                "executed": counts["succeeded"] + counts["failed"],
                "cached": self.cached_count,
                "failed": self.failed_count,
                "ignored": self.ignored_failure_count,
                "skipped": self.skipped_count,
            },
            "failures": [
                {
                    "task": task.label,
                    "category": task.failure_kind,
                    "exit_code": task.exit_code,
                    "ignored": task.ignored,
                    "log_tail": [
                        _ANSI_ESCAPE.sub("", line)
                        for line in combined_log_tail(
                            run_dir=self.run_dir,
                            stdout_log=task.stdout_log,
                            stderr_log=task.stderr_log,
                            lines=10,
                        )
                    ],
                    "log": str(self.run_dir / task.stderr_log) if task.stderr_log else None,
                }
                for task in self.failed_tasks
            ],
            "skipped": [
                {"task": task.label, "cause": cause}
                for task in self.tasks
                if (cause := task.skip_cause(labels=labels)) is not None
            ],
            "outputs": [
                {"task": task.label, "path": output["path"]}
                for task in self.tasks
                for output in task.outputs
                if "path" in output
            ],
        }

    # ----- Aggregations -----------------------------------------------------

    def task_counts(self) -> Counter[str]:
        """Return a counter of task statuses across the run."""
        return Counter(task.status for task in self.tasks)

    @property
    def task_count(self) -> int:
        """Return the total number of tasks in the run."""
        return len(self.tasks)

    @property
    def succeeded_count(self) -> int:
        """Return the number of tasks that completed successfully."""
        return sum(1 for task in self.tasks if task.status == "succeeded")

    @property
    def failed_count(self) -> int:
        """Return the number of tasks that failed."""
        return sum(1 for task in self.tasks if task.status == "failed")

    @property
    def cached_count(self) -> int:
        """Return the number of tasks served from cache."""
        return sum(1 for task in self.tasks if task.cached or task.status == "cached")

    @property
    def skipped_count(self) -> int:
        """Return the number of tasks left resultless by a failure."""
        return sum(1 for task in self.tasks if task.status == "skipped")

    @property
    def ignored_failure_count(self) -> int:
        """Return how many of the failed tasks the failure policy let pass."""
        return sum(1 for task in self.tasks if task.status == "failed" and task.ignored)

    @property
    def failed_tasks(self) -> tuple[TaskSummary, ...]:
        """Return failed tasks ordered by node id, ignored ones included.

        A failure the policy let pass is still a failure; ``TaskSummary.ignored``
        is what tells the two apart.
        """
        return tuple(task for task in self.tasks if task.status == "failed")

    @property
    def succeeded(self) -> bool:
        """Return whether the run finished successfully."""
        return self.status == "succeeded"


def _load_notices(*, store: ProvenanceStore, run_id: str) -> tuple[str, ...]:
    """Return the run's ``run_notice`` messages in ledger order."""
    rows = store.query(
        "SELECT payload FROM events WHERE run_id = ? AND type = 'run_notice' ORDER BY seq",
        (run_id,),
    )
    return tuple(str(loads(row["payload"]).get("message") or "") for row in rows)


def _load_tasks(*, store: ProvenanceStore, run_id: str) -> tuple[TaskSummary, ...]:
    """Build the ordered task list, with its inputs, outputs and edges."""
    inputs: dict[str, dict[str, Any]] = {}
    trackings: dict[str, dict[str, str]] = {}
    for row in store.query(
        "SELECT task_id, param, value_summary, tracking FROM task_inputs WHERE run_id = ? "
        "ORDER BY task_id, param",
        (run_id,),
    ):
        inputs.setdefault(row["task_id"], {})[row["param"]] = loads(row["value_summary"])
        if row["tracking"] is not None:
            trackings.setdefault(row["task_id"], {})[row["param"]] = str(row["tracking"])

    attempts: dict[str, list[dict[str, Any]]] = {}
    for row in store.query(
        "SELECT task_id, attempt, status, started_at, finished_at, exit_code, failure "
        "FROM attempts WHERE run_id = ? ORDER BY task_id, attempt",
        (run_id,),
    ):
        attempts.setdefault(row["task_id"], []).append(
            {
                "attempt": row["attempt"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "exit_code": row["exit_code"],
                "failure": loads(row["failure"]),
            }
        )

    tasks = store.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY node_id", (run_id,))
    node_ids = {row["task_id"]: row["node_id"] for row in tasks}

    dependencies: dict[tuple[str, str], list[int]] = {}
    for row in store.query(
        "SELECT dst_id, src_id, edge FROM edges "
        "WHERE run_id = ? AND dst_kind = 'task' AND src_kind = 'task' "
        "AND edge IN ('depends_on', 'dynamic_depends_on')",
        (run_id,),
    ):
        # An edge naming a task the run never registered is dropped rather
        # than collapsed onto a placeholder node: two unresolved sources
        # collapsed to one id become each other's dependency, and the report's
        # layering would follow that cycle forever. A dependency ginkgo cannot
        # resolve is a projector bug, and a missing edge is where it shows.
        source = node_ids.get(row["src_id"])
        if source is None:
            logger.warning(
                "Run %s: task %s depends on %s, which the run has no task row for",
                run_id,
                row["dst_id"],
                row["src_id"],
            )
            continue
        dependencies.setdefault((row["dst_id"], row["edge"]), []).append(source)

    return tuple(
        _build_task_summary(
            row=dict(row),
            inputs=inputs.get(row["task_id"]),
            input_trackings=trackings.get(row["task_id"]),
            attempt_history=attempts.get(row["task_id"], []),
            dependency_ids=dependencies.get((row["task_id"], "depends_on"), []),
            dynamic_dependency_ids=dependencies.get(
                (row["task_id"], "dynamic_depends_on"),
                [],
            ),
        )
        for row in tasks
    )


def _build_task_summary(
    *,
    row: dict[str, Any],
    inputs: dict[str, Any] | None,
    input_trackings: dict[str, str] | None,
    attempt_history: list[dict[str, Any]],
    dependency_ids: list[int],
    dynamic_dependency_ids: list[int],
) -> TaskSummary:
    """Build one ``TaskSummary`` from a ``tasks`` row and its related rows."""
    started = parse_timestamp(row.get("started_at"))
    finished = parse_timestamp(row.get("finished_at"))
    extra = _mapping(row.get("extra"))
    failure = _mapping(row.get("failure")) or None
    outputs = _sequence(row.get("output_summary"))
    name = str(row.get("name") or "unknown")

    return TaskSummary(
        task_key=str(row.get("task_id") or ""),
        node_id=row.get("node_id") if isinstance(row.get("node_id"), int) else None,
        name=name,
        base_name=_base_name(name),
        display_label=_text(row.get("display_label")),
        status=str(row.get("status") or "unknown"),
        cached=bool(row.get("cached")),
        started_at=started,
        finished_at=finished,
        duration_s=duration_seconds(started, finished),
        exit_code=row.get("exit_code") if isinstance(row.get("exit_code"), int) else None,
        error=_text(failure.get("message")) if failure else None,
        failure=failure,
        env=_text(row.get("env")),
        kind=_text(row.get("kind")),
        cache_key=_text(row.get("cache_key")),
        attempts=int(row.get("attempts") or 0),
        max_attempts=row.get("max_attempts") if isinstance(row.get("max_attempts"), int) else None,
        stdout_log=_text(row.get("stdout_log")),
        stderr_log=_text(row.get("stderr_log")),
        execution_backend=_text(row.get("execution_backend")),
        remote_job_id=_text(row.get("remote_job_id")),
        sub_run_id=_text(extra.get("sub_run_id")),
        rendered_html=_text(extra.get("rendered_html")),
        rendered_html_run_id=_text(extra.get("rendered_html_run_id")),
        dependency_ids=tuple(sorted(dependency_ids)),
        dynamic_dependency_ids=tuple(sorted(dynamic_dependency_ids)),
        inputs=inputs,
        input_trackings=input_trackings,
        outputs=tuple(item for item in outputs if isinstance(item, dict)),
        resource_usage=_mapping(row.get("resource_usage")) or None,
        skipped_because=_mapping(extra.get("skipped_because")) or None,
        ignored=bool(extra.get("ignored")),
        attempt_history=tuple(attempt_history),
        timings=_mapping(row.get("timings")),
    )


def _iso(value: datetime | None) -> str | None:
    """Return an ISO timestamp string, or ``None``."""
    return value.isoformat() if value is not None else None


def _text(value: Any) -> str | None:
    """Return *value* when it is a non-empty string."""
    return value if isinstance(value, str) and value else None


def _mapping(value: Any) -> dict[str, Any]:
    """Return a stored JSON column as a mapping, or an empty one."""
    parsed = loads(value)
    return parsed if isinstance(parsed, dict) else {}


def _sequence(value: Any) -> list[Any]:
    """Return a stored JSON column as a list, or an empty one."""
    parsed = loads(value)
    return parsed if isinstance(parsed, list) else []
