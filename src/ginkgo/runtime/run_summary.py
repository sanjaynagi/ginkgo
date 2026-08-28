"""The one read model for a run, built from the store's projections.

Every presenter — ``inspect run``, ``debug``, ``report``, notifications, the
end-of-run console summary — formats this and nothing else (issue #79). It is
loaded from the ledger's projection tables, so a run is visible the moment it
has rows, and the run directory is needed only for the bytes it holds: logs and
notebook artifacts.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ginkgo.formatting import parse_timestamp
from ginkgo.store.jsonio import loads
from ginkgo.runtime.events import task_id_for_node
from ginkgo.store.protocol import ProvenanceStore
from ginkgo.workspace_layout import WorkspaceLayout

# Statuses that mark a task as finished; shared by every consumer that
# needs to decide terminality (see TaskSummary.is_terminal and the CLI
# renderers).
TERMINAL_STATUSES = frozenset({"cached", "succeeded", "failed"})


def _duration_seconds(
    started_at: datetime | None,
    finished_at: datetime | None,
) -> float | None:
    """Return wall-clock seconds between two timestamps when both are valid."""
    if started_at is None or finished_at is None:
        return None
    return max(0.0, (finished_at - started_at).total_seconds())


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
    executed_notebook: str | None
    notebook_kind: str | None
    notebook_description: str | None
    notebook_path: str | None
    render_status: str | None
    render_error: str | None
    notebook_artifact_run_id: str | None
    task_type: str
    dependency_ids: tuple[int, ...]
    dynamic_dependency_ids: tuple[int, ...]
    inputs: dict[str, Any] | None
    outputs: tuple[dict[str, Any], ...]
    assets: tuple[dict[str, Any], ...]
    resource_usage: dict[str, Any] | None
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
    def kind_label(self) -> str:
        """Return a display label for the task kind (``"task"`` fallback)."""
        if self.kind:
            return self.kind
        if self.task_type == "notebook":
            return "notebook"
        return "task"

    @property
    def cache_label(self) -> str:
        """Return ``"hit"``, ``"miss"``, or ``"—"`` for the cache outcome."""
        if self.cached or self.status == "cached":
            return "hit"
        if self.status in {"succeeded", "failed"}:
            return "miss"
        return "—"

    @property
    def attempts_label(self) -> str:
        """Return ``"N"`` or ``"N / M"`` for attempts / max_attempts."""
        if self.status == "cached":
            return "—"
        if self.max_attempts is not None and self.max_attempts > 1:
            return f"{self.attempts + 1} / {self.max_attempts}"
        return f"{self.attempts + 1}"

    def rendered_html_absolute(self, *, run_dir: Path) -> Path | None:
        """Resolve ``rendered_html`` against the run directory.

        Cache hits replay an absolute path; freshly rendered notebooks store
        a path relative to ``run_dir``. ``Path /`` handles both.
        """
        if self.rendered_html is None:
            return None
        return (run_dir / self.rendered_html).resolve()


@dataclass(frozen=True, kw_only=True)
class NotebookSummary:
    """One materialised notebook from a finished run."""

    task_key: str
    task_name: str
    base_name: str
    description: str | None
    status: str
    render_status: str | None
    render_error: str | None
    notebook_kind: str | None
    notebook_path: str | None
    rendered_html: str | None
    rendered_html_path: Path | None
    notebook_artifact_run_id: str | None = None
    """Run that produced the artifacts, which is not this run on a cache hit."""


@dataclass(frozen=True, kw_only=True)
class AssetSummary:
    """One asset materialised in a finished run."""

    asset_key: str
    name: str


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
        Run status: ``"succeeded"``, ``"failed"``, ``"running"``, or ``"unknown"``.
    started_at : datetime | None
        Run start timestamp.
    finished_at : datetime | None
        Run end timestamp.
    duration_s : float | None
        Wall-clock duration in seconds when both timestamps exist.
    run_dir : Path
        The run's directory, which holds its logs, notebooks and snapshot.
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
    notebooks : tuple[NotebookSummary, ...]
        Notebook tasks materialised in this run.
    assets : tuple[AssetSummary, ...]
        Unique assets materialised in this run.
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
    notebooks: tuple[NotebookSummary, ...]
    assets: tuple[AssetSummary, ...]

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
            duration_s=_duration_seconds(started_at, finished_at),
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
            notebooks=_load_notebooks(tasks=tasks),
            assets=_load_assets(tasks=tasks),
        )

    def to_payload(self) -> dict[str, Any]:
        """Return the run as a JSON-serialisable mapping.

        The one home for the run's serialised form: ``ginkgo inspect run``
        prints this as JSON and the run directory keeps it as YAML, so the file
        and the command cannot disagree about what a run was.

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
    def failed_tasks(self) -> tuple[TaskSummary, ...]:
        """Return failed tasks ordered by node id."""
        return tuple(task for task in self.tasks if task.status == "failed")

    @property
    def succeeded(self) -> bool:
        """Return whether the run finished successfully."""
        return self.status == "succeeded"


def _load_tasks(*, store: ProvenanceStore, run_id: str) -> tuple[TaskSummary, ...]:
    """Build the ordered task list, with its inputs, outputs and edges."""
    inputs: dict[str, dict[str, Any]] = {}
    for row in store.query(
        "SELECT task_id, param, value_summary FROM task_inputs WHERE run_id = ? "
        "ORDER BY task_id, param, position",
        (run_id,),
    ):
        inputs.setdefault(row["task_id"], {})[row["param"]] = loads(row["value_summary"])

    tasks = store.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY node_id", (run_id,))
    node_ids = {row["task_id"]: row["node_id"] for row in tasks}

    dependencies: dict[tuple[str, str], list[int]] = {}
    for row in store.query(
        "SELECT dst_id, src_id, edge FROM edges "
        "WHERE run_id = ? AND dst_kind = 'task' AND src_kind = 'task' "
        "AND edge IN ('depends_on', 'dynamic_depends_on')",
        (run_id,),
    ):
        # A task is not its own dependency. Saying so here keeps a run whose
        # node ids did not resolve down to a graph with no edges, rather than a
        # self-loop that the report's layering would follow forever.
        if row["src_id"] == row["dst_id"]:
            continue
        dependencies.setdefault((row["dst_id"], row["edge"]), []).append(
            node_ids.get(row["src_id"], -1)
        )

    return tuple(
        _build_task_summary(
            row=dict(row),
            inputs=inputs.get(row["task_id"]),
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
    dependency_ids: list[int],
    dynamic_dependency_ids: list[int],
) -> TaskSummary:
    """Build one ``TaskSummary`` from a ``tasks`` row and its related rows."""
    started = parse_timestamp(row.get("started_at"))
    finished = parse_timestamp(row.get("finished_at"))
    extra = _mapping(row.get("extra"))
    failure = _mapping(row.get("failure")) or None
    outputs = _sequence(row.get("output_summary"))
    assets = tuple(item for item in extra.get("assets", []) if isinstance(item, dict))
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
        duration_s=_duration_seconds(started, finished),
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
        executed_notebook=_text(extra.get("executed_notebook")),
        notebook_kind=_text(extra.get("notebook_kind")),
        notebook_description=_text(extra.get("notebook_description")),
        notebook_path=_text(extra.get("notebook_path")),
        render_status=_text(extra.get("render_status")),
        render_error=_text(extra.get("render_error")),
        notebook_artifact_run_id=_text(extra.get("notebook_artifact_run_id")),
        task_type=str(extra.get("task_type") or "task"),
        dependency_ids=tuple(sorted(dependency_ids)),
        dynamic_dependency_ids=tuple(sorted(dynamic_dependency_ids)),
        inputs=inputs,
        outputs=tuple(item for item in outputs if isinstance(item, dict)),
        assets=assets,
        resource_usage=_mapping(row.get("resource_usage")) or None,
        timings=_mapping(row.get("timings")),
    )


def _load_notebooks(*, tasks: tuple[TaskSummary, ...]) -> tuple[NotebookSummary, ...]:
    """Extract notebook artifacts from the per-task summaries."""
    notebooks: list[NotebookSummary] = []
    for task in tasks:
        if task.task_type != "notebook" and task.rendered_html is None:
            continue
        notebooks.append(
            NotebookSummary(
                task_key=task.task_key,
                task_name=task.name,
                base_name=task.base_name,
                description=task.notebook_description,
                status=task.status,
                render_status=task.render_status,
                render_error=task.render_error,
                notebook_kind=task.notebook_kind,
                notebook_path=task.notebook_path,
                rendered_html=task.rendered_html,
                rendered_html_path=None,
                notebook_artifact_run_id=task.notebook_artifact_run_id,
            )
        )
    return tuple(notebooks)


def _load_assets(*, tasks: tuple[TaskSummary, ...]) -> tuple[AssetSummary, ...]:
    """Extract unique materialised asset references from tasks."""
    seen: set[str] = set()
    out: list[AssetSummary] = []
    for task in tasks:
        for asset in task.assets:
            key = asset.get("asset_key")
            if not isinstance(key, str) or key in seen:
                continue
            seen.add(key)
            name = asset.get("name") or key
            out.append(AssetSummary(asset_key=key, name=str(name)))
    return tuple(out)


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
