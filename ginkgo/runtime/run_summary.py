"""Shared run-summary model loaded from a completed run directory.

Both the CLI renderer and the UI server need to read a finished run's
manifest, count tasks by status, compute durations, and surface notebook
and asset listings. Before this module they each implemented those
calculations independently and had to be updated in lock-step. ``RunSummary``
centralises the parsing so each consumer becomes a pure formatter over a
single canonical model.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ginkgo.runtime.caching.provenance import load_manifest

_TERMINAL_STATUSES = frozenset({"cached", "succeeded", "failed"})


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO timestamp from a manifest, returning UTC-aware datetimes."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


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
    """One task entry from a finished run manifest."""

    task_key: str
    node_id: int | None
    name: str
    base_name: str
    status: str
    cached: bool
    started_at: datetime | None
    finished_at: datetime | None
    duration_s: float | None
    exit_code: int | None
    error: str | None
    env: str
    cache_key: str | None
    stdout_log: str | None
    stderr_log: str | None
    log_path: str | None
    rendered_html: str | None
    executed_notebook: str | None
    notebook_kind: str | None
    notebook_description: str | None
    notebook_path: str | None
    render_status: str | None
    task_type: str
    dependency_ids: tuple[int, ...]
    dynamic_dependency_ids: tuple[int, ...]
    inputs: dict[str, Any] | None
    assets: tuple[dict[str, Any], ...]
    raw: dict[str, Any] = field(repr=False)

    def is_terminal(self) -> bool:
        """Return True when the task reached a terminal status."""
        return self.status in _TERMINAL_STATUSES

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
    notebook_kind: str | None
    notebook_path: str | None
    rendered_html: str | None
    rendered_html_path: Path | None


@dataclass(frozen=True, kw_only=True)
class AssetSummary:
    """One asset materialised in a finished run."""

    asset_key: str
    name: str


@dataclass(frozen=True, kw_only=True)
class RunSummary:
    """Aggregate view of a finished run on disk.

    Parameters
    ----------
    run_id : str
        Identifier for the run, taken from the run directory name.
    workflow : str | None
        Workflow path string as recorded in the manifest.
    workflow_label : str
        Display name for the workflow (its basename).
    status : str
        Run status: ``"succeeded"``, ``"failed"``, ``"running"``, or ``"unknown"``.
    started_at : datetime | None
        Run start timestamp parsed from the manifest.
    finished_at : datetime | None
        Run end timestamp parsed from the manifest.
    duration_s : float | None
        Wall-clock duration in seconds when both timestamps exist.
    run_dir : Path
        Absolute path to the run directory on disk.
    resources : dict[str, Any]
        Resource summary recorded by the run.
    params : dict[str, Any]
        Workflow parameters, loaded from ``params.yaml`` if present.
    tasks : tuple[TaskSummary, ...]
        Tasks ordered by ``node_id`` ascending.
    notebooks : tuple[NotebookSummary, ...]
        Notebook tasks materialised in this run.
    assets : tuple[AssetSummary, ...]
        Unique assets materialised in this run.
    raw_manifest : dict[str, Any]
        Raw manifest dict for callers that need fields not yet promoted to
        the model.
    """

    run_id: str
    workflow: str | None
    workflow_label: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    duration_s: float | None
    run_dir: Path
    resources: dict[str, Any]
    params: dict[str, Any]
    tasks: tuple[TaskSummary, ...]
    notebooks: tuple[NotebookSummary, ...]
    assets: tuple[AssetSummary, ...]
    raw_manifest: dict[str, Any] = field(repr=False)

    @classmethod
    def load(cls, run_dir: Path) -> RunSummary:
        """Load a run summary from a finished run directory."""
        run_dir = Path(run_dir)
        manifest = load_manifest(run_dir)
        params_path = run_dir / "params.yaml"
        params = (
            yaml.safe_load(params_path.read_text(encoding="utf-8"))
            if params_path.is_file()
            else {}
        ) or {}

        tasks = _load_tasks(manifest=manifest, run_dir=run_dir)
        notebooks = _load_notebooks(tasks=tasks)
        assets = _load_assets(tasks=tasks)
        started_at = _parse_timestamp(manifest.get("started_at"))
        finished_at = _parse_timestamp(manifest.get("finished_at"))

        return cls(
            run_id=run_dir.name,
            workflow=manifest.get("workflow")
            if isinstance(manifest.get("workflow"), str)
            else None,
            workflow_label=_path_base_name(manifest.get("workflow")),
            status=str(manifest.get("status", "unknown")),
            started_at=started_at,
            finished_at=finished_at,
            duration_s=_duration_seconds(started_at, finished_at),
            run_dir=run_dir,
            resources=manifest.get("resources", {}) or {},
            params=params,
            tasks=tasks,
            notebooks=notebooks,
            assets=assets,
            raw_manifest=manifest,
        )

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


def _load_tasks(*, manifest: dict[str, Any], run_dir: Path) -> tuple[TaskSummary, ...]:
    """Build the ordered task list from a manifest."""
    tasks_raw = manifest.get("tasks", {})
    if not isinstance(tasks_raw, dict):
        return ()

    items: list[tuple[str, dict[str, Any]]] = []
    for task_key, task in tasks_raw.items():
        if isinstance(task_key, str) and isinstance(task, dict):
            items.append((task_key, task))

    items.sort(key=lambda item: int(item[1].get("node_id", -1)))
    return tuple(_build_task_summary(task_key=key, task=task) for key, task in items)


def _build_task_summary(*, task_key: str, task: dict[str, Any]) -> TaskSummary:
    """Build one ``TaskSummary`` from a manifest task entry."""
    started = _parse_timestamp(task.get("started_at"))
    finished = _parse_timestamp(task.get("finished_at"))

    name = task.get("task")
    name_str = name if isinstance(name, str) else "unknown"

    raw_assets = task.get("assets")
    assets: tuple[dict[str, Any], ...] = (
        tuple(item for item in raw_assets if isinstance(item, dict))
        if isinstance(raw_assets, list)
        else ()
    )

    dependency_ids = _coerce_int_tuple(task.get("dependency_ids"))
    dynamic_dependency_ids = _coerce_int_tuple(task.get("dynamic_dependency_ids"))

    stdout_log = task.get("stdout_log") if isinstance(task.get("stdout_log"), str) else None
    stderr_log = task.get("stderr_log") if isinstance(task.get("stderr_log"), str) else None
    legacy_log = task.get("log") if isinstance(task.get("log"), str) else None

    return TaskSummary(
        task_key=task_key,
        node_id=task.get("node_id") if isinstance(task.get("node_id"), int) else None,
        name=name_str,
        base_name=_base_name(name_str),
        status=str(task.get("status", "unknown")),
        cached=bool(task.get("cached", False)),
        started_at=started,
        finished_at=finished,
        duration_s=_duration_seconds(started, finished),
        exit_code=task.get("exit_code") if isinstance(task.get("exit_code"), int) else None,
        error=task.get("error") if isinstance(task.get("error"), str) else None,
        env=str(task.get("env") or "local"),
        cache_key=task.get("cache_key") if isinstance(task.get("cache_key"), str) else None,
        stdout_log=stdout_log,
        stderr_log=stderr_log,
        log_path=legacy_log,
        rendered_html=task.get("rendered_html")
        if isinstance(task.get("rendered_html"), str)
        else None,
        executed_notebook=task.get("executed_notebook")
        if isinstance(task.get("executed_notebook"), str)
        else None,
        notebook_kind=task.get("notebook_kind")
        if isinstance(task.get("notebook_kind"), str)
        else None,
        notebook_description=task.get("notebook_description")
        if isinstance(task.get("notebook_description"), str)
        else None,
        notebook_path=task.get("notebook_path")
        if isinstance(task.get("notebook_path"), str)
        else None,
        render_status=task.get("render_status")
        if isinstance(task.get("render_status"), str)
        else None,
        task_type=str(task.get("task_type", "task")),
        dependency_ids=dependency_ids,
        dynamic_dependency_ids=dynamic_dependency_ids,
        inputs=task.get("inputs") if isinstance(task.get("inputs"), dict) else None,
        assets=assets,
        raw=task,
    )


def _coerce_int_tuple(value: Any) -> tuple[int, ...]:
    """Coerce a manifest list to a tuple of ints, ignoring bad entries."""
    if not isinstance(value, list):
        return ()
    out: list[int] = []
    for item in value:
        try:
            out.append(int(item))
        except (TypeError, ValueError):
            continue
    return tuple(out)


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
                notebook_kind=task.notebook_kind,
                notebook_path=task.notebook_path,
                rendered_html=task.rendered_html,
                rendered_html_path=None,
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
