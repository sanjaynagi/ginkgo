"""Static execution-plan preview for ``ginkgo run --dry-run``.

Turns the validated task graph held by a :class:`ConcurrentEvaluator` into a
structured, render-ready plan: concurrency waves, per-task cache status, and a
resource summary. The builder is pure and read-only — no task is executed, no
environment prepared, and no cached output materialised into the workspace.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from ginkgo.runtime.caching.cache import MISSING

if TYPE_CHECKING:
    from ginkgo.runtime.evaluator import ConcurrentEvaluator, TaskNode

CacheStatus = Literal["cached", "will_run", "unknown"]

_DRIVER_KINDS = {"notebook", "script"}


@dataclass(kw_only=True)
class PlannedTask:
    """One task in the dry-run plan.

    Parameters
    ----------
    node_id : int
        Stable scheduler node id.
    base_name : str
        Task name without its module prefix.
    label : str
        Display label; includes resolved fan-out parameters for mapped tasks.
    kind : str
        Task kind (``python``, ``shell``, ``notebook``, ``script``).
    env : str | None
        Declared environment, or ``None`` for the local environment.
    mapped : bool
        Whether the task is a fan-out branch from ``.map()`` / ``.product_map()``.
    threads : int
        Declared core budget.
    memory_gb : int
        Declared memory budget in GiB (``0`` when unset).
    gpu : int
        Declared GPU count.
    cache_status : CacheStatus
        ``cached``, ``will_run``, or ``unknown`` (not determinable without
        running an upstream task).
    """

    node_id: int
    base_name: str
    label: str
    kind: str
    env: str | None
    mapped: bool
    threads: int
    memory_gb: int
    gpu: int
    cache_status: CacheStatus


@dataclass(kw_only=True)
class PlanWave:
    """A group of tasks with no dependencies on one another.

    Parameters
    ----------
    index : int
        1-based wave number.
    tasks : list[PlannedTask]
        Tasks in the wave, in scheduler-registration order.
    """

    index: int
    tasks: list[PlannedTask]


@dataclass(kw_only=True)
class ResourceSummary:
    """Aggregate resource demand across a dry-run plan.

    Parameters
    ----------
    total_threads : int
        Sum of declared core budgets across all tasks.
    peak_wave_threads : int
        Largest per-wave core total.
    peak_wave_index : int
        1-based index of the wave with the largest core total (``0`` if empty).
    total_memory_gb : int
        Sum of declared memory budgets in GiB.
    peak_wave_memory_gb : int
        Largest per-wave memory total in GiB.
    gpu_task_count : int
        Number of tasks declaring ``gpu > 0``.
    """

    total_threads: int
    peak_wave_threads: int
    peak_wave_index: int
    total_memory_gb: int
    peak_wave_memory_gb: int
    gpu_task_count: int


@dataclass(kw_only=True)
class DryRunPlan:
    """A render-ready static execution plan.

    Parameters
    ----------
    workflow_label : str
        Human-readable workflow name (typically the file name).
    task_count : int
        Total number of tasks in the static graph.
    wave_count : int
        Number of dependency waves.
    waves : list[PlanWave]
        The waves, in execution order.
    resources : ResourceSummary
        Aggregate resource demand.
    cached_count, will_run_count, unknown_count : int
        Task counts per cache status.
    """

    workflow_label: str
    task_count: int
    wave_count: int
    waves: list[PlanWave]
    resources: ResourceSummary
    cached_count: int
    will_run_count: int
    unknown_count: int


def build_dry_run_plan(*, evaluator: ConcurrentEvaluator, workflow_label: str) -> DryRunPlan:
    """Build a static execution plan from a validated evaluator.

    Must be called after ``evaluator.validate(...)``. Read-only: it resolves
    arguments and probes the cache, but executes no tasks and materialises
    nothing into the workspace.

    Parameters
    ----------
    evaluator : ConcurrentEvaluator
        An evaluator whose static graph has already been built by ``validate``.
    workflow_label : str
        Human-readable workflow name for the plan header.

    Returns
    -------
    DryRunPlan
        The structured plan, grouped into dependency waves.
    """
    nodes = evaluator.task_nodes
    waves_by_node = _assign_waves(nodes)
    topo_order = sorted(nodes, key=lambda node_id: (waves_by_node[node_id], node_id))
    cache_status = _resolve_cache_status(evaluator=evaluator, topo_order=topo_order)

    planned: dict[int, PlannedTask] = {}
    for node_id in topo_order:
        node = nodes[node_id]
        task_def = node.task_def
        planned[node_id] = PlannedTask(
            node_id=node_id,
            base_name=task_def.name.rsplit(".", 1)[-1],
            label=_task_label(node),
            kind=task_def.kind,
            env=task_def.env,
            mapped=node.expr.mapped,
            threads=task_def.threads,
            memory_gb=_memory_gb(task_def),
            gpu=task_def.gpu,
            cache_status=cache_status[node_id],
        )

    wave_count = (max(waves_by_node.values()) + 1) if waves_by_node else 0
    waves = [
        PlanWave(
            index=index + 1,
            tasks=[planned[nid] for nid in topo_order if waves_by_node[nid] == index],
        )
        for index in range(wave_count)
    ]

    statuses = [task.cache_status for task in planned.values()]
    return DryRunPlan(
        workflow_label=workflow_label,
        task_count=len(nodes),
        wave_count=wave_count,
        waves=waves,
        resources=_summarise_resources(waves),
        cached_count=statuses.count("cached"),
        will_run_count=statuses.count("will_run"),
        unknown_count=statuses.count("unknown"),
    )


def _assign_waves(nodes: Mapping[int, TaskNode]) -> dict[int, int]:
    """Assign each node a 0-based wave (longest dependency path to a root)."""
    indegree = {node_id: len(node.dependency_ids) for node_id, node in nodes.items()}
    dependents: dict[int, list[int]] = {node_id: [] for node_id in nodes}
    for node_id, node in nodes.items():
        for dep_id in node.dependency_ids:
            dependents[dep_id].append(node_id)

    wave = {node_id: 0 for node_id, degree in indegree.items() if degree == 0}
    queue = deque(wave)
    while queue:
        node_id = queue.popleft()
        for child_id in dependents[node_id]:
            wave[child_id] = max(wave.get(child_id, 0), wave[node_id] + 1)
            indegree[child_id] -= 1
            if indegree[child_id] == 0:
                queue.append(child_id)
    return wave


def _resolve_cache_status(
    *, evaluator: ConcurrentEvaluator, topo_order: list[int]
) -> dict[int, CacheStatus]:
    """Resolve cache status for every node via a leaf-anchored cascade.

    Nodes are visited in topological order. A node is checkable only while
    every dependency is a confirmed cache hit; once the chain breaks, the node
    and everything below it is ``unknown``.
    """
    nodes = evaluator.task_nodes
    status: dict[int, CacheStatus] = {}
    for node_id in topo_order:
        status[node_id] = _probe_node(evaluator=evaluator, node=nodes[node_id], status=status)
    return status


def _probe_node(
    *, evaluator: ConcurrentEvaluator, node: TaskNode, status: dict[int, CacheStatus]
) -> CacheStatus:
    """Return the cache status of one node, given resolved upstream statuses."""
    # Downstream of anything not known-cached: the cache key cannot be
    # computed without first running an upstream task.
    if any(status.get(dep_id) != "cached" for dep_id in node.dependency_ids):
        return "unknown"

    # Notebook/script cache keys fold in a source hash obtained by evaluating
    # the task body; skip them rather than run user code during a dry run.
    if node.task_def.kind in _DRIVER_KINDS:
        return "unknown"

    cache_store = evaluator.cache_store
    try:
        resolved_args = evaluator.resolve_probe_args(node=node)
        cache_key, _ = cache_store.build_cache_key(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )
    except Exception:
        # Cache status is best-effort: any resolution failure degrades the
        # node to "unknown" rather than aborting the preview.
        return "unknown"

    if not cache_store.has_entry(cache_key=cache_key):
        return "will_run"

    cached_value = cache_store.load(cache_key=cache_key)
    if cached_value is MISSING:
        return "will_run"

    # Record the hit so dependents can resolve their own arguments against
    # this output, mirroring the evaluator's prepare-phase cache fast path.
    node.cache_key = cache_key
    node.result = cached_value
    node.state = "completed"
    return "cached"


def _task_label(node: TaskNode) -> str:
    """Return a display label, including resolved fan-out parameters."""
    base_name = node.task_def.name.rsplit(".", 1)[-1]
    parts = node.expr.display_label_parts
    if parts:
        return f"{base_name}[{','.join(parts)}]"
    return f"{base_name}()"


def _memory_gb(task_def: object) -> int:
    """Return a task's declared memory budget in whole GiB (``0`` when unset)."""
    memory = getattr(task_def, "memory_gb", None)
    return int(memory) if memory else 0


def _summarise_resources(waves: list[PlanWave]) -> ResourceSummary:
    """Aggregate per-task resource declarations into run- and wave-level totals."""
    all_tasks = [task for wave in waves for task in wave.tasks]
    peak_threads = 0
    peak_index = 0
    peak_memory = 0
    for wave in waves:
        wave_threads = sum(task.threads for task in wave.tasks)
        if wave_threads > peak_threads:
            peak_threads = wave_threads
            peak_index = wave.index
        peak_memory = max(peak_memory, sum(task.memory_gb for task in wave.tasks))

    return ResourceSummary(
        total_threads=sum(task.threads for task in all_tasks),
        peak_wave_threads=peak_threads,
        peak_wave_index=peak_index,
        total_memory_gb=sum(task.memory_gb for task in all_tasks),
        peak_wave_memory_gb=peak_memory,
        gpu_task_count=sum(1 for task in all_tasks if task.gpu > 0),
    )
