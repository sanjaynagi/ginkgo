"""Static execution-plan preview for ``ginkgo run --dry-run``.

Turns the validated task graph held by a :class:`ConcurrentEvaluator` into a
structured, render-ready plan: concurrency waves, per-task cache status, and a
resource summary. The builder is pure and read-only — no task is executed, no
environment prepared, and no cached output materialised into the workspace.
"""

from __future__ import annotations

import os
import tempfile
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from ginkgo.core.expr import ConstructedCall
from ginkgo.runtime.caching.cache import MISSING, DynamicTemplate
from ginkgo.runtime.events import task_id_for_node
from ginkgo.runtime.static_directive import static_directive
from ginkgo.runtime.task_kinds import task_kind

if TYPE_CHECKING:
    from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
    from ginkgo.runtime.evaluator import ConcurrentEvaluator, NodeRun
    from ginkgo.runtime.graph import TaskNode

CacheStatus = Literal["cached", "will_run", "unknown"]


@dataclass(kw_only=True)
class PlanDiagnostic:
    """A problem the dry run can already prove would fail the real run.

    Either an input-contract violation found while probing the cache, or a
    resource demand the run's budgets can never satisfy.

    Parameters
    ----------
    task_id : str
        Stable task id (``task_0007``), for event-stream consumers.
    task_name : str
        Full task name (``module.task``), for event-stream consumers.
    label : str
        Display label of the task that would fail.
    message : str
        The refusal, verbatim — the same message the run would fail with.
    """

    task_id: str
    task_name: str
    label: str
    message: str


@dataclass(kw_only=True)
class ProbeFailure:
    """An unexpected error the cache probe hit while examining one task.

    Distinct from :class:`PlanDiagnostic`: a diagnostic says the *workflow*
    would fail, while a probe failure says *Ginkgo* could not work out the
    task's cache status. The node degrades to ``unknown`` either way, but the
    two must not read as the same thing.

    Parameters
    ----------
    task_id : str
        Stable task id (``task_0007``).
    task_name : str
        Full task name (``module.task``).
    label : str
        Display label of the task being probed.
    stage : str
        The probe step that raised (``resolve_args`` or ``cache_key``).
    exception_type : str
        Class name of the exception, e.g. ``KeyError``.
    message : str
        The exception's message.
    """

    task_id: str
    task_name: str
    label: str
    stage: str
    exception_type: str
    message: str


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
        Display label; the same one the live run table gives this node.
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
    custom : dict[str, int]
        Declared user-defined resource demands (empty when none).
    cache_status : CacheStatus
        ``cached``, ``will_run``, or ``unknown`` (not determinable without
        running an upstream task).
    run_reasons : tuple[str, ...]
        For a ``will_run`` task, why its key misses, as ``ginkgo cache
        explain`` reason codes (``source_hash_changed``, ``input_changed``,
        ``no_prior_entry``, ...), or ``rerun_requested`` for a task named
        by ``--rerun``. Empty otherwise.
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
    custom: dict[str, int]
    cache_status: CacheStatus
    run_reasons: tuple[str, ...] = ()


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
        Sum of declared threads across all tasks.
    peak_wave_threads : int
        Largest per-wave sum of declared threads. Not bounded by the run's
        core budget, which the scheduler enforces by queueing within a wave.
    peak_wave_index : int
        1-based index of the wave with the largest thread total (``0`` if empty).
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
    custom_totals: dict[str, int]


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
    dropped_labels : tuple[str, ...]
        Labels of task calls constructed by the flow but unreachable from its
        return value, and so absent from the waves above. A partial call's
        label names the required arguments it left out.
    diagnostics : tuple[PlanDiagnostic, ...]
        Failures the run is certain to hit: a task whose demand exceeds the
        run's ``--cores``/``--memory``/``--resource`` budgets, or whose
        resolved arguments the validator already refuses. The latter is only
        provable on a warm cache — a task downstream of anything un-cached
        cannot be resolved, so an empty tuple is "nothing provable", not
        "nothing wrong". The run command exits non-zero when any are present.
    probe_failures : tuple[ProbeFailure, ...]
        Unexpected errors raised while probing the cache. Each one turned a
        task ``unknown``; none of them says anything about whether the
        workflow itself is sound, so they neither fail the command nor show
        in the default output.
    """

    workflow_label: str
    task_count: int
    wave_count: int
    waves: list[PlanWave]
    resources: ResourceSummary
    cached_count: int
    will_run_count: int
    unknown_count: int
    dropped_labels: tuple[str, ...] = ()
    diagnostics: tuple[PlanDiagnostic, ...] = ()
    probe_failures: tuple[ProbeFailure, ...] = ()


def build_dry_run_plan(
    *,
    evaluator: ConcurrentEvaluator,
    workflow_label: str,
    unreachable_calls: Sequence[ConstructedCall] = (),
) -> DryRunPlan:
    """Build a static execution plan from a validated evaluator.

    Must be called after ``evaluator.build_graph(...)`` and
    ``evaluator.validate_graph()``. Read-only: it resolves arguments and
    probes the cache, but executes no tasks and materialises nothing into the
    workspace.

    Parameters
    ----------
    evaluator : ConcurrentEvaluator
        An evaluator whose static graph has already been built by ``validate``.
    workflow_label : str
        Human-readable workflow name for the plan header.
    unreachable_calls : Sequence[ConstructedCall]
        Task calls the graph never reached, listed as dropped.

    Returns
    -------
    DryRunPlan
        The structured plan, grouped into dependency waves.
    """
    nodes = evaluator.task_nodes
    static_waves = _assign_waves(evaluator.graph.nodes, dynamic_parent_ids={})
    diagnostics: list[PlanDiagnostic] = []
    probe_failures: list[ProbeFailure] = []
    with tempfile.TemporaryDirectory(prefix="ginkgo-dry-run-") as scratch:
        probes = _resolve_cache_status(
            evaluator=evaluator,
            topo_order=sorted(nodes, key=lambda node_id: (static_waves[node_id], node_id)),
            diagnostics=diagnostics,
            probe_failures=probe_failures,
            scratch=Path(scratch),
        )

    # Probing replays cached dynamic templates, so the graph may have grown.
    waves_by_node = _assign_waves(
        evaluator.graph.nodes, dynamic_parent_ids=evaluator.graph.dynamic_parent_ids
    )
    topo_order = sorted(nodes, key=lambda node_id: (waves_by_node[node_id], node_id))
    planned: dict[int, PlannedTask] = {}
    for node_id in topo_order:
        node = nodes[node_id]
        task_def = node.task_def
        resources = evaluator.placement.effective_resources(task_def=task_def)
        try:
            evaluator.check_resource_budget(
                task_name=task_def.name,
                threads=resources.threads,
                memory_gb=resources.memory_gb,
                custom=resources.custom,
                remote=evaluator.placement.resolve(task_def=task_def) is not None,
            )
        except ValueError as exc:
            diagnostics.append(_diagnostic(node=node, exc=exc))
        status, run_reasons = probes[node_id]
        planned[node_id] = PlannedTask(
            node_id=node_id,
            base_name=task_def.base_name,
            label=node.label,
            kind=task_def.kind,
            env=task_def.env,
            mapped=node.node.expr.mapped,
            threads=resources.threads,
            memory_gb=resources.memory_gb,
            gpu=resources.gpu,
            custom=dict(resources.custom),
            cache_status=status,
            run_reasons=run_reasons,
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
        dropped_labels=tuple(_dropped_label(call) for call in unreachable_calls),
        diagnostics=tuple(diagnostics),
        probe_failures=tuple(probe_failures),
    )


def _dropped_label(call: ConstructedCall) -> str:
    """Label one dropped call, naming what a partial call is missing."""
    if call.is_partial:
        return f"{call.label} (missing required argument(s): {', '.join(call.missing_params)})"
    return call.label


def _assign_waves(
    nodes: Mapping[int, TaskNode], *, dynamic_parent_ids: Mapping[int, int]
) -> dict[int, int]:
    """Assign each node a 0-based wave (longest dependency path to a root).

    A task a dynamic expansion registered runs after the task that returned
    it, and a dependent of that task runs after everything it returned, so
    each such node is placed after its parent and before the parent's
    dependents.
    """
    returned: dict[int, set[int]] = {}
    for child_id, parent_id in dynamic_parent_ids.items():
        returned.setdefault(parent_id, set()).add(child_id)

    def everything_returned(node_id: int) -> set[int]:
        found: set[int] = set()
        pending = list(returned.get(node_id, ()))
        while pending:
            child_id = pending.pop()
            if child_id not in found:
                found.add(child_id)
                pending.extend(returned.get(child_id, ()))
        return found

    waits_on: dict[int, set[int]] = {}
    for node_id, node in nodes.items():
        waits = set(node.dependency_ids)
        for dep_id in node.dependency_ids:
            waits |= everything_returned(dep_id)
        if node_id in dynamic_parent_ids:
            waits.add(dynamic_parent_ids[node_id])
        waits_on[node_id] = waits

    indegree = {node_id: len(waits) for node_id, waits in waits_on.items()}
    dependents: dict[int, list[int]] = {node_id: [] for node_id in nodes}
    for node_id, waits in waits_on.items():
        for dep_id in waits:
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
    *,
    evaluator: ConcurrentEvaluator,
    topo_order: list[int],
    diagnostics: list[PlanDiagnostic],
    probe_failures: list[ProbeFailure],
    scratch: Path,
) -> dict[int, tuple[CacheStatus, tuple[str, ...]]]:
    """Resolve cache status for every node via a leaf-anchored cascade.

    Nodes are visited in topological order. A node is checkable only while
    every dependency is a confirmed cache hit; once the chain breaks, the node
    and everything below it is ``unknown``. Probing a checkable node also
    validates its resolved arguments — the run's own input check, brought
    forward — and collects each refusal as a :class:`PlanDiagnostic` (#232).
    Anything the probe did not expect is collected as a
    :class:`ProbeFailure` rather than discarded (#294).

    A cached dynamic task's template is expanded as a hit would replay it,
    and each task it returns is probed in turn, so the plan shows every
    child's own status. The parent's value is known only when they are all
    cached.

    Returns
    -------
    dict[int, tuple[CacheStatus, tuple[str, ...]]]
        Each node's status, with the reasons a ``will_run`` node misses.
    """
    probes: dict[int, tuple[CacheStatus, tuple[str, ...]]] = {}
    _probe_nodes(
        evaluator=evaluator,
        node_ids=topo_order,
        probes=probes,
        diagnostics=diagnostics,
        probe_failures=probe_failures,
        scratch=scratch,
    )
    return probes


def _probe_nodes(
    *,
    evaluator: ConcurrentEvaluator,
    node_ids: list[int],
    probes: dict[int, tuple[CacheStatus, tuple[str, ...]]],
    diagnostics: list[PlanDiagnostic],
    probe_failures: list[ProbeFailure],
    scratch: Path,
) -> None:
    """Probe *node_ids* in order, recursing into each cached dynamic template."""
    nodes = evaluator.task_nodes
    for node_id in node_ids:
        node = nodes[node_id]
        status, reasons, template = _probe_node(
            evaluator=evaluator,
            node=node,
            dependencies_cached=all(
                nodes[dep_id].state == "completed" for dep_id in node.node.dependency_ids
            ),
            diagnostics=diagnostics,
            probe_failures=probe_failures,
            scratch=scratch,
        )
        probes[node_id] = (status, reasons)
        if template is None:
            continue
        try:
            returned = evaluator.expand_probed_template(node=node, template=template)
        except Exception as exc:
            # The run would fail the same way when the hit replays it.
            diagnostics.append(_diagnostic(node=node, exc=exc))
            probes[node_id] = ("unknown", ())
            continue
        _probe_nodes(
            evaluator=evaluator,
            node_ids=returned,
            probes=probes,
            diagnostics=diagnostics,
            probe_failures=probe_failures,
            scratch=scratch,
        )
        evaluator.complete_probed_template(node=node)


def _probe_node(
    *,
    evaluator: ConcurrentEvaluator,
    node: NodeRun,
    dependencies_cached: bool,
    diagnostics: list[PlanDiagnostic],
    probe_failures: list[ProbeFailure],
    scratch: Path,
) -> tuple[CacheStatus, tuple[str, ...], Any]:
    """Return the cache status of one node, with the reasons a ``will_run`` misses.

    The third item is the template a cached dynamic task's entry holds, for
    the caller to expand, and otherwise ``None``.
    """
    # Downstream of anything not known-cached: the cache key cannot be
    # computed without first running an upstream task.
    if not dependencies_cached:
        return "unknown", (), None

    try:
        resolved_args = evaluator.resolve_probe_args(node=node)
    except (FileNotFoundError, ValueError):
        # The honest unknowns: a working-tree path the live run would restore
        # from the artifact store before touching it, or a value the probe
        # cannot stage because staging is a side effect. No claim, no noise.
        return "unknown", (), None
    except Exception as exc:
        # Anything else is a probe defect, not an honest unknown. The preview
        # still degrades to "unknown" rather than aborting a dry run of a
        # workflow that may well execute fine — but it says so (#294).
        probe_failures.append(_probe_failure(node=node, stage="resolve_args", exc=exc))
        return "unknown", (), None

    # Validated and hashed as the run will see them, after it restores any
    # upstream folder; dependents still read the arguments as resolved.
    known_digests = evaluator.node_cache.digests.known
    cache_store = evaluator.cache_store
    probe_args = {
        name: _as_restored(
            value=value,
            known_digests=known_digests,
            artifact_store=cache_store.artifact_store_view,
            scratch=scratch,
        )
        for name, value in resolved_args.items()
    }

    # With the real arguments in hand — cached upstream outputs included —
    # the run's own input validation can already say whether this task would
    # fail on dispatch. A refusal is a diagnosis, not a probe failure, so it
    # is collected rather than swallowed (#232).
    try:
        evaluator.validator.validate_inputs(task_def=node.task_def, resolved_args=probe_args)
    except TypeError as exc:
        diagnostics.append(_diagnostic(node=node, exc=exc))
        return "will_run", (), None
    except (FileNotFoundError, ValueError):
        # The same errors NodeCache._is_valid_cached_result treats as "not
        # really cached": typically a working-tree path the live run would
        # restore from the artifact store before validating. The probe must
        # not materialise anything, so it cannot tell — no claim.
        return "unknown", (), None

    # Some kinds' cache keys fold in the source hash of the directive their
    # body returns. The run calls the body for it; the probe must not run
    # user code, so it predicts the directive statically or makes no claim.
    extra_source_hash: str | None = None
    kind = task_kind(node.task_def)
    if kind.eager_source_hash:
        directive = (
            None
            if kind.directive is None
            else static_directive(
                fn=node.task_def.fn, resolved_args=resolved_args, directive_type=kind.directive
            )
        )
        if directive is None:
            return "unknown", (), None
        extra_source_hash = directive.source_hash

    if evaluator.rerun_requested(node=node):
        return "will_run", ("rerun_requested",), None

    try:
        # Hash an upstream output by the digest its cache entry recorded, as
        # the run does: the run restores that output before this task reads
        # it, so the bytes on disk now are not what this task will see.
        cache_key, input_hashes = cache_store.build_cache_key(
            task_def=node.task_def,
            resolved_args=probe_args,
            extra_source_hash=extra_source_hash,
            known_digests=known_digests,
        )
    except (FileNotFoundError, ValueError):
        # Hashing a file input the live run would restore first, or a remote
        # ref the probe deliberately left unstaged (``_hash_value`` refuses a
        # ``RemoteRef`` without a ``version_id``). Cannot know without
        # executing — the same reasoning as the block above.
        return "unknown", (), None
    except Exception as exc:
        probe_failures.append(_probe_failure(node=node, stage="cache_key", exc=exc))
        return "unknown", (), None

    cached_value = (
        cache_store.load(cache_key=cache_key, task_def=node.task_def)
        if cache_store.has_entry(cache_key=cache_key, task_def=node.task_def)
        else MISSING
    )
    if cached_value is MISSING:
        reasons = cache_store.miss_reasons(
            cache_key=cache_key,
            task_def=node.task_def,
            input_hashes=input_hashes,
            extra_source_hash=extra_source_hash,
        )
        return "will_run", tuple(reasons), None

    # Record the hit so dependents can resolve their own arguments against
    # this output, mirroring the evaluator's prepare-phase cache fast path.
    # A dependent reading ``.output["name"]`` takes the value from this
    # node's resolved arguments rather than its result.
    node.cache_key = cache_key
    node.resolved_args = resolved_args
    if isinstance(cached_value, DynamicTemplate):
        # Its value is the one its returned tasks resolve to, if they all hit.
        return "cached", (), cached_value.template
    node.result = cached_value
    node.state = "completed"
    evaluator.node_cache.propagate_known_digests(cache_key=cache_key)
    return "cached", (), None


def _as_restored(
    *,
    value: Any,
    known_digests: Mapping[str, str],
    artifact_store: LocalArtifactStore,
    scratch: Path,
) -> Any:
    """Return *value* with each drifted upstream folder swapped for its stored copy.

    On a cache hit the run restores an upstream task's folder output before
    any consumer hashes it, so a hand edit inside the folder never reaches
    the consumer's key. The probe must not touch the workspace, so it points
    the consumer at a symlinked copy of the stored tree instead: a folder's
    hash covers its contents and relative paths, never its own location.
    Upstream files need no swap, since ``build_cache_key`` hashes them by
    their recorded digest.
    """
    if isinstance(value, list | tuple):
        return type(value)(
            _as_restored(
                value=item,
                known_digests=known_digests,
                artifact_store=artifact_store,
                scratch=scratch,
            )
            for item in value
        )
    if not isinstance(value, str | os.PathLike):
        return value
    path = Path(value).resolve()
    artifact_id = known_digests.get(str(path))
    if artifact_id is None:
        return value
    record = artifact_store.load_record(artifact_id=artifact_id)
    if record is None or record.kind != "tree":
        return value
    if artifact_store.matches(artifact_id=artifact_id, path=path):
        return value
    copy = Path(tempfile.mkdtemp(dir=scratch)) / path.name
    artifact_store.retrieve(artifact_id=artifact_id, dest_path=copy)
    return type(value)(copy)


def _diagnostic(*, node: NodeRun, exc: Exception) -> PlanDiagnostic:
    """Record a failure the run is certain to hit against *node*."""
    return PlanDiagnostic(
        task_id=task_id_for_node(node.node_id),
        task_name=node.task_def.base_name,
        label=node.label,
        message=str(exc),
    )


def _probe_failure(*, node: NodeRun, stage: str, exc: Exception) -> ProbeFailure:
    """Record one unexpected probe error against the node it was probing."""
    return ProbeFailure(
        task_id=task_id_for_node(node.node_id),
        task_name=node.task_def.base_name,
        label=node.label,
        stage=stage,
        exception_type=type(exc).__name__,
        message=str(exc),
    )


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

    custom_totals: dict[str, int] = {}
    for task in all_tasks:
        for dimension, demand in task.custom.items():
            custom_totals[dimension] = custom_totals.get(dimension, 0) + demand

    return ResourceSummary(
        total_threads=sum(task.threads for task in all_tasks),
        peak_wave_threads=peak_threads,
        peak_wave_index=peak_index,
        total_memory_gb=sum(task.memory_gb for task in all_tasks),
        peak_wave_memory_gb=peak_memory,
        gpu_task_count=sum(1 for task in all_tasks if task.gpu > 0),
        custom_totals=custom_totals,
    )
