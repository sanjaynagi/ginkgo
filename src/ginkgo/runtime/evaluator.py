"""Concurrent evaluator for Ginkgo expressions."""

from __future__ import annotations


import os
import shutil
import tempfile
import time
import builtins
from collections.abc import Mapping, Set as AbstractSet
from contextlib import ExitStack
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from multiprocessing import Manager
from pathlib import Path
from typing import Any, Literal

from ginkgo.core.asset import AssetRef, AssetVersion, collect_asset_refs
from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.expr import ConstructedCall, Expr, ExprList, OutputIndex, OutputName
from ginkgo.core.subworkflow import SubWorkflowResult
from ginkgo.core.notebook import NotebookDirective
from ginkgo.core.script import ScriptDirective
from ginkgo.core.shell import ShellDirective
from ginkgo.errors import GinkgoError
from ginkgo.params import ParamContext
from ginkgo.core.subworkflow import SubWorkflowDirective
from ginkgo.core.resources import ResourceOverrides, Resources
from ginkgo.core.task import TaskDef
from ginkgo.core.types import (
    is_path_shaped_annotation,
    pair_elements_with_annotations,
    tmp_dir,
    unwrap_optional_annotation,
)
from ginkgo.envs.container import is_container_env
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.edge_inference import (
    ConsumedPath,
    ProducedPath,
    PathIndex,
    collect_consumed_paths,
    collect_produced_paths,
)
from ginkgo.runtime.executor_registry import LOCAL, ExecutorRegistry
from ginkgo.runtime.remote_dispatch import RemoteDispatchManager
from ginkgo.runtime.remote_executor import RemoteDispatchStats
from ginkgo.runtime.artifacts.asset_registration import AssetRegistrar, asset_index_for
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry
from ginkgo.runtime.artifacts.output_index import output_summary
from ginkgo.runtime.artifacts.asset_kinds import REHYDRATABLE_KINDS
from ginkgo.runtime.artifacts.asset_loaders import load_from_ref as load_wrapped_ref
from ginkgo.runtime.caching.cache import MISSING, CacheStore
from ginkgo.runtime.caching.node_cache import NodeCache
from ginkgo.runtime.caching.digest_registry import DigestRegistry
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.executors import Executors
from ginkgo.runtime.events import (
    EnvPrepareCompleted,
    EnvPrepareFailed,
    EnvPrepareStarted,
    EventBus,
    GraphExpanded,
    GraphNodeRegistered,
    PhaseTimed,
    TaskAnnotated,
    TaskCacheHit,
    TaskCacheMiss,
    TaskCompleted,
    TaskFailed,
    TaskNotice,
    TaskPlanned,
    TaskReady,
    TaskRetrying,
    TaskSkipped,
    TaskStaging,
    TaskStarted,
    task_id_for_node,
)
from ginkgo.runtime.log_drain import LogDrain
from ginkgo.runtime.module_loader import resolve_module_file
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.profiling import ProfileRecorder
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset
from ginkgo.runtime.environment.secrets import (
    SecretResolver,
    collect_resolved_secret_values,
    resolve_secret_refs,
)
from ginkgo.runtime.remote_input_resolver import (
    RemoteStager,
    count_remote_inputs,
    load_remote_publisher,
    resolve_staging_jobs,
)
from ginkgo.runtime.task_runners.notebook import (
    NotebookRunner,
    first_label_param_name,
    render_label_value,
)
from ginkgo.runtime.task_runners.script import ScriptRunner
from ginkgo.runtime.task_runners.shell import (
    ShellRunner,
    SignalMonitor,
    classify_failure,
    sanitize_exception,
)
from ginkgo.runtime.task_runners.subworkflow import SubworkflowRunner
from ginkgo.runtime.task_validation import (
    TaskValidator,
    contains_dynamic_expression,
    declared_output_paths,
    is_untracked_directory_value,
)
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_value
from ginkgo.runtime.worker import _task_log_context, run_task
from ginkgo.workspace_layout import WorkspaceLayout

# Maps each ExecutionDirective subclass to the (runner_attr, method_name) pair used
# to dispatch it. The completeness check below catches any imported subclass that
# has no entry; it does not catch a subclass whose module is never imported.
_DIRECTIVE_RUNNER: dict[type[ExecutionDirective], tuple[str, str]] = {
    ShellDirective: ("_shell_runner", "run_shell"),
    NotebookDirective: ("_notebook_runner", "run_notebook"),
    ScriptDirective: ("_script_runner", "run_script"),
    SubWorkflowDirective: ("_subworkflow_runner", "run_subworkflow"),
}
_unregistered = set(ExecutionDirective.__subclasses__()) - set(_DIRECTIVE_RUNNER)
if _unregistered:
    raise ImportError(
        "ExecutionDirective subclasses with no runner entry: "
        + ", ".join(sorted(t.__name__ for t in _unregistered))
    )
del _unregistered


class CycleError(GinkgoError, RuntimeError):
    """Raised when the expression graph contains a dependency cycle."""

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        rendered = " -> ".join(cycle)
        super().__init__(f"Detected cycle in workflow graph: {rendered}")


class RootSkippedError(GinkgoError, RuntimeError):
    """Raised when the run drained but the workflow result was never produced.

    Only reachable under a failure policy that keeps dispatching: a task the
    result depends on failed and was ignored, so every task between it and
    the root was skipped and there is no value to return. The run drained
    normally, so this is an outcome rather than a crash.
    """

    def __init__(self, *, task_name: str, task_id: str) -> None:
        self.task_name = task_name
        self.task_id = task_id
        super().__init__(
            f"The workflow result depends on {task_name} ({task_id}), which failed. "
            "No result was produced."
        )


def _reconstruct_worker_error(error_payload: dict[str, Any]) -> BaseException:
    """Rebuild a task exception reported by a worker subprocess."""
    module_name = error_payload["module"]
    type_name = error_payload["type"]
    args = error_payload["args"]

    if module_name == "builtins":
        exc_type = getattr(builtins, type_name, RuntimeError)
        if isinstance(exc_type, type) and issubclass(exc_type, BaseException):
            return exc_type(*args)

    return RuntimeError(error_payload["message"])


def evaluate(
    expr: Any,
    *,
    jobs: int | None = None,
    cores: int | None = None,
    memory: int | None = None,
    gpus: int | None = None,
    resource_overrides: ResourceOverrides | None = None,
    resource_budgets: dict[str, int] | None = None,
    backend: ExecutionEnvironment | None = None,
    run_dir: RunDir | None = None,
    secret_resolver: SecretResolver | None = None,
    event_bus: EventBus | None = None,
) -> Any:
    """Resolve an expression tree to concrete values.

    Parameters
    ----------
    expr : Any
        The root expression or nested container to resolve.
    jobs : int | None
        Maximum number of concurrently running tasks.
    cores : int | None
        Maximum total thread budget across running tasks.
    memory : int | None
        Maximum total declared memory budget across running tasks in GiB.
    gpus : int | None
        Local GPU budget across running tasks. Defaults to 0 (no local
        GPUs); tasks declaring ``gpu > 0`` then require a remote executor.
    resource_overrides : ResourceOverrides | None
        Site-level resource overrides merged over each task's declaration.
    resource_budgets : dict[str, int] | None
        Run-level budgets for user-defined resource dimensions (e.g.
        ``{"api_calls": 10}``). Dimensions tasks request but this mapping
        omits are unconstrained.
    backend : ExecutionEnvironment | None
        Execution environment for environment-isolated tasks.
    run_dir : RunDir | None
        The run's directory, for per-task log paths and lockfile copies.
        ``None`` outside a live run.
    event_bus : EventBus | None
        Optional event bus to receive lifecycle events. Useful for tests
        and ad-hoc programmatic callers that want to observe task progress.

    Returns
    -------
    Any
        The concrete result of evaluating the input.
    """
    return ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        gpus=gpus,
        resource_overrides=resource_overrides,
        resource_budgets=resource_budgets,
        backend=backend,
        run_dir=run_dir,
        secret_resolver=secret_resolver,
        event_bus=event_bus,
    ).evaluate(expr)


# Closed set of task-node lifecycle states. Phase -> field-availability
# invariants (enforced by asserts at the read sites):
#
# - resolved_args is non-None from "ready" onward (set by _prepare_node,
#   refreshed on dispatch and after staging); it is None in "pending" and
#   is cleared by _schedule_retry ("waiting_retry" / retried "pending").
# - execution_args is non-None in "running" and "running_shell" (set when
#   entering "running"); cleared on completion and on retry.
# - transport_path is non-None only in "running" when the task executes via
#   the process pool or a remote executor (never for driver tasks); cleared
#   by _cleanup_transport on completion, failure, and retry.
# - blocked_by_task_id / blocked_by_task_name are non-None only in
#   "skipped", naming the failed task the skip is attributed to.
_NodeState = Literal[
    "pending",
    "ready",
    "staging",
    "running",
    "running_shell",
    "waiting_dynamic",
    "waiting_retry",
    "completed",
    "failed",
    "skipped",
]

_TERMINAL_NODE_STATES = frozenset({"completed", "failed", "skipped"})
"""Node states no scheduler pass can move a node out of."""

_IN_FLIGHT_NODE_STATES = frozenset({"staging", "running", "running_shell"})
"""Node states held by work the scheduler has already handed to an executor."""


@dataclass(frozen=True, eq=False, kw_only=True)
class TaskNode:
    """Immutable identity of one task in the evaluator's dependency graph.

    A node's identity is fixed when the graph is registered; everything
    that changes as the scheduler drives the task through its lifecycle
    lives on :class:`NodeRun`. Instances hash and compare by object
    identity, so pure scheduling code can hold them safely.
    """

    node_id: int
    expr: Expr
    dependency_ids: frozenset[int]
    inferred_dependency_ids: frozenset[int] = frozenset()
    """The subset of :attr:`dependency_ids` inferred from ``Out[...]`` paths
    (issue #307/#280) rather than declared through the ``Expr`` graph.

    Mutated in place after registration via ``object.__setattr__`` — see
    ``ConcurrentEvaluator._infer_and_apply_edges``. Kept separate from
    :attr:`dependency_ids` only so an inferred edge stays cheaply
    distinguishable; scheduling, dry-run waves and completion checks all
    still read the combined :attr:`dependency_ids`.
    """

    @property
    def task_def(self) -> TaskDef:
        """Return the task definition for the node."""
        return self.expr.task_def

    @property
    def concurrency_group(self) -> str | None:
        """Return the node's declared concurrency group, if any."""
        return self.expr.concurrency_group

    @property
    def concurrency_group_limit(self) -> int | None:
        """Return the concurrency limit for the node's group, if any."""
        return self.expr.concurrency_group_limit


@dataclass(kw_only=True)
class NodeRun:
    """Mutable run state of one task node.

    Runs are created and mutated by :class:`ConcurrentEvaluator` as it
    schedules work; the immutable graph vertex lives at :attr:`node`.
    Read-only consumers (such as the dry-run planner) access runs
    through :attr:`ConcurrentEvaluator.task_nodes`.
    """

    node: TaskNode
    state: _NodeState = "pending"
    resolved_args: dict[str, Any] | None = None
    execution_args: dict[str, Any] | None = None
    cache_key: str | None = None
    input_hashes: dict[str, Any] | None = None
    input_labels: dict[str, str] | None = None
    content_input_digests: dict[str, str | None] | None = None
    threads: int = 1
    memory_gb: int = 0
    declared_memory_gb: int = 0
    gpu: int = 0
    custom_resources: dict[str, int] = field(default_factory=dict)
    executor_name: str | None = None
    result: Any = MISSING
    tmp_paths: list[Path] = field(default_factory=list)
    transport_path: Path | None = None
    dynamic_template: Any = None
    dynamic_dependency_ids: set[int] = field(default_factory=set)
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    display_label: str | None = None
    attempt: int = 0
    retry_ready_at: float | None = None
    secret_values: tuple[str, ...] = ()
    driver_directive: Any = None
    extra_source_hash: str | None = None
    asset_versions: list[AssetVersion] = field(default_factory=list)
    asset_inputs: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    notebook_extras: dict[str, Any] | None = None
    remote_job_id: str | None = None
    measured_resources: dict[str, Any] | None = None
    blocked_by_task_id: str | None = None
    blocked_by_task_name: str | None = None

    @property
    def remote(self) -> bool:
        """Whether the node is placed on a remote executor."""
        return self.executor_name is not None

    # Identity views, delegated so collaborators that receive a run can
    # read the vertex without reaching through ``.node``.
    @property
    def node_id(self) -> int:
        return self.node.node_id

    @property
    def expr(self) -> Expr:
        return self.node.expr

    @property
    def task_def(self) -> TaskDef:
        return self.node.task_def

    @property
    def dependency_ids(self) -> frozenset[int]:
        return self.node.dependency_ids

    @property
    def inferred_dependency_ids(self) -> frozenset[int]:
        return self.node.inferred_dependency_ids

    @property
    def concurrency_group(self) -> str | None:
        return self.node.concurrency_group

    @property
    def concurrency_group_limit(self) -> int | None:
        return self.node.concurrency_group_limit


@dataclass(kw_only=True)
class ConcurrentEvaluator:
    """Concurrent evaluator with dependency tracking and cache integration."""

    jobs: int | None = None
    cores: int | None = None
    memory: int | None = None
    gpus: int | None = None
    resource_overrides: ResourceOverrides | None = None
    resource_budgets: dict[str, int] | None = None
    backend: ExecutionEnvironment | None = None
    executor_registry: ExecutorRegistry = field(default_factory=ExecutorRegistry)
    run_dir: RunDir | None = None
    secret_resolver: SecretResolver | None = None
    event_bus: EventBus | None = None
    trust_mtimes: bool = False
    keep_going: bool = False
    profiler: ProfileRecorder | None = None
    constructed_calls: tuple[ConstructedCall, ...] = ()
    _cache_store: CacheStore = field(init=False, repr=False)
    _asset_store: AssetStore = field(init=False, repr=False)
    _nodes: dict[int, NodeRun] = field(default_factory=dict, init=False, repr=False)
    _expr_nodes: dict[int, int] = field(default_factory=dict, init=False, repr=False)
    _running_futures: dict[Future[Any], tuple[int, str]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _next_node_id: int = field(default=0, init=False, repr=False)
    _root_template: Any = field(default=None, init=False, repr=False)
    _root_dependency_ids: set[int] = field(default_factory=set, init=False, repr=False)
    _failure: BaseException | None = field(default=None, init=False, repr=False)
    _ignored_failures: list[tuple[NodeRun, BaseException]] = field(
        default_factory=list, init=False, repr=False
    )
    _executors: Executors | None = field(default=None, init=False, repr=False)
    _log_drain: LogDrain = field(init=False, repr=False)
    _staging_jobs: int = field(default=0, init=False, repr=False)
    param_context: ParamContext | None = None
    _digests: DigestRegistry = field(init=False, repr=False)
    _remote_dispatch: RemoteDispatchManager = field(init=False, repr=False)
    _node_cache: NodeCache = field(init=False, repr=False)
    _untracked_path_warnings: set[tuple[str, str, str]] = field(
        default_factory=set, init=False, repr=False
    )
    _effective_resources_cache: dict[str, Resources] = field(
        default_factory=dict, init=False, repr=False
    )
    # Edge inference (#280/#307) -----------------------------------------
    _pending_node_ids: list[int] = field(default_factory=list, init=False, repr=False)
    """Node ids created since the last call to ``_infer_and_apply_edges``,
    whose ``GraphNodeRegistered`` event has not been emitted yet — emission is
    deferred so the event can carry inferred edges (see that method)."""
    _pending_registration_logs: dict[int, tuple[str | None, str | None]] = field(
        default_factory=dict, init=False, repr=False
    )
    _path_index: PathIndex = field(default_factory=PathIndex, init=False, repr=False)
    """Cumulative literal produced/consumed paths across every registration
    batch, so a later batch (dynamic expansion) is matched against everything
    seen so far, and vice versa."""
    _edges_inferred_this_batch: bool = field(default=False, init=False, repr=False)
    """Whether the current batch added an edge, gating the cycle check."""
    _dynamic_parent_ids: dict[int, int] = field(default_factory=dict, init=False, repr=False)
    """Node id -> id of the task whose dynamic expansion registered it."""

    @property
    def unreachable_calls(self) -> list[ConstructedCall]:
        """Task calls that were constructed but never reached by the graph walk.

        Empty unless the caller passed ``constructed_calls`` recorded around the
        flow body. Only meaningful after ``validate`` or ``evaluate`` has
        registered the graph.
        """
        return [
            call
            for call in self.constructed_calls
            if not any(id(expr) in self._expr_nodes for expr in call.exprs)
        ]

    def __post_init__(self) -> None:
        if self.profiler is None:
            self.profiler = ProfileRecorder(enabled=False)
        default_jobs = os.cpu_count() or 1
        self.jobs = default_jobs if self.jobs is None else self.jobs
        self.cores = self.jobs if self.cores is None else self.cores

        if self.jobs < 1:
            raise ValueError("jobs must be at least 1")
        if self.cores < 1:
            raise ValueError("cores must be at least 1")
        if self.memory is not None and self.memory < 1:
            raise ValueError("memory must be at least 1 when provided")
        self.gpus = 0 if self.gpus is None else self.gpus
        if self.gpus < 0:
            raise ValueError("gpus must be at least 0")

        # The cache index writes on the scheduler's threads, so it holds its
        # own connection rather than the recorder's, which belongs to the
        # writer thread.
        self._cache_index = CacheIndex.open(path=WorkspaceLayout.for_cwd().db)
        self._hash_memo = HashMemo(index=self._cache_index)
        self._cache_store = CacheStore(
            index=self._cache_index,
            backend=self.backend,
            publisher=load_remote_publisher(),
            hash_memo=self._hash_memo,
            trust_mtimes=self.trust_mtimes,
        )
        # The catalog shares the cache index's connection and lock: two sets of
        # tables in one database, not two databases.
        self._asset_store = AssetStore.attached_to(self._cache_index)
        self._staging_jobs = resolve_staging_jobs(jobs=self.jobs)
        self._digests = DigestRegistry()
        self._remote_dispatch = RemoteDispatchManager(
            registry=self.executor_registry,
            digests=self._digests,
            local_artifact_store=self._cache_store._artifact_store,
            run_id_provider=lambda: self._run_id,
            emit_event=self._emit_event,
        )

        # Helper runners. Constructed once per evaluation so unit tests can
        # exercise them in isolation and substitute fakes.
        self._validator = TaskValidator(
            backend=self.backend,
            secret_resolver=self.secret_resolver,
        )
        self._node_cache = NodeCache(
            cache_store=self._cache_store,
            validator=self._validator,
            digests=self._digests,
            index=self._cache_index,
        )
        self._log_drain = LogDrain(
            event_bus=self.event_bus,
            run_id_provider=lambda: self._run_id,
        )
        self._shell_runner = ShellRunner(
            backend=self.backend,
            validator=self._validator,
            log_emitter_factory=self._log_drain.make_emitter,
            usage_recorder=self._record_measured_usage,
        )
        self._notebook_runner = NotebookRunner(
            backend=self.backend,
            shell_runner=self._shell_runner,
            validator=self._validator,
            cache_store=self._cache_store,
            run_dir=self.run_dir,
            annotate=self._annotate_task,
            notice_emitter=self._emit_notebook_notice,
            runtime_root_factory=self._notebook_runtime_root,
        )
        self._script_runner = ScriptRunner(
            shell_runner=self._shell_runner,
            validator=self._validator,
        )
        self._subworkflow_runner = SubworkflowRunner(
            shell_runner=self._shell_runner,
            run_id_provider=lambda: self._run_id or "",
            db_path=WorkspaceLayout.for_cwd().db,
        )
        self._stager = RemoteStager(timing_recorder=self._record_task_timing)
        self._live_payloads = LivePayloadRegistry()
        self._asset_registrar = AssetRegistrar(
            cache_store=self._cache_store,
            asset_store=self._asset_store,
            run_id_provider=lambda: self._run_id,
            live_payloads=self._live_payloads,
            emit_event=self._emit_event,
        )

    @property
    def validator(self) -> TaskValidator:
        """The run's input/contract validator, shared with dry-run probing."""
        return self._validator

    @property
    def cache_store(self) -> CacheStore:
        """The cache store backing this evaluator."""
        return self._cache_store

    @property
    def remote_stats(self) -> RemoteDispatchStats:
        """Aggregated remote-dispatch statistics for this run."""
        return self._remote_dispatch.stats

    @property
    def task_nodes(self) -> Mapping[int, NodeRun]:
        """Read-only view of the task graph, keyed by scheduler node id.

        Populated once the graph has been built (after :meth:`build_and_validate` or
        during :meth:`evaluate`). Intended for read-only consumers such as
        the dry-run planner.
        """
        return self._nodes

    def resolve_probe_args(self, *, node: NodeRun) -> dict[str, Any]:
        """Resolve one node's concrete arguments without side effects.

        Read-only companion to the internal argument resolver, for cache
        probing: no scratch directories are created and no remote
        references are staged.

        Parameters
        ----------
        node : NodeRun
            A node whose dependencies have all completed (for example from
            cache hits recorded by a previous probe).

        Returns
        -------
        dict[str, Any]
            The resolved keyword arguments for the task call.
        """
        return self._resolve_task_args(
            expr=node.expr,
            task_def=node.task_def,
            include_tmp_dirs=False,
            stage_remote_refs=False,
        )

    def evaluate(self, expr: Any) -> Any:
        """Resolve a root expression or nested container concurrently.

        Two failure policies drive the scheduler loop. Under the default,
        fail-fast, the first failure a task's retries cannot absorb is kept
        as ``_failure``: no further node is prepared or dispatched, in-flight
        work drains, and the failure is re-raised. Under ``--keep-going`` or
        ``on_failure="ignore"`` on the failing task, the failure is recorded
        in :attr:`ignored_failures` instead, tasks downstream of it are
        marked ``skipped``, and dispatch continues for every branch that is
        still viable. An interrupt always stops the run, either way.

        Raises
        ------
        RootSkippedError
            If the run drained but an ignored failure left the workflow
            result unproducible.
        """
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        self._infer_and_apply_edges()
        if not self._root_dependency_ids:
            return self._materialize(expr)

        # Validate all statically declared environments before any work starts.
        self._validator.validate_declared_envs(nodes=self._nodes.values())
        self._validator.validate_declared_secrets(nodes=self._nodes.values())

        with ExitStack() as stack:
            executors = stack.enter_context(
                Executors(jobs=self.jobs, staging_jobs=self._staging_jobs)
            )
            log_manager = stack.enter_context(Manager())
            signals = stack.enter_context(SignalMonitor())
            self._executors = executors
            self._log_drain.start(queue=log_manager.Queue())
            try:
                while True:
                    if signals.exception is not None and self._failure is None:
                        self._failure = signals.exception
                        self._interrupt_running_work()

                    if self._failure is None:
                        self._promote_due_retries()
                        self._skip_blocked_nodes()
                        with self.profiler.timed("scheduler_prepare"):
                            self._prepare_pending_nodes()
                            self._finalize_dynamic_nodes()
                        with self.profiler.timed("scheduler_dispatch"):
                            self._dispatch_ready_nodes(
                                python_executor=executors.python,
                                shell_executor=executors.shell,
                            )

                        if self._is_root_resolved() and not self._running_futures:
                            return self._materialize(self._root_template)

                    if self._running_futures:
                        retry_wait = self._earliest_retry_wait() if self._failure is None else None
                        with self.profiler.timed("scheduler_wait"):
                            done, _ = wait(
                                tuple(self._running_futures.keys()),
                                return_when=FIRST_COMPLETED,
                                timeout=retry_wait,
                            )
                        with self.profiler.timed("scheduler_consume_completed"):
                            self._consume_completed_futures(done)
                        continue

                    if self._failure is not None:
                        raise self._failure

                    if self._is_root_resolved():
                        return self._materialize(self._root_template)

                    retry_wait = self._earliest_retry_wait()
                    if retry_wait is not None:
                        # Short, signal-interruptable sleep until the next retry is due.
                        time.sleep(min(retry_wait, 0.5))
                        continue

                    if self._can_make_scheduler_progress():
                        continue

                    if self._is_drained():
                        root_skipped = self._root_skipped_error()
                        if root_skipped is not None:
                            raise root_skipped

                    raise RuntimeError("Scheduler reached a deadlock with unresolved tasks")
            except BaseException as exc:
                # Whatever stopped the run, a task it started and will not see
                # finish is closed in the record rather than left running.
                self._close_unfinished_nodes(reason=exc)
                raise
            finally:
                self._log_drain.stop()
                self._executors = None
                self._cache_index.close()

    def _register_value(
        self,
        value: Any,
        *,
        expr_stack: tuple[int, ...] = (),
        task_path: tuple[str, ...] = (),
    ) -> set[int]:
        """Register all task nodes reachable from a nested value."""
        if isinstance(value, (OutputIndex, OutputName)):
            return self._register_value(
                value.expr,
                expr_stack=expr_stack,
                task_path=task_path,
            )

        if isinstance(value, Expr):
            return {
                self._register_expr(
                    value,
                    expr_stack=expr_stack,
                    task_path=task_path,
                )
            }

        if isinstance(value, ExprList):
            dependencies: set[int] = set()
            for item in value:
                dependencies |= self._register_value(
                    item,
                    expr_stack=expr_stack,
                    task_path=task_path,
                )
            return dependencies

        if isinstance(value, list | tuple):
            dependencies: set[int] = set()
            for item in value:
                dependencies |= self._register_value(
                    item,
                    expr_stack=expr_stack,
                    task_path=task_path,
                )
            return dependencies

        if isinstance(value, dict):
            dependencies: set[int] = set()
            for key, item in value.items():
                dependencies |= self._register_value(
                    key,
                    expr_stack=expr_stack,
                    task_path=task_path,
                )
                dependencies |= self._register_value(
                    item,
                    expr_stack=expr_stack,
                    task_path=task_path,
                )
            return dependencies

        return set()

    def _register_expr(
        self,
        expr: Expr,
        *,
        expr_stack: tuple[int, ...] = (),
        task_path: tuple[str, ...] = (),
    ) -> int:
        """Register a task expression node once per object identity."""
        expr_id = id(expr)
        if expr_id in expr_stack:
            cycle_start = expr_stack.index(expr_id)
            cycle = list(task_path[cycle_start:]) + [expr.task_def.name]
            raise CycleError(cycle)

        if expr_id in self._expr_nodes:
            return self._expr_nodes[expr_id]

        node_id = self._next_node_id
        self._next_node_id += 1

        next_expr_stack = (*expr_stack, expr_id)
        next_task_path = (*task_path, expr.task_def.name)
        dependency_ids: set[int] = set()
        for value in expr.args.values():
            dependency_ids |= self._register_value(
                value,
                expr_stack=next_expr_stack,
                task_path=next_task_path,
            )

        self._nodes[node_id] = NodeRun(
            node=TaskNode(
                node_id=node_id,
                expr=expr,
                dependency_ids=frozenset(dependency_ids),
            )
        )
        self._expr_nodes[expr_id] = node_id
        stdout_log = stderr_log = None
        if self.run_dir is not None:
            stdout_path, stderr_path = self.run_dir.log_paths_for(
                node_id=node_id,
                task_name=expr.task_def.name,
            )
            self._nodes[node_id].stdout_path = stdout_path
            self._nodes[node_id].stderr_path = stderr_path
            stdout_log = self.run_dir.relative(stdout_path)
            stderr_log = self.run_dir.relative(stderr_path)
        self._pending_registration_logs[node_id] = (stdout_log, stderr_log)
        # Emission is deferred to ``_infer_and_apply_edges``: a node's final
        # dependency ids are not known until every node reachable from this
        # registration batch has been walked and Out[...] edges inferred
        # across all of them (#280/#307) — a literal path can name a node
        # registered later in the same batch.
        self._pending_node_ids.append(node_id)
        return node_id

    def _infer_and_apply_edges(self, *, expanding_node_id: int | None = None) -> None:
        """Infer ``Out[...]`` path dependency edges and emit deferred events.

        Called once after every top-level registration batch — the initial
        graph build (``evaluate``/``build_and_validate``) and each dynamic
        expansion (``GraphExpanded``). Implements issue #280/#307's phase 2:

        1. Collect the literal produced (``Out[...]``) and consumed path
           values for the nodes registered in this batch.
        2. Reject two nodes declaring the same, or an overlapping, ``Out``
           path before any work starts.
        3. Match new consumers against every producer known so far (this
           batch and every earlier one) and add the producer as a dependency.
        4. Match new producers against consumers from *earlier* batches —
           the only way a dynamically registered node can retroactively
           matter. A still-pending old consumer gets the edge added; one
           already dispatched or completed can no longer be made to wait, so
           this raises rather than let the race happen silently. The
           expanding task and its own dynamic ancestors are exempt: a task
           that receives a folder and returns children writing inside it
           is the normal fan-out shape, and it already waits on those
           children through its dynamic template.
        5. Detect any cycle the new edges created.
        6. Emit each new node's ``GraphNodeRegistered`` event, now carrying
           its final (declared + inferred) dependency ids.

        A retroactively added edge on an *already emitted* node is applied
        to its live :class:`TaskNode` (so the scheduler honours it) but does
        not re-emit that node's registration event — by the time a node can
        acquire a retroactive dependency, it has not yet been dispatched, so
        its later ``TaskPlanned`` event (emitted right before dispatch, from
        the live ``dependency_ids``) already carries the edge into the
        ledger.
        """
        new_node_ids = self._pending_node_ids
        self._pending_node_ids = []
        if not new_node_ids:
            return

        expanding_lineage: set[int] = set()
        if expanding_node_id is not None:
            for node_id in new_node_ids:
                self._dynamic_parent_ids[node_id] = expanding_node_id
            ancestor: int | None = expanding_node_id
            while ancestor is not None:
                expanding_lineage.add(ancestor)
                ancestor = self._dynamic_parent_ids.get(ancestor)

        new_produced: list[ProducedPath] = []
        new_consumed: list[ConsumedPath] = []
        for node_id in new_node_ids:
            node = self._nodes[node_id]
            new_produced.extend(
                collect_produced_paths(
                    node_id=node_id,
                    task_name=node.task_def.name,
                    task_def=node.task_def,
                    args=node.expr.args,
                )
            )
            new_consumed.extend(
                collect_consumed_paths(
                    node_id=node_id,
                    task_name=node.task_def.name,
                    task_def=node.task_def,
                    args=node.expr.args,
                )
            )

        index = self._path_index
        index.add_produced(new_produced)
        self._edges_inferred_this_batch = False

        # New consumers against every producer indexed so far (new included).
        for consumer in new_consumed:
            producer_ids = {entry.node_id for entry in index.producers_for(consumer)}
            if producer_ids:
                self._add_inferred_dependencies(
                    node_id=consumer.node_id, producer_ids=producer_ids
                )

        # New producers against consumers registered in earlier batches — the
        # dynamic-expansion case, where the reader may already be in flight.
        for producer in new_produced:
            for consumer in index.consumers_for(producer):
                if consumer.node_id in expanding_lineage:
                    continue
                consumer_run = self._nodes[consumer.node_id]
                producer_run = self._nodes[producer.node_id]
                if consumer_run.state != "pending":
                    raise RuntimeError(
                        f"{producer_run.task_def.name!r} was just registered declaring "
                        f"`Out[...]` path {producer.path!r}, but {consumer_run.task_def.name!r} "
                        f"already read that path (state={consumer_run.state!r}) before the "
                        "edge could be inferred. Pass the path through the task graph "
                        "(the producer's return value or an `.output[...]` reference) "
                        "instead of a literal string so the dependency is explicit."
                    )
                self._add_inferred_dependencies(
                    node_id=consumer.node_id, producer_ids={producer.node_id}
                )

        index.add_consumed(new_consumed)

        # Registration already rejects cycles in the expression graph, so only
        # an inferred edge can close one; skip the walk when none was added.
        if self._edges_inferred_this_batch:
            cycle = self._find_dependency_cycle()
            if cycle is not None:
                raise CycleError([self._nodes[nid].task_def.name for nid in cycle])

        for node_id in new_node_ids:
            node = self._nodes[node_id]
            stdout_log, stderr_log = self._pending_registration_logs.pop(node_id, (None, None))
            self._emit_event(
                GraphNodeRegistered(
                    run_id=self._run_id,
                    task_id=task_id_for_node(node_id),
                    node_id=node_id,
                    task_name=node.task_def.name,
                    kind=node.task_def.kind,
                    execution_mode=node.task_def.execution_mode,
                    env=node.task_def.env,
                    retries=node.task_def.retries,
                    dependency_ids=[
                        task_id_for_node(dep_id) for dep_id in sorted(node.dependency_ids)
                    ],
                    inferred_dependency_ids=[
                        task_id_for_node(dep_id)
                        for dep_id in sorted(node.node.inferred_dependency_ids)
                    ],
                    stdout_log=stdout_log,
                    stderr_log=stderr_log,
                )
            )

    def _add_inferred_dependencies(self, *, node_id: int, producer_ids: set[int]) -> None:
        """Merge inferred producer ids into a node's dependency ids in place."""
        node = self._nodes[node_id].node
        producer_ids = producer_ids - {node_id}
        if not producer_ids:
            return
        object.__setattr__(node, "dependency_ids", node.dependency_ids | producer_ids)
        object.__setattr__(
            node, "inferred_dependency_ids", node.inferred_dependency_ids | producer_ids
        )
        self._edges_inferred_this_batch = True

    def _find_dependency_cycle(self) -> list[int] | None:
        """Return a cycle among the registered nodes' ``dependency_ids``, if any.

        Iterative depth-first search, so a long linear chain of tasks cannot
        exhaust the interpreter's recursion limit.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[int, int] = dict.fromkeys(self._nodes, WHITE)
        for root in sorted(self._nodes):
            if color[root] != WHITE:
                continue
            path: list[int] = [root]
            stack = [iter(sorted(self._nodes[root].dependency_ids))]
            color[root] = GRAY
            while stack:
                dep_id = next(stack[-1], None)
                if dep_id is None:
                    color[path.pop()] = BLACK
                    stack.pop()
                    continue
                state = color.get(dep_id, BLACK)
                if state == GRAY:
                    return [*path[path.index(dep_id) :], dep_id]
                if state == WHITE:
                    color[dep_id] = GRAY
                    path.append(dep_id)
                    stack.append(iter(sorted(self._nodes[dep_id].dependency_ids)))
        return None

    def _prepare_pending_nodes(self) -> None:
        """Resolve cache-ready nodes whose dependencies have completed.

        A node whose preparation raises fails like any other task attempt,
        except that it is never retried: nothing it raised came from running
        its body, so another attempt would only fail the same way.
        """
        while True:
            progressed = False
            for node in self._nodes.values():
                if node.state != "pending":
                    continue
                if not self._dependencies_complete(node.dependency_ids):
                    continue

                try:
                    self._prepare_node(node)
                except Exception as exc:
                    self._handle_task_exception(node=node, exc=exc, retryable=False)
                progressed = True
                # Fail-fast: once the run is stopping, nothing more is prepared.
                if self._failure is not None:
                    return

            if not progressed:
                return

    def _prepare_node(self, node: NodeRun) -> None:
        """Resolve non-ephemeral inputs, then either cache-hit or ready the task."""
        prepare_started = time.perf_counter()
        resolved_args = self._resolve_task_args(
            expr=node.expr,
            task_def=node.task_def,
            include_tmp_dirs=False,
            stage_remote_refs=False,
            asset_inputs=node.asset_inputs,
        )
        self._warn_on_untracked_path_inputs(node=node, resolved_args=resolved_args)
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=resolved_args)
        self._validator.validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )

        # For notebook/script tasks, eagerly evaluate the body to capture the
        # source hash of the underlying file and fold it into the cache key.
        # The body runs before the node's cache identity is known, so it is
        # handed the same rehydrated view of assets a cache miss would give
        # it — ``resolved_args`` itself keeps the ``AssetRef`` the cache key
        # is built from.
        extra_source_hash: str | None = None
        if node.task_def.kind in {"notebook", "script"}:
            directive = node.task_def.fn(
                **self._rehydrate_execution_args(
                    task_def=node.task_def,
                    resolved_args=resolved_args,
                )
            )
            node.driver_directive = directive
            extra_source_hash = directive.source_hash

        node.resolved_args = resolved_args
        node.extra_source_hash = extra_source_hash
        node.display_label = self._display_label_for(node=node)
        self._record_task_timing(
            node_id=node.node_id,
            phase="prepare_seconds",
            started=prepare_started,
        )
        if self._try_prepare_cache_hit(node=node):
            return

        # Materialize Pixi environments only after a cache miss is confirmed.
        self._prepare_task_environment(node=node)
        self._record_task_metadata(node=node)

        resources = self.effective_resources(task_def=node.task_def)
        node.threads = resources.threads
        node.memory_gb = resources.memory_gb
        # Kept across escalation: node.memory_gb becomes the retry's budget,
        # so without this the declaration a user would edit is lost.
        node.declared_memory_gb = resources.memory_gb
        node.gpu = resources.gpu
        node.custom_resources = dict(resources.custom)
        node.executor_name = self._resolve_placement(task_def=node.task_def)
        self._apply_memory_escalation(node=node, resources=resources)
        # Custom budgets are run-level, so the demand check applies wherever
        # the task is placed.
        for dimension, demand in node.custom_resources.items():
            budget = (self.resource_budgets or {}).get(dimension)
            if budget is not None and demand > budget:
                raise ValueError(
                    f"{node.task_def.name} requires {demand} {dimension} but only "
                    f"{budget} are available in the run's {dimension} budget"
                )
        if not node.remote:
            if node.threads > self.cores:
                raise ValueError(
                    f"{node.task_def.name} requires {node.threads} cores but only "
                    f"{self.cores} are available"
                )
            if self.memory is not None and node.memory_gb > self.memory:
                raise ValueError(
                    f"{node.task_def.name} requires {node.memory_gb} GiB but only "
                    f"{self.memory} GiB are available"
                )
        node.state = "ready"
        self._emit_event(
            TaskReady(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                resources=self._resources_payload(node=node),
            )
        )

    def _prepare_task_environment(self, *, node: NodeRun) -> None:
        """Materialize any external execution environment required by a task."""
        if node.task_def.env is None or self.backend is None:
            return

        self._emit_event(
            EnvPrepareStarted(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                env=node.task_def.env,
            )
        )
        env_prepare_started = time.perf_counter()
        try:
            self.backend.prepare(env=node.task_def.env)
        except BaseException as exc:
            # The task never starts, so nothing else would close out the
            # preparation window for observers of the event stream.
            self._record_task_timing(
                node_id=node.node_id,
                phase="env_prepare_seconds",
                started=env_prepare_started,
            )
            self._emit_event(
                EnvPrepareFailed(
                    run_id=self._run_id,
                    task_id=task_id_for_node(node.node_id),
                    task_name=node.task_def.name,
                    attempt=node.attempt,
                    env=node.task_def.env,
                    error=str(exc),
                )
            )
            raise
        self._record_task_timing(
            node_id=node.node_id,
            phase="env_prepare_seconds",
            started=env_prepare_started,
        )
        self._emit_event(
            EnvPrepareCompleted(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                env=node.task_def.env,
            )
        )

    def _dispatch_ready_nodes(
        self,
        *,
        python_executor: ProcessPoolExecutor,
        shell_executor: ThreadPoolExecutor,
    ) -> None:
        """Submit a resource-feasible subset of ready nodes."""
        ready_nodes = [node for node in self._nodes.values() if node.state == "ready"]
        if not ready_nodes:
            return

        available_jobs = self.jobs - len(self._running_futures)
        available_cores = self.cores - self._running_cores()
        available_memory = None if self.memory is None else self.memory - self._running_memory_gb()
        available_gpus = (self.gpus or 0) - self._running_gpus()
        available_group_slots = self._available_group_slots(ready_nodes=ready_nodes)
        available_custom = self._available_custom_budgets()
        # Remote-placed tasks consume a jobs slot but no local resource
        # budget — their threads/memory/gpu are satisfied by the executor.
        # Custom demands stay: those budgets are run-level.
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(
                    node_id=node.node_id,
                    threads=0 if node.remote else node.threads,
                    memory_gb=0 if node.remote else node.memory_gb,
                    gpu=0 if node.remote else node.gpu,
                    priority=node.task_def.priority,
                    concurrency_group=node.concurrency_group,
                    custom=node.custom_resources,
                )
                for node in ready_nodes
            ],
            jobs=available_jobs,
            cores=available_cores,
            memory=available_memory,
            gpus=available_gpus,
            available_group_slots=available_group_slots,
            custom_budgets=available_custom,
        )

        for node_id in selected:
            node = self._nodes[node_id]
            node.attempt += 1
            node.resolved_args = self._resolve_task_args(
                expr=node.expr,
                task_def=node.task_def,
                include_tmp_dirs=True,
                existing_args=node.resolved_args,
                tmp_paths=node.tmp_paths,
                stage_remote_refs=False,
            )
            remote_input_count = count_remote_inputs(node.resolved_args)
            if remote_input_count > 0:
                node.state = "staging"
                access_method = _classify_access_method(value=node.resolved_args)
                self._emit_event(
                    TaskStaging(
                        run_id=self._run_id,
                        task_id=task_id_for_node(node.node_id),
                        task_name=node.task_def.name,
                        attempt=node.attempt,
                        display_label=node.display_label,
                        remote_input_count=remote_input_count,
                        access_method=access_method,
                    )
                )
                assert self._executors is not None
                future = self._executors.staging.submit(self._stager.stage_task_inputs, node=node)
                self._running_futures[future] = (node_id, "staging")
                continue

            self._start_task_execution(
                node=node,
                python_executor=python_executor,
                shell_executor=shell_executor,
            )

    def _consume_completed_futures(self, done_futures: set[Future[Any]]) -> None:
        """Handle finished worker futures from the thread pool."""
        for future in done_futures:
            node_id, phase = self._running_futures.pop(future)
            node = self._nodes[node_id]

            if future.cancelled():
                self._remote_dispatch.pop_handle(node.node_id)
                # Only a stopping run cancels work.
                assert self._failure is not None
                self._close_unfinished_node(node=node, reason=self._failure)
                continue

            # Capture remote job id for provenance before processing result.
            if phase == "remote":
                handle = self._remote_dispatch.handle_for(node.node_id)
                if handle is not None:
                    node.remote_job_id = handle.job_id

            try:
                completed_value = future.result()
            except BaseException as exc:
                self._remote_dispatch.pop_handle(node.node_id)
                self._handle_task_exception(node=node, exc=exc)
                continue

            try:
                if phase == "staging":
                    self._handle_completed_staging_phase(
                        node=node, completed_value=completed_value
                    )
                elif phase in ("python", "remote"):
                    remote_handle = self._remote_dispatch.pop_handle(node.node_id)
                    if phase == "remote" and remote_handle is not None:
                        self._remote_dispatch.capture_logs(node=node, handle=remote_handle)
                    self._handle_completed_worker_phase(
                        node=node,
                        completed_value=completed_value,
                        remote_job_id=remote_handle.job_id if remote_handle is not None else None,
                    )
                elif phase == "driver":
                    self._handle_completed_driver_phase(node=node, completed_value=completed_value)
                else:
                    self._handle_completed_shell_phase(node=node, completed_value=completed_value)
            except BaseException as exc:
                self._handle_task_exception(node=node, exc=exc)

        if self._failure is None:
            self._finalize_dynamic_nodes()

    def _finalize_dynamic_nodes(self) -> None:
        """Complete nodes whose dynamic child expressions have finished."""
        while True:
            progressed = False
            for node in self._nodes.values():
                if node.state != "waiting_dynamic":
                    continue
                if not self._dependencies_complete(node.dynamic_dependency_ids):
                    continue

                value = self._materialize(node.dynamic_template)
                final_value = self._finalize_result_value(node=node, value=value)
                self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)
                progressed = True

            if not progressed:
                return

    def _handle_completed_worker_phase(
        self,
        *,
        node: NodeRun,
        completed_value: Any,
        remote_job_id: str | None = None,
    ) -> None:
        """Handle the result returned from a Python worker."""
        if isinstance(completed_value, dict) and isinstance(
            completed_value.get("measured_resources"), dict
        ):
            self._record_measured_usage(node=node, measured=completed_value["measured_resources"])
        self._fold_remote_input_access(node=node, payload=completed_value)
        completed_value = self._decode_worker_result(node=node, payload=completed_value)
        node.remote_job_id = remote_job_id
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_staging_phase(self, *, node: NodeRun, completed_value: Any) -> None:
        """Start task execution after remote inputs have been staged locally."""
        if not isinstance(completed_value, dict):
            raise TypeError("Expected staged task arguments from staging phase")

        node.resolved_args = completed_value
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=node.resolved_args)
        assert self._executors is not None
        self._start_task_execution(
            node=node,
            python_executor=self._executors.python,
            shell_executor=self._executors.shell,
        )

    def _handle_completed_driver_phase(self, *, node: NodeRun, completed_value: Any) -> None:
        """Handle the result returned from a driver-executed task wrapper."""
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_shell_phase(self, *, node: NodeRun, completed_value: Any) -> None:
        """Handle the result produced by the shell executor."""
        final_value = self._finalize_result_value(node=node, value=completed_value)
        self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)

    def _handle_task_exception(
        self,
        *,
        node: NodeRun,
        exc: BaseException,
        retryable: bool = True,
    ) -> None:
        """Either retry a failed task attempt or fail the run.

        ``retryable=False`` records the failure without consulting the
        task's retry policy.
        """
        sanitized_exc = sanitize_exception(exc=exc, secret_values=node.secret_values)
        if (
            retryable
            and self._failure is None
            and self._should_retry(node=node, exc=sanitized_exc)
        ):
            self._schedule_retry(node=node, exc=sanitized_exc)
            return

        # "ignore" is about scheduling only: the task is recorded as failed
        # either way, and the run still ends up failed. Once the run is
        # stopping — a fatal failure, or an interrupt — nothing is ignored any
        # more: the policy could not carry a run on that has already ended, so
        # every failure draining out behind it is reported as what it is.
        ignore = self._failure is None and (
            self.keep_going or node.task_def.on_failure == "ignore"
        )
        node.state = "failed"
        self._cleanup_transport(node)
        if ignore:
            self._ignored_failures.append((node, sanitized_exc))
        elif self._failure is None:
            self._failure = sanitized_exc
            self._cancel_pending_futures()
        # Usage measured before the failure helps right-size OOM-prone tasks.
        usage = self._resource_usage_for(node=node)
        if usage:
            self._annotate_task(node=node, fields={"resource_usage": usage})
        child_run_id = getattr(sanitized_exc, "child_run_id", None)
        if child_run_id is not None:
            self._annotate_task(node=node, fields={"sub_run_id": child_run_id})
        self._emit_event(
            TaskFailed(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                exit_code=getattr(sanitized_exc, "exit_code", None),
                failure=classify_failure(exc=sanitized_exc),
                remote_job_id=node.remote_job_id,
                ignored=ignore,
            )
        )

    def _close_unfinished_nodes(self, *, reason: BaseException) -> None:
        """Close every started task the stopping run leaves without an outcome.

        Work still in flight, and a task waiting on its own expansion, has
        been recorded as running; nothing else would ever end that record.
        """
        for node in self._nodes.values():
            if node.state in _IN_FLIGHT_NODE_STATES or node.state == "waiting_dynamic":
                self._close_unfinished_node(node=node, reason=reason)

    def _close_unfinished_node(self, *, node: NodeRun, reason: BaseException) -> None:
        """Record one started task as cancelled by the run stopping around it."""
        node.state = "failed"
        self._cleanup_transport(node)
        self._emit_event(
            TaskFailed(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                failure={
                    "kind": "cancelled",
                    "message": f"The run stopped before this task finished: {reason}",
                    "retryable": False,
                    "code": "cancelled",
                },
                remote_job_id=node.remote_job_id,
            )
        )

    def _should_retry(self, *, node: NodeRun, exc: BaseException) -> bool:
        """Return whether the current failed attempt should be retried."""
        if node.attempt > node.task_def.retries:
            return False
        return node.task_def.should_retry_exception(exc=exc)

    def _schedule_retry(self, *, node: NodeRun, exc: BaseException) -> None:
        """Reset node state so the scheduler can rerun the task from scratch."""
        self._cleanup_transport(node)

        # Remove any attempt-local scratch directories before rerunning.
        for path in node.tmp_paths:
            if path.exists():
                shutil.rmtree(path)

        # Attempt is incremented on dispatch, so the next attempt is node.attempt + 1.
        delay = node.task_def.retry_delay_seconds(attempt=node.attempt)
        if delay > 0:
            node.state = "waiting_retry"
            node.retry_ready_at = time.monotonic() + delay
        else:
            node.state = "pending"
            node.retry_ready_at = None

        node.resolved_args = None
        node.execution_args = None
        node.cache_key = None
        node.input_hashes = None
        node.input_labels = None
        node.content_input_digests = None
        node.threads = 1
        node.memory_gb = 0
        node.gpu = 0
        node.custom_resources = {}
        node.executor_name = None
        node.tmp_paths = []
        node.transport_path = None
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        node.secret_values = ()
        node.extra_source_hash = None
        node.asset_versions = []
        node.asset_inputs = {}
        # measured_resources is deliberately NOT reset: a retried attempt's
        # peak (an OOM kill under memory_retry_multiplier, say) is exactly
        # the number needed to right-size the task, so measurements span
        # attempts — peaks take the max, CPU seconds accumulate.

        retries_remaining = node.task_def.retries - node.attempt
        self._emit_event(
            TaskRetrying(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                retries_remaining=retries_remaining,
                failure=classify_failure(exc=exc),
                delay_seconds=delay,
            )
        )

    def _complete_node(self, *, node: NodeRun, value: Any, tmp_paths: list[Path]) -> None:
        """Persist and mark a task node as fully completed."""
        finalize_started = time.perf_counter()
        self._cleanup_transport(node)
        extra_meta: dict[str, Any] | None = None
        if node.notebook_extras is not None:
            extra_meta = {"notebook_extras": node.notebook_extras}
        self._warn_on_written_str_inputs(node=node)
        artifact_ids = self._cache_store.save(
            cache_key=node.cache_key,
            result=value,
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            input_hashes=node.input_hashes,
            extra_source_hash=node.extra_source_hash,
            extra_meta=extra_meta,
            run_id=self._run_id,
        )

        # Propagate output digests so downstream tasks can skip re-hashing.
        self._digests.record_artifacts(artifact_ids)

        # Record stat-index for future --trust-mtimes runs.
        self._node_cache.record_stat_index_entry(node=node, cache_key=node.cache_key)

        for path in tmp_paths:
            shutil.rmtree(path)

        node.result = value
        node.state = "completed"
        node.tmp_paths = []
        node.transport_path = None
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        node.execution_args = None
        node.secret_values = ()
        if isinstance(value, SubWorkflowResult):
            self._annotate_task(node=node, fields={"sub_run_id": value.run_id})
        self._emit_event(
            TaskCompleted(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                status="success",
                cache_key=node.cache_key,
                outputs=self._output_summary_for(node=node, value=value),
                assets=self._asset_index_for(value=value),
                resource_usage=self._resource_usage_for(node=node),
                remote_job_id=node.remote_job_id,
            )
        )
        self._record_task_timing(
            node_id=node.node_id,
            phase="finalize_seconds",
            started=finalize_started,
        )

    def _resolve_task_args(
        self,
        *,
        expr: Expr,
        task_def: TaskDef,
        include_tmp_dirs: bool,
        stage_remote_refs: bool = True,
        existing_args: dict[str, Any] | None = None,
        tmp_paths: list[Path] | None = None,
        asset_inputs: dict[str, list[dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        """Resolve concrete arguments for a task call.

        *asset_inputs* is filled in as arguments resolve, with the identity of
        every asset each parameter was handed. The resolved value keeps that
        same ``AssetRef`` identity rather than the live payload it names —
        see :meth:`_resolve_execution_args`, which rehydrates refs into the
        payload a task body actually receives.
        """
        resolved_args: dict[str, Any] = {} if existing_args is None else dict(existing_args)
        tmp_paths = [] if tmp_paths is None else tmp_paths

        for name, parameter in task_def.signature.parameters.items():
            if name in resolved_args:
                continue

            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                if not include_tmp_dirs:
                    continue
                scratch = Path(tempfile.mkdtemp(prefix=f"ginkgo-{task_def.fn.__name__}-{name}-"))
                tmp_paths.append(scratch)
                resolved_args[name] = tmp_dir(str(scratch))
                continue

            if name in expr.args:
                materialised = self._materialize(expr.args[name])
                if asset_inputs is not None:
                    refs = collect_asset_refs(materialised)
                    if refs:
                        # Every ref, not just the first: a fan-in consumer
                        # binds N assets to one parameter and each is a
                        # lineage parent — see TaskPlanned.asset_inputs.
                        asset_inputs[name] = [
                            {
                                "asset_key": str(ref.key),
                                "version_id": ref.version_id,
                                "artifact_id": ref.artifact_id,
                            }
                            for ref in refs
                        ]
                # ``resolved_args`` keeps every ``AssetRef`` as-is, whatever
                # the annotation: it is what the cache key is computed from
                # (see ``node_cache.content_lookup``), and an ``AssetRef``
                # hashes by its stable identity (asset key + version id)
                # rather than by its live payload's pickled bytes, which can
                # be non-deterministic (e.g. an sklearn model). Live objects
                # are materialised later, only on a cache miss, in
                # ``_resolve_execution_args``.
                resolved_args[name] = materialised
                continue

            if name == "threads":
                # Inject the effective thread count (declaration plus any site
                # override) so user code can use it for shell command
                # interpolation or in-process work.
                resolved_args[name] = self.effective_resources(task_def=task_def).threads
                continue

            if parameter.default is not parameter.empty:
                resolved_args[name] = parameter.default
                continue

            raise TypeError(f"{task_def.fn.__name__}() missing required argument: '{name}'")

        if stage_remote_refs:
            resolved_args = self._stager.stage_remote_refs(
                task_def=task_def,
                resolved_args=resolved_args,
            )

        return resolved_args

    def _resolve_execution_args(self, *, node: NodeRun) -> dict[str, Any]:
        """Resolve runtime-only inputs: rehydrated assets and secret references.

        Called only once a cache miss is confirmed (see ``_start_task_execution``),
        so a cache hit never pays to rehydrate a live asset payload.
        """
        assert node.resolved_args is not None
        rehydrated = self._rehydrate_execution_args(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
        )
        if self.secret_resolver is None:
            return rehydrated
        return {
            name: resolve_secret_refs(value=value, resolver=self.secret_resolver)
            for name, value in rehydrated.items()
        }

    def _rehydrate_execution_args(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Replace wrapped ``AssetRef`` values with live payloads for execution.

        Mirrors the path-shaped check in ``_resolve_task_args``: a
        path-shaped annotation binds a filesystem path, so its ``AssetRef``
        entries are converted to the ``file``/``folder`` value the path
        implies (see ``_convert_path_shaped_refs``) rather than rehydrated
        into a live object; every other parameter is rehydrated into the
        live object the task body asked for.
        """
        rehydrated: dict[str, Any] = {}
        for name, value in resolved_args.items():
            parameter = task_def.signature.parameters.get(name)
            annotation = task_def.type_hints.get(
                name,
                parameter.annotation if parameter is not None else Any,
            )
            rehydrated[name] = (
                self._convert_path_shaped_refs(annotation=annotation, value=value)
                if is_path_shaped_annotation(annotation)
                else self._rehydrate_wrapped_refs(value=value)
            )
        return rehydrated

    def _convert_path_shaped_refs(self, *, annotation: Any, value: Any) -> Any:
        """Convert ``AssetRef`` values bound to a ``file``/``folder`` annotation.

        Walks the same container shapes ``validate_annotated_value`` walks —
        ``| None``, then ``list[...]``/``tuple[...]`` element-wise via
        ``pair_elements_with_annotations`` — so every ``AssetRef`` that passed
        validation there reaches :meth:`AssetRef.as_execution_value` here with
        the same per-element annotation. A value that is not an ``AssetRef``
        (a literal path, ``None``, a ``file``/``folder`` marker already) is
        returned unchanged.
        """
        annotation, _ = unwrap_optional_annotation(annotation)
        if value is None:
            return None

        if isinstance(value, AssetRef):
            return value.as_execution_value(annotation=annotation)

        if isinstance(value, (list, tuple)):
            paired = pair_elements_with_annotations(annotation=annotation, value=value)
            converted = [
                self._convert_path_shaped_refs(annotation=item_annotation, value=item)
                for item_annotation, item in paired
            ]
            return tuple(converted) if isinstance(value, tuple) else converted

        return value

    def _materialize(self, value: Any) -> Any:
        """Materialize a nested value using completed task-node results."""
        if isinstance(value, OutputIndex):
            result = self._materialize(value.expr)
            return result[value.index]

        if isinstance(value, OutputName):
            node = self._nodes[self._expr_nodes[id(value.expr)]]
            if node.state != "completed":
                raise RuntimeError(f"Task {node.task_def.name} is not yet complete")
            return self._resolve_output_param_value(node=node, name=value.name)

        if isinstance(value, Expr):
            node = self._nodes[self._expr_nodes[id(value)]]
            if node.state != "completed":
                raise RuntimeError(f"Task {node.task_def.name} is not yet complete")
            return node.result

        if isinstance(value, ExprList):
            return [self._materialize(item) for item in value]

        if isinstance(value, list):
            return [self._materialize(item) for item in value]

        if isinstance(value, tuple):
            return tuple(self._materialize(item) for item in value)

        if isinstance(value, dict):
            return {self._materialize(key): self._materialize(item) for key, item in value.items()}

        return value

    def _rehydrate_wrapped_refs(self, *, value: Any) -> Any:
        """Replace wrapped ``AssetRef`` values with live Python payloads.

        Recurses into lists, tuples, and dicts. ``AssetRef`` entries with a
        wrapper kind (``table`` / ``array`` / ``text`` / ``model``) are
        rehydrated either from the in-process live-payload cache
        (zero-copy handoff) or from the on-disk loader as a fallback.
        ``file`` and ``fig`` refs are left as-is: the former flow through
        the existing file coercion path, and the latter carry binary
        payloads that users rarely consume as live Python objects.

        Callers decide whether to rehydrate at all:
        ``_rehydrate_execution_args`` skips this entirely for a path-shaped
        annotation, which binds a filesystem path rather than a live object.

        Parameters
        ----------
        value : Any
            The materialised argument value, possibly nesting ``AssetRef``.
        """
        if isinstance(value, AssetRef):
            if value.kind in REHYDRATABLE_KINDS:
                cached = self._live_payloads.get(artifact_id=value.artifact_id)
                if cached is not None:
                    return cached
                return load_wrapped_ref(
                    artifact_store=self._cache_store._artifact_store,
                    asset_ref=value,
                )
            return value
        if isinstance(value, list):
            return [self._rehydrate_wrapped_refs(value=item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._rehydrate_wrapped_refs(value=item) for item in value)
        if isinstance(value, dict):
            return {key: self._rehydrate_wrapped_refs(value=item) for key, item in value.items()}
        return value

    def _dependencies_complete(self, dependency_ids: AbstractSet[int]) -> bool:
        """Return whether all referenced nodes have completed."""
        return all(self._nodes[node_id].state == "completed" for node_id in dependency_ids)

    def _is_root_resolved(self) -> bool:
        """Return whether all root dependencies have completed."""
        return self._dependencies_complete(self._root_dependency_ids)

    def _can_make_scheduler_progress(self) -> bool:
        """Return whether another scheduler pass could unblock more work."""
        for node in self._nodes.values():
            if node.state == "ready":
                return True
            if node.state == "pending" and self._dependencies_complete(node.dependency_ids):
                return True
            if node.state == "waiting_dynamic" and self._dependencies_complete(
                node.dynamic_dependency_ids
            ):
                return True
            if node.state == "waiting_retry":
                return True
        return False

    def _is_drained(self) -> bool:
        """Return whether every node has reached a terminal state."""
        if self._running_futures:
            return False
        return all(node.state in _TERMINAL_NODE_STATES for node in self._nodes.values())

    def _skip_blocked_nodes(self) -> None:
        """Mark every node an ignored failure has made unrunnable as skipped.

        A node is unrunnable once any task it waits on has failed or been
        skipped: phase 1 has no partial fan-in, so one missing input skips
        the task outright. Iterated to a fixed point, so a chain of
        dependents collapses within a single scheduler pass rather than one
        level per pass. Nodes whose work is already in flight are left alone;
        their own completion decides what happens to them.

        An ignored failure is the only thing that can block a node while the
        scheduler is still dispatching — fail-fast stops it instead — so a run
        without one has nothing to sweep.
        """
        if not self._ignored_failures:
            return

        while True:
            progressed = False
            for node in self._nodes.values():
                if node.state in _TERMINAL_NODE_STATES or node.state in _IN_FLIGHT_NODE_STATES:
                    continue
                blocker = self._blocking_dependency(node=node)
                if blocker is None:
                    continue
                self._mark_node_skipped(node=node, blocker=blocker)
                progressed = True

            if not progressed:
                return

    def _blocking_dependency(self, *, node: NodeRun) -> NodeRun | None:
        """Return a dependency of *node* that has failed or been skipped."""
        for node_id in sorted(node.dependency_ids | node.dynamic_dependency_ids):
            dependency = self._nodes[node_id]
            if dependency.state in {"failed", "skipped"}:
                return dependency
        return None

    def _mark_node_skipped(self, *, node: NodeRun, blocker: NodeRun) -> None:
        """Record one node as skipped, attributed to the failure behind it.

        Nothing about the attempt the node may already have made is cleared:
        a node waiting on its own dynamic expansion has run its body, and the
        record of that work is true whatever the outcome.
        """
        if blocker.state == "skipped":
            blocked_by_id = blocker.blocked_by_task_id
            blocked_by_name = blocker.blocked_by_task_name
        else:
            blocked_by_id = task_id_for_node(blocker.node_id)
            blocked_by_name = blocker.task_def.name

        node.state = "skipped"
        node.blocked_by_task_id = blocked_by_id
        node.blocked_by_task_name = blocked_by_name
        self._emit_event(
            TaskSkipped(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                blocked_by_task_id=blocked_by_id or "",
                blocked_by_task_name=blocked_by_name or "",
            )
        )

    def _root_skipped_error(self) -> RootSkippedError | None:
        """Return the error naming why the run produced no result, if it is that.

        ``None`` when no root dependency is failed or skipped, which means
        the graph stalled for some other reason and the caller should say so.
        """
        for node_id in sorted(self._root_dependency_ids):
            node = self._nodes[node_id]
            if node.state == "skipped":
                return RootSkippedError(
                    task_name=node.blocked_by_task_name or node.task_def.name,
                    task_id=node.blocked_by_task_id or task_id_for_node(node.node_id),
                )
            if node.state == "failed":
                return RootSkippedError(
                    task_name=node.task_def.name,
                    task_id=task_id_for_node(node.node_id),
                )
        return None

    @property
    def ignored_failures(self) -> tuple[tuple[NodeRun, BaseException], ...]:
        """Failures the run's policy let pass, in the order they happened."""
        return tuple(self._ignored_failures)

    def _promote_due_retries(self) -> None:
        """Transition retry-delayed nodes back to pending once their deadline passes."""
        now = time.monotonic()
        for node in self._nodes.values():
            if node.state != "waiting_retry":
                continue
            if node.retry_ready_at is not None and node.retry_ready_at <= now:
                node.state = "pending"
                node.retry_ready_at = None

    def _earliest_retry_wait(self) -> float | None:
        """Return seconds until the next retry deadline, or ``None`` if none waiting."""
        deadlines = [
            node.retry_ready_at
            for node in self._nodes.values()
            if node.state == "waiting_retry" and node.retry_ready_at is not None
        ]
        if not deadlines:
            return None
        return max(0.0, min(deadlines) - time.monotonic())

    def _cancel_pending_futures(self) -> None:
        """Cancel queued futures that have not started yet."""
        for future in self._running_futures:
            future.cancel()

    def _interrupt_running_work(self) -> None:
        """Stop queued and active work after an external interrupt."""
        self._cancel_pending_futures()
        self._remote_dispatch.cancel_all()
        self._shell_runner.terminate_all()
        if self._executors is not None:
            self._executors.shutdown_all()

    def _running_cores(self) -> int:
        """Return the local core footprint of currently running tasks."""
        return sum(
            self._nodes[node_id].threads
            for node_id, _ in self._running_futures.values()
            if not self._nodes[node_id].remote
        )

    def _running_gpus(self) -> int:
        """Return the local GPU footprint of currently running tasks."""
        return sum(
            self._nodes[node_id].gpu
            for node_id, _ in self._running_futures.values()
            if not self._nodes[node_id].remote
        )

    def _available_group_slots(self, *, ready_nodes: list[NodeRun]) -> dict[str, int]:
        """Return the remaining concurrency budget per active group.

        For each named concurrency group represented in the ready set, the
        result contains the group's declared limit minus the number of tasks
        from that group currently in flight.
        """
        active_groups: dict[str, int] = {}
        for node in ready_nodes:
            if node.concurrency_group is None or node.concurrency_group_limit is None:
                continue
            active_groups[node.concurrency_group] = node.concurrency_group_limit

        if not active_groups:
            return {}

        running_per_group: dict[str, int] = {}
        for node_id, _ in self._running_futures.values():
            running_node = self._nodes[node_id]
            if running_node.concurrency_group is None:
                continue
            running_per_group[running_node.concurrency_group] = (
                running_per_group.get(running_node.concurrency_group, 0) + 1
            )

        return {
            group_id: max(0, limit - running_per_group.get(group_id, 0))
            for group_id, limit in active_groups.items()
        }

    def _resources_payload(self, *, node: NodeRun) -> dict[str, Any]:
        """Return the event-payload view of a node's resolved resources."""
        payload: dict[str, Any] = {
            "cores": node.threads,
            "memory_gb": node.memory_gb,
            "gpu": node.gpu,
        }
        if node.custom_resources:
            payload["custom"] = dict(node.custom_resources)
        return payload

    def _available_custom_budgets(self) -> dict[str, int] | None:
        """Return the remaining budget per user-defined resource dimension.

        Unlike the built-in dimensions, in-flight remote-placed tasks are
        counted too: custom budgets (API quotas, database connections) are
        run-level and apply wherever the task runs. Returns ``None`` when no
        budgets are configured.
        """
        if not self.resource_budgets:
            return None
        running: dict[str, int] = {}
        for node_id, _ in self._running_futures.values():
            for dimension, demand in self._nodes[node_id].custom_resources.items():
                running[dimension] = running.get(dimension, 0) + demand
        return {
            dimension: budget - running.get(dimension, 0)
            for dimension, budget in self.resource_budgets.items()
        }

    def _running_memory_gb(self) -> int:
        """Return the declared local memory footprint of currently running tasks."""
        return sum(
            self._nodes[node_id].memory_gb
            for node_id, _ in self._running_futures.values()
            if not self._nodes[node_id].remote
        )

    def _start_task_execution(
        self,
        *,
        node: NodeRun,
        python_executor: ProcessPoolExecutor | ThreadPoolExecutor,
        shell_executor: ThreadPoolExecutor,
    ) -> None:
        """Launch a task after its inputs have been staged locally."""
        assert node.resolved_args is not None

        # Fast path: in --trust-mtimes mode, try a stat-based index lookup
        # before computing content-addressed cache keys.
        if self.trust_mtimes and self._try_stat_index_hit(node=node):
            return

        if self._try_content_cache_hit(node=node):
            return

        self._emit_event(
            TaskCacheMiss(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                cache_key=node.cache_key,
            )
        )

        node.state = "running"
        node.execution_args = self._resolve_execution_args(node=node)
        node.secret_values = collect_resolved_secret_values(
            template=node.resolved_args,
            resolved=node.execution_args,
        )
        self._validator.validate_task_contract(
            task_def=node.task_def,
            execution_args=node.execution_args,
        )
        if node.task_def.output_params:
            self._create_output_parent_dirs(node=node)
        # Placement was resolved when the node was prepared; the backend
        # recorded in events and provenance is the executor's name.
        execution_backend = node.executor_name or LOCAL

        self._emit_event(
            TaskStarted(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                kind=node.task_def.kind,
                env=node.task_def.env,
                resources={
                    **self._resources_payload(node=node),
                    "max_attempts": node.task_def.retries + 1,
                },
                execution_backend=execution_backend,
            )
        )
        if node.task_def.kind in {"notebook", "script", "shell"}:
            future = shell_executor.submit(
                self._run_driver_task,
                node=node,
            )
            self._running_futures[future] = (node.node_id, "driver")
            return

        # Remote dispatch: the node was placed on an executor either
        # explicitly (executor= / remote=True) or because its GPU requirement
        # exceeds the local budget.
        if node.executor_name is not None:
            node.transport_path = Path(
                tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-")
            )
            assert self._executors is not None
            future = self._remote_dispatch.dispatch(
                node=node,
                executor_name=node.executor_name,
                payload=self._build_worker_payload(node=node),
                gpu_type=self.effective_resources(task_def=node.task_def).gpu_type,
                watcher=self._executors.get_or_create_remote_watcher(),
            )
            self._running_futures[future] = (node.node_id, "remote")
            return

        node.transport_path = Path(tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-"))
        payload = self._build_worker_payload(node=node)
        future = python_executor.submit(run_task, payload)
        self._running_futures[future] = (node.node_id, "python")

    def _create_output_parent_dirs(self, *, node: NodeRun) -> None:
        """Create the parent directory of every declared ``Out[...]`` path.

        Runs once per node, after pre-execution validation passes and before
        the task body dispatches — on the driver, ahead of python workers,
        shell/script/notebook commands, and container runs alike, since all
        of those are launched from this one call site. Only the *parent* is
        created (``exist_ok=True``): for ``Out[folder]`` the folder itself is
        left for the task body to create, since tools differ on whether they
        want it to already exist.
        """
        assert node.execution_args is not None
        for _, path, _kind in declared_output_paths(
            task_def=node.task_def, resolved_args=node.execution_args
        ):
            Path(path).parent.mkdir(parents=True, exist_ok=True)

    def _fold_remote_input_access(self, *, node: NodeRun, payload: Any) -> None:
        """Fold worker-reported input-access stats into provenance.

        Records FUSE mount cost, cache hits, and fallbacks for both remote
        and local (process-pool) workers, and surfaces a notice when a
        mount fell back to staging.
        """
        if isinstance(payload, dict) and isinstance(payload.get("remote_input_access"), dict):
            access_stats = payload["remote_input_access"]
            self._annotate_task(node=node, fields={"remote_input_access": access_stats})
            self._warn_on_access_fallback(node=node, access_stats=access_stats)

    def _warn_on_access_fallback(
        self,
        *,
        node: NodeRun,
        access_stats: dict[str, Any],
    ) -> None:
        """Surface a user-visible notice when fuse mounts fell back to staging.

        ``access_stats["fallback_reason"]`` is populated by
        :class:`~ginkgo.remote.access.mounted.MountedAccess` and the worker
        hydration layer when a requested fuse mount could not be
        established (missing driver, no ``/dev/fuse``, permission denied,
        etc.). Without this notice, users who declared
        ``access="fuse"`` would silently pay staging costs and never know
        their policy was downgraded.
        """
        reason = access_stats.get("fallback_reason")
        if not reason:
            return
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=f"FUSE access fell back to staging: {reason}",
            )
        )

    def _warn_on_written_str_inputs(self, *, node: NodeRun) -> None:
        """Warn when a task wrote a file it received as a plain ``str`` path.

        That path is content-hashed as an input, so the task invalidates its
        own cache entry and re-runs on every run until it is re-annotated.
        """
        written = self._node_cache.written_str_inputs(node=node)
        if not written:
            return
        paths = ", ".join(written)
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=(
                    f"wrote {paths}, which it received as a plain `str` path, so the "
                    "file is tracked as an input and this task will re-run every time. "
                    "Annotate that parameter `Out[file]` if the task writes it, or "
                    "`untracked` to key it by its path only."
                ),
            )
        )

    def effective_resources(self, *, task_def: TaskDef) -> Resources:
        """Return the task's declared resources with site overrides applied.

        Site overrides come from the ``[resources.overrides]`` runtime-config
        table and are merged over the decorator declaration. Memoized per
        task name — the inputs are static for the lifetime of a run.
        """
        name = task_def.name
        cached = self._effective_resources_cache.get(name)
        if cached is None:
            cached = task_def.resources
            if self.resource_overrides is not None:
                cached = self.resource_overrides.apply(task_name=name, base=cached)
            self._effective_resources_cache[name] = cached
        return cached

    def _apply_memory_escalation(self, *, node: NodeRun, resources: Resources) -> None:
        """Raise a retrying node's memory footprint per its retry multiplier.

        Locally-placed escalation is capped at the run's ``--memory`` budget
        so a retry always remains dispatchable; remote-placed tasks escalate
        uncapped because the executor satisfies their request. A change from
        the declared footprint is surfaced as a task notice.
        """
        if node.attempt == 0:
            return
        escalated = resources.memory_gb_for_attempt(node.attempt)
        if not node.remote and self.memory is not None and escalated > self.memory:
            escalated = self.memory
        if escalated == node.memory_gb:
            return
        node.memory_gb = escalated
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=f"memory escalated to {escalated} GiB for attempt {node.attempt + 1}",
            )
        )

    def _resolve_placement(self, *, task_def: TaskDef) -> str | None:
        """Return the executor a task is placed on, or ``None`` for local.

        Placement is derived from the declared requirement and the available
        capability. A task naming an ``executor`` routes there whatever the
        run default is; ``remote=True`` routes to the run's default executor
        (``--executor``); and a GPU requirement the local ``--gpus`` budget
        cannot satisfy falls back to that same default. Any route without a
        usable executor is an error rather than a silent local run.
        Placement depends only on the task definition, so it is validated
        for every node up front in ``build_and_validate``.
        """
        registry = self.executor_registry
        if task_def.executor is not None:
            self._require_remote_capable_kind(task_def=task_def, reason="executor")
            return registry.resolve(task_def.executor, task_name=task_def.name)
        if task_def.remote:
            self._require_remote_capable_kind(task_def=task_def, reason="remote")
            if not registry.has_default:
                raise ValueError(
                    f"{task_def.name} declares remote=True but this run has no default "
                    f"executor (pass --executor <name>). {registry.available_hint()}"
                )
            assert registry.default_name is not None
            return registry.default_name
        gpu = self.effective_resources(task_def=task_def).gpu
        if gpu > (self.gpus or 0):
            if task_def.kind != "python":
                raise ValueError(
                    f"{task_def.name} requires {gpu} GPU(s) but only {self.gpus} "
                    "are available locally (--gpus), and remote dispatch only "
                    f"supports python tasks, not kind={task_def.kind!r}"
                )
            self._require_remote_capable_kind(task_def=task_def, reason="a GPU requirement")
            if not registry.has_default:
                raise ValueError(
                    f"{task_def.name} requires {gpu} GPU(s) but only {self.gpus} "
                    "are available locally (--gpus) and this run has no default "
                    f"executor (--executor). {registry.available_hint()}"
                )
            return registry.default_name
        return None

    def _require_remote_capable_kind(self, *, task_def: TaskDef, reason: str) -> None:
        """Reject remote placement for task kinds the workers cannot run.

        ``Out[...]`` parameters are supported on remote tasks: when a remote
        artifact store is configured (``[remote.artifacts] store``), declared
        output paths are rewritten to worker-local scratch paths, staged back
        through the same channel returned files use, and restored at their
        declared driver path once the job completes — see
        ``RemoteDispatchManager.dispatch`` and
        ``ginkgo.runtime.artifacts.remote_arg_transfer``. Without a configured
        store, ``Out[...]`` paths are sent to the worker unchanged, which only
        works when the worker shares the driver's filesystem.
        """
        if task_def.kind != "python":
            declaration = (
                "remote=True" if reason == "remote" else f"executor={task_def.executor!r}"
            )
            raise ValueError(
                f"{task_def.name} declares {declaration} but remote dispatch "
                f"only supports python tasks, not kind={task_def.kind!r}"
            )

    def _build_worker_payload(self, *, node: NodeRun) -> dict[str, Any]:
        """Encode task inputs into a transport payload for the process pool."""
        assert node.transport_path is not None
        assert node.execution_args is not None
        return {
            "args": {
                name: encode_value(value, base_dir=node.transport_path)
                for name, value in node.execution_args.items()
            },
            "stdout_path": str(node.stdout_path) if node.stdout_path is not None else None,
            "stderr_path": str(node.stderr_path) if node.stderr_path is not None else None,
            "secret_values": list(node.secret_values),
            "run_id": self._run_id,
            "task_id": task_id_for_node(node.node_id),
            "task_name": node.task_def.name,
            "attempt": node.attempt,
            "display_label": node.display_label,
            "log_event_queue": self._log_drain.queue,
            "env": node.task_def.env,
            "module": node.task_def.fn.__module__,
            "module_file": resolve_module_file(node.task_def.fn.__module__),
            "task_kind": node.task_def.kind,
            "binding_name": node.task_def.fn.__name__,
            "transport_dir": str(node.transport_path),
            # Workers re-import the workflow module, which re-runs its param()
            # calls; without this they would resolve to the declared defaults.
            "param_context": (
                self.param_context.to_payload() if self.param_context is not None else None
            ),
        }

    def _decode_worker_result(self, *, node: NodeRun, payload: dict[str, Any]) -> Any:
        """Decode a process-pool worker response."""
        if not payload["ok"]:
            self._cleanup_transport(node)
            raise _reconstruct_worker_error(payload["error"])

        encoding = payload.get("result_encoding")

        if encoding == "direct":
            # Process-pool path: Python object passed directly (no serialization).
            return payload["result"]

        if encoding == "pixi_direct_pickled":
            # Pixi subprocess path: dynamic result (ExecutionDirective / Expr / ExprList)
            # was pickle+base64 encoded to cross the JSON bridge.
            import base64
            import pickle

            return pickle.loads(base64.b64decode(payload["result"]))

        assert node.transport_path is not None
        return decode_value(payload["result"], base_dir=node.transport_path)

    def _cleanup_transport(self, node: NodeRun) -> None:
        """Remove temporary transport artifacts for a task node."""
        if node.transport_path is None:
            return
        if node.transport_path.exists():
            shutil.rmtree(node.transport_path)
        node.transport_path = None

    def _finalize_result_value(self, *, node: NodeRun, value: Any) -> Any:
        """Coerce and validate a fully resolved task result.

        Called after the task body has actually run — for shell/script/
        notebook tasks, after the directive's command has executed, not when
        the body merely returned the directive — so this is also where each
        declared ``Out[...]`` parameter's path is checked to have been
        written, with the right kind.

        A task with an inferred return (no return annotation, ``Out[...]``
        parameters present — see ``TaskDef.has_inferred_return``) has *value*
        replaced here with the resolved value of its output parameter(s): a
        python task body must have returned ``None`` (anything else is a
        clear error), while a shell/script/notebook directive's own computed
        result is discarded in favour of the same output-parameter values, so
        both kinds go through this one substitution point.
        """
        task_def = node.task_def
        if task_def.has_inferred_return:
            if task_def.kind == "python" and value is not None:
                raise TypeError(
                    f"{task_def.name} has Out[...] parameters and no return annotation, "
                    "so its return value is inferred from those outputs — but the task "
                    f"body returned {value!r} instead of None. Add an explicit return "
                    "annotation (e.g. `-> file`) if this task needs to return something "
                    "else."
                )
            value = self._inferred_return_value(node=node)
        coerced = self._validator.coerce_return_value(task_def=task_def, value=value)
        finalized = self._asset_registrar.materialize_results(node=node, value=coerced)
        self._validator.validate_return_value(task_def=task_def, value=finalized)
        if task_def.output_params:
            self._validator.validate_declared_outputs_written(
                task_def=task_def,
                resolved_args=node.execution_args or {},
            )
        return finalized

    def _inferred_return_value(self, *, node: NodeRun) -> Any:
        """Build a task's inferred return from its ``Out[...]`` parameters.

        One parameter's resolved value stands alone; several are combined
        into a tuple, both in declaration order — matching
        ``TaskDef.effective_return_annotation``.
        """
        ordered = node.task_def.output_params_in_order
        values = [self._resolve_output_param_value(node=node, name=name) for name in ordered]
        return values[0] if len(values) == 1 else tuple(values)

    def _resolve_output_param_value(self, *, node: NodeRun, name: str) -> Any:
        """Return one ``Out[...]`` parameter's resolved value, as file/folder.

        Shared by inferred-return substitution and named ``.output[name]``
        access — both read the parameter's own resolved argument, coerced to
        its declared (inner) annotation, never the task's return value.
        """
        assert node.resolved_args is not None
        annotation = node.task_def.type_hints.get(name)
        return self._validator.coerce_annotated_value(
            annotation=annotation, value=node.resolved_args.get(name)
        )

    def _notebook_runtime_root(self) -> Path:
        """Return the shared runtime root for notebook support files."""
        if self.run_dir is not None:
            return self.run_dir.root.parent
        return WorkspaceLayout.for_cwd().root

    def _warn_on_untracked_path_inputs(
        self,
        *,
        node: NodeRun,
        resolved_args: dict[str, Any],
    ) -> None:
        """Warn when a directory crosses a task boundary without content tracking.

        Fires only for arguments resolved from an upstream expression in this
        graph: those are the ones where the producer can rewrite the
        directory's contents while the consumer's cache key, built from the
        path string alone, stays put. Narrowed to directories since #307
        phase 2: a same-shaped upstream *file* path is now content-hashed by
        default (``CacheStore._hash_value``'s root-input rule), so warning
        about it would describe something that no longer happens. Deduplicated
        per producer/consumer/parameter so fan-out branches report once. Runs
        before the cache-hit branch so the warning appears on the run that
        serves the stale result.
        """
        for name, unresolved in node.expr.args.items():
            self._scan_untracked_path_argument(
                node=node,
                parameter=name,
                annotation=node.task_def.type_hints.get(name),
                unresolved=unresolved,
                resolved=resolved_args.get(name),
            )

    def _scan_untracked_path_argument(
        self,
        *,
        node: NodeRun,
        parameter: str,
        annotation: Any,
        unresolved: Any,
        resolved: Any,
    ) -> None:
        """Warn for each upstream path one argument carries, at any depth.

        Containers are walked in step with their resolved counterparts, so a
        path arriving inside ``inputs=[a, b]`` — the ordinary fan-in shape — is
        checked exactly as one passed directly. The container annotation is
        carried down unchanged: ``annotation_includes`` already looks inside
        ``list[file]``, so the same predicate answers for the elements.
        """
        if isinstance(unresolved, ExprList) and isinstance(resolved, list | tuple):
            unresolved = list(unresolved)

        if isinstance(unresolved, list | tuple) and isinstance(resolved, list | tuple):
            for item, item_resolved in zip(unresolved, resolved):
                self._scan_untracked_path_argument(
                    node=node,
                    parameter=parameter,
                    annotation=annotation,
                    unresolved=item,
                    resolved=item_resolved,
                )
            return

        if isinstance(unresolved, dict) and isinstance(resolved, dict):
            for key, item in unresolved.items():
                # A key that is itself an expression resolves to a different
                # key, so its value cannot be paired up.
                if key not in resolved:
                    continue
                self._scan_untracked_path_argument(
                    node=node,
                    parameter=parameter,
                    annotation=annotation,
                    unresolved=item,
                    resolved=resolved[key],
                )
            return

        producer = _producer_task_name(unresolved)
        if producer is None:
            return

        # Checked before the filesystem probe below, so a fan-out costs one
        # stat rather than one per branch.
        warning_key = (producer, node.task_def.name, parameter)
        if warning_key in self._untracked_path_warnings:
            return
        if not is_untracked_directory_value(annotation=annotation, value=resolved):
            return
        self._untracked_path_warnings.add(warning_key)

        producer_base = producer.rsplit(".", 1)[-1]
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=(
                    f"{producer_base} returns a path to a directory as 'str', so '{parameter}' "
                    "is cached on the path only and content changes will not invalidate this "
                    f"task. Annotate {producer_base}'s return '-> folder' and "
                    f"'{parameter}: folder'."
                ),
            )
        )

    def _emit_notebook_notice(self, node: NodeRun, message: str) -> None:
        """Surface a notebook runner notice (e.g. ipykernel install) as an event."""
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=message,
            )
        )

    def build_and_validate(self, expr: Any) -> None:
        """Build the static task graph and validate import/env/input constraints."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        self._infer_and_apply_edges()
        self._validator.validate_declared_envs(nodes=self._nodes.values())
        self._validator.validate_declared_secrets(nodes=self._nodes.values())

        # A `file`/`folder` input that does not exist yet is normally a static
        # error, but it is not when the path is one of this graph's own
        # Out[...] outputs (#280): a real run would produce it before this
        # node runs, since an edge was just inferred for it. Dry-run static
        # validation must not reject what the run would actually satisfy.
        produced_paths = set(self._path_index.produced_exact)
        for node in self._nodes.values():
            self._validator.validate_task_importable(task_def=node.task_def)
            self._validator.validate_static_inputs(node=node, produced_paths=produced_paths)
            # Placement is static per task definition; resolving it here
            # surfaces misconfiguration (remote=True or an unsatisfiable GPU
            # requirement without a usable executor) before anything runs.
            self._resolve_placement(task_def=node.task_def)

    def _try_prepare_cache_hit(self, *, node: NodeRun) -> bool:
        """Attempt to complete a node from cache during preparation.

        This fast path only runs when cache identity can be decided without
        staging remote inputs first.
        """
        if node.resolved_args is None or self._stager.cache_lookup_requires_staging(node=node):
            return False

        if self.trust_mtimes and self._try_stat_index_hit(node=node):
            return True

        return self._try_content_cache_hit(node=node)

    def _try_content_cache_hit(self, *, node: NodeRun) -> bool:
        """Attempt a content-addressed cache hit for one prepared node."""
        assert node.resolved_args is not None
        cache_lookup_started = time.perf_counter()
        hit = self._node_cache.content_lookup(node=node)
        self._record_task_metadata(
            node=node,
            include_env_metadata=False,
        )
        self._record_task_timing(
            node_id=node.node_id,
            phase="cache_lookup_seconds",
            started=cache_lookup_started,
        )
        if hit is None:
            return False
        self._mark_node_cached(node=node, value=hit.value, cache_key=hit.cache_key)
        return True

    def _try_stat_index_hit(self, *, node: NodeRun) -> bool:
        """Attempt a stat-index cache hit for ``--trust-mtimes`` mode.

        Returns ``True`` if the hit succeeded and the node was marked
        complete, ``False`` to fall through to the content-addressed path.
        """
        cache_lookup_started = time.perf_counter()
        hit = self._node_cache.lookup_by_stat(node=node)
        if hit is None:
            self._record_task_timing(
                node_id=node.node_id,
                phase="cache_lookup_seconds",
                started=cache_lookup_started,
            )
            return False

        self._record_task_metadata(
            node=node,
            include_env_metadata=False,
        )
        self._record_task_timing(
            node_id=node.node_id,
            phase="cache_lookup_seconds",
            started=cache_lookup_started,
        )
        self._mark_node_cached(node=node, value=hit.value, cache_key=hit.cache_key)
        return True

    def _mark_node_cached(self, *, node: NodeRun, value: Any, cache_key: str) -> None:
        """Mark one node complete from cache and emit cached completion events."""
        if node.attempt == 0:
            node.attempt = 1

        # Counted here rather than projected from the event: every other
        # cache_entries column is written by the index on its own connection,
        # and a hit routed through the ledger's writer could land while
        # another process held the write lock for a save.
        self._cache_index.record_hit(cache_key)
        self._node_cache.propagate_known_digests(cache_key=cache_key)
        # A hit publishes the same asset versions an execution would, so the
        # catalog has to know them either way: their rows are what lineage
        # resolves a consumed version through, and what keeps the artifact
        # collector from treating an asset's bytes as orphaned (issue #263).
        # Best-effort by contract — the registrar contains and logs its own
        # failures, so a repair cannot cost the task its cache hit.
        self._asset_registrar.reassert_cached_versions(value=value, cache_key=cache_key)
        node.result = value
        node.state = "completed"
        for path in node.tmp_paths:
            shutil.rmtree(path)
        node.tmp_paths = []
        if self.run_dir is not None:
            self._notebook_runner.replay_cached_extras(node=node, cache_key=cache_key)
        self._emit_event(
            TaskCacheHit(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                cache_key=cache_key,
            )
        )
        self._emit_event(
            TaskCompleted(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                status="cached",
                cache_key=cache_key,
                outputs=self._output_summary_for(node=node, value=value),
                assets=self._asset_index_for(value=value),
            )
        )

        # Record stat-index entry so future --trust-mtimes runs can
        # find this cache key without content hashing.
        self._node_cache.record_stat_index_entry(node=node, cache_key=cache_key)

    def _record_task_metadata(
        self,
        *,
        node: NodeRun,
        include_env_metadata: bool = True,
    ) -> None:
        """Announce the task's resolved inputs, cache identity, and environment."""
        if self.event_bus is None:
            return
        self._emit_event(
            TaskPlanned(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                inputs=render_value(node.resolved_args or {}),
                input_hashes=_input_hash_entries(node.input_hashes),
                input_labels=dict(node.input_labels or {}),
                asset_inputs={
                    param: list(declared) for param, declared in node.asset_inputs.items()
                },
                cache_key=node.cache_key,
                source_hash=node.task_def.cache_source_hash,
                version=node.task_def.version,
                env_hash=self._env_identity(env=node.task_def.env),
                extra_source_hash=node.extra_source_hash,
                dependency_ids=[task_id_for_node(dep) for dep in sorted(node.dependency_ids)],
                dynamic_dependency_ids=[
                    task_id_for_node(dep) for dep in sorted(node.dynamic_dependency_ids)
                ],
            )
        )
        if not include_env_metadata:
            return
        if node.task_def.env is None or self.backend is None:
            return

        if is_container_env(node.task_def.env):
            fields: dict[str, Any] = {"backend": "container"}
            digest = self.backend.materialized_digest(env=node.task_def.env)
            if digest is not None:
                fields["container_image_digest"] = digest
            self._annotate_task(node=node, fields=fields)
            return

        fields = {"backend": "local"}
        lock_path = self.backend.env_lock_path(env=node.task_def.env)
        if lock_path is not None and self.run_dir is not None:
            copied = self.run_dir.copy_env_lock(env_name=node.task_def.env, lock_path=lock_path)
            if copied is not None:
                fields["env_lock"] = copied
        self._annotate_task(node=node, fields=fields)

    def _record_measured_usage(self, *, node: NodeRun, measured: dict[str, Any]) -> None:
        """Fold one usage measurement into the node's running totals.

        A task may run several subprocesses (a notebook executes, then
        renders) and several attempts, so peaks take the max and CPU
        seconds accumulate.
        """
        current = node.measured_resources
        if current is None:
            node.measured_resources = dict(measured)
            return
        current["peak_rss_bytes"] = max(
            current.get("peak_rss_bytes", 0), measured.get("peak_rss_bytes", 0)
        )
        current["cpu_seconds"] = round(
            current.get("cpu_seconds", 0.0) + measured.get("cpu_seconds", 0.0), 3
        )

    def _resource_usage_for(self, *, node: NodeRun) -> dict[str, Any]:
        """Return measured-vs-declared resource usage for one task.

        The measured values cover every attempt of the task: the peak is
        the maximum across attempts and CPU seconds are the total cost.
        """
        if node.measured_resources is None:
            return {}
        return {
            "declared": {
                "threads": node.threads,
                "memory_gb": node.declared_memory_gb,
                "effective_memory_gb": node.memory_gb,
            },
            "measured": dict(node.measured_resources),
        }

    def _record_task_timing(self, *, node_id: int, phase: str, started: float) -> None:
        """Record how long one task phase took."""
        seconds = time.perf_counter() - started
        if seconds < 0:
            return
        self._emit_event(
            PhaseTimed(
                run_id=self._run_id,
                task_id=task_id_for_node(node_id),
                phase=phase,
                seconds=round(seconds, 6),
            )
        )

    def _env_identity(self, *, env: str | None) -> str | None:
        """Return the backend's identity string for *env*, if there is one."""
        if env is None or self.backend is None:
            return None
        return self.backend.env_identity(env=env) or None

    def _annotate_task(self, *, node: NodeRun, fields: dict[str, Any]) -> None:
        """Attach open-ended facts to a task node."""
        if not fields:
            return
        self._emit_event(
            TaskAnnotated(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                fields=fields,
            )
        )

    def _display_label_for(self, *, node: NodeRun) -> str | None:
        """Return a richer CLI label for mapped tasks once args are resolved."""
        if not node.expr.mapped or node.resolved_args is None:
            return None

        if node.expr.display_label_parts:
            return node.expr.display_label

        label_key = first_label_param_name(task_def=node.task_def)
        if label_key is None or label_key not in node.resolved_args:
            return None

        rendered = render_label_value(node.resolved_args[label_key])
        if rendered is None:
            return None

        base_name = node.task_def.name.rsplit(".", 1)[-1]
        return f"{base_name}[{rendered}]"

    def _output_summary_for(self, *, node: NodeRun, value: Any) -> list[dict[str, Any]]:
        """Return a compact typed output summary for one task result."""
        annotation = node.task_def.effective_return_annotation
        return output_summary(annotation, value)

    def _asset_index_for(self, *, value: Any) -> list[dict[str, Any]]:
        """Return recorded asset summaries for one task result."""
        return asset_index_for(value=value)

    @property
    def _run_id(self) -> str:
        """Return the active run id, or a placeholder outside live runs."""
        return self.run_dir.run_id if self.run_dir is not None else "validation"

    def _emit_event(self, event: object) -> None:
        """Emit a runtime event to the attached event bus, if any."""
        if self.event_bus is not None:
            with self.profiler.timed("event_emit"):
                self.event_bus.emit(event)

    def _run_driver_task(self, *, node: NodeRun) -> Any:
        """Run a driver-task wrapper on the scheduler process.

        For notebook and script tasks the body was already evaluated eagerly
        in ``_prepare_node`` to extract the source hash for the cache key.
        The stored directive is returned directly to avoid re-running the body.
        """
        assert node.execution_args is not None
        if node.driver_directive is not None:
            return node.driver_directive
        with _task_log_context(
            stdout_path=str(node.stdout_path) if node.stdout_path is not None else None,
            stderr_path=str(node.stderr_path) if node.stderr_path is not None else None,
            secret_values=node.secret_values,
            log_emitter=lambda *, stream, chunk: self._log_drain.make_emitter(
                node=node,
                stream=stream,
            )(chunk),
        ):
            return node.task_def.fn(**node.execution_args)

    def _handle_task_body_result(self, *, node: NodeRun, completed_value: Any) -> None:
        """Advance a task after its driver wrapper has finished."""
        if self._failure is not None and (
            isinstance(completed_value, ExecutionDirective)
            or contains_dynamic_expression(completed_value)
        ):
            self._cleanup_transport(node)
            for path in node.tmp_paths:
                shutil.rmtree(path)
            node.tmp_paths = []
            node.state = "failed"
            return

        if node.task_def.kind == "python":
            if isinstance(completed_value, ExecutionDirective):
                directive_name = type(completed_value).__name__
                self._cleanup_transport(node)
                raise TypeError(
                    f"{node.task_def.name} returned {directive_name}, but the task is declared "
                    "with kind='python'. Use @task(kind='shell'), @task('notebook'), "
                    "@task('script'), or @task('subworkflow') for the appropriate task kind."
                )

            self._validator.validate_process_safe_value(
                value=completed_value,
                label=f"{node.task_def.name}.return",
            )
            self._cleanup_transport(node)

            dynamic_dependencies = self._register_value(completed_value)
            self._infer_and_apply_edges(expanding_node_id=node.node_id)
            if dynamic_dependencies:
                node.state = "waiting_dynamic"
                node.dynamic_template = completed_value
                node.dynamic_dependency_ids = dynamic_dependencies
                self._record_task_metadata(node=node)
                self._emit_event(
                    GraphExpanded(
                        run_id=self._run_id,
                        parent_task_id=task_id_for_node(node.node_id),
                        new_node_ids=[
                            task_id_for_node(dep_id) for dep_id in sorted(dynamic_dependencies)
                        ],
                    )
                )
                return

            final_value = self._finalize_result_value(node=node, value=completed_value)
            self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)
            return

        # Driver task: dispatch to the appropriate runner via the type-keyed table.
        assert self._executors is not None
        runner_entry = _DIRECTIVE_RUNNER.get(type(completed_value))
        if runner_entry is not None:
            runner_attr, method_name = runner_entry
            runner_fn = getattr(getattr(self, runner_attr), method_name)
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._executors.shell.submit(runner_fn, node=node, directive=completed_value)
            self._running_futures[future] = (node.node_id, "shell")
            return

        dynamic_dependencies = self._register_value(completed_value)
        self._infer_and_apply_edges(expanding_node_id=node.node_id)
        if dynamic_dependencies:
            self._cleanup_transport(node)
            node.state = "waiting_dynamic"
            node.dynamic_template = completed_value
            node.dynamic_dependency_ids = dynamic_dependencies
            self._record_task_metadata(node=node)
            self._emit_event(
                GraphExpanded(
                    run_id=self._run_id,
                    parent_task_id=task_id_for_node(node.node_id),
                    new_node_ids=[
                        task_id_for_node(dep_id) for dep_id in sorted(dynamic_dependencies)
                    ],
                )
            )
            return

        self._cleanup_transport(node)
        kind = node.task_def.kind
        _expected = {
            "shell": "shell(...)",
            "notebook": "notebook(...)",
            "script": "script(...)",
            "subworkflow": "subworkflow(...)",
        }
        raise TypeError(
            f"{node.task_def.name} is declared with kind={kind!r} and must return "
            f"{_expected.get(kind, 'an execution directive')} or dynamic task expressions."
        )


def _input_hash_entries(input_hashes: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return one entry per hashed input, digests spelled ``digest``.

    The cache key's own payload still says ``sha256`` — renaming it there would
    invalidate every entry on disk for no gain — but the ledger records what
    the value is, and it is a BLAKE3 digest.
    """
    entries: list[dict[str, Any]] = []
    for param, value in (input_hashes or {}).items():
        entry: dict[str, Any] = {"param": str(param)}
        if isinstance(value, dict):
            entry.update(
                {("digest" if key == "sha256" else key): item for key, item in value.items()}
            )
        else:
            entry["digest"] = value
        entries.append(entry)
    return entries


def _producer_task_name(value: Any) -> str | None:
    """Return the name of the task an unresolved argument came from, if any.

    Only single expressions are named: an ``ExprList`` resolves to a list, which
    the caller walks element by element, so each branch arrives here as its own
    ``Expr``.
    """
    if isinstance(value, (OutputIndex, OutputName)):
        return _producer_task_name(value.expr)
    if isinstance(value, Expr):
        return value.task_def.name
    return None


def _classify_access_method(*, value: Any) -> str:
    """Return ``"stage"``, ``"fuse"``, or ``"hybrid"`` for a resolved-args tree.

    Walks the value recursively, inspecting explicit ``access`` hints on
    :class:`RemoteRef` leaves. Refs without an explicit ``access`` hint
    count as ``"stage"`` for reporting purposes; the auto-enable
    heuristic may still promote them at staging time.
    """
    from ginkgo.core.remote import RemoteRef

    seen: set[str] = set()

    def walk(item: Any) -> None:
        if isinstance(item, RemoteRef):
            seen.add(item.access or "stage")
            return
        if isinstance(item, dict):
            for v in item.values():
                walk(v)
            return
        if isinstance(item, (list, tuple)):
            for v in item:
                walk(v)

    walk(value)
    if "fuse" in seen and len(seen) > 1:
        return "hybrid"
    if "fuse" in seen:
        return "fuse"
    return "stage"
