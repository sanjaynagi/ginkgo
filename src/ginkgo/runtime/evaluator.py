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
from ginkgo.core.expr import ConstructedCall, Expr, ExprList, OutputIndex
from ginkgo.core.subworkflow import SubWorkflowResult
from ginkgo.core.notebook import NotebookDirective
from ginkgo.core.script import ScriptDirective
from ginkgo.core.shell import ShellDirective
from ginkgo.errors import GinkgoError
from ginkgo.params import ParamContext
from ginkgo.core.subworkflow import SubWorkflowDirective
from ginkgo.core.resources import ResourceOverrides, Resources
from ginkgo.core.task import TaskDef
from ginkgo.core.types import is_path_shaped_annotation, tmp_dir
from ginkgo.envs.container import is_container_env
from ginkgo.runtime.backend import ExecutionEnvironment
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
    is_untracked_path_value,
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
]


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
    threads: int = 1
    memory_gb: int = 0
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
    asset_inputs: dict[str, dict[str, Any]] = field(default_factory=dict)
    notebook_extras: dict[str, Any] | None = None
    remote_job_id: str | None = None
    measured_resources: dict[str, Any] | None = None

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
        """Resolve a root expression or nested container concurrently."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
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
                        break

                    if self._is_root_resolved():
                        return self._materialize(self._root_template)

                    retry_wait = self._earliest_retry_wait()
                    if retry_wait is not None:
                        # Short, signal-interruptable sleep until the next retry is due.
                        time.sleep(min(retry_wait, 0.5))
                        continue

                    if self._can_make_scheduler_progress():
                        continue

                    raise RuntimeError("Scheduler reached a deadlock with unresolved tasks")
            finally:
                self._log_drain.stop()
                self._executors = None
                self._cache_index.close()

        assert self._failure is not None
        raise self._failure

    def _register_value(
        self,
        value: Any,
        *,
        expr_stack: tuple[int, ...] = (),
        task_path: tuple[str, ...] = (),
    ) -> set[int]:
        """Register all task nodes reachable from a nested value."""
        if isinstance(value, OutputIndex):
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
        self._emit_event(
            GraphNodeRegistered(
                run_id=self._run_id,
                task_id=task_id_for_node(node_id),
                node_id=node_id,
                task_name=expr.task_def.name,
                kind=expr.task_def.kind,
                execution_mode=expr.task_def.execution_mode,
                env=expr.task_def.env,
                retries=expr.task_def.retries,
                dependency_ids=[task_id_for_node(dep_id) for dep_id in sorted(dependency_ids)],
                stdout_log=stdout_log,
                stderr_log=stderr_log,
            )
        )
        return node_id

    def _prepare_pending_nodes(self) -> None:
        """Resolve cache-ready nodes whose dependencies have completed."""
        while True:
            progressed = False
            for node in self._nodes.values():
                if node.state != "pending":
                    continue
                if not self._dependencies_complete(node.dependency_ids):
                    continue

                self._prepare_node(node)
                progressed = True

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
        extra_source_hash: str | None = None
        if node.task_def.kind in {"notebook", "script"}:
            directive = node.task_def.fn(**resolved_args)
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
                node.state = "failed"
                self._remote_dispatch.pop_handle(node.node_id)
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

    def _handle_task_exception(self, *, node: NodeRun, exc: BaseException) -> None:
        """Either retry a failed task attempt or fail the run."""
        sanitized_exc = sanitize_exception(exc=exc, secret_values=node.secret_values)
        if self._failure is None and self._should_retry(node=node, exc=sanitized_exc):
            self._schedule_retry(node=node, exc=sanitized_exc)
            return

        node.state = "failed"
        self._cleanup_transport(node)
        if self._failure is None:
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
        asset_inputs: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Resolve concrete arguments for a task call.

        *asset_inputs* is filled in as arguments resolve, with the identity of
        the asset each parameter was handed. This is the only moment it is
        knowable for a semantically typed parameter: the next line rehydrates
        the ref into the payload the task asked for, and the identity is gone.
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
                        # One row per parameter is what task_inputs holds, so
                        # the first ref is the one recorded — see the
                        # ``asset_inputs`` docstring on TaskPlanned.
                        asset_inputs[name] = {
                            "asset_key": str(refs[0].key),
                            "version_id": refs[0].version_id,
                            "artifact_id": refs[0].artifact_id,
                        }
                # A path-shaped annotation binds a filesystem path at every
                # depth, so the whole value — including any nested containers
                # — keeps its ``AssetRef`` entries rather than becoming live
                # objects that later code would stringify as paths.
                resolved_args[name] = (
                    materialised
                    if is_path_shaped_annotation(annotation)
                    else self._rehydrate_wrapped_refs(value=materialised)
                )
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
        """Resolve runtime-only inputs such as secret references."""
        assert node.resolved_args is not None
        if self.secret_resolver is None:
            return dict(node.resolved_args)
        return {
            name: resolve_secret_refs(value=value, resolver=self.secret_resolver)
            for name, value in node.resolved_args.items()
        }

    def _materialize(self, value: Any) -> Any:
        """Materialize a nested value using completed task-node results."""
        if isinstance(value, OutputIndex):
            result = self._materialize(value.expr)
            return result[value.index]

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

        Callers decide whether to rehydrate at all: ``_resolve_task_args``
        skips this entirely for a path-shaped annotation, which binds a
        filesystem path rather than a live object.

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
            if not registry.has_default:
                raise ValueError(
                    f"{task_def.name} requires {gpu} GPU(s) but only {self.gpus} "
                    "are available locally (--gpus) and this run has no default "
                    f"executor (--executor). {registry.available_hint()}"
                )
            return registry.default_name
        return None

    def _require_remote_capable_kind(self, *, task_def: TaskDef, reason: str) -> None:
        """Reject remote placement for task kinds the workers cannot run."""
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
        """Coerce and validate a fully resolved task result."""
        coerced = self._validator.coerce_return_value(task_def=node.task_def, value=value)
        finalized = self._asset_registrar.materialize_results(node=node, value=coerced)
        self._validator.validate_return_value(task_def=node.task_def, value=finalized)
        return finalized

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
        """Warn when a path crosses a task boundary without content tracking.

        Fires only for arguments resolved from an upstream expression in this
        graph: those are the ones where the producer can rewrite the file while
        the consumer's cache key, built from the path string alone, stays put.
        Deduplicated per producer/consumer/parameter so fan-out branches report
        once. Runs before the cache-hit branch so the warning appears on the
        run that serves the stale result.
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
        if not is_untracked_path_value(annotation=annotation, value=resolved):
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
                    f"{producer_base} returns a path as 'str', so '{parameter}' is cached on the "
                    "path only and content changes will not invalidate this task. Annotate "
                    f"{producer_base}'s return '-> file' and '{parameter}: file'."
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
        self._validator.validate_declared_envs(nodes=self._nodes.values())
        self._validator.validate_declared_secrets(nodes=self._nodes.values())

        for node in self._nodes.values():
            self._validator.validate_task_importable(task_def=node.task_def)
            self._validator.validate_static_inputs(node=node)
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
                asset_inputs=dict(node.asset_inputs),
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
            "declared": {"threads": node.threads, "memory_gb": node.memory_gb},
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
        annotation = node.task_def.type_hints.get(
            "return", node.task_def.signature.return_annotation
        )
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
    if isinstance(value, OutputIndex):
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
