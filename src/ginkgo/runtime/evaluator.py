"""Concurrent evaluator for Ginkgo expressions."""

from __future__ import annotations


import os
import shutil
import tempfile
import time
import builtins
from collections.abc import Callable, Mapping, Set as AbstractSet
from contextlib import ExitStack
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    wait,
)
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.expr import Expr, ExprList, OutputIndex, OutputName
from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.params import ParamContext
from ginkgo.plugins.api import RunnerContext, TaskInvocation, TaskKind, TaskRunner
from ginkgo.plugins.registry import plugin_registry
from ginkgo.core.resources import ResourceOverrides, Resources
from ginkgo.core.task import TaskDef
from ginkgo.core.types import tmp_dir
from ginkgo.remote.staging import RemoteIdentityError
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.advisories import (
    access_fallback_notice,
    untracked_directory_notices,
    written_str_inputs_notice,
)
from ginkgo.runtime.executor_registry import LOCAL, ExecutorRegistry
from ginkgo.runtime.graph import TaskGraph, TaskNode
from ginkgo.runtime.placement import Placement
from ginkgo.runtime.remote_dispatch import (
    RemoteArtifactStoreMissingError,
    RemoteDispatchManager,
)
from ginkgo.runtime.remote_executor import RemoteDispatchStats
from ginkgo.runtime.artifacts.output_index import output_summary
from ginkgo.runtime.caching.cache import MISSING, CacheStore, DynamicTemplate
from ginkgo.runtime.caching.node_cache import NodeCache
from ginkgo.runtime.caching.digest_registry import DigestRegistry
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.executors import Executors
from ginkgo.runtime.events import (
    TaskEvent,
    EnvPrepareCompleted,
    EnvPrepareFailed,
    EnvPrepareStarted,
    EventBus,
    GraphExpanded,
    GraphNodeRegistered,
    PhaseTimed,
    SkipReason,
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
from ginkgo.runtime.rundir import RunDir, attempt_log_header
from ginkgo.runtime.scheduler import ResourceLedger, SchedulableTask
from ginkgo.runtime.environment.secrets import (
    SecretResolver,
    collect_resolved_secret_values,
    resolve_secret_refs,
)
from ginkgo.runtime.remote_input_resolver import (
    RemoteStager,
    StagedInputs,
    count_remote_inputs,
    load_remote_publisher,
    resolve_staging_jobs,
)
from ginkgo.runtime.display_labels import first_label_param_name, render_label_value
from ginkgo.runtime.task_kinds import task_kind
from ginkgo.runtime.task_runners.shell import (
    ShellRunner,
    SignalMonitor,
    classify_failure,
    remove_declared_output,
    sanitize_exception,
)
from ginkgo.runtime.task_validation import (
    TaskValidator,
    contains_dynamic_expression,
    declared_output_paths,
)
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_value
from ginkgo.runtime.worker import _task_log_context, run_task
from ginkgo.workspace_layout import WorkspaceLayout

if TYPE_CHECKING:
    from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore


class ResourceBudgetError(GinkgoError, ValueError):
    """Raised when a task asks for more of a resource than the run has."""

    failure_kind: ClassVar[FailureKind] = "insufficient_resources"


class RootSkippedError(GinkgoError, RuntimeError):
    """Raised when the run drained but the workflow result was never produced.

    Only reachable under a failure policy that keeps dispatching: a task the
    result depends on failed and was ignored, so every task between it and
    the root was skipped and there is no value to return. The run drained
    normally, so this is an outcome rather than a crash.
    """

    def __init__(self, *, label: str) -> None:
        self.label = label
        super().__init__(
            f"The workflow result depends on {label}, which failed. No result was produced."
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

    # Rebuilt as itself so the scheduler knows not to retry it.
    if (module_name, type_name) == (RemoteIdentityError.__module__, RemoteIdentityError.__name__):
        return RemoteIdentityError(*args)

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
#   the process pool or a remote executor (never for driver tasks); removed
#   by _cleanup_transport once the worker's result is decoded, or when the
#   attempt fails or is cancelled.
# - blocked_by is non-None only in "skipped", naming the failed task the
#   skip is attributed to; it stays None for a skip no task caused, such as
#   one by an interrupt.
# - last_failure is non-None from a scheduled retry onward: the classified
#   failure of the attempt being retried.
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

_SIGNAL_POLL_SECONDS = 0.5
"""Longest the scheduler blocks on running work before checking for an interrupt."""

_IN_FLIGHT_NODE_STATES = frozenset({"staging", "running", "running_shell"})
"""Node states held by work the scheduler has already handed to an executor."""


@dataclass(kw_only=True)
class NodeRun:
    """Mutable run state of one task node.

    Runs are created and mutated by :class:`ConcurrentEvaluator` as it
    schedules work; the graph vertex lives at :attr:`node`.
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
    content_input_digests: dict[tuple[str, str], str | None] | None = None
    threads: int = 1
    memory_gb: int = 0
    declared_memory_gb: int = 0
    declared_memory: str | None = None
    """The memory declaration as written, such as ``"64Mi"``, before rounding to GiB."""
    gpu: int = 0
    custom_resources: dict[str, int] = field(default_factory=dict)
    executor_name: str | None = None
    result: Any = MISSING
    tmp_paths: list[Path] = field(default_factory=list)
    transport_path: Path | None = None
    dynamic_template: Any = None
    dynamic_dependency_ids: set[int] = field(default_factory=set)
    replaying_cache: bool = False
    """Whether ``dynamic_template`` came from the node's cache entry rather
    than from running its body, so the node completes as cached."""
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    display_label: str | None = None
    """The label the node is reported and recorded under, fixed when it is
    registered; ``None`` when that is the bare task name."""
    attempt: int = 0
    retry_ready_at: float | None = None
    secret_values: tuple[str, ...] = ()
    driver_directive: Any = None
    extra_source_hash: str | None = None
    runner_extras: dict[str, Any] | None = None
    remote_job_id: str | None = None
    remote_input_digests: dict[str, str] = field(default_factory=dict)
    measured_resources: dict[str, Any] | None = None
    blocked_by: TaskNode | None = None
    """The failed task a skip is attributed to; set only when "skipped"."""
    last_failure: dict[str, Any] | None = None
    """The classified failure of the attempt a scheduled retry follows."""
    planned: dict[str, Any] | None = None
    """The ``TaskPlanned`` fields last emitted for this attempt; cleared on retry."""

    @property
    def remote(self) -> bool:
        """Whether the node is placed on a remote executor."""
        return self.executor_name is not None

    @property
    def label(self) -> str:
        """The name the node is shown under: its display label or bare task name."""
        return self.display_label or self.task_def.base_name

    @property
    def rerun_names(self) -> frozenset[str]:
        """The names ``--rerun`` selects the node by.

        Its qualified task name, its base name (every call of the task), and
        the label it is shown under (one call).
        """
        return frozenset({self.task_def.name, self.task_def.base_name, self.label})

    # The two identity views the runner contract (``TaskInvocation``) names;
    # everything else about the graph vertex is read through ``node``.
    @property
    def node_id(self) -> int:
        return self.node.node_id

    @property
    def task_def(self) -> TaskDef:
        return self.node.task_def


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
    rerun: frozenset[str] = frozenset()
    """Tasks whose cache lookup is skipped, so they run again: each a task's
    qualified name, base name, or one branch's display label."""
    _cache_store: CacheStore = field(init=False, repr=False)
    _nodes: dict[int, NodeRun] = field(default_factory=dict, init=False, repr=False)
    graph: TaskGraph = field(default_factory=TaskGraph, init=False, repr=False)
    placement: Placement = field(init=False)
    _ledger: ResourceLedger = field(init=False, repr=False)
    # Each in-flight future maps to its node and the handler for its result.
    _running_futures: dict[Future[Any], tuple[int, Callable[..., None]]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _root_template: Any = field(default=None, init=False, repr=False)
    _root_dependency_ids: set[int] = field(default_factory=set, init=False, repr=False)
    _failure: BaseException | None = field(default=None, init=False, repr=False)
    _failed_node: NodeRun | None = field(default=None, init=False, repr=False)
    """The task whose failure is ``_failure``; ``None`` for an interrupt."""
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

    def __post_init__(self) -> None:
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
        self._ledger = ResourceLedger(
            jobs=self.jobs,
            cores=self.cores,
            memory=self.memory,
            gpus=self.gpus,
            custom_budgets=self.resource_budgets,
        )
        self.placement = Placement(
            executor_registry=self.executor_registry,
            resource_overrides=self.resource_overrides,
            gpus=self.gpus,
        )

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
        self._staging_jobs = resolve_staging_jobs()
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
        # Runners for the other task kinds are built on first use, from the
        # kind's plugin spec, so a run builds only the runners it needs.
        self._runner_context = RunnerContext(
            validator=self._validator,
            shell_runner=self._shell_runner,
            run_dir=self.run_dir,
            annotate=self._annotate_task,
            notice=lambda *, node, message: self._emit_task_event(
                TaskNotice, node=node, message=message
            ),
            run_id=lambda: self._run_id or "",
            db_path=WorkspaceLayout.for_cwd().db,
        )
        self._runners: dict[str, TaskRunner] = {}
        self._stager = RemoteStager(timing_recorder=self._record_task_timing)

    @property
    def validator(self) -> TaskValidator:
        """The run's input/contract validator, shared with dry-run probing."""
        return self._validator

    @property
    def cache_store(self) -> CacheStore:
        """The cache store backing this evaluator."""
        return self._cache_store

    @property
    def node_cache(self) -> NodeCache:
        """The run's cache lookups, shared with dry-run probing."""
        return self._node_cache

    @property
    def remote_stats(self) -> RemoteDispatchStats:
        """Aggregated remote-dispatch statistics for this run."""
        return self._remote_dispatch.stats

    @property
    def task_nodes(self) -> Mapping[int, NodeRun]:
        """Read-only view of the task graph, keyed by scheduler node id.

        Populated once the graph has been built (after :meth:`build_graph` or
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
            expr=node.node.expr,
            task_def=node.task_def,
            include_tmp_dirs=False,
        )

    def expand_probed_template(self, *, node: NodeRun, template: Any) -> list[int]:
        """Register a cached dynamic template for a dry-run probe, running nothing.

        The probe's companion to a cache hit's replay: *node* waits on the
        template's tasks, which the probe then examines like any other.

        Parameters
        ----------
        node : NodeRun
            The probed node whose cache entry holds *template*.
        template : Any
            The task expressions the entry holds.

        Returns
        -------
        list[int]
            The nodes the template added, dependencies first.
        """
        node.dynamic_dependency_ids = self._register(template, expanding_node_id=node.node_id)
        node.dynamic_template = template
        node.state = "waiting_dynamic"
        return list(self.graph.new_node_ids)

    def complete_probed_template(self, *, node: NodeRun) -> None:
        """Complete a probed node from its template, if every task it returned is cached.

        Otherwise the node stays waiting, so nothing downstream of it can be
        resolved.
        """
        if not self._dependencies_complete(node.dynamic_dependency_ids):
            return
        node.result = self._validator.coerce_return_value(
            task_def=node.task_def, value=self._materialize(node.dynamic_template)
        )
        node.state = "completed"

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
        self._root_dependency_ids = self._register(expr)
        if not self._root_dependency_ids:
            return self._materialize(expr)

        with ExitStack() as stack:
            # The monitor is installed first so an interrupt during pool
            # setup or teardown is recorded rather than raised mid-way.
            signals = stack.enter_context(SignalMonitor())
            executors = stack.enter_context(
                Executors(jobs=self.jobs, staging_jobs=self._staging_jobs)
            )
            self._executors = executors
            self._log_drain.start(queue=executors.log_queue)
            try:
                # Validate all statically declared environments before any work
                # starts. Inside the try, so a refusal closes the run's tasks too.
                self._validator.validate_declared_envs(nodes=self.graph.nodes.values())
                self._validator.validate_declared_secrets(nodes=self.graph.nodes.values())
                while True:
                    if signals.exception is not None and self._failure is None:
                        self._failure = signals.exception
                        self._interrupt_running_work()

                    if self._failure is None:
                        self._promote_due_retries()
                        self._skip_blocked_nodes()
                        self._prepare_pending_nodes()
                        if self._failure is None:
                            self._finalize_dynamic_nodes()

                    # A preparation failure stops the run here: nothing it
                    # readied in the same pass is dispatched.
                    if self._failure is None:
                        self._dispatch_ready_nodes()

                        if self._is_root_resolved() and not self._running_futures:
                            return self._materialize(self._root_template)

                    if self._running_futures:
                        # The signal handler only records an interrupt, so the
                        # wait is bounded for the loop to notice one while
                        # long tasks run.
                        retry_wait = self._earliest_retry_wait() if self._failure is None else None
                        done, _ = wait(
                            tuple(self._running_futures.keys()),
                            return_when=FIRST_COMPLETED,
                            timeout=min(
                                _SIGNAL_POLL_SECONDS if retry_wait is None else retry_wait,
                                _SIGNAL_POLL_SECONDS,
                            ),
                        )
                        self._consume_completed_futures(done)
                        continue

                    if self._failure is not None:
                        raise self._failure

                    if self._is_root_resolved():
                        return self._materialize(self._root_template)

                    retry_wait = self._earliest_retry_wait()
                    if retry_wait is not None:
                        # Short, signal-interruptable sleep until the next retry is due.
                        time.sleep(min(retry_wait, _SIGNAL_POLL_SECONDS))
                        continue

                    if self._can_make_scheduler_progress():
                        continue

                    if self._is_drained():
                        root_skipped = self._root_skipped_error()
                        if root_skipped is not None:
                            raise root_skipped

                    raise self._deadlock_error()
            except BaseException as exc:
                # Whatever stopped the run, a task it started and will not see
                # finish is closed in the record rather than left running, and
                # a remote job it will not watch any more is cancelled.
                self._remote_dispatch.cancel_all()
                self._close_unfinished_nodes(reason=exc)
                raise
            finally:
                self._log_drain.stop()
                self._executors = None
                self._cache_index.close()

    def _register(self, value: Any, *, expanding_node_id: int | None = None) -> set[int]:
        """Register the task nodes *value* reaches, giving each new one its run state.

        Returns
        -------
        set[int]
            The nodes *value* refers to directly.
        """
        try:
            registration = self.graph.register(
                value,
                expanding_node_id=expanding_node_id,
                node_state=lambda node_id: self._nodes[node_id].state,
            )
        except BaseException:
            # The nodes a failed registration added stay in the graph, and an
            # inferred edge may already point at one, so each still gets run
            # state rather than leave a dependency on nothing.
            for node_id in self.graph.new_node_ids:
                self._add_node_run(self.graph.nodes[node_id])
            raise
        for node_id in registration.new_node_ids:
            self._add_node_run(self.graph.nodes[node_id])
        return registration.dependency_ids

    def _add_node_run(self, node: TaskNode) -> None:
        """Give a newly registered node its run state, and announce it."""
        run = NodeRun(node=node, display_label=node.display_label)
        self._nodes[node.node_id] = run
        stdout_log = stderr_log = None
        if self.run_dir is not None:
            run.stdout_path, run.stderr_path = self.run_dir.log_paths_for(
                node_id=node.node_id,
                task_name=node.task_def.base_name,
            )
            stdout_log = self.run_dir.relative(run.stdout_path)
            stderr_log = self.run_dir.relative(run.stderr_path)
        self._emit_event(
            GraphNodeRegistered(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                node_id=node.node_id,
                task_name=node.task_def.name,
                display_label=node.display_label,
                kind=node.task_def.kind,
                execution_mode=task_kind(node.task_def).execution_mode,
                env=node.task_def.env,
                retries=node.task_def.retries,
                dependency_ids=[
                    task_id_for_node(dep_id) for dep_id in sorted(node.dependency_ids)
                ],
                inferred_dependency_ids=[
                    task_id_for_node(dep_id) for dep_id in sorted(node.inferred_dependency_ids)
                ],
                stdout_log=stdout_log,
                stderr_log=stderr_log,
            )
        )

    def _prepare_pending_nodes(self) -> None:
        """Resolve cache-ready nodes whose dependencies have completed.

        A node whose preparation raises fails like any other task attempt,
        except that it is never retried: nothing it raised came from running
        its body, so another attempt would only fail the same way.
        """
        while True:
            progressed = False
            # A copy: a cache hit that replays a dynamic template registers
            # nodes, which the next pass prepares.
            for node in list(self._nodes.values()):
                if node.state != "pending":
                    continue
                if not self._dependencies_complete(node.node.dependency_ids):
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
            expr=node.node.expr,
            task_def=node.task_def,
            include_tmp_dirs=False,
        )
        # Before the cache-hit branch, so the warning appears on the run that
        # serves the stale result.
        for message in untracked_directory_notices(
            task_def=node.task_def,
            args=node.node.expr.args,
            resolved_args=resolved_args,
            warned=self._untracked_path_warnings,
        ):
            self._emit_task_event(TaskNotice, node=node, message=message)
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=resolved_args)
        self._validator.validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )

        # For kinds whose directive names source files (a notebook, a script,
        # a child workflow), evaluate the body now so their hash joins the
        # cache key. A body that returns anything else is reported where its
        # result is handled.
        extra_source_hash: str | None = None
        kind = task_kind(node.task_def)
        if kind.eager_source_hash:
            directive = node.task_def.fn(**resolved_args)
            node.driver_directive = directive
            if kind.directive is not None and isinstance(directive, kind.directive):
                extra_source_hash = directive.source_hash

        node.resolved_args = resolved_args
        node.extra_source_hash = extra_source_hash
        node.display_label = self._resolved_display_label(node=node)
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

        resources = self.placement.effective_resources(task_def=node.task_def)
        node.threads = resources.threads
        node.memory_gb = resources.memory_gb
        # Kept across escalation: node.memory_gb becomes the retry's budget,
        # so without this the declaration a user would edit is lost.
        node.declared_memory_gb = resources.memory_gb
        node.declared_memory = resources.memory
        node.gpu = resources.gpu
        node.custom_resources = dict(resources.custom)
        node.executor_name = self.placement.resolve(task_def=node.task_def)
        # A remote worker passes files, folders and large values through the
        # artifact store, so a remote task cannot run without one. Raised
        # here, where a failure is not retried, before anything is submitted.
        if node.executor_name is not None and self._remote_dispatch.artifact_store is None:
            raise RemoteArtifactStoreMissingError(
                f"Task {node.task_def.name!r} runs on executor {node.executor_name!r}, but "
                "no artifact store is configured; set [remote.artifacts] store = "
                '"<scheme>://<bucket>/<prefix>/" in ginkgo.toml.'
            )
        self._apply_memory_escalation(node=node, resources=resources)
        self.check_resource_budget(
            task_name=node.task_def.base_name,
            threads=node.threads,
            memory_gb=node.memory_gb,
            custom=node.custom_resources,
            remote=node.remote,
        )
        node.state = "ready"
        self._emit_task_event(
            TaskReady,
            node=node,
            resources=self._resources_payload(node=node),
        )

    def check_resource_budget(
        self,
        *,
        task_name: str,
        threads: int,
        memory_gb: int,
        custom: Mapping[str, int],
        remote: bool,
    ) -> None:
        """Refuse a task whose demand can never fit the run's budgets.

        Parameters
        ----------
        task_name : str
            The task, for the message.
        threads, memory_gb : int
            Its core and memory demand. Only checked when it runs locally:
            a remote executor satisfies its own requests.
        custom : Mapping[str, int]
            Its user-defined resource demands. Checked wherever it runs,
            since those budgets are run-level.
        remote : bool
            Whether the task is placed on a remote executor.

        Raises
        ------
        ResourceBudgetError
            If any demand exceeds its budget.
        """
        for dimension, demand in custom.items():
            budget = (self.resource_budgets or {}).get(dimension)
            if budget is not None and demand > budget:
                raise ResourceBudgetError(
                    f"{task_name} requires {demand} {dimension} but only "
                    f"{budget} are available in the run's {dimension} budget"
                )
        if remote:
            return
        if threads > self.cores:
            raise ResourceBudgetError(
                f"{task_name} requires {threads} cores but only {self.cores} are available"
            )
        if self.memory is not None and memory_gb > self.memory:
            raise ResourceBudgetError(
                f"{task_name} requires {memory_gb} GiB but only {self.memory} GiB are available"
            )

    def _prepare_task_environment(self, *, node: NodeRun) -> None:
        """Materialize any external execution environment required by a task."""
        if node.task_def.env is None or self.backend is None:
            return

        self._emit_task_event(
            EnvPrepareStarted,
            node=node,
            env=node.task_def.env,
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
            self._emit_task_event(
                EnvPrepareFailed,
                node=node,
                env=node.task_def.env,
                error=str(exc),
            )
            raise
        self._record_task_timing(
            node_id=node.node_id,
            phase="env_prepare_seconds",
            started=env_prepare_started,
        )
        self._emit_task_event(
            EnvPrepareCompleted,
            node=node,
            env=node.task_def.env,
        )

    def _footprint(self, node: NodeRun) -> SchedulableTask:
        """Return what *node* holds of the run's budgets while it is in flight.

        A remote-placed task holds a jobs slot but no local resources: the
        executor satisfies its threads, memory and GPUs. Custom demands
        stay, because those budgets are run-level.
        """
        return SchedulableTask(
            node_id=node.node_id,
            threads=0 if node.remote else node.threads,
            memory_gb=0 if node.remote else node.memory_gb,
            gpu=0 if node.remote else node.gpu,
            priority=node.task_def.priority,
            concurrency_group=node.node.expr.concurrency_group,
            concurrency_group_limit=node.node.expr.concurrency_group_limit,
            custom=node.custom_resources,
        )

    def _track(self, future: Future[Any], *, node: NodeRun, on_done: Callable[..., None]) -> None:
        """Hold *node*'s footprint until *future*, its work in flight, is handled."""
        self._running_futures[future] = (node.node_id, on_done)
        self._ledger.reserve(self._footprint(node))

    def _dispatch_ready_nodes(self) -> None:
        """Submit a resource-feasible subset of ready nodes."""
        ready_nodes = [node for node in self._nodes.values() if node.state == "ready"]
        if not ready_nodes:
            return

        selected = self._ledger.select([self._footprint(node) for node in ready_nodes])

        for node_id in selected:
            node = self._nodes[node_id]
            node.attempt += 1
            node.resolved_args = self._resolve_task_args(
                expr=node.node.expr,
                task_def=node.task_def,
                include_tmp_dirs=True,
                existing_args=node.resolved_args,
                tmp_paths=node.tmp_paths,
            )
            if count_remote_inputs(node.resolved_args) > 0:
                node.state = "staging"
                # An input whose identity is unknown is resolved first, so a
                # cache hit downloads nothing; staging follows only a miss.
                if self._stager.needs_identity(node=node):
                    assert self._executors is not None
                    future = self._stager.resolve_task_identities(
                        node=node, pool=self._executors.staging
                    )
                    self._track(future, node=node, on_done=self._on_identities_resolved)
                else:
                    self._stage_inputs(node=node)
                continue

            self._start_task_execution(node=node)

    def _stage_inputs(self, *, node: NodeRun) -> None:
        """Stage a node's remote inputs on the staging pool, then start it."""
        assert node.resolved_args is not None
        assert self._executors is not None
        self._emit_task_event(
            TaskStaging,
            node=node,
            remote_input_count=count_remote_inputs(node.resolved_args),
            access_method=_classify_access_method(
                value=node.resolved_args,
                default=self._stager.default_access(task_def=node.task_def),
            ),
        )
        future = self._stager.stage_task_inputs(node=node, pool=self._executors.staging)
        self._track(future, node=node, on_done=self._on_inputs_staged)

    def _consume_completed_futures(self, done_futures: set[Future[Any]]) -> None:
        """Hand each finished future's result to the handler it was submitted with."""
        for future in done_futures:
            node_id, on_done = self._running_futures.pop(future)
            self._ledger.release(node_id)
            node = self._nodes[node_id]
            remote_handle = self._remote_dispatch.pop_handle(node_id)
            if remote_handle is not None:
                node.remote_job_id = remote_handle.job_id

            if isinstance(self._failure, KeyboardInterrupt) and (
                future.cancelled() or future.exception() is not None
            ):
                # Work the interrupt stopped (a killed worker, a terminated
                # command) did not fail: the node stays in flight and is
                # closed as interrupted with every other unfinished task.
                continue

            if future.cancelled():
                # Only a stopping run cancels work; anything else is a
                # scheduler bug, which stops the run and closes the node.
                if self._failure is None:
                    raise RuntimeError(
                        f"Task {node.display_label or node.task_def.base_name!r} was cancelled while the run was not stopping"
                    )
                self._close_unfinished_node(node=node, reason=self._failure)
                continue

            try:
                completed_value = future.result()
                if remote_handle is not None:
                    self._remote_dispatch.capture_logs(node=node, handle=remote_handle)
                on_done(node=node, completed_value=completed_value)
            except BaseException as exc:
                # A remote input that cannot be identified, or changed under
                # its key, fails every attempt in this run the same way.
                self._handle_task_exception(
                    node=node, exc=exc, retryable=not isinstance(exc, RemoteIdentityError)
                )

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

                try:
                    value = self._materialize(node.dynamic_template)
                    if node.replaying_cache:
                        self._complete_replayed_node(node=node, value=value)
                    else:
                        self._complete_node(node=node, value=value)
                except Exception as exc:
                    # The result is wrong, not the attempt: a retry would
                    # return the same expressions.
                    self._handle_task_exception(node=node, exc=exc, retryable=False)
                    if self._failure is not None:
                        return
                progressed = True

            if not progressed:
                return

    def _on_worker_done(self, *, node: NodeRun, completed_value: Any) -> None:
        """Handle the result returned from a Python worker, local or remote."""
        try:
            if isinstance(completed_value, dict) and isinstance(
                completed_value.get("measured_resources"), dict
            ):
                self._record_measured_usage(
                    node=node, measured=completed_value["measured_resources"]
                )
            self._fold_remote_input_access(node=node, payload=completed_value)
            completed_value = self._decode_worker_result(node=node, payload=completed_value)
        finally:
            # The decoded result no longer refers to the transport directory.
            self._cleanup_transport(node)
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _on_identities_resolved(self, *, node: NodeRun, completed_value: Any) -> None:
        """Look a node up once its remote inputs' identities are known; stage on a miss."""
        node.resolved_args = completed_value
        if self._try_cache_hit(node=node):
            return
        self._stage_inputs(node=node)

    def _on_inputs_staged(self, *, node: NodeRun, completed_value: Any) -> None:
        """Start task execution after remote inputs have been staged locally."""
        if not isinstance(completed_value, StagedInputs):
            raise TypeError("Expected staged task arguments from staging phase")

        node.resolved_args = completed_value.args
        self._record_input_digests(node=node, digests=completed_value.digests)
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=node.resolved_args)
        self._start_task_execution(node=node)

    def _on_runner_done(self, *, node: NodeRun, completed_value: Any) -> None:
        """Handle the result of a directive a task runner executed."""
        self._complete_node(node=node, value=completed_value)

    def _handle_task_exception(
        self,
        *,
        node: NodeRun,
        exc: BaseException,
        retryable: bool = True,
    ) -> None:
        """Either retry a failed task attempt or fail the run.

        Parameters
        ----------
        node : NodeRun
            The task whose attempt failed.
        exc : BaseException
            What the attempt raised; secrets are redacted before it is
            recorded.
        retryable : bool
            Whether the task's retry policy is consulted. ``False`` records
            the failure outright, for failures that did not come from running
            the task body (such as preparation) and would recur unchanged.
        """
        self._cleanup_transport(node)
        sanitized_exc = sanitize_exception(exc=exc, secret_values=node.secret_values)
        if retryable and self._should_retry(node=node, exc=sanitized_exc):
            if self._failure is None:
                self._schedule_retry(node=node, exc=sanitized_exc)
                return
            # The run is already stopping, so the retry this attempt earned is
            # abandoned. Say so, or the failure reads as the task's last word.
            self._notice_retry_abandoned(node=node, reason=self._failure)

        # "ignore" is about scheduling only: the task is recorded as failed
        # either way, and the run still ends up failed. Once the run is
        # stopping — a fatal failure, or an interrupt — nothing is ignored any
        # more: the policy could not carry a run on that has already ended, so
        # every failure draining out behind it is reported as what it is.
        ignore = self._failure is None and (
            self.keep_going or node.task_def.on_failure == "ignore"
        )
        node.state = "failed"
        if ignore:
            self._ignored_failures.append((node, sanitized_exc))
        elif self._failure is None:
            self._failure = sanitized_exc
            self._failed_node = node
            self._cancel_pending_futures()
        # Usage measured before the failure helps right-size OOM-prone tasks.
        usage = self._resource_usage_for(node=node)
        if usage:
            self._annotate_task(node=node, fields={"resource_usage": usage})
        child_run_id = getattr(sanitized_exc, "child_run_id", None)
        if child_run_id is not None:
            self._annotate_task(node=node, fields={"sub_run_id": child_run_id})
        self._emit_task_event(
            TaskFailed,
            node=node,
            exit_code=getattr(sanitized_exc, "exit_code", None),
            failure=classify_failure(exc=sanitized_exc),
            remote_job_id=node.remote_job_id,
            ignored=ignore,
        )

    def _close_unfinished_nodes(self, *, reason: BaseException) -> None:
        """Give every task the stopping run leaves without an outcome a terminal one.

        A finished run has no task left ``pending``:

        - Work still in flight, and a task waiting on its own expansion, is
          recorded as failed, cancelled by the stop, when a failure stopped
          the run. An interrupt is no task's failure, so under one it is
          skipped as interrupted, like a task that never started.
        - A task between attempts, its retry scheduled, is recorded as failed
          with the failure of its last attempt; its remaining retries are
          abandoned, and a notice says so.
        - A task that never started is skipped. Under an interrupt that is
          all there is to say. Otherwise it is blocked by the failure it
          waits on, if it waits on one, and else by the failure that stopped
          the run.
        """
        interrupted = isinstance(reason, KeyboardInterrupt)
        for node in self._nodes.values():
            if node.state in _IN_FLIGHT_NODE_STATES or node.state == "waiting_dynamic":
                if interrupted:
                    self._cleanup_transport(node)
                else:
                    self._close_unfinished_node(node=node, reason=reason)
            elif node.state not in _TERMINAL_NODE_STATES and node.attempt > 0:
                self._abandon_retry(node=node, reason=reason)

        if interrupted:
            for node in self._unfinished_nodes():
                self._mark_node_skipped(node=node, reason="interrupted", blocker=None)
            return

        while True:
            blocked = [
                (node, blocker)
                for node in self._unfinished_nodes()
                if (blocker := self._blocking_dependency(node=node)) is not None
            ]
            if not blocked:
                break
            for node, blocker in blocked:
                self._mark_node_skipped(node=node, reason="upstream_failed", blocker=blocker)
        for node in self._unfinished_nodes():
            self._mark_node_skipped(node=node, reason="run_stopped", blocker=self._failed_node)

    def _deadlock_error(self) -> RuntimeError:
        """Describe a scheduler that can make no progress: each unresolved task and its waits."""
        lines = []
        for node in self._unfinished_nodes():
            waits = sorted(
                self._nodes[dep_id].label
                for dep_id in node.node.dependency_ids | node.dynamic_dependency_ids
                if self._nodes[dep_id].state != "completed"
            )
            lines.append(
                f"  {node.label} ({node.state}) waits on: {', '.join(waits) or 'nothing'}"
            )
        return RuntimeError(
            "Scheduler reached a deadlock with unresolved tasks:\n" + "\n".join(lines)
        )

    def _unfinished_nodes(self) -> list[NodeRun]:
        """Return the nodes not yet in a terminal state."""
        return [node for node in self._nodes.values() if node.state not in _TERMINAL_NODE_STATES]

    def _abandon_retry(self, *, node: NodeRun, reason: BaseException) -> None:
        """Record a task whose retry the stopping run will not make as failed."""
        assert node.last_failure is not None
        node.state = "failed"
        self._notice_retry_abandoned(node=node, reason=reason)
        self._emit_task_event(
            TaskFailed,
            node=node,
            failure=node.last_failure,
            remote_job_id=node.remote_job_id,
        )

    def _notice_retry_abandoned(self, *, node: NodeRun, reason: BaseException) -> None:
        """Say that the retry *node*'s last attempt earned will not be made."""
        cause = (
            "the run was interrupted"
            if isinstance(reason, KeyboardInterrupt)
            else "the run is stopping after an earlier failure"
        )
        self._emit_task_event(
            TaskNotice,
            node=node,
            message=f"retry {node.attempt}/{node.task_def.retries} not attempted: {cause}",
        )

    def _close_unfinished_node(self, *, node: NodeRun, reason: BaseException) -> None:
        """Record one started task as cancelled by the run stopping around it."""
        node.state = "failed"
        self._cleanup_transport(node)
        self._emit_task_event(
            TaskFailed,
            node=node,
            failure={
                "kind": "cancelled",
                "message": f"The run stopped before this task finished: {reason}",
                "code": "cancelled",
            },
            remote_job_id=node.remote_job_id,
        )

    def _should_retry(self, *, node: NodeRun, exc: BaseException) -> bool:
        """Return whether the current failed attempt should be retried."""
        if node.attempt > node.task_def.retries:
            return False
        return node.task_def.should_retry_exception(exc=exc)

    def _schedule_retry(self, *, node: NodeRun, exc: BaseException) -> None:
        """Reset node state so the scheduler can rerun the task from scratch."""
        self._remove_tmp_paths(node)

        # Attempt is incremented on dispatch, so the next attempt is node.attempt + 1.
        delay = node.task_def.retry_delay_seconds(attempt=node.attempt)
        node.last_failure = classify_failure(exc=exc)
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
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        node.replaying_cache = False
        node.secret_values = ()
        node.extra_source_hash = None
        node.planned = None
        # measured_resources is deliberately NOT reset: a retried attempt's
        # peak (an OOM kill under memory_retry_multiplier, say) is exactly
        # the number needed to right-size the task, so measurements span
        # attempts — peaks take the max, CPU seconds accumulate.

        retries_remaining = node.task_def.retries - node.attempt
        self._emit_task_event(
            TaskRetrying,
            node=node,
            retries_remaining=retries_remaining,
            failure=node.last_failure,
            delay_seconds=delay,
        )

    def _complete_node(self, *, node: NodeRun, value: Any) -> None:
        """Validate a task's result, then persist it and mark the node completed."""
        value = self._finalize_result_value(node=node, value=value)
        finalize_started = time.perf_counter()
        # The task's own text, apart from the modules folded into its source
        # hash, lets `cache explain` say when only a neighbour changed.
        extra_meta: dict[str, Any] = {"own_source_hash": node.task_def.own_source_hash}
        if node.runner_extras is not None:
            extra_meta["runner_extras"] = node.runner_extras
        written_notice = written_str_inputs_notice(self._node_cache.written_str_inputs(node=node))
        if written_notice is not None:
            self._emit_task_event(TaskNotice, node=node, message=written_notice)
        # A dynamic task's entry keeps the expressions it returned, so a hit
        # replays them and each child is looked up on its own.
        dynamic = node.dynamic_template is not None
        artifact_ids = self._cache_store.save(
            cache_key=node.cache_key,
            result=DynamicTemplate(node.dynamic_template) if dynamic else value,
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

        self._remove_tmp_paths(node)

        node.result = value
        node.state = "completed"
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        node.execution_args = None
        node.secret_values = ()
        self._emit_task_event(
            TaskCompleted,
            node=node,
            status="success",
            cache_key=node.cache_key,
            outputs=self._output_summary_for(
                node=node,
                value=value,
                # A dynamic task's outputs are its children's, already stored.
                artifact_ids=self._digests.known if dynamic else artifact_ids,
            ),
            resource_usage=self._resource_usage_for(node=node),
            remote_job_id=node.remote_job_id,
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
        existing_args: dict[str, Any] | None = None,
        tmp_paths: list[Path] | None = None,
    ) -> dict[str, Any]:
        """Resolve concrete arguments for a task call."""
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
                resolved_args[name] = materialised
                continue

            if name == "threads":
                # Inject the effective thread count (declaration plus any site
                # override) so user code can use it for shell command
                # interpolation or in-process work.
                resolved_args[name] = self.placement.effective_resources(task_def=task_def).threads
                continue

            if parameter.default is not parameter.empty:
                resolved_args[name] = parameter.default
                continue

            raise TypeError(f"{task_def.fn.__name__}() missing required argument: '{name}'")

        return resolved_args

    def _resolve_execution_args(self, *, node: NodeRun) -> dict[str, Any]:
        """Resolve runtime-only inputs: secret references.

        Called only once a cache miss is confirmed (see ``_start_task_execution``),
        so a cache hit never resolves a secret it does not need.
        """
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

        if isinstance(value, OutputName):
            node = self._nodes[self.graph.node_id_for(value.expr)]
            if node.state != "completed":
                raise RuntimeError(f"Task {node.task_def.name} is not yet complete")
            return self._resolve_output_param_value(node=node, name=value.name)

        if isinstance(value, Expr):
            node = self._nodes[self.graph.node_id_for(value)]
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
            if node.state == "pending" and self._dependencies_complete(node.node.dependency_ids):
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
                self._mark_node_skipped(node=node, reason="upstream_failed", blocker=blocker)
                progressed = True

            if not progressed:
                return

    def _blocking_dependency(self, *, node: NodeRun) -> NodeRun | None:
        """Return a dependency of *node* that has failed or been skipped."""
        for node_id in sorted(node.node.dependency_ids | node.dynamic_dependency_ids):
            dependency = self._nodes[node_id]
            if dependency.state in {"failed", "skipped"}:
                return dependency
        return None

    def _mark_node_skipped(
        self, *, node: NodeRun, reason: SkipReason, blocker: NodeRun | None
    ) -> None:
        """Record one node as skipped, attributed to the failure behind it.

        Nothing about the attempt the node may already have made is cleared:
        a node waiting on its own dynamic expansion has run its body, and the
        record of that work is true whatever the outcome.

        Parameters
        ----------
        node : NodeRun
            The node to skip.
        reason : SkipReason
            Why it produces no result.
        blocker : NodeRun | None
            The failed or skipped node it is attributed to, reached through
            any intervening skips; ``None`` when no task caused the skip.
        """
        blocked_by = None
        if blocker is not None:
            blocked_by = blocker.blocked_by if blocker.state == "skipped" else blocker.node
        node.state = "skipped"
        node.blocked_by = blocked_by
        self._emit_task_event(
            TaskSkipped,
            node=node,
            reason=reason,
            blocked_by_task_id=task_id_for_node(blocked_by.node_id) if blocked_by else None,
            blocked_by_task_name=blocked_by.task_def.name if blocked_by else None,
        )

    def _root_skipped_error(self) -> RootSkippedError | None:
        """Return the error naming why the run produced no result, if it is that.

        ``None`` when no root dependency is failed or skipped, which means
        the graph stalled for some other reason and the caller should say so.
        """
        for node_id in sorted(self._root_dependency_ids):
            node = self._nodes[node_id]
            if node.state == "skipped":
                # Labelled as its run is, which a dynamic branch gets on resolution.
                blocker = node if node.blocked_by is None else self._nodes[node.blocked_by.node_id]
                return RootSkippedError(label=blocker.label)
            if node.state == "failed":
                return RootSkippedError(label=node.label)
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
        """Cancel queued futures that have not started yet.

        A queued remote watcher's job is already submitted, and once its
        watcher is cancelled nothing else would cancel the job, so it is
        cancelled on its executor here.
        """
        for future, (node_id, _) in self._running_futures.items():
            if future.cancel():
                self._remote_dispatch.cancel(node_id=node_id)

    def _interrupt_running_work(self) -> None:
        """Stop queued and active work after an external interrupt."""
        self._cancel_pending_futures()
        self._remote_dispatch.cancel_all()
        self._shell_runner.terminate_all()
        if self._executors is not None:
            self._executors.shutdown_all()

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

    def _start_task_execution(self, *, node: NodeRun) -> None:
        """Launch a task after its inputs have been staged locally."""
        assert node.resolved_args is not None
        assert self._executors is not None

        if self._try_cache_hit(node=node):
            return

        self._emit_task_event(
            TaskCacheMiss,
            node=node,
            cache_key=node.cache_key,
            rerun_requested=self.rerun_requested(node=node),
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
            self._prepare_output_paths(node=node)
        # Placement was resolved when the node was prepared; the backend
        # recorded in events and provenance is the executor's name.
        execution_backend = node.executor_name or LOCAL

        self._emit_task_event(
            TaskStarted,
            node=node,
            kind=node.task_def.kind,
            env=node.task_def.env,
            resources={
                **self._resources_payload(node=node),
                "max_attempts": node.task_def.retries + 1,
            },
            execution_backend=execution_backend,
        )
        if node.attempt > 1:
            self._mark_attempt_in_logs(node=node)
        if task_kind(node.task_def).execution_mode == "driver":
            future = self._executors.shell.submit(self._run_driver_task, node=node)
            self._track(future, node=node, on_done=self._handle_task_body_result)
            return

        node.transport_path = Path(tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-"))
        payload = self._build_worker_payload(node=node)
        # Remote dispatch: the node was placed on an executor either
        # explicitly (executor= / remote=True) or because its GPU requirement
        # exceeds the local budget.
        if node.executor_name is not None:
            future = self._remote_dispatch.dispatch(
                node=node,
                executor_name=node.executor_name,
                payload=payload,
                gpu_type=self.placement.effective_resources(task_def=node.task_def).gpu_type,
                watcher=self._executors.get_or_create_remote_watcher(),
            )
        else:
            future = self._executors.python.submit(run_task, payload)
        self._track(future, node=node, on_done=self._on_worker_done)

    def _mark_attempt_in_logs(self, *, node: NodeRun) -> None:
        """Write a header into the task's logs at the start of a retry.

        Every attempt appends to the same log files, so without a marker a
        log tail cannot say which attempt a line came from.
        """
        header = attempt_log_header(attempt=node.attempt, attempts=node.task_def.retries + 1)
        for path in (node.stdout_path, node.stderr_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(header)

    def _prepare_output_paths(self, *, node: NodeRun) -> None:
        """Clear every declared ``Out[...]`` path before the task writes it.

        Runs once per attempt, after pre-execution validation passes and
        before the task body dispatches — on the driver, ahead of python
        workers, shell/script/notebook commands, and container runs alike,
        since all of those are launched from this one call site. Whatever an
        earlier run left at a declared path is removed (a file, or a folder
        with all its contents), so a body that globs its own ``Out[folder]``
        never sees stale files. The *parent* is created; an ``Out[folder]``
        itself is left for the task body to create, since tools differ on
        whether they want it to already exist.
        """
        assert node.execution_args is not None
        for _, path, _ in declared_output_paths(
            task_def=node.task_def, resolved_args=node.execution_args
        ):
            output = Path(path)
            remove_declared_output(output)
            output.parent.mkdir(parents=True, exist_ok=True)

    def _record_input_digests(self, *, node: NodeRun, digests: dict[str, str]) -> None:
        """Record the digest of each remote input fetched for *node*, by URI.

        The driver reports what it staged and the worker what it downloaded;
        a task can have both, so the record accumulates.
        """
        if not digests:
            return
        node.remote_input_digests.update(digests)
        self._annotate_task(
            node=node, fields={"remote_input_digests": dict(node.remote_input_digests)}
        )

    def _fold_remote_input_access(self, *, node: NodeRun, payload: Any) -> None:
        """Fold worker-reported input-access stats into provenance.

        Records FUSE mount cost, cache hits, and fallbacks for both remote
        and local (process-pool) workers, and surfaces a notice when a
        mount fell back to staging.
        """
        if isinstance(payload, dict) and isinstance(payload.get("remote_input_digests"), dict):
            self._record_input_digests(node=node, digests=payload["remote_input_digests"])
        if isinstance(payload, dict) and isinstance(payload.get("remote_input_access"), dict):
            access_stats = payload["remote_input_access"]
            self._annotate_task(node=node, fields={"remote_input_access": access_stats})
            fallback_notice = access_fallback_notice(access_stats)
            if fallback_notice is not None:
                self._emit_task_event(TaskNotice, node=node, message=fallback_notice)

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
        self._emit_task_event(
            TaskNotice,
            node=node,
            message=f"memory escalated to {escalated} GiB for attempt {node.attempt + 1}",
        )

    def _build_worker_payload(self, *, node: NodeRun) -> dict[str, Any]:
        """Encode task inputs into a transport payload for the process pool."""
        assert node.transport_path is not None
        assert node.execution_args is not None
        artifact_store = self._value_store(node=node)
        return {
            "args": {
                name: encode_value(
                    value, base_dir=node.transport_path, artifact_store=artifact_store
                )
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
            "env": node.task_def.env,
            "module": node.task_def.fn.__module__,
            "module_file": resolve_module_file(node.task_def.fn.__module__),
            "task_kind": node.task_def.kind,
            "binding_name": node.task_def.fn.__name__,
            "transport_dir": str(node.transport_path),
            # A pod downloads its ``direct`` inputs this many at a time.
            "staging_jobs": self._staging_jobs,
            # Workers re-import the workflow module, which re-runs its param()
            # calls; without this they would resolve to the declared defaults.
            "param_context": (
                self.param_context.to_payload() if self.param_context is not None else None
            ),
        }

    def _decode_worker_result(self, *, node: NodeRun, payload: dict[str, Any]) -> Any:
        """Decode a process-pool worker response."""
        if not payload["ok"]:
            raise _reconstruct_worker_error(payload["error"])

        encoding = payload.get("result_encoding")

        if encoding == "direct":
            # Process-pool path: Python object passed directly (no serialization).
            return payload["result"]

        assert node.transport_path is not None
        return decode_value(
            payload["result"],
            base_dir=node.transport_path,
            artifact_store=self._value_store(node=node),
        )

    def _value_store(self, *, node: NodeRun) -> RemoteArtifactStore | None:
        """Return the store large values go through for *node*, if not its transport dir.

        A remote worker cannot read the driver's transport directory, so a
        remote node's large values travel through the shared artifact store.
        """
        if node.executor_name is None:
            return None
        return self._remote_dispatch.artifact_store

    def _remove_tmp_paths(self, node: NodeRun) -> None:
        """Remove the attempt's ``tmp_dir`` scratch directories."""
        for path in node.tmp_paths:
            if path.exists():
                shutil.rmtree(path)
        node.tmp_paths = []

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
                raise TypeError(task_def.inferred_return_message(returned=repr(value)))
            value = self._inferred_return_value(node=node)
        # An unwritten Out[...] path is checked first: a task returning that
        # path would otherwise fail the generic return check, which names
        # `.return` rather than the parameter.
        if task_def.output_params:
            self._validator.validate_declared_outputs_written(
                task_def=task_def,
                resolved_args=node.execution_args or {},
            )
        coerced = self._validator.coerce_return_value(task_def=task_def, value=value)
        self._validator.validate_return_value(task_def=task_def, value=coerced)
        return coerced

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
        assert node.resolved_args is not None, (
            f"Out[...] parameter {name!r} of {node.task_def.name} was read before "
            "the task's arguments were resolved"
        )
        annotation = node.task_def.type_hints.get(name)
        return self._validator.coerce_annotated_value(
            annotation=annotation, value=node.resolved_args.get(name)
        )

    def _replay_runner_extras(self, *, node: NodeRun, cache_key: str) -> None:
        """Record against a cache hit what its runner recorded when it ran.

        A runner keeps such facts in ``node.runner_extras`` (a notebook's
        rendered page, say), which go into the cache entry; a hit records them
        again, so this run's record points at what the producing run made.
        """
        if self.run_dir is None:
            return
        cached = self._cache_store.load_extra_meta(cache_key=cache_key) or {}
        extras = cached.get("runner_extras")
        if isinstance(extras, dict) and extras:
            self._annotate_task(node=node, fields=extras)

    def build_graph(self, expr: Any) -> None:
        """Register the static task graph reachable from *expr*, with inferred edges.

        After this, the graph's unreachable calls are known, so a caller can report
        dropped calls before :meth:`validate_graph` fails on a consequence of
        one, such as a consumer of a dropped producer's ``Out[...]`` path.

        Parameters
        ----------
        expr : Any
            The flow's return value: an expression or a nested container of
            them.
        """
        self._root_template = expr
        self._root_dependency_ids = self._register(expr)

    def validate_graph(self) -> None:
        """Validate the built graph's import/env/input constraints."""
        self.graph.validate(validator=self._validator, placement=self.placement)

    def _try_prepare_cache_hit(self, *, node: NodeRun) -> bool:
        """Attempt to complete a node from cache during preparation.

        This fast path only runs once every remote input's identity is known;
        a node whose identities must be resolved is looked up after that.
        """
        if node.resolved_args is None or self._stager.needs_identity(node=node):
            return False
        return self._try_cache_hit(node=node)

    def _try_cache_hit(self, *, node: NodeRun) -> bool:
        """Complete *node* from the cache if it has an entry.

        Under ``--trust-mtimes`` the stat index is tried first, before any
        content hashing; otherwise, or when it misses, the lookup is by
        content. A task named by ``--rerun`` is never looked up: its key is
        computed, for the entry its run will save, and it misses.

        An entry holding a :class:`DynamicTemplate` replays it: the returned
        expressions are registered again, each child is prepared and looked
        up on its own, and the node completes as cached once they finish.
        """
        assert node.resolved_args is not None
        cache_lookup_started = time.perf_counter()
        if self.rerun_requested(node=node):
            self._node_cache.ensure_cache_key(node=node)
            hit = None
        else:
            hit = self._node_cache.lookup_by_stat(node=node) if self.trust_mtimes else None
            if hit is None:
                hit = self._node_cache.content_lookup(node=node)
        self._record_task_metadata(node=node, include_env_metadata=False)
        self._record_task_timing(
            node_id=node.node_id,
            phase="cache_lookup_seconds",
            started=cache_lookup_started,
        )
        if hit is None:
            return False
        self._record_cache_hit(node=node, cache_key=hit.cache_key)
        if isinstance(hit.value, DynamicTemplate):
            node.replaying_cache = True
            expanded = self._expand_dynamic(node=node, value=hit.value.template)
            assert expanded, "a dynamic template is only stored when it holds task expressions"
            return True

        node.result = hit.value
        node.state = "completed"
        self._emit_task_event(
            TaskCompleted,
            node=node,
            status="cached",
            cache_key=hit.cache_key,
            outputs=self._output_summary_for(
                node=node,
                value=hit.value,
                artifact_ids=self._cache_store.load_artifact_ids(cache_key=hit.cache_key),
            ),
        )
        return True

    def rerun_requested(self, *, node: NodeRun) -> bool:
        """Return whether ``--rerun`` names *node*, so its cache is not consulted."""
        return not self.rerun.isdisjoint(node.rerun_names)

    def _record_cache_hit(self, *, node: NodeRun, cache_key: str) -> None:
        """Record that *node* is served by the entry under *cache_key*."""
        if node.attempt == 0:
            node.attempt = 1

        # Counted here rather than projected from the event: every other
        # cache_entries column is written by the index on its own connection,
        # and a hit routed through the ledger's writer could land while
        # another process held the write lock for a save.
        self._cache_index.record_hit(cache_key)
        self._node_cache.propagate_known_digests(cache_key=cache_key)
        self._remove_tmp_paths(node)
        self._replay_runner_extras(node=node, cache_key=cache_key)
        self._emit_task_event(
            TaskCacheHit,
            node=node,
            cache_key=cache_key,
        )

        # Record stat-index entry so future --trust-mtimes runs can
        # find this cache key without content hashing.
        self._node_cache.record_stat_index_entry(node=node, cache_key=cache_key)

    def _complete_replayed_node(self, *, node: NodeRun, value: Any) -> None:
        """Complete a cached dynamic task once the children it replayed have finished.

        Its body did not run, so nothing is saved: the entry already holds the
        template, and each child saved its own result.
        """
        value = self._finalize_result_value(node=node, value=value)
        node.result = value
        node.state = "completed"
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        self._emit_task_event(
            TaskCompleted,
            node=node,
            status="cached",
            cache_key=node.cache_key,
            outputs=self._output_summary_for(
                node=node, value=value, artifact_ids=self._digests.known
            ),
        )

    def _record_task_metadata(
        self,
        *,
        node: NodeRun,
        include_env_metadata: bool = True,
    ) -> None:
        """Announce the task's resolved inputs, cache identity, and environment.

        Called at each point of an attempt where they may have changed —
        preparation, the cache probe after remote inputs are staged, a
        dynamic expansion — so ``TaskPlanned`` is emitted only when they did.
        """
        if self.event_bus is None:
            return
        planned = {
            "inputs": render_value(node.resolved_args or {}),
            "input_hashes": _input_hash_entries(node.input_hashes),
            "input_labels": dict(node.input_labels or {}),
            "cache_key": node.cache_key,
            "source_hash": node.task_def.cache_source_hash,
            "version": node.task_def.version,
            "env_hash": self._env_identity(env=node.task_def.env),
            "extra_source_hash": node.extra_source_hash,
            "dependency_ids": [task_id_for_node(dep) for dep in sorted(node.node.dependency_ids)],
            "dynamic_dependency_ids": [
                task_id_for_node(dep) for dep in sorted(node.dynamic_dependency_ids)
            ],
        }
        if planned != node.planned:
            node.planned = planned
            self._emit_task_event(TaskPlanned, node=node, **planned)
        if not include_env_metadata:
            return
        if node.task_def.env is None or self.backend is None:
            return

        fields = self.backend.provenance_fields(env=node.task_def.env)
        lock_path = self.backend.env_lock_path(env=node.task_def.env)
        if lock_path is not None and self.run_dir is not None:
            copied = self.run_dir.copy_env_lock(env_name=node.task_def.env, lock_path=lock_path)
            if copied is not None:
                fields["env_lock"] = copied
        self._annotate_task(node=node, fields=fields)

    def _record_measured_usage(self, *, node: NodeRun, measured: dict[str, Any]) -> None:
        """Fold one usage measurement into the node's running totals.

        A task may run several subprocesses and several attempts, so peaks
        take the max and CPU seconds accumulate.
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
                "memory": node.declared_memory,
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

    def _annotate_task(self, *, node: TaskInvocation, fields: dict[str, Any]) -> None:
        """Attach open-ended facts to a task node."""
        if not fields:
            return
        self._emit_task_event(
            TaskAnnotated,
            node=node,
            fields=fields,
        )

    def _resolved_display_label(self, *, node: NodeRun) -> str | None:
        """Return a prepared node's display label, given its resolved args.

        The label fixed at registration stands, except for a mapped branch
        whose fan-out values gave it no label part (each was an upstream
        result, say): that branch is named by the resolved value of its
        first label parameter, when one renders.
        """
        expr = node.node.expr
        if not expr.mapped or expr.display_label_parts or node.resolved_args is None:
            return node.display_label

        label_key = first_label_param_name(task_def=node.task_def)
        if label_key is None or label_key not in node.resolved_args:
            return node.display_label

        rendered = render_label_value(node.resolved_args[label_key])
        if rendered is None:
            return node.display_label

        # Through the graph, so the label is unique among every node's.
        return self.graph.relabel(
            current=node.label, wanted=f"{node.task_def.base_name}[{rendered}]"
        )

    def _output_summary_for(
        self,
        *,
        node: NodeRun,
        value: Any,
        artifact_ids: dict[str, str] | None,
    ) -> list[dict[str, Any]]:
        """Return a compact typed output summary for one task result.

        ``artifact_ids`` is the cache entry's output path to artifact id
        mapping, so each stored ``file``/``folder`` output names its artifact.
        """
        annotation = node.task_def.effective_return_annotation
        return output_summary(annotation, value, artifact_ids=artifact_ids)

    @property
    def _run_id(self) -> str:
        """Return the active run id, or a placeholder outside live runs."""
        return self.run_dir.run_id if self.run_dir is not None else "validation"

    def _emit_task_event(
        self, event_type: type[TaskEvent], *, node: TaskInvocation, **fields: Any
    ) -> None:
        """Emit a per-task event, filling in the fields every task event carries."""
        self._emit_event(
            event_type(
                run_id=self._run_id,
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                **fields,
            )
        )

    def _emit_event(self, event: object) -> None:
        """Emit a runtime event to the attached event bus, if any."""
        if self.event_bus is not None:
            self.event_bus.emit(event)

    def _run_driver_task(self, *, node: NodeRun) -> Any:
        """Run a driver-task wrapper on the scheduler process.

        For a kind with ``eager_source_hash`` the body was already evaluated
        in ``_prepare_node`` to fold its source file's hash into the cache key.
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
        """Advance a task after its body has returned."""
        if self._failure is not None and (
            isinstance(completed_value, ExecutionDirective)
            or contains_dynamic_expression(completed_value)
        ):
            self._remove_tmp_paths(node)
            node.state = "failed"
            return

        kind = task_kind(node.task_def)
        if isinstance(completed_value, ExecutionDirective):
            if kind.directive is None:
                raise TypeError(
                    f"{node.task_def.base_name} returned {type(completed_value).__name__}, but the "
                    f"task is declared with kind={kind.name!r}. "
                    f"{self._kind_suggestion(type(completed_value))}"
                )
            if not isinstance(completed_value, kind.directive):
                raise TypeError(
                    f"{node.task_def.base_name} is declared with kind={kind.name!r} and must "
                    f"return {kind.call_form}, not {type(completed_value).__name__}. "
                    f"{self._kind_suggestion(type(completed_value))}"
                )
            # A task runs its own kind's directive on its own kind's runner, so
            # the capabilities checked against the kind are those of what runs.
            assert self._executors is not None
            runner = self._runner_for(kind)
            node.state = "running_shell"
            future = self._executors.shell.submit(runner.run, node=node, directive=completed_value)
            self._track(future, node=node, on_done=self._on_runner_done)
            return

        if kind.directive is None:
            self._validator.validate_process_safe_value(
                value=completed_value,
                label=f"{node.task_def.base_name}.return",
            )
        if self._expand_dynamic(node=node, value=completed_value):
            return
        if kind.directive is not None:
            raise TypeError(
                f"{node.task_def.base_name} is declared with kind={kind.name!r} and must return "
                f"{kind.call_form or 'an execution directive'} or dynamic task expressions."
            )

        self._complete_node(node=node, value=completed_value)

    def _expand_dynamic(self, *, node: NodeRun, value: Any) -> bool:
        """Register the task expressions *value* holds, and wait on any it adds.

        Returns
        -------
        bool
            ``True`` when *node* now waits on new tasks, ``False`` when *value*
            added none.
        """
        dynamic_dependencies = self._register(value, expanding_node_id=node.node_id)
        if not dynamic_dependencies:
            return False
        node.state = "waiting_dynamic"
        node.dynamic_template = value
        node.dynamic_dependency_ids = dynamic_dependencies
        self._record_task_metadata(node=node)
        self._emit_event(
            GraphExpanded(
                run_id=self._run_id,
                parent_task_id=task_id_for_node(node.node_id),
                new_node_ids=[task_id_for_node(dep_id) for dep_id in sorted(dynamic_dependencies)],
            )
        )
        return True

    def _runner_for(self, kind: TaskKind) -> TaskRunner:
        """Return the runner for *kind*, building it on first use."""
        runner = self._runners.get(kind.name)
        if runner is None:
            assert kind.runner is not None, f"task kind {kind.name!r} has no runner"
            runner = kind.runner(self._runner_context)
            self._runners[kind.name] = runner
        return runner

    @staticmethod
    def _kind_suggestion(directive_type: type[ExecutionDirective]) -> str:
        """Name the kind that returns *directive_type*, for a mismatch error."""
        kind = plugin_registry().kind_for_directive(directive_type)
        if kind is None:
            return "No installed task kind returns it."
        return f'Use @task("{kind.name}").'


def _input_hash_entries(input_hashes: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return one entry per hashed input: its parameter name and key component."""
    return [
        {"param": str(param), **(value if isinstance(value, dict) else {"digest": value})}
        for param, value in (input_hashes or {}).items()
    ]


def _classify_access_method(*, value: Any, default: str) -> str:
    """Return the access mode a resolved-args tree asks for, or ``"hybrid"``.

    Walks the value recursively, reading the ``access`` hint on each
    :class:`RemoteRef` leaf; a ref without one takes *default*, the task's or
    the config's mode.
    """
    from ginkgo.core.remote import RemoteRef
    from ginkgo.remote.access.protocol import is_fuse_policy

    seen: set[str] = set()

    def walk(item: Any) -> None:
        if isinstance(item, RemoteRef):
            seen.add(item.access or default)
            return
        if isinstance(item, dict):
            for v in item.values():
                walk(v)
            return
        if isinstance(item, (list, tuple)):
            for v in item:
                walk(v)

    walk(value)
    if len(seen) > 1:
        return "hybrid"
    mode = seen.pop() if seen else default
    return "fuse" if is_fuse_policy(mode) else mode
