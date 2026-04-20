"""Concurrent evaluator for Ginkgo expressions."""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import builtins
from contextlib import ExitStack, suppress
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from multiprocessing import Manager, get_context
from pathlib import Path
from queue import Empty
from threading import Event, Thread
from typing import Any

from ginkgo.core.asset import AssetRef, AssetVersion
from ginkgo.core.expr import Expr, ExprList, OutputIndex
from ginkgo.core.notebook import NotebookExpr
from ginkgo.core.script import ScriptExpr
from ginkgo.core.shell import ShellExpr
from ginkgo.core.subworkflow import SubWorkflowExpr
from ginkgo.core.task import TaskDef
from ginkgo.core.types import tmp_dir
from ginkgo.envs.container import is_container_env
from ginkgo.runtime.backend import TaskBackend
from ginkgo.runtime.remote_executor import (
    RemoteDispatchStats,
    RemoteExecutor,
    RemoteJobHandle,
    RemoteJobState,
)
from ginkgo.runtime.artifacts.asset_registration import AssetRegistrar, asset_index_for
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry
from ginkgo.runtime.artifacts.output_index import output_summary
from ginkgo.runtime.artifacts.wrapper_loaders import load_from_ref as load_wrapped_ref
from ginkgo.runtime.caching.cache import MISSING, CacheStore
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.runtime.caching.materialization_log import MaterializationLog
from ginkgo.runtime.events import (
    EnvPrepareCompleted,
    EnvPrepareStarted,
    EventBus,
    GraphExpanded,
    GraphNodeRegistered,
    TaskCacheHit,
    TaskCacheMiss,
    TaskCompleted,
    TaskFailed,
    TaskLog,
    TaskNotice,
    TaskReady,
    TaskRetrying,
    TaskRunning,
    TaskStaging,
    TaskStarted,
)
from ginkgo.runtime.module_loader import resolve_module_file
from ginkgo.runtime.caching.provenance import RunProvenanceRecorder
from ginkgo.runtime.profiling import ProfileRecorder
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
from ginkgo.runtime.task_runners.shell import (
    ShellRunner,
    SignalMonitor,
    classify_failure,
    sanitize_exception,
)
from ginkgo.runtime.task_runners.subworkflow import SubworkflowRunner
from ginkgo.runtime.task_validation import TaskValidator, contains_dynamic_expression
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_value
from ginkgo.runtime.worker import _task_log_context, run_task


class CycleError(RuntimeError):
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
    backend: TaskBackend | None = None,
    provenance: RunProvenanceRecorder | None = None,
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
    backend : TaskBackend | None
        Execution backend for environment-isolated tasks.
    event_bus : EventBus | None
        Optional event bus to receive lifecycle events. Useful for tests
        and ad-hoc programmatic callers that want to observe task progress.

    Returns
    -------
    Any
        The concrete result of evaluating the input.
    """
    return _ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        backend=backend,
        provenance=provenance,
        secret_resolver=secret_resolver,
        event_bus=event_bus,
    ).evaluate(expr)


@dataclass(kw_only=True)
class _TaskNode:
    """Internal task node tracked by the concurrent scheduler."""

    node_id: int
    expr: Expr
    dependency_ids: set[int]
    state: str = "pending"
    resolved_args: dict[str, Any] | None = None
    execution_args: dict[str, Any] | None = None
    cache_key: str | None = None
    input_hashes: dict[str, Any] | None = None
    threads: int = 1
    memory_gb: int = 0
    gpu: int = 0
    concurrency_group: str | None = None
    concurrency_group_limit: int | None = None
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
    driver_sentinel: Any = None
    extra_source_hash: str | None = None
    asset_versions: list[AssetVersion] = field(default_factory=list)
    notebook_extras: dict[str, Any] | None = None
    remote_job_id: str | None = None

    @property
    def task_def(self) -> TaskDef:
        """Return the task definition for the node."""
        return self.expr.task_def


@dataclass(kw_only=True)
class _ConcurrentEvaluator:
    """Concurrent evaluator with dependency tracking and cache integration."""

    jobs: int | None = None
    cores: int | None = None
    memory: int | None = None
    backend: TaskBackend | None = None
    remote_executor: RemoteExecutor | None = None
    provenance: RunProvenanceRecorder | None = None
    secret_resolver: SecretResolver | None = None
    event_bus: EventBus | None = None
    trust_workspace: bool = False
    profiler: ProfileRecorder | None = None
    _cache_store: CacheStore = field(init=False, repr=False)
    _asset_store: AssetStore = field(init=False, repr=False)
    _nodes: dict[int, _TaskNode] = field(default_factory=dict, init=False, repr=False)
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
    _python_executor: ProcessPoolExecutor | ThreadPoolExecutor | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _shell_executor: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _staging_executor: ThreadPoolExecutor | None = field(default=None, init=False, repr=False)
    _log_event_queue: Any = field(default=None, init=False, repr=False)
    _log_drain_stop: Event | None = field(default=None, init=False, repr=False)
    _log_drain_thread: Thread | None = field(default=None, init=False, repr=False)
    _task_log_sequences: dict[tuple[int, str], int] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _staging_jobs: int = field(default=0, init=False, repr=False)
    code_bundle_config: dict[str, Any] | None = None
    _remote_watcher_executor: ThreadPoolExecutor | None = field(
        default=None, init=False, repr=False
    )
    _remote_handles: dict[int, RemoteJobHandle] = field(
        default_factory=dict, init=False, repr=False
    )
    _code_bundle_meta: dict[str, str] | None = field(default=None, init=False, repr=False)
    _known_digests: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    _remote_artifact_store: Any = field(default=None, init=False, repr=False)
    _remote_artifact_store_checked: bool = field(default=False, init=False, repr=False)
    _remote_published_artifacts: set[str] = field(default_factory=set, init=False, repr=False)
    _remote_stats: RemoteDispatchStats = field(
        default_factory=RemoteDispatchStats, init=False, repr=False
    )
    _staging_cache_path: Path = field(init=False, repr=False)

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

        self._hash_memo = HashMemo()
        self._staging_cache_path = Path.cwd() / ".ginkgo" / "remote-staged.json"
        artifacts_root = Path.cwd() / ".ginkgo" / "artifacts"
        self._materialization_log = MaterializationLog(
            path=artifacts_root / "materializations.json"
        )
        self._cache_store = CacheStore(
            backend=self.backend,
            publisher=load_remote_publisher(),
            hash_memo=self._hash_memo,
            materialization_log=self._materialization_log,
            trust_workspace=self.trust_workspace,
        )
        self._asset_store = AssetStore(root=self._cache_store._root.parent / "assets")
        self._staging_jobs = resolve_staging_jobs(jobs=self.jobs)

        # Helper runners. Constructed once per evaluation so unit tests can
        # exercise them in isolation and substitute fakes.
        self._validator = TaskValidator(
            backend=self.backend,
            secret_resolver=self.secret_resolver,
        )
        self._shell_runner = ShellRunner(
            backend=self.backend,
            validator=self._validator,
            log_emitter_factory=self._task_log_emitter,
        )
        self._notebook_runner = NotebookRunner(
            backend=self.backend,
            shell_runner=self._shell_runner,
            validator=self._validator,
            cache_store=self._cache_store,
            provenance=self.provenance,
            notice_emitter=self._emit_notebook_notice,
            runtime_root_factory=self._notebook_runtime_root,
        )
        self._subworkflow_runner = SubworkflowRunner(
            shell_runner=self._shell_runner,
            run_id_provider=lambda: self._run_id or "",
            runs_root=(
                self.provenance.root_dir
                if self.provenance is not None
                else Path.cwd() / ".ginkgo" / "runs"
            ),
        )
        self._stager = RemoteStager(timing_recorder=self._record_task_timing)
        self._live_payloads = LivePayloadRegistry()
        self._asset_registrar = AssetRegistrar(
            cache_store=self._cache_store,
            asset_store=self._asset_store,
            run_id_provider=lambda: self._run_id,
            live_payloads=self._live_payloads,
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
            try:
                python_executor = stack.enter_context(
                    ProcessPoolExecutor(
                        max_workers=self.jobs,
                        mp_context=get_context("spawn"),
                    )
                )
            except PermissionError:
                python_executor = stack.enter_context(ThreadPoolExecutor(max_workers=self.jobs))
            shell_executor = stack.enter_context(ThreadPoolExecutor(max_workers=self.jobs))
            staging_executor = stack.enter_context(
                ThreadPoolExecutor(max_workers=self._staging_jobs)
            )
            log_manager = stack.enter_context(Manager())
            signals = stack.enter_context(SignalMonitor())
            self._python_executor = python_executor
            self._shell_executor = shell_executor
            self._staging_executor = staging_executor
            self._start_log_drain(queue=log_manager.Queue())
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
                                python_executor=python_executor,
                                shell_executor=shell_executor,
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
                self._stop_log_drain()
                self._python_executor = None
                self._shell_executor = None
                self._materialization_log.save()
                self._cache_store.save_stat_index()
                self._save_staging_cache()

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

        self._nodes[node_id] = _TaskNode(
            node_id=node_id,
            expr=expr,
            dependency_ids=dependency_ids,
            concurrency_group=expr.concurrency_group,
            concurrency_group_limit=expr.concurrency_group_limit,
        )
        self._expr_nodes[expr_id] = node_id
        self._emit_event(
            GraphNodeRegistered(
                run_id=self._run_id,
                task_id=_task_id_for_node(node_id),
                task_name=expr.task_def.name,
                kind=expr.task_def.kind,
                env=expr.task_def.env,
                dependency_ids=[_task_id_for_node(dep_id) for dep_id in sorted(dependency_ids)],
            )
        )
        if self.provenance is not None:
            stdout_path, stderr_path = self.provenance.ensure_task(
                node_id=node_id,
                task_name=expr.task_def.name,
                env=expr.task_def.env,
                kind=expr.task_def.kind,
                execution_mode=expr.task_def.execution_mode,
                retries=expr.task_def.retries,
            )
            self._nodes[node_id].stdout_path = stdout_path
            self._nodes[node_id].stderr_path = stderr_path
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

    def _prepare_node(self, node: _TaskNode) -> None:
        """Resolve non-ephemeral inputs, then either cache-hit or ready the task."""
        prepare_started = time.perf_counter()
        resolved_args = self._resolve_task_args(
            expr=node.expr,
            task_def=node.task_def,
            node=node,
            include_tmp_dirs=False,
            stage_remote_refs=False,
        )
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=resolved_args)
        self._validator.validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )

        # For notebook/script tasks, eagerly evaluate the body to capture the
        # source hash of the underlying file and fold it into the cache key.
        extra_source_hash: str | None = None
        if node.task_def.kind in {"notebook", "script"}:
            sentinel = node.task_def.fn(**resolved_args)
            node.driver_sentinel = sentinel
            extra_source_hash = sentinel.source_hash

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

        node.threads = self._task_threads(node.task_def)
        node.memory_gb = self._task_memory_gb(node.task_def, resolved_args)
        node.gpu = node.task_def.gpu
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
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                resources={"cores": node.threads, "memory_gb": node.memory_gb},
            )
        )

    def _prepare_task_environment(self, *, node: _TaskNode) -> None:
        """Materialize any external execution environment required by a task."""
        if node.task_def.env is None or self.backend is None:
            return

        self._emit_event(
            EnvPrepareStarted(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                env=node.task_def.env,
            )
        )
        env_prepare_started = time.perf_counter()
        self.backend.prepare(env=node.task_def.env)
        self._record_task_timing(
            node_id=node.node_id,
            phase="env_prepare_seconds",
            started=env_prepare_started,
        )
        self._emit_event(
            EnvPrepareCompleted(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
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
        available_group_slots = self._available_group_slots(ready_nodes=ready_nodes)
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(
                    node_id=node.node_id,
                    threads=node.threads,
                    memory_gb=node.memory_gb,
                    priority=node.task_def.priority,
                    concurrency_group=node.concurrency_group,
                )
                for node in ready_nodes
            ],
            jobs=available_jobs,
            cores=available_cores,
            memory=available_memory,
            available_group_slots=available_group_slots,
        )

        for node_id in selected:
            node = self._nodes[node_id]
            node.attempt += 1
            node.resolved_args = self._resolve_task_args(
                expr=node.expr,
                task_def=node.task_def,
                node=node,
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
                        task_id=_task_id_for_node(node.node_id),
                        task_name=node.task_def.name,
                        attempt=node.attempt,
                        display_label=node.display_label,
                        remote_input_count=remote_input_count,
                        access_method=access_method,
                    )
                )
                assert self._staging_executor is not None
                future = self._staging_executor.submit(self._stager.stage_task_inputs, node=node)
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
                self._remote_handles.pop(node.node_id, None)
                continue

            # Capture remote job id for provenance before processing result.
            if phase == "remote":
                handle = self._remote_handles.get(node.node_id)
                if handle is not None:
                    node.remote_job_id = handle.job_id

            try:
                completed_value = future.result()
            except BaseException as exc:
                self._remote_handles.pop(node.node_id, None)
                self._handle_task_exception(node=node, exc=exc)
                continue

            try:
                if phase == "staging":
                    self._handle_completed_staging_phase(
                        node=node, completed_value=completed_value
                    )
                elif phase in ("python", "remote"):
                    remote_handle = self._remote_handles.pop(node.node_id, None)
                    if phase == "remote" and remote_handle is not None:
                        self._capture_remote_logs(node=node, handle=remote_handle)
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
        node: _TaskNode,
        completed_value: Any,
        remote_job_id: str | None = None,
    ) -> None:
        """Handle the result returned from a Python worker."""
        completed_value = self._decode_worker_result(node=node, payload=completed_value)
        node.remote_job_id = remote_job_id
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_staging_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Start task execution after remote inputs have been staged locally."""
        if not isinstance(completed_value, dict):
            raise TypeError("Expected staged task arguments from staging phase")

        node.resolved_args = completed_value
        self._validator.validate_inputs(task_def=node.task_def, resolved_args=node.resolved_args)
        assert self._python_executor is not None
        assert self._shell_executor is not None
        self._start_task_execution(
            node=node,
            python_executor=self._python_executor,
            shell_executor=self._shell_executor,
        )

    def _handle_completed_driver_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Handle the result returned from a driver-executed task wrapper."""
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_shell_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Handle the result produced by the shell executor."""
        final_value = self._finalize_result_value(node=node, value=completed_value)
        self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)

    def _handle_task_exception(self, *, node: _TaskNode, exc: BaseException) -> None:
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
        self._record_task_failure(node=node, exc=sanitized_exc)
        self._emit_event(
            TaskFailed(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                exit_code=getattr(sanitized_exc, "exit_code", None),
                failure=classify_failure(exc=sanitized_exc),
                remote_job_id=node.remote_job_id,
            )
        )

    def _should_retry(self, *, node: _TaskNode, exc: BaseException) -> bool:
        """Return whether the current failed attempt should be retried."""
        if node.attempt > node.task_def.retries:
            return False
        return node.task_def.should_retry_exception(exc=exc)

    def _schedule_retry(self, *, node: _TaskNode, exc: BaseException) -> None:
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
        node.tmp_paths = []
        node.transport_path = None
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()
        node.secret_values = ()
        node.extra_source_hash = None
        node.asset_versions = []

        retries_remaining = node.task_def.retries - node.attempt
        if self.provenance is not None:
            self.provenance.mark_retrying(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                exc=exc,
                attempt=node.attempt,
                retries_remaining=retries_remaining,
            )
        self._emit_event(
            TaskRetrying(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                retries_remaining=retries_remaining,
                failure=classify_failure(exc=exc),
                delay_seconds=delay,
            )
        )

    def _complete_node(self, *, node: _TaskNode, value: Any, tmp_paths: list[Path]) -> None:
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
            extra_meta=extra_meta,
        )

        # Propagate output digests so downstream tasks can skip re-hashing.
        for path_str, artifact_id in artifact_ids.items():
            resolved_key = str(Path(path_str).resolve())
            self._known_digests[resolved_key] = artifact_id

        # Record stat-index for future --trust-workspace runs.
        self._record_stat_index_entry(node=node, cache_key=node.cache_key)

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
        if self.provenance is not None:
            self.provenance.mark_succeeded(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                value=value,
                outputs=self._output_summary_for(node=node, value=value),
                assets=self._asset_index_for(value=value),
            )
        self._emit_event(
            TaskCompleted(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                status="success",
                cache_key=node.cache_key,
                outputs=self._output_summary_for(node=node, value=value),
                remote_job_id=node.remote_job_id,
            )
        )
        if self.provenance is not None and node.remote_job_id is not None:
            self.provenance.update_task_extra(
                node_id=node.node_id,
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
        node: _TaskNode | None = None,
        include_tmp_dirs: bool,
        stage_remote_refs: bool = True,
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
                resolved_args[name] = self._rehydrate_wrapped_refs(value=materialised)
                continue

            if name == "threads":
                # Inject the decorator-declared thread count so user code can
                # use it for shell command interpolation or in-process work.
                resolved_args[name] = task_def.threads
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

    def _resolve_execution_args(self, *, node: _TaskNode) -> dict[str, Any]:
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
        """
        if isinstance(value, AssetRef):
            if value.kind in {"table", "array", "text", "model"}:
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

    def _dependencies_complete(self, dependency_ids: set[int]) -> bool:
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
        self._cancel_remote_handles()
        self._shell_runner.terminate_all()
        self._shutdown_staging_executor()
        self._shutdown_shell_executor()
        self._shutdown_python_executor()
        self._shutdown_remote_watcher_executor()

    def _cancel_remote_handles(self) -> None:
        """Cancel all in-flight remote job handles."""
        for handle in self._remote_handles.values():
            with suppress(Exception):
                handle.cancel()
        self._remote_handles.clear()

    def _shutdown_remote_watcher_executor(self) -> None:
        """Shut down the remote watcher executor."""
        if self._remote_watcher_executor is None:
            return
        with suppress(Exception):
            self._remote_watcher_executor.shutdown(wait=False, cancel_futures=True)

    def _shutdown_staging_executor(self) -> None:
        """Shut down the staging executor without waiting for new tasks."""
        if self._staging_executor is None:
            return
        with suppress(Exception):
            self._staging_executor.shutdown(wait=False, cancel_futures=True)

    def _shutdown_shell_executor(self) -> None:
        """Shut down the shell executor without waiting for new tasks."""
        if self._shell_executor is None:
            return
        with suppress(Exception):
            self._shell_executor.shutdown(wait=False, cancel_futures=True)

    def _shutdown_python_executor(self) -> None:
        """Shut down the Python executor and terminate worker processes."""
        if self._python_executor is None:
            return

        executor = self._python_executor
        with suppress(Exception):
            executor.shutdown(wait=False, cancel_futures=True)

        if isinstance(executor, ProcessPoolExecutor):
            self._terminate_process_pool_workers(executor=executor)

    def _terminate_process_pool_workers(self, *, executor: ProcessPoolExecutor) -> None:
        """Terminate active process-pool workers using the executor's process table."""
        processes = getattr(executor, "_processes", None)
        if not isinstance(processes, dict):
            return

        for process in list(processes.values()):
            with suppress(Exception):
                if process.is_alive():
                    process.terminate()

        for process in list(processes.values()):
            with suppress(Exception):
                process.join(timeout=0.2)

        for process in list(processes.values()):
            with suppress(Exception):
                if process.is_alive():
                    process.kill()

    def _start_log_drain(self, *, queue: Any) -> None:
        """Start draining worker log chunks from the multiprocessing queue."""
        self._log_event_queue = queue
        self._log_drain_stop = Event()
        self._log_drain_thread = Thread(
            target=self._drain_log_events,
            name="ginkgo-log-drain",
            daemon=True,
        )
        self._log_drain_thread.start()

    def _stop_log_drain(self) -> None:
        """Stop the worker log drain thread."""
        if self._log_drain_stop is not None:
            self._log_drain_stop.set()
        if self._log_drain_thread is not None:
            self._log_drain_thread.join(timeout=1.0)
        self._log_event_queue = None
        self._log_drain_stop = None
        self._log_drain_thread = None

    def _drain_log_events(self) -> None:
        """Convert queued worker log chunks into runtime events."""
        if self._log_event_queue is None or self._log_drain_stop is None:
            return

        while True:
            try:
                payload = self._log_event_queue.get(timeout=0.1)
            except Empty:
                if self._log_drain_stop.is_set():
                    return
                continue
            except Exception:
                return

            chunk = payload.get("chunk")
            stream = payload.get("stream")
            task_id = payload.get("task_id")
            if (
                not isinstance(chunk, str)
                or not isinstance(stream, str)
                or not isinstance(task_id, str)
                or not chunk
            ):
                continue
            node_id = int(task_id.split("_")[-1])
            sequence_key = (node_id, stream)
            sequence = self._task_log_sequences.get(sequence_key, 0) + 1
            self._task_log_sequences[sequence_key] = sequence
            self._emit_event(
                TaskLog(
                    run_id=str(payload.get("run_id") or self._run_id),
                    task_id=task_id,
                    task_name=str(payload.get("task_name") or ""),
                    attempt=int(payload.get("attempt") or 0),
                    display_label=payload.get("display_label"),
                    stream=stream,
                    chunk=chunk,
                    sequence=sequence,
                )
            )

    def _running_cores(self) -> int:
        """Return the core footprint of currently running tasks."""
        return sum(self._nodes[node_id].threads for node_id, _ in self._running_futures.values())

    def _available_group_slots(self, *, ready_nodes: list[_TaskNode]) -> dict[str, int]:
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

    def _running_memory_gb(self) -> int:
        """Return the declared memory footprint of currently running tasks."""
        return sum(self._nodes[node_id].memory_gb for node_id, _ in self._running_futures.values())

    def _start_task_execution(
        self,
        *,
        node: _TaskNode,
        python_executor: ProcessPoolExecutor | ThreadPoolExecutor,
        shell_executor: ThreadPoolExecutor,
    ) -> None:
        """Launch a task after its inputs have been staged locally."""
        assert node.resolved_args is not None

        # Fast path: in --trust-workspace mode, try a stat-based index lookup
        # before computing content-addressed cache keys.
        if self.trust_workspace and self._try_stat_index_hit(node=node):
            return

        if self._try_content_cache_hit(node=node):
            return

        self._emit_event(
            TaskCacheMiss(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
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
        # Determine execution backend for events/provenance.
        task_is_remote = node.task_def.remote or node.gpu > 0
        execution_backend = (
            "remote" if (task_is_remote and self.remote_executor is not None) else "local"
        )

        self._emit_event(
            TaskStarted(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                kind=node.task_def.kind,
                env=node.task_def.env,
                resources={
                    "cores": node.threads,
                    "memory_gb": node.memory_gb,
                    "max_attempts": node.task_def.retries + 1,
                },
                execution_backend=execution_backend,
            )
        )
        if self.provenance is not None:
            self.provenance.mark_running(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                attempt=node.attempt,
                retries=node.task_def.retries,
                execution_backend=execution_backend,
            )

        if node.task_def.kind in {"notebook", "script", "shell"}:
            future = shell_executor.submit(
                self._run_driver_task,
                node=node,
            )
            self._running_futures[future] = (node.node_id, "driver")
            return

        # Remote dispatch: submit to remote executor when the task opts in
        # (via gpu > 0 or remote=True) and an executor is configured.
        if task_is_remote and self.remote_executor is not None:
            node.transport_path = Path(
                tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-")
            )
            self._ensure_code_bundle()
            self._ensure_remote_artifact_store()
            payload = self._build_worker_payload(node=node)
            payload["resources"] = {
                "threads": node.threads,
                "memory_gb": node.memory_gb,
                "gpu": node.gpu,
            }
            if self._code_bundle_meta is not None:
                payload["code_bundle"] = self._code_bundle_meta
            if self._remote_artifact_store is not None:
                from ginkgo.runtime.artifacts.remote_arg_transfer import stage_args_for_remote

                payload["args"] = stage_args_for_remote(
                    args=payload["args"],
                    type_hints=node.task_def.type_hints,
                    remote_store=self._remote_artifact_store,
                    known_digests=self._known_digests,
                    published_artifacts=self._remote_published_artifacts,
                )
                payload["remote_artifact_store"] = {
                    "scheme": self._remote_artifact_store.scheme,
                    "bucket": self._remote_artifact_store.bucket,
                    "prefix": self._remote_artifact_store.prefix,
                }
            handle = self.remote_executor.submit(attempt=payload)
            self._remote_stats.record_submit()
            self._remote_handles[node.node_id] = handle
            if self._remote_watcher_executor is None:
                self._remote_watcher_executor = ThreadPoolExecutor(
                    max_workers=self.jobs or 8,
                    thread_name_prefix="ginkgo-remote-watcher",
                )
            future = self._remote_watcher_executor.submit(self._poll_remote_job, handle, node=node)
            self._running_futures[future] = (node.node_id, "remote")
            return

        node.transport_path = Path(tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-"))
        payload = self._build_worker_payload(node=node)
        future = python_executor.submit(run_task, payload)
        self._running_futures[future] = (node.node_id, "python")

    def _poll_remote_job(self, handle: RemoteJobHandle, *, node: _TaskNode) -> dict[str, Any]:
        """Poll a remote job handle until it reaches a terminal state.

        Called on a watcher thread — blocks until the remote job finishes.
        Returns the worker result payload for consumption by the evaluator
        loop (same shape as a local ``run_task`` return value).
        """
        poll_interval = 5.0
        max_interval = 30.0
        emitted_running = False
        t_submit = time.monotonic()
        t_running: float | None = None
        while True:
            state = handle.state()
            if state.is_terminal:
                break
            if not emitted_running and state == RemoteJobState.RUNNING:
                emitted_running = True
                t_running = time.monotonic()
                self._emit_event(
                    TaskRunning(
                        run_id=self._run_id,
                        task_id=_task_id_for_node(node.node_id),
                        task_name=node.task_def.name,
                        attempt=node.attempt,
                        display_label=node.display_label,
                        remote_job_id=handle.job_id,
                    )
                )
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, max_interval)

        t_done = time.monotonic()
        pending_s = (t_running or t_done) - t_submit
        running_s = t_done - t_running if t_running else 0.0
        self._remote_stats.record_terminal(state=state)
        self._remote_stats.record_phase_time(pending=pending_s, running=running_s)

        result = handle.result()

        if result.state == RemoteJobState.CANCELLED:
            raise KeyboardInterrupt("Remote job was cancelled")

        if result.state == RemoteJobState.FAILED and not result.payload:
            raise RuntimeError(
                f"Remote job {handle.job_id} failed"
                + (f" (exit code {result.exit_code})" if result.exit_code is not None else "")
                + (f"\n{result.logs}" if result.logs else "")
            )

        payload = result.payload
        if (
            self._remote_artifact_store is not None
            and isinstance(payload, dict)
            and payload.get("ok")
            and payload.get("result_encoding") == "encoded"
            and "result" in payload
        ):
            from ginkgo.runtime.artifacts.remote_arg_transfer import hydrate_result_from_remote

            scratch_dir = self._remote_artifact_store.local._root / "remote-outputs"
            payload["result"] = hydrate_result_from_remote(
                result=payload["result"],
                remote_store=self._remote_artifact_store,
                scratch_dir=scratch_dir,
            )

        # Fold remote input-access stats (FUSE mount cost, cache hits, fallbacks)
        # into provenance when the worker reported them.
        if (
            isinstance(payload, dict)
            and isinstance(payload.get("remote_input_access"), dict)
            and self.provenance is not None
        ):
            access_stats = payload["remote_input_access"]
            self.provenance.update_task_extra(
                node_id=node.node_id,
                remote_input_access=access_stats,
            )
            self._warn_on_access_fallback(node=node, access_stats=access_stats)

        return payload

    def _warn_on_access_fallback(
        self,
        *,
        node: _TaskNode,
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
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=f"FUSE access fell back to staging: {reason}",
            )
        )

    def _capture_remote_logs(self, *, node: _TaskNode, handle: RemoteJobHandle) -> None:
        """Fetch pod logs and write them to the standard task log paths."""
        try:
            logs = handle.logs_tail(lines=10000)
        except Exception:
            return
        if not logs:
            return

        # Remote workers merge stdout/stderr into one stream — write to both
        # paths so users find tracebacks where they expect them.
        for log_path in (node.stdout_path, node.stderr_path):
            if log_path is not None:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(logs, encoding="utf-8")

    def _load_staging_cache(self) -> None:
        """Restore persisted staging state from ``.ginkgo/remote-staged.json``."""
        from ginkgo.runtime.artifacts.remote_arg_transfer import load_staging_cache

        digests, published = load_staging_cache(cache_path=self._staging_cache_path)
        self._known_digests.update(digests)
        self._remote_published_artifacts.update(published)

    def _save_staging_cache(self) -> None:
        """Persist staging state so the next run skips re-hashing unchanged inputs."""
        if not self._known_digests and not self._remote_published_artifacts:
            return
        from ginkgo.runtime.artifacts.remote_arg_transfer import save_staging_cache

        save_staging_cache(
            cache_path=self._staging_cache_path,
            known_digests=self._known_digests,
            published_artifacts=self._remote_published_artifacts,
        )

    def _ensure_code_bundle(self) -> None:
        """Create and publish the code bundle on first remote dispatch.

        Reads ``code_bundle_config`` (from ``[remote.k8s.code]``) to decide
        whether to sync workflow code to the remote backend. The bundle is
        created once per evaluator run and reused for all remote tasks.
        """
        if self._code_bundle_meta is not None:
            return
        if self.code_bundle_config is None:
            return
        mode = self.code_bundle_config.get("mode", "baked")
        if mode != "sync":
            return

        package = self.code_bundle_config.get("package")
        if not package:
            raise ValueError(
                "Code-sync mode requires [remote.k8s.code] package to be set "
                'in ginkgo.toml (e.g. package = "my_workflow")'
            )

        from ginkgo.remote.code_bundle import create_code_bundle, publish_code_bundle
        from ginkgo.remote.resolve import resolve_backend
        from ginkgo.core.remote import _parse_uri

        package_path = Path.cwd() / package
        if not package_path.is_dir():
            raise FileNotFoundError(f"Code-sync package directory not found: {package_path}")

        # Determine remote storage from [remote.artifacts] config.
        from ginkgo.config import load_runtime_config

        config = load_runtime_config(project_root=Path.cwd())
        artifacts_config = config.get("remote", {}).get("artifacts", {})
        store_uri = artifacts_config.get("store") if isinstance(artifacts_config, dict) else None
        if store_uri is None:
            raise ValueError(
                "Code-sync mode requires [remote.artifacts] store to be configured in ginkgo.toml"
            )

        parsed = _parse_uri(store_uri)
        backend = resolve_backend(parsed["scheme"])
        prefix = parsed["key"]
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        extra_excludes = self.code_bundle_config.get("exclude")
        if isinstance(extra_excludes, str):
            extra_excludes = [extra_excludes]
        bundle_path, digest = create_code_bundle(
            package_path=package_path,
            extra_excludes=extra_excludes,
        )
        try:
            remote_key = publish_code_bundle(
                backend=backend,
                bucket=parsed["bucket"],
                prefix=prefix,
                bundle_path=bundle_path,
                digest=digest,
            )
        finally:
            Path(bundle_path).unlink(missing_ok=True)

        self._code_bundle_meta = {
            "scheme": parsed["scheme"],
            "bucket": parsed["bucket"],
            "key": remote_key,
            "digest": digest,
            "package": package,
            "package_parent": str(package_path.parent.resolve()),
        }

    def _ensure_remote_artifact_store(self) -> None:
        """Lazily construct a ``RemoteArtifactStore`` from project config.

        Uses the ``[remote.artifacts]`` store URI, wrapping the local
        artifact store already owned by the cache. Called just before
        dispatching a remote task so that ``file`` / ``folder`` inputs
        can be uploaded to the shared object store and hydrated inside
        the worker pod.
        """
        if self._remote_artifact_store_checked:
            return
        self._remote_artifact_store_checked = True
        self._load_staging_cache()
        from ginkgo.runtime.artifacts.remote_artifact_store import (
            load_remote_artifact_store,
        )

        self._remote_artifact_store = load_remote_artifact_store(
            local=self._cache_store._artifact_store,
        )

    def _task_threads(self, task_def: TaskDef) -> int:
        """Return the scheduler core footprint for a task."""
        return task_def.threads

    def _task_memory_gb(self, task_def: TaskDef, resolved_args: dict[str, Any]) -> int:
        """Return the scheduler memory footprint for a task in GiB.

        Prefers the ``memory`` declared on the ``@task`` decorator. Falls
        back to a ``memory_gb`` key in *resolved_args* for backward
        compatibility.
        """
        if task_def.memory_gb > 0:
            return task_def.memory_gb

        raw_memory = resolved_args.get("memory_gb", 0)
        try:
            memory_gb = int(raw_memory)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"memory_gb must be an integer, got {raw_memory!r}") from exc

        if memory_gb < 0:
            raise ValueError(f"memory_gb must be at least 0, got {memory_gb}")

        return memory_gb

    def _build_worker_payload(self, *, node: _TaskNode) -> dict[str, Any]:
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
            "task_id": _task_id_for_node(node.node_id),
            "task_name": node.task_def.name,
            "attempt": node.attempt,
            "display_label": node.display_label,
            "log_event_queue": self._log_event_queue,
            "env": node.task_def.env,
            "module": node.task_def.fn.__module__,
            "module_file": resolve_module_file(node.task_def.fn.__module__),
            "task_kind": node.task_def.kind,
            "binding_name": node.task_def.fn.__name__,
            "transport_dir": str(node.transport_path),
        }

    def _decode_worker_result(self, *, node: _TaskNode, payload: dict[str, Any]) -> Any:
        """Decode a process-pool worker response."""
        if not payload["ok"]:
            self._cleanup_transport(node)
            raise _reconstruct_worker_error(payload["error"])

        encoding = payload.get("result_encoding")

        if encoding == "direct":
            # Process-pool path: Python object passed directly (no serialization).
            return payload["result"]

        if encoding == "pixi_direct_pickled":
            # Pixi subprocess path: dynamic result (ShellExpr / Expr / ExprList)
            # was pickle+base64 encoded to cross the JSON bridge.
            import base64
            import pickle

            return pickle.loads(base64.b64decode(payload["result"]))

        assert node.transport_path is not None
        return decode_value(payload["result"], base_dir=node.transport_path)

    def _cleanup_transport(self, node: _TaskNode) -> None:
        """Remove temporary transport artifacts for a task node."""
        if node.transport_path is None:
            return
        if node.transport_path.exists():
            shutil.rmtree(node.transport_path)
        node.transport_path = None

    def _finalize_result_value(self, *, node: _TaskNode, value: Any) -> Any:
        """Coerce and validate a fully resolved task result."""
        coerced = self._validator.coerce_return_value(task_def=node.task_def, value=value)
        finalized = self._asset_registrar.materialize_results(node=node, value=coerced)
        self._validator.validate_return_value(task_def=node.task_def, value=finalized)
        return finalized

    def _notebook_runtime_root(self) -> Path:
        """Return the shared runtime root for notebook support files."""
        if self.provenance is not None:
            return self.provenance.root_dir.parent
        return Path.cwd() / ".ginkgo"

    def _emit_notebook_notice(self, node: _TaskNode, message: str) -> None:
        """Surface a notebook runner notice (e.g. ipykernel install) as an event."""
        self._emit_event(
            TaskNotice(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                message=message,
            )
        )

    def validate(self, expr: Any) -> None:
        """Build the static task graph and validate import/env/input constraints."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        self._validator.validate_declared_envs(nodes=self._nodes.values())
        self._validator.validate_declared_secrets(nodes=self._nodes.values())

        for node in self._nodes.values():
            self._validator.validate_task_importable(task_def=node.task_def)
            self._validator.validate_static_inputs(node=node)

    def _is_valid_cached_result(self, *, cache_key: str, task_def: TaskDef, value: Any) -> bool:
        """Return whether a cached value still satisfies return validation.

        For file/folder outputs, the cache store ensures the working tree has a
        matching writable materialization before standard return validation
        checks run.
        """
        if not self._cache_store.validate_cached_outputs(
            cache_key=cache_key,
            task_def=task_def,
            value=value,
        ):
            return False

        try:
            self._validator.validate_return_value(task_def=task_def, value=value)
        except (FileNotFoundError, ValueError):
            return False

        return True

    def _try_prepare_cache_hit(self, *, node: _TaskNode) -> bool:
        """Attempt to complete a node from cache during preparation.

        This fast path only runs when cache identity can be decided without
        staging remote inputs first.
        """
        if node.resolved_args is None or self._stager.cache_lookup_requires_staging(node=node):
            return False

        if self.trust_workspace and self._try_stat_index_hit(node=node):
            return True

        return self._try_content_cache_hit(node=node)

    def _try_content_cache_hit(self, *, node: _TaskNode) -> bool:
        """Attempt a content-addressed cache hit for one prepared node."""
        assert node.resolved_args is not None
        cache_lookup_started = time.perf_counter()

        if node.cache_key is None or node.input_hashes is None:
            cache_key, input_hashes = self._cache_store.build_cache_key(
                task_def=node.task_def,
                resolved_args=node.resolved_args,
                extra_source_hash=node.extra_source_hash,
                known_digests=self._known_digests,
            )
            node.cache_key = cache_key
            node.input_hashes = input_hashes

        self._record_task_metadata(
            node=node,
            include_env_metadata=False,
        )
        cached_result = self._cache_store.load(cache_key=node.cache_key)
        if cached_result is MISSING or not self._is_valid_cached_result(
            cache_key=node.cache_key,
            task_def=node.task_def,
            value=cached_result,
        ):
            self._record_task_timing(
                node_id=node.node_id,
                phase="cache_lookup_seconds",
                started=cache_lookup_started,
            )
            return False

        self._record_task_timing(
            node_id=node.node_id,
            phase="cache_lookup_seconds",
            started=cache_lookup_started,
        )
        self._mark_node_cached(node=node, value=cached_result, cache_key=node.cache_key)
        return True

    def _try_stat_index_hit(self, *, node: _TaskNode) -> bool:
        """Attempt a stat-index cache hit for ``--trust-workspace`` mode.

        Returns ``True`` if the hit succeeded and the node was marked
        complete, ``False`` to fall through to the content-addressed path.
        """
        cache_lookup_started = time.perf_counter()
        stat_key = self._cache_store.stat_fingerprint(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            extra_source_hash=node.extra_source_hash,
        )
        cached_result = self._cache_store.try_stat_index(stat_key=stat_key)
        if cached_result is MISSING:
            self._record_task_timing(
                node_id=node.node_id,
                phase="cache_lookup_seconds",
                started=cache_lookup_started,
            )
            return False

        # In trust-workspace mode we only check that output files exist,
        # not that their content matches the artifact store.
        content_key = self._cache_store._stat_index.get(stat_key)
        if content_key is None:
            self._record_task_timing(
                node_id=node.node_id,
                phase="cache_lookup_seconds",
                started=cache_lookup_started,
            )
            return False

        node.cache_key = content_key
        node.input_hashes = {}
        self._record_task_metadata(
            node=node,
            include_env_metadata=False,
        )
        self._record_task_timing(
            node_id=node.node_id,
            phase="cache_lookup_seconds",
            started=cache_lookup_started,
        )
        self._mark_node_cached(node=node, value=cached_result, cache_key=content_key)
        return True

    def _record_stat_index_entry(self, *, node: _TaskNode, cache_key: str) -> None:
        """Record a stat-index entry for a completed task."""
        if node.resolved_args is None:
            return
        stat_key = self._cache_store.stat_fingerprint(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            extra_source_hash=node.extra_source_hash,
        )
        self._cache_store.record_stat_index(stat_key=stat_key, cache_key=cache_key)

    def _propagate_known_digests(self, *, cache_key: str) -> None:
        """Populate ``_known_digests`` from a cache entry's artifact IDs.

        Called on cache hits so that downstream tasks can skip re-hashing
        file outputs that this task produced.
        """
        artifact_ids = self._cache_store._load_artifact_ids(cache_key=cache_key)
        if artifact_ids is None:
            return
        for path_str, artifact_id in artifact_ids.items():
            resolved_key = str(Path(path_str).resolve())
            self._known_digests[resolved_key] = artifact_id

    def _mark_node_cached(self, *, node: _TaskNode, value: Any, cache_key: str) -> None:
        """Mark one node complete from cache and emit cached completion events."""
        if node.attempt == 0:
            node.attempt = 1

        self._propagate_known_digests(cache_key=cache_key)
        node.result = value
        node.state = "completed"
        for path in node.tmp_paths:
            shutil.rmtree(path)
        node.tmp_paths = []
        if self.provenance is not None:
            self.provenance.mark_cached(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                value=value,
                outputs=self._output_summary_for(node=node, value=value),
                assets=self._asset_index_for(value=value),
            )
            self._notebook_runner.replay_cached_extras(node=node, cache_key=cache_key)
        self._emit_event(
            TaskCacheHit(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                cache_key=cache_key,
            )
        )
        self._emit_event(
            TaskCompleted(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                status="cached",
                cache_key=cache_key,
                outputs=self._output_summary_for(node=node, value=value),
            )
        )

        # Record stat-index entry so future --trust-workspace runs can
        # find this cache key without content hashing.
        self._record_stat_index_entry(node=node, cache_key=cache_key)

    def _record_task_metadata(
        self,
        *,
        node: _TaskNode,
        include_env_metadata: bool = True,
    ) -> None:
        """Update provenance inputs and environment copies for a task node."""
        if self.provenance is None:
            return
        self.provenance.update_task_inputs(
            node_id=node.node_id,
            task_name=node.task_def.name,
            env=node.task_def.env,
            kind=node.task_def.kind,
            execution_mode=node.task_def.execution_mode,
            resolved_args=node.resolved_args,
            input_hashes=node.input_hashes,
            cache_key=node.cache_key,
            dependency_ids=sorted(node.dependency_ids),
            dynamic_dependency_ids=sorted(node.dynamic_dependency_ids),
        )
        if not include_env_metadata:
            return

        if node.task_def.env is not None and self.backend is not None:
            # Record backend type and container-specific metadata.
            if is_container_env(node.task_def.env):
                extra: dict[str, Any] = {"backend": "container"}
                digest = self.backend.env_identity(env=node.task_def.env)
                if digest is not None:
                    extra["container_image_digest"] = digest
                self.provenance.update_task_extra(
                    node_id=node.node_id,
                    **extra,
                )
            else:
                self.provenance.update_task_extra(
                    node_id=node.node_id,
                    backend="local",
                )
                lock_path = self.backend.env_lock_path(env=node.task_def.env)
                if lock_path is not None:
                    self.provenance.copy_env_lock(
                        env_name=node.task_def.env,
                        lock_path=lock_path,
                    )

    def _record_task_timing(self, *, node_id: int, phase: str, started: float) -> None:
        """Record one task-phase timing bucket when provenance is enabled."""
        if self.provenance is None:
            return
        self.provenance.add_task_timing(
            node_id=node_id,
            phase=phase,
            seconds=time.perf_counter() - started,
        )

    def _record_task_failure(self, *, node: _TaskNode, exc: BaseException) -> None:
        """Persist task failure details to the run manifest."""
        if self.provenance is None:
            return
        self.provenance.mark_failed(
            node_id=node.node_id,
            task_name=node.task_def.name,
            env=node.task_def.env,
            exc=exc,
            failure=classify_failure(exc=exc),
        )

    def _display_label_for(self, *, node: _TaskNode) -> str | None:
        """Return a richer CLI label for mapped tasks once args are resolved."""
        if not node.expr.mapped or node.resolved_args is None:
            return None

        if node.expr.display_label_parts:
            base_name = node.task_def.name.rsplit(".", 1)[-1]
            return f"{base_name}[{','.join(node.expr.display_label_parts)}]"

        label_key = first_label_param_name(task_def=node.task_def)
        if label_key is None or label_key not in node.resolved_args:
            return None

        rendered = render_label_value(node.resolved_args[label_key])
        if rendered is None:
            return None

        base_name = node.task_def.name.rsplit(".", 1)[-1]
        return f"{base_name}[{rendered}]"

    def _output_summary_for(self, *, node: _TaskNode, value: Any) -> list[dict[str, Any]]:
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
        if self.provenance is not None:
            return self.provenance.run_id
        return "validation"

    def _emit_event(self, event: object) -> None:
        """Emit a runtime event to the attached event bus, if any."""
        if self.event_bus is not None:
            with self.profiler.timed("event_emit"):
                self.event_bus.emit(event)

    def _task_log_emitter(self, *, node: _TaskNode, stream: str) -> Any:
        """Return a task-log callback bound to one task node and stream."""

        def emit(chunk: str) -> None:
            if not chunk:
                return
            sequence_key = (node.node_id, stream)
            sequence = self._task_log_sequences.get(sequence_key, 0) + 1
            self._task_log_sequences[sequence_key] = sequence
            self._emit_event(
                TaskLog(
                    run_id=self._run_id,
                    task_id=_task_id_for_node(node.node_id),
                    task_name=node.task_def.name,
                    attempt=node.attempt,
                    display_label=node.display_label,
                    stream=stream,
                    chunk=chunk,
                    sequence=sequence,
                )
            )

        return emit

    def _run_driver_task(self, *, node: _TaskNode) -> Any:
        """Run a driver-task wrapper on the scheduler process.

        For notebook and script tasks the body was already evaluated eagerly
        in ``_prepare_node`` to extract the source hash for the cache key.
        The stored sentinel is returned directly to avoid re-running the body.
        """
        assert node.execution_args is not None
        if node.driver_sentinel is not None:
            return node.driver_sentinel
        with _task_log_context(
            stdout_path=str(node.stdout_path) if node.stdout_path is not None else None,
            stderr_path=str(node.stderr_path) if node.stderr_path is not None else None,
            secret_values=node.secret_values,
            log_emitter=lambda *, stream, chunk: self._task_log_emitter(
                node=node,
                stream=stream,
            )(chunk),
        ):
            return node.task_def.fn(**node.execution_args)

    def _handle_task_body_result(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Advance a task after its driver wrapper has finished."""
        _driver_sentinels = (ShellExpr, NotebookExpr, ScriptExpr, SubWorkflowExpr)
        if self._failure is not None and (
            isinstance(completed_value, _driver_sentinels)
            or contains_dynamic_expression(completed_value)
        ):
            self._cleanup_transport(node)
            for path in node.tmp_paths:
                shutil.rmtree(path)
            node.tmp_paths = []
            node.state = "failed"
            return

        if node.task_def.kind == "python":
            if isinstance(completed_value, _driver_sentinels):
                sentinel_name = type(completed_value).__name__
                self._cleanup_transport(node)
                raise TypeError(
                    f"{node.task_def.name} returned {sentinel_name}, but the task is declared "
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
                        parent_task_id=_task_id_for_node(node.node_id),
                        new_node_ids=[
                            _task_id_for_node(dep_id) for dep_id in sorted(dynamic_dependencies)
                        ],
                    )
                )
                return

            final_value = self._finalize_result_value(node=node, value=completed_value)
            self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)
            return

        # Driver task: shell / notebook / script — dispatch to the appropriate runner.
        assert self._shell_executor is not None

        if isinstance(completed_value, ShellExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._shell_runner.run_shell,
                node=node,
                shell_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        if isinstance(completed_value, NotebookExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._notebook_runner.run_notebook,
                node=node,
                notebook_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        if isinstance(completed_value, ScriptExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._notebook_runner.run_script,
                node=node,
                script_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        if isinstance(completed_value, SubWorkflowExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._subworkflow_runner.run_subworkflow,
                node=node,
                subworkflow_expr=completed_value,
            )
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
                    parent_task_id=_task_id_for_node(node.node_id),
                    new_node_ids=[
                        _task_id_for_node(dep_id) for dep_id in sorted(dynamic_dependencies)
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
            f"{_expected.get(kind, 'the appropriate sentinel')} or dynamic task expressions."
        )


def _task_id_for_node(node_id: int) -> str:
    """Return the stable task identifier for a node."""
    return f"task_{node_id:04d}"


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
