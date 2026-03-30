"""Concurrent evaluator for Ginkgo expressions."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import builtins
import inspect
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
from threading import Event, Lock, Thread, current_thread, main_thread
from types import FrameType
from typing import Any, get_args, get_origin

import yaml

from ginkgo.core.asset import (
    AssetKey,
    AssetRef,
    AssetResult,
    AssetVersion,
    asset_ref_from_version,
    collect_asset_refs,
    make_asset_version,
)
from ginkgo.core.expr import Expr, ExprList, OutputIndex
from ginkgo.core.notebook import NotebookExpr
from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef, is_remote_uri
from ginkgo.core.script import ScriptExpr
from ginkgo.core.secret import SecretRef
from ginkgo.core.shell import ShellExpr
from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.config import load_runtime_config
from ginkgo.envs.container import is_container_env
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.backend import LocalBackend, TaskBackend
from ginkgo.runtime.asset_store import AssetStore
from ginkgo.runtime.cache import MISSING, CacheStore
from ginkgo.runtime.events import (
    EnvPrepareCompleted,
    EnvPrepareStarted,
    EventBus,
    GraphExpanded,
    GraphNodeRegistered,
    GinkgoEvent,
    TaskCacheHit,
    TaskCacheMiss,
    TaskCompleted,
    TaskFailed,
    TaskLog,
    TaskReady,
    TaskRetrying,
    TaskStaging,
    TaskStarted,
)
from ginkgo.runtime.module_loader import load_module, resolve_module_file
from ginkgo.runtime.provenance import RunProvenanceRecorder
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset
from ginkgo.runtime.secrets import (
    SecretResolver,
    collect_secret_refs,
    collect_resolved_secret_values,
    redact_text,
    resolve_secret_refs,
)
from ginkgo.runtime.value_codec import (
    CodecError,
    decode_value,
    encode_value,
    ensure_serializable,
    summarise_value,
)
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
    pixi_registry: PixiRegistry | None = None,
    provenance: RunProvenanceRecorder | None = None,
    secret_resolver: SecretResolver | None = None,
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
        Execution backend for environment-isolated tasks.  When ``None`` and
        *pixi_registry* is provided, a ``LocalBackend`` is created
        automatically for backward compatibility.
    pixi_registry : PixiRegistry | None
        Deprecated — use *backend* instead.  Registry for resolving Pixi
        environments.  Ignored when *backend* is provided.

    Returns
    -------
    Any
        The concrete result of evaluating the input.
    """
    if backend is None and pixi_registry is not None:
        backend = LocalBackend(pixi_registry=pixi_registry)

    return _ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        backend=backend,
        provenance=provenance,
        secret_resolver=secret_resolver,
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
    result: Any = MISSING
    tmp_paths: list[Path] = field(default_factory=list)
    transport_path: Path | None = None
    dynamic_template: Any = None
    dynamic_dependency_ids: set[int] = field(default_factory=set)
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    display_label: str | None = None
    attempt: int = 0
    secret_values: tuple[str, ...] = ()
    driver_sentinel: Any = None
    extra_source_hash: str | None = None
    asset_versions: list[AssetVersion] = field(default_factory=list)

    @property
    def task_def(self) -> TaskDef:
        """Return the task definition for the node."""
        return self.expr.task_def


@dataclass(frozen=True, kw_only=True)
class _NotebookArtifacts:
    """Run-scoped artifact locations for one notebook task."""

    root_dir: Path
    html_path: Path
    executed_path: Path | None
    params_path: Path


class _SignalMonitor:
    """Temporary signal handler that requests a graceful scheduler stop."""

    def __init__(self) -> None:
        self.exception: BaseException | None = None
        self._installed = False
        self._previous: dict[int, Any] = {}

    def __enter__(self) -> _SignalMonitor:
        if current_thread() is not main_thread():
            return self

        for signum in (signal.SIGINT, signal.SIGTERM):
            self._previous[signum] = signal.getsignal(signum)
            signal.signal(signum, self._handler)

        self._installed = True
        return self

    def __exit__(self, *_: object) -> None:
        if not self._installed:
            return

        for signum, previous in self._previous.items():
            signal.signal(signum, previous)

    def _handler(self, signum: int, _frame: FrameType | None) -> None:
        if self.exception is None:
            self.exception = KeyboardInterrupt(f"Received signal {signum}")


@dataclass(kw_only=True)
class _ConcurrentEvaluator:
    """Concurrent evaluator with dependency tracking and cache integration."""

    jobs: int | None = None
    cores: int | None = None
    memory: int | None = None
    backend: TaskBackend | None = None
    provenance: RunProvenanceRecorder | None = None
    secret_resolver: SecretResolver | None = None
    event_bus: EventBus | None = None
    _cache_store: CacheStore = field(init=False, repr=False)
    _asset_store: AssetStore = field(init=False, repr=False)
    _stderr: Any = field(default_factory=lambda: sys.stderr)
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
    _subprocess_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _active_subprocesses: dict[int, subprocess.Popen[str]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _log_event_queue: Any = field(default=None, init=False, repr=False)
    _log_drain_stop: Event | None = field(default=None, init=False, repr=False)
    _log_drain_thread: Thread | None = field(default=None, init=False, repr=False)
    _task_log_sequences: dict[tuple[int, str], int] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _staging_jobs: int = field(default=0, init=False, repr=False)
    _staging_inflight: dict[str, Future[Path]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _staging_lock: Lock = field(default_factory=Lock, init=False, repr=False)

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

        publisher = self._load_remote_publisher()
        self._cache_store = CacheStore(backend=self.backend, publisher=publisher)
        self._asset_store = AssetStore(root=self._cache_store._root.parent / "assets")
        self._staging_cache = None  # Lazily created on first remote ref.
        self._staging_jobs = _resolve_staging_jobs(jobs=self.jobs)

    def evaluate(self, expr: Any) -> Any:
        """Resolve a root expression or nested container concurrently."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        if not self._root_dependency_ids:
            return self._materialize(expr)

        # Validate all statically declared environments before any work starts.
        self._validate_declared_envs()
        self._validate_declared_secrets()

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
            signals = stack.enter_context(_SignalMonitor())
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
                        self._prepare_pending_nodes()
                        self._finalize_dynamic_nodes()
                        self._dispatch_ready_nodes(
                            python_executor=python_executor,
                            shell_executor=shell_executor,
                        )

                        if self._is_root_resolved() and not self._running_futures:
                            return self._materialize(self._root_template)
                    elif not self._running_futures:
                        break

                    if self._running_futures:
                        done, _ = wait(
                            tuple(self._running_futures.keys()),
                            return_when=FIRST_COMPLETED,
                        )
                        self._consume_completed_futures(done)
                        continue

                    if self._failure is not None:
                        break

                    if self._is_root_resolved():
                        return self._materialize(self._root_template)

                    if self._can_make_scheduler_progress():
                        continue

                    raise RuntimeError("Scheduler reached a deadlock with unresolved tasks")
            finally:
                self._stop_log_drain()
                self._python_executor = None
                self._shell_executor = None

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
        resolved_args = self._resolve_task_args(
            expr=node.expr,
            task_def=node.task_def,
            node=node,
            include_tmp_dirs=False,
            stage_remote_refs=False,
        )
        self._validate_inputs(task_def=node.task_def, resolved_args=resolved_args)
        self._validate_task_preconditions(
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
        self._record_task_metadata(node)
        # Materialize Pixi environments before any parallel dispatch starts.
        self._prepare_task_environment(node=node)

        node.threads = self._task_threads(resolved_args)
        node.memory_gb = self._task_memory_gb(resolved_args)
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
        self.backend.prepare(env=node.task_def.env)
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
        selected = select_dispatch_subset(
            ready_tasks=[
                SchedulableTask(
                    node_id=node.node_id,
                    threads=node.threads,
                    memory_gb=node.memory_gb,
                )
                for node in ready_nodes
            ],
            jobs=available_jobs,
            cores=available_cores,
            memory=available_memory,
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
            remote_input_count = _count_remote_inputs(node.resolved_args)
            if remote_input_count > 0:
                node.state = "staging"
                self._emit_event(
                    TaskStaging(
                        run_id=self._run_id,
                        task_id=_task_id_for_node(node.node_id),
                        task_name=node.task_def.name,
                        attempt=node.attempt,
                        display_label=node.display_label,
                        remote_input_count=remote_input_count,
                    )
                )
                assert self._staging_executor is not None
                future = self._staging_executor.submit(self._stage_task_inputs, node=node)
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
                continue

            try:
                completed_value = future.result()
            except BaseException as exc:
                self._handle_task_exception(node=node, exc=exc)
                continue

            try:
                if phase == "staging":
                    self._handle_completed_staging_phase(
                        node=node, completed_value=completed_value
                    )
                elif phase == "python":
                    self._handle_completed_worker_phase(node=node, completed_value=completed_value)
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

    def _handle_completed_worker_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Handle the result returned from a Python worker."""
        completed_value = self._decode_worker_result(node=node, payload=completed_value)
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_staging_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Start task execution after remote inputs have been staged locally."""
        if not isinstance(completed_value, dict):
            raise TypeError("Expected staged task arguments from staging phase")

        node.resolved_args = completed_value
        self._validate_inputs(task_def=node.task_def, resolved_args=node.resolved_args)
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
        sanitized_exc = self._sanitize_exception(exc=exc, secret_values=node.secret_values)
        if self._failure is None and self._should_retry(node=node):
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
                failure=_classify_failure(exc=sanitized_exc),
            )
        )

    def _should_retry(self, *, node: _TaskNode) -> bool:
        """Return whether the current failed attempt should be retried."""
        return node.attempt <= node.task_def.retries

    def _schedule_retry(self, *, node: _TaskNode, exc: BaseException) -> None:
        """Reset node state so the scheduler can rerun the task from scratch."""
        self._cleanup_transport(node)

        # Remove any attempt-local scratch directories before rerunning.
        for path in node.tmp_paths:
            if path.exists():
                shutil.rmtree(path)

        node.state = "pending"
        node.resolved_args = None
        node.execution_args = None
        node.cache_key = None
        node.input_hashes = None
        node.threads = 1
        node.memory_gb = 0
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
                failure=_classify_failure(exc=exc),
            )
        )

    def _complete_node(self, *, node: _TaskNode, value: Any, tmp_paths: list[Path]) -> None:
        """Persist and mark a task node as fully completed."""
        self._cleanup_transport(node)
        self._cache_store.save(
            cache_key=node.cache_key,
            result=value,
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            input_hashes=node.input_hashes,
        )
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
                outputs=self._artifact_index_for(node=node, value=value),
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
                outputs=self._artifact_index_for(node=node, value=value),
            )
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
                resolved_args[name] = self._materialize(expr.args[name])
                continue

            if parameter.default is not parameter.empty:
                resolved_args[name] = parameter.default
                continue

            raise TypeError(f"{task_def.fn.__name__}() missing required argument: '{name}'")

        if stage_remote_refs:
            resolved_args = self._stage_remote_refs(
                task_def=task_def,
                resolved_args=resolved_args,
            )

        return resolved_args

    def _stage_remote_refs(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Stage remote references into local paths."""
        staged: dict[str, Any] = {}
        for name, value in resolved_args.items():
            annotation = task_def.type_hints.get(
                name,
                task_def.signature.parameters[name].annotation
                if name in task_def.signature.parameters
                else Any,
            )
            staged[name] = self._stage_remote_value(annotation=annotation, value=value)
        return staged

    def _stage_remote_value(self, *, annotation: Any, value: Any) -> Any:
        """Stage a single value, recursing into containers."""
        # Explicit remote refs.
        if isinstance(value, RemoteFileRef):
            return file(str(self._stage_remote_ref(ref=value)))
        if isinstance(value, RemoteFolderRef):
            return folder(str(self._stage_remote_ref(ref=value)))

        # Annotation-aware coercion: raw URI string → remote ref → staged path.
        if isinstance(value, str) and is_remote_uri(value):
            if annotation is file:
                from ginkgo.core.remote import remote_file

                ref = remote_file(value)
                return file(str(self._stage_remote_ref(ref=ref)))
            if annotation is folder:
                from ginkgo.core.remote import remote_folder

                ref = remote_folder(value)
                return folder(str(self._stage_remote_ref(ref=ref)))

        # Recurse into typed containers.
        origin = get_origin(annotation)
        if origin in {list, tuple} and isinstance(value, (list, tuple)):
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            staged_items = [
                self._stage_remote_value(annotation=inner_annotation, value=item) for item in value
            ]
            return type(value)(staged_items)

        return value

    def _stage_task_inputs(self, *, node: _TaskNode) -> dict[str, Any]:
        """Stage remote inputs for one task in the reserved worker slot."""
        assert node.resolved_args is not None
        return self._stage_remote_refs(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
        )

    def _stage_remote_ref(self, *, ref: RemoteRef) -> Path:
        """Stage one remote ref with in-flight deduplication."""
        identity = _remote_ref_identity(ref=ref)

        with self._staging_lock:
            inflight = self._staging_inflight.get(identity)
            if inflight is None:
                inflight = Future()
                self._staging_inflight[identity] = inflight
                should_stage = True
            else:
                should_stage = False

        if not should_stage:
            return inflight.result()

        try:
            staged_path = self._stage_remote_ref_uncached(ref=ref)
            inflight.set_result(staged_path)
            return staged_path
        except BaseException as exc:
            inflight.set_exception(exc)
            raise
        finally:
            with self._staging_lock:
                self._staging_inflight.pop(identity, None)

    def _stage_remote_ref_uncached(self, *, ref: RemoteRef) -> Path:
        """Stage one remote ref through the worker-local staging cache."""
        cache = self._get_staging_cache()
        if isinstance(ref, RemoteFileRef):
            return cache.stage_file(ref=ref)
        if isinstance(ref, RemoteFolderRef):
            return cache.stage_folder(ref=ref)
        raise TypeError(f"Unsupported remote ref type: {type(ref).__name__}")

    def _get_staging_cache(self) -> Any:
        """Lazily create and return the staging cache."""
        if self._staging_cache is None:
            from ginkgo.remote.staging import StagingCache

            self._staging_cache = StagingCache()
        return self._staging_cache

    def _load_remote_publisher(self) -> Any | None:
        """Load a remote publisher from ginkgo.toml if configured.

        Looks for a ``[remote] store`` key containing a remote URI string
        (e.g. ``s3://bucket/prefix/``).

        Returns
        -------
        RemotePublisher | None
            A publisher instance, or ``None`` if not configured.
        """
        from ginkgo.config import load_runtime_config

        config = load_runtime_config(project_root=Path.cwd())
        store_uri = config.get("remote", {}).get("store")
        if store_uri is None:
            return None

        from ginkgo.core.remote import _parse_uri
        from ginkgo.remote.publisher import RemotePublisher
        from ginkgo.remote.resolve import resolve_backend

        parsed = _parse_uri(store_uri)
        backend = resolve_backend(parsed["scheme"])

        # The key from the URI becomes the prefix for all published artifacts.
        prefix = parsed["key"]
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        # Artifact store directories.
        artifacts_root = Path.cwd() / ".ginkgo" / "artifacts"
        return RemotePublisher(
            backend=backend,
            bucket=parsed["bucket"],
            prefix=prefix,
            local_blobs_dir=artifacts_root / "blobs",
            local_trees_dir=artifacts_root / "trees",
            local_refs_dir=artifacts_root / "refs",
        )

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
        return False

    def _cancel_pending_futures(self) -> None:
        """Cancel queued futures that have not started yet."""
        for future in self._running_futures:
            future.cancel()

    def _interrupt_running_work(self) -> None:
        """Stop queued and active work after an external interrupt."""
        self._cancel_pending_futures()
        self._terminate_active_subprocesses()
        self._shutdown_staging_executor()
        self._shutdown_shell_executor()
        self._shutdown_python_executor()

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

    def _register_subprocess(self, *, process: subprocess.Popen[str]) -> None:
        """Track a subprocess so interrupts can terminate it."""
        with self._subprocess_lock:
            self._active_subprocesses[process.pid] = process

    def _unregister_subprocess(self, *, process: subprocess.Popen[str]) -> None:
        """Stop tracking a subprocess after it exits."""
        with self._subprocess_lock:
            self._active_subprocesses.pop(process.pid, None)

    def _terminate_active_subprocesses(self) -> None:
        """Terminate all active shell and Pixi subprocesses."""
        with self._subprocess_lock:
            processes = list(self._active_subprocesses.values())

        for process in processes:
            self._terminate_subprocess(process=process)

    def _terminate_subprocess(self, *, process: subprocess.Popen[str]) -> None:
        """Terminate one subprocess, escalating to kill if needed."""
        if process.poll() is not None:
            return

        if os.name == "posix":
            with suppress(ProcessLookupError, OSError):
                os.killpg(process.pid, signal.SIGTERM)
        else:
            with suppress(Exception):
                process.terminate()

        with suppress(subprocess.TimeoutExpired):
            process.wait(timeout=0.2)
            return

        if os.name == "posix":
            with suppress(ProcessLookupError, OSError):
                os.killpg(process.pid, signal.SIGKILL)
        else:
            with suppress(Exception):
                process.kill()

        with suppress(Exception):
            process.wait(timeout=0.2)

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

    def _run_subprocess(
        self,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a subprocess while tracking it for interrupt-time termination."""
        popen_kwargs: dict[str, Any] = {
            "shell": use_shell,
            "stderr": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "text": True,
        }
        if os.name == "posix":
            popen_kwargs["start_new_session"] = True

        process = subprocess.Popen(argv, **popen_kwargs)
        self._register_subprocess(process=process)
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        if not hasattr(process, "stdout") or not hasattr(process, "stderr"):
            try:
                stdout_text, stderr_text = process.communicate()
            finally:
                self._unregister_subprocess(process=process)
            return subprocess.CompletedProcess(
                args=argv,
                returncode=process.returncode,
                stdout=stdout_text,
                stderr=stderr_text,
            )

        def consume_stream(*, pipe: Any, sink: list[str], callback: Any) -> None:
            try:
                while True:
                    chunk = pipe.readline()
                    if chunk == "":
                        break
                    sink.append(chunk)
                    if callback is not None:
                        callback(chunk)
            finally:
                pipe.close()

        stdout_thread = Thread(
            target=consume_stream,
            kwargs={"pipe": process.stdout, "sink": stdout_chunks, "callback": on_stdout},
            daemon=True,
        )
        stderr_thread = Thread(
            target=consume_stream,
            kwargs={"pipe": process.stderr, "sink": stderr_chunks, "callback": on_stderr},
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        try:
            returncode = process.wait()
        finally:
            stdout_thread.join()
            stderr_thread.join()
            self._unregister_subprocess(process=process)

        return subprocess.CompletedProcess(
            args=argv,
            returncode=returncode,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
        )

    def _call_run_subprocess(
        self,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any,
        on_stderr: Any,
    ) -> tuple[subprocess.CompletedProcess[str], bool]:
        """Call ``_run_subprocess`` while tolerating legacy test doubles."""
        run_subprocess = self._run_subprocess
        parameters = inspect.signature(run_subprocess).parameters
        supports_stream_callbacks = "on_stdout" in parameters and "on_stderr" in parameters
        if supports_stream_callbacks:
            completed = run_subprocess(
                argv=argv,
                use_shell=use_shell,
                on_stdout=on_stdout,
                on_stderr=on_stderr,
            )
            return completed, True

        completed = run_subprocess(argv=argv, use_shell=use_shell)
        return completed, False

    def _running_cores(self) -> int:
        """Return the core footprint of currently running tasks."""
        return sum(self._nodes[node_id].threads for node_id, _ in self._running_futures.values())

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

        cache_key, input_hashes = self._cache_store.build_cache_key(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
            extra_source_hash=node.extra_source_hash,
        )
        node.cache_key = cache_key
        node.input_hashes = input_hashes
        self._record_task_metadata(node)

        cached_result = self._cache_store.load(cache_key=cache_key)
        if cached_result is not MISSING and self._is_valid_cached_result(
            cache_key=cache_key,
            task_def=node.task_def,
            value=cached_result,
        ):
            node.result = cached_result
            node.state = "completed"
            for path in node.tmp_paths:
                shutil.rmtree(path)
            node.tmp_paths = []
            if self.provenance is not None:
                self.provenance.mark_cached(
                    node_id=node.node_id,
                    task_name=node.task_def.name,
                    env=node.task_def.env,
                    value=cached_result,
                    outputs=self._artifact_index_for(node=node, value=cached_result),
                    assets=self._asset_index_for(value=cached_result),
                )
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
                    outputs=self._artifact_index_for(node=node, value=cached_result),
                )
            )
            return

        self._emit_event(
            TaskCacheMiss(
                run_id=self._run_id,
                task_id=_task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                attempt=node.attempt,
                display_label=node.display_label,
                cache_key=cache_key,
            )
        )

        node.state = "running"
        node.execution_args = self._resolve_execution_args(node=node)
        node.secret_values = collect_resolved_secret_values(
            template=node.resolved_args,
            resolved=node.execution_args,
        )
        self._validate_task_contract(node=node)
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
            )
        )
        if self.provenance is not None:
            self.provenance.mark_running(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                attempt=node.attempt,
                retries=node.task_def.retries,
            )

        if node.task_def.kind in {"notebook", "script", "shell"}:
            future = shell_executor.submit(
                self._run_driver_task,
                node=node,
            )
            self._running_futures[future] = (node.node_id, "driver")
            return

        node.transport_path = Path(tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-"))
        payload = self._build_worker_payload(node=node)
        future = python_executor.submit(run_task, payload)
        self._running_futures[future] = (node.node_id, "python")

    def _task_threads(self, resolved_args: dict[str, Any]) -> int:
        """Return the scheduler core footprint for a task."""
        raw_threads = resolved_args.get("threads", 1)
        try:
            threads = int(raw_threads)
        except (TypeError, ValueError) as exc:
            raise TypeError(f"threads must be an integer, got {raw_threads!r}") from exc

        if threads < 1:
            raise ValueError(f"threads must be at least 1, got {threads}")

        return threads

    def _task_memory_gb(self, resolved_args: dict[str, Any]) -> int:
        """Return the scheduler memory footprint for a task in GiB."""
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

    def _validate_task_contract(self, *, node: _TaskNode) -> None:
        """Validate that a task can run safely under its declared contract."""
        assert node.execution_args is not None
        self._validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=node.execution_args,
        )

    def _validate_task_preconditions(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> None:
        """Validate top-level importability and serializable input types."""
        self._validate_task_importable(task_def=task_def)
        for name, value in resolved_args.items():
            if task_def.type_hints.get(name) is tmp_dir:
                continue
            self._validate_process_safe_value(
                value=value,
                label=f"{task_def.name}.{name}",
            )

    def _validate_static_inputs(self, *, node: _TaskNode) -> None:
        """Validate literal-only task inputs during dry-run mode."""
        for name, parameter in node.task_def.signature.parameters.items():
            annotation = node.task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir or name not in node.expr.args:
                continue
            value = node.expr.args[name]
            if self._contains_dynamic_expression(value):
                continue
            if collect_secret_refs(value):
                continue
            self._validate_annotated_value(
                annotation=annotation,
                value=value,
                label=f"{node.task_def.name}.{name}",
            )

    def _contains_dynamic_expression(self, value: Any) -> bool:
        """Return whether a nested value contains unresolved expressions."""
        if isinstance(value, (Expr, ExprList, OutputIndex)):
            return True
        if isinstance(value, list | tuple):
            return any(self._contains_dynamic_expression(item) for item in value)
        if isinstance(value, dict):
            return any(
                self._contains_dynamic_expression(key) or self._contains_dynamic_expression(item)
                for key, item in value.items()
            )
        return False

    def _validate_task_importable(self, *, task_def: TaskDef) -> None:
        """Require Python tasks to be plain top-level importable functions."""
        fn = task_def.fn
        if fn.__qualname__ != fn.__name__:
            raise TypeError(
                f"{task_def.name} is not a top-level function. "
                "Define tasks at module scope for process execution."
            )

        if fn.__closure__:
            raise TypeError(
                f"{task_def.name} closes over local state. "
                "Pass required values as task arguments instead."
            )

        module = load_module(fn.__module__, module_file=resolve_module_file(fn.__module__))
        imported = getattr(module, fn.__name__, None)
        if imported is not fn and getattr(imported, "fn", None) is not fn:
            raise TypeError(
                f"{task_def.name} is not importable by module path. "
                "Define tasks as plain module-level functions."
            )

    def _validate_process_safe_value(self, *, value: Any, label: str) -> None:
        """Reject values that are not supported across process and cache boundaries."""
        if isinstance(value, (Expr, ExprList, ShellExpr, SecretRef)):
            return
        if collect_secret_refs(value):
            return
        try:
            ensure_serializable(value, label=label)
        except CodecError as exc:
            raise TypeError(str(exc)) from exc

    def _finalize_result_value(self, *, node: _TaskNode, value: Any) -> Any:
        """Coerce and validate a fully resolved task result."""
        coerced = self._coerce_return_value(task_def=node.task_def, value=value)
        finalized = self._materialize_asset_results(node=node, value=coerced)
        self._validate_return_value(task_def=node.task_def, value=finalized)
        return finalized

    def _run_shell(self, *, node: _TaskNode, shell_expr: ShellExpr) -> Any:
        """Execute a shell command and return its declared output path or paths."""
        task_def = node.task_def
        user_log_path = Path(shell_expr.log) if shell_expr.log is not None else None

        for output_path in self._iter_shell_output_paths(shell_expr.output):
            _remove_declared_output(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        completed = self._run_logged_command(
            node=node,
            cmd=shell_expr.cmd,
            user_log_path=user_log_path,
        )
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=task_def.name,
                cmd=self._redact_text(shell_expr.cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=shell_expr.log,
            )

        missing_outputs = [
            str(output_path)
            for output_path in self._iter_shell_output_paths(shell_expr.output)
            if not output_path.exists()
        ]
        if missing_outputs:
            missing_label = missing_outputs[0] if len(missing_outputs) == 1 else missing_outputs
            raise FileNotFoundError(
                f"Shell task {task_def.name} completed but did not create output {missing_label!r}"
            )

        return self._coerce_return_value(task_def=task_def, value=shell_expr.output)

    def _run_notebook_expr(self, *, node: _TaskNode, notebook_expr: NotebookExpr) -> Any:
        """Execute a notebook task from a ``NotebookExpr`` sentinel.

        Determines the notebook backend from the file extension, runs
        execution, renders HTML, validates any declared outputs, and
        returns the appropriate result value.
        """
        assert node.execution_args is not None
        notebook_path = notebook_expr.path
        notebook_kind = "ipynb" if notebook_path.suffix.lower() == ".ipynb" else "marimo"
        user_log_path = Path(notebook_expr.log) if notebook_expr.log is not None else None
        description = _fn_description(node.task_def.fn)

        artifacts = self._notebook_artifacts(node=node, notebook_kind=notebook_kind)
        self._prepare_notebook_artifacts(artifacts=artifacts)
        if notebook_expr.outputs is not None:
            for output_path in self._iter_output_values(notebook_expr.outputs):
                _remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
        self._record_notebook_manifest(
            node=node,
            notebook_kind=notebook_kind,
            notebook_path=notebook_path,
            notebook_description=description,
            executed_path=artifacts.executed_path,
            rendered_html=artifacts.html_path,
            render_status="pending",
            render_error=None,
        )

        # Build and run the execution command.
        if notebook_kind == "ipynb":
            command = self._build_ipynb_execute_command(
                notebook_path=notebook_path,
                executed_path=artifacts.executed_path,
                params_path=artifacts.params_path,
                resolved_args=node.execution_args,
            )
            executed_artifact = artifacts.executed_path
        else:
            command = self._build_marimo_execute_command(
                notebook_path=notebook_path,
                resolved_args=node.execution_args,
            )
            executed_artifact = None

        completed = self._run_logged_command(node=node, cmd=command, user_log_path=user_log_path)
        if completed.returncode != 0:
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="not_started",
                render_error=None,
            )
            raise NotebookTaskError(
                task_name=node.task_def.name,
                phase="execute",
                cmd=command,
                exit_code=completed.returncode,
                output=(completed.stdout or "") + (completed.stderr or ""),
            )

        # Render notebook to HTML.
        render_command = self._build_notebook_render_command(
            notebook_path=notebook_path,
            notebook_kind=notebook_kind,
            executed_path=artifacts.executed_path,
            html_path=artifacts.html_path,
        )
        render_result = self._run_logged_command(node=node, cmd=render_command)
        if render_result.returncode != 0 or not artifacts.html_path.is_file():
            render_error = self._render_notebook_failure_page(
                html_path=artifacts.html_path,
                task_name=node.task_def.name,
                error_output=(render_result.stdout or "") + (render_result.stderr or ""),
            )
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="failed",
                render_error=render_error,
            )
        else:
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="succeeded",
                render_error=None,
            )

        # Validate and return declared outputs, or fall back to HTML artifact.
        if notebook_expr.outputs is None:
            return self._coerce_return_value(
                task_def=node.task_def, value=str(artifacts.html_path)
            )
        return self._validate_and_return_outputs(
            task_name=node.task_def.name,
            task_def=node.task_def,
            outputs=notebook_expr.outputs,
        )

    def _run_script(self, *, node: _TaskNode, script_expr: ScriptExpr) -> Any:
        """Execute a script task, forwarding task inputs as CLI arguments."""
        assert node.execution_args is not None
        user_log_path = Path(script_expr.log) if script_expr.log is not None else None
        if script_expr.outputs is not None:
            for output_path in self._iter_output_values(script_expr.outputs):
                _remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

        # Resolve the interpreter: use sys.executable for Python to stay in the same env.
        interpreter_cmd = (
            shlex.quote(sys.executable)
            if script_expr.interpreter == "python"
            else shlex.quote(script_expr.interpreter)
        )

        # Build command: interpreter script_path --arg-name value ...
        cmd_parts = [interpreter_cmd, shlex.quote(str(script_expr.path))]
        for name, value in node.execution_args.items():
            option = f"--{name.replace('_', '-')}"
            cmd_parts.extend(
                [shlex.quote(option), shlex.quote(_stringify_notebook_argument(value))]
            )
        cmd = " ".join(cmd_parts)

        completed = self._run_logged_command(node=node, cmd=cmd, user_log_path=user_log_path)
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=node.task_def.name,
                cmd=self._redact_text(cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=script_expr.log,
            )

        if script_expr.outputs is None:
            return None
        return self._validate_and_return_outputs(
            task_name=node.task_def.name,
            task_def=node.task_def,
            outputs=script_expr.outputs,
        )

    def _validate_and_return_outputs(
        self,
        *,
        task_name: str,
        task_def: TaskDef,
        outputs: str | list[str] | AssetResult | list[AssetResult],
    ) -> Any:
        """Validate declared output paths exist and return coerced value."""
        output_paths = self._iter_output_values(outputs)
        missing = [str(path) for path in output_paths if not path.exists()]
        if missing:
            label = missing[0] if len(missing) == 1 else missing
            raise FileNotFoundError(
                f"Task {task_name} completed but did not create declared output {label!r}"
            )
        return self._coerce_return_value(task_def=task_def, value=outputs)

    def _run_logged_command(
        self,
        *,
        node: _TaskNode,
        cmd: str,
        user_log_path: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run one command while appending to provenance logs."""
        for path in (node.stdout_path, node.stderr_path, user_log_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)

        if node.task_def.env is not None and self.backend is not None:
            argv = self.backend.exec_argv(env=node.task_def.env, cmd=cmd)
            use_shell = False
        else:
            argv = cmd
            use_shell = True

        stdout_handle = node.stdout_path.open("a", encoding="utf-8") if node.stdout_path else None
        stderr_handle = node.stderr_path.open("a", encoding="utf-8") if node.stderr_path else None
        user_log_handle = user_log_path.open("a", encoding="utf-8") if user_log_path else None

        def emit_chunk(*, stream: str, chunk: str) -> None:
            if stream == "stdout" and stdout_handle is not None:
                stdout_handle.write(chunk)
                stdout_handle.flush()
            if stream == "stderr" and stderr_handle is not None:
                stderr_handle.write(chunk)
                stderr_handle.flush()
            if user_log_handle is not None:
                user_log_handle.write(chunk)
                user_log_handle.flush()
            self._task_log_emitter(node=node, stream=stream)(chunk)

        try:
            completed, streamed = self._call_run_subprocess(
                argv=argv,
                use_shell=use_shell,
                on_stdout=lambda chunk: emit_chunk(stream="stdout", chunk=chunk),
                on_stderr=lambda chunk: emit_chunk(stream="stderr", chunk=chunk),
            )
            if not streamed:
                if completed.stdout:
                    emit_chunk(stream="stdout", chunk=completed.stdout)
                if completed.stderr:
                    emit_chunk(stream="stderr", chunk=completed.stderr)
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
            if stderr_handle is not None:
                stderr_handle.close()
            if user_log_handle is not None:
                user_log_handle.close()

        return completed

    def _notebook_artifacts(self, *, node: _TaskNode, notebook_kind: str) -> _NotebookArtifacts:
        """Return deterministic artifact paths for one notebook task.

        Parameters
        ----------
        node : _TaskNode
            The task node being executed.
        notebook_kind : str
            Either ``"ipynb"`` (Jupyter/Papermill) or ``"marimo"``.
        """
        task_key = f"task_{node.node_id:04d}"
        root_dir = (
            self.provenance.run_dir / "notebooks"
            if self.provenance is not None
            else Path.cwd() / ".ginkgo" / "notebooks"
        )
        root_dir.mkdir(parents=True, exist_ok=True)
        executed_path = root_dir / f"{task_key}.ipynb" if notebook_kind == "ipynb" else None
        return _NotebookArtifacts(
            root_dir=root_dir,
            html_path=root_dir / f"{task_key}.html",
            executed_path=executed_path,
            params_path=root_dir / f"{task_key}.params.yaml",
        )

    def _prepare_notebook_artifacts(self, *, artifacts: _NotebookArtifacts) -> None:
        """Clear stale notebook artifacts before a fresh execution attempt."""
        artifacts.root_dir.mkdir(parents=True, exist_ok=True)
        for path in (artifacts.html_path, artifacts.executed_path, artifacts.params_path):
            if path is None:
                continue
            if path.exists():
                path.unlink()

    def _build_ipynb_execute_command(
        self,
        *,
        notebook_path: Path,
        executed_path: Path | None,
        params_path: Path,
        resolved_args: dict[str, Any],
    ) -> str:
        """Build the Papermill execution command for one Jupyter notebook."""
        if executed_path is None:
            raise RuntimeError("ipynb notebooks require an executed output path")
        params_path.write_text(
            yaml.safe_dump(
                _serialize_notebook_value(resolved_args),
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return " ".join(
            [
                shlex.quote(sys.executable),
                "-m",
                "papermill",
                shlex.quote(str(notebook_path)),
                shlex.quote(str(executed_path)),
                "-f",
                shlex.quote(str(params_path)),
            ]
        )

    def _build_marimo_execute_command(
        self,
        *,
        notebook_path: Path,
        resolved_args: dict[str, Any],
    ) -> str:
        """Build the command used to execute one marimo notebook script."""
        args: list[str] = [shlex.quote(sys.executable), shlex.quote(str(notebook_path))]
        for name, value in resolved_args.items():
            option = f"--{name.replace('_', '-')}"
            args.extend([shlex.quote(option), shlex.quote(_stringify_notebook_argument(value))])
        return " ".join(args)

    def _build_notebook_render_command(
        self,
        *,
        notebook_path: Path,
        notebook_kind: str,
        executed_path: Path | None,
        html_path: Path,
    ) -> str:
        """Build the HTML render command for one notebook task."""
        if notebook_kind == "ipynb":
            if executed_path is None:
                raise RuntimeError("ipynb notebooks require an executed output path")
            return " ".join(
                [
                    shlex.quote(sys.executable),
                    "-m",
                    "jupyter",
                    "nbconvert",
                    "--to",
                    "html",
                    "--output",
                    shlex.quote(html_path.stem),
                    "--output-dir",
                    shlex.quote(str(html_path.parent)),
                    shlex.quote(str(executed_path)),
                ]
            )

        return " ".join(
            [
                shlex.quote(sys.executable),
                "-m",
                "marimo",
                "export",
                "html",
                shlex.quote(str(notebook_path)),
                "-o",
                shlex.quote(str(html_path)),
            ]
        )

    def _record_notebook_manifest(
        self,
        *,
        node: _TaskNode,
        notebook_kind: str,
        notebook_path: Path,
        notebook_description: str | None,
        executed_path: Path | None,
        rendered_html: Path,
        render_status: str,
        render_error: str | None,
    ) -> None:
        """Persist notebook-specific metadata to the task manifest.

        Parameters
        ----------
        node : _TaskNode
            The task node being recorded.
        notebook_kind : str
            Either ``"ipynb"`` or ``"marimo"``.
        notebook_path : Path
            Resolved path to the source notebook file.
        notebook_description : str | None
            Human-readable description from the task function docstring.
        executed_path : Path | None
            Path to the executed notebook artifact (ipynb only).
        rendered_html : Path
            Path to the rendered HTML artifact.
        render_status : str
            One of ``"pending"``, ``"not_started"``, ``"failed"``, ``"succeeded"``.
        render_error : str | None
            Error message when rendering fails.
        """
        if self.provenance is None:
            return
        extra: dict[str, Any] = {
            "task_type": "notebook",
            "notebook_kind": notebook_kind,
            "notebook_path": str(notebook_path),
            "notebook_description": notebook_description,
            "render_status": render_status,
            "rendered_html": _relativize_to_run_dir(
                run_dir=self.provenance.run_dir,
                path=rendered_html,
            ),
        }
        if executed_path is not None:
            extra["executed_notebook"] = _relativize_to_run_dir(
                run_dir=self.provenance.run_dir,
                path=executed_path,
            )
        if render_error is not None:
            extra["render_error"] = render_error
        elif render_status != "failed":
            extra["render_error"] = None
        self.provenance.update_task_extra(node_id=node.node_id, **extra)

    def _render_notebook_failure_page(
        self,
        *,
        html_path: Path,
        task_name: str,
        error_output: str,
    ) -> str:
        """Write a fallback HTML page when notebook rendering fails."""
        html_path.parent.mkdir(parents=True, exist_ok=True)
        message = error_output.strip() or "Notebook HTML export failed."
        html_path.write_text(
            "\n".join(
                [
                    "<html><body>",
                    f"<h1>{task_name}</h1>",
                    "<p>Notebook execution succeeded, but HTML export failed.</p>",
                    "<pre>",
                    _escape_html(message),
                    "</pre>",
                    "</body></html>",
                ]
            ),
            encoding="utf-8",
        )
        return message

    def _validate_declared_envs(self) -> None:
        """Raise before any work starts if a declared env cannot be resolved.

        Only statically registered nodes are checked here. Dynamic nodes
        (discovered mid-run via conditional branching) are validated when
        ``_prepare_node`` is called for them.
        """
        # Foreign execution environments only support shell-like tasks.
        for node in self._nodes.values():
            if node.task_def.env is not None and node.task_def.kind not in {
                "notebook",
                "script",
                "shell",
            }:
                raise TypeError(
                    f"{node.task_def.name} uses env {node.task_def.env!r} "
                    "but is declared with kind='python'. Foreign environments "
                    "only support driver tasks — use @task('shell'), "
                    "@task('notebook'), or @task('script')."
                )

        if self.backend is None:
            return

        env_names: set[str] = {
            node.task_def.env for node in self._nodes.values() if node.task_def.env is not None
        }
        if env_names:
            self.backend.validate_envs(env_names=env_names)

    def _validate_declared_secrets(self) -> None:
        """Raise before execution if any statically declared secrets are missing."""
        if self.secret_resolver is None:
            return

        missing: list[SecretRef] = []
        seen: set[SecretRef] = set()
        for node in self._nodes.values():
            for ref in collect_secret_refs(node.expr.args):
                if ref in seen:
                    continue
                seen.add(ref)
                try:
                    self.secret_resolver.resolve(ref=ref)
                except BaseException:
                    missing.append(ref)

        if missing:
            rendered = ", ".join(f"{ref.backend}:{ref.name}" for ref in sorted(missing, key=str))
            raise RuntimeError(f"Missing secrets: {rendered}")

    def validate(self, expr: Any) -> None:
        """Build the static task graph and validate import/env/input constraints."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        self._validate_declared_envs()
        self._validate_declared_secrets()

        for node in self._nodes.values():
            self._validate_task_importable(task_def=node.task_def)
            self._validate_static_inputs(node=node)

    def _validate_inputs(self, *, task_def: TaskDef, resolved_args: dict[str, Any]) -> None:
        """Validate resolved task inputs against Ginkgo path types."""
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir or name not in resolved_args:
                continue
            self._validate_annotated_value(
                annotation=annotation,
                value=resolved_args[name],
                label=f"{task_def.name}.{name}",
            )

    def _validate_return_value(self, *, task_def: TaskDef, value: Any) -> None:
        """Validate a task return value when it uses a Ginkgo path type."""
        annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        self._validate_annotated_value(
            annotation=annotation,
            value=value,
            label=f"{task_def.name}.return",
        )

    def _validate_annotated_value(self, *, annotation: Any, value: Any, label: str) -> None:
        """Validate a value for direct and container-wrapped Ginkgo types."""
        if annotation in {None, Any}:
            return

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_annotations = get_args(annotation)
            inner_annotation = inner_annotations[0] if inner_annotations else Any
            for index, item in enumerate(value):
                self._validate_annotated_value(
                    annotation=inner_annotation,
                    value=item,
                    label=f"{label}[{index}]",
                )
            return

        if isinstance(value, list | tuple):
            for index, item in enumerate(value):
                self._validate_annotated_value(
                    annotation=annotation,
                    value=item,
                    label=f"{label}[{index}]",
                )
            return

        if annotation is file:
            if isinstance(value, AssetRef) and value.kind == "file":
                return
            if _is_remote_path_value(value):
                return
            self._validate_file_path(path=value, label=label)
            return

        if annotation is folder:
            if isinstance(value, AssetRef) and value.kind == "folder":
                return
            if _is_remote_path_value(value):
                return
            self._validate_folder_path(path=value, label=label)
            return

        if annotation is tmp_dir:
            self._validate_tmp_dir_path(path=value, label=label)
            return

        if _is_path_annotation(annotation):
            if not Path(value).exists():
                raise FileNotFoundError(f"{label} must exist: {str(value)!r}")

    def _validate_file_path(self, *, path: Any, label: str) -> None:
        """Validate a concrete file path argument or return value."""
        path_str = str(path)
        if " " in path_str:
            raise ValueError(f"{label} must not contain spaces: {path_str!r}")

        if not Path(path_str).is_file():
            raise FileNotFoundError(f"{label} must exist and be a file: {path_str!r}")

    def _validate_folder_path(self, *, path: Any, label: str) -> None:
        """Validate a concrete folder path argument or return value."""
        path_str = str(path)
        if " " in path_str:
            raise ValueError(f"{label} must not contain spaces: {path_str!r}")

        path_obj = Path(path_str)
        if not path_obj.exists() or not path_obj.is_dir():
            raise FileNotFoundError(f"{label} must exist and be a directory: {path_str!r}")

    def _validate_tmp_dir_path(self, *, path: Any, label: str) -> None:
        """Validate an auto-created scratch directory."""
        path_obj = Path(str(path))
        if not path_obj.exists() or not path_obj.is_dir():
            raise FileNotFoundError(f"{label} tmp_dir does not exist: {str(path)!r}")

    def _coerce_return_value(self, *, task_def: TaskDef, value: Any) -> Any:
        """Coerce string returns into the declared Ginkgo path marker type."""
        annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        return self._coerce_annotated_value(annotation=annotation, value=value)

    def _coerce_annotated_value(self, *, annotation: Any, value: Any) -> Any:
        """Coerce values recursively for direct and container-wrapped path types."""
        if annotation in {None, Any}:
            return value

        origin = get_origin(annotation)
        if origin is list and isinstance(value, list):
            inner_annotations = get_args(annotation)
            inner_annotation = inner_annotations[0] if inner_annotations else Any
            return [
                self._coerce_annotated_value(annotation=inner_annotation, value=item)
                for item in value
            ]

        if origin is tuple and isinstance(value, tuple):
            inner_annotations = get_args(annotation)
            if len(inner_annotations) == 2 and inner_annotations[1] is Ellipsis:
                inner_annotation = inner_annotations[0]
                return tuple(
                    self._coerce_annotated_value(annotation=inner_annotation, value=item)
                    for item in value
                )

            if inner_annotations and len(inner_annotations) == len(value):
                return tuple(
                    self._coerce_annotated_value(annotation=item_annotation, value=item)
                    for item_annotation, item in zip(inner_annotations, value, strict=True)
                )

            inner_annotation = inner_annotations[0] if inner_annotations else Any
            return tuple(
                self._coerce_annotated_value(annotation=inner_annotation, value=item)
                for item in value
            )

        if isinstance(value, list):
            return [
                self._coerce_annotated_value(annotation=annotation, value=item) for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self._coerce_annotated_value(annotation=annotation, value=item) for item in value
            )

        if annotation in {file, folder, tmp_dir} and isinstance(value, str):
            return annotation(value)

        if _is_path_annotation(annotation) and isinstance(value, str):
            return Path(value)

        return value

    def _materialize_asset_results(self, *, node: _TaskNode, value: Any) -> Any:
        """Register nested asset sentinels and replace them with asset refs."""
        node.asset_versions = []
        parent_refs = self._parent_asset_refs(node=node)
        return self._replace_asset_results(node=node, value=value, parent_refs=parent_refs)

    def _replace_asset_results(
        self,
        *,
        node: _TaskNode,
        value: Any,
        parent_refs: list[AssetRef],
    ) -> Any:
        """Recursively replace nested asset sentinels with asset refs."""
        if isinstance(value, AssetResult):
            asset_ref, asset_version = self._register_asset_result(
                node=node,
                asset_result=value,
                parent_refs=parent_refs,
            )
            node.asset_versions.append(asset_version)
            return asset_ref

        if isinstance(value, list):
            return [
                self._replace_asset_results(node=node, value=item, parent_refs=parent_refs)
                for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self._replace_asset_results(node=node, value=item, parent_refs=parent_refs)
                for item in value
            )

        return value

    def _register_asset_result(
        self,
        *,
        node: _TaskNode,
        asset_result: AssetResult,
        parent_refs: list[AssetRef],
    ) -> tuple[AssetRef, AssetVersion]:
        """Store one file asset and register its immutable catalog version."""
        source_path = asset_result.path
        if not source_path.is_file():
            raise FileNotFoundError(
                f"{node.task_def.name}.return asset file must exist: {str(source_path)!r}"
            )

        record = self._cache_store._artifact_store.store(src_path=source_path)

        asset_name = asset_result.name or node.task_def.fn.__name__
        version = make_asset_version(
            key=_asset_key_for_result(name=asset_name, kind=asset_result.kind),
            kind=asset_result.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self._run_id,
            producer_task=node.task_def.name,
            metadata=asset_result.metadata,
        )
        self._asset_store.register_version(version=version)
        asset_ref = asset_ref_from_version(
            version=version,
            artifact_path=self._cache_store._artifact_store.artifact_path(
                artifact_id=record.artifact_id
            ),
        )
        if parent_refs:
            self._asset_store.record_lineage(child=asset_ref, parents=parent_refs)
        return asset_ref, version

    def _parent_asset_refs(self, *, node: _TaskNode) -> list[AssetRef]:
        """Collect unique upstream asset references consumed by one task."""
        if node.resolved_args is None:
            return []
        unique: dict[tuple[str, str, str], AssetRef] = {}
        for asset_ref in collect_asset_refs(node.resolved_args):
            unique[(asset_ref.namespace, asset_ref.name, asset_ref.version_id)] = asset_ref
        return list(unique.values())

    def _iter_output_values(
        self,
        output: str | list[str] | tuple[str, ...] | AssetResult | list[AssetResult],
    ) -> list[Path]:
        """Return concrete filesystem paths from declared output values."""
        if isinstance(output, AssetResult):
            return [output.path]
        if isinstance(output, str):
            return [Path(output)]
        paths: list[Path] = []
        for item in output:
            if isinstance(item, AssetResult):
                paths.append(item.path)
            else:
                paths.append(Path(item))
        return paths

    def _iter_shell_output_paths(
        self,
        output: str | list[str] | tuple[str, ...] | AssetResult | list[AssetResult],
    ) -> list[Path]:
        """Return concrete output paths for a shell task declaration."""
        return self._iter_output_values(output)

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
            self._validate_return_value(task_def=task_def, value=value)
        except (FileNotFoundError, ValueError):
            return False

        return True

    def _record_task_metadata(self, node: _TaskNode) -> None:
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

    def _record_task_failure(self, *, node: _TaskNode, exc: BaseException) -> None:
        """Persist task failure details to the run manifest."""
        if self.provenance is None:
            return
        self.provenance.mark_failed(
            node_id=node.node_id,
            task_name=node.task_def.name,
            env=node.task_def.env,
            exc=exc,
            failure=_classify_failure(exc=exc),
        )

    def _redact_text(self, text: str, *, secret_values: tuple[str, ...]) -> str:
        """Redact known secret values from text."""
        return redact_text(text=text, secret_values=secret_values)

    def _sanitize_exception(
        self,
        *,
        exc: BaseException,
        secret_values: tuple[str, ...],
    ) -> BaseException:
        """Return an exception with redacted message text."""
        if not secret_values:
            return exc

        message = self._redact_text(str(exc), secret_values=secret_values)
        try:
            exc.args = (message,)
        except Exception:
            return RuntimeError(message)

        if hasattr(exc, "output"):
            try:
                exc.output = self._redact_text(str(exc.output), secret_values=secret_values)
            except Exception:
                pass
        if hasattr(exc, "cmd"):
            try:
                exc.cmd = self._redact_text(str(exc.cmd), secret_values=secret_values)
            except Exception:
                pass
        return exc

    def _display_label_for(self, *, node: _TaskNode) -> str | None:
        """Return a richer CLI label for mapped tasks once args are resolved."""
        if not node.expr.mapped or node.resolved_args is None:
            return None

        if node.expr.display_label_parts:
            base_name = node.task_def.name.rsplit(".", 1)[-1]
            return f"{base_name}[{','.join(node.expr.display_label_parts)}]"

        label_key = _first_label_param_name(task_def=node.task_def)
        if label_key is None or label_key not in node.resolved_args:
            return None

        rendered = _render_label_value(node.resolved_args[label_key])
        if rendered is None:
            return None

        base_name = node.task_def.name.rsplit(".", 1)[-1]
        return f"{base_name}[{rendered}]"

    def _artifact_index_for(self, *, node: _TaskNode, value: Any) -> list[dict[str, Any]]:
        """Return a compact typed artifact summary for one task result."""
        annotation = node.task_def.type_hints.get(
            "return", node.task_def.signature.return_annotation
        )
        return _artifact_index(annotation=annotation, value=value)

    def _asset_index_for(self, *, value: Any) -> list[dict[str, Any]]:
        """Return recorded asset summaries for one task result."""
        return [
            self._render_asset_ref(asset_ref=asset_ref) for asset_ref in collect_asset_refs(value)
        ]

    def _render_asset_ref(self, *, asset_ref: AssetRef) -> dict[str, Any]:
        """Render one asset reference for provenance and events."""
        return {
            "artifact_id": asset_ref.artifact_id,
            "artifact_path": asset_ref.artifact_path,
            "asset_key": str(asset_ref.key),
            "content_hash": asset_ref.content_hash,
            "kind": asset_ref.kind,
            "metadata": dict(asset_ref.metadata),
            "name": asset_ref.name,
            "namespace": asset_ref.namespace,
            "version_id": asset_ref.version_id,
        }

    @property
    def _run_id(self) -> str:
        """Return the active run id, or a placeholder outside live runs."""
        if self.provenance is not None:
            return self.provenance.run_id
        return "validation"

    def _emit_event(self, event: object) -> None:
        """Emit a runtime event when an event bus is attached."""
        if self.event_bus is not None:
            self.event_bus.emit(event)
        elif isinstance(event, GinkgoEvent):
            self._emit_legacy_log(event)

    def _emit_legacy_log(self, event: GinkgoEvent) -> None:
        """Emit the pre-Phase-4 stderr task stream for compatibility."""
        payload: dict[str, object] | None = None
        if isinstance(event, TaskStarted):
            payload = {
                "task": event.task_name,
                "status": "running",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
                "max_attempts": event.resources.get("max_attempts"),
            }
        elif isinstance(event, TaskStaging):
            payload = {
                "task": event.task_name,
                "status": "staging",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
                "remote_input_count": event.remote_input_count,
            }
        elif isinstance(event, TaskCacheHit):
            payload = {
                "task": event.task_name,
                "status": "cached",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
            }
        elif isinstance(event, TaskRetrying):
            payload = {
                "task": event.task_name,
                "status": "waiting",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
                "retries_remaining": event.retries_remaining,
            }
        elif isinstance(event, TaskCompleted) and event.status == "success":
            payload = {
                "task": event.task_name,
                "status": "succeeded",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
            }
        elif isinstance(event, TaskFailed):
            payload = {
                "task": event.task_name,
                "status": "failed",
                "node_id": int(event.task_id.split("_")[-1]),
                "attempt": event.attempt,
                "exit_code": event.exit_code,
            }

        if payload is not None:
            print(json.dumps(payload, sort_keys=True), file=self._stderr)

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
        _driver_sentinels = (ShellExpr, NotebookExpr, ScriptExpr)
        if self._failure is not None and (
            isinstance(completed_value, _driver_sentinels)
            or self._contains_dynamic_expression(completed_value)
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
                    "with kind='python'. Use @task(kind='shell'), @task('notebook'), or "
                    "@task('script') for the appropriate task kind."
                )

            self._validate_process_safe_value(
                value=completed_value,
                label=f"{node.task_def.name}.return",
            )
            self._cleanup_transport(node)

            dynamic_dependencies = self._register_value(completed_value)
            if dynamic_dependencies:
                node.state = "waiting_dynamic"
                node.dynamic_template = completed_value
                node.dynamic_dependency_ids = dynamic_dependencies
                self._record_task_metadata(node)
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
                self._run_shell,
                node=node,
                shell_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        if isinstance(completed_value, NotebookExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._run_notebook_expr,
                node=node,
                notebook_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        if isinstance(completed_value, ScriptExpr):
            self._cleanup_transport(node)
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._run_script,
                node=node,
                script_expr=completed_value,
            )
            self._running_futures[future] = (node.node_id, "shell")
            return

        dynamic_dependencies = self._register_value(completed_value)
        if dynamic_dependencies:
            self._cleanup_transport(node)
            node.state = "waiting_dynamic"
            node.dynamic_template = completed_value
            node.dynamic_dependency_ids = dynamic_dependencies
            self._record_task_metadata(node)
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
        _expected = {"shell": "shell(...)", "notebook": "notebook(...)", "script": "script(...)"}
        raise TypeError(
            f"{node.task_def.name} is declared with kind={kind!r} and must return "
            f"{_expected.get(kind, 'the appropriate sentinel')} or dynamic task expressions."
        )


class ShellTaskError(RuntimeError):
    """Shell task execution failure."""

    def __init__(
        self,
        *,
        task_name: str,
        cmd: str,
        exit_code: int,
        output: str,
        log: str | None,
    ) -> None:
        self.exit_code = exit_code

        details = f"Shell task {task_name} failed with exit code {exit_code}: {cmd}"
        if log is not None:
            details = f"{details} (log: {log})"
        elif output:
            details = f"{details}\n{output.strip()}"

        super().__init__(details)


class NotebookTaskError(RuntimeError):
    """Notebook task execution failure."""

    def __init__(
        self,
        *,
        task_name: str,
        phase: str,
        cmd: str,
        exit_code: int,
        output: str,
    ) -> None:
        self.exit_code = exit_code
        details = (
            f"Notebook task {task_name} failed during {phase} with exit code {exit_code}: {cmd}"
        )
        if output:
            details = f"{details}\n{output.strip()}"
        super().__init__(details)


def _fn_description(fn: Any) -> str | None:
    """Return the first line of a function's docstring, or None."""
    import inspect

    return inspect.getdoc(fn)


def _serialize_notebook_value(value: Any) -> Any:
    """Convert runtime values into YAML/CLI-safe notebook parameters."""
    if isinstance(value, Path | file | folder | tmp_dir):
        return str(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list):
        return [_serialize_notebook_value(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_notebook_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(_serialize_notebook_value(key)): _serialize_notebook_value(item)
            for key, item in value.items()
        }
    return value


def _stringify_notebook_argument(value: Any) -> str:
    """Render one notebook argument for a CLI invocation."""
    serialized = _serialize_notebook_value(value)
    if isinstance(serialized, str):
        return serialized
    return json.dumps(serialized, sort_keys=True)


def _relativize_to_run_dir(*, run_dir: Path, path: Path) -> str:
    """Return a run-relative path when possible."""
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def _escape_html(value: str) -> str:
    """Escape plain text for a tiny fallback HTML page."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _is_path_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a pathlib path type."""
    return isinstance(annotation, type) and issubclass(annotation, Path)


def _first_label_param_name(*, task_def: TaskDef) -> str | None:
    """Return the first user-declared parameter name for CLI labeling."""
    for name, parameter in task_def.signature.parameters.items():
        annotation = task_def.type_hints.get(name, parameter.annotation)
        if annotation is tmp_dir:
            continue
        return name
    return None


def _render_label_value(value: Any) -> str | None:
    """Return a compact string for a mapped-task display label."""
    if isinstance(value, Path):
        text = value.name or str(value)
    elif isinstance(value, (str, int, float, bool)):
        text = str(value)
    else:
        text = repr(value)

    compact = " ".join(text.split()).strip()
    if not compact:
        return None
    if len(compact) > 24:
        compact = f"{compact[:21]}..."
    return compact


def _remove_declared_output(path: Path) -> None:
    """Remove one pre-existing declared output before task execution."""
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)


def _task_id_for_node(node_id: int) -> str:
    """Return the stable task identifier for a node."""
    return f"task_{node_id:04d}"


def _artifact_index(annotation: Any, value: Any, *, name: str = "return") -> list[dict[str, Any]]:
    """Return a compact typed output index for a task result."""
    if value is None:
        return []

    origin = get_origin(annotation)
    if origin in {list, tuple} and isinstance(value, (list, tuple)):
        inner_args = get_args(annotation)
        inner_annotation = inner_args[0] if inner_args else Any
        outputs: list[dict[str, Any]] = []
        for index, item in enumerate(value):
            outputs.extend(_artifact_index(inner_annotation, item, name=f"{name}[{index}]"))
        return outputs

    if isinstance(value, (list, tuple)):
        outputs: list[dict[str, Any]] = []
        for index, item in enumerate(value):
            outputs.extend(_artifact_index(annotation, item, name=f"{name}[{index}]"))
        return outputs

    if isinstance(value, AssetRef):
        return [
            {
                "name": name,
                "type": "asset",
                "asset_key": str(value.key),
                "version_id": value.version_id,
                "artifact_id": value.artifact_id,
                "path": value.artifact_path,
            }
        ]

    if annotation is file or isinstance(value, file):
        return [{"name": name, "type": "file", "path": str(value)}]

    if annotation is folder or isinstance(value, folder):
        return [{"name": name, "type": "folder", "path": str(value)}]

    if isinstance(value, Path):
        return [{"name": name, "type": "path", "path": str(value)}]

    shape = getattr(value, "shape", None)
    dtype = getattr(value, "dtype", None)
    if shape is not None and dtype is not None:
        return [
            {
                "name": name,
                "type": "ndarray",
                "shape": list(shape),
                "dtype": str(dtype),
                "summary": summarise_value(value),
            }
        ]

    if value.__class__.__module__.startswith("pandas") and value.__class__.__name__ == "DataFrame":
        return [
            {
                "name": name,
                "type": "dataframe",
                "shape": [int(value.shape[0]), int(value.shape[1])],
                "summary": summarise_value(value),
            }
        ]

    return [{"name": name, "type": "value", "summary": summarise_value(value)}]


def _is_remote_path_value(value: Any) -> bool:
    """Return whether a value is a remote reference or supported remote URI."""
    if isinstance(value, RemoteRef):
        return True
    return isinstance(value, str) and is_remote_uri(value)


def _count_remote_inputs(value: Any) -> int:
    """Count nested remote refs and supported remote URI strings."""
    if isinstance(value, RemoteRef):
        return 1
    if isinstance(value, str) and is_remote_uri(value):
        return 1
    if isinstance(value, list | tuple):
        return sum(_count_remote_inputs(item) for item in value)
    if isinstance(value, dict):
        return sum(_count_remote_inputs(item) for item in value.values())
    return 0


def _asset_key_for_result(*, name: str, kind: str) -> AssetKey:
    """Build one asset key for a supported asset result."""
    if kind != "file":
        raise ValueError(f"Unsupported asset kind in Phase 7: {kind!r}")
    return AssetKey(namespace="file", name=name)


def _remote_ref_identity(*, ref: RemoteRef) -> str:
    """Return a stable in-flight identity key for a remote ref."""
    return "|".join(
        [
            type(ref).__name__,
            ref.scheme,
            ref.bucket,
            ref.key,
            ref.version_id or "",
        ]
    )


def _resolve_staging_jobs(*, jobs: int) -> int:
    """Return the configured staging concurrency."""
    configured = os.environ.get("GINKGO_STAGING_JOBS")
    if configured:
        return _parse_positive_int(value=configured, label="GINKGO_STAGING_JOBS")

    config = load_runtime_config(project_root=Path.cwd())
    remote_config = config.get("remote", {})
    if isinstance(remote_config, dict) and remote_config.get("staging_jobs") is not None:
        return _parse_positive_int(
            value=remote_config["staging_jobs"],
            label="remote.staging_jobs",
        )

    return max(1, min(jobs, 4))


def _parse_positive_int(*, value: Any, label: str) -> int:
    """Parse a required positive integer config value."""
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _classify_failure(*, exc: BaseException) -> dict[str, Any]:
    """Return a structured task failure summary."""
    message = str(exc)
    if isinstance(exc, CycleError):
        kind = "cycle_detected"
    elif isinstance(exc, CodecError):
        kind = "serialization_error"
    elif isinstance(exc, (ShellTaskError, NotebookTaskError)):
        kind = "shell_command_error"
    elif isinstance(exc, FileNotFoundError):
        kind = "missing_input" if "did not create" not in message else "output_validation_error"
    elif isinstance(exc, (TypeError, ValueError)):
        kind = "user_code_error"
    else:
        exc_name = exc.__class__.__name__.lower()
        if "env" in exc_name or "container" in exc_name:
            kind = "environment_error"
        elif "cache" in exc_name:
            kind = "cache_error"
        else:
            kind = "scheduler_error"

    return {
        "kind": kind,
        "message": message,
        "retryable": False,
        "code": exc.__class__.__name__,
    }
