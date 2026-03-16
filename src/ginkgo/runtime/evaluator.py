"""Concurrent evaluator for Ginkgo expressions."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
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
from multiprocessing import get_context
from pathlib import Path
from threading import Lock, current_thread, main_thread
from types import FrameType
from typing import Any, get_args, get_origin

from ginkgo.core.expr import Expr, ExprList
from ginkgo.core.shell import ShellExpr
from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.cache import MISSING, CacheStore
from ginkgo.runtime.module_loader import import_roots_for_path, load_module, resolve_module_file
from ginkgo.runtime.provenance import RunProvenanceRecorder
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset
from ginkgo.runtime.value_codec import (
    CodecError,
    decode_value,
    encode_value,
    ensure_serializable,
)
from ginkgo.runtime.worker import _task_log_context, run_task

# Inline Python code executed via ``python -c`` inside a Pixi environment.
# Using ``-c`` avoids adding the script directory to sys.path, which would
# otherwise make the worker's package-local modules import precedence-sensitive.
#
# Dynamic results (ShellExpr, Expr, ExprList) are not JSON-serializable, so
# when run_task returns one, we pickle+base64 encode it under the special
# encoding "pixi_direct_pickled" for the main process to decode.
_PIXI_WORKER_C = (
    "import sys,json,pathlib,base64,pickle;"
    "p=json.loads(pathlib.Path(sys.argv[1]).read_bytes());"
    "[sys.path.insert(0,x) for x in p.get('ginkgo_import_roots',[]) if x not in sys.path];"
    "from ginkgo.runtime.worker import run_task;"
    "r=dict(run_task(p));"
    "enc=r.get('result_encoding');"
    "r['result'],r['result_encoding']=("
    "base64.b64encode(pickle.dumps(r['result'],5)).decode(),'pixi_direct_pickled'"
    ") if r.get('ok') and enc=='direct' else (r.get('result'),enc);"
    "pathlib.Path(sys.argv[2]).write_text(json.dumps(r))"
)


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
    pixi_registry: PixiRegistry | None = None,
    provenance: RunProvenanceRecorder | None = None,
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
    pixi_registry : PixiRegistry | None
        Registry for resolving Pixi environments. When ``None``, tasks with
        ``env=`` will raise at dispatch time.

    Returns
    -------
    Any
        The concrete result of evaluating the input.
    """
    return _ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        pixi_registry=pixi_registry,
        provenance=provenance,
    ).evaluate(expr)


@dataclass(kw_only=True)
class _TaskNode:
    """Internal task node tracked by the concurrent scheduler."""

    node_id: int
    expr: Expr
    dependency_ids: set[int]
    state: str = "pending"
    resolved_args: dict[str, Any] | None = None
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

    @property
    def task_def(self) -> TaskDef:
        """Return the task definition for the node."""
        return self.expr.task_def


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
    pixi_registry: PixiRegistry | None = None
    provenance: RunProvenanceRecorder | None = None
    _cache_store: CacheStore = field(init=False, repr=False)
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
    _subprocess_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _active_subprocesses: dict[int, subprocess.Popen[str]] = field(
        default_factory=dict,
        init=False,
        repr=False,
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

        self._cache_store = CacheStore(pixi_registry=self.pixi_registry)

    def evaluate(self, expr: Any) -> Any:
        """Resolve a root expression or nested container concurrently."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        if not self._root_dependency_ids:
            return self._materialize(expr)

        # Validate all statically declared environments before any work starts.
        self._validate_declared_envs()

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
            signals = stack.enter_context(_SignalMonitor())
            self._python_executor = python_executor
            self._shell_executor = shell_executor
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

                    raise RuntimeError("Scheduler reached a deadlock with unresolved tasks")
            finally:
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
            for expr in value:
                dependencies.add(
                    self._register_expr(
                        expr,
                        expr_stack=expr_stack,
                        task_path=task_path,
                    )
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
            include_tmp_dirs=False,
        )
        self._validate_inputs(task_def=node.task_def, resolved_args=resolved_args)
        self._validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )

        cache_key, input_hashes = self._cache_store.build_cache_key(
            task_def=node.task_def,
            resolved_args=resolved_args,
        )
        node.resolved_args = resolved_args
        node.cache_key = cache_key
        node.input_hashes = input_hashes
        node.display_label = self._display_label_for(node=node)
        self._record_task_metadata(node)
        cached_result = self._cache_store.load(cache_key=cache_key)
        if cached_result is not MISSING and self._is_valid_cached_result(
            task_def=node.task_def,
            value=cached_result,
        ):
            node.result = cached_result
            node.state = "completed"
            if self.provenance is not None:
                self.provenance.mark_cached(
                    node_id=node.node_id,
                    task_name=node.task_def.name,
                    env=node.task_def.env,
                    value=cached_result,
                )
            self._log(
                task=node.task_def.name,
                status="cached",
                exit_code=0,
                node_id=node.node_id,
                display_label=node.display_label,
            )
            return

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

    def _prepare_task_environment(self, *, node: _TaskNode) -> None:
        """Materialize any external execution environment required by a task."""
        if node.task_def.env is None or self.pixi_registry is None:
            return

        self.pixi_registry.prepare(env=node.task_def.env)

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
                    task_id=node.node_id,
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
            node.state = "running"
            self._log(
                task=node.task_def.name,
                status="running",
                node_id=node.node_id,
                display_label=node.display_label,
                attempt=node.attempt,
                max_attempts=node.task_def.retries + 1,
            )
            if self.provenance is not None:
                self.provenance.mark_running(
                    node_id=node.node_id,
                    task_name=node.task_def.name,
                    env=node.task_def.env,
                    attempt=node.attempt,
                    retries=node.task_def.retries,
                )
            node.resolved_args = self._resolve_task_args(
                expr=node.expr,
                task_def=node.task_def,
                include_tmp_dirs=True,
                existing_args=node.resolved_args,
                tmp_paths=node.tmp_paths,
            )
            self._record_task_metadata(node)
            self._validate_task_contract(node=node)

            if node.task_def.kind == "shell":
                assert self._shell_executor is not None
                future = self._shell_executor.submit(
                    self._run_driver_task,
                    node=node,
                )
                self._running_futures[future] = (node_id, "driver")
                continue

            node.transport_path = Path(
                tempfile.mkdtemp(prefix=f"ginkgo-transport-{node.node_id}-")
            )
            payload = self._build_worker_payload(node=node)

            if node.task_def.env is not None and self.pixi_registry is not None:
                # Run the Python task body inside the Pixi environment via a subprocess.
                assert self._shell_executor is not None
                future = self._shell_executor.submit(
                    self._run_pixi_python_task,
                    node=node,
                    payload=payload,
                )
                self._running_futures[future] = (node_id, "pixi_python")
            else:
                future = python_executor.submit(run_task, payload)
                self._running_futures[future] = (node_id, "python")

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
                if phase in {"python", "pixi_python"}:
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

    def _handle_completed_driver_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Handle the result returned from a driver-executed task wrapper."""
        self._handle_task_body_result(node=node, completed_value=completed_value)

    def _handle_completed_shell_phase(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Handle the result produced by the shell executor."""
        final_value = self._finalize_result_value(node=node, value=completed_value)
        self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)

    def _handle_task_exception(self, *, node: _TaskNode, exc: BaseException) -> None:
        """Either retry a failed task attempt or fail the run."""
        if self._failure is None and self._should_retry(node=node):
            self._schedule_retry(node=node, exc=exc)
            return

        node.state = "failed"
        self._cleanup_transport(node)
        if self._failure is None:
            self._failure = exc
            self._cancel_pending_futures()
        self._record_task_failure(node=node, exc=exc)
        self._log(
            task=node.task_def.name,
            status="failed",
            exit_code=getattr(exc, "exit_code", None),
            node_id=node.node_id,
            display_label=node.display_label,
            attempt=node.attempt,
            max_attempts=node.task_def.retries + 1,
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
        node.cache_key = None
        node.input_hashes = None
        node.threads = 1
        node.memory_gb = 0
        node.tmp_paths = []
        node.transport_path = None
        node.dynamic_template = None
        node.dynamic_dependency_ids.clear()

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
        self._log(
            task=node.task_def.name,
            status="waiting",
            exit_code=getattr(exc, "exit_code", None),
            node_id=node.node_id,
            display_label=node.display_label,
            attempt=node.attempt,
            max_attempts=node.task_def.retries + 1,
            retries_remaining=retries_remaining,
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
        if self.provenance is not None:
            self.provenance.mark_succeeded(
                node_id=node.node_id,
                task_name=node.task_def.name,
                env=node.task_def.env,
                value=value,
            )
        self._log(
            task=node.task_def.name,
            status="succeeded",
            exit_code=0,
            node_id=node.node_id,
            display_label=node.display_label,
            attempt=node.attempt,
            max_attempts=node.task_def.retries + 1,
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
                resolved_args[name] = self._materialize(expr.args[name])
                continue

            if parameter.default is not parameter.empty:
                resolved_args[name] = parameter.default
                continue

            raise TypeError(f"{task_def.fn.__name__}() missing required argument: '{name}'")

        return resolved_args

    def _materialize(self, value: Any) -> Any:
        """Materialize a nested value using completed task-node results."""
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

    def _cancel_pending_futures(self) -> None:
        """Cancel queued futures that have not started yet."""
        for future in self._running_futures:
            future.cancel()

    def _interrupt_running_work(self) -> None:
        """Stop queued and active work after an external interrupt."""
        self._cancel_pending_futures()
        self._terminate_active_subprocesses()
        self._shutdown_shell_executor()
        self._shutdown_python_executor()

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

    def _run_subprocess(
        self,
        *,
        argv: str | list[str],
        use_shell: bool,
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

    def _running_cores(self) -> int:
        """Return the core footprint of currently running tasks."""
        return sum(self._nodes[node_id].threads for node_id, _ in self._running_futures.values())

    def _running_memory_gb(self) -> int:
        """Return the declared memory footprint of currently running tasks."""
        return sum(self._nodes[node_id].memory_gb for node_id, _ in self._running_futures.values())

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
        assert node.resolved_args is not None
        worker_module_file = resolve_module_file("ginkgo.runtime.worker")
        assert worker_module_file is not None
        return {
            "args": {
                name: encode_value(value, base_dir=node.transport_path)
                for name, value in node.resolved_args.items()
            },
            "ginkgo_import_roots": import_roots_for_path(worker_module_file),
            "stdout_path": str(node.stdout_path) if node.stdout_path is not None else None,
            "stderr_path": str(node.stderr_path) if node.stderr_path is not None else None,
            "env": node.task_def.env,
            "module": node.task_def.fn.__module__,
            "module_file": resolve_module_file(node.task_def.fn.__module__),
            "task_kind": node.task_def.kind,
            "task_name": node.task_def.fn.__name__,
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
        self._validate_task_preconditions(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
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
            self._validate_annotated_value(
                annotation=annotation,
                value=value,
                label=f"{node.task_def.name}.{name}",
            )

    def _contains_dynamic_expression(self, value: Any) -> bool:
        """Return whether a nested value contains unresolved expressions."""
        if isinstance(value, (Expr, ExprList)):
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
        if isinstance(value, (Expr, ExprList, ShellExpr)):
            return
        try:
            ensure_serializable(value, label=label)
        except CodecError as exc:
            raise TypeError(str(exc)) from exc

    def _finalize_result_value(self, *, node: _TaskNode, value: Any) -> Any:
        """Coerce and validate a fully resolved task result."""
        coerced = self._coerce_return_value(task_def=node.task_def, value=value)
        self._validate_return_value(task_def=node.task_def, value=coerced)
        return coerced

    def _run_shell(self, *, node: _TaskNode, shell_expr: ShellExpr) -> Any:
        """Execute a shell command and return its declared output path or paths."""
        task_def = node.task_def
        user_log_path = Path(shell_expr.log) if shell_expr.log is not None else None

        # Ensure parent directories for all log paths.
        for path in (node.stdout_path, node.stderr_path, user_log_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)

        # Ensure declared output parents exist before running the command.
        for output_path in self._iter_shell_output_paths(shell_expr.output):
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Build the argv: wrap through pixi when the task declares an env.
        if task_def.env is not None and self.pixi_registry is not None:
            argv = self.pixi_registry.shell_argv(env=task_def.env, cmd=shell_expr.cmd)
            use_shell = False
        else:
            argv = shell_expr.cmd
            use_shell = True

        completed = self._run_subprocess(argv=argv, use_shell=use_shell)
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""

        # Write stdout and stderr to separate provenance logs.
        if node.stdout_path is not None and stdout_text:
            with node.stdout_path.open("a", encoding="utf-8") as handle:
                handle.write(stdout_text)
        if node.stderr_path is not None and stderr_text:
            with node.stderr_path.open("a", encoding="utf-8") as handle:
                handle.write(stderr_text)

        # User-specified log gets combined output for backwards compatibility.
        if user_log_path is not None and (stdout_text or stderr_text):
            with user_log_path.open("a", encoding="utf-8") as handle:
                handle.write(stdout_text + stderr_text)

        combined_output = stdout_text + stderr_text
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=task_def.name,
                cmd=shell_expr.cmd,
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

    def _run_pixi_python_task(self, *, node: _TaskNode, payload: dict[str, Any]) -> dict[str, Any]:
        """Run a Python task body inside its declared Pixi environment.

        Serializes the worker payload to disk, invokes ``pixi_worker.py``
        via the environment's Python interpreter, and deserializes the result.

        Parameters
        ----------
        node : _TaskNode
            The task node being executed (used for error context and paths).
        payload : dict[str, Any]
            Encoded worker payload (same format as the process-pool path).

        Returns
        -------
        dict[str, Any]
            Decoded worker response dict (``{"ok": bool, "result": ...}``).
        """
        assert node.transport_path is not None
        assert self.pixi_registry is not None

        input_path = node.transport_path / "pixi_input.json"
        output_path = node.transport_path / "pixi_output.json"

        # Write the serialized payload for the worker script to consume.
        input_path.write_text(json.dumps(payload), encoding="utf-8")

        argv = self.pixi_registry.python_argv_c(
            env=node.task_def.env,
            code=_PIXI_WORKER_C,
            args=(str(input_path), str(output_path)),
        )
        completed = self._run_subprocess(argv=argv, use_shell=False)
        stdout_text = completed.stdout or ""
        stderr_text = completed.stderr or ""
        if node.stdout_path is not None and stdout_text:
            with node.stdout_path.open("a", encoding="utf-8") as handle:
                handle.write(stdout_text)
        if node.stderr_path is not None and stderr_text:
            with node.stderr_path.open("a", encoding="utf-8") as handle:
                handle.write(stderr_text)

        combined = stdout_text + stderr_text
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=node.task_def.name,
                cmd=" ".join(argv),
                exit_code=completed.returncode,
                output=combined.strip(),
                log=None,
            )

        if not output_path.exists():
            raise RuntimeError(
                f"Pixi Python task {node.task_def.name} completed but produced no output. "
                f"Command: {' '.join(argv)}"
            )

        return json.loads(output_path.read_text(encoding="utf-8"))

    def _validate_declared_envs(self) -> None:
        """Raise before any work starts if a declared env cannot be resolved.

        Only statically registered nodes are checked here. Dynamic nodes
        (discovered mid-run via conditional branching) are validated when
        ``_prepare_node`` is called for them.
        """
        if self.pixi_registry is None:
            return

        env_names: set[str] = {
            node.task_def.env for node in self._nodes.values() if node.task_def.env is not None
        }
        if env_names:
            self.pixi_registry.validate_envs(env_names=env_names)

    def validate(self, expr: Any) -> None:
        """Build the static task graph and validate import/env/input constraints."""
        self._root_template = expr
        self._root_dependency_ids = self._register_value(expr)
        self._validate_declared_envs()

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

        if annotation is file:
            self._validate_file_path(path=value, label=label)
            return

        if annotation is folder:
            self._validate_folder_path(path=value, label=label)
            return

        if annotation is tmp_dir:
            self._validate_tmp_dir_path(path=value, label=label)

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

        if annotation in {file, folder, tmp_dir} and isinstance(value, str):
            return annotation(value)

        return value

    def _iter_shell_output_paths(self, output: str | list[str] | tuple[str, ...]) -> list[Path]:
        """Return concrete output paths for a shell task declaration."""
        if isinstance(output, str):
            return [Path(output)]

        return [Path(item) for item in output]

    def _is_valid_cached_result(self, *, task_def: TaskDef, value: Any) -> bool:
        """Return whether a cached value still satisfies return validation."""
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
        if node.task_def.env is not None and self.pixi_registry is not None:
            manifest = self.pixi_registry.resolve(env=node.task_def.env)
            self.provenance.copy_env_lock(
                env_name=node.task_def.env,
                lock_path=manifest.parent / "pixi.lock",
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
        )

    def _display_label_for(self, *, node: _TaskNode) -> str | None:
        """Return a richer CLI label for mapped tasks once args are resolved."""
        if not node.expr.mapped or node.resolved_args is None:
            return None

        label_key = _first_label_param_name(task_def=node.task_def)
        if label_key is None or label_key not in node.resolved_args:
            return None

        rendered = _render_label_value(node.resolved_args[label_key])
        if rendered is None:
            return None

        base_name = node.task_def.name.rsplit(".", 1)[-1]
        return f"{base_name}[{rendered}]"

    def _log(
        self,
        *,
        task: str,
        status: str,
        exit_code: int | None = None,
        node_id: int | None = None,
        display_label: str | None = None,
        attempt: int | None = None,
        max_attempts: int | None = None,
        retries_remaining: int | None = None,
    ) -> None:
        """Emit basic structured execution logs to stderr."""
        payload = {"task": task, "status": status}
        if exit_code is not None:
            payload["exit_code"] = exit_code
        if node_id is not None:
            payload["node_id"] = node_id
        if display_label is not None:
            payload["display_label"] = display_label
        if attempt is not None:
            payload["attempt"] = attempt
        if max_attempts is not None:
            payload["max_attempts"] = max_attempts
        if retries_remaining is not None:
            payload["retries_remaining"] = retries_remaining
        print(json.dumps(payload, sort_keys=True), file=self._stderr)

    def _run_driver_task(self, *, node: _TaskNode) -> Any:
        """Run a shell-task wrapper on the scheduler process."""
        assert node.resolved_args is not None
        with _task_log_context(
            stdout_path=str(node.stdout_path) if node.stdout_path is not None else None,
            stderr_path=str(node.stderr_path) if node.stderr_path is not None else None,
        ):
            return node.task_def.fn(**node.resolved_args)

    def _handle_task_body_result(self, *, node: _TaskNode, completed_value: Any) -> None:
        """Advance a task after its Python wrapper has finished."""
        if self._failure is not None and (
            isinstance(completed_value, ShellExpr)
            or self._contains_dynamic_expression(completed_value)
        ):
            self._cleanup_transport(node)
            for path in node.tmp_paths:
                shutil.rmtree(path)
            node.tmp_paths = []
            node.state = "failed"
            return

        if node.task_def.kind == "python":
            if isinstance(completed_value, ShellExpr):
                self._cleanup_transport(node)
                raise TypeError(
                    f"{node.task_def.name} returned shell(...), but the task is declared "
                    "with kind='python'. Use @task(kind='shell') for shell command tasks."
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
                return

            final_value = self._finalize_result_value(node=node, value=completed_value)
            self._complete_node(node=node, value=final_value, tmp_paths=node.tmp_paths)
            return

        if isinstance(completed_value, ShellExpr):
            self._cleanup_transport(node)
            assert self._shell_executor is not None
            node.state = "running_shell"
            future = self._shell_executor.submit(
                self._run_shell,
                node=node,
                shell_expr=completed_value,
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
            return

        self._cleanup_transport(node)
        raise TypeError(
            f"{node.task_def.name} is declared with kind='shell' and must return "
            "shell(...) or dynamic task expressions."
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
