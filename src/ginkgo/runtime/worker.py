"""Worker entrypoints for Python task execution."""

from __future__ import annotations

import contextlib
import ctypes
import functools
import io
import os
import signal
import sys
from pathlib import Path
from threading import Event, Thread, current_thread, main_thread
import traceback
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING, Any

from ginkgo.runtime.module_loader import load_module
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_dynamic_value, encode_value

if TYPE_CHECKING:
    from collections.abc import Callable

    from ginkgo.runtime.artifacts.artifact_store import ArtifactStore
    from ginkgo.runtime.backend import ExecutionEnvironment
    from ginkgo.runtime.task_runners.shell import ShellRunner

_log_queue: Any = None
"""Queue this worker pushes task log chunks onto; ``None`` outside a local pool."""


def install_log_queue(log_queue: Any) -> None:
    """Process-pool initializer: route this worker's task log chunks to *log_queue*."""
    global _log_queue
    _log_queue = log_queue
    # A terminal Ctrl-C signals the whole process group. The parent owns the
    # interrupt and terminates its workers; a task body must not see it and
    # report a KeyboardInterrupt as its own failure. (Thread-pool fallback:
    # not the main thread, and the parent's handler stays put.)
    if current_thread() is main_thread():
        signal.signal(signal.SIGINT, signal.SIG_IGN)


class CommandHandedOff(Exception):
    """Raised by a shell runner that leaves its command to another container to run.

    The worker that raised it has done its part of the task, so
    :func:`run_task` reports the attempt as handed off rather than failed.
    """


def error_response(exc: BaseException) -> dict[str, Any]:
    """Build the standard error response dict for a failed task."""
    return {
        "ok": False,
        "error": {
            "type": type(exc).__name__,
            "module": type(exc).__module__,
            "message": str(exc),
            "args": [str(a) for a in exc.args],
            "exit_code": getattr(exc, "exit_code", None),
        },
    }


def _max_rss_bytes(ru_maxrss: int) -> int:
    """Normalize ``ru_maxrss`` to bytes (macOS reports bytes, Linux KiB)."""
    return int(ru_maxrss if sys.platform == "darwin" else ru_maxrss * 1024)


def _rusage_snapshot() -> Any | None:
    """Return this process's rusage, or ``None`` where unsupported (Windows)."""
    try:
        import resource
    except ImportError:  # pragma: no cover - resource is POSIX-only
        return None
    return resource.getrusage(resource.RUSAGE_SELF)


class _MachTaskBasicInfo(ctypes.Structure):
    """``mach_task_basic_info``, for this process's resident size on macOS."""

    _fields_ = [
        ("virtual_size", ctypes.c_uint64),
        ("resident_size", ctypes.c_uint64),
        ("resident_size_max", ctypes.c_uint64),
        ("user_time", ctypes.c_int32 * 2),
        ("system_time", ctypes.c_int32 * 2),
        ("policy", ctypes.c_int32),
        ("suspend_count", ctypes.c_int32),
    ]


_MACH_TASK_BASIC_INFO = 20


@functools.cache
def _libsystem() -> ctypes.CDLL:
    """Return macOS's C library, loaded once per process."""
    return ctypes.CDLL("/usr/lib/libSystem.dylib")


def _current_rss_bytes() -> int | None:
    """Return this process's resident set size now, or ``None`` where unknown.

    Read in-process — ``/proc`` on Linux, ``task_info`` on macOS — so a
    sample costs microseconds rather than a ``ps``.
    """
    try:
        if sys.platform == "darwin":
            libc = _libsystem()
            info = _MachTaskBasicInfo()
            count = ctypes.c_uint32(ctypes.sizeof(info) // 4)
            task = ctypes.c_uint32.in_dll(libc, "mach_task_self_")
            if libc.task_info(
                task, _MACH_TASK_BASIC_INFO, ctypes.byref(info), ctypes.byref(count)
            ):
                return None
            return int(info.resident_size)
        resident_pages = Path("/proc/self/statm").read_text().split()[1]
        return int(resident_pages) * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError, AttributeError):
        return None


class _TaskPeakRss:
    """Measure how far one task body raised this process's resident memory.

    The worker is already resident before the body runs: the interpreter,
    ginkgo, and the workflow module with everything it imports, which for a
    workflow using notebooks includes marimo. That baseline, sampled as the
    body starts, is the worker's and not the task's, so the measurement is the
    peak above it. A task that allocates nothing measures close to zero.

    ``ru_maxrss`` is the process's lifetime high-water mark, and a pool
    worker runs many tasks, so on its own it hands every task the peak of the
    heaviest one before it. When the mark rose during the task, the task set
    it and it is exact; otherwise the peak is the highest resident size
    sampled while the task ran, a lower bound.

    Parameters
    ----------
    usage_start : Any
        The process's rusage as the task started.
    interval_seconds : float
        Seconds between resident-size samples.
    """

    def __init__(self, *, usage_start: Any, interval_seconds: float = 0.1) -> None:
        self._usage_start = usage_start
        self._interval = interval_seconds
        self._stop = Event()
        self._peak = 0
        # Started before the baseline is read, so the sampler's own thread
        # stack is the worker's and not charged to the task.
        self._thread = Thread(target=self._run, name="ginkgo-task-rss", daemon=True)
        self._thread.start()
        self._sample()
        # Resident bytes as the task started; 0 where they cannot be read.
        self.baseline_bytes = self._peak

    def stop(self, *, usage_end: Any) -> int:
        """Stop sampling and return the task's peak resident bytes above the baseline.

        Parameters
        ----------
        usage_end : Any
            The process's rusage as the task finished.

        Returns
        -------
        int
            Peak resident bytes since the task started, less ``baseline_bytes``.
        """
        self._stop.set()
        self._thread.join(timeout=1.0)
        self._sample()
        peak = self._peak
        if peak == 0 or usage_end.ru_maxrss > self._usage_start.ru_maxrss:
            peak = _max_rss_bytes(usage_end.ru_maxrss)
        return max(peak - self.baseline_bytes, 0)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            self._sample()

    def _sample(self) -> None:
        rss = _current_rss_bytes()
        if rss is not None:
            self._peak = max(self._peak, rss)


def _body_usage(
    *, usage_start: Any | None, peak_rss: _TaskPeakRss | None
) -> dict[str, Any] | None:
    """Return the task body's usage, measured as the body returns or raises.

    Taken before the worker encodes the result and closes the task's logs:
    that is the worker's work, and a fresh worker's first encode imports and
    allocates enough to swamp a small task's own figure. Recorded for failed
    bodies too — the usage measured before a failure is what right-sizes an
    OOM-prone task.
    """
    usage_end = _rusage_snapshot() if usage_start is not None else None
    if usage_start is None or usage_end is None or peak_rss is None:
        return None
    return {
        "peak_rss_bytes": peak_rss.stop(usage_end=usage_end),
        "baseline_rss_bytes": peak_rss.baseline_bytes,
        "cpu_seconds": round(
            (usage_end.ru_utime - usage_start.ru_utime)
            + (usage_end.ru_stime - usage_start.ru_stime),
            3,
        ),
        "source": "rusage",
    }


def _traceback_from_task(*, exc: BaseException, fn: Any) -> TracebackType | None:
    """Return *exc*'s traceback, starting at the task's frame if it raised there.

    The first frame of a traceback caught in :func:`run_task` is
    ``run_task`` itself. When the next frame is the task function's own, the
    log starts there, at the user's code; an exception raised by
    ``run_task``'s own work (loading the task, hydrating arguments) keeps
    every frame.
    """
    tb = exc.__traceback__
    task_code = getattr(fn, "__code__", None)
    if tb is not None and tb.tb_next is not None and tb.tb_next.tb_frame.f_code is task_code:
        return tb.tb_next
    return tb


def run_task(
    payload: dict[str, Any],
    *,
    artifact_store: ArtifactStore | None = None,
    backend: ExecutionEnvironment | None = None,
    shell_runner_type: Callable[..., ShellRunner] | None = None,
) -> dict[str, Any]:
    """Execute a task payload inside a process-pool or remote worker.

    Parameters
    ----------
    payload : dict[str, Any]
        Worker payload built by the evaluator.
    artifact_store : ArtifactStore | None
        Store that carries large values, for a remote worker whose transport
        directory the driver cannot read. ``None`` keeps them in the
        transport directory.
    backend : ExecutionEnvironment | None
        The prepared environment a remote worker runs a driver-kind task's
        command in, for a task that declares a Pixi ``env=``.
    shell_runner_type : Callable[..., ShellRunner] | None
        Builds the shell runner that a remote driver-kind task's directive
        runs on, in place of a :class:`ShellRunner`.

    Returns
    -------
    dict[str, Any]
        The worker response: the encoded result, or the error. A runner that
        raised :class:`CommandHandedOff` gives ``{"ok": True, "handed_off":
        True}``.
    """
    base_dir = Path(payload["transport_dir"])

    # Mount any fuse-marked inputs against a task-local mount root so
    # concurrent workers never collide on a shared bucket mount point. The
    # access instance is held before hydration so the finally below always
    # tears mounts down, even if hydration itself fails partway. Encoding
    # passes fuse markers through, so the encoded arguments show them.
    mounted_access = _local_fuse_access(args=payload["args"], base_dir=base_dir)

    stdout_path = payload.get("stdout_path")
    stderr_path = payload.get("stderr_path")
    secret_values = tuple(payload.get("secret_values", ()))
    log_context = {
        "run_id": payload.get("run_id"),
        "task_id": payload.get("task_id"),
        "task_name": payload.get("task_name"),
        "attempt": payload.get("attempt"),
        "display_label": payload.get("display_label"),
    }
    fn = None
    measured: dict[str, Any] | None = None
    input_digests: dict[str, str] = {}
    runner_record: dict[str, Any] = {}
    try:
        with (
            _task_log_context(
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                secret_values=secret_values,
                log_emitter=_queue_log_emitter(event_queue=_log_queue, context=log_context),
            ),
            _worker_param_session(payload=payload),
        ):
            # The task's module is loaded before its arguments are decoded,
            # so a value of a class the workflow defines finds that module
            # already imported, as the task itself uses it.
            task_binding = _load_task_binding(payload=payload)
            fn = getattr(task_binding, "fn", task_binding)
            decoded_args = {
                name: decode_value(value, base_dir=base_dir, artifact_store=artifact_store)
                for name, value in payload["args"].items()
            }
            if mounted_access is not None:
                from ginkgo.remote.access.worker_hydration import hydrate_remote_inputs

                hydration = hydrate_remote_inputs(args=decoded_args, mounted_access=mounted_access)
                decoded_args, input_digests = hydration.args, hydration.digests
            # Measured from here, once the workflow module is imported, so
            # the body's usage is not charged with the worker's imports.
            usage_start = _rusage_snapshot()
            peak_rss = _TaskPeakRss(usage_start=usage_start) if usage_start is not None else None
            try:
                result = fn(**decoded_args)
            finally:
                measured = _body_usage(usage_start=usage_start, peak_rss=peak_rss)
            ran_directive = _runs_its_directive_here(task_def=task_binding, result=result)
            if ran_directive:
                command_usage: dict[str, Any] = {}
                try:
                    result = _run_directive(
                        task_def=task_binding,
                        directive=result,
                        args=decoded_args,
                        payload=payload,
                        usage=command_usage,
                        record=runner_record,
                        backend=backend,
                        shell_runner_type=shell_runner_type,
                    )
                finally:
                    # The command's own usage, not the body's that built it,
                    # and none for a command too quick to sample, as locally.
                    measured = command_usage or None

        # Encode while any fuse mounts are still live: the result may
        # reference paths inside a mount (e.g. an input path returned
        # through), and encoding reads file content.
        if _is_dynamic_result(result):
            encoded_result = encode_dynamic_value(
                result, base_dir=base_dir, artifact_store=artifact_store
            )
            response = {"ok": True, "result": encoded_result, "result_encoding": "dynamic"}
        else:
            encoded_result = encode_value(result, base_dir=base_dir, artifact_store=artifact_store)
            response = {"ok": True, "result": encoded_result, "result_encoding": "encoded"}
        # The driver completes the task with the runner's result, as it would
        # a local runner's, rather than treating it as the body's.
        if ran_directive:
            response["ran_directive"] = True
    except CommandHandedOff:
        return {"ok": True, "handed_off": True}
    except BaseException as exc:  # pragma: no cover - exercised via parent tests
        # With no task log file (a remote worker, or a run without a run
        # directory) the traceback goes to the process's own stderr.
        trace = redact_text(
            text="".join(
                traceback.format_exception(type(exc), exc, _traceback_from_task(exc=exc, fn=fn))
            ),
            secret_values=secret_values,
        )
        if stderr_path is not None:
            with Path(stderr_path).open("a", encoding="utf-8") as handle:
                handle.write(trace)
        else:
            sys.stderr.write(trace)
        # The message travels as the rebuilt exception's one argument. A
        # KeyError's message is its key's repr, so it carries the key itself,
        # or the rebuilt one would quote it twice.
        message = str(exc.args[0]) if isinstance(exc, KeyError) and exc.args else str(exc)
        exc.args = (redact_text(text=message, secret_values=secret_values),)
        response = error_response(exc)
        if measured is not None:
            response["measured_resources"] = measured
        if runner_record:
            response["runner_record"] = _encoded_runner_record(
                runner_record, base_dir=base_dir, artifact_store=artifact_store
            )
        return response
    finally:
        if mounted_access is not None:
            mounted_access.close()

    if measured is not None:
        response["measured_resources"] = measured
    if runner_record:
        response["runner_record"] = _encoded_runner_record(
            runner_record, base_dir=base_dir, artifact_store=artifact_store
        )
    if mounted_access is not None:
        response["remote_input_access"] = mounted_access.stats().to_dict()
    # A mount that fell back to a download leaves that input's digest.
    if input_digests:
        response["remote_input_digests"] = input_digests
    return response


def _local_fuse_access(*, args: dict[str, Any], base_dir: Path) -> Any:
    """Build a task-local :class:`MountedAccess` when args carry fuse markers.

    Mount points live under ``base_dir`` (the task's transport dir) so
    parallel process-pool workers do not share a bucket mount point.
    Returns ``None`` when no fuse markers are present so non-streaming
    tasks pay no import or setup cost.
    """
    if not _args_have_fuse_inputs(args):
        return None

    from ginkgo.remote.access.mounted import MountedAccess

    return MountedAccess(
        mount_root=base_dir / "fuse",
        cache_root=base_dir / "fuse-cache",
    )


def _args_have_fuse_inputs(value: Any) -> bool:
    """Return whether ``value`` contains any remote-input marker to mount.

    A local task's other remote inputs were staged on the driver, so only
    ``fuse`` markers reach it.
    """
    from ginkgo.remote.access.protocol import is_fuse_input

    if is_fuse_input(value):
        return True
    if isinstance(value, dict):
        return any(_args_have_fuse_inputs(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_args_have_fuse_inputs(item) for item in value)
    return False


def _runs_its_directive_here(*, task_def: Any, result: Any) -> bool:
    """Return whether *result* is a driver kind's directive, to run in this worker.

    A driver kind's body reaches a worker only when its task runs remotely,
    so its command runs here, against this worker's paths, rather than going
    back to a driver that cannot see them.
    """
    from ginkgo.core.directive import ExecutionDirective
    from ginkgo.runtime.task_kinds import task_kind

    # Not repeated by the kind's own check below: it keeps the kind lookup
    # off every other result, and off a binding that is a bare function.
    if not isinstance(result, ExecutionDirective):
        return False
    kind = task_kind(task_def)
    return (
        kind.execution_mode == "driver"
        and kind.directive is not None
        and isinstance(result, kind.directive)
    )


@dataclass(kw_only=True)
class _WorkerInvocation:
    """One attempt of a task, as a runner in a remote worker sees it.

    The worker has no task log files: the command's output goes to the
    process's own streams, which its executor collects.
    """

    node_id: int
    task_def: Any
    attempt: int
    display_label: str | None
    resolved_args: dict[str, Any]
    execution_args: dict[str, Any]
    threads: int
    secret_values: tuple[str, ...]
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    runner_extras: dict[str, Any] | None = None


def _run_directive(
    *,
    task_def: Any,
    directive: Any,
    args: dict[str, Any],
    payload: dict[str, Any],
    usage: dict[str, Any],
    record: dict[str, Any],
    backend: ExecutionEnvironment | None,
    shell_runner_type: Callable[..., ShellRunner] | None = None,
) -> Any:
    """Run *directive* on its kind's runner, as the driver runs a local one.

    The command runs in *backend*'s env when the task declares a Pixi one, on
    what *shell_runner_type* builds, or on a :class:`ShellRunner`. Returns the
    runner's result, and records the command's measured usage in *usage*.

    The runner gets a run directory of this worker's own, one per attempt,
    under the payload's ``run_dir_root``. What it writes there and what it
    annotates go in *record*, whether or not it succeeds, for the driver to
    take into its run directory.
    """
    from ginkgo.plugins.api import RunnerContext
    from ginkgo.runtime.executor_registry import LOCAL
    from ginkgo.runtime.rundir import RunDir
    from ginkgo.runtime.task_kinds import task_kind
    from ginkgo.runtime.task_runners.shell import ShellRunner
    from ginkgo.runtime.task_validation import TaskValidator
    from ginkgo.workspace_layout import WorkspaceLayout

    # A log file written here would stay in this worker, and the command's
    # output reaches the task's log anyway.
    log = getattr(directive, "log", None)
    if log is not None:
        raise ValueError(
            f"{task_def.base_name} names log={log!r}, but a remote worker cannot bring "
            f"a log file back to the driver. Drop log=, since the command's output "
            f"reaches the task's log, or declare executor={LOCAL!r} to run it on the driver."
        )

    def emitter(*, node: Any, stream: str) -> Any:
        return lambda chunk: getattr(sys, stream).write(chunk)

    run_dir = RunDir(
        run_id=payload.get("run_id") or "",
        root=Path(payload["run_dir_root"])
        / f"task_{payload['node_id']:04d}_attempt_{payload['attempt']}",
    )
    annotated: dict[str, Any] = {}
    validator = TaskValidator()
    context = RunnerContext(
        validator=validator,
        shell_runner=(shell_runner_type or ShellRunner)(
            backend=backend,
            validator=validator,
            log_emitter_factory=emitter,
            usage_recorder=lambda *, node, measured: usage.update(measured),
        ),
        run_dir=run_dir,
        annotate=lambda *, node, fields: annotated.update(fields),
        notice=lambda *, node, message: print(f"warning: {message}", file=sys.stderr),
        run_id=lambda: payload.get("run_id") or "",
        db_path=WorkspaceLayout.for_cwd().db,
    )
    invocation = _WorkerInvocation(
        node_id=payload["node_id"],
        task_def=task_def,
        attempt=payload["attempt"],
        display_label=payload.get("display_label"),
        resolved_args=args,
        execution_args=args,
        threads=payload["threads"],
        secret_values=tuple(payload.get("secret_values", ())),
    )
    runner = task_kind(task_def).runner
    assert runner is not None
    try:
        return runner(context).run(node=invocation, directive=directive)
    finally:
        if annotated or run_dir.path.exists():
            record.update(
                run_dir=run_dir.path,
                fields=annotated,
                runner_extras=invocation.runner_extras,
            )


def _encoded_runner_record(
    record: dict[str, Any], *, base_dir: Path, artifact_store: ArtifactStore | None
) -> dict[str, Any]:
    """Encode what :func:`_run_directive` recorded, its run directory as a folder value."""
    from ginkgo.core.types import folder

    run_dir = record["run_dir"]
    return {
        "run_dir": str(run_dir),
        "fields": record["fields"],
        "runner_extras": record["runner_extras"],
        "files": (
            encode_value(folder(run_dir), base_dir=base_dir, artifact_store=artifact_store)
            if run_dir.exists()
            else None
        ),
    }


def _is_dynamic_result(value: Any) -> bool:
    """Return whether *value* is a directive or holds task expressions anywhere.

    Such a result goes back to the driver pickled as one object rather than
    encoded leaf by leaf, so an expression it holds twice (``[merge(ps=calls),
    calls]``) still arrives as one expression and registers as one task.
    """
    from ginkgo.core.directive import ExecutionDirective
    from ginkgo.runtime.task_validation import contains_dynamic_expression

    return isinstance(value, ExecutionDirective) or contains_dynamic_expression(value)


@contextlib.contextmanager
def _worker_param_session(*, payload: dict[str, Any]) -> Any:
    """Make the run's workflow parameters available while the module is imported.

    Loading the task binding imports the workflow module, re-running its
    ``param()`` calls in this process. Without the run's resolution inputs those
    calls would fall back to the declared defaults, so a value supplied on the
    command line would be silently ignored, and a required parameter would raise.
    """
    from ginkgo.config import config_session
    from ginkgo.params import ParamContext

    raw_context = payload.get("param_context")
    if raw_context is None:
        yield
        return

    context = ParamContext.from_payload(raw_context)
    with config_session(param_config=context.config, cli_extras=context.cli_extras):
        yield


def _load_task_binding(*, payload: dict[str, Any]) -> Any:
    """Load the declared task binding for worker-executed Python tasks."""
    try:
        module = load_module(
            payload["module"],
            module_file=payload.get("module_file"),
        )
        return getattr(module, payload["binding_name"])
    except BaseException:
        raise


def _queue_log_emitter(*, event_queue: Any, context: dict[str, Any]) -> Any:
    """Return a best-effort queue-backed task log emitter."""
    if event_queue is None:
        return None

    def emit(*, stream: str, chunk: str) -> None:
        if not chunk:
            return
        event_queue.put(
            {
                "run_id": context.get("run_id"),
                "task_id": context.get("task_id"),
                "task_name": context.get("task_name"),
                "attempt": context.get("attempt"),
                "display_label": context.get("display_label"),
                "stream": stream,
                "chunk": chunk,
            }
        )

    return emit


@contextlib.contextmanager
def _task_log_context(
    *,
    stdout_path: str | None,
    stderr_path: str | None,
    secret_values: tuple[str, ...] = (),
    log_emitter: Any = None,
):
    """Redirect task stdout and stderr to separate per-task log files."""
    if stdout_path is None and stderr_path is None:
        yield
        return

    managers: list[contextlib.AbstractContextManager] = []
    writers: list[_RedactingWriter] = []

    if stdout_path is not None:
        path = Path(stdout_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stdout_writer = _RedactingWriter(
            handle=path.open("a", encoding="utf-8"),
            secret_values=secret_values,
            stream_name="stdout",
            log_emitter=log_emitter,
        )
        writers.append(stdout_writer)
        managers.append(contextlib.redirect_stdout(stdout_writer))

    if stderr_path is not None:
        path = Path(stderr_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stderr_writer = _RedactingWriter(
            handle=path.open("a", encoding="utf-8"),
            secret_values=secret_values,
            stream_name="stderr",
            log_emitter=log_emitter,
        )
        writers.append(stderr_writer)
        managers.append(contextlib.redirect_stderr(stderr_writer))

    # Close the writers themselves (not just the raw handles) on exit, so a
    # reference that escaped the task body - an atexit hook, a daemon
    # thread, or a logging handler pinned to sys.stderr - sees a writer
    # marked closed and becomes a safe no-op rather than raising against a
    # closed file handle during interpreter shutdown.
    with contextlib.ExitStack() as stack:
        for writer in writers:
            stack.callback(writer.close)
        for manager in managers:
            stack.enter_context(manager)
        yield


class _RedactingWriter(io.TextIOBase):
    """Text writer that redacts known secret values before writing."""

    def __init__(
        self,
        *,
        handle: io.TextIOBase,
        secret_values: tuple[str, ...],
        stream_name: str | None = None,
        log_emitter: Any = None,
    ) -> None:
        self._handle = handle
        self._secret_values = secret_values
        self._stream_name = stream_name
        self._log_emitter = log_emitter
        self._closed = False

    def write(self, text: str) -> int:
        # Anything that escapes the task body - an atexit hook, a daemon
        # thread, a logging handler pinned to sys.stderr - may still hold
        # this writer after its task context has exited and the handle
        # closed. Once closed, writes are a safe no-op rather than a
        # ValueError surfacing as raw interpreter-shutdown noise. The
        # _closed check is a fast path only: close() can run concurrently
        # between the check and the handle.write() call below, so the
        # actual write is also guarded against a closed handle.
        if self._closed:
            return 0

        redacted = redact_text(text=text, secret_values=self._secret_values)
        try:
            written = self._handle.write(redacted)
        except ValueError:
            return 0
        if self._log_emitter is not None and self._stream_name is not None and redacted:
            self._log_emitter(stream=self._stream_name, chunk=redacted)
        return written

    def flush(self) -> None:
        if self._closed:
            return
        try:
            self._handle.flush()
        except ValueError:
            # CPython may flush redirected streams during finalization after
            # the underlying file handle has already been closed, or close()
            # may run concurrently between the _closed check and this call.
            return

    def close(self) -> None:
        """Mark the writer closed and close the underlying handle.

        Marking closed first ensures any concurrent or later write sees a
        closed writer and no-ops, instead of racing the handle close.
        """
        if self._closed:
            return
        self._closed = True
        self._handle.close()
