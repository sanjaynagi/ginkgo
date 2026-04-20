"""Shell task execution and subprocess lifecycle management.

The ``ShellRunner`` owns the subprocess registry, the ``_run_subprocess``
implementation, and the ``run_shell`` driver-task path. Centralising this
here keeps subprocess termination on interrupt in one place and lets the
notebook runner reuse the logged-command machinery.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock, Thread, current_thread, main_thread
from types import FrameType
from typing import Any, Callable

from ginkgo.core.asset import AssetResult
from ginkgo.runtime.backend import TaskBackend
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.task_validation import TaskValidator
from ginkgo.runtime.artifacts.value_codec import CodecError


# ----- Exceptions -----------------------------------------------------------


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


# ----- Helpers --------------------------------------------------------------


_THREAD_ENV_VARS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)


def build_shell_subprocess_env(*, task_def: Any) -> dict[str, str]:
    """Return the environment for a shell-task subprocess.

    Always exports ``GINKGO_THREADS`` carrying the task's declared thread
    count. When ``task_def.export_thread_env`` is ``True``, also exports the
    common BLAS/OpenMP thread variables so off-the-shelf tools pick up the
    declared budget without per-workflow boilerplate.
    """
    env = dict(os.environ)
    threads = int(getattr(task_def, "threads", 1))
    env["GINKGO_THREADS"] = str(threads)
    if getattr(task_def, "export_thread_env", False):
        for name in _THREAD_ENV_VARS:
            env[name] = str(threads)
    return env


def remove_declared_output(path: Path) -> None:
    """Remove one pre-existing declared output before task execution."""
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)


def iter_output_values(
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


def sanitize_exception(
    *,
    exc: BaseException,
    secret_values: tuple[str, ...],
) -> BaseException:
    """Return an exception with redacted message text."""
    if not secret_values:
        return exc

    message = redact_text(text=str(exc), secret_values=secret_values)
    try:
        exc.args = (message,)
    except Exception:
        return RuntimeError(message)

    if hasattr(exc, "output"):
        try:
            exc.output = redact_text(text=str(exc.output), secret_values=secret_values)
        except Exception:
            pass
    if hasattr(exc, "cmd"):
        try:
            exc.cmd = redact_text(text=str(exc.cmd), secret_values=secret_values)
        except Exception:
            pass
    return exc


def classify_failure(*, exc: BaseException) -> dict[str, Any]:
    """Return a structured task failure summary."""
    # Imported lazily to avoid a hard import cycle with the notebook runner.
    from ginkgo.envs.container import ContainerPrepareError, ContainerRuntimeNotFoundError
    from ginkgo.envs.pixi import (
        PixiEnvImportError,
        PixiEnvNotFoundError,
        PixiEnvPrepareError,
    )
    from ginkgo.runtime.evaluator import CycleError
    from ginkgo.runtime.task_runners.notebook import NotebookTaskError

    message = str(exc)
    if isinstance(exc, CycleError):
        kind = "cycle_detected"
    elif isinstance(
        exc,
        (
            PixiEnvNotFoundError,
            PixiEnvImportError,
            PixiEnvPrepareError,
            ContainerRuntimeNotFoundError,
            ContainerPrepareError,
        ),
    ):
        kind = "env_mismatch"
    elif isinstance(exc, ModuleNotFoundError):
        kind = "import_error"
    elif isinstance(exc, ImportError):
        kind = "import_error"
    elif isinstance(exc, CodecError):
        kind = "serialization_error"
    elif isinstance(exc, (ShellTaskError, NotebookTaskError)):
        kind = "shell_command_error"
    elif isinstance(exc, (IsADirectoryError, NotADirectoryError, PermissionError)):
        kind = "invalid_path"
    elif isinstance(exc, FileNotFoundError):
        kind = "missing_input" if "did not create" not in message else "output_validation_error"
    elif isinstance(exc, (TypeError, ValueError)):
        kind = "user_code_error"
    else:
        exc_name = exc.__class__.__name__.lower()
        if "env" in exc_name or "container" in exc_name:
            kind = "env_mismatch"
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


# ----- Signal monitor -------------------------------------------------------


class SignalMonitor:
    """Temporary signal handler that requests a graceful scheduler stop."""

    def __init__(self) -> None:
        self.exception: BaseException | None = None
        self._installed = False
        self._previous: dict[int, Any] = {}

    def __enter__(self) -> SignalMonitor:
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


# ----- Shell runner ---------------------------------------------------------


# Type aliases used to keep signatures readable without forcing import cycles.
LogEmitter = Callable[[str], None]
LogEmitterFactory = Callable[..., LogEmitter]


@dataclass(kw_only=True)
class ShellRunner:
    """Run shell commands and own the subprocess registry.

    Parameters
    ----------
    backend : TaskBackend | None
        Execution backend used when a task declares a non-default env.
    validator : TaskValidator
        Used to coerce return values for shell tasks.
    log_emitter_factory : Callable
        Factory ``log_emitter_factory(node=..., stream=...)`` returning a
        ``Callable[[str], None]`` that consumes one log chunk.
    """

    backend: TaskBackend | None
    validator: TaskValidator
    log_emitter_factory: LogEmitterFactory
    _subprocess_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _active_subprocesses: dict[int, subprocess.Popen[str]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    # Subprocess registry ----------------------------------------------------

    def register(self, *, process: subprocess.Popen[str]) -> None:
        """Track a subprocess so interrupts can terminate it."""
        with self._subprocess_lock:
            self._active_subprocesses[process.pid] = process

    def unregister(self, *, process: subprocess.Popen[str]) -> None:
        """Stop tracking a subprocess after it exits."""
        with self._subprocess_lock:
            self._active_subprocesses.pop(process.pid, None)

    def terminate_all(self) -> None:
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

    # Subprocess execution ---------------------------------------------------

    def _run_subprocess(
        self,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
        env: dict[str, str] | None = None,
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
        if env is not None:
            popen_kwargs["env"] = env

        process = subprocess.Popen(argv, **popen_kwargs)
        self.register(process=process)
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        if not hasattr(process, "stdout") or not hasattr(process, "stderr"):
            try:
                stdout_text, stderr_text = process.communicate()
            finally:
                self.unregister(process=process)
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
            self.unregister(process=process)

        return subprocess.CompletedProcess(
            args=argv,
            returncode=returncode,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
        )

    # Logged command --------------------------------------------------------

    def run_logged_command(
        self,
        *,
        node: Any,
        cmd: str,
        user_log_path: Path | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run one command while appending to provenance logs."""
        for path in (node.stdout_path, node.stderr_path, user_log_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)

        if node.task_def.env is not None and self.backend is not None:
            argv: str | list[str] = self.backend.exec_argv(env=node.task_def.env, cmd=cmd)
            use_shell = False
        else:
            argv = cmd
            use_shell = True

        subprocess_env = build_shell_subprocess_env(task_def=node.task_def)
        if extra_env:
            subprocess_env.update(extra_env)

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
            self.log_emitter_factory(node=node, stream=stream)(chunk)

        try:
            completed = self._run_subprocess(
                argv=argv,
                use_shell=use_shell,
                on_stdout=lambda chunk: emit_chunk(stream="stdout", chunk=chunk),
                on_stderr=lambda chunk: emit_chunk(stream="stderr", chunk=chunk),
                env=subprocess_env,
            )
        finally:
            if stdout_handle is not None:
                stdout_handle.close()
            if stderr_handle is not None:
                stderr_handle.close()
            if user_log_handle is not None:
                user_log_handle.close()

        return completed

    # Shell driver ----------------------------------------------------------

    def run_shell(self, *, node: Any, shell_expr: Any) -> Any:
        """Execute a shell command and return its declared output path or paths."""
        task_def = node.task_def
        user_log_path = Path(shell_expr.log) if shell_expr.log is not None else None

        for output_path in iter_output_values(shell_expr.output):
            remove_declared_output(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        completed = self.run_logged_command(
            node=node,
            cmd=shell_expr.cmd,
            user_log_path=user_log_path,
        )
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=task_def.name,
                cmd=redact_text(text=shell_expr.cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=shell_expr.log,
            )

        missing_outputs = [
            str(output_path)
            for output_path in iter_output_values(shell_expr.output)
            if not output_path.exists()
        ]
        if missing_outputs:
            missing_label = missing_outputs[0] if len(missing_outputs) == 1 else missing_outputs
            raise FileNotFoundError(
                f"Shell task {task_def.name} completed but did not create output {missing_label!r}"
            )

        return self.validator.coerce_return_value(task_def=task_def, value=shell_expr.output)
