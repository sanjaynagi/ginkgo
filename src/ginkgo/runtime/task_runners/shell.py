"""Shell task execution and subprocess lifecycle management.

The ``ShellRunner`` owns the subprocess registry, the ``run_subprocess``
implementation, and the ``run_shell`` driver-task path. Centralising this
here keeps subprocess termination on interrupt in one place and lets the
notebook runner reuse the logged-command machinery.
"""

from __future__ import annotations

import datetime
import json
import os
import shutil
import signal
import subprocess
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock, Thread, current_thread, main_thread
from types import FrameType
from typing import Any, Callable, Sequence

from ginkgo.core.asset import AssetRef, AssetResult
from ginkgo.core.optional import OptionalOutput
from ginkgo.core.shell import ShellDirective
from ginkgo.core.types import file, folder, is_path_shaped_annotation, tmp_dir
from ginkgo.envs.mounts import Mount, MountMode, mount
from ginkgo.errors import GinkgoError
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.environment.resources import SubprocessUsageSampler
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.task_validation import TaskValidator
from ginkgo.runtime.artifacts.value_codec import CodecError


# ----- Exceptions -----------------------------------------------------------


class ShellTaskError(GinkgoError, RuntimeError):
    """Shell task execution failure."""

    def __init__(
        self,
        *,
        task_name: str,
        cmd: str,
        exit_code: int,
        output: str,
        log: str | None,
        hint: str | None = None,
    ) -> None:
        self.exit_code = exit_code

        details = f"Shell task {task_name} failed with exit code {exit_code}: {cmd}"
        if log is not None:
            details = f"{details} (log: {log})"
        elif output:
            details = f"{details}\n{output.strip()}"
        if hint is not None:
            details = f"{details}\n{hint}"

        super().__init__(details)


# ----- Helpers --------------------------------------------------------------


_THREAD_ENV_VARS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)


def build_shell_subprocess_env(*, task_def: Any, threads: int | None = None) -> dict[str, str]:
    """Return the environment for a shell-task subprocess.

    Always exports ``GINKGO_THREADS`` carrying the task's effective thread
    count (*threads* when given — the scheduler passes the node's resolved
    value, which includes site overrides — else the declaration). When
    ``task_def.export_thread_env`` is ``True``, also exports the common
    BLAS/OpenMP thread variables so off-the-shelf tools pick up the budget
    without per-workflow boilerplate.
    """
    env = dict(os.environ)
    if threads is None:
        threads = int(getattr(task_def, "threads", 1))
    env["GINKGO_THREADS"] = str(threads)
    if getattr(task_def, "export_thread_env", False):
        for name in _THREAD_ENV_VARS:
            env[name] = str(threads)
    return env


def computed_env_var_names(*, task_def: Any) -> tuple[str, ...]:
    """Return the variable names :func:`build_shell_subprocess_env` computes.

    An isolated environment inherits nothing, so it has to be told which
    variables to carry across. Only Ginkgo's own computed set is named here:
    forwarding the rest of ``os.environ`` would undermine the controlled
    environment that is a container's whole point.
    """
    if getattr(task_def, "export_thread_env", False):
        return ("GINKGO_THREADS", *_THREAD_ENV_VARS)
    return ("GINKGO_THREADS",)


def remove_declared_output(path: Path) -> None:
    """Remove one pre-existing declared output before task execution."""
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    if path.is_dir():
        shutil.rmtree(path)


# Sub-kind values that identify a path-backed non-file asset (payload is
# a path, not an in-memory object). The kind's ``detect`` sets these
# sub-kinds based on the declared file extension.
_PATH_SUB_KINDS: dict[str, frozenset[str]] = {
    "file": frozenset(),  # file always has a path payload; handled below.
    "table": frozenset({"csv", "tsv"}),
    "fig": frozenset({"png", "svg", "html"}),
}


def _asset_result_path(result: AssetResult) -> Path:
    """Return the declared output path from an :class:`AssetResult`.

    File assets carry their path directly. Non-file kinds only qualify
    as declared outputs when they wrap a path: ``fig("p.png")``,
    ``table("d.csv")``, ``text(Path("doc.md"))``. In-memory payloads
    (matplotlib figures, pandas DataFrames, etc.) are not valid declared
    task outputs and raise a clear error here rather than at a
    downstream serialisation boundary.
    """
    payload = result.payload

    if result.kind == "file":
        return Path(str(payload))

    path_sub_kinds = _PATH_SUB_KINDS.get(result.kind)
    if path_sub_kinds is not None and result.sub_kind in path_sub_kinds:
        return Path(str(payload))

    # ``text(Path("doc.md"))`` stores a Path payload directly.
    if result.kind == "text" and isinstance(payload, Path):
        return payload

    raise TypeError(
        f"{result.kind}(...) output must wrap a declared file path; got "
        f"in-memory payload of type "
        f"{type(payload).__module__}.{type(payload).__name__}. "
        "Path-based wrappers accept a str or Path argument (e.g. "
        f'{result.kind}("results/foo.png")).'
    )


def serialize_cli_argument_value(
    value: Any,
    *,
    label: str = "A task argument",
    task_kind: str | None = None,
) -> Any:
    """Convert one resolved task argument into a YAML/JSON-safe value.

    Shared by the two driver task kinds that forward resolved arguments to an
    external process: a script task renders each one as a CLI option, a
    notebook task writes them to a parameter file. A shell task never reaches
    here — its body is Python and runs before the command is built, so it can
    legitimately take a live payload and write the format its command expects.

    An :class:`AssetRef` crosses that boundary as the path to its stored
    bytes, since a CLI argument and a parameter file carry text rather than
    Python objects. ``AssetRef.as_file`` decides whether the ref has such a
    path at all, so a kind stored in Ginkgo's own encoding is refused by name
    here instead of reaching ``json.dumps`` and failing as "not JSON
    serializable". Every caller is a driver task kind, so the refusal names
    the remedies that work for one.

    A live Python payload — a DataFrame handed to a parameter annotated
    ``object``, say — has no text form either, and is refused for the same
    reason rather than falling through to ``json.dumps`` or ``yaml.safe_dump``
    and naming only its type. ``label`` names the parameter in that refusal,
    e.g. ``"summarise.scores"``; nested values extend it with their index or
    key, so a payload inside a list reports ``summarise.scores[1]``.
    """
    if isinstance(value, AssetRef):
        return str(value.as_file(execution_mode="driver"))
    if isinstance(value, Path | file | folder | tmp_dir):
        return str(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    # ``datetime`` subclasses ``date``. Both have a text form, which is what
    # this boundary carries; ``json.dumps`` would refuse the object itself.
    if isinstance(value, datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, list | tuple):
        return [
            serialize_cli_argument_value(item, label=f"{label}[{index}]", task_kind=task_kind)
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        # A JSON object or YAML mapping keys by text, so keys stringify rather
        # than serialize: only the values can carry a payload worth refusing.
        return {
            str(key): serialize_cli_argument_value(
                item, label=f"{label}[{key!r}]", task_kind=task_kind
            )
            for key, item in value.items()
        }

    received = f"{type(value).__module__}.{type(value).__name__}"
    carrier = f"a `{task_kind}` task" if task_kind is not None else "a script or notebook task"
    raise TypeError(
        f"{label} is a {received}. The arguments of {carrier} cross to another "
        "process as CLI options and parameter-file entries, which carry text rather "
        "than Python objects. Write the payload to a file in a Python task first and "
        "pass that path (with `asset(path)` to track it), or do the work in a "
        "`python` task."
    )


def stringify_cli_argument(
    value: Any,
    *,
    label: str = "A task argument",
    task_kind: str | None = None,
) -> str:
    """Render one resolved task argument for a CLI invocation."""
    serialized = serialize_cli_argument_value(value, label=label, task_kind=task_kind)
    if isinstance(serialized, str):
        return serialized
    return json.dumps(serialized, sort_keys=True)


def _declared_item_path(item: Any) -> Path:
    """Return the filesystem path declared by one output item."""
    if isinstance(item, OptionalOutput):
        return _declared_item_path(item.payload)
    if isinstance(item, AssetResult):
        return _asset_result_path(item)
    return Path(str(item))


def iter_output_values(
    output: Any,
) -> list[Path]:
    """Return every declared output path, required and optional alike.

    Used for pre-execution cleanup and parent-directory creation, where an
    optional path must be treated exactly like a required one: a stale file
    left by an earlier run must not be mistaken for this run's output.
    """
    if isinstance(output, (str, AssetResult, OptionalOutput)):
        return [_declared_item_path(output)]
    return [_declared_item_path(item) for item in output]


def declared_input_mounts(*, node: Any) -> list[Mount]:
    """Return the bind mounts a node's declared path inputs need.

    A container sees only what is mounted into it, and the task's command
    carries host absolute paths.  Path-shaped inputs (``file``/``folder``) are
    mounted read-only; a ``tmp_dir`` scratch directory is mounted read-write,
    since the task writes into it.

    A ``file`` input mounts its *directory*, not just the file. Command-line
    bioinformatics tools routinely read a sibling of the file they are given —
    ``ref.fa.fai`` beside ``ref.fa``, ``.bai`` beside ``.bam`` — and a mount of
    the file alone makes an index that exists on the host invisible inside the
    container. Read-only means a tool that wants to *write* its index fails and
    says so; declaring that index as an output is what makes it writable, and
    keeps it after the container exits.

    Inputs that do not exist are skipped rather than mounted: the runtime would
    create the host path to satisfy the mount, and a missing declared input is
    better reported by the command that needs it.
    """
    resolved_args = getattr(node, "resolved_args", None) or {}
    type_hints = getattr(node.task_def, "type_hints", None) or {}

    mounts: list[Mount] = []
    for name, value in resolved_args.items():
        annotation = type_hints.get(name)
        if annotation is tmp_dir:
            mode: MountMode = "rw"
        elif annotation is not None and is_path_shaped_annotation(annotation):
            mode = "ro"
        else:
            continue
        for path in _iter_declared_paths(value):
            if not path.exists():
                continue
            mounts.append(mount(path if path.is_dir() else path.parent, mode=mode))
    return mounts


def declared_output_mounts(*, output: Any) -> list[Mount]:
    """Return read-write mounts for a directive's declared outputs.

    The parent directory is mounted rather than the output path itself: the
    output does not exist yet, and the runtime would create a *directory* at
    that path to satisfy the mount.  Parents have already been created by the
    caller.

    A directive's ``log`` is deliberately not included. The log is captured on
    the host from the pipes the runtime client writes to, so the container never
    opens that path and needs no access to its directory.
    """
    if output is None:
        return []
    return [mount(path.parent, mode="rw") for path in iter_output_values(output)]


def _iter_declared_paths(value: Any) -> list[Path]:
    """Return every filesystem path a resolved argument value carries."""
    if isinstance(value, AssetRef):
        return [Path(value.artifact_path)]
    if isinstance(value, (str, Path, os.PathLike)):
        return [Path(str(value))]
    if isinstance(value, dict):
        return [path for item in value.values() for path in _iter_declared_paths(item)]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [path for item in value for path in _iter_declared_paths(item)]
    return []


def iter_required_output_values(
    output: Any,
) -> list[Path]:
    """Return only the declared output paths that must exist after execution."""
    if isinstance(output, OptionalOutput):
        return []
    if isinstance(output, (str, AssetResult)):
        return [_declared_item_path(output)]
    return [_declared_item_path(item) for item in output if not isinstance(item, OptionalOutput)]


def resolve_output_value(output: Any) -> Any:
    """Rebuild a declared output with absent optional entries replaced by ``None``.

    Preserves the declared scalar, list, or tuple shape so the task's return
    annotation lines up positionally with what it declared.
    """
    if isinstance(output, OptionalOutput):
        path = _declared_item_path(output)
        return output.payload if path.exists() else None

    if isinstance(output, (str, AssetResult)):
        return output

    resolved = [resolve_output_value(item) for item in output]
    return tuple(resolved) if isinstance(output, tuple) else resolved


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
    else:
        # Anything unrecognised was raised inside a task body, so it is user code
        # unless the class name identifies it as a framework failure.
        exc_name = exc.__class__.__name__.lower()
        if "env" in exc_name or "container" in exc_name:
            kind = "env_mismatch"
        elif "cache" in exc_name:
            kind = "cache_error"
        else:
            kind = "user_code_error"

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
    backend : ExecutionEnvironment | None
        Execution environment used when a task declares a non-default env.
    validator : TaskValidator
        Used to coerce return values for shell tasks.
    log_emitter_factory : Callable
        Factory ``log_emitter_factory(node=..., stream=...)`` returning a
        ``Callable[[str], None]`` that consumes one log chunk.
    usage_recorder : Callable | None
        Optional ``usage_recorder(node=..., measured=...)`` callback that
        receives one measured-usage dict per task subprocess (peak RSS and
        CPU seconds, sampled from the process tree).
    """

    backend: ExecutionEnvironment | None
    validator: TaskValidator
    log_emitter_factory: LogEmitterFactory
    usage_recorder: Callable[..., None] | None = None
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

    def run_subprocess(
        self,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
        env: dict[str, str] | None = None,
        usage_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a subprocess while tracking it for interrupt-time termination.

        When ``usage_callback`` is given, the subprocess tree's peak RSS and
        CPU time are sampled while it runs and reported to the callback once
        the process exits (skipped when nothing could be sampled).
        """
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
        sampler = (
            SubprocessUsageSampler(root_pid=process.pid) if usage_callback is not None else None
        )
        if sampler is not None:
            sampler.start()

        def finish_sampler() -> None:
            if sampler is None:
                return
            sampler.stop()
            usage = sampler.result()
            if usage is not None and usage_callback is not None:
                usage_callback(usage)

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        if not hasattr(process, "stdout") or not hasattr(process, "stderr"):
            try:
                stdout_text, stderr_text = process.communicate()
            finally:
                finish_sampler()
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
            finish_sampler()
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
        mounts: Sequence[Mount] = (),
    ) -> subprocess.CompletedProcess[str]:
        """Run one command while appending to provenance logs.

        The node's declared path inputs are mounted for environments that need
        it; *mounts* carries anything else the command touches, such as the
        declared outputs a shell directive names.
        """
        for path in (node.stdout_path, node.stderr_path, user_log_path):
            if path is not None:
                path.parent.mkdir(parents=True, exist_ok=True)

        subprocess_env = build_shell_subprocess_env(task_def=node.task_def, threads=node.threads)
        if extra_env:
            subprocess_env.update(extra_env)

        if node.task_def.env is not None and self.backend is not None:
            # The subprocess is the environment's launcher, not the command, so
            # an isolated environment is told which of the variables just
            # computed the command itself needs to see.
            argv: str | list[str] = self.backend.exec_argv(
                env=node.task_def.env,
                cmd=cmd,
                mounts=[*declared_input_mounts(node=node), *mounts],
                env_vars=[
                    *computed_env_var_names(task_def=node.task_def),
                    *sorted(extra_env or {}),
                ],
            )
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
            self.log_emitter_factory(node=node, stream=stream)(chunk)

        usage_callback: Callable[[dict[str, Any]], None] | None = None
        if self.usage_recorder is not None:
            recorder = self.usage_recorder

            def usage_callback(measured: dict[str, Any]) -> None:
                recorder(node=node, measured=measured)

        try:
            completed = self.run_subprocess(
                argv=argv,
                use_shell=use_shell,
                on_stdout=lambda chunk: emit_chunk(stream="stdout", chunk=chunk),
                on_stderr=lambda chunk: emit_chunk(stream="stderr", chunk=chunk),
                env=subprocess_env,
                usage_callback=usage_callback,
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

    def failure_hint(self, *, node: Any, exit_code: int, output: str) -> str | None:
        """Ask the execution environment to diagnose a failed command.

        Every driver task kind raises its own error type, so each one asks here
        rather than the diagnosis living with one of them.
        """
        if node.task_def.env is None or self.backend is None:
            return None
        return self.backend.exec_failure_hint(
            env=node.task_def.env, exit_code=exit_code, output=output
        )

    def run_shell(self, *, node: Any, directive: ShellDirective) -> Any:
        """Execute a shell command and return its declared output path or paths."""
        task_def = node.task_def
        user_log_path = Path(directive.log) if directive.log is not None else None

        for output_path in iter_output_values(directive.output):
            remove_declared_output(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        completed = self.run_logged_command(
            node=node,
            cmd=directive.cmd,
            user_log_path=user_log_path,
            mounts=declared_output_mounts(output=directive.output),
        )
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=task_def.name,
                cmd=redact_text(text=directive.cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=directive.log,
                hint=self.failure_hint(
                    node=node, exit_code=completed.returncode, output=combined_output
                ),
            )

        missing_outputs = [
            str(output_path)
            for output_path in iter_required_output_values(directive.output)
            if not output_path.exists()
        ]
        if missing_outputs:
            missing_label = missing_outputs[0] if len(missing_outputs) == 1 else missing_outputs
            raise FileNotFoundError(
                f"Shell task {task_def.name} completed but did not create output {missing_label!r}"
            )

        return self.validator.coerce_return_value(
            task_def=task_def,
            value=resolve_output_value(directive.output),
        )
