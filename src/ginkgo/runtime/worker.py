"""Worker entrypoints for Python task execution."""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
import traceback
from typing import Any

from ginkgo.runtime.module_loader import load_module
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.artifacts.value_codec import decode_value, encode_value


def error_response(exc: BaseException) -> dict[str, Any]:
    """Build the standard error response dict for a failed task."""
    return {
        "ok": False,
        "error": {
            "type": type(exc).__name__,
            "module": type(exc).__module__,
            "message": str(exc),
            "args": [str(a) for a in exc.args],
        },
    }


def run_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a task payload inside a process-pool worker."""
    base_dir = Path(payload["transport_dir"])
    decoded_args = {
        name: decode_value(value, base_dir=base_dir) for name, value in payload["args"].items()
    }

    # Mount any fuse-marked inputs against a task-local mount root so
    # concurrent workers never collide on a shared bucket mount point. The
    # access instance is held before hydration so the finally below always
    # tears mounts down, even if hydration itself fails partway.
    mounted_access = _local_fuse_access(args=decoded_args, base_dir=base_dir)

    stdout_path = payload.get("stdout_path")
    stderr_path = payload.get("stderr_path")
    secret_values = tuple(payload.get("secret_values", ()))
    event_queue = payload.get("log_event_queue")
    log_context = {
        "run_id": payload.get("run_id"),
        "task_id": payload.get("task_id"),
        "task_name": payload.get("task_name"),
        "attempt": payload.get("attempt"),
        "display_label": payload.get("display_label"),
    }
    try:
        with _task_log_context(
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            secret_values=secret_values,
            log_emitter=_queue_log_emitter(event_queue=event_queue, context=log_context),
        ):
            if mounted_access is not None:
                from ginkgo.remote.access.worker_hydration import hydrate_fuse_refs

                decoded_args, _ = hydrate_fuse_refs(
                    args=decoded_args, mounted_access=mounted_access
                )
            task_binding = _load_task_binding(payload=payload)
            fn = getattr(task_binding, "fn", task_binding)
            result = fn(**decoded_args)
    except BaseException as exc:  # pragma: no cover - exercised via parent tests
        if stderr_path is not None:
            with Path(stderr_path).open("a", encoding="utf-8") as handle:
                traceback.print_exc(
                    file=_RedactingWriter(handle=handle, secret_values=secret_values)
                )
        exc.args = (redact_text(text=str(exc), secret_values=secret_values),)
        return error_response(exc)
    finally:
        if mounted_access is not None:
            mounted_access.close()

    if payload.get("dynamic_result", True) and _is_dynamic_result(result):
        response = {"ok": True, "result": result, "result_encoding": "direct"}
    else:
        encoded_result = encode_value(result, base_dir=base_dir)
        response = {"ok": True, "result": encoded_result, "result_encoding": "encoded"}

    if mounted_access is not None:
        response["remote_input_access"] = mounted_access.stats().to_dict()
    return response


def _local_fuse_access(*, args: dict[str, Any], base_dir: Path) -> Any:
    """Build a task-local :class:`MountedAccess` when args carry fuse markers.

    Mount points live under ``base_dir`` (the task's transport dir) so
    parallel process-pool workers do not share a bucket mount point.
    Returns ``None`` when no fuse markers are present so non-streaming
    tasks pay no import or setup cost.
    """
    if not _args_have_fuse_markers(args):
        return None

    from ginkgo.remote.access.mounted import MountedAccess

    return MountedAccess(
        mount_root=base_dir / "fuse",
        cache_root=base_dir / "fuse-cache",
    )


def _args_have_fuse_markers(value: Any) -> bool:
    """Return whether ``value`` contains any fuse marker dict."""
    from ginkgo.remote.access.protocol import is_fuse_ref

    if is_fuse_ref(value):
        return True
    if isinstance(value, dict):
        return any(_args_have_fuse_markers(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_args_have_fuse_markers(item) for item in value)
    return False


def _is_dynamic_result(value: Any) -> bool:
    from ginkgo.core.directive import ExecutionDirective
    from ginkgo.core.expr import Expr, ExprList

    return isinstance(value, (Expr, ExprList, ExecutionDirective))


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
    handles = []

    if stdout_path is not None:
        path = Path(stdout_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stdout_handle = path.open("a", encoding="utf-8")
        handles.append(stdout_handle)
        managers.append(
            contextlib.redirect_stdout(
                _RedactingWriter(
                    handle=stdout_handle,
                    secret_values=secret_values,
                    stream_name="stdout",
                    log_emitter=log_emitter,
                )
            )
        )

    if stderr_path is not None:
        path = Path(stderr_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stderr_handle = path.open("a", encoding="utf-8")
        handles.append(stderr_handle)
        managers.append(
            contextlib.redirect_stderr(
                _RedactingWriter(
                    handle=stderr_handle,
                    secret_values=secret_values,
                    stream_name="stderr",
                    log_emitter=log_emitter,
                )
            )
        )

    with contextlib.ExitStack() as stack:
        for handle in handles:
            stack.callback(handle.close)
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

    def write(self, text: str) -> int:
        redacted = redact_text(text=text, secret_values=self._secret_values)
        written = self._handle.write(redacted)
        if self._log_emitter is not None and self._stream_name is not None and redacted:
            self._log_emitter(stream=self._stream_name, chunk=redacted)
        return written

    def flush(self) -> None:
        try:
            self._handle.flush()
        except ValueError:
            # CPython may flush redirected streams during finalization after
            # the underlying file handle has already been closed.
            return
