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

    if payload.get("dynamic_result", True) and _is_dynamic_result(result):
        return {"ok": True, "result": result, "result_encoding": "direct"}

    encoded_result = encode_value(result, base_dir=base_dir)
    return {"ok": True, "result": encoded_result, "result_encoding": "encoded"}


def _is_dynamic_result(value: Any) -> bool:
    from ginkgo.core.expr import Expr, ExprList
    from ginkgo.core.shell import ShellExpr

    return isinstance(value, (Expr, ExprList, ShellExpr))


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
