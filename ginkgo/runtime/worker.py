"""Worker entrypoints for Python task execution."""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
import traceback
from typing import Any

from ginkgo.runtime.module_loader import load_module
from ginkgo.runtime.secrets import redact_text
from ginkgo.runtime.value_codec import decode_value, encode_value


def run_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a task payload inside a process-pool worker."""
    base_dir = Path(payload["transport_dir"])
    decoded_args = {
        name: decode_value(value, base_dir=base_dir) for name, value in payload["args"].items()
    }

    stdout_path = payload.get("stdout_path")
    stderr_path = payload.get("stderr_path")
    secret_values = tuple(payload.get("secret_values", ()))
    try:
        with _task_log_context(
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            secret_values=secret_values,
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
        return {
            "error": {
                "args": exc.args,
                "message": str(exc),
                "module": type(exc).__module__,
                "type": type(exc).__name__,
            },
            "ok": False,
        }

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
        return getattr(module, payload["task_name"])
    except BaseException as exc:
        if payload.get("env") is None or payload.get("task_kind") != "python":
            raise

        task_name = f"{payload['module']}.{payload['task_name']}"
        env_name = payload["env"]
        raise RuntimeError(
            f"Foreign Python task {task_name} could not be imported inside env {env_name!r}. "
            "Python tasks with env= must live in importable packaged modules available in the "
            "target environment. Use @task(kind='shell') for shell command wrappers."
        ) from exc


@contextlib.contextmanager
def _task_log_context(
    *,
    stdout_path: str | None,
    stderr_path: str | None,
    secret_values: tuple[str, ...] = (),
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
                _RedactingWriter(handle=stdout_handle, secret_values=secret_values)
            )
        )

    if stderr_path is not None:
        path = Path(stderr_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stderr_handle = path.open("a", encoding="utf-8")
        handles.append(stderr_handle)
        managers.append(
            contextlib.redirect_stderr(
                _RedactingWriter(handle=stderr_handle, secret_values=secret_values)
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

    def __init__(self, *, handle: io.TextIOBase, secret_values: tuple[str, ...]) -> None:
        self._handle = handle
        self._secret_values = secret_values

    def write(self, text: str) -> int:
        redacted = redact_text(text=text, secret_values=self._secret_values)
        return self._handle.write(redacted)

    def flush(self) -> None:
        self._handle.flush()
