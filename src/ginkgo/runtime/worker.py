"""Worker entrypoints for Python task execution."""

from __future__ import annotations

import contextlib
from pathlib import Path
import sys
import traceback
from typing import Any

from ginkgo.runtime.module_loader import load_module
from ginkgo.runtime.value_codec import decode_value, encode_value


def run_task(payload: dict[str, Any]) -> dict[str, Any]:
    """Execute a task payload inside a process-pool worker."""
    for path in payload.get("sys_path", []):
        if path not in sys.path:
            sys.path.insert(0, path)

    base_dir = Path(payload["transport_dir"])
    decoded_args = {
        name: decode_value(value, base_dir=base_dir) for name, value in payload["args"].items()
    }

    stdout_path = payload.get("stdout_path")
    stderr_path = payload.get("stderr_path")
    try:
        with _task_log_context(stdout_path=stdout_path, stderr_path=stderr_path):
            module = load_module(
                payload["module"],
                module_file=payload.get("module_file"),
            )
            task_binding = getattr(module, payload["task_name"])
            fn = getattr(task_binding, "fn", task_binding)
            result = fn(**decoded_args)
    except BaseException as exc:  # pragma: no cover - exercised via parent tests
        if stderr_path is not None:
            with Path(stderr_path).open("a", encoding="utf-8") as handle:
                traceback.print_exc(file=handle)
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


@contextlib.contextmanager
def _task_log_context(*, stdout_path: str | None, stderr_path: str | None):
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
        managers.append(contextlib.redirect_stdout(stdout_handle))

    if stderr_path is not None:
        path = Path(stderr_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        stderr_handle = path.open("a", encoding="utf-8")
        handles.append(stderr_handle)
        managers.append(contextlib.redirect_stderr(stderr_handle))

    with contextlib.ExitStack() as stack:
        for handle in handles:
            stack.callback(handle.close)
        for manager in managers:
            stack.enter_context(manager)
        yield
