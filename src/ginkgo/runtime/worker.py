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

    log_path = payload.get("log_path")
    try:
        with _task_log_context(log_path):
            module = load_module(
                payload["module"],
                module_file=payload.get("module_file"),
            )
            task_binding = getattr(module, payload["task_name"])
            fn = getattr(task_binding, "fn", task_binding)
            result = fn(**decoded_args)
    except BaseException as exc:  # pragma: no cover - exercised via parent tests
        if log_path is not None:
            with Path(log_path).open("a", encoding="utf-8") as handle:
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
def _task_log_context(log_path: str | None):
    """Redirect task stdout/stderr to a per-task log file when configured."""
    if log_path is None:
        yield
        return

    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        with contextlib.redirect_stdout(handle), contextlib.redirect_stderr(handle):
            yield
