"""Tests for the process-pool worker's task log capture (issue #125)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from ginkgo.runtime.worker import _RedactingWriter, _task_log_context, _traceback_from_task


@pytest.mark.parametrize("closes", [["writer"], ["writer", "writer"], ["handle"]])
def test_a_late_write_to_a_closed_task_log_is_a_noop(tmp_path: Path, closes: list[str]) -> None:
    """A writer reference that escapes the task body must not raise once closed.

    Closing the handle directly simulates a concurrent ``close()`` landing
    between the writer's own closed check and the underlying write.
    """
    handle = (tmp_path / "out.log").open("a", encoding="utf-8")
    writer = _RedactingWriter(handle=handle, secret_values=())

    for closed in closes:
        (handle if closed == "handle" else writer).close()

    assert writer.write("late write\n") == 0
    writer.flush()


def test_the_task_log_context_s_writer_survives_a_late_write(tmp_path: Path) -> None:
    with _task_log_context(
        stdout_path=str(tmp_path / "stdout.log"), stderr_path=str(tmp_path / "stderr.log")
    ):
        captured_stderr = sys.stderr

    captured_stderr.write("late write to closed handle\n")
    captured_stderr.flush()


def _user_task() -> None:
    raise ValueError("from the task")


def _caught_in_worker(call) -> BaseException:
    """Raise *call*'s exception and catch it here, as ``run_task`` does."""
    try:
        call()
    except BaseException as exc:
        return exc
    raise AssertionError("expected an exception")


def test_a_task_traceback_starts_at_the_tasks_frame() -> None:
    exc = _caught_in_worker(_user_task)

    tb = _traceback_from_task(exc=exc, fn=_user_task)

    assert tb is not None
    assert tb.tb_frame.f_code is _user_task.__code__


def test_a_traceback_not_from_the_task_keeps_every_frame() -> None:
    def _load_fails() -> None:
        raise ImportError("cannot load the task")

    exc = _caught_in_worker(_load_fails)

    assert _traceback_from_task(exc=exc, fn=None) is exc.__traceback__
    assert _traceback_from_task(exc=exc, fn=_user_task) is exc.__traceback__


def test_a_worker_starts_without_importing_numpy_or_pandas() -> None:
    # A fresh process, as a spawned worker is: what it imports to load the
    # worker and a workflow's public API is charged to its first task.
    script = (
        "import sys\n"
        "import ginkgo.runtime.worker\n"
        "from ginkgo import Out, file, flow, folder, shell, task\n"
        "print(sorted({'numpy', 'pandas'} & set(sys.modules)))\n"
    )

    result = subprocess.run(
        [sys.executable, "-c", script], check=True, text=True, capture_output=True
    )

    assert result.stdout.strip() == "[]"
