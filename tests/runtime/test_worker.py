"""Tests for the process-pool worker's task log capture (issue #125)."""

from __future__ import annotations

from pathlib import Path

from ginkgo.runtime.worker import _RedactingWriter, _task_log_context, _traceback_from_task


def test_redacting_writer_write_after_close_is_a_noop(tmp_path: Path) -> None:
    """A write to an already-closed writer must not raise."""
    handle = (tmp_path / "out.log").open("a", encoding="utf-8")
    writer = _RedactingWriter(handle=handle, secret_values=())

    writer.close()

    # Should be a silent no-op, not a ValueError on the closed handle.
    assert writer.write("late write\n") == 0
    writer.flush()


def test_captured_writer_survives_late_write_after_task_context_exits(
    tmp_path: Path,
) -> None:
    """Reproduce the reported scenario: a reference to the per-task writer
    escapes the task body (e.g. via an atexit-style hook) and is written to
    after `_task_log_context` has exited and closed the underlying handle.
    """
    stdout_path = tmp_path / "stdout.log"
    stderr_path = tmp_path / "stderr.log"

    captured_stderr = None
    with _task_log_context(stdout_path=str(stdout_path), stderr_path=str(stderr_path)):
        import sys

        captured_stderr = sys.stderr

    # The task context has exited; the underlying handle is closed, but a
    # late write through the escaped writer reference must be a safe no-op.
    captured_stderr.write("late write to closed handle\n")
    captured_stderr.flush()


def test_redacting_writer_close_is_idempotent(tmp_path: Path) -> None:
    """Closing a writer twice must not raise."""
    handle = (tmp_path / "out.log").open("a", encoding="utf-8")
    writer = _RedactingWriter(handle=handle, secret_values=())

    writer.close()
    writer.close()


def test_redacting_writer_write_racing_concurrent_close_is_a_noop(tmp_path: Path) -> None:
    """A write must not raise even if the handle closes between the
    writer's own _closed check and the underlying handle.write() call.

    This simulates the race directly: the handle is closed out from under
    an in-flight write, bypassing the writer's _closed fast path, which is
    exactly what a concurrent close() from another thread could do.
    """
    handle = (tmp_path / "out.log").open("a", encoding="utf-8")
    writer = _RedactingWriter(handle=handle, secret_values=())

    handle.close()

    assert writer.write("late write\n") == 0
    writer.flush()


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
