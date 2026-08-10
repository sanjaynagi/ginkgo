"""Tests for the process-pool worker's task log capture (issue #125)."""

from __future__ import annotations

from pathlib import Path

from ginkgo.runtime.worker import _RedactingWriter, _task_log_context


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
