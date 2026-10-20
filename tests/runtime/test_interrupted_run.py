"""An interrupted run closes in the ledger under the ``cancelled`` status.

Ctrl-C on a `ginkgo run` used to leave the run recorded `running` forever.
Two things had to be true for that: CP-SAT disarmed the scheduler's interrupt
handler on every dispatch, so the signal killed the process outright, and the
paths that do close a run only knew ``success`` and ``failed``. Both are
covered here — the signal one in a subprocess, because a regression in it
would otherwise take the test session down with it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from ginkgo.runtime.events import RunCompleted

PYTHON = Path(sys.executable)

_WORKFLOW = """
import time
from ginkgo import flow, task


@task()
def slow(i: int) -> int:
    time.sleep(SLEEP_SECONDS)
    return i


@task()
def total(values: list[int]) -> int:
    return sum(values)


@flow
def main():
    return total(values=slow().map(i=[1, 2, 3, 4, 5, 6]))
"""

#: Installs a handler, dispatches once through CP-SAT, then interrupts itself.
#: Prints "handled" only if the handler survived the solve; a process whose
#: handler was disarmed dies of the signal instead and never prints.
_SIGNAL_SCRIPT = """
import signal

from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset

seen = []
signal.signal(signal.SIGINT, lambda *_: seen.append("sigint"))
select_dispatch_subset(
    ready_tasks=[SchedulableTask(node_id=1, threads=1, memory_gb=1)],
    jobs=2,
    cores=2,
    memory=4,
    gpus=0,
    available_group_slots=None,
    custom_budgets=None,
)
signal.raise_signal(signal.SIGINT)
print("handled" if seen else "swallowed")
"""


#: Interrupts itself mid-"solve", while a stand-in for CP-SAT's handler is
#: installed, as a Ctrl-C landing during a dispatch solve would. Prints
#: "handled" only if the interrupt reached the scheduler's handler after the
#: solve, rather than the solver's during it.
_MID_SOLVE_SIGNAL_SCRIPT = """
import signal

from ginkgo.runtime.scheduler import _signal_handlers_preserved

seen = []
signal.signal(signal.SIGINT, lambda *_: seen.append("scheduler"))
with _signal_handlers_preserved():
    signal.signal(signal.SIGINT, lambda *_: seen.append("solver"))
    signal.raise_signal(signal.SIGINT)
print("handled" if seen == ["scheduler"] else f"lost: {seen}")
"""


def _write_project(root: Path, *, sleep_seconds: int = 3) -> None:
    """Write a workspace whose one workflow runs six tasks of *sleep_seconds* each."""
    (root / "ginkgo.toml").write_text('[project]\nname = "interrupt"\n', encoding="utf-8")
    package = root / "workflow"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    workflow = f"SLEEP_SECONDS = {sleep_seconds}\n" + textwrap.dedent(_WORKFLOW)
    (package / "flow.py").write_text(workflow, encoding="utf-8")


def _detach() -> None:
    """Give the child its own session and the default interrupt disposition.

    Without this the child inherits the test session's process group and,
    when pytest runs it in the background, ``SIG_IGN`` — neither of which is
    what a terminal Ctrl-C looks like.
    """
    os.setsid()
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def _wait_for_a_succeeded_task(*, root: Path, timeout: float = 60.0) -> None:
    """Block until the workspace's ledger records a succeeded task."""
    import ginkgo.query as query
    from ginkgo.workspace_layout import WorkspaceLayout

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        time.sleep(0.2)
        with query.open(WorkspaceLayout(root=root / ".ginkgo"), missing_ok=True) as reader:
            rows = reader.runs(limit=1)
            if rows and reader.task_status_counts(rows[0].run_id).get("succeeded"):
                return
    pytest.fail("no task succeeded before the interrupt was due")


class TestCpSatSignalHandling:
    """CP-SAT must not disarm the interrupt handler the scheduler runs under."""

    def test_dispatch_selection_leaves_the_interrupt_handler_armed(self) -> None:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(_SIGNAL_SCRIPT)],
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "handled"

    def test_an_interrupt_during_a_solve_reaches_the_scheduler(self) -> None:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(_MID_SOLVE_SIGNAL_SCRIPT)],
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "handled"


class TestCancelledRunSummary:
    """A cancelled run reads back as terminal, and as not a success."""

    def test_run_summary_reports_a_cancelled_run(self, ledger) -> None:
        summary = ledger.finish(status="cancelled", error="Interrupted by SIGINT")

        assert summary.status == "cancelled"
        assert not summary.succeeded


class TestCloseUnfinishedRun:
    """The exit-stack safety net tells an interrupt apart from a failure."""

    def _emitted(self, exc: BaseException | None) -> RunCompleted:
        from ginkgo.cli.commands.run import _close_unfinished_run
        from ginkgo.runtime.events import EventBus

        class _Recorder:
            completed = False

        emitted: list[RunCompleted] = []
        bus = EventBus()
        bus.subscribe(emitted.append)
        try:
            if exc is not None:
                raise exc
        except BaseException:
            _close_unfinished_run(bus=bus, recorder=_Recorder(), run_id="run-1")
        else:
            _close_unfinished_run(bus=bus, recorder=_Recorder(), run_id="run-1")
        return emitted[0]

    def test_an_interrupt_closes_the_run_cancelled(self) -> None:
        assert self._emitted(KeyboardInterrupt("Interrupted by SIGINT")).status == "cancelled"

    def test_any_other_exception_still_closes_the_run_failed(self) -> None:
        assert self._emitted(RuntimeError("boom")).status == "failed"

    def test_no_exception_at_all_closes_the_run_failed(self) -> None:
        assert self._emitted(None).status == "failed"


@pytest.mark.integration
class TestInterruptedRunEndToEnd:
    """The whole path: SIGINT a real run, then read what the ledger says."""

    def test_ctrl_c_records_the_run_cancelled_and_exits_130(self, tmp_path: Path) -> None:
        if not PYTHON.exists():  # pragma: no cover - depends on a prepared env
            pytest.skip("pixi environment not installed")
        _write_project(tmp_path)

        process = subprocess.Popen(
            [str(PYTHON), "-m", "ginkgo.cli", "run", "--jobs", "2"],
            cwd=tmp_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=_detach,
        )
        # Interrupted once the first wave has finished, which puts a CP-SAT
        # solve between the run starting and the interrupt arriving.
        _wait_for_a_succeeded_task(root=tmp_path)
        process.send_signal(signal.SIGINT)
        output = process.communicate(timeout=60)[0]

        # A clean 130, not death by signal: the run got to unwind and record.
        assert process.returncode == 130, output
        assert "Cancelled after" in output

        import ginkgo.query as query
        from ginkgo.workspace_layout import WorkspaceLayout

        with query.open(WorkspaceLayout(root=tmp_path / ".ginkgo")) as reader:
            rows = reader.runs(limit=5)
            assert [row.status for row in rows] == ["cancelled"]
            assert rows[0].finished_at is not None
            summary = reader.run(rows[0].run_id)

        # The tally of what had finished by the interrupt is part of the record.
        assert summary.status == "cancelled"
        assert 0 < summary.succeeded_count < len(summary.tasks)
        # Every task ends terminal: what never started is skipped, not pending.
        assert all(task.is_terminal() for task in summary.tasks)
        assert any(
            (task.skipped_because or {}).get("reason") == "interrupted" for task in summary.tasks
        )

    def test_a_cancelled_run_is_listed_by_its_status(self, tmp_path: Path) -> None:
        if not PYTHON.exists():  # pragma: no cover - depends on a prepared env
            pytest.skip("pixi environment not installed")
        from tests.conftest import Ledger

        ledger = Ledger.start(root=tmp_path, run_id="20260101_000000_000000_abcdef12")
        ledger.finish(status="cancelled")
        ledger.close()

        listing = subprocess.run(
            [str(PYTHON), "-m", "ginkgo.cli", "runs", "ls", "--status", "cancelled"],
            cwd=tmp_path,
            check=False,
            text=True,
            capture_output=True,
        )
        assert "20260101_000000_000000_abcdef12" in listing.stdout

    @pytest.mark.parametrize("delay", [0.1, 0.8, 1.6, 2.4, 4.0])
    def test_ctrl_c_at_any_moment_exits_promptly_with_130(
        self, tmp_path: Path, delay: float
    ) -> None:
        # The delays land the signal while the CLI imports, while the run
        # starts its pools, and while tasks run. Only the parent is signalled,
        # as `kill -INT` would: the run must stop its minute-long tasks rather
        # than wait for them.
        if not PYTHON.exists():  # pragma: no cover - depends on a prepared env
            pytest.skip("pixi environment not installed")
        _write_project(tmp_path, sleep_seconds=60)

        process = subprocess.Popen(
            [str(PYTHON), "-m", "ginkgo.cli", "run", "--jobs", "2"],
            cwd=tmp_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            preexec_fn=_detach,
        )
        time.sleep(delay)
        process.send_signal(signal.SIGINT)
        try:
            output = process.communicate(timeout=15)[0]
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            pytest.fail(f"ginkgo run did not exit within 15s of SIGINT at {delay}s")

        assert process.returncode == 130, output
