"""Output capture for command tasks: binary output, and failures that print a lot."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import Out, evaluate, file, shell, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.task_runners.shell import RETAINED_OUTPUT_CHARS, ShellTaskError
from ginkgo.store import open_store
from tests.conftest import Ledger


@task(kind="shell")
def print_binary(dest: Out[file], log_path: str) -> file:
    # More than a pipe buffer of undecodable bytes, then text after them.
    return shell(
        cmd=f"head -c 200000 /dev/zero | tr '\\0' '\\377'; echo; echo after; touch {dest}",
        output=dest,
        log=log_path,
    )


def test_a_command_printing_binary_succeeds_and_keeps_its_output(tmp_path: Path) -> None:
    log_path = tmp_path / "binary.log"
    evaluate(print_binary(dest=str(tmp_path / "done.txt"), log_path=str(log_path)))

    log = log_path.read_text(encoding="utf-8")
    assert log.count("�") == 200_000
    assert log.endswith("after\n")


@task(kind="shell")
def fail_loudly(dest: Out[file]) -> file:
    return shell(cmd="head -c 500000 /dev/zero | tr '\\0' 'x'; echo; echo last words; exit 3")


def test_a_failure_quotes_a_bounded_tail_and_the_log_keeps_everything(tmp_path: Path) -> None:
    ledger = Ledger.start(root=tmp_path)
    try:
        with pytest.raises(ShellTaskError) as raised:
            ConcurrentEvaluator(jobs=1, run_dir=ledger.run_dir, event_bus=ledger.bus).evaluate(
                fail_loudly(dest=str(tmp_path / "out.txt"))
            )
        ledger.finish(status="failed", error=str(raised.value))
        with open_store(ledger.db, readonly=True) as store:
            largest_event = store.query("SELECT max(length(payload)) AS size FROM events")
    finally:
        ledger.close()

    message = str(raised.value)
    assert len(message) < 2 * RETAINED_OUTPUT_CHARS + 1_000
    assert message.rstrip().endswith("last words")
    assert "earlier characters are only in the task's log" in message
    assert largest_event[0]["size"] < 3 * RETAINED_OUTPUT_CHARS
    assert (ledger.path / "manifest.yaml").stat().st_size < 6 * RETAINED_OUTPUT_CHARS
    stdout_log = next((ledger.path / "logs").glob("*fail_loudly.stdout.log"))
    assert stdout_log.stat().st_size > 500_000
