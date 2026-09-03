"""Measured-vs-declared resource usage tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from ginkgo import file, shell, task
from tests.conftest import Ledger
from ginkgo.runtime.environment.resources import SubprocessUsageSampler, _parse_cputime
from ginkgo.runtime.evaluator import ConcurrentEvaluator


class TestParseCputime:
    def test_minutes_seconds(self) -> None:
        assert _parse_cputime("1:02.50") == 62.5

    def test_hours_minutes_seconds(self) -> None:
        assert _parse_cputime("1:02:03") == 3723.0

    def test_days_prefix(self) -> None:
        assert _parse_cputime("2-01:00:00") == 2 * 86400 + 3600.0


class TestSubprocessUsageSampler:
    def test_samples_live_subprocess(self) -> None:
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(0.5)"])
        sampler = SubprocessUsageSampler(root_pid=process.pid, interval_seconds=0.1)
        sampler.start()
        try:
            process.wait()
        finally:
            sampler.stop()

        usage = sampler.result()
        assert usage is not None
        assert usage["peak_rss_bytes"] > 0
        assert usage["cpu_seconds"] >= 0.0
        assert usage["source"] == "sampled"

    def test_missing_process_yields_none(self) -> None:
        sampler = SubprocessUsageSampler(root_pid=-12345, interval_seconds=0.05)
        sampler.start()
        sampler.stop()
        assert sampler.result() is None


class TestUsageMerging:
    def test_peaks_take_max_and_cpu_accumulates(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1)
        holder: Any = SimpleNamespace(measured_resources=None)
        evaluator._record_measured_usage(
            node=holder, measured={"peak_rss_bytes": 100, "cpu_seconds": 1.0, "source": "sampled"}
        )
        evaluator._record_measured_usage(
            node=holder, measured={"peak_rss_bytes": 50, "cpu_seconds": 0.5, "source": "sampled"}
        )
        assert holder.measured_resources == {
            "peak_rss_bytes": 100,
            "cpu_seconds": 1.5,
            "source": "sampled",
        }


# ----- End-to-end manifest tests ----------------------------------------------


@task(memory="1Gi")
def _python_work() -> int:
    data = bytearray(4_000_000)
    return len(data)


@task("shell")
def _shell_work(out: str):
    return shell(cmd=f"sleep 0.4 && echo done > {out}", output=file(out))


@task(retries=1)
def _retried_work() -> int:
    return 1


@task()
def _failing_work() -> int:
    raise RuntimeError("boom")


def _run_with_ledger(expr: Any, tmp_path: Path) -> dict[str, Any]:
    ledger = Ledger.start(root=tmp_path, run_id="run-measured")
    try:
        ConcurrentEvaluator(jobs=2, run_dir=ledger.run_dir, event_bus=ledger.bus).evaluate(expr)
        return _single_task_usage(ledger.finish())
    finally:
        ledger.close()


def _single_task_usage(summary: Any) -> dict[str, Any]:
    (task,) = summary.tasks
    return task.resource_usage


class TestManifestRecording:
    def test_python_task_records_rusage(self, tmp_path: Path) -> None:
        usage = _run_with_ledger(_python_work(), tmp_path)
        assert usage["measured"]["source"] == "rusage"
        assert usage["measured"]["peak_rss_bytes"] > 0
        assert usage["measured"]["cpu_seconds"] >= 0.0
        assert usage["declared"] == {
            "threads": 1,
            "memory_gb": 1,
            "effective_memory_gb": 1,
        }

    def test_an_escalated_retry_records_both_budgets(self, tmp_path: Path) -> None:
        """A retry runs against a budget the declaration never named.

        ``node.memory_gb`` is raised in place by escalation, so recording only
        it would lose the number a user edits; recording only the declaration
        would call a 30 GiB peak an overrun of 16 when the attempt was given 32
        and fitted.
        """
        from ginkgo.runtime.evaluator import NodeRun

        node = NodeRun.__new__(NodeRun)
        node.threads = 4
        node.memory_gb = 32  # after escalation
        node.declared_memory_gb = 16  # what the task asked for
        node.measured_resources = {"peak_rss_bytes": 30_000, "cpu_seconds": 1.0}

        usage = ConcurrentEvaluator(jobs=1)._resource_usage_for(node=node)

        assert usage["declared"]["memory_gb"] == 16
        assert usage["declared"]["effective_memory_gb"] == 32

    def test_shell_task_records_sampled_usage(self, tmp_path: Path) -> None:
        out = tmp_path / "shell-out.txt"
        usage = _run_with_ledger(_shell_work(out=str(out)), tmp_path)
        assert usage["measured"]["source"] == "sampled"
        assert usage["measured"]["peak_rss_bytes"] > 0
        assert usage["declared"]["threads"] == 1

    def test_failed_task_records_usage(self, tmp_path: Path) -> None:
        ledger = Ledger.start(root=tmp_path, run_id="run-failed")
        evaluator = ConcurrentEvaluator(jobs=1, run_dir=ledger.run_dir, event_bus=ledger.bus)
        with pytest.raises(RuntimeError, match="boom"):
            evaluator.evaluate(_failing_work())
        usage = _single_task_usage(ledger.finish(status="failed"))
        ledger.close()
        assert usage["measured"]["source"] == "rusage"
        assert usage["measured"]["peak_rss_bytes"] > 0


class TestRetryCarry:
    def test_schedule_retry_keeps_measurements(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1)
        node_id = evaluator._register_expr(_retried_work())
        node = evaluator.task_nodes[node_id]
        node.measured_resources = {"peak_rss_bytes": 42, "cpu_seconds": 0.1, "source": "rusage"}

        evaluator._schedule_retry(node=node, exc=RuntimeError("oom"))

        assert node.measured_resources == {
            "peak_rss_bytes": 42,
            "cpu_seconds": 0.1,
            "source": "rusage",
        }
