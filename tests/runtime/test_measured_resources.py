"""Measured-vs-declared resource usage tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from ginkgo import file, shell, task
from tests.conftest import Ledger
from ginkgo.runtime.environment.resources import SubprocessUsageSampler, _parse_cputime
from ginkgo.runtime.worker import (
    _TaskPeakRss,
    _current_rss_bytes,
    _max_rss_bytes,
    _rusage_snapshot,
    run_task,
)
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("1:02.50", 62.5), ("1:02:03", 3723.0), ("2-01:00:00", 2 * 86400 + 3600.0)],
)
def test_parse_cputime(text: str, seconds: float) -> None:
    assert _parse_cputime(text) == seconds


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

    def test_a_command_shorter_than_the_interval_never_runs_ps(self, monkeypatch) -> None:
        calls: list[int] = []
        monkeypatch.setattr(
            "ginkgo.runtime.environment.resources._sample_tree_usage",
            lambda *, root_pid: calls.append(root_pid),
        )
        sampler = SubprocessUsageSampler(root_pid=1, interval_seconds=10.0)
        sampler.start()
        sampler.stop()

        assert calls == []
        assert sampler.result() is None

    def test_missing_process_yields_none(self) -> None:
        sampler = SubprocessUsageSampler(root_pid=-12345, interval_seconds=0.05)
        sampler.start()
        sampler.stop()
        assert sampler.result() is None


class TestTaskPeakRss:
    """A reused worker reports each task's own peak, not its lifetime one."""

    def test_an_earlier_heavier_task_does_not_set_a_later_tasks_peak(self) -> None:
        heavy = b"x" * 200_000_000
        del heavy
        lifetime = _max_rss_bytes(_rusage_snapshot().ru_maxrss)

        tracker = _TaskPeakRss(usage_start=_rusage_snapshot())
        peak = tracker.stop(usage_end=_rusage_snapshot())

        assert peak < lifetime - 100_000_000

    def test_the_tasks_own_allocation_is_seen(self) -> None:
        tracker = _TaskPeakRss(usage_start=_rusage_snapshot())
        held = b"x" * 100_000_000
        peak = tracker.stop(usage_end=_rusage_snapshot())

        assert held
        assert peak >= 90_000_000

    def test_the_workers_resident_memory_is_the_baseline_not_the_tasks(self) -> None:
        tracker = _TaskPeakRss(usage_start=_rusage_snapshot())
        peak = tracker.stop(usage_end=_rusage_snapshot())

        assert tracker.baseline_bytes >= _current_rss_bytes() // 2
        assert peak < 10_000_000


_held: list[bytes] = []


def _returns_one() -> int:
    return 1


class TestBodyMeasurement:
    """The measurement is the body's, not the worker's work around it."""

    def test_encoding_the_result_is_not_charged_to_the_task(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A fresh worker's first encode imports and allocates; a task that
        # allocates nothing must still read close to zero.
        def heavy_encode(value: Any, *, base_dir: Path, artifact_store: Any) -> Any:
            _held.append(b"x" * 100_000_000)
            return value

        monkeypatch.setattr("ginkgo.runtime.worker.encode_value", heavy_encode)
        try:
            response = run_task(
                {
                    "transport_dir": str(tmp_path),
                    "args": {},
                    "module": __name__,
                    "module_file": __file__,
                    "binding_name": "_returns_one",
                }
            )
        finally:
            _held.clear()

        assert response["ok"] is True
        assert response["measured_resources"]["peak_rss_bytes"] < 20_000_000


def test_measurements_merge_across_attempts_and_survive_a_retry() -> None:
    evaluator = ConcurrentEvaluator(jobs=1)
    (node_id,) = evaluator._register(_retried_work())
    node = evaluator.task_nodes[node_id]
    for peak, cpu in ((100, 1.0), (50, 0.5)):
        evaluator._record_measured_usage(
            node=node, measured={"peak_rss_bytes": peak, "cpu_seconds": cpu, "source": "sampled"}
        )

    evaluator._schedule_retry(node=node, exc=RuntimeError("oom"))

    assert node.measured_resources == {
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
        # The 4 MB the body allocates, not the worker's imports.
        assert usage["measured"]["peak_rss_bytes"] < 50_000_000
        assert usage["measured"]["baseline_rss_bytes"] > usage["measured"]["peak_rss_bytes"]
        assert usage["measured"]["cpu_seconds"] >= 0.0
        assert usage["declared"] == {
            "threads": 1,
            "memory": "1Gi",
            "memory_gb": 1,
            "effective_memory_gb": 1,
        }

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
        assert usage["measured"]["baseline_rss_bytes"] > 0
