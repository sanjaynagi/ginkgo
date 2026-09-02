"""Focused tests for agent-mode runtime event streaming."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


def test_run_agent_verbose_emits_task_log_events(tmp_path: Path) -> None:
    (tmp_path / "workflow.py").write_text(
        """
from ginkgo import flow, task

@task()
def produce() -> str:
    print("streamed stdout line")
    return "ok"

@flow
def main():
    return produce()
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = _run_cli("run", "workflow.py", "--agent-output", "--verbose", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    task_logs = [event for event in events if event["event"] == "task_log"]
    assert task_logs
    assert any(event["stream"] == "stdout" for event in task_logs)
    assert any("streamed stdout line" in event["chunk"] for event in task_logs)


def test_keep_going_streams_the_ignored_failure_and_the_skips(tmp_path: Path) -> None:
    """A scripted run reads the failure policy's outcome off the event stream."""
    (tmp_path / "workflow.py").write_text(
        """
from ginkgo import flow, task

@task()
def load(sample: str) -> str:
    if sample == "bad":
        raise RuntimeError("malformed input")
    return sample

@task()
def analyse(sample: str) -> str:
    return f"analysed:{sample}"

@flow
def main():
    return [analyse(sample=load(sample=name)) for name in ("good", "bad")]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = _run_cli("run", "workflow.py", "--keep-going", "--agent-output", cwd=tmp_path)

    assert result.returncode == 3, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    failed = [event for event in events if event["event"] == "task_failed"]
    skipped = [event for event in events if event["event"] == "task_skipped"]
    completed = [event for event in events if event["event"] == "task_completed"]
    assert [event["ignored"] for event in failed] == [True]
    assert [event["ancestor_task_name"].rsplit(".", 1)[-1] for event in skipped] == ["load"]
    assert any(event["task_name"].endswith(".analyse") for event in completed)
    run_completed = [event for event in events if event["event"] == "run_completed"][-1]
    assert run_completed["status"] == "failed"
    assert run_completed["task_counts"]["skipped"] == 1
