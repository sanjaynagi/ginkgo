"""Focused tests for agent-mode runtime event streaming."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
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

    result = _run_cli("run", "workflow.py", "--agent", "--verbose", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    task_logs = [event for event in events if event["event"] == "task_log"]
    assert task_logs
    assert any(event["stream"] == "stdout" for event in task_logs)
    assert any("streamed stdout line" in event["chunk"] for event in task_logs)
