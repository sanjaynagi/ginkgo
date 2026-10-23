"""Helpers shared by the CLI tests: writing a project and reading a run back."""

from __future__ import annotations

import re
import textwrap
from pathlib import Path
from typing import Any

import yaml

from tests.conftest import CliResult, run_cli

SIMPLE_FLOW = """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
"""


def write(path: str | Path, source: str = "") -> Path:
    """Write dedented *source* to *path*, creating its parent directories."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source).lstrip("\n"), encoding="utf-8")
    return path


def run_ok(*args: str, cwd: Path | None = None) -> CliResult:
    """Run ``ginkgo *args`` in process and assert it exited 0."""
    result = run_cli(*args, cwd=cwd)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def unwrapped(output: str) -> str:
    """Collapse Rich's console wrapping so a multi-word phrase reads contiguously."""
    return " ".join(output.split())


def run_dir_of(output: str) -> Path:
    """Return the run directory a ``ginkgo run`` printed."""
    match = re.search(r"Run directory: (.+)", output)
    assert match is not None, f"Run directory not found in output:\n{output}"
    return Path(match.group(1).strip())


def manifest_of(run_dir: Path) -> dict[str, Any]:
    """Read a run's exported manifest, keying its tasks by task id."""
    data = yaml.safe_load((run_dir / "manifest.yaml").read_text(encoding="utf-8"))
    return {**data, "tasks": {task["task_id"]: task for task in data["tasks"]}}
