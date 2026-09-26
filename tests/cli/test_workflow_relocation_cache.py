"""Regression cover for issue #291.

Renaming or copying a byte-identical single-file workflow used to cold-start
every cached task, because the synthetic per-path module name the workflow
loads under (see ``USER_MODULE_PREFIX``) leaked into the cache key's task
identity and into its source hash. A workflow run twice from the same path
was already known to hit cache; the case that mattered was a *third* run of
the identical bytes under a new file name.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"

WORKFLOW_SOURCE = (
    """
from ginkgo import flow, task

@task()
def greet(name: str) -> str:
    return f"hello {name}"

@flow
def main():
    return greet(name="world")
""".strip()
    + "\n"
)


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


class TestRenamedWorkflowStaysCached:
    def test_byte_identical_copy_hits_cache_under_a_new_name(self, tmp_path: Path) -> None:
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text(WORKFLOW_SOURCE, encoding="utf-8")

        first = _run_cli("run", "workflow.py", cwd=tmp_path)
        assert first.returncode == 0, first.stderr
        assert "1 tasks executed, 0 cached" in first.stdout

        second = _run_cli("run", "workflow.py", cwd=tmp_path)
        assert second.returncode == 0, second.stderr
        assert "0 tasks executed, 1 cached" in second.stdout

        renamed_path = tmp_path / "renamed.py"
        renamed_path.write_text(workflow_path.read_text(encoding="utf-8"), encoding="utf-8")

        third = _run_cli("run", "renamed.py", cwd=tmp_path)
        assert third.returncode == 0, third.stderr
        assert "0 tasks executed, 1 cached" in third.stdout

    def test_copy_into_a_different_directory_hits_cache_too(self, tmp_path: Path) -> None:
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        first_dir = tmp_path / "a"
        first_dir.mkdir()
        (first_dir / "workflow.py").write_text(WORKFLOW_SOURCE, encoding="utf-8")

        first = _run_cli("run", "a/workflow.py", cwd=tmp_path)
        assert first.returncode == 0, first.stderr
        assert "1 tasks executed, 0 cached" in first.stdout

        second_dir = tmp_path / "b"
        second_dir.mkdir()
        (second_dir / "renamed.py").write_text(WORKFLOW_SOURCE, encoding="utf-8")

        second = _run_cli("run", "b/renamed.py", cwd=tmp_path)
        assert second.returncode == 0, second.stderr
        assert "0 tasks executed, 1 cached" in second.stdout
