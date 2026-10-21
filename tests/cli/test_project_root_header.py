"""NC-2: a run says which project it runs, and when it is a parent's."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

FLOW = """\
from ginkgo import flow, task


@task()
def one() -> int:
    return 1


@flow
def main():
    return one()
"""


def _dry_run(cwd: Path) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "ginkgo.cli", "run", "--dry-run"],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _project(tmp_path: Path) -> Path:
    (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
    (tmp_path / "workflow").mkdir()
    (tmp_path / "workflow" / "flow.py").write_text(FLOW, encoding="utf-8")
    return tmp_path


def test_the_header_names_the_project_root(tmp_path: Path) -> None:
    root = _project(tmp_path)

    out = _dry_run(root)

    assert f"Project: {root.resolve()}" in out.replace("\n", "")
    assert "Run from" not in out


def test_a_bare_run_in_a_subdirectory_says_it_uses_the_parent_project(tmp_path: Path) -> None:
    root = _project(tmp_path)
    (root / "analysis").mkdir()

    out = _dry_run(root / "analysis")

    assert f"Project: {root.resolve()}" in out.replace("\n", "")
    assert "Run from analysis/, which has no ginkgo.toml of its own" in out
