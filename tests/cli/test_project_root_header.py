"""NC-2: a run says which project it runs, and when it is a parent's."""

from __future__ import annotations

from pathlib import Path

from tests.cli.conftest import run_ok, write

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
    return run_ok("run", "--dry-run", cwd=cwd).stdout


def _project(tmp_path: Path) -> Path:
    write(tmp_path / "ginkgo.toml")
    write(tmp_path / "workflow" / "flow.py", FLOW)
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
