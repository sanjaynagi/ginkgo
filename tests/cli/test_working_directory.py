"""Tests for project-root normalisation of the CLI working directory.

Ginkgo's runtime reads the working directory as the project root in dozens of
places. The CLI makes that true by moving to the discovered root before it
dispatches, so a command run from a subdirectory must land its state, its
config, and its outputs where a command run from the root would.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from ginkgo.cli.app import _normalize_working_directory
from tests.cli.conftest import run_ok, write

WORKFLOW_SOURCE = """
import ginkgo
from pathlib import Path
from ginkgo import flow, task

cfg = ginkgo.config("ginkgo.toml")

@task()
def write_message(message: str, output_path: str) -> str:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(message, encoding="utf-8")
    return output_path

@flow
def main():
    return write_message(message=cfg["message"], output_path="results/out.txt")
"""


@pytest.fixture
def workflow_dir(tmp_path: Path) -> Path:
    """A runnable project whose workflow sits in a `workflow/` subdirectory."""
    write(tmp_path / "ginkgo.toml", 'message = "from-root"\n')
    write(tmp_path / "workflow" / "flow.py", WORKFLOW_SOURCE)
    return tmp_path / "workflow"


def _out(root: Path) -> str:
    return (root / "results" / "out.txt").read_text(encoding="utf-8")


def test_a_run_from_a_subdirectory_behaves_as_one_from_the_root(
    tmp_path: Path, workflow_dir: Path
) -> None:
    run_ok("run", "flow.py", cwd=workflow_dir)

    # State and relative outputs land at the root; ginkgo.config("ginkgo.toml")
    # is root-relative, so the message proves the root's config was read.
    assert (tmp_path / ".ginkgo").is_dir() and not (workflow_dir / ".ginkgo").exists()
    assert _out(tmp_path) == "from-root"
    assert not (workflow_dir / "results").exists()
    # Read back from the subdirectory too.
    assert "write_message" in run_ok("cache", "ls", cwd=workflow_dir).stdout


def test_relative_config_override_resolves_against_the_invocation_directory(
    tmp_path: Path, workflow_dir: Path
) -> None:
    write(workflow_dir / "override.toml", 'message = "overridden"\n')
    run_ok("run", "flow.py", "--config", "override.toml", cwd=workflow_dir)
    assert _out(tmp_path) == "overridden"


def test_init_scaffolds_relative_to_the_invocation_directory(
    tmp_path: Path, workflow_dir: Path
) -> None:
    """`ginkgo init` creates a project rather than running inside one."""
    run_ok("init", "nested", "--no-skills", cwd=workflow_dir)
    assert (workflow_dir / "nested" / "ginkgo.toml").is_file()
    assert not (tmp_path / "nested").exists()


@pytest.mark.parametrize(
    ("project", "cwd", "args", "moves"),
    [
        (True, ".", {"command": "run", "workflow": None, "config": []}, False),
        (False, "elsewhere", {"command": "run", "workflow": None, "config": []}, False),
        (True, "workflow", {"command": "init", "directory": "nested"}, False),
        (True, "workflow", {"command": "run", "workflow": None, "config": []}, True),
    ],
    ids=["at-root", "no-project", "init-stays", "subdirectory"],
)
def test_normalize_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, project: bool, cwd: str, args, moves
) -> None:
    if project:
        write(tmp_path / "ginkgo.toml")
    start = (tmp_path / cwd).resolve()
    start.mkdir(exist_ok=True)
    monkeypatch.chdir(start)
    _normalize_working_directory(argparse.Namespace(**args))
    assert Path.cwd() == (tmp_path.resolve() if moves else start)


def test_path_arguments_are_absolutised_before_moving(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path / "ginkgo.toml")
    nested = (tmp_path / "workflow").resolve()
    nested.mkdir()
    monkeypatch.chdir(nested)
    args = argparse.Namespace(
        command="run", workflow="flow.py", config=["a.toml", "b.toml"], out="report-dir"
    )

    _normalize_working_directory(args)

    assert Path.cwd() == tmp_path.resolve()
    assert args.workflow == str(nested / "flow.py")
    assert args.config == [str(nested / "a.toml"), str(nested / "b.toml")]
    assert args.out == str(nested / "report-dir")
