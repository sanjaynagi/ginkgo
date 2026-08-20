"""Tests for project-root normalisation of the CLI working directory.

Ginkgo's runtime reads the working directory as the project root in dozens of
places. The CLI makes that true by moving to the discovered root before it
dispatches, so the tests that matter are the ones showing a command run from a
subdirectory lands its state, its config, and its outputs where a command run
from the root would.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess

from ginkgo.cli.app import _normalize_working_directory

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"

WORKFLOW_SOURCE = (
    """
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


def _project(tmp_path: Path, *, message: str = "default") -> Path:
    """Write a minimal runnable project with a `workflow/` subdirectory."""
    (tmp_path / "ginkgo.toml").write_text(f'message = "{message}"\n', encoding="utf-8")
    workflow_dir = tmp_path / "workflow"
    workflow_dir.mkdir()
    (workflow_dir / "flow.py").write_text(WORKFLOW_SOURCE, encoding="utf-8")
    return workflow_dir


class TestRunFromASubdirectory:
    """A run invoked below the root behaves as one invoked at the root."""

    def test_workspace_state_lands_at_the_project_root(self, tmp_path: Path):
        workflow_dir = _project(tmp_path)

        result = _run_cli("run", "flow.py", cwd=workflow_dir)

        assert result.returncode == 0, result.stderr
        assert (tmp_path / ".ginkgo").is_dir()
        assert not (workflow_dir / ".ginkgo").exists()

    def test_relative_task_outputs_land_at_the_project_root(self, tmp_path: Path):
        workflow_dir = _project(tmp_path)

        result = _run_cli("run", "flow.py", cwd=workflow_dir)

        assert result.returncode == 0, result.stderr
        # The task wrote "results/out.txt"; the root is what that is relative to.
        assert (tmp_path / "results" / "out.txt").read_text(encoding="utf-8") == "default"
        assert not (workflow_dir / "results").exists()

    def test_config_from_the_root_is_found(self, tmp_path: Path):
        # ginkgo.config("ginkgo.toml") is a root-relative path inside the
        # workflow, so importing the module at all proves the root was reached.
        workflow_dir = _project(tmp_path, message="from-root")

        result = _run_cli("run", "flow.py", cwd=workflow_dir)

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "results" / "out.txt").read_text(encoding="utf-8") == "from-root"

    def test_relative_config_override_resolves_against_the_invocation_directory(
        self, tmp_path: Path
    ):
        workflow_dir = _project(tmp_path)
        (workflow_dir / "override.toml").write_text('message = "overridden"\n', encoding="utf-8")

        result = _run_cli("run", "flow.py", "--config", "override.toml", cwd=workflow_dir)

        assert result.returncode == 0, result.stderr
        assert (tmp_path / "results" / "out.txt").read_text(encoding="utf-8") == "overridden"

    def test_inspect_reads_the_run_written_from_a_subdirectory(self, tmp_path: Path):
        workflow_dir = _project(tmp_path)
        assert _run_cli("run", "flow.py", cwd=workflow_dir).returncode == 0

        # Written from the subdirectory, read back from the subdirectory: the
        # CLI's run paths are relative to the working directory too.
        listed = _run_cli("cache", "ls", cwd=workflow_dir)

        assert listed.returncode == 0, listed.stderr
        assert "write_message" in listed.stdout


class TestInitIsExempt:
    """`ginkgo init` creates a project rather than running inside one."""

    def test_init_scaffolds_relative_to_the_invocation_directory(self, tmp_path: Path):
        workflow_dir = _project(tmp_path)

        result = _run_cli("init", "nested", "--no-skills", cwd=workflow_dir)

        assert result.returncode == 0, result.stderr
        assert (workflow_dir / "nested" / "ginkgo.toml").is_file()
        assert not (tmp_path / "nested").exists()


class TestNormalizeWorkingDirectory:
    """The normalisation step itself."""

    def test_does_not_move_when_already_at_the_project_root(self, tmp_path, monkeypatch):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        _normalize_working_directory(argparse.Namespace(command="run", workflow=None, config=[]))

        assert Path.cwd() == tmp_path.resolve()

    def test_does_not_move_when_there_is_no_project(self, tmp_path, monkeypatch):
        nested = tmp_path / "elsewhere"
        nested.mkdir()
        monkeypatch.chdir(nested)

        _normalize_working_directory(argparse.Namespace(command="run", workflow=None, config=[]))

        assert Path.cwd() == nested.resolve()

    def test_absolutises_path_arguments_before_moving(self, tmp_path, monkeypatch):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        nested = tmp_path / "workflow"
        nested.mkdir()
        monkeypatch.chdir(nested)
        args = argparse.Namespace(
            command="run", workflow="flow.py", config=["a.toml", "b.toml"], out="report-dir"
        )

        _normalize_working_directory(args)

        assert Path.cwd() == tmp_path.resolve()
        assert args.workflow == str(nested.resolve() / "flow.py")
        assert args.config == [
            str(nested.resolve() / "a.toml"),
            str(nested.resolve() / "b.toml"),
        ]
        assert args.out == str(nested.resolve() / "report-dir")

    def test_leaves_init_where_it_was_invoked(self, tmp_path, monkeypatch):
        (tmp_path / "ginkgo.toml").write_text("", encoding="utf-8")
        nested = tmp_path / "workflow"
        nested.mkdir()
        monkeypatch.chdir(nested)

        _normalize_working_directory(argparse.Namespace(command="init", directory="nested"))

        assert Path.cwd() == nested.resolve()
