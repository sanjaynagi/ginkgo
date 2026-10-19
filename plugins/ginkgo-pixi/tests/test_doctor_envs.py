"""``ginkgo doctor`` and ``--dry-run`` with Pixi environments from ginkgo-pixi."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

PYTHON = Path(sys.executable)


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each test writes a workflow and envs/ into the working directory.
    monkeypatch.chdir(tmp_path)


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


def _run_doctor(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return _run_cli("doctor", "workflow.py", *args, cwd=cwd)


def _write_workflow(*, env: str) -> None:
    """Write a single-shell-task workflow declaring *env*."""
    Path("workflow.py").write_text(
        f"""
from ginkgo import flow, task

@task("shell", env="{env}")
def greet() -> str:
    return "echo hello"

@flow
def main():
    return greet()
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_env(*, name: str) -> Path:
    """Create a minimal pixi manifest under ``envs/<name>/``."""
    env_dir = Path("envs") / name
    env_dir.mkdir(parents=True)
    (env_dir / "pixi.toml").write_text(
        '[project]\nname = "probe"\nchannels = ["conda-forge"]\nplatforms = ["osx-arm64"]\n',
        encoding="utf-8",
    )
    return env_dir


class TestDoctorEnvValidation:
    def test_nonexistent_env_produces_a_diagnostic(self) -> None:
        _write_workflow(env="not_a_real_env")

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 1
        assert "MISSING_ENV" in result.stderr
        assert "not_a_real_env" in result.stderr

    def test_nonexistent_env_is_reported_in_json(self) -> None:
        _write_workflow(env="not_a_real_env")

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert payload["diagnostics"][0]["code"] == "MISSING_ENV"

    def test_existing_env_passes(self) -> None:
        _write_env(name="probe_env")
        _write_workflow(env="probe_env")

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "Workflow validation passed" in result.stdout

    def test_doctor_does_not_build_the_environment(self) -> None:
        env_dir = _write_env(name="probe_env")
        _write_workflow(env="probe_env")

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert not (env_dir / ".pixi").exists()
        assert not (env_dir / "pixi.lock").exists()
        assert sorted(path.name for path in env_dir.iterdir()) == ["pixi.toml"]


class TestDoctorEnvRootMatchesRun:
    def test_envs_resolve_from_the_canonical_package_not_the_checked_file(self) -> None:
        """``run`` anchors Pixi discovery on the canonical package; doctor must agree.

        Checking a workflow that lives outside that package must still find the
        project's environments, or doctor validates a different env set than
        the run it is meant to vet.
        """
        package_dir = Path("workflow")
        (package_dir / "envs" / "probe_env").mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "flow.py").write_text("", encoding="utf-8")
        (package_dir / "envs" / "probe_env" / "pixi.toml").write_text(
            '[project]\nname = "probe"\nchannels = ["conda-forge"]\nplatforms = ["osx-arm64"]\n',
            encoding="utf-8",
        )

        alt_dir = Path("experiments")
        alt_dir.mkdir()
        (alt_dir / "alt_flow.py").write_text(
            """
from ginkgo import flow, task

@task("shell", env="probe_env")
def greet() -> str:
    return "echo hello"

@flow
def main():
    return greet()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = subprocess.run(
            [str(PYTHON), "-m", "ginkgo.cli", "doctor", "experiments/alt_flow.py"],
            cwd=Path.cwd(),
            check=False,
            text=True,
            capture_output=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Workflow validation passed" in result.stdout

    def test_envs_still_resolve_when_the_canonical_package_is_ambiguous(self) -> None:
        """Regression test for issue #309.

        The documented, ``ginkgo init``-scaffolded layout -- ``workflow/envs/``
        beside ``workflow/flow.py`` -- must resolve its envs even when a
        second candidate entry file elsewhere makes canonical-package
        discovery ambiguous. Checking the workflow by its own explicit,
        unambiguous path must not lose the envs directory that sits right
        beside it.
        """
        package_dir = Path("workflow")
        (package_dir / "envs" / "probe_env").mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "envs" / "probe_env" / "pixi.toml").write_text(
            '[project]\nname = "probe"\nchannels = ["conda-forge"]\nplatforms = ["osx-arm64"]\n',
            encoding="utf-8",
        )
        (package_dir / "flow.py").write_text(
            """
from ginkgo import flow, task

@task("shell", env="probe_env")
def greet() -> str:
    return "echo hello"

@flow
def main():
    return greet()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        # A second top-level entry file makes canonical discovery ambiguous.
        other_dir = Path("other")
        other_dir.mkdir()
        (other_dir / "flow.py").write_text("", encoding="utf-8")

        result = subprocess.run(
            [str(PYTHON), "-m", "ginkgo.cli", "doctor", "workflow/flow.py"],
            cwd=Path.cwd(),
            check=False,
            text=True,
            capture_output=True,
        )

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Workflow validation passed" in result.stdout


class TestDryRun:
    def test_envs_resolve_from_the_canonical_package_not_the_workflow_parent(self) -> None:
        """Regression test for issue #119.

        On a canonical ``ginkgo init`` layout, Pixi environments live under
        ``workflow/envs/``, a sibling of ``workflow/flow.py`` -- not of
        ``tests/workflows/*.py``. Running a validation workflow by path must
        still resolve them.
        """
        package_dir = Path("workflow")
        (package_dir / "envs" / "analysis_tools").mkdir(parents=True)
        (package_dir / "__init__.py").write_text("")
        (package_dir / "flow.py").write_text("")
        (package_dir / "envs" / "analysis_tools" / "pixi.toml").write_text(
            "[workspace]\nname = 'analysis_tools'\nchannels = []\nplatforms = []\n"
        )

        tests_workflows_dir = Path("tests") / "workflows"
        tests_workflows_dir.mkdir(parents=True)
        (tests_workflows_dir / "smoke.py").write_text(
            """
from ginkgo import flow, shell, task

@task(env="analysis_tools", kind="shell")
def touch() -> str:
    return shell(cmd="true", output="marker.txt")

@flow
def main():
    return touch()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "tests/workflows/smoke.py", "--dry-run", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "Pixi environment 'analysis_tools' not found" not in result.stderr
