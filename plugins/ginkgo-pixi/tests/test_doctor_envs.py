"""``ginkgo doctor`` and ``--dry-run`` with Pixi environments from ginkgo-pixi."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.conftest import run_cli

_WORKFLOW = """
from ginkgo import flow, shell, task

@task("shell", env="{env}")
def greet() -> str:
    return shell(cmd="true", output="marker.txt")

@flow
def main():
    return greet()
"""


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each test writes a workflow and envs/ into the working directory.
    monkeypatch.chdir(tmp_path)


def _write(path: str, *, env: str = "probe_env") -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(_WORKFLOW.format(env=env), encoding="utf-8")


def _write_env(env_dir: Path) -> Path:
    """Create a minimal pixi manifest in *env_dir*."""
    env_dir.mkdir(parents=True)
    (env_dir / "pixi.toml").write_text(
        "[workspace]\nname = 'probe'\nchannels = []\nplatforms = []\n", encoding="utf-8"
    )
    return env_dir


def _canonical_package() -> None:
    """A ``ginkgo init`` package: ``workflow/`` with ``envs/probe_env`` beside its flow."""
    _write_env(Path("workflow") / "envs" / "probe_env")
    Path("workflow", "__init__.py").write_text("", encoding="utf-8")
    Path("workflow", "flow.py").write_text("", encoding="utf-8")


class TestDoctorEnvValidation:
    def test_nonexistent_env_produces_a_diagnostic(self) -> None:
        _write("workflow.py", env="not_a_real_env")

        text = run_cli("doctor", "workflow.py")
        assert text.returncode == 1
        assert "missing_env" in text.stderr
        assert "not_a_real_env" in text.stderr

        as_json = run_cli("doctor", "workflow.py", "--json")
        assert as_json.returncode == 1
        payload = json.loads(as_json.stdout)
        assert payload["ok"] is False
        assert payload["diagnostics"][0]["code"] == "missing_env"

    def test_existing_env_passes_without_being_built(self) -> None:
        env_dir = _write_env(Path("envs") / "probe_env")
        _write("workflow.py")

        result = run_cli("doctor", "workflow.py")

        assert result.returncode == 0, result.stderr
        assert "Workflow validation passed" in result.stdout
        assert sorted(path.name for path in env_dir.iterdir()) == ["pixi.toml"]


class TestEnvRootMatchesRun:
    """``run`` anchors Pixi discovery on the canonical package; doctor must agree."""

    def test_a_workflow_outside_the_package_finds_the_project_envs(self) -> None:
        _canonical_package()
        _write("experiments/alt_flow.py")

        result = run_cli("doctor", "experiments/alt_flow.py")

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Workflow validation passed" in result.stdout

    def test_envs_still_resolve_when_the_canonical_package_is_ambiguous(self) -> None:
        """Regression test for issue #309.

        A second candidate entry file elsewhere makes canonical-package discovery
        ambiguous; checking ``workflow/flow.py`` by its own path must still find
        the envs directory right beside it.
        """
        _canonical_package()
        _write("workflow/flow.py")
        _write("other/flow.py")

        result = run_cli("doctor", "workflow/flow.py")

        assert result.returncode == 0, result.stderr + result.stdout
        assert "Workflow validation passed" in result.stdout

    def test_a_dry_run_resolves_envs_from_the_package_not_the_workflow_parent(self) -> None:
        """Regression test for issue #119: ``tests/workflows/*.py`` must find ``workflow/envs``."""
        _canonical_package()
        _write("tests/workflows/smoke.py")

        result = run_cli("run", "tests/workflows/smoke.py", "--dry-run")

        assert result.returncode == 0, result.stderr
        assert "Pixi environment 'probe_env' not found" not in result.stderr
