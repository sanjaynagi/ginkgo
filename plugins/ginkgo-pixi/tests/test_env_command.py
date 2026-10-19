"""``ginkgo env``, contributed by ginkgo-pixi."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from ginkgo.runtime.task_runners.shell import classify_failure
from ginkgo_pixi.environment import PixiEnvNotFoundError


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each test lays out envs/ in the working directory.
    monkeypatch.chdir(tmp_path)


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


def test_env_without_a_subcommand_shows_its_help() -> None:
    result = _run_cli("env", cwd=Path.cwd())

    assert result.returncode == 2
    assert "{ls,clear}" in result.stderr


def test_a_missing_env_classifies_as_an_environment_mismatch() -> None:
    failure = classify_failure(exc=PixiEnvNotFoundError(env="analysis", searched=None))

    assert failure["kind"] == "env_mismatch"


class TestCliEnv:
    def test_env_ls_empty_state_is_styled(self) -> None:
        result = _run_cli("env", "ls", cwd=Path.cwd())
        assert result.returncode == 0
        assert "🌿 ginkgo env ls" in result.stdout
        assert "No Pixi environments found. Searched:" in result.stdout

    def test_env_ls_and_clear_manage_project_local_pixi_installs(self) -> None:
        analysis_dir = Path("envs") / "analysis_tools"
        analysis_dir.mkdir(parents=True)
        (analysis_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'analysis-tools'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        analysis_install = analysis_dir / ".pixi" / "envs" / "default"
        analysis_install.mkdir(parents=True)
        (analysis_install / "marker.txt").write_text("installed", encoding="utf-8")

        bioinfo_dir = Path("envs") / "bioinfo_tools"
        bioinfo_dir.mkdir(parents=True)
        (bioinfo_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'bioinfo-tools'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        bioinfo_install = bioinfo_dir / ".pixi" / "envs" / "default"
        bioinfo_install.mkdir(parents=True)
        (bioinfo_install / "marker.txt").write_text("installed", encoding="utf-8")

        listed = _run_cli("env", "ls", cwd=Path.cwd())
        assert listed.returncode == 0, listed.stderr
        assert "analysis_tools" in listed.stdout
        assert "bioinfo_tools" in listed.stdout
        assert "yes" in listed.stdout

        preview = _run_cli("env", "clear", "--all", "--dry-run", cwd=Path.cwd())
        assert preview.returncode == 0, preview.stderr
        assert "would be removed" in preview.stdout
        assert analysis_dir.joinpath(".pixi").exists()
        assert bioinfo_dir.joinpath(".pixi").exists()

        cleared_one = _run_cli("env", "clear", "analysis_tools", cwd=Path.cwd())
        assert cleared_one.returncode == 0, cleared_one.stderr
        assert "🌿 ginkgo env clear" in cleared_one.stdout
        assert "✓ Removed 1 Pixi env" in cleared_one.stdout
        assert not analysis_dir.joinpath(".pixi").exists()
        assert bioinfo_dir.joinpath(".pixi").exists()

        cleared_all = _run_cli("env", "clear", "--all", cwd=Path.cwd())
        assert cleared_all.returncode == 0, cleared_all.stderr
        assert "✓ Removed 1 Pixi env" in cleared_all.stdout
        assert not bioinfo_dir.joinpath(".pixi").exists()

    def test_env_clear_requires_exactly_one_target(self) -> None:
        result = _run_cli("env", "clear", cwd=Path.cwd())
        assert result.returncode == 1
        assert "Specify exactly one of <env> or --all." in result.stderr


def test_bare_env_names_are_labelled_with_pixi() -> None:
    from ginkgo.cli.renderers.common import environment_label

    assert environment_label("bioinfo_tools") == "pixi:bioinfo_tools"
