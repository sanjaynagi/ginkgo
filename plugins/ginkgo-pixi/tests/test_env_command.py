"""``ginkgo env``, contributed by ginkgo-pixi."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.renderers.common import environment_label
from ginkgo.runtime.task_runners.shell import classify_failure
from ginkgo_pixi.environment import PixiEnvNotFoundError
from tests.conftest import run_cli


@pytest.fixture(autouse=True)
def _run_in_tmp_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Each test lays out envs/ in the working directory.
    monkeypatch.chdir(tmp_path)


def _env(envs_root: Path, name: str, *, installed: bool = False) -> Path:
    """Write ``envs_root/<name>/pixi.toml``, with a fake install beside it if asked."""
    env_dir = envs_root / name
    env_dir.mkdir(parents=True)
    (env_dir / "pixi.toml").write_text(
        f"[workspace]\nname = '{name}'\nchannels = []\nplatforms = []\n", encoding="utf-8"
    )
    if installed:
        (env_dir / ".pixi" / "envs" / "default").mkdir(parents=True)
    return env_dir


def _canonical_project(root: Path) -> Path:
    """Create a ``ginkgo init`` style layout with one env under ``workflow/envs``."""
    (root / "workflow").mkdir()
    (root / "workflow" / "__init__.py").write_text("")
    (root / "workflow" / "flow.py").write_text("")
    (root / "ginkgo.toml").write_text("")
    return _env(root / "workflow" / "envs", "analysis_tools", installed=True)


@pytest.mark.parametrize(
    ("argv", "message"),
    [(("env",), "{ls,clear}"), (("env", "clear"), "Specify exactly one of <env> or --all.")],
)
def test_env_usage_errors(argv: tuple[str, ...], message: str) -> None:
    result = run_cli(*argv)

    assert result.returncode == 2
    assert message in result.stderr


def test_a_missing_env_classifies_as_an_environment_mismatch() -> None:
    failure = classify_failure(exc=PixiEnvNotFoundError(env="analysis", searched=None))

    assert failure["kind"] == "env_mismatch"


def test_bare_env_names_are_labelled_with_pixi() -> None:
    assert environment_label("bioinfo_tools") == "pixi:bioinfo_tools"


def test_env_commands_degrade_without_a_resolvable_workflow() -> None:
    listed = run_cli("env", "ls")
    assert listed.returncode == 0
    assert "🌿 ginkgo env ls" in listed.stdout
    assert "No Pixi environments found. Searched:" in listed.stdout.replace("\n", "")

    cleared = run_cli("env", "clear", "missing_env", "--dry-run")
    assert cleared.returncode == 1
    assert "'missing_env' not found" in cleared.stderr.replace("\n", "")


def test_env_ls_and_clear_manage_project_local_pixi_installs() -> None:
    analysis = _env(Path("envs"), "analysis_tools", installed=True)
    bioinfo = _env(Path("envs"), "bioinfo_tools", installed=True)

    listed = run_cli("env", "ls")
    assert listed.returncode == 0, listed.stderr
    assert "analysis_tools" in listed.stdout
    assert "bioinfo_tools" in listed.stdout
    assert "yes" in listed.stdout

    preview = run_cli("env", "clear", "--all", "--dry-run")
    assert preview.returncode == 0, preview.stderr
    assert "would be removed" in preview.stdout
    assert (analysis / ".pixi").exists() and (bioinfo / ".pixi").exists()

    cleared_one = run_cli("env", "clear", "analysis_tools")
    assert cleared_one.returncode == 0, cleared_one.stderr
    assert "✓ Removed 1 Pixi env" in cleared_one.stdout
    assert not (analysis / ".pixi").exists()
    assert (bioinfo / ".pixi").exists()

    cleared_all = run_cli("env", "clear", "--all")
    assert cleared_all.returncode == 0, cleared_all.stderr
    assert "✓ Removed 1 Pixi env" in cleared_all.stdout
    assert not (bioinfo / ".pixi").exists()


class TestCanonicalLayout:
    def test_env_ls_finds_the_env_and_names_its_scope(self, tmp_path: Path) -> None:
        _canonical_project(tmp_path)

        result = run_cli("env", "ls")

        assert result.returncode == 0
        assert "analysis_tools" in result.stdout
        # A non-empty listing still names its own scope, so it is not read as complete.
        out = result.stdout.replace("\n", " ")
        assert "Container environments are managed by the container runtime" in out

    def test_env_clear_dry_run_finds_the_env(self, tmp_path: Path) -> None:
        env_dir = _canonical_project(tmp_path)

        result = run_cli("env", "clear", "analysis_tools", "--dry-run")

        assert result.returncode == 0
        assert "1 Pixi env would be removed" in result.stdout
        assert (env_dir / ".pixi").exists()

    def test_env_clear_absent_env_reports_every_root_searched(self, tmp_path: Path) -> None:
        _canonical_project(tmp_path)

        result = run_cli("env", "clear", "missing_env", "--dry-run")

        assert result.returncode == 1
        # The message names both discovery roots and the envs each holds, so a
        # genuinely absent env is distinguishable from looking in the wrong place.
        err = result.stderr.replace("\n", "")
        assert "'missing_env' not found" in err
        assert "workflow/envs (envs: ['analysis_tools'])" in err
