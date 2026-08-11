"""Tests for ``ginkgo env`` discovery on the canonical project layout."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo.cli.app import main


def _canonical_project(root: Path, *, env: str = "analysis_tools") -> Path:
    """Create a canonical ``ginkgo init`` style layout with one Pixi env."""
    package = root / "pkg"
    (package / "envs" / env).mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "workflow.py").write_text("")
    (package / "envs" / env / "pixi.toml").write_text('[project]\nname = "env"\n')
    (root / "ginkgo.toml").write_text("")
    return package / "envs" / env / "pixi.toml"


def test_env_ls_finds_env_in_canonical_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _canonical_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    assert main(["env", "ls"]) == 0
    assert "analysis_tools" in capsys.readouterr().out


def test_env_clear_finds_env_in_canonical_layout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = _canonical_project(tmp_path)
    install_dir = manifest.parent / ".pixi"
    install_dir.mkdir()
    monkeypatch.chdir(tmp_path)

    assert main(["env", "clear", "analysis_tools", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "1 Pixi env would be removed" in out
    assert install_dir.exists()


def test_env_clear_absent_env_reports_every_root_searched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _canonical_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    assert main(["env", "clear", "missing_env", "--dry-run"]) == 1
    # The message names both discovery roots and the envs each holds, so a
    # genuinely absent env is distinguishable from looking in the wrong place.
    err = capsys.readouterr().err.replace("\n", "")
    assert "'missing_env' not found" in err
    assert "pkg/envs (envs: ['analysis_tools'])" in err


def test_env_commands_degrade_without_a_resolvable_workflow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.chdir(tmp_path)

    assert main(["env", "ls"]) == 0
    assert "No Pixi environments found. Searched:" in capsys.readouterr().out.replace("\n", "")

    assert main(["env", "clear", "missing_env", "--dry-run"]) == 1
    assert "'missing_env' not found" in capsys.readouterr().err.replace("\n", "")
