"""Tests for ``ginkgo doctor``."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

from ginkgo.cli.commands.doctor import _extract_executor_config

REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"


def _run_doctor(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", "doctor", "workflow.py", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


def _write_workflow(*, env: str | None) -> None:
    """Write a single-shell-task workflow, optionally declaring an ``env``."""
    decorator = '@task("shell")' if env is None else f'@task("shell", env="{env}")'
    Path("workflow.py").write_text(
        f"""
from ginkgo import flow, task

{decorator}
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


class TestDoctorJsonShape:
    def test_json_success_is_an_object_reporting_ok(self) -> None:
        _write_workflow(env=None)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload == {"ok": True, "diagnostics": []}

    def test_json_failure_reports_not_ok_with_diagnostics(self, monkeypatch) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, secret, task

@task()
def echo_token(token: str) -> str:
    return token

@flow
def main():
    return echo_token(token=secret("MISSING_TOKEN"))
""".strip()
            + "\n",
            encoding="utf-8",
        )
        monkeypatch.delenv("MISSING_TOKEN", raising=False)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert payload["diagnostics"][0]["code"] == "MISSING_SECRET"


class TestDoctorUnreachableCalls:
    """A task call the flow never returns is reported, but is not an error."""

    def _write_workflow_with_dropped_call(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task("shell")
def greet(text: str) -> str:
    return f"echo {text}"

@flow
def main():
    greet(text="dropped")
    return greet(text="kept")
""".strip()
            + "\n",
            encoding="utf-8",
        )

    def test_dropped_call_is_a_warning_not_a_failure(self) -> None:
        self._write_workflow_with_dropped_call()

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "unreachable_task_call" in result.stdout
        assert "greet()" in result.stdout

    def test_dropped_call_is_reported_in_json_as_ok(self) -> None:
        self._write_workflow_with_dropped_call()

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["ok"] is True
        assert payload["diagnostics"][0]["severity"] == "warning"
        assert payload["diagnostics"][0]["code"] == "unreachable_task_call"


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


class TestDoctorExecutorSelection:
    """Which executor's settings the FUSE probes are diagnosed against."""

    def test_named_executor_with_fuse_image_is_preferred(self) -> None:
        config = {
            "remote": {
                "k8s": {"image": "plain"},
                "executors": {
                    "cheap-batch": {"type": "batch", "image": "plain"},
                    "stream-k8s": {"type": "k8s", "fuse_image": "fuse-worker"},
                },
            }
        }
        assert _extract_executor_config(config=config) == {
            "type": "k8s",
            "fuse_image": "fuse-worker",
        }

    def test_first_section_is_used_when_none_streams(self) -> None:
        config = {"remote": {"executors": {"a": {"type": "k8s", "image": "one"}}}}
        assert _extract_executor_config(config=config) == {"type": "k8s", "image": "one"}

    def test_no_remote_config_yields_nothing(self) -> None:
        assert _extract_executor_config(config={}) is None
