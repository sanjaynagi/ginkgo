"""Tests for ``ginkgo doctor``."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from ginkgo.cli.commands.doctor import _extract_executor_configs
from ginkgo.remote.access import doctor as access_doctor

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
    """Which executor settings the FUSE probes are diagnosed against."""

    def test_every_executor_section_is_collected(self) -> None:
        config = {
            "remote": {
                "k8s": {"image": "plain"},
                "executors": {
                    "cheap-batch": {"type": "batch", "image": "plain"},
                    "stream-k8s": {"type": "k8s", "fuse_image": "fuse-worker"},
                },
            }
        }
        assert _extract_executor_configs(config=config) == {
            "[remote.k8s]": {"image": "plain"},
            "[remote.executors.cheap-batch]": {"type": "batch", "image": "plain"},
            "[remote.executors.stream-k8s]": {"type": "k8s", "fuse_image": "fuse-worker"},
        }

    def test_named_executors_are_keyed_by_section(self) -> None:
        config = {"remote": {"executors": {"a": {"type": "k8s", "image": "one"}}}}
        assert _extract_executor_configs(config=config) == {
            "[remote.executors.a]": {"type": "k8s", "image": "one"}
        }

    def test_no_remote_config_yields_nothing(self) -> None:
        assert _extract_executor_configs(config={}) == {}

    def test_executor_without_fuse_image_is_reported_despite_a_streaming_peer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One well-configured executor must not clear the others."""
        (tmp_path / "ginkgo.toml").write_text(
            '[remote.access]\ndefault = "fuse"\n',
            encoding="utf-8",
        )
        monkeypatch.setattr(access_doctor, "_probe_driver_binaries", lambda: [])

        diagnostics = access_doctor.collect_access_diagnostics(
            project_root=tmp_path,
            executor_configs={
                "[remote.executors.stream-k8s]": {"fuse_image": "fuse-worker"},
                "[remote.executors.gpu-k8s]": {"image": "plain"},
            },
        )

        missing = [item for item in diagnostics if item.code == "FUSE_IMAGE_NOT_CONFIGURED"]
        assert len(missing) == 1
        assert "[remote.executors.gpu-k8s]" in missing[0].message


class TestDoctorInterpreterEnvironment:
    """Whether the interpreter running ginkgo can import what the project declares.

    Cover for issue #221: Python and notebook task bodies execute in the CLI's
    own interpreter and cannot declare ``env=``, so the project manifest is the
    environment they need. Doctor is where that mismatch is cheap to see.
    """

    def _manifest(self) -> None:
        """Write a project manifest declaring the scaffold's ``run`` task."""
        Path("pixi.toml").write_text(
            '[workspace]\nname = "demo"\n\n[dependencies]\npython = ">=3.11"\n\n'
            '[tasks]\nrun = "ginkgo run"\n',
            encoding="utf-8",
        )

    def test_a_missing_import_is_reported_against_the_manifest(self) -> None:
        self._manifest()
        _write_workflow(env=None)
        Path("analysis.py").write_text("import totally_absent_lib\n", encoding="utf-8")

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 1
        combined = result.stdout + result.stderr
        assert "interpreter_env_mismatch" in combined
        assert "cannot import: totally_absent_lib" in combined
        assert "pixi.toml" in combined
        assert "Try: pixi run run" in combined

    def test_json_reports_the_mismatch_as_a_diagnostic(self) -> None:
        self._manifest()
        _write_workflow(env=None)
        Path("analysis.py").write_text("import totally_absent_lib\n", encoding="utf-8")

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert [item["code"] for item in payload["diagnostics"]] == ["interpreter_env_mismatch"]

    def test_an_interpreter_that_imports_everything_stays_quiet(self) -> None:
        """The ``pixi run`` case, where the interpreter is the declared environment."""
        self._manifest()
        _write_workflow(env=None)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"ok": True, "diagnostics": []}
