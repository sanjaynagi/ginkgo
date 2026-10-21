"""Tests for ``ginkgo doctor``."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from ginkgo.cli.commands.doctor import _extract_executor_configs
from ginkgo.plugins.api import MountDriverType, Plugin
from ginkgo.plugins.builtin import BUILTIN
from ginkgo.plugins.registry import PluginRegistry, use_plugin_registry
from ginkgo.remote.access import doctor as access_doctor
from ginkgo.remote.access.drivers.base import DriverUnavailableError

PYTHON = Path(sys.executable)


def _run_doctor(
    *args: str, cwd: Path, path_prefix: Path | None = None
) -> subprocess.CompletedProcess[str]:
    env = None
    if path_prefix is not None:
        env = {**os.environ, "PATH": f"{path_prefix}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", "doctor", "workflow.py", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )


def _write_fake_docker(*, daemon_up: bool) -> Path:
    """Write a ``docker`` stub that has no local images and logs each call.

    Returns the directory to put first on ``PATH``.
    """
    bin_dir = Path("fake-bin").resolve()
    bin_dir.mkdir()
    info = (
        "exit 0"
        if daemon_up
        else "echo 'failed to connect to the docker API at unix:///fake/docker.sock' >&2; exit 1"
    )
    stub = bin_dir / "docker"
    stub.write_text(
        f"""#!/bin/sh
echo "$@" >> "{bin_dir / "calls.log"}"
case "$1" in
  info) {info} ;;
  *) exit 1 ;;
esac
""",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return bin_dir


def _write_workflow(*, env: str | None) -> None:
    """Write a single-shell-task workflow, optionally declaring an ``env``."""
    decorator = '@task("shell")' if env is None else f'@task("shell", env="{env}")'
    Path("workflow.py").write_text(
        f"""
from ginkgo import flow, shell, task

{decorator}
def greet() -> str:
    return shell(cmd="echo hello")

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
        assert payload["diagnostics"][0]["code"] == "missing_secret"


class TestDoctorUnreachableCalls:
    """A task call the flow never returns is reported, but is not an error."""

    def _write_workflow_with_dropped_call(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, shell, task

@task("shell")
def greet(text: str) -> str:
    return shell(cmd=f"echo {text}")

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

    def test_dropped_producer_accompanies_its_consumers_validation_error(self) -> None:
        """#341: the warning explains why the consumer's `file` input does not exist."""
        Path("workflow.py").write_text(
            """
from pathlib import Path
from ginkgo import Out, file, flow, task

@task()
def produce(out: Out[file]) -> None:
    Path(out).write_text("x")

@task()
def consume(p: file) -> str:
    return Path(p).read_text()

@flow
def main():
    produce(out="p.txt")
    return consume(p="p.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert [entry["severity"] for entry in payload["diagnostics"]] == ["warning", "error"]
        assert payload["diagnostics"][0]["code"] == "unreachable_task_call"
        assert "produce()" in payload["diagnostics"][0]["message"]
        assert "must exist and be a file" in payload["diagnostics"][1]["message"]


class TestDoctorPartialCalls:
    """#332: a call missing a required argument is reported, not passed."""

    def test_consumed_partial_call_is_an_error(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def join(left: str, right: str) -> str:
    return left + right

@task()
def shout(text: str) -> str:
    return text.upper()

@flow
def main():
    return shout(text=join(left="a"))
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        [diagnostic] = payload["diagnostics"]
        assert diagnostic["code"] == "incomplete_call"
        assert "join() is missing required argument(s): right" in diagnostic["message"]
        assert ".map()" in diagnostic["suggestion"]


class TestDoctorPathLikeStrParam:
    """Cover for issue #307: a ``str`` path parameter is a warning, not a failure."""

    def _write_workflow_with_path_like_str_param(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def write_report(output_path: str) -> str:
    return output_path

@flow
def main():
    return write_report(output_path="report.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

    def test_path_like_str_param_is_a_warning_not_a_failure(self) -> None:
        self._write_workflow_with_path_like_str_param()

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "path_like_str_param" in result.stdout
        assert "output_path" in result.stdout

    def test_path_like_str_param_is_reported_in_json_as_ok(self) -> None:
        self._write_workflow_with_path_like_str_param()

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["ok"] is True
        assert payload["diagnostics"][0]["severity"] == "warning"
        assert payload["diagnostics"][0]["code"] == "path_like_str_param"


class TestDoctorWorkspaceLedger:
    """The workspace doctor runs in is reported on, not only the workflow."""

    def test_run_directories_without_a_ledger_are_reported(self) -> None:
        """Doctor names the mismatch every read surface otherwise hides (issue #282)."""
        _write_workflow(env=None)
        (Path(".ginkgo") / "runs" / "20260101_000000_000000_deadbeef").mkdir(parents=True)

        result = _run_doctor(cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "runs_without_ledger" in result.stdout
        assert "1 run directory under .ginkgo/runs but no database" in result.stdout

    def test_the_missing_ledger_is_a_warning_in_json_not_a_failure(self) -> None:
        _write_workflow(env=None)
        (Path(".ginkgo") / "runs" / "20260101_000000_000000_deadbeef").mkdir(parents=True)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["ok"] is True
        assert [item["code"] for item in payload["diagnostics"]] == ["runs_without_ledger"]
        assert payload["diagnostics"][0]["severity"] == "warning"

    def _run_once(self) -> None:
        Path("workflow.py").write_text(
            "from ginkgo import flow, task\n\n@task()\ndef one() -> int:\n    return 1\n\n"
            "@flow\ndef main():\n    return one()\n",
            encoding="utf-8",
        )
        result = subprocess.run(
            [str(PYTHON), "-m", "ginkgo.cli", "run", "workflow.py"],
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def test_run_directories_older_than_a_new_ledger_are_reported(self) -> None:
        """ED-6: a database recreated after a deletion hides the older runs too."""
        self._run_once()
        (Path(".ginkgo") / "runs" / "20260101_000000_000000_deadbeef").mkdir()

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        [diagnostic] = json.loads(result.stdout)["diagnostics"]
        assert diagnostic["code"] == "runs_without_ledger"
        assert "1 run directory under .ginkgo/runs with no row" in diagnostic["message"]

    def test_recorded_runs_without_directories_are_reported(self) -> None:
        """ED-6: deleting .ginkgo/runs fails db check, so doctor must not pass silently."""
        self._run_once()
        shutil.rmtree(Path(".ginkgo") / "runs")

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        [diagnostic] = json.loads(result.stdout)["diagnostics"]
        assert diagnostic["code"] == "runs_without_directory"
        assert "1 run recorded" in diagnostic["message"]

    def test_a_workspace_that_has_not_run_yet_is_not_reported(self) -> None:
        _write_workflow(env=None)
        (Path(".ginkgo") / "runs").mkdir(parents=True)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"ok": True, "diagnostics": []}


class TestDoctorEnvValidation:
    def test_env_on_a_kind_without_env_support_is_invalid_env_kind(self) -> None:
        Path("workflow.py").write_text(
            "from ginkgo import flow, task\n\n"
            '@task(kind="subworkflow", env="analysis")\n'
            "def child() -> str:\n    return 'x'\n\n"
            "@flow\ndef main():\n    return child()\n",
            encoding="utf-8",
        )

        result = _run_doctor("--json", cwd=Path.cwd())

        payload = json.loads(result.stdout)
        assert [item["code"] for item in payload["diagnostics"]] == ["invalid_env_kind"]
        assert "kind='subworkflow'" in payload["diagnostics"][0]["message"]


class TestDoctorContainerImageValidation:
    """Cover for issue #288: a container image that cannot possibly run is
    caught by ``doctor`` rather than surfacing mid-run.

    ``pull_policy = "never"`` and a stub ``docker`` with a running daemon and
    no local images keep this deterministic and network-free: the image is
    certain to be absent and no pull/registry attempt will ever be made, so
    the diagnostic follows from the local check alone.
    """

    def _write_never_pull_config(self) -> None:
        Path("ginkgo.toml").write_text(
            '[container]\npull_policy = "never"\n',
            encoding="utf-8",
        )

    def test_bogus_image_produces_a_diagnostic(self) -> None:
        self._write_never_pull_config()
        _write_workflow(env="docker://nope-this-image-does-not-exist:99.99")
        fake_bin = _write_fake_docker(daemon_up=True)

        result = _run_doctor(cwd=Path.cwd(), path_prefix=fake_bin)

        assert result.returncode == 1
        assert "missing_image" in result.stderr
        assert "nope-this-image-does-not-exist:99.99" in result.stderr

    def test_bogus_image_is_reported_in_json(self) -> None:
        self._write_never_pull_config()
        _write_workflow(env="docker://nope-this-image-does-not-exist:99.99")
        fake_bin = _write_fake_docker(daemon_up=True)

        result = _run_doctor("--json", cwd=Path.cwd(), path_prefix=fake_bin)

        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert payload["diagnostics"][0]["code"] == "missing_image"


class TestDoctorContainerDaemonValidation:
    """Issue #343: a Docker CLI whose daemon is down is named by ``doctor``.
    It is a warning: only a container task that has to run needs the daemon."""

    def test_unreachable_daemon_produces_a_warning(self) -> None:
        _write_workflow(env="docker://ubuntu:24.04")
        fake_bin = _write_fake_docker(daemon_up=False)

        result = _run_doctor("--json", cwd=Path.cwd(), path_prefix=fake_bin)

        assert result.returncode == 0, result.stdout
        [diagnostic] = json.loads(result.stdout)["diagnostics"]
        assert diagnostic["severity"] == "warning"
        assert diagnostic["code"] == "container_daemon_unreachable"
        assert "cached tasks do not need it" in diagnostic["message"]
        assert "docker daemon is not reachable" in diagnostic["message"]
        assert "failed to connect to the docker API" in diagnostic["message"]

    def test_dry_run_warns_and_passes(self) -> None:
        _write_workflow(env="docker://ubuntu:24.04")
        fake_bin = _write_fake_docker(daemon_up=False)

        result = subprocess.run(
            [str(PYTHON), "-m", "ginkgo.cli", "run", "workflow.py", "--dry-run"],
            check=False,
            text=True,
            capture_output=True,
            env={**os.environ, "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"},
        )

        assert result.returncode == 0, result.stderr
        assert "docker daemon is not reachable" in result.stderr
        assert "cached tasks do not need it" in result.stderr

    def test_workflow_without_container_envs_does_not_probe(self) -> None:
        _write_workflow(env=None)
        fake_bin = _write_fake_docker(daemon_up=False)

        result = _run_doctor(cwd=Path.cwd(), path_prefix=fake_bin)

        assert result.returncode == 0, result.stderr
        assert not (fake_bin / "calls.log").exists()


class TestDoctorExecutorSelection:
    """Which executor settings the FUSE probes are diagnosed against."""

    def test_every_executor_section_is_collected(self) -> None:
        config = {
            "remote": {
                "executors": {
                    "cheap-batch": {"type": "batch", "image": "plain"},
                    "stream-k8s": {"type": "k8s", "fuse_image": "fuse-worker"},
                },
            }
        }
        assert _extract_executor_configs(config=config) == {
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

        missing = [item for item in diagnostics if item.code == "fuse_image_not_configured"]
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

    def test_a_docstring_example_import_does_not_fail_doctor(self) -> None:
        """An import inside prose is not one the environment has to supply."""
        self._manifest()
        _write_workflow(env=None)
        Path("analysis.py").write_text(
            '"""Notes.\n\nExample::\n\n    import totally_absent_lib\n"""\n',
            encoding="utf-8",
        )

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"ok": True, "diagnostics": []}

    def test_an_interpreter_that_imports_everything_stays_quiet(self) -> None:
        """The ``pixi run`` case, where the interpreter is the declared environment."""
        self._manifest()
        _write_workflow(env=None)

        result = _run_doctor("--json", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout) == {"ok": True, "diagnostics": []}


class _Driver:
    def __init__(self, *, name: str, healthy: bool) -> None:
        self.name = name
        self._healthy = healthy

    def health_check(self) -> None:
        if not self._healthy:
            raise DriverUnavailableError(f"{self.name} not on PATH")


def test_only_installed_drivers_that_fail_their_health_check_are_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GINKGO_FUSE_SKIP_DRIVER_PROBE", raising=False)
    drivers = Plugin(
        name="drivers",
        contributions=(
            MountDriverType(scheme="gs", build=lambda: _Driver(name="gcsfuse", healthy=True)),
            MountDriverType(
                scheme="s3", build=lambda: _Driver(name="mountpoint-s3", healthy=False)
            ),
        ),
    )
    with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, drivers))):
        (diagnostic,) = access_doctor._probe_driver_binaries()

    assert diagnostic.code == "fuse_drivers_not_on_host"
    assert diagnostic.message.endswith("mountpoint-s3 (s3://)")


class TestDoctorRefusedDirective:
    """A directive refused whatever the run's state fails doctor, as it fails a dry run."""

    def test_a_jupyter_notebook_task_is_an_error(self, tmp_path: Path) -> None:
        pytest.importorskip("ginkgo_notebook")
        Path("legacy.ipynb").write_text("{}", encoding="utf-8")
        Path("workflow.py").write_text(
            """
from pathlib import Path

from ginkgo import flow, task
from ginkgo_notebook import notebook


@task("notebook")
def legacy() -> str:
    return notebook(Path(__file__).parent / "legacy.ipynb")


@flow
def main():
    return legacy()
""".lstrip(),
            encoding="utf-8",
        )

        result = _run_doctor(cwd=tmp_path)

        assert result.returncode == 1
        assert "refused_directive" in result.stderr
        assert "marimo convert" in result.stderr
