"""Tests for ``ginkgo doctor``."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest

from ginkgo.cli.commands.doctor import _extract_executor_configs
from ginkgo.plugins.api import MountDriverType, Plugin
from ginkgo.plugins.builtin import BUILTIN
from ginkgo.plugins.registry import PluginRegistry, use_plugin_registry
from ginkgo.remote.access import doctor as access_doctor
from ginkgo.remote.access.drivers.base import DriverUnavailableError
from tests.cli.conftest import SIMPLE_FLOW, run_ok, write
from tests.conftest import run_cli

RUN_DIR = Path(".ginkgo") / "runs" / "20260101_000000_000000_deadbeef"


def _doctor_json(*args: str, code: int = 0) -> list[dict]:
    """Run ``ginkgo doctor workflow.py --json`` and return its diagnostics."""
    result = run_cli("doctor", "workflow.py", "--json", *args)
    assert result.returncode == code, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is (code == 0)
    return payload["diagnostics"]


def _shell_workflow(*, env: str | None = None) -> None:
    """Write a single-shell-task workflow, optionally declaring an ``env``."""
    decorator = '@task("shell")' if env is None else f'@task("shell", env="{env}")'
    write(
        "workflow.py",
        f"""
from ginkgo import flow, shell, task

{decorator}
def greet() -> str:
    return shell(cmd="echo hello")

@flow
def main():
    return greet()
""",
    )


def _fake_docker(monkeypatch: pytest.MonkeyPatch, *, daemon_up: bool) -> Path:
    """Put a ``docker`` stub with no local images first on ``PATH``; return its directory."""
    bin_dir = Path("fake-bin").resolve()
    info = (
        "exit 0"
        if daemon_up
        else ("echo 'failed to connect to the docker API at unix:///fake/docker.sock' >&2; exit 1")
    )
    stub = write(
        bin_dir / "docker",
        f'#!/bin/sh\necho "$@" >> "{bin_dir / "calls.log"}"\n'
        f'case "$1" in\n  info) {info} ;;\n  *) exit 1 ;;\nesac\n',
    )
    stub.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return bin_dir


def test_a_clean_workflow_passes() -> None:
    _shell_workflow()
    assert _doctor_json() == []


def test_a_missing_secret_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    write(
        "workflow.py",
        """
        from ginkgo import flow, secret, task

        @task()
        def echo_token(token: str) -> str:
            return token

        @flow
        def main():
            return echo_token(token=secret("MISSING_TOKEN"))
        """,
    )
    monkeypatch.delenv("MISSING_TOKEN", raising=False)
    assert _doctor_json(code=1)[0]["code"] == "missing_secret"
    text = run_cli("doctor", "workflow.py")
    assert text.returncode == 1
    assert "Missing secrets: env:MISSING_TOKEN" in text.stderr


@pytest.mark.parametrize(
    ("source", "code", "phrase"),
    [
        (
            """
            from ginkgo import flow, task

            @task()
            def greet(text: str) -> str:
                return text

            @flow
            def main():
                greet(text="dropped")
                return greet(text="kept")
            """,
            "unreachable_task_call",
            "greet()",
        ),
        # #307: a ``str`` path parameter is a warning, not a failure.
        (
            """
            from ginkgo import flow, task

            @task()
            def write_report(output_path: str) -> str:
                return output_path

            @flow
            def main():
                return write_report(output_path="report.txt")
            """,
            "path_like_str_param",
            "output_path",
        ),
    ],
)
def test_a_warning_does_not_fail_doctor(source: str, code: str, phrase: str) -> None:
    write("workflow.py", source)
    text = run_ok("doctor", "workflow.py").stdout
    assert code in text and phrase in text
    (diagnostic,) = _doctor_json()
    assert (diagnostic["severity"], diagnostic["code"]) == ("warning", code)


def test_dropped_producer_accompanies_its_consumers_validation_error() -> None:
    """#341: the warning explains why the consumer's `file` input does not exist."""
    write(
        "workflow.py",
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
        """,
    )
    warning, error = _doctor_json(code=1)
    assert (warning["severity"], warning["code"]) == ("warning", "unreachable_task_call")
    assert "produce()" in warning["message"]
    assert error["severity"] == "error"
    assert "must exist and be a file" in error["message"]


def test_consumed_partial_call_is_an_error() -> None:
    """#332: a call missing a required argument is reported, not passed."""
    write(
        "workflow.py",
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
        """,
    )
    (diagnostic,) = _doctor_json(code=1)
    assert diagnostic["code"] == "incomplete_call"
    assert "join() is missing required argument(s): right" in diagnostic["message"]
    assert ".map()" in diagnostic["suggestion"]


def test_env_on_a_kind_without_env_support_is_invalid_env_kind() -> None:
    write(
        "workflow.py",
        """
        from ginkgo import flow, task

        @task(kind="subworkflow", env="analysis")
        def child() -> str:
            return "x"

        @flow
        def main():
            return child()
        """,
    )
    result = run_cli("doctor", "workflow.py", "--json")
    (diagnostic,) = json.loads(result.stdout)["diagnostics"]
    assert diagnostic["code"] == "invalid_env_kind"
    assert "kind='subworkflow'" in diagnostic["message"]


class TestDoctorWorkspaceLedger:
    """The workspace doctor runs in is reported on, not only the workflow."""

    def test_run_directories_without_a_ledger_are_a_warning(self) -> None:
        """Doctor names the mismatch every read surface otherwise hides (issue #282)."""
        _shell_workflow()
        RUN_DIR.mkdir(parents=True)
        assert "1 run directory under .ginkgo/runs but no database" in (
            run_ok("doctor", "workflow.py").stdout
        )
        (diagnostic,) = _doctor_json()
        assert (diagnostic["severity"], diagnostic["code"]) == ("warning", "runs_without_ledger")

    @pytest.mark.parametrize(
        ("damage", "code", "phrase"),
        [
            # ED-6: a database recreated after a deletion hides the older runs too.
            (
                RUN_DIR.mkdir,
                "runs_without_ledger",
                "1 run directory under .ginkgo/runs with no row",
            ),
            # ED-6: deleting .ginkgo/runs fails db check, so doctor must not pass silently.
            (lambda: shutil.rmtree(RUN_DIR.parent), "runs_without_directory", "1 run recorded"),
            # ED5-7: what `db check` fails on, doctor reports.
            (
                lambda: [shutil.rmtree(entry) for entry in (Path(".ginkgo") / "cache").iterdir()],
                "ledger_drift",
                "has a row but no output.json",
            ),
        ],
        ids=["recreated-ledger", "deleted-runs", "deleted-cache"],
    )
    def test_a_damaged_workspace_is_reported(self, damage, code: str, phrase: str) -> None:
        write("workflow.py", SIMPLE_FLOW)
        run_ok("run", "workflow.py")
        damage()
        (diagnostic,) = _doctor_json()
        assert diagnostic["code"] == code
        assert phrase in diagnostic["message"]
        if code == "ledger_drift":
            assert "db check --fix" in diagnostic["suggestion"]

    def test_a_corrupt_ledger_is_an_error(self) -> None:
        """ED5-7: doctor must not pass a ledger `db check` cannot open."""
        _shell_workflow()
        write(".ginkgo/ginkgo.db").write_bytes(b"not a database" * 100)
        assert "corrupt_ledger" in [item["code"] for item in _doctor_json(code=1)]

    def test_a_workspace_that_has_not_run_yet_is_not_reported(self) -> None:
        _shell_workflow()
        RUN_DIR.parent.mkdir(parents=True)
        assert _doctor_json() == []


class TestDoctorContainers:
    """``pull_policy = "never"`` and a stub ``docker`` keep these network-free."""

    def test_bogus_image_is_an_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """#288: an image that cannot possibly run is caught before the run."""
        write("ginkgo.toml", '[container]\npull_policy = "never"\n')
        _shell_workflow(env="docker://nope-this-image-does-not-exist:99.99")
        _fake_docker(monkeypatch, daemon_up=True)

        text = run_cli("doctor", "workflow.py")
        assert text.returncode == 1
        assert "missing_image" in text.stderr
        assert "nope-this-image-does-not-exist:99.99" in text.stderr
        assert _doctor_json(code=1)[0]["code"] == "missing_image"

    def test_unreachable_daemon_is_a_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """#343: only a container task that has to run needs the daemon."""
        _shell_workflow(env="docker://ubuntu:24.04")
        _fake_docker(monkeypatch, daemon_up=False)

        (diagnostic,) = _doctor_json()
        assert (diagnostic["severity"], diagnostic["code"]) == (
            "warning",
            "container_daemon_unreachable",
        )
        for phrase in (
            "cached tasks do not need it",
            "docker daemon is not reachable",
            "failed to connect to the docker API",
        ):
            assert phrase in diagnostic["message"]

        dry_run = run_ok("run", "workflow.py", "--dry-run")
        assert "docker daemon is not reachable" in dry_run.stderr

    def test_workflow_without_container_envs_does_not_probe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _shell_workflow()
        fake_bin = _fake_docker(monkeypatch, daemon_up=False)
        run_ok("doctor", "workflow.py")
        assert not (fake_bin / "calls.log").exists()


def test_every_executor_section_is_collected() -> None:
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
    assert _extract_executor_configs(config={}) == {}


def test_executor_without_fuse_image_is_reported_despite_a_streaming_peer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One well-configured executor must not clear the others."""
    write(tmp_path / "ginkgo.toml", '[remote.access]\ndefault = "fuse"\n')
    monkeypatch.setattr(access_doctor, "_probe_driver_binaries", lambda: [])
    diagnostics = access_doctor.collect_access_diagnostics(
        project_root=tmp_path,
        executor_configs={
            "[remote.executors.stream-k8s]": {"fuse_image": "fuse-worker"},
            "[remote.executors.gpu-k8s]": {"image": "plain"},
        },
    )
    (missing,) = [item for item in diagnostics if item.code == "fuse_image_not_configured"]
    assert "[remote.executors.gpu-k8s]" in missing.message


def test_a_missing_import_is_reported_against_the_manifest() -> None:
    """#221: Python task bodies run in the CLI's interpreter, so doctor checks it."""
    write(
        "pixi.toml",
        '[workspace]\nname = "demo"\n\n[dependencies]\npython = ">=3.11"\n\n'
        '[tasks]\nrun = "ginkgo run"\n',
    )
    _shell_workflow()
    write("analysis.py", "import totally_absent_lib\n")

    result = run_cli("doctor", "workflow.py")
    assert result.returncode == 1
    combined = result.stdout + result.stderr
    for phrase in (
        "interpreter_env_mismatch",
        "cannot import: totally_absent_lib",
        "pixi.toml",
        "Try: pixi run run",
    ):
        assert phrase in combined
    assert [item["code"] for item in _doctor_json(code=1)] == ["interpreter_env_mismatch"]


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


def test_a_jupyter_notebook_task_is_a_refused_directive() -> None:
    """A directive refused whatever the run's state fails doctor, as it fails a dry run."""
    pytest.importorskip("ginkgo_notebook")
    write("legacy.ipynb", "{}")
    write(
        "workflow.py",
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
        """,
    )
    with use_plugin_registry(PluginRegistry.discover()):
        result = run_cli("doctor", "workflow.py")
    assert result.returncode == 1
    assert "refused_directive" in result.stderr and "marimo convert" in result.stderr
