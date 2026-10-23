"""Tests for the container execution backend and composite routing.

The runtime is a real executable on ``PATH`` (the ``fake_runtime`` fixture)
that logs each call and answers as each test tells it, so these tests cross
the same process boundary the backend does in production.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import ginkgo.envs.container as container_module
from ginkgo import evaluate, shell, task
from ginkgo.envs.container import (
    ContainerBackend,
    ContainerDaemonUnreachableError,
    ContainerImageNotFoundError,
    ContainerPrepareError,
    ContainerRef,
    ContainerRuntimeNotFoundError,
    is_container_env,
    parse_container_uri,
)
from ginkgo.envs.mounts import mount
from ginkgo.plugins.api import EnvironmentContext, EnvironmentType, Plugin
from ginkgo.plugins.builtin import BUILTIN
from ginkgo.plugins.registry import PluginNotFoundError, PluginRegistry, use_plugin_registry
from ginkgo.runtime.backend import EnvironmentRouter
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from tests.conftest import Ledger, ShellEnvironment


@pytest.fixture
def short_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the runtime's bounded probes give up after a fraction of a second."""
    monkeypatch.setattr(container_module, "_DAEMON_PROBE_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(container_module, "_MANIFEST_INSPECT_TIMEOUT_SECONDS", 0.3)


def _subcommands(fake_runtime) -> list[str]:
    """Each call as ``"<runtime> <subcommand>"``."""
    return [" ".join(call.split()[:2]) for call in fake_runtime.calls]


# ------------------------------------------------------------------
# URI parsing
# ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("uri", "ref"),
    [
        ("docker://myorg/image:tag", ContainerRef(scheme="docker", image="myorg/image:tag")),
        (
            "oci://registry.io/image@sha256:abc",
            ContainerRef(scheme="oci", image="registry.io/image@sha256:abc"),
        ),
    ],
)
def test_container_uris_parse(uri: str, ref: ContainerRef) -> None:
    assert parse_container_uri(uri) == ref
    assert is_container_env(uri)


@pytest.mark.parametrize(
    ("uri", "match"),
    [
        ("myenv", "Not a container URI"),
        ("./envs/foo/pixi.toml", "Not a container URI"),
        ("docker://", "no image reference"),
    ],
)
def test_non_container_uris_are_refused(uri: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_container_uri(uri)


def test_plain_names_and_paths_are_not_container_envs() -> None:
    assert not is_container_env("my_env")
    assert not is_container_env("./envs/foo/pixi.toml")


# ------------------------------------------------------------------
# exec_argv
# ------------------------------------------------------------------


def _volumes(argv: list[str]) -> list[str]:
    return [argv[i + 1] for i, part in enumerate(argv) if part == "-v"]


class TestExecArgv:
    def test_builds_the_run_command(self, tmp_path: Path):
        argv = ContainerBackend(runtime="docker", project_root=tmp_path, user="root").exec_argv(
            env="docker://myorg/image:3.11", cmd="echo hello > output.txt"
        )
        assert argv == [
            "docker", "run", "--rm", "-v", f"{tmp_path}:{tmp_path}", "-w", str(tmp_path),
            "myorg/image:3.11", "bash", "-eo", "pipefail", "-c", "echo hello > output.txt",
        ]  # fmt: skip

    def test_runtime_and_shell_are_configurable(self, tmp_path: Path):
        argv = ContainerBackend(project_root=tmp_path, runtime="podman", shell="sh").exec_argv(
            env="docker://img:1", cmd="ls"
        )
        assert argv[0] == "podman"
        assert argv[-5:] == ["sh", "-eo", "pipefail", "-c", "ls"]

    def test_declared_inputs_outside_the_project_are_mounted_read_only(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        outside = tmp_path / "data" / "reads.fastq"
        outside.parent.mkdir()
        outside.write_text("@r\n")
        inside = project / "reads.fastq"
        inside.write_text("@r\n")
        link = project / "linked.fastq"
        link.symlink_to(outside)

        backend = ContainerBackend(project_root=project, user="root")
        argv = backend.exec_argv(
            env="docker://img:1", cmd="ls", mounts=[mount(outside), mount(inside), mount(link)]
        )
        # The project mount covers what is inside it; a symlink mounts its
        # real path at the path the command names.
        assert _volumes(argv) == [
            f"{project}:{project}",
            f"{outside}:{outside}:ro",
            f"{outside}:{link}:ro",
        ]

    def test_auto_mount_off_ignores_declared_inputs_but_not_extra_mounts(self, tmp_path: Path):
        reads = tmp_path / "reads.fastq"
        reads.write_text("@r\n")
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        project = tmp_path / "project"
        project.mkdir()

        backend = ContainerBackend(
            project_root=project, user="root", auto_mount=False, extra_mounts=(f"{scratch}:rw",)
        )
        argv = backend.exec_argv(env="docker://img:1", cmd="ls", mounts=[mount(reads)])
        assert _volumes(argv) == [f"{project}:{project}", f"{scratch}:{scratch}:rw"]


# ------------------------------------------------------------------
# validate_envs: runtime, image and registry checks
# ------------------------------------------------------------------


class TestValidateEnvs:
    """Pre-flight checks (issue #288): a bogus image is caught before the run."""

    def test_present_images_pass_without_probing_daemon_or_registry(self, tmp_path, fake_runtime):
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)
        assert backend.validate_envs(env_names={"docker://img:1", "oci://img:2"}) == []
        # A successful local lookup already proves the daemon is up.
        assert fake_runtime.calls == ["docker image inspect img:1", "docker image inspect img:2"]

    def test_an_invalid_uri_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="Not a container URI"):
            ContainerBackend(project_root=tmp_path).validate_envs(env_names={"not_a_container"})

    def test_a_missing_runtime_raises(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("PATH", str(tmp_path))
        with pytest.raises(ContainerRuntimeNotFoundError, match="docker"):
            ContainerBackend(project_root=tmp_path).validate_envs(env_names={"docker://img:1"})

    @pytest.mark.parametrize("probe_registry", [False, True])
    def test_an_image_absent_with_pull_policy_never_raises(
        self, tmp_path, fake_runtime, probe_registry: bool
    ):
        fake_runtime.respond("image", rc=1)
        backend = ContainerBackend(
            project_root=tmp_path, pull_policy="never", probe_registry=probe_registry
        )
        with pytest.raises(ContainerImageNotFoundError, match="pull_policy") as excinfo:
            backend.validate_envs(env_names={"docker://nope:99.99"})
        assert "'nope:99.99' is not present locally" in str(excinfo.value)

    def test_a_real_run_asks_neither_daemon_nor_registry(self, tmp_path, fake_runtime):
        """A real run's own pull reports a missing image; and a run whose
        container tasks are all cached must not fail on a stopped daemon."""
        fake_runtime.respond("image", rc=1)
        fake_runtime.respond("info", rc=1)
        backend = ContainerBackend(project_root=tmp_path, pull_policy="if-not-present")
        assert backend.validate_envs(env_names={"docker://img:1"}) == []
        assert _subcommands(fake_runtime) == ["docker image"]

    def test_a_registry_that_confirms_the_image_missing_raises(self, tmp_path, fake_runtime):
        fake_runtime.respond("image", rc=1)
        fake_runtime.respond("manifest", rc=1, err="manifest unknown: manifest not found")
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)
        with pytest.raises(ContainerImageNotFoundError, match="not found in its registry"):
            backend.validate_envs(env_names={"docker://nope:99.99"})
        assert "docker manifest inspect nope:99.99" in fake_runtime.calls

    @pytest.mark.parametrize(
        "response",
        [{"sleep": 5}, {"rc": 1, "err": "some transient network hiccup"}],
        ids=["timeout", "unrecognised-error"],
    )
    @pytest.mark.usefixtures("short_timeouts")
    def test_an_inconclusive_registry_never_blocks_the_run(self, tmp_path, fake_runtime, response):
        fake_runtime.respond("image", rc=1)
        fake_runtime.respond("manifest", **response)
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)

        assert backend.validate_envs(env_names={"docker://img:1"}) == []
        backend.validate_envs(env_names={"docker://img:1"})
        # Two envs resolving to the same image only probe it once too.
        backend.validate_envs(env_names={"docker://img:1", "oci://img:1"})
        manifest_calls = _subcommands(fake_runtime).count("docker manifest")
        # A probe killed by its timeout may not get as far as logging itself.
        assert manifest_calls == 1 or ("sleep" in response and manifest_calls == 0)

    def test_an_unknown_runtime_is_not_probed(self, tmp_path, fake_runtime):
        fake_runtime.respond("image", rc=1)
        backend = ContainerBackend(
            project_root=tmp_path, runtime="custom-runtime", probe_registry=True
        )
        assert backend.validate_envs(env_names={"docker://img:1"}) == []
        assert _subcommands(fake_runtime) == ["custom-runtime image"]


class TestDaemonProbe:
    """Issue #343: under ``doctor`` and ``--dry-run`` (``probe_registry``), a
    stopped daemon is reported as such, not as a missing image."""

    @pytest.fixture(autouse=True)
    def _image_absent(self, fake_runtime) -> None:
        fake_runtime.respond("image", rc=1)

    @pytest.mark.parametrize("pull_policy", ["if-not-present", "never"])
    def test_an_unreachable_daemon_is_one_warning_not_a_missing_image(
        self, tmp_path, fake_runtime, pull_policy: str
    ):
        fake_runtime.respond("info", rc=1, err="failed to connect to the docker API at unix:///x")
        backend = ContainerBackend(
            project_root=tmp_path, pull_policy=pull_policy, probe_registry=True
        )

        [warning] = backend.validate_envs(env_names={"docker://img:1", "docker://img:2"})

        assert isinstance(warning, ContainerDaemonUnreachableError)
        assert "docker daemon is not reachable" in str(warning)
        assert "failed to connect to the docker API" in str(warning)
        assert "pull" not in str(warning).lower()
        assert _subcommands(fake_runtime) == ["docker image", "docker info"]

    def test_permission_denied_suggests_the_docker_group(self, tmp_path, fake_runtime):
        fake_runtime.respond(
            "info", rc=1, err="permission denied while trying to connect to the Docker daemon"
        )
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)
        [warning] = backend.validate_envs(env_names={"docker://img:1"})
        assert "denied access to the docker daemon" in str(warning)
        assert "docker group" in str(warning)
        assert "systemctl start" not in str(warning)

    @pytest.mark.parametrize(
        ("runtime", "platform", "hint"),
        [
            ("docker", "darwin", "open -a Docker"),
            ("docker", "linux", "sudo systemctl start docker"),
            ("podman", "darwin", "podman machine start"),
        ],
    )
    def test_a_stopped_daemon_gets_the_platform_s_start_hint(
        self, tmp_path, fake_runtime, monkeypatch, runtime, platform, hint
    ):
        """NC-5: no Linux systemctl advice on macOS."""
        monkeypatch.setattr(container_module.sys, "platform", platform)
        fake_runtime.respond("info", rc=125, err="Cannot connect to the daemon")
        backend = ContainerBackend(project_root=tmp_path, runtime=runtime, probe_registry=True)
        [warning] = backend.validate_envs(env_names={"docker://img:1"})
        assert hint in str(warning)
        assert ("systemctl" in str(warning)) == (platform == "linux")
        assert f"{runtime} info" in fake_runtime.calls

    def test_a_reachable_daemon_is_probed_once_per_backend(self, tmp_path, fake_runtime):
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)
        backend.validate_envs(env_names={"docker://img:1", "docker://img:2"})
        backend.validate_envs(env_names={"docker://img:3"})
        assert _subcommands(fake_runtime).count("docker info") == 1

    @pytest.mark.usefixtures("short_timeouts")
    def test_a_hung_daemon_neither_stalls_validation_nor_blames_the_image(
        self, tmp_path, fake_runtime
    ):
        fake_runtime.respond("info", sleep=5)
        backend = ContainerBackend(project_root=tmp_path, probe_registry=True)
        assert backend.validate_envs(env_names={"docker://img:1"}) == []

        # A hung local lookup reads as "cannot tell", not as a missing image.
        fake_runtime.respond("image", sleep=5)
        never = ContainerBackend(project_root=tmp_path, pull_policy="never")
        assert never.validate_envs(env_names={"docker://img:2"}) == []


# ------------------------------------------------------------------
# prepare, identity and digest
# ------------------------------------------------------------------


class TestPrepare:
    @pytest.mark.parametrize(
        ("pull_policy", "image_rc", "calls"),
        [
            # The daemon is checked before a pull, which could otherwise hang.
            ("always", 0, ["docker info", "docker pull myimg:latest"]),
            ("if-not-present", 0, ["docker image inspect myimg:latest"]),
            (
                "if-not-present",
                1,
                ["docker image inspect myimg:latest", "docker info", "docker pull myimg:latest"],
            ),
            ("never", 1, []),
        ],
    )
    def test_the_pull_policy_decides_the_pull(
        self, tmp_path, fake_runtime, pull_policy, image_rc, calls
    ):
        fake_runtime.respond("image", rc=image_rc)
        backend = ContainerBackend(project_root=tmp_path, pull_policy=pull_policy)
        backend.prepare(env="docker://myimg:latest")
        backend.prepare(env="docker://myimg:latest")  # idempotent within a session
        assert fake_runtime.calls == calls

    @pytest.mark.usefixtures("short_timeouts")
    def test_a_daemon_that_does_not_answer_fails_before_the_pull(self, tmp_path, fake_runtime):
        fake_runtime.respond("info", sleep=5)
        backend = ContainerBackend(project_root=tmp_path, pull_policy="always")
        with pytest.raises(ContainerDaemonUnreachableError, match="did not answer"):
            backend.prepare(env="docker://myimg:latest")
        assert "docker pull" not in _subcommands(fake_runtime)

    def test_a_failed_pull_raises_naming_the_image(self, tmp_path, fake_runtime):
        fake_runtime.respond("pull", rc=1, err="not found")
        backend = ContainerBackend(project_root=tmp_path, pull_policy="always")
        with pytest.raises(ContainerPrepareError, match="myimg:latest"):
            backend.prepare(env="docker://myimg:latest")


def test_identity_is_the_declared_image_whatever_the_local_store(tmp_path, fake_runtime):
    """Regression for #194: pulling the image must not move the identity.

    The image is pulled by ``prepare``, after the cache key is built, so an
    identity read from the local store changed on the run that pulled it.
    """
    backend = ContainerBackend(project_root=tmp_path)
    assert backend.env_identity(env="docker://myimg:latest") == "myimg:latest"
    assert backend.env_lock_path(env="docker://myimg:latest") is None
    assert fake_runtime.calls == []


def test_the_materialized_digest_is_read_once_it_exists(tmp_path, fake_runtime):
    backend = ContainerBackend(project_root=tmp_path)
    fake_runtime.respond("image", rc=1, err="No such image")
    # An absent image is not remembered: prepare pulls it, and a later ask sees it.
    assert backend.materialized_digest(env="docker://myimg:latest") is None

    fake_runtime.respond("image", out="sha256:abc123def456\n")
    assert backend.materialized_digest(env="docker://myimg:latest") == "sha256:abc123def456"
    assert backend.materialized_digest(env="docker://myimg:latest") == "sha256:abc123def456"
    assert len(fake_runtime.calls) == 2


# ------------------------------------------------------------------
# Kind restriction and routing
# ------------------------------------------------------------------


@task(kind="shell", env="docker://myimg:latest")
def _container_shell_task(output_path: str) -> str:
    return shell(cmd=f"echo ok > {output_path}", output=output_path)


@task(kind="shell", env="docker://myimg:other")
def _other_image_task(output_path: str) -> str:
    return shell(cmd=f"echo ok > {output_path}", output=output_path)


def test_a_python_task_cannot_run_in_a_container():
    @task(env="docker://myimg:latest")
    def python_in_container(x: int) -> int:
        return x + 1

    with pytest.raises(TypeError, match="cannot run in a foreign environment"):
        evaluate(python_in_container(x=1))


def test_a_shell_task_with_a_container_env_passes_validation(tmp_path, fake_runtime):
    evaluator = ConcurrentEvaluator(backend=ContainerBackend(project_root=tmp_path))
    evaluator.build_graph(_container_shell_task(output_path=str(tmp_path / "out.txt")))
    evaluator._validator.validate_declared_envs(nodes=evaluator.graph.nodes.values())


def _composite(tmp_path: Path, *, pull_policy: str | None = None) -> EnvironmentRouter:
    """Route envs for a project at *tmp_path* through the built-in backends."""
    config = {"container": {"pull_policy": pull_policy}} if pull_policy else {}
    return EnvironmentRouter(
        context=EnvironmentContext(project_root=tmp_path, workflow_root=None, config=config)
    )


class TestEnvironmentRouter:
    def test_container_envs_share_one_container_backend(self, tmp_path, fake_runtime):
        composite = _composite(tmp_path)
        fake_runtime.respond("image", out="sha256:abc\n")

        assert composite.exec_argv(env="docker://img:1", cmd="echo hi")[:2] == ["docker", "run"]
        assert composite.env_identity(env="oci://img:1") == "img:1"
        assert composite.env_lock_path(env="docker://img:1") is None
        assert composite.materialized_digest(env="docker://img:1") == "sha256:abc"
        composite.validate_envs(env_names={"docker://img:1"})
        assert len(composite._built) == 1

    def test_a_bare_name_routes_to_the_bare_name_environment(self, tmp_path: Path):
        bare = Plugin(
            name="bare",
            contributions=(
                EnvironmentType(
                    name="shell", schemes=("",), label="shell", build=lambda _: ShellEnvironment()
                ),
            ),
        )
        with use_plugin_registry(PluginRegistry(plugins=(BUILTIN, bare))):
            argv = _composite(tmp_path).exec_argv(env="myenv", cmd="echo hi")
        assert argv == ["bash", "-eo", "pipefail", "-c", "echo hi"]

    @pytest.mark.parametrize(
        ("env", "match"),
        [
            ("myenv", "Install ginkgo-pixi to add it"),
            ("singularity://img:1", "environment for scheme 'singularity'"),
        ],
    )
    def test_an_env_no_plugin_serves_is_refused(self, tmp_path: Path, env: str, match: str):
        with pytest.raises(PluginNotFoundError, match=match):
            _composite(tmp_path).exec_argv(env=env, cmd="ls")


# ------------------------------------------------------------------
# End to end through the evaluator
# ------------------------------------------------------------------


def test_a_container_shell_task_runs_and_records_its_image(tmp_path, fake_runtime):
    """evaluator -> EnvironmentRouter -> ``docker run``, with provenance."""
    fake_runtime.respond("image", out="sha256:deadbeef1234\n")
    output = tmp_path / "result.txt"
    ledger = Ledger.start(root=tmp_path)

    result = evaluate(
        _container_shell_task(output_path=str(output)),
        backend=_composite(tmp_path, pull_policy="never"),
        run_dir=ledger.run_dir,
        event_bus=ledger.bus,
    )

    assert str(result) == str(output)
    assert output.read_text() == "ok\n"
    [run] = [call for call in fake_runtime.calls if call.startswith("docker run")]
    assert "myimg:latest" in run.split()
    [task_entry] = ledger.tasks().values()
    ledger.close()
    assert task_entry["backend"] == "container"
    assert task_entry["container_image_digest"] == "sha256:deadbeef1234"


def test_the_cache_key_follows_the_declared_image_not_the_local_store(tmp_path, fake_runtime):
    """A key built from the pulled image's ID moved on the first pull (#194)."""
    store = CacheStore(index=CacheIndex.in_memory(), backend=_composite(tmp_path))
    resolved_args = {"output_path": str(tmp_path / "out.txt")}

    def key_for(task_def, *, image_present: bool) -> str:
        if image_present:
            fake_runtime.respond("image", out="sha256:abc123\n")
        else:
            fake_runtime.respond("image", rc=1)
        return store.build_cache_key(task_def=task_def, resolved_args=resolved_args)[0]

    absent = key_for(_container_shell_task, image_present=False)
    assert absent == key_for(_container_shell_task, image_present=True)
    assert absent != key_for(_other_image_task, image_present=False)
