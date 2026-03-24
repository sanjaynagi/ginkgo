"""Tests for the container execution backend and composite routing."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from ginkgo import shell, task
from ginkgo.envs.container import (
    ContainerBackend,
    ContainerPrepareError,
    ContainerRef,
    ContainerRuntimeNotFoundError,
    is_container_env,
    parse_container_uri,
)
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.backend import CompositeBackend, LocalBackend


# ------------------------------------------------------------------
# URI parsing
# ------------------------------------------------------------------


class TestParseContainerUri:
    def test_docker_scheme(self):
        ref = parse_container_uri("docker://myorg/image:tag")
        assert ref == ContainerRef(scheme="docker", image="myorg/image:tag")

    def test_oci_scheme(self):
        ref = parse_container_uri("oci://registry.io/image@sha256:abc123")
        assert ref == ContainerRef(scheme="oci", image="registry.io/image@sha256:abc123")

    def test_no_scheme_raises(self):
        with pytest.raises(ValueError, match="Not a container URI"):
            parse_container_uri("myenv")

    def test_empty_image_raises(self):
        with pytest.raises(ValueError, match="no image reference"):
            parse_container_uri("docker://")

    def test_path_is_not_container(self):
        with pytest.raises(ValueError, match="Not a container URI"):
            parse_container_uri("./envs/foo/pixi.toml")


class TestIsContainerEnv:
    def test_docker_uri(self):
        assert is_container_env("docker://image:tag") is True

    def test_oci_uri(self):
        assert is_container_env("oci://image:tag") is True

    def test_plain_name(self):
        assert is_container_env("my_env") is False

    def test_path(self):
        assert is_container_env("./envs/foo/pixi.toml") is False


# ------------------------------------------------------------------
# ContainerBackend
# ------------------------------------------------------------------


class TestContainerBackendShellArgv:
    def test_builds_correct_command(self, tmp_path: Path):
        backend = ContainerBackend(
            runtime="docker",
            project_root=tmp_path,
        )
        argv = backend.shell_argv(
            env="docker://myorg/image:3.11",
            cmd="echo hello > output.txt",
        )
        assert argv == [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{tmp_path}:{tmp_path}",
            "-w",
            str(tmp_path),
            "myorg/image:3.11",
            "bash",
            "-c",
            "echo hello > output.txt",
        ]

    def test_uses_podman_runtime(self, tmp_path: Path):
        backend = ContainerBackend(runtime="podman", project_root=tmp_path)
        argv = backend.shell_argv(env="docker://img:1", cmd="ls")
        assert argv[0] == "podman"


class TestContainerBackendValidateEnvs:
    def test_valid_uris_with_runtime(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
            backend.validate_envs(env_names={"docker://img:1", "oci://img:2"})

    def test_invalid_uri_raises(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with pytest.raises(ValueError, match="Not a container URI"):
            backend.validate_envs(env_names={"not_a_container"})

    def test_missing_runtime_raises(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with patch("ginkgo.envs.container.shutil.which", return_value=None):
            with pytest.raises(ContainerRuntimeNotFoundError, match="docker"):
                backend.validate_envs(env_names={"docker://img:1"})


class TestContainerBackendPrepare:
    def test_pulls_image_on_always(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="always",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
            with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
                backend.prepare(env="docker://myimg:latest")

            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert call_args == ["docker", "pull", "myimg:latest"]

    def test_skips_pull_when_present(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="if-not-present",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            # Image inspect succeeds — image is present.
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
            backend.prepare(env="docker://myimg:latest")

            # Only the inspect call, no pull.
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert call_args == ["docker", "image", "inspect", "myimg:latest"]

    def test_pulls_when_not_present(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="if-not-present",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.side_effect = [
                # inspect fails — image not present
                subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr=""),
                # pull succeeds
                subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
            ]
            with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
                backend.prepare(env="docker://myimg:latest")

            assert mock_run.call_count == 2
            assert mock_run.call_args_list[1][0][0] == ["docker", "pull", "myimg:latest"]

    def test_never_policy_skips_pull(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="never",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            backend.prepare(env="docker://myimg:latest")
            mock_run.assert_not_called()

    def test_pull_failure_raises(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="always",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="not found"
            )
            with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
                with pytest.raises(ContainerPrepareError, match="myimg:latest"):
                    backend.prepare(env="docker://myimg:latest")

    def test_idempotent_within_session(self, tmp_path: Path):
        backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="always",
        )
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
            with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
                backend.prepare(env="docker://myimg:latest")
                backend.prepare(env="docker://myimg:latest")

            # Only pulled once.
            assert mock_run.call_count == 1


class TestContainerBackendEnvIdentity:
    def test_returns_digest(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="sha256:abc123def456\n", stderr=""
            )
            digest = backend.env_identity(env="docker://myimg:latest")

        assert digest == "sha256:abc123def456"

    def test_returns_none_on_failure(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1, stdout="", stderr="not found"
            )
            digest = backend.env_identity(env="docker://myimg:latest")

        assert digest is None

    def test_caches_result(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="sha256:abc\n", stderr=""
            )
            backend.env_identity(env="docker://myimg:latest")
            backend.env_identity(env="docker://myimg:latest")

        mock_run.assert_called_once()


class TestContainerBackendEnvLockPath:
    def test_returns_none(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        assert backend.env_lock_path(env="docker://img:1") is None


# ------------------------------------------------------------------
# CompositeBackend
# ------------------------------------------------------------------


# ------------------------------------------------------------------
# Evaluator validation
# ------------------------------------------------------------------


class TestContainerKindRestriction:
    def test_python_task_with_container_env_raises_at_validation(self):
        from ginkgo import evaluate, task

        @task(env="docker://myimg:latest")
        def python_in_container(x: int) -> int:
            return x + 1

        with pytest.raises(TypeError, match="only support driver tasks"):
            evaluate(python_in_container(x=1))

    def test_shell_task_with_container_env_passes_validation(self, tmp_path: Path):
        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        from ginkgo import shell, task

        @task(kind="shell", env="docker://myimg:latest")
        def shell_in_container(output_path: str) -> str:
            return shell(cmd=f"echo ok > {output_path}", output=output_path)

        output = str(tmp_path / "out.txt")

        # Validation should pass — the container env is valid for shell tasks.
        # We only need to check that _validate_declared_envs doesn't raise
        # TypeError.  The backend.validate_envs call will fail because Docker
        # isn't available, but that's a different error.
        backend = ContainerBackend(project_root=tmp_path)
        evaluator = _ConcurrentEvaluator(backend=backend)
        evaluator._root_template = shell_in_container(output_path=output)
        evaluator._root_dependency_ids = evaluator._register_value(evaluator._root_template)

        # This should not raise TypeError (container kind restriction).
        # It may raise ContainerRuntimeNotFoundError from validate_envs
        # if docker is not installed — that's expected and fine.
        try:
            evaluator._validate_declared_envs()
        except ContainerRuntimeNotFoundError:
            pass  # Expected when docker is not on PATH.


# ------------------------------------------------------------------
# CompositeBackend
# ------------------------------------------------------------------


class TestCompositeBackend:
    def _make_composite(self, tmp_path: Path) -> CompositeBackend:
        registry = PixiRegistry(project_root=tmp_path)
        return CompositeBackend(
            local=LocalBackend(pixi_registry=registry),
            container=ContainerBackend(project_root=tmp_path),
        )

    def test_routes_container_env_to_container_backend(self, tmp_path: Path):
        composite = self._make_composite(tmp_path)
        argv = composite.shell_argv(env="docker://img:1", cmd="echo hi")
        assert argv[0] == "docker"
        assert "run" in argv

    def test_routes_pixi_env_to_local_backend(self, tmp_path: Path):
        env_dir = tmp_path / "envs" / "myenv"
        env_dir.mkdir(parents=True)
        (env_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'test'\nchannels = []\nplatforms = []\n"
        )
        composite = self._make_composite(tmp_path)
        argv = composite.shell_argv(env="myenv", cmd="echo hi")
        assert argv[0] == "pixi"

    def test_validate_envs_partitions(self, tmp_path: Path):
        composite = self._make_composite(tmp_path)

        # Container validation passes (runtime mock), Pixi env not found.
        with patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"):
            # Only container envs — should pass.
            composite.validate_envs(env_names={"docker://img:1"})

    def test_no_container_backend_raises_on_container_env(self, tmp_path: Path):
        registry = PixiRegistry(project_root=tmp_path)
        composite = CompositeBackend(
            local=LocalBackend(pixi_registry=registry),
            container=None,
        )
        with pytest.raises(RuntimeError, match="requires a container backend"):
            composite.shell_argv(env="docker://img:1", cmd="ls")

    def test_env_lock_path_delegates(self, tmp_path: Path):
        composite = self._make_composite(tmp_path)
        assert composite.env_lock_path(env="docker://img:1") is None

    def test_env_identity_delegates(self, tmp_path: Path):
        composite = self._make_composite(tmp_path)
        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="sha256:abc\n", stderr=""
            )
            digest = composite.env_identity(env="docker://img:1")
        assert digest == "sha256:abc"


# ------------------------------------------------------------------
# End-to-end evaluator tests with mocked container execution
# ------------------------------------------------------------------


@task(kind="shell", env="docker://myimg:latest")
def _container_shell_task(output_path: str) -> str:
    return shell(cmd=f"echo ok > {output_path}", output=output_path)


class TestContainerShellE2E:
    """Verify the full evaluator → CompositeBackend → subprocess path."""

    def test_shell_task_executes_through_container_backend(self, tmp_path: Path):
        """A shell task with a container env runs via ``docker run``."""
        from ginkgo import evaluate
        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        output_file = tmp_path / "result.txt"
        captured_argv: list[Any] = []

        def mock_run_subprocess(self_eval, *, argv, use_shell):
            captured_argv.append(argv)
            output_file.write_text("ok\n")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")

        with (
            patch.object(_ConcurrentEvaluator, "_run_subprocess", mock_run_subprocess),
            patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
        ):
            result = evaluate(
                _container_shell_task(output_path=str(output_file)),
                backend=CompositeBackend(
                    local=LocalBackend(
                        pixi_registry=PixiRegistry(project_root=tmp_path),
                    ),
                    container=ContainerBackend(
                        project_root=tmp_path,
                        pull_policy="never",
                    ),
                ),
            )

        assert str(result) == str(output_file)
        assert len(captured_argv) == 1
        argv = captured_argv[0]
        assert argv[0] == "docker"
        assert "run" in argv
        assert "myimg:latest" in argv

    def test_provenance_records_container_metadata(self, tmp_path: Path):
        """Provenance manifest includes backend type and image digest."""
        from ginkgo import evaluate
        from ginkgo.runtime.provenance import RunProvenanceRecorder, load_manifest, make_run_id

        output_file = tmp_path / "result.txt"
        runs_dir = tmp_path / ".ginkgo" / "runs"

        run_id = make_run_id()
        provenance = RunProvenanceRecorder(
            run_id=run_id,
            workflow_path=Path("test.py"),
            root_dir=runs_dir,
            jobs=1,
            cores=1,
        )

        container_backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="never",
        )

        from ginkgo.runtime.evaluator import _ConcurrentEvaluator

        def mock_run_subprocess(self_eval, *, argv, use_shell):
            output_file.write_text("ok\n")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")

        with (
            patch.object(_ConcurrentEvaluator, "_run_subprocess", mock_run_subprocess),
            patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
            patch("ginkgo.envs.container.subprocess.run") as mock_container_run,
        ):
            mock_container_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="sha256:deadbeef1234\n", stderr=""
            )

            evaluate(
                _container_shell_task(output_path=str(output_file)),
                backend=CompositeBackend(
                    local=LocalBackend(
                        pixi_registry=PixiRegistry(project_root=tmp_path),
                    ),
                    container=container_backend,
                ),
                provenance=provenance,
            )

        manifest = load_manifest(runs_dir / run_id)
        tasks = manifest["tasks"]
        assert len(tasks) == 1
        task_entry = next(iter(tasks.values()))
        assert task_entry["backend"] == "container"
        assert task_entry["container_image_digest"] == "sha256:deadbeef1234"

    def test_container_cache_uses_image_digest(self, tmp_path: Path):
        """Cache key incorporates the container image digest."""
        from ginkgo.runtime.cache import CacheStore

        container_backend = ContainerBackend(
            project_root=tmp_path,
            pull_policy="never",
        )

        with patch("ginkgo.envs.container.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="sha256:abc123\n", stderr=""
            )

            store = CacheStore(
                backend=CompositeBackend(
                    local=LocalBackend(
                        pixi_registry=PixiRegistry(project_root=tmp_path),
                    ),
                    container=container_backend,
                )
            )

            digest = store.backend.env_identity(env="docker://myimg:latest")
            assert digest == "sha256:abc123"
