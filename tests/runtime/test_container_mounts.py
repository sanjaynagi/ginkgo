"""Tests for container bind-mount resolution and environment forwarding."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.envs.container import ContainerBackend, container_backend_from_config
from ginkgo.envs.mounts import (
    Mount,
    UnsafeMountError,
    mount,
    parse_extra_mount,
    resolve_mounts,
)
from ginkgo.runtime.task_runners.shell import (
    computed_env_var_names,
    declared_input_mounts,
    declared_output_mounts,
)


# ------------------------------------------------------------------
# Mount specifications
# ------------------------------------------------------------------


class TestParseExtraMount:
    def test_bare_path_defaults_to_read_only(self):
        assert parse_extra_mount("/scratch") == Mount(
            host_path=Path("/scratch"), container_path=Path("/scratch"), mode="ro"
        )

    def test_mode_suffix(self):
        assert parse_extra_mount("/scratch:rw").mode == "rw"

    def test_distinct_container_path(self):
        parsed = parse_extra_mount("/opt/refs:/refs:ro")
        assert parsed.host_path == Path("/opt/refs")
        assert parsed.container_path == Path("/refs")

    def test_two_fields_without_mode_is_a_remap(self):
        parsed = parse_extra_mount("/opt/refs:/refs")
        assert parsed.container_path == Path("/refs")
        assert parsed.mode == "ro"

    def test_unknown_mode_raises(self):
        with pytest.raises(ValueError, match="Invalid container mount mode"):
            parse_extra_mount("/scratch:/mnt:write")

    def test_empty_spec_raises(self):
        with pytest.raises(ValueError, match="Empty container mount"):
            parse_extra_mount("")

    def test_invalid_mode_on_mount_helper_raises(self):
        with pytest.raises(ValueError, match="Mount mode must be"):
            mount("/scratch", mode="write")


# ------------------------------------------------------------------
# Mount resolution
# ------------------------------------------------------------------


class TestResolveMounts:
    def test_path_inside_project_is_dropped(self, tmp_path: Path):
        inside = tmp_path / "data" / "reads.fastq"
        inside.parent.mkdir()
        inside.write_text("@r\n")

        assert resolve_mounts(project_root=tmp_path, mounts=[mount(inside)]) == []

    def test_path_outside_project_is_kept(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        outside = tmp_path / "data"
        outside.mkdir()

        resolved = resolve_mounts(project_root=project, mounts=[mount(outside)])
        assert [item.host_path for item in resolved] == [outside]

    def test_symlink_mounts_real_path_at_declared_path(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        real = tmp_path / "store"
        real.mkdir()
        link = project / "store"
        link.symlink_to(real)

        (resolved,) = resolve_mounts(project_root=project, mounts=[mount(link)])
        # The command dereferences the link, so the real path must be what is
        # mounted -- but at the path the command actually names.
        assert resolved.host_path == real
        assert resolved.container_path == link

    def test_descendant_collapses_into_same_mode_ancestor(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        parent = tmp_path / "data"
        child = parent / "reads.fastq"
        parent.mkdir()
        child.write_text("@r\n")

        resolved = resolve_mounts(project_root=project, mounts=[mount(parent), mount(child)])
        assert [item.host_path for item in resolved] == [parent]

    def test_descendant_of_different_mode_is_kept(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        parent = tmp_path / "data"
        child = parent / "out"
        child.mkdir(parents=True)

        resolved = resolve_mounts(
            project_root=project, mounts=[mount(parent), mount(child, mode="rw")]
        )
        assert {(item.host_path, item.mode) for item in resolved} == {
            (parent, "ro"),
            (child, "rw"),
        }

    def test_read_write_wins_for_the_same_path(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        shared = tmp_path / "shared"
        shared.mkdir()

        resolved = resolve_mounts(
            project_root=project, mounts=[mount(shared), mount(shared, mode="rw")]
        )
        assert [item.mode for item in resolved] == ["rw"]

    def test_not_yet_created_output_resolves_through_its_parent(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        results = tmp_path / "results"
        results.mkdir()

        (resolved,) = resolve_mounts(
            project_root=project, mounts=[mount(results / "out.txt", mode="rw")]
        )
        assert resolved.host_path == results / "out.txt"

    def test_filesystem_root_is_refused(self, tmp_path: Path):
        with pytest.raises(UnsafeMountError, match="host root or a system directory"):
            resolve_mounts(project_root=tmp_path, mounts=[mount("/")])

    def test_system_directory_is_refused(self, tmp_path: Path):
        with pytest.raises(UnsafeMountError, match="/etc"):
            resolve_mounts(project_root=tmp_path, mounts=[mount("/etc")])

    def test_non_system_top_level_directory_is_allowed(self, tmp_path: Path):
        # A data root like /scratch is exactly what these mounts are for.
        (resolved,) = resolve_mounts(project_root=tmp_path, mounts=[mount("/scratch")])
        assert resolved.host_path == Path("/scratch")


# ------------------------------------------------------------------
# Deriving mounts from a task
# ------------------------------------------------------------------


@dataclass
class _FakeTaskDef:
    type_hints: dict[str, Any] = field(default_factory=dict)
    export_thread_env: bool = False
    threads: int = 1


@dataclass
class _FakeNode:
    task_def: _FakeTaskDef
    resolved_args: dict[str, Any] = field(default_factory=dict)


class TestDeclaredInputMounts:
    def test_path_shaped_arguments_are_mounted_read_only(self, tmp_path: Path):
        reads = tmp_path / "reads.fastq"
        reads.write_text("@r\n")
        refs = tmp_path / "refs"
        refs.mkdir()

        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"reads": file, "refs": folder}),
            resolved_args={"reads": file(str(reads)), "refs": folder(str(refs))},
        )
        mounts = declared_input_mounts(node=node)
        assert {(item.host_path, item.mode) for item in mounts} == {
            (reads, "ro"),
            (refs, "ro"),
        }

    def test_scratch_directory_is_mounted_read_write(self, tmp_path: Path):
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"work": tmp_dir}),
            resolved_args={"work": tmp_dir(str(scratch))},
        )
        assert [item.mode for item in declared_input_mounts(node=node)] == ["rw"]

    def test_nested_and_optional_values(self, tmp_path: Path):
        first = tmp_path / "a.fastq"
        second = tmp_path / "b.fastq"
        for path in (first, second):
            path.write_text("@r\n")

        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"reads": list[file], "index": file | None}),
            resolved_args={
                "reads": [file(str(first)), file(str(second))],
                "index": None,
            },
        )
        assert {item.host_path for item in declared_input_mounts(node=node)} == {
            first,
            second,
        }

    def test_non_path_arguments_are_ignored(self, tmp_path: Path):
        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"sample": str, "threads": int}),
            resolved_args={"sample": str(tmp_path), "threads": 4},
        )
        assert declared_input_mounts(node=node) == []

    def test_missing_input_is_skipped(self, tmp_path: Path):
        # Mounting it would make the runtime create the host path; the command
        # that needs it gives the better error.
        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"reads": file}),
            resolved_args={"reads": file(str(tmp_path / "absent.fastq"))},
        )
        assert declared_input_mounts(node=node) == []


class TestDeclaredOutputMounts:
    def test_output_mounts_its_parent_read_write(self, tmp_path: Path):
        output = tmp_path / "results" / "out.txt"
        log = tmp_path / "logs" / "run.log"

        mounts = declared_output_mounts(output=str(output), log=log)
        assert {(item.host_path, item.mode) for item in mounts} == {
            (output.parent, "rw"),
            (log.parent, "rw"),
        }

    def test_multiple_outputs(self, tmp_path: Path):
        first = tmp_path / "one" / "a.txt"
        second = tmp_path / "two" / "b.txt"

        mounts = declared_output_mounts(output=[str(first), str(second)])
        assert {item.host_path for item in mounts} == {first.parent, second.parent}


# ------------------------------------------------------------------
# Environment forwarding
# ------------------------------------------------------------------


class TestComputedEnvVarNames:
    def test_thread_count_is_always_forwarded(self):
        assert computed_env_var_names(task_def=_FakeTaskDef()) == ("GINKGO_THREADS",)

    def test_thread_env_adds_blas_variables(self):
        names = computed_env_var_names(task_def=_FakeTaskDef(export_thread_env=True))
        assert "OMP_NUM_THREADS" in names
        assert "GINKGO_THREADS" in names


class TestContainerEnvForwarding:
    def test_names_are_forwarded_without_values(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, user="root")
        argv = backend.exec_argv(
            env="docker://img:1",
            cmd="tool --threads ${GINKGO_THREADS}",
            env_vars=["GINKGO_THREADS", "OMP_NUM_THREADS"],
        )
        # A bare -e NAME resolves from the client's own environment, so no
        # value reaches the host process table.
        assert argv.count("-e") == 2
        assert "GINKGO_THREADS" in argv
        assert not any(part.startswith("GINKGO_THREADS=") for part in argv)

    def test_forwarding_is_opt_in_per_call(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, user="root")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert "-e" not in argv


# ------------------------------------------------------------------
# Config table
# ------------------------------------------------------------------


class TestContainerBackendFromConfig:
    def test_defaults_without_a_table(self, tmp_path: Path):
        backend = container_backend_from_config(project_root=tmp_path, config={})
        assert backend.runtime == "docker"
        assert backend.pull_policy == "if-not-present"
        assert backend.user == "auto"
        assert backend.shell == "bash"
        assert backend.auto_mount is True
        assert backend.extra_mounts == ()

    def test_reads_every_setting(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path,
            config={
                "container": {
                    "runtime": "podman",
                    "pull_policy": "always",
                    "user": "root",
                    "shell": "sh",
                    "auto_mount": False,
                    "extra_mounts": ["/scratch:rw"],
                }
            },
        )
        assert backend.runtime == "podman"
        assert backend.pull_policy == "always"
        assert backend.user == "root"
        assert backend.shell == "sh"
        assert backend.auto_mount is False
        assert backend.extra_mounts == ("/scratch:rw",)

    def test_single_extra_mount_string(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path, config={"container": {"extra_mounts": "/scratch"}}
        )
        assert backend.extra_mounts == ("/scratch",)

    def test_non_mapping_table_falls_back_to_defaults(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path, config={"container": "docker"}
        )
        assert backend.runtime == "docker"

    def test_validate_envs_rejects_a_malformed_table(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path, config={"container": {"extra_mounts": ["/a:nope"]}}
        )
        with pytest.raises(ValueError, match="Invalid container mount mode"):
            backend.validate_envs(env_names={"docker://img:1"})


# ------------------------------------------------------------------
# Failure diagnosis
# ------------------------------------------------------------------


class TestExecFailureHint:
    def test_names_a_missing_shell(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        hint = backend.exec_failure_hint(
            env="docker://alpine:3.20",
            exit_code=127,
            output='exec: "bash": executable file not found in $PATH',
        )
        assert hint is not None
        assert "alpine:3.20" in hint
        assert "[container] shell" in hint

    def test_silent_on_an_ordinary_tool_failure(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        assert (
            backend.exec_failure_hint(
                env="docker://img:1", exit_code=1, output="samtools: no such reference"
            )
            is None
        )

    def test_silent_when_the_shell_is_not_what_failed(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        assert (
            backend.exec_failure_hint(
                env="docker://img:1",
                exit_code=127,
                output="minimap2: command not found",
            )
            is None
        )


@pytest.mark.skipif(not hasattr(os, "getuid"), reason="POSIX uid/gid only")
class TestUserDefault:
    def test_auto_resolves_to_the_invoking_user(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert f"{os.getuid()}:{os.getgid()}" in argv
