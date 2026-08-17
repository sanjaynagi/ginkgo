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
    MissingMountError,
    Mount,
    MountModeConflictError,
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
            host_path=Path("/scratch"),
            container_path=Path("/scratch"),
            mode="ro",
            origin="configured",
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

    def test_ambiguous_second_field_raises(self):
        # Neither a mode nor an absolute path: mounting at a relative container
        # path or guessing the mode would both be silent wrongness.
        with pytest.raises(ValueError, match="neither a mode"):
            parse_extra_mount("/scratch:nope")

    def test_too_many_fields_raises(self):
        with pytest.raises(ValueError, match="Invalid container mount specification"):
            parse_extra_mount("/a:/b:rw:extra")

    def test_config_entries_are_marked_configured(self):
        # The origin is what stops a derived mount widening a user's "ro".
        assert parse_extra_mount("/scratch:ro").origin == "configured"


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

    def test_absent_declared_path_is_skipped(self, tmp_path: Path):
        # Mounting it would make the runtime create the host path; the command
        # that needs it gives the better error.
        project = tmp_path / "project"
        project.mkdir()
        assert resolve_mounts(project_root=project, mounts=[mount(tmp_path / "gone")]) == []

    def test_absent_configured_path_raises(self, tmp_path: Path):
        project = tmp_path / "project"
        project.mkdir()
        with pytest.raises(MissingMountError, match="does not exist"):
            resolve_mounts(
                project_root=project, mounts=[parse_extra_mount(str(tmp_path / "gone"))]
            )

    def test_filesystem_root_is_refused(self, tmp_path: Path):
        with pytest.raises(UnsafeMountError, match="filesystem root"):
            resolve_mounts(project_root=tmp_path, mounts=[mount("/")])

    def test_system_directory_is_refused(self, tmp_path: Path):
        with pytest.raises(UnsafeMountError, match="system directory"):
            resolve_mounts(project_root=tmp_path, mounts=[mount("/etc")])

    def test_system_directory_is_refused_under_its_resolved_name(self, tmp_path: Path):
        # On macOS /etc resolves to /private/etc; checking only one spelling let
        # the other through.
        target = "/private/etc" if Path("/private/etc").exists() else "/etc"
        with pytest.raises(UnsafeMountError, match="system directory"):
            resolve_mounts(project_root=tmp_path, mounts=[mount(target, mode="rw")])

    def test_home_directory_is_refused(self, tmp_path: Path):
        # An output written straight into $HOME would otherwise hand the image
        # ~/.ssh and ~/.aws along with it.
        with pytest.raises(UnsafeMountError, match="home directory"):
            resolve_mounts(project_root=tmp_path, mounts=[mount(Path.home(), mode="rw")])

    def test_non_system_data_root_is_allowed(self, tmp_path: Path):
        # A data root of its own is exactly what these mounts are for.
        data = tmp_path / "data"
        data.mkdir()
        (resolved,) = resolve_mounts(project_root=tmp_path / "project", mounts=[mount(data)])
        assert resolved.host_path == data

    def test_configured_read_only_is_not_widened_by_a_derived_output(self, tmp_path: Path):
        refs = tmp_path / "refs"
        refs.mkdir()
        with pytest.raises(MountModeConflictError, match="read-only"):
            resolve_mounts(
                project_root=tmp_path / "project",
                mounts=[parse_extra_mount(f"{refs}:ro"), mount(refs, mode="rw")],
            )

    def test_symlinked_project_root_does_not_shadow_its_own_contents(self, tmp_path: Path):
        # The backend resolves project_root, so a project reached through a
        # symlink must not get every input mounted a second time, read-only,
        # over the read-write project mount.
        real = tmp_path / "real_project"
        (real / "data").mkdir(parents=True)
        (real / "data" / "in.txt").write_text("x\n")
        link = tmp_path / "project"
        link.symlink_to(real)

        resolved = resolve_mounts(
            project_root=link.resolve(), mounts=[mount(real / "data" / "in.txt")]
        )
        assert resolved == []


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
        # A file mounts its directory, so an index sitting beside it is visible;
        # a folder mounts itself.
        assert {(item.host_path, item.mode) for item in mounts} == {
            (reads.parent, "ro"),
            (refs, "ro"),
        }

    def test_a_sibling_index_of_a_file_input_is_visible(self, tmp_path: Path):
        reference = tmp_path / "refs" / "ref.fa"
        reference.parent.mkdir()
        reference.write_text(">chr1\n")
        index = reference.with_suffix(".fa.fai")
        index.write_text("chr1\t1\n")

        node = _FakeNode(
            task_def=_FakeTaskDef(type_hints={"reference": file}),
            resolved_args={"reference": file(str(reference))},
        )
        (item,) = declared_input_mounts(node=node)
        # Mounting the file alone would make an index that exists on the host
        # invisible to the tool that needs it.
        assert item.host_path == reference.parent
        assert index.parent == item.host_path

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
        assert {item.host_path for item in declared_input_mounts(node=node)} == {tmp_path}

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

        mounts = declared_output_mounts(output=str(output))
        assert {(item.host_path, item.mode) for item in mounts} == {(output.parent, "rw")}

    def test_no_output_declares_no_mounts(self):
        assert declared_output_mounts(output=None) == []

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

    def test_non_mapping_table_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match=r"\[container\] must be a table"):
            container_backend_from_config(project_root=tmp_path, config={"container": "docker"})

    def test_unknown_key_raises(self, tmp_path: Path):
        # A typo must not leave the run quietly on defaults.
        with pytest.raises(ValueError, match="unknown keys"):
            container_backend_from_config(
                project_root=tmp_path, config={"container": {"extra_mount": ["/a"]}}
            )

    def test_string_where_a_bool_belongs_raises(self, tmp_path: Path):
        # bool("false") is True, so coercing would invert what the file says.
        with pytest.raises(ValueError, match="must be true or false"):
            container_backend_from_config(
                project_root=tmp_path, config={"container": {"auto_mount": "false"}}
            )

    def test_non_string_setting_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="runtime must be a string"):
            container_backend_from_config(
                project_root=tmp_path, config={"container": {"runtime": 7}}
            )

    def test_non_list_extra_mounts_raises(self, tmp_path: Path):
        with pytest.raises(ValueError, match="extra_mounts must be a list"):
            container_backend_from_config(
                project_root=tmp_path, config={"container": {"extra_mounts": 7}}
            )

    def test_validate_envs_rejects_a_malformed_mount_spec(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path, config={"container": {"extra_mounts": ["/a:nope"]}}
        )
        with pytest.raises(ValueError, match="neither a mode"):
            backend.validate_envs(env_names={"docker://img:1"})


# ------------------------------------------------------------------
# Failure diagnosis
# ------------------------------------------------------------------


class TestUserSelection:
    def test_docker_auto_pins_the_invoking_uid(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, runtime="docker")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert argv[argv.index("-u") + 1] == f"{os.getuid()}:{os.getgid()}"

    def test_podman_auto_passes_no_user_flag(self, tmp_path: Path):
        # Rootless Podman already maps the container's root to the invoking
        # user. Passing -u there maps into the *subordinate* uid range instead,
        # leaving outputs owned by a uid the user cannot chmod or delete --
        # worse than doing nothing.
        backend = ContainerBackend(project_root=tmp_path, runtime="podman")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert "-u" not in argv

    def test_explicit_uid_is_honoured_on_podman_too(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, runtime="podman", user="1000:1000")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert argv[argv.index("-u") + 1] == "1000:1000"

    def test_root_passes_no_user_flag(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, user="root")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert "-u" not in argv
        assert "-e" not in argv

    def test_a_home_is_supplied_alongside_an_explicit_uid(self, tmp_path: Path):
        # A uid with no passwd entry has no home either, and images that resolve
        # $HOME fail on that rather than on anything the task did.
        backend = ContainerBackend(project_root=tmp_path, user="1000:1000")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        pairs = [(argv[i], argv[i + 1]) for i, part in enumerate(argv) if part == "-e"]
        assert ("-e", "HOME=/tmp") in pairs

    def test_forwarded_names_do_not_disturb_the_home_flag(self, tmp_path: Path):
        # The two uses of -e must coexist; asserting on the *first* -e only
        # looks like it tests this.
        backend = ContainerBackend(project_root=tmp_path, user="1000:1000")
        argv = backend.exec_argv(env="docker://img:1", cmd="ls", env_vars=["GINKGO_THREADS"])
        pairs = [(argv[i], argv[i + 1]) for i, part in enumerate(argv) if part == "-e"]
        assert ("-e", "HOME=/tmp") in pairs
        assert ("-e", "GINKGO_THREADS") in pairs


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
        assert 'shell = "sh"' in hint

    def test_silent_on_a_crlf_script(self, tmp_path: Path):
        # A shell that started and then failed on a line-ending problem says
        # "No such file or directory" too. Sending its author to the shell
        # setting would cost them more time than saying nothing.
        backend = ContainerBackend(project_root=tmp_path)
        assert (
            backend.exec_failure_hint(
                env="docker://img:1",
                exit_code=126,
                output=(
                    "bash: ./run.sh: /usr/bin/python3\r: bad interpreter: "
                    "No such file or directory"
                ),
            )
            is None
        )

    def test_silent_on_a_missing_input_file(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path)
        assert (
            backend.exec_failure_hint(
                env="docker://img:1",
                exit_code=127,
                output="bash: /data/missing.fastq: No such file or directory",
            )
            is None
        )

    def test_shell_name_is_matched_as_a_token(self, tmp_path: Path):
        # "sh" is a substring of "bash"; a substring test fired here and then
        # advised setting shell to the value it already had.
        backend = ContainerBackend(project_root=tmp_path, shell="sh")
        assert (
            backend.exec_failure_hint(
                env="docker://img:1",
                exit_code=127,
                output='exec: "bash": executable file not found in $PATH',
            )
            is None
        )

    def test_suggests_a_shell_other_than_the_configured_one(self, tmp_path: Path):
        backend = ContainerBackend(project_root=tmp_path, shell="sh")
        hint = backend.exec_failure_hint(
            env="docker://distroless:latest",
            exit_code=127,
            output='exec: "sh": executable file not found in $PATH',
        )
        assert hint is not None
        assert 'shell = "sh"' not in hint

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


class TestProjectRootResolution:
    def test_project_root_is_resolved_once(self, tmp_path: Path):
        real = tmp_path / "real_project"
        real.mkdir()
        link = tmp_path / "project"
        link.symlink_to(real)

        backend = ContainerBackend(project_root=link, user="root")
        # The path mounted and the path mounts are compared against have to be
        # the same one, or every declared input gets a second read-only mount
        # laid over the read-write project mount.
        assert backend.project_root == real
        argv = backend.exec_argv(env="docker://img:1", cmd="ls")
        assert f"{real}:{real}" in argv
