"""Tests for container bind-mount resolution, user flags and environment forwarding."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from ginkgo import Out, task
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


def _argv(tmp_path: Path, *, env_vars=(), **backend) -> list[str]:
    backend.setdefault("user", "root")
    return ContainerBackend(project_root=tmp_path, **backend).exec_argv(
        env="docker://img:1", cmd="ls", env_vars=env_vars
    )


def _pairs(argv: list[str], flag: str) -> list[str]:
    return [argv[i + 1] for i, part in enumerate(argv) if part == flag]


# ------------------------------------------------------------------
# Mount specifications
# ------------------------------------------------------------------


def test_a_bare_extra_mount_is_read_only_and_configured() -> None:
    # The origin is what stops a derived mount widening a user's "ro".
    assert parse_extra_mount("/scratch") == Mount(
        host_path=Path("/scratch"), container_path=Path("/scratch"), mode="ro", origin="configured"
    )


@pytest.mark.parametrize(
    ("spec", "container_path", "mode"),
    [
        ("/scratch:rw", "/scratch", "rw"),
        ("/opt/refs:/refs:ro", "/refs", "ro"),
        ("/opt/refs:/refs", "/refs", "ro"),
    ],
)
def test_extra_mount_fields(spec: str, container_path: str, mode: str) -> None:
    parsed = parse_extra_mount(spec)
    assert (parsed.container_path, parsed.mode) == (Path(container_path), mode)


@pytest.mark.parametrize(
    ("spec", "match"),
    [
        ("/scratch:/mnt:write", "Invalid container mount mode"),
        ("", "Empty container mount"),
        # Neither a mode nor an absolute path: both guesses would be silently wrong.
        ("/scratch:nope", "neither a mode"),
        ("/a:/b:rw:extra", "Invalid container mount specification"),
    ],
)
def test_malformed_extra_mounts_are_refused(spec: str, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        parse_extra_mount(spec)


# ------------------------------------------------------------------
# Mount resolution
# ------------------------------------------------------------------


@pytest.fixture
def project(tmp_path: Path) -> Path:
    path = tmp_path / "project"
    path.mkdir()
    return path


class TestResolveMounts:
    def test_paths_inside_the_project_are_dropped_and_outside_kept(self, tmp_path, project):
        inside = project / "reads.fastq"
        inside.write_text("@r\n")
        outside = tmp_path / "data"
        outside.mkdir()

        resolved = resolve_mounts(project_root=project, mounts=[mount(inside), mount(outside)])
        assert [item.host_path for item in resolved] == [outside]

    def test_symlink_mounts_real_path_at_declared_path(self, tmp_path, project):
        real = tmp_path / "store"
        real.mkdir()
        link = project / "store"
        link.symlink_to(real)

        (resolved,) = resolve_mounts(project_root=project, mounts=[mount(link)])
        # The command dereferences the link, so the real path is what is
        # mounted, at the path the command names.
        assert (resolved.host_path, resolved.container_path) == (real, link)

    def test_descendants_collapse_into_a_same_mode_ancestor_only(self, tmp_path, project):
        parent = tmp_path / "data"
        (parent / "out").mkdir(parents=True)
        (parent / "reads.fastq").write_text("@r\n")

        collapsed = resolve_mounts(
            project_root=project, mounts=[mount(parent), mount(parent / "reads.fastq")]
        )
        assert [item.host_path for item in collapsed] == [parent]
        kept = resolve_mounts(
            project_root=project, mounts=[mount(parent), mount(parent / "out", mode="rw")]
        )
        assert {(item.host_path, item.mode) for item in kept} == {
            (parent, "ro"),
            (parent / "out", "rw"),
        }

    def test_read_write_wins_for_the_same_path(self, tmp_path, project):
        shared = tmp_path / "shared"
        shared.mkdir()
        resolved = resolve_mounts(
            project_root=project, mounts=[mount(shared), mount(shared, mode="rw")]
        )
        assert [item.mode for item in resolved] == ["rw"]

    def test_an_absent_path_is_skipped_if_declared_and_refused_if_configured(
        self, tmp_path, project
    ):
        # Mounting it would make the runtime create the host path; the command
        # that needs it gives the better error.
        gone = tmp_path / "gone"
        assert resolve_mounts(project_root=project, mounts=[mount(gone)]) == []
        with pytest.raises(MissingMountError, match="does not exist"):
            resolve_mounts(project_root=project, mounts=[parse_extra_mount(str(gone))])

    @pytest.mark.parametrize(
        ("target", "match"),
        [
            ("/", "filesystem root"),
            ("/etc", "system directory"),
            # On macOS /etc resolves to /private/etc; both spellings are refused.
            ("/private/etc" if Path("/private/etc").exists() else "/etc", "system directory"),
            # $HOME would hand the image ~/.ssh and ~/.aws along with an output.
            (str(Path.home()), "home directory"),
        ],
    )
    def test_unsafe_paths_are_refused(self, tmp_path, target: str, match: str):
        with pytest.raises(UnsafeMountError, match=match):
            resolve_mounts(project_root=tmp_path, mounts=[mount(target, mode="rw")])

    def test_configured_read_only_is_not_widened_by_a_derived_output(self, tmp_path, project):
        refs = tmp_path / "refs"
        refs.mkdir()
        with pytest.raises(MountModeConflictError, match="read-only"):
            resolve_mounts(
                project_root=project,
                mounts=[parse_extra_mount(f"{refs}:ro"), mount(refs, mode="rw")],
            )

    def test_a_symlinked_project_root_is_resolved_once(self, tmp_path: Path):
        # Otherwise every input inside the project is mounted a second time,
        # read-only, over the read-write project mount.
        real = tmp_path / "real_project"
        (real / "data").mkdir(parents=True)
        (real / "data" / "in.txt").write_text("x\n")
        link = tmp_path / "project"
        link.symlink_to(real)

        backend = ContainerBackend(project_root=link, user="root")
        assert backend.project_root == real
        assert f"{real}:{real}" in backend.exec_argv(env="docker://img:1", cmd="ls")
        assert (
            resolve_mounts(project_root=link.resolve(), mounts=[mount(real / "data/in.txt")]) == []
        )


# ------------------------------------------------------------------
# Deriving mounts from a task
# ------------------------------------------------------------------


@task(kind="shell")
def reads_inputs(reads: file, refs: folder, sample: str, threads: int) -> file:
    raise AssertionError("never runs")


@task(kind="shell")
def reads_many(reads: list[file], index: file | None, work: tmp_dir) -> file:
    raise AssertionError("never runs")


@task(kind="shell")
def writes_bam(bam: Out[file]) -> file:
    raise AssertionError("never runs")


@task(kind="shell", export_thread_env=True)
def exports_threads() -> file:
    raise AssertionError("never runs")


def _mounts(task_def, **resolved_args) -> set[tuple[Path, str]]:
    node = SimpleNamespace(task_def=task_def, resolved_args=resolved_args)
    return {(item.host_path, item.mode) for item in declared_input_mounts(node=node)}


class TestDeclaredInputMounts:
    def test_path_inputs_mount_read_only_and_others_are_ignored(self, tmp_path: Path):
        reference = tmp_path / "ref" / "ref.fa"
        reference.parent.mkdir()
        reference.write_text(">chr1\n")
        refs = tmp_path / "refs"
        refs.mkdir()

        # A file mounts its directory, so an index beside it (ref.fa.fai) is
        # visible; a folder mounts itself; plain values mount nothing.
        assert _mounts(
            reads_inputs,
            reads=file(str(reference)),
            refs=folder(str(refs)),
            sample=str(tmp_path),
            threads=4,
        ) == {(reference.parent, "ro"), (refs, "ro")}

    def test_nested_optional_and_scratch_values(self, tmp_path: Path):
        first, second = tmp_path / "a.fastq", tmp_path / "b.fastq"
        for path in (first, second):
            path.write_text("@r\n")
        scratch = tmp_path / "scratch"
        scratch.mkdir()

        assert _mounts(
            reads_many,
            reads=[file(str(first)), file(str(second))],
            index=None,
            work=tmp_dir(str(scratch)),
        ) == {(tmp_path, "ro"), (scratch, "rw")}

    def test_missing_input_is_skipped(self, tmp_path: Path):
        assert _mounts(reads_inputs, reads=file(str(tmp_path / "absent.fastq"))) == set()

    @pytest.mark.parametrize("exists", [False, True])
    def test_an_out_parameter_mounts_its_parent_read_write(self, tmp_path: Path, exists: bool):
        # Unlike a read input, a not-yet-written output still has to be mounted.
        bam = tmp_path / "results" / "out.bam"
        if exists:
            bam.parent.mkdir()
            bam.write_text("data")
        assert _mounts(writes_bam, bam=str(bam)) == {(bam.parent, "rw")}


def test_declared_outputs_mount_their_parents_read_write(tmp_path: Path):
    first, second = tmp_path / "one" / "a.txt", tmp_path / "two" / "b.txt"
    mounts = declared_output_mounts(output=[str(first), str(second)])
    assert {(item.host_path, item.mode) for item in mounts} == {
        (first.parent, "rw"),
        (second.parent, "rw"),
    }
    assert declared_output_mounts(output=None) == []


# ------------------------------------------------------------------
# Environment forwarding and user selection
# ------------------------------------------------------------------


def test_thread_variables_are_forwarded_by_name_only(tmp_path: Path):
    assert computed_env_var_names(task_def=writes_bam) == ("GINKGO_THREADS",)
    names = computed_env_var_names(task_def=exports_threads)
    assert {"OMP_NUM_THREADS", "GINKGO_THREADS"} <= set(names)

    argv = _argv(tmp_path, env_vars=["GINKGO_THREADS", "OMP_NUM_THREADS"])
    # A bare -e NAME resolves from the client's own environment, so no value
    # reaches the host process table.
    assert _pairs(argv, "-e") == ["GINKGO_THREADS", "OMP_NUM_THREADS"]
    assert "-e" not in _argv(tmp_path)


@pytest.mark.parametrize(
    ("backend", "user_flag"),
    [
        ({"runtime": "docker", "user": "auto"}, f"{os.getuid()}:{os.getgid()}"),
        # Rootless Podman already maps root to the invoking user; -u would map
        # into the subordinate range and leave outputs the user cannot delete.
        ({"runtime": "podman", "user": "auto"}, None),
        ({"runtime": "podman", "user": "1000:1000"}, "1000:1000"),
        ({"user": "root"}, None),
    ],
)
def test_the_container_user(tmp_path: Path, backend: dict, user_flag: str | None):
    argv = _argv(tmp_path, **backend)
    assert _pairs(argv, "-u") == ([user_flag] if user_flag else [])


def test_an_explicit_uid_gets_a_home_beside_forwarded_names(tmp_path: Path):
    # A uid with no passwd entry has no $HOME, which images that resolve it fail on.
    argv = _argv(tmp_path, user="1000:1000", env_vars=["GINKGO_THREADS"])
    assert _pairs(argv, "-e") == ["HOME=/tmp", "GINKGO_THREADS"]


def test_an_invalid_user_is_refused(tmp_path: Path):
    with pytest.raises(ValueError, match="Invalid \\[container\\] user"):
        _argv(tmp_path, user="nobody")


# ------------------------------------------------------------------
# Config table
# ------------------------------------------------------------------


class TestContainerBackendFromConfig:
    def test_defaults_and_every_setting(self, tmp_path: Path):
        default = container_backend_from_config(project_root=tmp_path, config={})
        assert (default.runtime, default.pull_policy, default.user, default.shell) == (
            "docker",
            "if-not-present",
            "auto",
            "bash",
        )
        assert default.auto_mount is True and default.extra_mounts == ()

        table = {
            "runtime": "podman",
            "pull_policy": "always",
            "user": "root",
            "shell": "sh",
            "auto_mount": False,
            "extra_mounts": ["/scratch:rw"],
        }
        backend = container_backend_from_config(project_root=tmp_path, config={"container": table})
        assert {key: getattr(backend, key) for key in table} == {
            **table,
            "extra_mounts": ("/scratch:rw",),
        }
        single = container_backend_from_config(
            project_root=tmp_path, config={"container": {"extra_mounts": "/scratch"}}
        )
        assert single.extra_mounts == ("/scratch",)

    @pytest.mark.parametrize(
        ("table", "match"),
        [
            ("docker", r"\[container\] must be a table"),
            # A typo must not leave the run quietly on defaults.
            ({"extra_mount": ["/a"]}, "unknown keys"),
            # bool("false") is True, so coercing would invert what the file says.
            ({"auto_mount": "false"}, "must be true or false"),
            ({"runtime": 7}, "runtime must be a string"),
            ({"extra_mounts": 7}, "extra_mounts must be a list"),
            ({"pull_policy": "nevr"}, r"\[container\] pull_policy must be one of"),
            ({"runtime": "dockr"}, r"\[container\] runtime must be one of"),
        ],
    )
    def test_bad_settings_are_refused(self, tmp_path: Path, table, match: str):
        with pytest.raises(ValueError, match=match):
            container_backend_from_config(project_root=tmp_path, config={"container": table})

    def test_validate_envs_rejects_a_malformed_mount_spec(self, tmp_path: Path):
        backend = container_backend_from_config(
            project_root=tmp_path, config={"container": {"extra_mounts": ["/a:nope"]}}
        )
        with pytest.raises(ValueError, match="neither a mode"):
            backend.validate_envs(env_names={"docker://img:1"})


# ------------------------------------------------------------------
# Failure diagnosis
# ------------------------------------------------------------------

_BASH_MISSING = 'exec: "bash": executable file not found in $PATH'


def test_a_missing_shell_is_named_with_the_setting_to_change(tmp_path: Path):
    hint = ContainerBackend(project_root=tmp_path).exec_failure_hint(
        env="docker://alpine:3.20", exit_code=127, output=_BASH_MISSING
    )
    assert hint is not None
    for phrase in ("alpine:3.20", "[container] shell", 'shell = "sh"'):
        assert phrase in hint

    # It never suggests the shell already configured.
    hint = ContainerBackend(project_root=tmp_path, shell="sh").exec_failure_hint(
        env="docker://distroless:latest",
        exit_code=127,
        output='exec: "sh": executable file not found in $PATH',
    )
    assert hint is not None and 'shell = "sh"' not in hint


@pytest.mark.parametrize(
    ("shell", "exit_code", "output"),
    [
        # A shell that started, then failed on CRLF line endings, says
        # "No such file or directory" too; sending its author to the shell
        # setting would cost them more than silence.
        ("bash", 126, "bash: ./run.sh: /usr/bin/python3\r: bad interpreter: No such file"),
        ("bash", 127, "bash: /data/missing.fastq: No such file or directory"),
        # "sh" is a substring of "bash": it must match as a token.
        ("sh", 127, _BASH_MISSING),
        ("bash", 1, "samtools: no such reference"),
        ("bash", 127, "minimap2: command not found"),
    ],
    ids=["crlf", "missing-input", "substring", "tool-failure", "tool-not-found"],
)
def test_no_hint_when_the_shell_is_not_what_failed(tmp_path, shell, exit_code, output):
    backend = ContainerBackend(project_root=tmp_path, shell=shell)
    assert (
        backend.exec_failure_hint(env="docker://img:1", exit_code=exit_code, output=output) is None
    )
