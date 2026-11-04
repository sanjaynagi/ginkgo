"""Remote worker entry point for Kubernetes and other remote executors.

Usage::

    python -m ginkgo.remote.worker

Reads a base64-encoded JSON worker payload from the ``GINKGO_WORKER_PAYLOAD``
environment variable, executes the task via the standard ``run_task`` worker
function, and prints the structured JSON result to stdout.

A task whose command runs in an image of its own takes a worker either side
of that image's container (see :func:`ginkgo.remote.executor_support.job_steps`),
and ``GINKGO_WORKER_PHASE`` says which: ``stage-in`` or ``stage-out``.
"""

from __future__ import annotations

import base64
import json
import os
import pickle
import shlex
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ginkgo.core.shell import shell_argv
from ginkgo.remote.executor_support import COMMAND_DIR
from ginkgo.runtime.task_runners.shell import (
    ShellRunner,
    build_shell_subprocess_env,
    computed_env_var_names,
)
from ginkgo.runtime.worker import CommandHandedOff

if TYPE_CHECKING:
    from ginkgo.envs.mounts import Mount
    from ginkgo.plugins.api import TaskInvocation
    from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore
    from ginkgo.runtime.backend import EnvironmentRouter


def main() -> None:
    """Execute a task, or one phase of it, from a remote worker payload."""
    from ginkgo.runtime.worker import error_response

    # Taken out of the environment, so neither the task nor any command it
    # runs sees the arguments and resolved secrets the payload holds.
    payload_b64 = os.environ.pop("GINKGO_WORKER_PAYLOAD", None)
    if payload_b64 is None:
        _print_result(
            error_response(RuntimeError("GINKGO_WORKER_PAYLOAD environment variable not set"))
        )
        sys.exit(1)

    try:
        payload = json.loads(base64.b64decode(payload_b64))
    except Exception as exc:
        exc.args = (f"Failed to decode worker payload: {exc}",)
        _print_result(error_response(exc))
        sys.exit(1)

    phase = os.environ.get("GINKGO_WORKER_PHASE")
    if phase == "stage-in":
        # The task's image may run as another user, and it writes its outputs
        # into the directories this worker makes.
        os.umask(0)
    try:
        result = run_worker_payload(payload, phase=phase)
    except Exception as exc:
        _print_result(error_response(exc))
        sys.exit(1)

    # Stage-in leaves the result for stage-out to print.
    if result is None:
        return
    _print_result(result)
    sys.exit(0 if result.get("ok", False) else 1)


def _print_result(result: dict) -> None:
    """Print *result* as the JSON line the executor parses from the job's logs.

    It starts a line of its own: the task's output, printed before it, may
    not end in a newline, and the executor reads only whole lines.
    """
    print(f"\n{json.dumps(result, default=str)}")


def run_worker_payload(payload: dict, *, phase: str | None = None) -> dict | None:
    """Execute one worker payload dict and return its result dict.

    The body of :func:`main`, which reads the payload and prints the result:
    code bundle install, input hydration, the task body, and output
    publishing.

    A task whose command runs in its own image is split around that image's
    container, which shares this worker's scratch root. *phase*
    ``"stage-in"`` installs the code and inputs, calls the body and leaves its
    command in :func:`_command_dir`, and returns ``None``. ``"stage-out"``
    takes the task up from the exit code and output the task's container
    recorded there, and returns the result. With no phase, the whole task
    runs here.
    """
    if phase == "stage-out":
        return _stage_out()

    from ginkgo.runtime.worker import error_response, run_task

    # Remove remote-only keys that the local worker doesn't expect.
    payload.pop("resources", None)
    image = payload.pop("image", None)
    if image is not None and phase is None:
        return error_response(
            RuntimeError(
                f"This executor ran the task in its worker image, not in {image!r}: "
                "it does not support container envs."
            )
        )
    code_bundle = payload.pop("code_bundle", None)
    remote_artifact_config = payload.pop("remote_artifact_store", None)
    output_param_entries = payload.pop("output_param_entries", None)
    payload["transport_dir"] = str(_scratch_root() / "ginkgo-transport")
    if phase == "stage-in":
        _reset_command_dir()

    mounted_access = None
    handed_off = False
    try:
        # Code-sync: download and extract the workflow package before import.
        # The bundle stands in for the driver's project root; without one,
        # the image's working directory does.
        project_root = Path.cwd()
        dest_dir = None
        if code_bundle is not None:
            dest_dir = _install_code_bundle(code_bundle)
            _rewrite_module_file(payload, code_bundle=code_bundle, dest_dir=dest_dir)
            project_root = dest_dir

        # Hydrate file / folder inputs that were uploaded to the shared
        # remote artifact store on the client side.
        remote_store = None
        if remote_artifact_config is not None:
            remote_store = _worker_remote_store(remote_artifact_config)
            _hydrate_remote_inputs(payload, remote_store=remote_store)

        # Turn declared Out[...] markers into real, writable scratch paths
        # before the task body runs — see stage_output_params_for_dispatch.
        if output_param_entries:
            _resolve_output_param_paths(payload, entries=output_param_entries)

        # Read the inputs the driver left to the worker: download or mount them.
        hydration = _hydrate_remote_markers(payload)
        mounted_access = hydration.mounted_access if hydration is not None else None

        stage = {
            "payload": payload,
            "code_bundle_dir": dest_dir,
            "remote_artifact_store": remote_artifact_config,
            "output_param_entries": output_param_entries,
            "input_digests": hydration.digests if hydration is not None else {},
        }
        backend = _prepare_task_env(payload, project_root=project_root)
        # Large values are read from and written to the shared store directly,
        # since nothing in the pod's transport directory reaches the client.
        result = run_task(
            payload,
            artifact_store=remote_store,
            backend=backend,
            shell_runner_type=_StageIn if phase == "stage-in" else None,
        )
        if result.get("handed_off"):
            handed_off = True
            # The payload holds the inputs' paths as file and folder values.
            (_command_dir() / "stage.pickle").write_bytes(pickle.dumps(stage))
            return None
        result = _finish(
            result,
            stage=stage,
            remote_store=remote_store,
            input_access=mounted_access.stats().to_dict() if mounted_access is not None else None,
        )
    except Exception as exc:
        result = error_response(exc)
    finally:
        if mounted_access is not None:
            try:
                mounted_access.close()
            except Exception:  # noqa: BLE001
                pass
        if output_param_entries and not handed_off:
            _remove_output_param_scratch(entries=output_param_entries)

    if output_param_entries and not result.get("ok"):
        _name_output_params_by_driver_path(result, entries=output_param_entries)
    if phase == "stage-in":
        (_command_dir() / "result.json").write_text(json.dumps(result, default=str))
        return None
    return result


def _stage_out() -> dict:
    """Finish a task whose command ran in its own image, from what stage-in left."""
    from ginkgo.runtime.worker import error_response, run_task

    directory = _command_dir()
    # Stage-in finished the task itself when there was no command to hand off.
    if (directory / "result.json").exists():
        return json.loads((directory / "result.json").read_text())
    if not (directory / "stage.pickle").exists():
        return error_response(RuntimeError("Stage-in left no task for stage-out to finish."))

    stage = pickle.loads((directory / "stage.pickle").read_bytes())
    entries = stage["output_param_entries"]
    try:
        if stage["code_bundle_dir"] is not None:
            sys.path.insert(0, str(stage["code_bundle_dir"]))
        remote_store = None
        if stage["remote_artifact_store"] is not None:
            remote_store = _worker_remote_store(stage["remote_artifact_store"])
        result = run_task(
            stage["payload"], artifact_store=remote_store, shell_runner_type=_StageOut
        )
        result = _finish(result, stage=stage, remote_store=remote_store, input_access=None)
    except Exception as exc:
        result = error_response(exc)
    finally:
        if entries:
            _remove_output_param_scratch(entries=entries)

    if entries and not result.get("ok"):
        _name_output_params_by_driver_path(result, entries=entries)
    return result


def _finish(
    result: dict,
    *,
    stage: dict[str, Any],
    remote_store: RemoteArtifactStore | None,
    input_access: dict[str, Any] | None,
) -> dict:
    """Record how the inputs were read, and publish the outputs the task wrote."""
    # Fold remote-input-access stats into the envelope for provenance,
    # with the digest of each input the worker downloaded.
    if input_access is not None and result.get("ok"):
        result["remote_input_access"] = input_access
    if stage["input_digests"]:
        result["remote_input_digests"] = stage["input_digests"]

    # Publish declared Out[...] outputs the task body wrote back to the
    # shared artifact store so the client can restore them at their
    # declared driver paths.
    entries = stage["output_param_entries"]
    if entries and result.get("ok") and remote_store is not None:
        _stage_output_params(result, entries=entries, remote_store=remote_store)

    # Publish produced file/folder outputs back to the shared artifact
    # store so the client can hydrate them. Skipped for failures, which
    # contain no encoded file/folder leaves.
    if remote_store is not None and result.get("ok"):
        _stage_remote_outputs(result, remote_store=remote_store, output_param_entries=entries)
    return result


def _command_dir() -> Path:
    """Return where stage-in leaves the task's command, under the shared scratch root."""
    return _scratch_root() / COMMAND_DIR


def _reset_command_dir() -> None:
    """Empty the command directory, leaving a ``command.sh`` that does nothing.

    Stage-in replaces the script when the task has a command to run, so the
    task's container always finds one.
    """
    shutil.rmtree(_command_dir(), ignore_errors=True)
    _command_dir().mkdir(parents=True)
    (_command_dir() / "command.sh").write_text("")


_RECORDED = ("stdout", "stderr", "exit_code")
"""What ``command.sh`` records of its command, each in a file of that name."""


@dataclass(kw_only=True)
class _StageIn(ShellRunner):
    """Leave a task's command for its own image to run, instead of running it here.

    ``command.sh`` runs it as the local runner runs a container env's command:
    in strict mode under ``bash`` (``sh`` when the image has no bash), with only
    the variables ginkgo computes for it. It records the command's output and exit status beside itself rather
    than exiting with it, so the job goes on to stage-out.
    """

    def run_logged_command(
        self,
        *,
        node: TaskInvocation,
        cmd: str,
        user_log_path: Path | None = None,
        extra_env: dict[str, str] | None = None,
        mounts: Sequence[Mount] = (),
    ) -> subprocess.CompletedProcess[str]:
        """Write *cmd* to ``command.sh``, and stop: this worker's part is done."""
        env = build_shell_subprocess_env(task_def=node.task_def, threads=node.threads)
        env.update(extra_env or {})
        names = [*computed_env_var_names(task_def=node.task_def), *sorted(extra_env or {})]
        recorded = {name: shlex.quote(str(_command_dir() / name)) for name in _RECORDED}
        # bash when the image has it, as a local container task's default
        # shell; otherwise sh, which must accept -o pipefail (busybox, Alpine).
        strict_args = shlex.join(shell_argv(shell="sh", cmd=cmd)[1:])
        lines = [
            *(f"export {name}={shlex.quote(env[name])}" for name in names),
            "if command -v bash >/dev/null 2>&1; then shell=bash; else shell=sh; fi",
            f'"$shell" {strict_args} > {recorded["stdout"]} 2> {recorded["stderr"]}',
            f"echo $? > {recorded['exit_code']}",
        ]
        (_command_dir() / "command.sh").write_text("\n".join(lines) + "\n", encoding="utf-8")
        raise CommandHandedOff


@dataclass(kw_only=True)
class _StageOut(ShellRunner):
    """Take a task up after its command ran in the task's own image.

    The runner finishes the directive as it does around a local command, from
    the output and exit status the task's container recorded.
    """

    def prepare_outputs(self, output: Any) -> None:
        """Leave the declared outputs: the command has written them."""

    def run_subprocess(
        self,
        *,
        argv: list[str],
        on_stdout: Any = None,
        on_stderr: Any = None,
        env: dict[str, str] | None = None,
        usage_callback: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        """Replay the command's recorded output, then exit with its exit status.

        Replayed by a process of its own, so the runner reads the output, and
        redacts it into the task's log, exactly as it reads a command's. The
        command ran elsewhere, so it has no measured usage.
        """
        directory = _command_dir()
        if not (directory / "exit_code").exists():
            raise RuntimeError(
                "The task's container recorded no exit code for its command: it did not "
                "start, its image has no sh, or it was killed."
            )
        recorded = {name: shlex.quote(str(directory / name)) for name in _RECORDED}
        replay = (
            f"cat {recorded['stdout']}; cat {recorded['stderr']} >&2; "
            f"exit $(cat {recorded['exit_code']})"
        )
        return super().run_subprocess(
            argv=["sh", "-c", replay], on_stdout=on_stdout, on_stderr=on_stderr
        )


def _install_code_bundle(code_bundle: dict[str, str]):
    """Download and extract a code bundle, prepending it to sys.path."""
    from ginkgo.remote.code_bundle import download_and_extract
    from ginkgo.remote.resolve import resolve_backend

    scheme = code_bundle["scheme"]
    bucket = code_bundle["bucket"]
    key = code_bundle["key"]

    backend = resolve_backend(scheme)
    dest_dir = _scratch_root() / "ginkgo-code-bundle"
    download_and_extract(
        backend=backend,
        bucket=bucket,
        key=key,
        dest_dir=dest_dir,
    )
    sys.path.insert(0, str(dest_dir))
    return dest_dir


def _prepare_task_env(payload: dict, *, project_root: Path) -> EnvironmentRouter | None:
    """Install the env the driver's cache key names, and return its backend.

    The env is found as the driver finds it, under *project_root*, which
    carries each env's manifest and lock. Its first use in a worker installs
    it. Returns ``None`` when the driver named no env: the task declares
    none, or the driver had no environment backend to run one in.

    Raises
    ------
    RuntimeError
        If the env found here is not the one the driver named.
    """
    from ginkgo.envs.container import is_container_env

    expected = payload.get("env_identity")
    # A container env's command runs in its own image, not in an env here.
    if expected is None or is_container_env(payload["env"]):
        return None

    from ginkgo.cli.workspace import resolve_envs_workflow_root
    from ginkgo.plugins.api import EnvironmentContext
    from ginkgo.runtime.backend import EnvironmentRouter

    env = payload["env"]
    module_file = payload.get("module_file")
    backend = EnvironmentRouter(
        context=EnvironmentContext(
            project_root=project_root,
            workflow_root=resolve_envs_workflow_root(
                project_root=project_root,
                fallback=Path(module_file).parent if module_file else None,
            ),
            config={},
        )
    )
    identity = backend.env_identity(env=env)
    if identity != expected:
        raise RuntimeError(
            f"The worker's env {env!r} is not the one the driver's cache key names "
            f"(its identity is {identity}, the driver's {expected}). Ship the "
            "driver's manifest in the code bundle, or rebuild the worker image."
        )
    backend.prepare(env=env)
    return backend


def _scratch_root():
    """Return the base scratch directory for worker temp files.

    Checks ``$GINKGO_SCRATCH_ROOT``, then ``$TMPDIR``, then ``/tmp``.
    """
    from pathlib import Path

    for var in ("GINKGO_SCRATCH_ROOT", "TMPDIR"):
        val = os.environ.get(var)
        if val:
            return Path(val)
    return Path("/tmp")


def _stage_remote_outputs(
    result: dict,
    *,
    remote_store: RemoteArtifactStore,
    output_param_entries: list[dict[str, str]] | None = None,
) -> None:
    """Upload encoded file/folder outputs to the shared remote store."""
    from ginkgo.runtime.artifacts.remote_arg_transfer import stage_result_for_remote

    output_param_paths = None
    if output_param_entries:
        scratch_dir = _scratch_root() / "ginkgo-outputs"
        output_param_paths = {
            str(scratch_dir / entry["relative"]): entry["original"]
            for entry in output_param_entries
        }
    result["result"] = stage_result_for_remote(
        result=result["result"],
        remote_store=remote_store,
        output_param_paths=output_param_paths,
    )


def _resolve_output_param_paths(payload: dict, *, entries: list[dict[str, str]]) -> None:
    """Rewrite declared ``Out[...]`` markers into absolute pod-local paths."""
    from ginkgo.runtime.artifacts.remote_arg_transfer import resolve_output_param_scratch_paths

    scratch_dir = _scratch_root() / "ginkgo-outputs"
    payload["args"] = resolve_output_param_scratch_paths(
        args=payload.get("args", {}), entries=entries, scratch_dir=scratch_dir
    )


def _remove_output_param_scratch(*, entries: list[dict[str, str]]) -> None:
    """Delete this dispatch's ``Out[...]`` scratch directory once uploaded.

    A long-lived worker would otherwise keep every output it ever wrote.
    """
    import shutil

    scratch_dir = _scratch_root() / "ginkgo-outputs"
    for dispatch_dir in {entry["relative"].split("/", 1)[0] for entry in entries}:
        shutil.rmtree(scratch_dir / dispatch_dir, ignore_errors=True)


def _stage_output_params(
    result: dict, *, entries: list[dict[str, str]], remote_store: RemoteArtifactStore
) -> None:
    """Upload each declared ``Out[...]`` output the task body wrote."""
    from ginkgo.runtime.artifacts.remote_arg_transfer import stage_output_param_results

    result["output_param_results"] = stage_output_param_results(
        entries=entries, remote_store=remote_store, scratch_dir=_scratch_root() / "ginkgo-outputs"
    )


def _name_output_params_by_driver_path(result: dict, *, entries: list[dict[str, str]]) -> None:
    """Name each ``Out[...]`` path in a failure's error by its declared driver path.

    The task ran against scratch paths, which mean nothing on the driver. A
    failure that names one, such as a runner's check that its command created
    the output, names the declared path instead, as a local task's would.
    """
    scratch_dir = _scratch_root() / "ginkgo-outputs"
    error = result["error"]
    for entry in entries:
        scratch, original = str(scratch_dir / entry["relative"]), entry["original"]
        error["message"] = error["message"].replace(scratch, original)
        error["args"] = [arg.replace(scratch, original) for arg in error["args"]]


def _hydrate_remote_inputs(payload: dict, *, remote_store: RemoteArtifactStore) -> None:
    """Download remote-staged ``file`` / ``folder`` inputs into the pod."""
    from ginkgo.runtime.artifacts.remote_arg_transfer import hydrate_args_from_remote

    payload["args"] = hydrate_args_from_remote(
        args=payload.get("args", {}),
        remote_store=remote_store,
        scratch_dir=_scratch_root() / "ginkgo-inputs",
    )


def _worker_remote_store(config: dict[str, str]) -> RemoteArtifactStore:
    """Build the shared artifact store, with its local CAS in the pod's scratch."""
    from ginkgo.runtime.artifacts.remote_arg_transfer import build_worker_remote_store

    return build_worker_remote_store(
        scheme=config["scheme"],
        bucket=config["bucket"],
        prefix=config["prefix"],
        local_root=_scratch_root() / "ginkgo-remote-cas",
    )


def _hydrate_remote_markers(payload: dict):
    """Read the inputs the driver left to the worker, replacing markers with paths.

    A ``direct`` input is downloaded into the pod's scratch area, the run's
    ``staging_jobs`` at a time; a ``fuse`` input is mounted. Returns the
    :class:`Hydration`, holding the mounts for the caller to close and the
    downloads' digests, or ``None`` when the payload has no markers.
    """
    args = payload.get("args")
    if not isinstance(args, dict) or not _payload_has_remote_markers(args):
        return None

    from ginkgo.remote.access.worker_hydration import hydrate_remote_inputs
    from ginkgo.remote.staging import DEFAULT_STAGING_JOBS

    hydration = hydrate_remote_inputs(
        args=args,
        staging_root=_scratch_root() / "ginkgo-staging",
        jobs=payload.get("staging_jobs") or DEFAULT_STAGING_JOBS,
    )
    payload["args"] = hydration.args
    return hydration


def _payload_has_remote_markers(value) -> bool:
    """Shallow/recursive scan for a remote-input marker."""
    from ginkgo.remote.access.protocol import is_remote_input

    if is_remote_input(value):
        return True
    if isinstance(value, dict):
        return any(_payload_has_remote_markers(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_payload_has_remote_markers(v) for v in value)
    return False


def _rewrite_module_file(payload: dict, *, code_bundle: dict[str, str], dest_dir) -> None:
    """Rewrite payload['module_file'] to the extracted bundle path.

    The payload carries the host-side absolute path to the workflow
    module. After extracting the code bundle inside the pod, the file
    lives at ``dest_dir/<relative path from package parent>``.
    """
    from pathlib import Path

    module_file = payload.get("module_file")
    package_parent = code_bundle.get("package_parent")
    if not module_file or not package_parent:
        return

    try:
        relative = Path(module_file).resolve().relative_to(Path(package_parent).resolve())
    except ValueError:
        return

    payload["module_file"] = str(dest_dir / relative)


if __name__ == "__main__":
    main()
