"""Remote worker entry point for Kubernetes and other remote executors.

Usage::

    python -m ginkgo.remote.worker

Reads a base64-encoded JSON worker payload from the ``GINKGO_WORKER_PAYLOAD``
environment variable, executes the task via the standard ``run_task`` worker
function, and prints the structured JSON result to stdout.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore


def main() -> None:
    """Execute a task from a remote worker payload."""
    from ginkgo.runtime.worker import error_response

    payload_b64 = os.environ.get("GINKGO_WORKER_PAYLOAD")
    if payload_b64 is None:
        print(
            json.dumps(
                error_response(RuntimeError("GINKGO_WORKER_PAYLOAD environment variable not set"))
            )
        )
        sys.exit(1)

    try:
        payload = json.loads(base64.b64decode(payload_b64))
    except Exception as exc:
        exc.args = (f"Failed to decode worker payload: {exc}",)
        print(json.dumps(error_response(exc)))
        sys.exit(1)

    try:
        result = run_worker_payload(payload)
    except Exception as exc:
        print(json.dumps(error_response(exc)))
        sys.exit(1)

    # Print the result as a JSON line for the handle to parse.
    print(json.dumps(result, default=str))
    sys.exit(0 if result.get("ok", False) else 1)


def run_worker_payload(payload: dict) -> dict:
    """Execute one worker payload dict and return its result dict.

    The pure body of :func:`main`, split out so tests (and in-process fake
    executors) can drive a remote task end to end — code bundle install,
    input hydration, the task body, and output publishing — without the
    ``GINKGO_WORKER_PAYLOAD`` environment variable or process exit calls.
    """
    from ginkgo.runtime.worker import error_response

    # Remove remote-only keys that the local worker doesn't expect.
    payload.pop("resources", None)
    code_bundle = payload.pop("code_bundle", None)
    remote_artifact_config = payload.pop("remote_artifact_store", None)
    output_param_entries = payload.pop("output_param_entries", None)
    payload["transport_dir"] = str(_scratch_root() / "ginkgo-transport")

    mounted_access = None
    try:
        # Code-sync: download and extract the workflow package before import.
        if code_bundle is not None:
            dest_dir = _install_code_bundle(code_bundle)
            _rewrite_module_file(payload, code_bundle=code_bundle, dest_dir=dest_dir)

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

        from ginkgo.runtime.worker import run_task

        # Large values are read from and written to the shared store directly,
        # since nothing in the pod's transport directory reaches the client.
        result = run_task(payload, artifact_store=remote_store)

        # Fold remote-input-access stats into the envelope for provenance,
        # with the digest of each input the worker downloaded.
        if mounted_access is not None and result.get("ok"):
            result["remote_input_access"] = mounted_access.stats().to_dict()
        if hydration is not None and hydration.digests:
            result["remote_input_digests"] = hydration.digests

        # Publish declared Out[...] outputs the task body wrote back to the
        # shared artifact store so the client can restore them at their
        # declared driver paths.
        if output_param_entries and result.get("ok") and remote_store is not None:
            _stage_output_params(result, entries=output_param_entries, remote_store=remote_store)

        # Publish produced file/folder outputs back to the shared artifact
        # store so the client can hydrate them. Skipped for dynamic results
        # and failures — both contain no encoded file/folder leaves.
        if (
            remote_store is not None
            and result.get("ok")
            and result.get("result_encoding") == "encoded"
        ):
            _stage_remote_outputs(
                result, remote_store=remote_store, output_param_entries=output_param_entries
            )
    except Exception as exc:
        if mounted_access is not None:
            try:
                mounted_access.close()
            except Exception:  # noqa: BLE001
                pass
        return error_response(exc)
    finally:
        if mounted_access is not None:
            try:
                mounted_access.close()
            except Exception:  # noqa: BLE001
                pass
        if output_param_entries:
            _remove_output_param_scratch(entries=output_param_entries)

    return result


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

    from ginkgo.remote.access.worker_hydration import (
        DEFAULT_HYDRATION_JOBS,
        hydrate_remote_inputs,
    )

    hydration = hydrate_remote_inputs(
        args=args,
        staging_root=_scratch_root() / "ginkgo-staging",
        jobs=payload.get("staging_jobs") or DEFAULT_HYDRATION_JOBS,
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
