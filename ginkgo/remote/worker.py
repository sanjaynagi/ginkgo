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


def main() -> None:
    """Execute a task from a remote worker payload."""
    payload_b64 = os.environ.get("GINKGO_WORKER_PAYLOAD")
    if payload_b64 is None:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "type": "RuntimeError",
                        "module": "builtins",
                        "message": "GINKGO_WORKER_PAYLOAD environment variable not set",
                        "args": ["GINKGO_WORKER_PAYLOAD environment variable not set"],
                    },
                }
            )
        )
        sys.exit(1)

    try:
        payload = json.loads(base64.b64decode(payload_b64))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "type": type(exc).__name__,
                        "module": type(exc).__module__,
                        "message": f"Failed to decode worker payload: {exc}",
                        "args": [str(exc)],
                    },
                }
            )
        )
        sys.exit(1)

    # Remove remote-only keys that the local worker doesn't expect.
    payload.pop("resources", None)
    code_bundle = payload.pop("code_bundle", None)
    remote_artifact_config = payload.pop("remote_artifact_store", None)

    try:
        # Code-sync: download and extract the workflow package before import.
        if code_bundle is not None:
            dest_dir = _install_code_bundle(code_bundle)
            _rewrite_module_file(payload, code_bundle=code_bundle, dest_dir=dest_dir)

        # Hydrate file / folder inputs that were uploaded to the shared
        # remote artifact store on the client side.
        if remote_artifact_config is not None:
            _hydrate_remote_inputs(payload, config=remote_artifact_config)

        from ginkgo.runtime.worker import run_task

        result = run_task(payload)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": {
                        "type": type(exc).__name__,
                        "module": type(exc).__module__,
                        "message": str(exc),
                        "args": [str(a) for a in exc.args],
                    },
                }
            )
        )
        sys.exit(1)

    # Print the result as a JSON line for the handle to parse.
    print(json.dumps(result, default=str))
    sys.exit(0 if result.get("ok", False) else 1)


def _install_code_bundle(code_bundle: dict[str, str]):
    """Download and extract a code bundle, prepending it to sys.path."""
    from pathlib import Path
    from ginkgo.remote.code_bundle import download_and_extract
    from ginkgo.remote.resolve import resolve_backend

    scheme = code_bundle["scheme"]
    bucket = code_bundle["bucket"]
    key = code_bundle["key"]

    backend = resolve_backend(scheme)
    dest_dir = Path("/tmp/ginkgo-code-bundle")
    download_and_extract(
        backend=backend,
        bucket=bucket,
        key=key,
        dest_dir=dest_dir,
    )
    sys.path.insert(0, str(dest_dir))
    return dest_dir


def _hydrate_remote_inputs(payload: dict, *, config: dict[str, str]) -> None:
    """Download remote-staged ``file`` / ``folder`` inputs into the pod."""
    from pathlib import Path
    from ginkgo.runtime.artifacts.remote_staging import (
        build_worker_remote_store,
        hydrate_args_from_remote,
    )

    local_root = Path("/tmp/ginkgo-remote-cas")
    scratch_dir = Path("/tmp/ginkgo-inputs")
    remote_store = build_worker_remote_store(
        scheme=config["scheme"],
        bucket=config["bucket"],
        prefix=config["prefix"],
        local_root=local_root,
    )
    payload["args"] = hydrate_args_from_remote(
        args=payload.get("args", {}),
        remote_store=remote_store,
        scratch_dir=scratch_dir,
    )


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
