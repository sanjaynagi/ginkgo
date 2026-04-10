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

    # Code-sync: download and extract the workflow package before import.
    if code_bundle is not None:
        _install_code_bundle(code_bundle)

    from ginkgo.runtime.worker import run_task

    result = run_task(payload)

    # Print the result as a JSON line for the handle to parse.
    print(json.dumps(result, default=str))
    sys.exit(0 if result.get("ok", False) else 1)


def _install_code_bundle(code_bundle: dict[str, str]) -> None:
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


if __name__ == "__main__":
    main()
