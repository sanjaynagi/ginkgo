"""Reference implementation of the Pixi Python task worker.

The evaluator does NOT run this file directly as a script (doing so would add
the worker package directory to ``sys.path[0]`` and make imports fragile).

Instead, the evaluator uses ``python -c`` with inline code equivalent to
the body of :func:`run` below.  This file exists for human readability.

The actual inline code used by the evaluator is built in
:meth:`_ConcurrentEvaluator._run_pixi_python_task`.
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any


def run(input_path: pathlib.Path, output_path: pathlib.Path) -> None:
    """Execute a task payload and write the result to *output_path*.

    Parameters
    ----------
    input_path : pathlib.Path
        JSON file containing the encoded worker payload.
    output_path : pathlib.Path
        JSON file to write the encoded worker result to.
    """
    payload: dict[str, Any] = json.loads(input_path.read_bytes())

    # Inject the host sys.path so ginkgo and the workflow module are importable
    # inside the Pixi env even when ginkgo is not installed there.
    for p in payload.get("sys_path", []):
        if p not in sys.path:
            sys.path.insert(0, p)

    from ginkgo.runtime.worker import run_task  # noqa: PLC0415

    result = run_task(payload)
    output_path.write_text(json.dumps(result), encoding="utf-8")
