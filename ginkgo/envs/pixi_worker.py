"""Pixi Python task worker — runnable as ``python -m ginkgo.envs.pixi_worker``.

The evaluator dispatches pixi-environment Python tasks by running::

    pixi run -- python -m ginkgo.envs.pixi_worker <input_path> <output_path>

This avoids the ``python -c`` inline-string approach and relies on ginkgo
being a proper installed dependency inside every pixi environment.

Dynamic results (ShellExpr, Expr, ExprList) are not JSON-serializable, so
when ``run_task`` returns one the result is pickle+base64 encoded under the
special encoding ``"pixi_direct_pickled"`` for the main process to decode.
"""

from __future__ import annotations

import base64
import json
import pathlib
import pickle
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

    from ginkgo.runtime.worker import run_task  # noqa: PLC0415

    result: dict[str, Any] = dict(run_task(payload))

    # Dynamic results cross the JSON bridge as pickle+base64.
    enc = result.get("result_encoding")
    if result.get("ok") and enc == "direct":
        result["result"] = base64.b64encode(pickle.dumps(result["result"], 5)).decode()
        result["result_encoding"] = "pixi_direct_pickled"

    output_path.write_text(json.dumps(result), encoding="utf-8")


if __name__ == "__main__":
    run(
        input_path=pathlib.Path(sys.argv[1]),
        output_path=pathlib.Path(sys.argv[2]),
    )
