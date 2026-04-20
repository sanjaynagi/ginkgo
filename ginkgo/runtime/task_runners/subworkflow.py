"""Sub-workflow task execution.

The ``SubworkflowRunner`` dispatches a ``SubWorkflowExpr`` by invoking
``ginkgo run`` as a subprocess. It reuses :class:`ShellRunner` for the
subprocess lifecycle, log plumbing, and termination-on-interrupt
guarantees.

The child run writes its own ``.ginkgo/runs/<child_id>/`` directory and
emits a machine-readable ``GINKGO_CHILD_RUN_ID=<id>`` line on stdout when
``GINKGO_CALLED_FROM_PARENT_RUN`` is set in its environment. The runner
parses that line to stitch the child ``run_id`` into the parent's
provenance.
"""

from __future__ import annotations

import os
import re
import shlex
import sys
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

from ginkgo.core.subworkflow import SubWorkflowExpr, SubWorkflowResult
from ginkgo.runtime.task_runners.shell import ShellRunner


CHILD_RUN_ID_PREFIX = "GINKGO_CHILD_RUN_ID="
DEPTH_ENV = "GINKGO_CALL_DEPTH"
PARENT_RUN_ENV = "GINKGO_CALLED_FROM_PARENT_RUN"
DEFAULT_MAX_CALL_DEPTH = 8
_CHILD_RUN_ID_RE = re.compile(r"^GINKGO_CHILD_RUN_ID=(\S+)\s*$", re.MULTILINE)


class SubWorkflowError(RuntimeError):
    """Sub-workflow subprocess returned a non-zero exit code."""

    def __init__(
        self,
        *,
        task_name: str,
        path: str,
        exit_code: int,
        child_run_id: str | None,
    ) -> None:
        self.exit_code = exit_code
        self.child_run_id = child_run_id

        details = f"Sub-workflow {task_name} ({path!r}) failed with exit code {exit_code}"
        if child_run_id is not None:
            details = f"{details} (child run: {child_run_id})"
        super().__init__(details)


class SubWorkflowRecursionError(RuntimeError):
    """Sub-workflow dispatch exceeded the configured recursion limit."""


def _parent_depth() -> int:
    raw = os.environ.get(DEPTH_ENV, "0")
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _extract_child_run_id(text: str) -> str | None:
    """Return the last ``GINKGO_CHILD_RUN_ID=<id>`` line from subprocess output."""
    matches = _CHILD_RUN_ID_RE.findall(text)
    if not matches:
        return None
    return matches[-1]


@dataclass(kw_only=True)
class SubworkflowRunner:
    """Run ``SubWorkflowExpr`` descriptors via ``ginkgo run`` subprocesses.

    Parameters
    ----------
    shell_runner : ShellRunner
        Provides ``run_logged_command`` and the shared subprocess registry
        so interrupts terminate child ``ginkgo run`` processes.
    run_id_provider : Callable[[], str]
        Returns the current parent run id; forwarded to the child via
        ``GINKGO_CALLED_FROM_PARENT_RUN`` and used in recursion diagnostics.
    runs_root : Path
        Root directory under which child run manifests are written.
    python_executable : str
        Interpreter to use for the child subprocess.
    max_depth : int
        Reject dispatch when ``GINKGO_CALL_DEPTH`` would exceed this value.
    """

    shell_runner: ShellRunner
    run_id_provider: Callable[[], str]
    runs_root: Path
    python_executable: str = field(default_factory=lambda: sys.executable)
    max_depth: int = DEFAULT_MAX_CALL_DEPTH

    def run_subworkflow(
        self,
        *,
        node: Any,
        subworkflow_expr: SubWorkflowExpr,
    ) -> SubWorkflowResult:
        """Dispatch a child ``ginkgo run`` subprocess for one sub-workflow."""
        parent_depth = _parent_depth()
        next_depth = parent_depth + 1
        if next_depth > self.max_depth:
            raise SubWorkflowRecursionError(
                f"Sub-workflow call depth {next_depth} exceeds max_depth={self.max_depth}. "
                "Check for recursive or mutually-recursive workflow calls."
            )

        workflow_path = Path(subworkflow_expr.path)
        if not workflow_path.is_absolute():
            workflow_path = Path.cwd() / workflow_path
        if not workflow_path.exists():
            raise FileNotFoundError(f"Sub-workflow path does not exist: {subworkflow_expr.path!r}")

        tmp_dir = Path(tempfile.mkdtemp(prefix="ginkgo-subworkflow-"))
        tmp_params_path: Path | None = None
        try:
            config_paths: list[str] = []
            if subworkflow_expr.params:
                tmp_params_path = tmp_dir / "params.yaml"
                tmp_params_path.write_text(
                    yaml.safe_dump(subworkflow_expr.params, sort_keys=True),
                    encoding="utf-8",
                )
                config_paths.append(str(tmp_params_path))
            config_paths.extend(subworkflow_expr.config)

            parts = [
                shlex.quote(self.python_executable),
                "-m",
                "ginkgo.cli",
                "run",
                shlex.quote(str(workflow_path)),
            ]
            for path in config_paths:
                parts.extend(["--config", shlex.quote(path)])
            cmd = " ".join(parts)

            extra_env = {
                PARENT_RUN_ENV: self.run_id_provider() or "",
                DEPTH_ENV: str(next_depth),
            }

            completed = self.shell_runner.run_logged_command(
                node=node,
                cmd=cmd,
                extra_env=extra_env,
            )

            combined = (completed.stdout or "") + (completed.stderr or "")
            child_run_id = _extract_child_run_id(combined)

            if completed.returncode != 0:
                raise SubWorkflowError(
                    task_name=node.task_def.name,
                    path=str(workflow_path),
                    exit_code=completed.returncode,
                    child_run_id=child_run_id,
                )

            if child_run_id is None:
                raise RuntimeError(
                    f"Sub-workflow {node.task_def.name} ({workflow_path!r}) exited "
                    "successfully but did not emit a GINKGO_CHILD_RUN_ID line."
                )

            manifest_path = self.runs_root / child_run_id / "manifest.yaml"
            return SubWorkflowResult(
                run_id=child_run_id,
                status="success",
                manifest_path=str(manifest_path),
            )
        finally:
            if tmp_params_path is not None:
                with suppress(FileNotFoundError, OSError):
                    tmp_params_path.unlink()
            with suppress(FileNotFoundError, OSError):
                tmp_dir.rmdir()
