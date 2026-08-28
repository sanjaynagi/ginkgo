"""Sub-workflow task execution.

The ``SubworkflowRunner`` dispatches a ``SubWorkflowDirective`` by invoking
``ginkgo run`` as a subprocess. It reuses :class:`ShellRunner` for the
subprocess lifecycle, log plumbing, and termination-on-interrupt
guarantees.

The child is told who called it through ``GINKGO_PARENT_RUN_ID`` and
``GINKGO_PARENT_TASK_ID``; it records both on its own ``RunStarted``, which
puts the link in the ledger. The parent then reads the child's run id back out
of the store rather than scraping it from the child's output — a run id that
only ever existed as a line of stdout was one interleaved log line away from
being lost.
"""

from __future__ import annotations

import os
import shlex
import sys
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

from ginkgo.config import PARAMS_CONFIG_KEY
from ginkgo.core.subworkflow import SubWorkflowDirective, SubWorkflowResult
from ginkgo.envs.mounts import mount
from ginkgo.errors import GinkgoError
from ginkgo.runtime.events import task_id_for_node
from ginkgo.runtime.task_runners.shell import ShellRunner
from ginkgo.store.sqlite import open_store


DEPTH_ENV = "GINKGO_CALL_DEPTH"
PARENT_RUN_ID_ENV = "GINKGO_PARENT_RUN_ID"
PARENT_TASK_ID_ENV = "GINKGO_PARENT_TASK_ID"
DEFAULT_MAX_CALL_DEPTH = 8


class SubWorkflowError(GinkgoError, RuntimeError):
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


@dataclass(kw_only=True)
class SubworkflowRunner:
    """Run ``SubWorkflowDirective`` descriptors via ``ginkgo run`` subprocesses.

    Parameters
    ----------
    shell_runner : ShellRunner
        Provides ``run_logged_command`` and the shared subprocess registry
        so interrupts terminate child ``ginkgo run`` processes.
    run_id_provider : Callable[[], str]
        Returns the current parent run id; forwarded to the child via
        ``GINKGO_PARENT_RUN_ID`` and used in recursion diagnostics.
    runs_root : Path
        Root directory under which child run manifests are written.
    db_path : Path
        The provenance database, read to resolve which run the child was.
    python_executable : str
        Interpreter to use for the child subprocess.
    max_depth : int
        Reject dispatch when ``GINKGO_CALL_DEPTH`` would exceed this value.
    """

    shell_runner: ShellRunner
    run_id_provider: Callable[[], str]
    runs_root: Path
    db_path: Path
    python_executable: str = field(default_factory=lambda: sys.executable)
    max_depth: int = DEFAULT_MAX_CALL_DEPTH

    def run_subworkflow(
        self,
        *,
        node: Any,
        directive: SubWorkflowDirective,
    ) -> SubWorkflowResult:
        """Dispatch a child ``ginkgo run`` subprocess for one sub-workflow."""
        parent_depth = _parent_depth()
        next_depth = parent_depth + 1
        if next_depth > self.max_depth:
            raise SubWorkflowRecursionError(
                f"Sub-workflow call depth {next_depth} exceeds max_depth={self.max_depth}. "
                "Check for recursive or mutually-recursive workflow calls."
            )

        workflow_path = Path(directive.path)
        if not workflow_path.is_absolute():
            workflow_path = Path.cwd() / workflow_path
        if not workflow_path.exists():
            raise FileNotFoundError(f"Sub-workflow path does not exist: {directive.path!r}")

        tmp_dir = Path(tempfile.mkdtemp(prefix="ginkgo-subworkflow-"))
        tmp_params_path: Path | None = None
        try:
            config_paths: list[str] = []
            if directive.params:
                tmp_params_path = tmp_dir / "params.yaml"
                # Written as a [params] table so the child resolves them through
                # ginkgo.param() like any other parameter source. The table layers
                # over the child's own, so a parameter the parent does not pass
                # keeps whatever the child's config set.
                tmp_params_path.write_text(
                    yaml.safe_dump(
                        {PARAMS_CONFIG_KEY: dict(directive.params)},
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
                config_paths.append(str(tmp_params_path))
            config_paths.extend(directive.config)

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

            parent_task_id = task_id_for_node(node.node_id)
            extra_env = {
                PARENT_RUN_ID_ENV: self.run_id_provider() or "",
                PARENT_TASK_ID_ENV: parent_task_id,
                DEPTH_ENV: str(next_depth),
            }

            completed = self.shell_runner.run_logged_command(
                node=node,
                cmd=cmd,
                extra_env=extra_env,
                # The params file lives in a scratch directory outside the
                # project, so an isolated environment needs it named to see the
                # --config path the command just referenced.
                mounts=[mount(tmp_dir, mode="ro")] if tmp_params_path is not None else [],
            )

            child_run_id = self._child_run_id(parent_task_id=parent_task_id)

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
                    f"successfully but recorded no run in {self.db_path}."
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

    def _child_run_id(self, *, parent_task_id: str) -> str | None:
        """Return the run the child process recorded for this task, if any.

        The newest wins: a retried sub-workflow task starts a fresh child run
        each attempt, and the one that just exited is the one being reported.
        """
        with open_store(self.db_path, readonly=True) as store:
            rows = store.query(
                "SELECT run_id FROM runs WHERE parent_run_id = ? AND parent_task_id = ? "
                "ORDER BY started_at DESC LIMIT 1",
                (self.run_id_provider() or "", parent_task_id),
            )
        return str(rows[0][0]) if rows else None
