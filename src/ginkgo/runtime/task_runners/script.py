"""Script task execution.

``ScriptRunner`` executes ``ScriptDirective`` driver tasks: ``script(...)``
calls made from inside a ``@task("script")`` body, forwarding resolved task
inputs as CLI arguments to the declared interpreter.
"""

from __future__ import annotations

import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ginkgo.core.script import ScriptDirective
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.task_runners.driver import DriverTaskRunner
from ginkgo.runtime.task_runners.shell import (
    ShellTaskError,
    iter_output_values,
    remove_declared_output,
    stringify_cli_argument,
)


@dataclass(kw_only=True)
class ScriptRunner(DriverTaskRunner):
    """Execute script driver tasks."""

    def run_script(self, *, node: Any, directive: ScriptDirective) -> Any:
        """Execute a script task, forwarding task inputs as CLI arguments."""
        assert node.execution_args is not None
        user_log_path = Path(directive.log) if directive.log is not None else None
        if directive.output is not None:
            for output_path in iter_output_values(directive.output):
                remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

        # Resolve the interpreter. With no declared env, use sys.executable
        # directly to stay in the scheduler's own environment. With a
        # declared env, run_logged_command wraps the whole command through
        # the backend's Pixi/container shell, so "python" must resolve via
        # that shell's PATH rather than being pinned to the scheduler's
        # interpreter — otherwise env= is silently ignored for Python scripts.
        interpreter_cmd = (
            "python"
            if directive.interpreter == "python" and node.task_def.env is not None
            else shlex.quote(sys.executable)
            if directive.interpreter == "python"
            else shlex.quote(directive.interpreter)
        )

        # Build command: interpreter script_path --arg-name value ...
        cmd_parts = [interpreter_cmd, shlex.quote(str(directive.path))]
        for name, value in node.execution_args.items():
            option = f"--{name.replace('_', '-')}"
            cmd_parts.extend([shlex.quote(option), shlex.quote(stringify_cli_argument(value))])
        cmd = " ".join(cmd_parts)

        completed = self.shell_runner.run_logged_command(
            node=node, cmd=cmd, user_log_path=user_log_path
        )
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=node.task_def.name,
                cmd=redact_text(text=cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=directive.log,
            )

        if directive.output is None:
            return None
        return self._validate_and_return_output(
            task_name=node.task_def.name,
            task_def=node.task_def,
            output=directive.output,
        )
