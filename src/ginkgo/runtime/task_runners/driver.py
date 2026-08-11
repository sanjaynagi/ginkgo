"""Shared base for out-of-process driver task runners.

A "driver" task kind executes as an external process (shell, script,
notebook, ...) rather than in-process Python. ``DriverTaskRunner`` holds the
machinery every such runner needs: the shell/subprocess primitives and
output-coercion validation. Runners for a specific driver kind subclass it
and add their own command-building and execution logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ginkgo.core.task import TaskDef
from ginkgo.runtime.task_runners.shell import ShellRunner, iter_output_values
from ginkgo.runtime.task_validation import TaskValidator


@dataclass(kw_only=True)
class DriverTaskRunner:
    """Base for task runners that execute out-of-process and validate declared outputs.

    Parameters
    ----------
    shell_runner : ShellRunner
        Provides ``run_logged_command`` and the underlying subprocess
        primitives.
    validator : TaskValidator
        Used to coerce return values for declared outputs.
    """

    shell_runner: ShellRunner
    validator: TaskValidator

    def _validate_and_return_output(
        self,
        *,
        task_name: str,
        task_def: TaskDef,
        output: Any,
    ) -> Any:
        """Validate declared output paths exist and return coerced value."""
        output_paths = iter_output_values(output)
        missing = [str(path) for path in output_paths if not path.exists()]
        if missing:
            label = missing[0] if len(missing) == 1 else missing
            raise FileNotFoundError(
                f"Task {task_name} completed but did not create declared output {label!r}"
            )
        return self.validator.coerce_return_value(task_def=task_def, value=output)
