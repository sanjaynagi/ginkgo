"""Shell task execution primitive.

``shell()`` is called from inside a ``@task(kind="shell")`` body and returns
a ``ShellDirective``. The evaluator detects this and dispatches the
command to the configured shell runner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias, final

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.optional import OptionalOutput

ShellOutputItem: TypeAlias = str | OptionalOutput
ShellOutput: TypeAlias = ShellOutputItem | list[ShellOutputItem] | tuple[ShellOutputItem, ...]


def shell_argv(*, shell: str, cmd: str) -> list[str]:
    """Return the argument vector that runs *cmd* under *shell* in strict mode.

    Every shell task runs this way, local or in an environment, so one
    command means the same thing wherever it runs. ``-e`` stops at the first
    failing command and ``-o pipefail`` fails a pipeline when any stage
    fails; without them a failing step can still exit 0 and be cached as a
    success.

    Parameters
    ----------
    shell : str
        Shell executable, which must accept ``-o pipefail`` (bash, zsh, ash).
    cmd : str
        Shell command string.
    """
    return [shell, "-eo", "pipefail", "-c", cmd]


@final
@dataclass(frozen=True)
class ShellDirective(ExecutionDirective):
    """Execution directive representing a shell command to execute.

    Parameters
    ----------
    cmd : str
        The shell command (already interpolated with resolved values).
    output : str | list[str] | tuple[str, ...] | None
        Expected output path or paths. Used for cache checking and post-
        execution validation. May be omitted (``None``) when the task
        declares ``Out[...]`` parameters — the runner infers it from their
        declared paths; a task with no ``Out[...]`` parameters must still
        declare it.
    log : str | None
        Optional path to capture stdout/stderr.
    """

    cmd: str
    output: ShellOutput | None = None
    log: str | None = None


def shell(
    *, cmd: str, output: ShellOutput | None = None, log: str | None = None
) -> ShellDirective:
    """Create a shell command expression.

    Called from inside a ``@task(kind="shell")`` body with fully resolved
    argument values. The ``cmd`` is a standard Python f-string — all variables are concrete
    at the point this is called.

    Parameters
    ----------
    cmd : str
        The shell command to run.
    output : str | list[str] | tuple[str, ...] | None
        The expected output path or paths. May be omitted when the task
        declares ``Out[...]`` parameters, in which case the runner infers
        it from their declared paths; the runner still requires it when the
        task declares none.
    log : str | None
        Optional path to capture stdout/stderr.

    Returns
    -------
    ShellDirective
    """
    if isinstance(output, list | tuple) and not output:
        raise ValueError("shell output must contain at least one declared path")

    return ShellDirective(cmd=cmd, output=output, log=log)
