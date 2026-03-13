"""Shell task execution primitive.

``shell_task()`` is called from inside a ``@task()`` body and returns a
``ShellExpr`` sentinel.  The evaluator (Phase 2) detects this and dispatches
to a shell runner.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

ShellOutput: TypeAlias = str | list[str] | tuple[str, ...]


@dataclass(frozen=True)
class ShellExpr:
    """Sentinel representing a shell command to execute.

    Parameters
    ----------
    cmd : str
        The shell command (already interpolated with resolved values).
    output : str | list[str] | tuple[str, ...]
        Expected output path or paths. Used for cache checking and post-
        execution validation.
    log : str | None
        Optional path to capture stdout/stderr.
    """

    cmd: str
    output: ShellOutput
    log: str | None = None


def shell_task(*, cmd: str, output: ShellOutput, log: str | None = None) -> ShellExpr:
    """Create a shell command expression.

    Called from inside a ``@task()`` body with fully resolved argument values.
    The ``cmd`` is a standard Python f-string — all variables are concrete
    at the point this is called.

    Parameters
    ----------
    cmd : str
        The shell command to run.
    output : str | list[str] | tuple[str, ...]
        The expected output path or paths.
    log : str | None
        Optional path to capture stdout/stderr.

    Returns
    -------
    ShellExpr
    """
    if isinstance(output, list | tuple) and not output:
        raise ValueError("shell_task output must contain at least one declared path")

    return ShellExpr(cmd=cmd, output=output, log=log)
