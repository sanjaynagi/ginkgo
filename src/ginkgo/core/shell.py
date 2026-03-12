"""Shell task execution primitive.

``shell_task()`` is called from inside a ``@task()`` body and returns a
``ShellExpr`` sentinel.  The evaluator (Phase 2) detects this and dispatches
to a shell runner.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShellExpr:
    """Sentinel representing a shell command to execute.

    Parameters
    ----------
    cmd : str
        The shell command (already interpolated with resolved values).
    output : str
        Expected output path.  Used for cache checking and post-execution
        validation.
    log : str | None
        Optional path to capture stdout/stderr.
    """

    cmd: str
    output: str
    log: str | None = None


def shell_task(*, cmd: str, output: str, log: str | None = None) -> ShellExpr:
    """Create a shell command expression.

    Called from inside a ``@task()`` body with fully resolved argument values.
    The ``cmd`` is a standard Python f-string — all variables are concrete
    at the point this is called.

    Parameters
    ----------
    cmd : str
        The shell command to run.
    output : str
        The expected output path.
    log : str | None
        Optional path to capture stdout/stderr.

    Returns
    -------
    ShellExpr
    """
    return ShellExpr(cmd=cmd, output=output, log=log)
