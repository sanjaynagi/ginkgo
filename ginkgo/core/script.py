"""Script task execution primitive.

``script()`` is called from inside a ``@task("script")`` body and returns a
``ScriptExpr`` sentinel. The evaluator detects this and dispatches execution
to the appropriate interpreter, forwarding resolved task inputs as CLI
arguments.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ginkgo.core.asset import AssetResult

# Maps file extension (lower-case) to interpreter command.
_EXTENSION_TO_INTERPRETER: dict[str, str] = {
    ".py": "python",
    ".r": "rscript",
}


@dataclass(frozen=True)
class ScriptExpr:
    """Sentinel representing a script execution request.

    Parameters
    ----------
    path : Path
        Resolved source script path.
    outputs : list[str | AssetResult] | str | AssetResult | None
        Declared output paths. When provided, all paths are validated for
        existence after execution.
    log : str | None
        Optional path to capture stdout/stderr.
    interpreter : str
        Interpreter command (e.g. ``"python"`` or ``"rscript"``).
    source_hash : str
        BLAKE3 hash of the script source file, used for cache invalidation.
    """

    path: Path
    outputs: list[str | AssetResult] | str | AssetResult | None
    log: str | None
    interpreter: str
    source_hash: str


def script(
    path: str | Path,
    *,
    outputs: list[str | AssetResult] | str | AssetResult | None = None,
    log: str | None = None,
    interpreter: str | None = None,
) -> ScriptExpr:
    """Create a script execution expression.

    Called from inside a ``@task("script")`` body with fully resolved
    argument values. Resolved task inputs are forwarded to the script as
    ``--param-name value`` CLI arguments.

    Parameters
    ----------
    path : str | Path
        Source script file. Relative paths resolve from the current working
        directory at the time of the call.
    outputs : list[str | AssetResult] | str | AssetResult | None
        Declared output paths validated for existence after execution.
    log : str | None
        Optional path to capture stdout/stderr during execution.
    interpreter : str | None
        Interpreter command override. When ``None``, the interpreter is
        inferred from the file extension: ``.py`` → ``python``,
        ``.R``/``.r`` → ``rscript``.

    Returns
    -------
    ScriptExpr

    Raises
    ------
    FileNotFoundError
        If the script file does not exist.
    ValueError
        If the interpreter cannot be inferred from the extension and no
        explicit ``interpreter`` is given.
    """
    from ginkgo.runtime.caching.hashing import hash_file

    resolved = Path(path).resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Script source not found: {str(resolved)!r}")

    if interpreter is None:
        suffix = resolved.suffix.lower()
        interpreter = _EXTENSION_TO_INTERPRETER.get(suffix)
        if interpreter is None:
            supported = ", ".join(sorted(_EXTENSION_TO_INTERPRETER.keys()))
            raise ValueError(
                f"Cannot infer interpreter for {str(resolved)!r}. "
                f"Supported extensions: {supported}. Pass interpreter= explicitly."
            )

    return ScriptExpr(
        path=resolved,
        outputs=outputs,
        log=log,
        interpreter=interpreter,
        source_hash=hash_file(resolved),
    )
