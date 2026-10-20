"""Sub-workflow invocation primitive.

``subworkflow()`` is called from inside a ``@task(kind="subworkflow")`` body
and returns a ``SubWorkflowDirective``. The evaluator detects this and
dispatches the nested workflow via a ``ginkgo run`` subprocess.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import final

from ginkgo.core.directive import ExecutionDirective
from ginkgo.core.hashing import hash_file, new_hasher


@final
@dataclass(frozen=True)
class SubWorkflowDirective(ExecutionDirective):
    """Execution directive representing a nested Ginkgo workflow invocation.

    Parameters
    ----------
    path : str
        Path to the child workflow file, resolved by the caller.
    params : dict
        Parameter overrides to pass to the child run. Written to a
        temporary YAML config file at dispatch time and passed via
        ``--config``.
    config : tuple of str
        Additional ``--config`` paths to forward to the child run.
    source_hash : str
        Digest of the child's Python sources and config files, folded into the
        parent task's cache key so editing the child re-runs it.
    """

    path: str
    params: dict = field(default_factory=dict)
    config: tuple[str, ...] = ()
    source_hash: str = ""


@dataclass(frozen=True)
class SubWorkflowResult:
    """Outcome of a completed sub-workflow invocation.

    Parameters
    ----------
    run_id : str
        The child run's identifier.
    status : str
        ``"success"`` on a clean exit. Failures raise ``SubWorkflowError``
        before a result is returned, so this is always ``"success"`` when
        a result object is observed.
    """

    run_id: str
    status: str


def subworkflow(
    path: str | Path,
    *,
    params: dict | None = None,
    config: str | Path | list[str | Path] | tuple[str | Path, ...] | None = None,
) -> SubWorkflowDirective:
    """Create a sub-workflow invocation expression.

    Called from inside a ``@task(kind="subworkflow")`` body with fully
    resolved argument values. The child workflow runs as a self-contained
    ``ginkgo run`` subprocess; its ``run_id`` is returned to the parent
    task, and ``ginkgo runs show <run_id>`` reads what it did.

    Parameters
    ----------
    path : str or Path
        Path to the child workflow file.
    params : dict, optional
        Values for parameters the child declares with ``ginkgo.param``. Passed
        as a ``[params]`` table in a temporary ``--config`` file, layering over
        the child's own table, so a parameter not named here keeps the value the
        child's config gives it.
    config : str, Path, or sequence of either, optional
        Additional ``--config`` paths to forward to the child.

    Returns
    -------
    SubWorkflowDirective
    """
    if not path:
        raise ValueError("subworkflow path must not be empty")

    config_tuple: tuple[str, ...]
    if config is None:
        config_tuple = ()
    elif isinstance(config, (str, Path)):
        config_tuple = (str(config),)
    else:
        config_tuple = tuple(str(item) for item in config)

    return SubWorkflowDirective(
        path=str(path),
        params=dict(params) if params is not None else {},
        config=config_tuple,
        source_hash=_child_source_hash(path=Path(path), config=config_tuple),
    )


def _child_source_hash(*, path: Path, config: tuple[str, ...]) -> str:
    """Hash the Python sources beside a child workflow and its config files.

    Every ``.py`` file under the child workflow's directory counts, which
    covers the local modules it imports however it imports them. Hidden
    directories and ``__pycache__`` are skipped, and nothing else is read, so
    the files the child run writes never change the digest.
    """
    path = path.resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Sub-workflow path does not exist: {str(path)!r}")
    root = path.parent
    sources = sorted(
        source
        for source in root.rglob("*.py")
        if not any(
            part.startswith(".") or part == "__pycache__"
            for part in source.relative_to(root).parts
        )
    )
    hasher = new_hasher()
    for source in sources:
        hasher.update(source.relative_to(root).as_posix().encode())
        hasher.update(hash_file(source).encode())
    for item in config:
        hasher.update(item.encode())
        hasher.update(hash_file(Path(item)).encode())
    return hasher.hexdigest()
