"""Sub-workflow invocation primitive.

``subworkflow()`` is called from inside a ``@task(kind="subworkflow")`` body
and returns a ``SubWorkflowExpr`` sentinel. The evaluator detects this and
dispatches the nested workflow via a ``ginkgo run`` subprocess.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class SubWorkflowExpr:
    """Sentinel representing a nested Ginkgo workflow invocation.

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
    """

    path: str
    params: dict = field(default_factory=dict)
    config: tuple[str, ...] = ()


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
    manifest_path : str
        Path to the child run's ``manifest.yaml``.
    """

    run_id: str
    status: str
    manifest_path: str


def subworkflow(
    path: str | Path,
    *,
    params: dict | None = None,
    config: str | Path | list[str | Path] | tuple[str | Path, ...] | None = None,
) -> SubWorkflowExpr:
    """Create a sub-workflow invocation expression.

    Called from inside a ``@task(kind="subworkflow")`` body with fully
    resolved argument values. The child workflow runs as a self-contained
    ``ginkgo run`` subprocess; its ``run_id`` and manifest path are
    returned to the parent task.

    Parameters
    ----------
    path : str or Path
        Path to the child workflow file.
    params : dict, optional
        Parameter overrides for the child run. Serialised as YAML and
        passed via a temporary ``--config`` file.
    config : str, Path, or sequence of either, optional
        Additional ``--config`` paths to forward to the child.

    Returns
    -------
    SubWorkflowExpr
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

    return SubWorkflowExpr(
        path=str(path),
        params=dict(params) if params is not None else {},
        config=config_tuple,
    )
