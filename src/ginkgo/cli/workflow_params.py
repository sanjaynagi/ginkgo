"""CLI-side plumbing for workflow parameters declared with ``ginkgo.param``.

Parameters resolve against the ``[params]`` table of the project config, which
must be loaded *before* the workflow module is imported so that resolution does
not depend on whether the workflow calls ``config()`` before or after
``param()``. These helpers keep that ordering in one place, shared by every
command that imports a workflow.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from ginkgo.cli.renderers.common import task_base_name
from ginkgo.config import PARAMS_CONFIG_KEY, config_session, load_runtime_config_layers
from ginkgo.core.flow import discover_flow
from ginkgo.params import (
    GlobalParamRead,
    ParamDecl,
    ParamError,
    find_global_param_reads,
    flag_for,
)
from ginkgo.runtime.module_loader import load_module_from_path

if TYPE_CHECKING:
    from ginkgo.config import _ConfigSession


def params_table(layers: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Build the ``[params]`` table by layering it across config sources.

    Parameters layer key by key, so an override that sets one parameter leaves
    the rest of an earlier table intact. Merging the configs first and reading
    ``[params]`` from the result would instead replace the whole table, silently
    dropping parameters the override did not mention.

    Parameters
    ----------
    layers : Sequence[dict[str, Any]]
        Config sources in load order, from
        :func:`ginkgo.config.load_runtime_config_layers`.

    Returns
    -------
    dict[str, Any]
        The layered parameter table, empty when no source declares one.

    Raises
    ------
    TypeError
        If any source's ``params`` key is not a mapping.
    """
    merged: dict[str, Any] = {}
    for layer in layers:
        table = layer.get(PARAMS_CONFIG_KEY, {})
        if not isinstance(table, dict):
            raise TypeError(
                f"Config key {PARAMS_CONFIG_KEY!r} must be a mapping of parameter names to "
                f"values, got {type(table).__name__}."
            )
        merged.update(table)
    return merged


def load_param_config(
    *,
    project_root: Path,
    config_paths: Sequence[str | Path] | None = None,
) -> dict[str, Any]:
    """Load the project config and return its ``[params]`` table.

    For callers that already hold a loaded runtime config, use
    :func:`params_table` instead of reading the files a second time.

    Parameters
    ----------
    project_root : Path
        Directory holding the canonical ``ginkgo.toml``/``ginkgo.yaml``.
    config_paths : Sequence[str | Path] | None, optional
        Config files given with ``--config``, layered over the project config.

    Returns
    -------
    dict[str, Any]
        The parameter table, empty when the config declares none.
    """
    return params_table(
        load_runtime_config_layers(project_root=project_root, override_paths=config_paths)
    )


def validate_param_extras(session: _ConfigSession) -> None:
    """Reject command-line tokens that no parameter declaration claimed.

    Call once the flow has been built, so that parameters declared inside the
    flow body have had their chance to claim a flag.

    Parameters
    ----------
    session : _ConfigSession
        The session the workflow was imported under.

    Raises
    ------
    ParamError
        If any command-line token went unclaimed.
    """
    unknown = session.unconsumed_extras()
    if not unknown:
        return

    declared = sorted(session.declarations)
    known = ", ".join(flag_for(name) for name in declared) if declared else "none"
    raise ParamError(
        f"unrecognized arguments: {' '.join(unknown)}\n"
        f"Parameters declared by this workflow: {known}"
    )


def global_param_reads(
    *,
    declaration_globals: dict[str, dict[str, Any]],
    evaluator: Any,
) -> list[GlobalParamRead]:
    """Find validated tasks whose bodies read a declared parameter as a global.

    Parameters
    ----------
    declaration_globals : dict[str, dict[str, Any]]
        Parameter name to the globals of its declaring module, from the session
        the workflow was imported under.
    evaluator : Any
        An evaluator whose graph has been validated, supplying the task nodes.

    Returns
    -------
    list[GlobalParamRead]
        One finding per task and parameter.
    """
    tasks = [
        (task_base_name(node.task_def.name), node.task_def.fn)
        for node in sorted(evaluator.task_nodes.values(), key=lambda item: item.node_id)
        if node.task_def.kind == "python"
    ]
    return find_global_param_reads(declaration_globals=declaration_globals, tasks=tasks)


def collect_param_declarations(
    *,
    workflow_path: Path,
    config_paths: Sequence[str | Path] | None = None,
) -> list[ParamDecl]:
    """Import a workflow and return the parameters it declares, in declaration order.

    Used to render ``ginkgo run <workflow> --help``. Required parameters need not
    be supplied, and a flow body that fails to build still yields whatever was
    declared at module level.

    Parameters
    ----------
    workflow_path : Path
        The workflow module to import.
    config_paths : Sequence[str | Path] | None, optional
        Config files given with ``--config``.

    Returns
    -------
    list[ParamDecl]
        The declarations found, in the order they were declared.
    """
    param_config = load_param_config(
        project_root=Path.cwd(),
        config_paths=config_paths,
    )
    with config_session(
        override_paths=config_paths,
        param_config=param_config,
        require_params=False,
    ) as session:
        module = load_module_from_path(workflow_path)
        try:
            discover_flow(module)()
        except BaseException:
            # Module-level declarations are still worth listing even when the
            # flow body cannot be built without real parameter values.
            pass
        return list(session.declarations.values())


__all__ = [
    "collect_param_declarations",
    "global_param_reads",
    "load_param_config",
    "params_table",
    "validate_param_extras",
]
