"""``ginkgo inspect workflow`` — the static task graph, without running it."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from ginkgo.cli.renderers.common import task_base_name
from ginkgo.cli.workflow_params import load_param_config, validate_param_extras
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import config_session
from ginkgo.core.flow import discover_flow
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path


def command_inspect(args) -> int:
    """Handle ``ginkgo inspect``.

    A recorded run is ``ginkgo runs show <run_id> --json``: one command per
    concept, and a run is the ``runs`` group's concept rather than this one's.
    """
    payload = inspect_workflow(
        workflow_path=resolve_workflow_path(
            project_root=Path.cwd(),
            workflow=args.workflow,
        ).path,
        config_paths=[Path(path).resolve() for path in args.config],
        param_extras=getattr(args, "param_extras", ()),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def inspect_workflow(
    *,
    workflow_path: Path,
    config_paths: list[Path],
    param_extras: Sequence[str] = (),
) -> dict[str, Any]:
    """Return a static workflow graph snapshot.

    Parameters
    ----------
    workflow_path : Path
        The workflow module to inspect.
    config_paths : list[Path]
        Config files given with ``--config``.
    param_extras : Sequence[str], optional
        Command-line tokens supplying declared workflow parameters. A required
        parameter left unsupplied is reported rather than raised, so a workflow
        can still be described without its inputs.

    Returns
    -------
    dict[str, Any]
        Graph snapshot, including a ``params`` list describing every parameter
        the workflow declares.
    """
    param_config = load_param_config(project_root=Path.cwd(), config_paths=config_paths)
    with config_session(
        override_paths=config_paths,
        param_config=param_config,
        cli_extras=param_extras,
        require_params=False,
    ) as session:
        module = load_module_from_path(workflow_path)
        flow = discover_flow(module)
        expr = flow()
        validate_param_extras(session)
        params = [
            {
                **decl.to_payload(),
                "source": session.param_sources()[name],
                "supplied": session.param_sources()[name] != "default" or not decl.required,
            }
            for name, decl in session.declarations.items()
        ]

    evaluator = ConcurrentEvaluator(
        executor_registry=ExecutorRegistry.for_validation(
            project_root=Path.cwd(),
            config_paths=config_paths,
        )
    )
    evaluator.build_and_validate(expr)

    nodes = []
    for node in sorted(evaluator.task_nodes.values(), key=lambda item: item.node_id):
        nodes.append(
            {
                "task_id": f"task_{node.node_id:04d}",
                "task_name": task_base_name(node.task_def.name),
                "kind": node.task_def.kind,
                "env": node.task_def.env,
                "execution_mode": node.task_def.execution_mode,
                "retries": node.task_def.retries,
                "dependencies": [f"task_{dep_id:04d}" for dep_id in sorted(node.dependency_ids)],
            }
        )

    return {
        "workflow": str(workflow_path),
        "task_count": len(nodes),
        "edge_count": sum(len(node["dependencies"]) for node in nodes),
        "params": params,
        "tasks": nodes,
    }
