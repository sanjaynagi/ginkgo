"""Structured inspection command handlers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

from ginkgo.cli.common import resolve_run_dir
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.cli.workflow_params import load_param_config, validate_param_extras
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import config_session
from ginkgo.core.flow import discover_flow
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.caching.provenance import load_manifest


def command_inspect(args) -> int:
    """Handle ``ginkgo inspect``."""
    if args.inspect_command == "workflow":
        payload = inspect_workflow(
            workflow_path=resolve_workflow_path(
                project_root=Path.cwd(),
                workflow=args.workflow,
            ).path,
            config_paths=[Path(path).resolve() for path in args.config],
            param_extras=getattr(args, "param_extras", ()),
        )
    else:
        payload = inspect_run(run_dir=resolve_run_dir(args.run_id))

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

    evaluator = ConcurrentEvaluator()
    evaluator.validate(expr)

    nodes = []
    for node in sorted(evaluator._nodes.values(), key=lambda item: item.node_id):
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


def inspect_run(*, run_dir: Path) -> dict[str, Any]:
    """Return a normalized run snapshot from provenance."""
    manifest = load_manifest(run_dir)
    tasks = manifest.get("tasks", {})
    task_rows = []
    dynamic_expansions = []
    if isinstance(tasks, dict):
        for task_id, task in sorted(tasks.items()):
            if not isinstance(task, dict):
                continue
            dynamic_dependencies = task.get("dynamic_dependency_ids") or []
            if dynamic_dependencies:
                dynamic_expansions.append(
                    {
                        "parent_task_id": task_id,
                        "dynamic_dependency_ids": [
                            f"task_{int(dep_id):04d}" for dep_id in dynamic_dependencies
                        ],
                    }
                )
            row: dict[str, Any] = {
                "task_id": task_id,
                "task_name": task_base_name(str(task.get("task", "unknown"))),
                "status": task.get("status"),
                "attempt": task.get("attempt"),
                "attempts": task.get("attempts"),
                "cache_key": task.get("cache_key"),
                "cached": task.get("cached"),
                "exit_code": task.get("exit_code"),
                "env": task.get("env"),
                "kind": task.get("kind"),
                "dependency_ids": [
                    f"task_{int(dep_id):04d}" for dep_id in task.get("dependency_ids", [])
                ],
                "dynamic_dependency_ids": [
                    f"task_{int(dep_id):04d}" for dep_id in dynamic_dependencies
                ],
                "failure": task.get("failure"),
                "outputs": task.get("outputs", []),
                "stdout_log": task.get("stdout_log"),
                "stderr_log": task.get("stderr_log"),
                "started_at": task.get("started_at"),
                "finished_at": task.get("finished_at"),
                "timings": task.get("timings", {}),
            }
            # Remote execution metadata (present only for remote tasks).
            if task.get("remote_job_id") is not None:
                row["remote_job_id"] = task["remote_job_id"]
            if task.get("execution_backend") is not None:
                row["execution_backend"] = task["execution_backend"]
            if task.get("resources") is not None:
                row["resources"] = task["resources"]
            if task.get("sub_run_id") is not None:
                row["sub_run_id"] = task["sub_run_id"]
            task_rows.append(row)

    return {
        "run_id": manifest.get("run_id", run_dir.name),
        "workflow": manifest.get("workflow"),
        "status": manifest.get("status"),
        "started_at": manifest.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "error": manifest.get("error"),
        "resources": manifest.get("resources"),
        "timings": manifest.get("timings", {}),
        "tasks": task_rows,
        "dynamic_expansions": dynamic_expansions,
    }
