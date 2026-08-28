"""Structured inspection command handlers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Sequence

from ginkgo.cli.common import open_run
from ginkgo.cli.renderers.common import task_base_name
from ginkgo.cli.workflow_params import load_param_config, validate_param_extras
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import config_session
from ginkgo.core.flow import discover_flow
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.run_summary import RunSummary


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
        with open_run(args.run_id) as (store, run_id):
            payload = inspect_run(summary=store.run(run_id))

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


def inspect_run(*, summary: RunSummary) -> dict[str, Any]:
    """Return a normalized run snapshot from the ledger."""
    task_rows = []
    dynamic_expansions = []
    for task in summary.tasks:
        dynamic_dependencies = [f"task_{dep:04d}" for dep in task.dynamic_dependency_ids]
        if dynamic_dependencies:
            dynamic_expansions.append(
                {
                    "parent_task_id": task.task_key,
                    "dynamic_dependency_ids": dynamic_dependencies,
                }
            )
        row: dict[str, Any] = {
            "task_id": task.task_key,
            "task_name": task_base_name(task.name),
            "status": task.status,
            "attempts": task.attempts,
            "cache_key": task.cache_key,
            "cached": task.cached,
            "exit_code": task.exit_code,
            "env": task.env,
            "kind": task.kind,
            "dependency_ids": [f"task_{dep:04d}" for dep in task.dependency_ids],
            "dynamic_dependency_ids": dynamic_dependencies,
            "failure": task.failure,
            "outputs": list(task.outputs),
            "stdout_log": task.stdout_log,
            "stderr_log": task.stderr_log,
            "started_at": _iso(task.started_at),
            "finished_at": _iso(task.finished_at),
            "timings": task.timings,
        }
        # Remote execution metadata (present only for remote tasks).
        if task.remote_job_id is not None:
            row["remote_job_id"] = task.remote_job_id
        if task.execution_backend is not None:
            row["execution_backend"] = task.execution_backend
        if task.resource_usage is not None:
            row["resource_usage"] = task.resource_usage
        if task.sub_run_id is not None:
            row["sub_run_id"] = task.sub_run_id
        task_rows.append(row)

    return {
        "run_id": summary.run_id,
        "workflow": summary.workflow,
        "status": summary.status,
        "started_at": _iso(summary.started_at),
        "finished_at": _iso(summary.finished_at),
        "error": summary.error,
        "resources": summary.resources,
        "timings": summary.timings,
        "tasks": task_rows,
        "dynamic_expansions": dynamic_expansions,
    }


def _iso(value: datetime | None) -> str | None:
    """Return an ISO timestamp string, or ``None``."""
    return value.isoformat() if value is not None else None
