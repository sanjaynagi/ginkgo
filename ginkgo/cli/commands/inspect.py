"""Structured inspection command handlers."""

from __future__ import annotations

import json
from pathlib import Path
from types import ModuleType
from typing import Any

from ginkgo.cli.common import resolve_run_dir
from ginkgo.cli.renderers.common import _task_base_name
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.core.flow import FlowDef
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.provenance import load_manifest


def command_inspect(args) -> int:
    """Handle ``ginkgo inspect``."""
    if args.inspect_command == "workflow":
        payload = inspect_workflow(
            workflow_path=resolve_workflow_path(
                project_root=Path.cwd(),
                workflow=args.workflow,
            ).path,
            config_paths=[Path(path).resolve() for path in args.config],
        )
    else:
        payload = inspect_run(run_dir=resolve_run_dir(args.run_id))

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def inspect_workflow(*, workflow_path: Path, config_paths: list[Path]) -> dict[str, Any]:
    """Return a static workflow graph snapshot."""
    with _config_session(override_paths=config_paths):
        module = load_module_from_path(workflow_path)
        flow = _discover_flow(module)
        expr = flow()

    evaluator = _ConcurrentEvaluator()
    evaluator.validate(expr)

    nodes = []
    for node in sorted(evaluator._nodes.values(), key=lambda item: item.node_id):
        nodes.append(
            {
                "task_id": f"task_{node.node_id:04d}",
                "task_name": _task_base_name(node.task_def.name),
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
            task_rows.append(
                {
                    "task_id": task_id,
                    "task_name": _task_base_name(str(task.get("task", "unknown"))),
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
                }
            )

    return {
        "run_id": manifest.get("run_id", run_dir.name),
        "workflow": manifest.get("workflow"),
        "status": manifest.get("status"),
        "started_at": manifest.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "error": manifest.get("error"),
        "resources": manifest.get("resources"),
        "tasks": task_rows,
        "dynamic_expansions": dynamic_expansions,
    }


def _discover_flow(module: ModuleType) -> FlowDef:
    flows = {id(value): value for value in vars(module).values() if isinstance(value, FlowDef)}
    if len(flows) != 1:
        raise RuntimeError(f"Expected exactly one @flow in {module.__file__}, found {len(flows)}")
    return next(iter(flows.values()))
