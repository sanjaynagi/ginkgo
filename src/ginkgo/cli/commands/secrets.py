"""Secrets command handlers."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from ginkgo.cli.common import console
from ginkgo.cli.workflow_params import load_param_config, validate_param_extras
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import config_session
from ginkgo.core.flow import discover_flow
from ginkgo.runtime.graph import TaskGraph
from ginkgo.runtime.placement import Placement
from ginkgo.runtime.task_validation import TaskValidator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.secrets import build_secret_resolver, collect_secret_refs


def command_secrets(args) -> int:
    """Handle ``ginkgo secrets`` subcommands."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path

    config_paths = [Path(path).resolve() for path in args.config]
    param_config = load_param_config(project_root=Path.cwd(), config_paths=config_paths)
    with config_session(
        override_paths=config_paths,
        param_config=param_config,
        cli_extras=getattr(args, "param_extras", ()),
        require_params=False,
    ) as session:
        module = load_module_from_path(workflow_path)
        flow = discover_flow(module)
        expr = flow()
        validate_param_extras(session, command_flags=getattr(args, "command_flags", ()))
        config = session.merged_loaded_values()

    graph = TaskGraph.build(expr)
    graph.validate(
        validator=TaskValidator(),
        placement=Placement(
            executor_registry=ExecutorRegistry.for_validation(
                project_root=Path.cwd(),
                config_paths=config_paths,
            )
        ),
    )

    refs = sorted(
        {ref for node in graph.nodes.values() for ref in collect_secret_refs(node.expr.args)},
        key=lambda item: (item.backend, item.name),
    )
    rich_console = console(sys.stdout)

    if args.secrets_command == "list":
        rich_console.print("[bold green]🌿 ginkgo secrets list[/]\n")
        if not refs:
            rich_console.print("No declared secrets found.")
            return 0
        for ref in refs:
            rich_console.print(f"- {ref.backend}:{ref.name}")
        return 0

    resolver = build_secret_resolver(
        project_root=Path.cwd(),
        config=config,
        environ=os.environ,
    )
    missing = resolver.validate(refs=set(refs))
    rich_console.print("[bold green]🌿 ginkgo secrets validate[/]\n")
    if missing:
        for ref in missing:
            rich_console.print(f"[red]✖[/] {ref.backend}:{ref.name}")
        return 1

    if not refs:
        rich_console.print("No declared secrets found.")
        return 0

    for ref in refs:
        rich_console.print(f"[green]✓[/] {ref.backend}:{ref.name}")
    return 0
