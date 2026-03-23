"""Secrets command handlers."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.secrets import build_secret_resolver, collect_secret_refs


def command_secrets(args) -> int:
    """Handle ``ginkgo secrets`` subcommands."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path

    with _config_session(override_paths=[Path(path).resolve() for path in args.config]) as session:
        module = load_module_from_path(workflow_path)
        flow = _discover_flow(module)
        expr = flow()
        config = session.merged_loaded_values()

    evaluator = _ConcurrentEvaluator()
    evaluator.validate(expr)

    refs = sorted(
        {ref for node in evaluator._nodes.values() for ref in collect_secret_refs(node.expr.args)},
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


def _discover_flow(module):
    from ginkgo.core.flow import FlowDef

    flows = {id(value): value for value in vars(module).values() if isinstance(value, FlowDef)}
    if len(flows) != 1:
        raise RuntimeError(f"Expected exactly one @flow in {module.__file__}, found {len(flows)}")
    return next(iter(flows.values()))
