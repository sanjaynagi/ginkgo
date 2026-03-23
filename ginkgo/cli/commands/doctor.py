"""Doctor command handlers."""

from __future__ import annotations

import os
from pathlib import Path
import sys

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.secrets import build_secret_resolver


def command_doctor(args) -> int:
    """Handle ``ginkgo doctor``."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path
    rich_console = console(sys.stdout)
    rich_console.print("[bold green]🌿 ginkgo doctor[/]\n")

    with _config_session(override_paths=[Path(path).resolve() for path in args.config]) as session:
        module = load_module_from_path(workflow_path)
        flow = _discover_flow(module)
        expr = flow()
        config = session.merged_loaded_values()

    evaluator = _ConcurrentEvaluator(
        secret_resolver=build_secret_resolver(
            project_root=Path.cwd(),
            config=config,
            environ=os.environ,
        )
    )
    evaluator.validate(expr)
    rich_console.print("[green]✓[/] Workflow validation passed")
    return 0


def _discover_flow(module):
    from ginkgo.core.flow import FlowDef

    flows = {id(value): value for value in vars(module).values() if isinstance(value, FlowDef)}
    if len(flows) != 1:
        raise RuntimeError(f"Expected exactly one @flow in {module.__file__}, found {len(flows)}")
    return next(iter(flows.values()))
