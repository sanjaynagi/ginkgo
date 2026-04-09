"""Doctor command handlers."""

from __future__ import annotations

import os
import json
from pathlib import Path
import sys

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.runtime.diagnostics import collect_workflow_diagnostics
from ginkgo.runtime.environment.secrets import build_secret_resolver


def command_doctor(args) -> int:
    """Handle ``ginkgo doctor``."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path
    with _config_session(override_paths=[Path(path).resolve() for path in args.config]) as session:
        config = session.merged_loaded_values()
    diagnostics = collect_workflow_diagnostics(
        workflow_path=workflow_path,
        config_paths=[Path(path).resolve() for path in args.config],
        secret_resolver=build_secret_resolver(
            project_root=Path.cwd(),
            config=config,
            environ=os.environ,
        ),
    )
    if args.json:
        print(json.dumps([item.to_payload() for item in diagnostics], indent=2, sort_keys=True))
        return 0 if not diagnostics else 1

    if diagnostics:
        rich_console = console(sys.stderr)
        for item in diagnostics:
            rich_console.print(f"[red]✖[/] {item.code}: {item.message}")
            if item.suggestion:
                rich_console.print(f"[dim]{item.suggestion}[/]")
        return 1

    rich_console = console(sys.stdout)
    rich_console.print("[bold green]🌿 ginkgo doctor[/]\n")
    rich_console.print("[green]✓[/] Workflow validation passed")
    return 0
