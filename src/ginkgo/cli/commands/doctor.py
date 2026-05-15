"""Doctor command handlers."""

from __future__ import annotations

import os
import json
from pathlib import Path
import sys

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.remote.access.doctor import collect_access_diagnostics
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

    # Additional FUSE-streaming probes. These produce their own diagnostic
    # shape; normalise into the workflow diagnostic format for rendering.
    executor_cfg = _extract_executor_config(config=config)
    access_diagnostics = collect_access_diagnostics(
        project_root=Path.cwd(),
        executor_config=executor_cfg,
    )

    if args.json:
        combined = [item.to_payload() for item in diagnostics]
        combined.extend(
            {
                "severity": item.severity,
                "code": item.code,
                "message": item.message,
                "location": None,
                "suggestion": item.suggestion,
            }
            for item in access_diagnostics
        )
        print(json.dumps(combined, indent=2, sort_keys=True))
        has_errors = any(item.severity == "error" for item in access_diagnostics) or bool(
            diagnostics
        )
        return 0 if not has_errors else 1

    rich_console_out = console(sys.stdout)
    rich_console_err = console(sys.stderr)

    if diagnostics:
        for item in diagnostics:
            rich_console_err.print(f"[red]✖[/] {item.code}: {item.message}")
            if item.suggestion:
                rich_console_err.print(f"[dim]{item.suggestion}[/]")
    else:
        rich_console_out.print("[bold green]🌿 ginkgo doctor[/]\n")
        rich_console_out.print("[green]✓[/] Workflow validation passed")

    for item in access_diagnostics:
        marker = {"error": "[red]✖[/]", "warning": "[yellow]![/]"}.get(item.severity, "[cyan]ℹ[/]")
        target = rich_console_err if item.severity == "error" else rich_console_out
        target.print(f"{marker} {item.code}: {item.message}")
        if item.suggestion:
            target.print(f"[dim]{item.suggestion}[/]")

    has_errors = bool(diagnostics) or any(item.severity == "error" for item in access_diagnostics)
    return 1 if has_errors else 0


def _extract_executor_config(*, config: dict) -> dict | None:
    """Return the executor-scoped config section, if any."""
    remote = config.get("remote") if isinstance(config, dict) else None
    if not isinstance(remote, dict):
        return None
    for key in ("k8s", "batch"):
        section = remote.get(key)
        if isinstance(section, dict):
            return section
    return None
