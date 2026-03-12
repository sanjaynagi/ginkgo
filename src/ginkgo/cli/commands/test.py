"""Test command handlers."""

from __future__ import annotations

import sys
from pathlib import Path

from ginkgo.cli.common import console
from ginkgo.cli.commands.run import run_workflow


def command_test(args) -> int:
    """Handle ``ginkgo test``."""
    tests_dir = Path(".tests")
    if not tests_dir.is_dir():
        raise FileNotFoundError(f"No .tests directory found in {Path.cwd()}")

    rich_console = console(sys.stdout)
    if args.dry_run:
        rich_console.print("[bold green]🌿 ginkgo test[/] [bold]--dry-run[/]\n")
    else:
        rich_console.print("[bold green]🌿 ginkgo test[/]\n")

    status = 0
    workflow_count = 0
    for workflow_path in sorted(tests_dir.glob("*.py")):
        workflow_count += 1
        exit_code = run_workflow(
            workflow_path=workflow_path.resolve(),
            config_paths=[],
            jobs=None,
            cores=None,
            dry_run=args.dry_run,
        )
        if exit_code != 0:
            status = exit_code
            if not args.dry_run:
                break
    if workflow_count == 0:
        rich_console.print("[dim]No test workflows found in .tests/[/]")
        return 0

    if status == 0:
        if args.dry_run:
            rich_console.print(
                f"\n[green]✓[/] Validated [bold]{workflow_count}[/] "
                f"test {'workflow' if workflow_count == 1 else 'workflows'}"
            )
        else:
            rich_console.print(
                f"\n[green]✓[/] Completed [bold]{workflow_count}[/] "
                f"test {'workflow' if workflow_count == 1 else 'workflows'}"
            )
    return status
