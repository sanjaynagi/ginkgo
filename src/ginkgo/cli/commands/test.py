"""Test command handlers."""

from __future__ import annotations

from pathlib import Path

from ginkgo.cli.commands.run import run_workflow


def command_test(args) -> int:
    """Handle ``ginkgo test``."""
    tests_dir = Path(".tests")
    if not tests_dir.is_dir():
        raise FileNotFoundError(f"No .tests directory found in {Path.cwd()}")

    status = 0
    for workflow_path in sorted(tests_dir.glob("*.py")):
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
    return status
