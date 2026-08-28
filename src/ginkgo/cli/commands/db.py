"""Provenance-database command handlers.

``ginkgo db`` is the maintenance surface for the ledger at
``.ginkgo/ginkgo.db``: create or upgrade it, check that it is intact, and say
where it is. Later phases add ``vacuum`` and ``prune`` beside these.

Every subcommand but ``path`` opens the database in write mode, so each needs
write access to the workspace: ``check`` reports on the database a run would
use, which means creating it if it is not there yet.
"""

from __future__ import annotations

import sys

from ginkgo.cli.common import console
from ginkgo.store.sqlite import open_store
from ginkgo.workspace_layout import WorkspaceLayout


def command_db(args) -> int:
    """Handle ``ginkgo db`` subcommands."""
    path = WorkspaceLayout.relative().db
    rich_console = console(sys.stdout)

    if args.db_command == "path":
        rich_console.print(str(path))
        return 0

    if args.db_command == "migrate":
        rich_console.print("[bold green]🌿 ginkgo db migrate[/]\n")
        with open_store(path) as store:
            rich_console.print(f"[green]✓[/] {path} at schema version {store.schema_version}")
        return 0

    rich_console.print("[bold green]🌿 ginkgo db check[/]\n")
    # Write mode, so checking an empty workspace creates the database rather
    # than reporting the absence of one as a fault.
    with open_store(path) as store:
        rich_console.print(f"Database: {path}")
        rich_console.print(f"Schema version: {store.schema_version}")
        problems = [row[0] for row in store.query("PRAGMA integrity_check") if row[0] != "ok"]

    if problems:
        for problem in problems:
            rich_console.print(f"[red]✖[/] {problem}")
        return 1

    rich_console.print("[green]✓[/] integrity check passed")
    return 0
