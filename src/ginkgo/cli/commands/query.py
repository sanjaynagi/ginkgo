"""``ginkgo query`` — one read-only SQL statement against the ledger."""

from __future__ import annotations

import csv
import json
import sys


from ginkgo import query as ledger
from ginkgo.cli.common import console, stdout_console, new_table
from ginkgo.query import SqlResult

__all__ = ["command_query"]


def command_query(args) -> int:
    """Handle ``ginkgo query`` — run one SELECT and print what it selected.

    A statement the ledger refuses raises :class:`~ginkgo.store.errors.StoreError`,
    which the CLI's top-level handler prints as a single line; there is nothing
    for this command to add to it.
    """
    with ledger.open(missing_ok=True) as reader:
        result = reader.sql(args.sql, limit=getattr(args, "limit", ledger.SQL_ROW_LIMIT))

    if getattr(args, "json", False):
        print(json.dumps(result.to_payload(), indent=2, sort_keys=True, default=str))
        return 0
    if getattr(args, "csv", False):
        _write_csv(result)
        return 0

    return _render_table(stdout_console(), result=result)


def _write_csv(result: SqlResult) -> None:
    """Write the result to stdout as CSV, header first.

    A truncation notice goes to stderr rather than into the stream: stdout has
    to stay CSV a spreadsheet can open, and a warning that redirects away with
    the data is a warning nobody reads.
    """
    writer = csv.writer(sys.stdout)
    writer.writerow(result.columns)
    writer.writerows(tuple(row) for row in result.rows)
    if result.truncated:
        console(sys.stderr).print(_truncation_notice(result))


def _render_table(rich_console, *, result: SqlResult) -> int:
    """Print the result as a table, saying so when the row limit cut it short."""
    rich_console.print("[bold green]🌿 ginkgo query[/]\n")
    if not result.rows:
        rich_console.print("[dim]No rows.[/]")
        return 0

    table = new_table()
    for column in result.columns:
        table.add_column(column, overflow="fold")
    for row in result.rows:
        table.add_row(*("" if value is None else str(value) for value in tuple(row)))
    rich_console.print(table)
    if result.truncated:
        rich_console.print(f"\n[dim]{_truncation_notice(result)}[/]")
    return 0


def _truncation_notice(result: SqlResult) -> str:
    """Return the one line every output mode uses to report a cut-short result."""
    return f"Stopped at {result.limit} rows. Pass --limit for more."
