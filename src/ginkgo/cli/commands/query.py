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

    ``--schema`` answers the question that comes before the first statement:
    which tables are there, and what is in them.

    A statement the ledger refuses raises :class:`~ginkgo.store.errors.StoreError`,
    which the CLI's top-level handler prints as a single line; there is nothing
    for this command to add to it.
    """
    if getattr(args, "schema", False):
        return _print_schema(args)
    if not args.sql:
        console(sys.stderr).print(
            "Give `ginkgo query` one SELECT, or --schema for the tables and their columns."
        )
        return 2

    with ledger.open(missing_ok=True) as reader:
        result = reader.sql(args.sql, limit=getattr(args, "limit", ledger.SQL_ROW_LIMIT))

    if getattr(args, "json", False):
        print(json.dumps(result.to_payload(), indent=2, sort_keys=True, default=str))
        return 0
    if getattr(args, "csv", False):
        _write_csv(result)
        return 0

    return _render_table(stdout_console(), result=result)


def _print_schema(args) -> int:
    """Print every table and its columns, in whichever output mode was asked for.

    An empty workspace answers the same as a populated one: the schema is what
    ginkgo would write, not what has been written.
    """
    with ledger.open(missing_ok=True) as reader:
        schema = reader.schema()

    if getattr(args, "json", False):
        print(json.dumps({table: list(columns) for table, columns in schema.items()}, indent=2))
        return 0
    if getattr(args, "csv", False):
        writer = csv.writer(sys.stdout)
        writer.writerow(("table", "column"))
        writer.writerows(
            (table, column) for table, columns in schema.items() for column in columns
        )
        return 0

    rich_console = stdout_console()
    rich_console.print("[bold green]🌿 ginkgo query --schema[/]\n")
    table = new_table()
    table.add_column("Table", overflow="fold")
    table.add_column("Columns", overflow="fold")
    for name, columns in schema.items():
        table.add_row(name, ", ".join(columns))
    rich_console.print(table)
    rich_console.print(
        "\n[dim]The schema is versioned but not stable: a query written against it "
        "may need rewriting after an upgrade.[/]"
    )
    return 0


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
