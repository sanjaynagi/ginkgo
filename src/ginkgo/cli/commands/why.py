"""``ginkgo why`` — which task produced a file, and from what."""

from __future__ import annotations

import json

from ginkgo import query
from ginkgo.cli.common import stdout_console
from ginkgo.query import Provenance

__all__ = ["command_why"]


def command_why(args) -> int:
    """Handle ``ginkgo why`` — trace one materialized path or artifact id."""
    rich_console = stdout_console()

    # An unknown path raises, and the CLI's own error handler reports it.
    with query.open(missing_ok=True) as reader:
        provenance = reader.why(args.target)
    return _render_why(rich_console, provenance=provenance, as_json=bool(args.json))


def _render_why(rich_console, *, provenance: Provenance, as_json: bool) -> int:
    """Print what produced one artifact."""
    if as_json:
        print(json.dumps(provenance.to_payload(), indent=2, sort_keys=True))
        return 0
    rich_console.print("[bold green]🌿 ginkgo why[/]\n")
    if provenance.path:
        rich_console.print(f"Path: {provenance.path}")
    rich_console.print(f"Artifact ID: [bold]{provenance.artifact_id}[/]")
    rich_console.print(
        f"Produced by: {provenance.task_name or '-'} [dim]({provenance.task_id})[/]"
    )
    rich_console.print(f"Run: {provenance.run_id or '-'}")
    rich_console.print(f"Cache key: {provenance.cache_key or '-'}")
    if not provenance.inputs:
        return 0
    rich_console.print("\n[bold]Inputs[/]")
    for entry in provenance.inputs:
        detail = entry.get("digest") or entry.get("value_summary") or "-"
        rich_console.print(f"  {entry.get('param')} = {detail}")
    return 0
