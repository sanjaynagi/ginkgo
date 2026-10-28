"""``ginkgo why`` — which task produced a file, and from what."""

from __future__ import annotations

import json
import sys
from typing import Any

from rich.markup import escape

from ginkgo import query
from ginkgo.cli.common import console
from ginkgo.query import Provenance
from ginkgo.store.jsonio import loads

__all__ = ["command_why"]


def command_why(args) -> int:
    """Handle ``ginkgo why`` — trace one materialized path or artifact id."""
    rich_console = console(sys.stdout)

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
        rich_console.print(f"Path: {escape(provenance.path)}", soft_wrap=True)
    rich_console.print(f"Artifact ID: [bold]{provenance.artifact_id}[/]")
    producer = f"{provenance.task_name or '-'} [dim]({provenance.task_id})[/]"
    if provenance.containing_output:
        inside = escape(provenance.containing_output)
        rich_console.print(f"Inside {inside}, produced by {producer}", soft_wrap=True)
    else:
        rich_console.print(f"Produced by: {producer}")
    rich_console.print(f"Run: {provenance.run_id or '-'}")
    rich_console.print(f"Cache key: {provenance.cache_key or '-'}")
    if not provenance.inputs:
        return 0
    rich_console.print("\n[bold]Inputs[/]")
    for entry in provenance.inputs:
        rich_console.print(f"  {escape(str(entry.get('param')))} = {_input_detail(entry)}")
    return 0


# Inputs tracked by their bytes or remote version: the digest says which bytes.
_DIGEST_TRACKED = frozenset({"content", "version"})


def _input_detail(entry: dict[str, Any]) -> str:
    """Return one recorded input as Rich markup.

    The value is shown decoded, and a file or remote input tracked by its bytes
    also shows a short content digest, which is what its cache key used.
    """
    digest = entry.get("digest")
    short = f"[dim]({escape(digest[:12])})[/]" if digest else None
    summary = entry.get("value_summary")
    if summary is None:
        return short or "-"
    value = loads(summary)
    text = escape(value if isinstance(value, str) else json.dumps(value))
    if short is not None and entry.get("tracking") in _DIGEST_TRACKED:
        return f"{text} {short}"
    return text
