"""``ginkgo lineage`` — what an asset was built from, and what came of it."""

from __future__ import annotations

import json

from rich.tree import Tree

from ginkgo import query
from ginkgo.cli.common import stdout_console
from ginkgo.core.asset import AssetVersion
from ginkgo.query import LineageGraph, Provenance

__all__ = ["command_lineage", "render_lineage_tree"]

_EMPTY_WALK = {
    "upstream": "No upstream assets: no catalogued version was consumed to build this one.",
    "downstream": "No downstream assets: no catalogued version was derived from this one.",
}
"""What to say when the walk found nothing but the version it started at."""


def command_lineage(args) -> int:
    """Handle ``ginkgo lineage`` — trace one asset, path, or artifact."""
    rich_console = stdout_console()
    direction = "downstream" if getattr(args, "downstream", False) else "upstream"
    as_json = bool(getattr(args, "json", False))

    # An unknown asset or path raises, and the CLI's own error handler reports
    # it — the same path `ginkgo asset` takes for the same mistake.
    with query.open(missing_ok=True) as reader:
        if ":" in args.target:
            key_text, separator, selector = args.target.partition("@")
            graph = reader.lineage(
                key_text,
                selector if separator else None,
                direction=direction,
                depth=getattr(args, "depth", None),
            )
            return _render_graph(rich_console, graph=graph, as_json=as_json)
        return _render_why(rich_console, provenance=reader.why(args.target), as_json=as_json)


def _render_graph(rich_console, *, graph: LineageGraph, as_json: bool) -> int:
    """Print one lineage graph as JSON or as a tree."""
    if as_json:
        print(json.dumps(graph.to_payload(), indent=2, sort_keys=True))
        return 0
    rich_console.print(
        f"[bold green]🌿 ginkgo lineage[/] [dim]{graph.direction}[/]\n",
    )
    rich_console.print(render_lineage_tree(graph))
    if len(graph.versions) == 1:
        rich_console.print(f"\n[dim]{_EMPTY_WALK[graph.direction]}[/]")
    return 0


def render_lineage_tree(graph: LineageGraph) -> Tree:
    """Build the Rich tree for one lineage graph.

    Each node names an asset version and the task and run that made it;
    children are the versions one hop away in the walk's direction. A version
    reached by more than one path is shown once and then marked, so a diamond
    does not become an infinite tree.
    """
    tree = Tree(_label(graph.versions[graph.root.version_id]))
    _grow(tree, graph=graph, version_id=graph.root.version_id, seen={graph.root.version_id})
    return tree


def _grow(branch: Tree, *, graph: LineageGraph, version_id: str, seen: set[str]) -> None:
    """Attach the versions one hop from *version_id*, depth-first."""
    for neighbour in graph.neighbours(version_id):
        version = graph.versions.get(neighbour)
        if version is None:
            continue
        if neighbour in seen:
            branch.add(f"{_label(version)} [dim](already shown)[/]")
            continue
        child = branch.add(_label(version))
        _grow(child, graph=graph, version_id=neighbour, seen=seen | {neighbour})


def _label(version: AssetVersion) -> str:
    """Render one asset version as a single tree line."""
    return (
        f"[bold]{version.key}[/][dim]@{version.version_id}[/] "
        f"[dim]task={version.producer_task} run={version.run_id}[/]"
    )


def _render_why(rich_console, *, provenance: Provenance, as_json: bool) -> int:
    """Print what produced one artifact."""
    if as_json:
        print(json.dumps(provenance.to_payload(), indent=2, sort_keys=True))
        return 0
    rich_console.print("[bold green]🌿 ginkgo lineage[/] [bold]why[/]\n")
    if provenance.path:
        rich_console.print(f"Path: {provenance.path}")
    rich_console.print(f"Artifact ID: [bold]{provenance.artifact_id}[/]")
    rich_console.print(
        f"Produced by: {provenance.task_name or '-'} [dim]({provenance.task_id})[/]"
    )
    rich_console.print(f"Run: {provenance.run_id or '-'}")
    rich_console.print(f"Cache key: {provenance.cache_key or '-'}")
    if provenance.asset_key:
        rich_console.print(f"Asset: {provenance.asset_key}@{provenance.version_id}")
    if not provenance.inputs:
        return 0
    rich_console.print("\n[bold]Inputs[/]")
    for entry in provenance.inputs:
        detail = entry.get("asset_key") or entry.get("digest") or entry.get("value_summary") or "-"
        rich_console.print(f"  {entry.get('param')} = {detail}")
    return 0
