"""Asset command handlers."""

from __future__ import annotations

import sys
from dataclasses import dataclass

from rich import box
from rich.table import Table

from ginkgo.cli.common import ASSETS_ROOT, console
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.core.asset import AssetKey


def command_asset(args) -> int:
    """Handle ``ginkgo asset`` subcommands."""
    is_tty = getattr(sys.stdout, "isatty", lambda: False)()
    rich_console = console(sys.stdout, width=None if is_tty else 160)
    store = AssetStore(root=ASSETS_ROOT)

    if args.asset_command == "ls":
        rich_console.print("[bold green]🌿 ginkgo asset[/] [bold]ls[/]\n")
        rows = list_asset_rows(store=store)
        if not rows:
            rich_console.print("[dim]No assets found.[/]")
            return 0

        table = Table(
            box=box.SQUARE,
            border_style="#0f766e",
            header_style="bold #134e4a",
            expand=False,
        )
        table.add_column("Asset Key", style="bold", overflow="fold")
        table.add_column("Latest Version", overflow="fold")
        table.add_column("Versions", justify="right")
        for row in rows:
            table.add_row(row.asset_key, row.latest_version_id, str(row.version_count))
        rich_console.print(table)
        return 0

    if args.asset_command == "versions":
        key = parse_asset_key(args.key)
        rich_console.print("[bold green]🌿 ginkgo asset[/] [bold]versions[/]\n")
        versions = store.list_versions(key=key)
        if not versions:
            rich_console.print(f"[dim]No versions found for {key}.[/]")
            return 0

        index = store._load_index(key)
        aliases = {
            str(version_id): alias for alias, version_id in dict(index.get("aliases", {})).items()
        }
        rich_console.print(f"Asset Key: [bold]{key}[/]")
        for version in versions:
            rich_console.print(
                " | ".join(
                    [
                        f"version={version.version_id}",
                        f"alias={aliases.get(version.version_id, '-')}",
                        f"run={version.run_id}",
                        f"artifact={version.artifact_id}",
                        f"hash={version.content_hash}",
                    ]
                )
            )
        return 0

    if args.asset_command == "show":
        asset_ref = parse_asset_selector(args.ref)
        version = store.resolve_version(key=asset_ref.key, selector=asset_ref.selector)
        return render_asset_show(console=rich_console, version=version)

    asset_ref = parse_asset_selector(args.ref)
    version = store.resolve_version(key=asset_ref.key, selector=asset_ref.selector)
    artifact_store = LocalArtifactStore(root=ASSETS_ROOT.parent / "artifacts")
    artifact_path = (
        artifact_store.artifact_path(artifact_id=version.artifact_id)
        if artifact_store.exists(artifact_id=version.artifact_id)
        else None
    )

    rich_console.print("[bold green]🌿 ginkgo asset[/] [bold]inspect[/]\n")
    rich_console.print(f"Asset Key: [bold]{version.key}[/]")
    rich_console.print(f"Version: [bold]{version.version_id}[/]")
    rich_console.print(f"Kind: {version.kind}")
    rich_console.print(f"Run: {version.run_id}")
    rich_console.print(f"Producer: {version.producer_task}")
    rich_console.print(f"Artifact ID: {version.artifact_id}")
    rich_console.print(f"Content Hash: {version.content_hash}")
    rich_console.print(f"Created: {version.created_at}")
    rich_console.print(f"Artifact Path: {artifact_path or '-'}")
    if version.metadata:
        rich_console.print(f"Metadata: {version.metadata}")
    return 0


@dataclass(frozen=True)
class AssetListRow:
    """Summary row for ``ginkgo asset ls``."""

    asset_key: str
    latest_version_id: str
    version_count: int


@dataclass(frozen=True)
class AssetSelector:
    """Parsed asset selector from CLI input."""

    key: AssetKey
    selector: str | None


def list_asset_rows(*, store: AssetStore) -> list[AssetListRow]:
    """Return display rows for all known asset keys."""
    rows: list[AssetListRow] = []
    for key in store.list_asset_keys():
        latest = store.get_latest_version(key=key)
        versions = store.list_versions(key=key)
        rows.append(
            AssetListRow(
                asset_key=str(key),
                latest_version_id=latest.version_id if latest is not None else "-",
                version_count=len(versions),
            )
        )
    return rows


def parse_asset_key(value: str) -> AssetKey:
    """Parse a CLI asset key.

    Accepts ``namespace:name`` or ``name``. Bare names default to ``file``.
    """
    namespace, separator, name = value.partition(":")
    if separator:
        if not namespace or not name:
            raise ValueError(f"Invalid asset key: {value!r}")
        return AssetKey(namespace=namespace, name=name)
    if not namespace:
        raise ValueError(f"Invalid asset key: {value!r}")
    return AssetKey(namespace="file", name=namespace)


def parse_asset_selector(value: str) -> AssetSelector:
    """Parse ``<key>[@<version|alias>]`` syntax."""
    key_text, separator, selector = value.partition("@")
    key = parse_asset_key(key_text)
    return AssetSelector(key=key, selector=selector if separator else None)


def render_asset_show(*, console, version) -> int:
    """Render a Rich metadata summary for a wrapped asset version.

    Parameters
    ----------
    console : rich.console.Console
        Target console.
    version : AssetVersion
        Version resolved by ``asset show``.

    Returns
    -------
    int
        Process exit code.
    """
    from rich.panel import Panel

    metadata = dict(version.metadata)
    namespace = version.key.namespace

    console.print("[bold green]🌿 ginkgo asset[/] [bold]show[/]\n")
    console.print(f"Asset Key: [bold]{version.key}[/]")
    console.print(f"Version: {version.version_id}")
    console.print(f"Kind: {version.kind}")
    console.print(f"Sub-kind: {metadata.get('sub_kind', '-')}")
    console.print(f"Artifact ID: {version.artifact_id}")

    if namespace == "table":
        _render_table_metadata(console=console, metadata=metadata)
    elif namespace == "array":
        _render_array_metadata(console=console, metadata=metadata)
    elif namespace == "fig":
        _render_fig_metadata(console=console, metadata=metadata)
    elif namespace == "text":
        _render_text_metadata(console=console, metadata=metadata)
    else:
        console.print(Panel.fit(repr(metadata), title="metadata"))

    return 0


def _render_table_metadata(*, console, metadata: dict) -> None:
    """Print schema and row-count for a table asset."""
    console.print(f"Row count: {metadata.get('row_count', '-')}")
    console.print(f"Byte size: {metadata.get('byte_size', '-')}")
    schema = metadata.get("schema") or []
    if not schema:
        return
    schema_table = Table(
        box=box.SQUARE,
        border_style="#0f766e",
        header_style="bold #134e4a",
        expand=False,
    )
    schema_table.add_column("Column", style="bold")
    schema_table.add_column("Dtype")
    for entry in schema:
        schema_table.add_row(str(entry.get("name")), str(entry.get("dtype")))
    console.print(schema_table)


def _render_array_metadata(*, console, metadata: dict) -> None:
    """Print shape, dtype, and chunking for an array asset."""
    console.print(f"Shape: {metadata.get('shape')}")
    console.print(f"Dtype: {metadata.get('dtype')}")
    console.print(f"Chunks: {metadata.get('chunks')}")
    console.print(f"Byte size: {metadata.get('byte_size', '-')}")
    coords = metadata.get("coordinates")
    if coords:
        console.print("Coordinates:")
        for name, sample in coords.items():
            console.print(f"  {name}: {sample}")


def _render_fig_metadata(*, console, metadata: dict) -> None:
    """Print source format and dimensions for a figure asset."""
    console.print(f"Source format: {metadata.get('source_format')}")
    console.print(f"Byte size: {metadata.get('byte_size', '-')}")
    dimensions = metadata.get("dimensions")
    if dimensions:
        console.print(f"Dimensions: {dimensions.get('width')}x{dimensions.get('height')}")


def _render_text_metadata(*, console, metadata: dict) -> None:
    """Print format, line count, and byte size for a text asset."""
    console.print(f"Format: {metadata.get('format')}")
    console.print(f"Byte size: {metadata.get('byte_size', '-')}")
    console.print(f"Lines: {metadata.get('line_count', '-')}")
