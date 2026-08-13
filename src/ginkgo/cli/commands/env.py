"""Environment command handlers."""

from __future__ import annotations

import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from rich import box
from rich.table import Table
from rich.text import Text

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import load_runtime_config
from ginkgo.envs.pixi import (
    PixiEnvNotFoundError,
    PixiRegistry,
    _env_manifest,
    _list_envs,
    resolve_shared_env_root,
)


@dataclass(frozen=True)
class EnvEntryRow:
    """Display metadata for a project-local Pixi environment."""

    env: str
    manifest: Path
    install_dir: Path
    installed: bool


def command_env(args) -> int:
    """Handle ``ginkgo env`` subcommands."""
    rich_console = console(sys.stdout)
    registry = _build_project_registry(project_root=Path.cwd())

    if args.env_command == "ls":
        rich_console.print("[bold green]🌿 ginkgo env[/] [bold]ls[/]\n")
        entries = list_project_envs(registry=registry)
        if not entries:
            rich_console.print(
                f"[dim]No Pixi environments found. Searched: {_searched_roots(registry=registry)}[/]"
            )
            return 0

        table = Table(
            box=box.SQUARE,
            border_style="#0f766e",
            header_style="bold #134e4a",
            expand=False,
        )
        table.add_column("Env", style="bold")
        table.add_column("Installed", no_wrap=True)
        table.add_column("Manifest")
        table.add_column("Install Dir")
        for entry in entries:
            table.add_row(
                entry.env,
                "yes" if entry.installed else "no",
                str(entry.manifest),
                str(entry.install_dir),
            )
        rich_console.print(table)
        return 0

    rich_console.print("[bold green]🌿 ginkgo env[/] [bold]clear[/]\n")
    targets = _clear_targets(registry=registry, env=args.env, clear_all=args.all)
    installed_targets = [target for target in targets if target.install_dir.exists()]

    if args.dry_run:
        rich_console.print(
            f"[cyan]Preview:[/] {len(installed_targets)} Pixi "
            f"{'env' if len(installed_targets) == 1 else 'envs'} would be removed."
        )
        for entry in installed_targets:
            rich_console.print(f"[dim]-[/] {entry.env}: {entry.install_dir}")
        if not installed_targets:
            rich_console.print("[dim]Nothing to remove.[/]")
        return 0

    for entry in installed_targets:
        shutil.rmtree(entry.install_dir)

    if not installed_targets:
        rich_console.print("[dim]Nothing to remove.[/]")
        return 0

    message = Text()
    message.append("✓ ", style="green")
    message.append("Removed ")
    message.append(str(len(installed_targets)), style="bold")
    message.append(" Pixi ")
    message.append("env" if len(installed_targets) == 1 else "envs")
    rich_console.print(message)
    return 0


def _build_project_registry(*, project_root: Path) -> PixiRegistry:
    """Build a Pixi registry with the same discovery roots ``ginkgo run`` uses.

    Parameters
    ----------
    project_root : Path
        Directory the command was invoked from.

    Returns
    -------
    PixiRegistry
        Registry anchored on the resolved workflow's directory when a workflow
        can be discovered, and on ``project_root`` alone otherwise.
    """
    try:
        workflow_root = resolve_workflow_path(project_root=project_root, workflow=None).path.parent
    except (OSError, RuntimeError):
        # Discovery is best-effort here: env ls/clear don't need a workflow,
        # so any failure to locate one (missing file, unreadable directory,
        # ambiguous candidates) just falls back to project_root alone.
        workflow_root = None
    return PixiRegistry(
        project_root=project_root,
        workflow_root=workflow_root,
        shared_env_root=_shared_env_root(project_root=project_root),
    )


def _shared_env_root(*, project_root: Path) -> Path | None:
    """Return the configured shared env prefix, ignoring an unreadable config."""
    try:
        config = load_runtime_config(project_root=project_root)
    except Exception:  # noqa: BLE001
        return None
    return resolve_shared_env_root(config=config, project_root=project_root)


def _searched_roots(*, registry: PixiRegistry) -> str:
    """Describe the discovery roots searched, with the environments each holds."""
    return ", ".join(
        f"{envs_dir} (envs: {_list_envs(envs_dir)})" for envs_dir in registry.env_directories
    )


def list_project_envs(*, registry: PixiRegistry) -> list[EnvEntryRow]:
    """Return discoverable project-local Pixi environments."""
    entries: list[EnvEntryRow] = []
    seen_manifests: set[Path] = set()
    for envs_dir in registry.env_directories:
        if not envs_dir.is_dir():
            continue
        for child in sorted(envs_dir.iterdir(), key=lambda path: path.name):
            if not child.is_dir():
                continue
            manifest = _env_manifest(child)
            if manifest is None:
                continue
            resolved_manifest = manifest.resolve()
            if resolved_manifest in seen_manifests:
                continue
            seen_manifests.add(resolved_manifest)

            install_dir = registry.install_dir_for(manifest=manifest)
            entries.append(
                EnvEntryRow(
                    env=child.name,
                    manifest=resolved_manifest,
                    install_dir=install_dir.resolve(strict=False),
                    installed=install_dir.exists(),
                )
            )
    return entries


def _clear_targets(
    *, registry: PixiRegistry, env: str | None, clear_all: bool
) -> list[EnvEntryRow]:
    """Resolve the env install directories targeted by ``ginkgo env clear``."""
    if clear_all == (env is not None):
        raise ValueError("Specify exactly one of <env> or --all.")

    if clear_all:
        return list_project_envs(registry=registry)

    assert env is not None
    try:
        manifest = registry.resolve(env=env)
    except PixiEnvNotFoundError:
        # PixiEnvNotFoundError names only the first discovery root; report all of
        # them so "wrong directory" is distinguishable from "env does not exist".
        raise ValueError(
            f"Pixi environment {env!r} not found. Searched: {_searched_roots(registry=registry)}"
        ) from None
    install_dir = registry.install_dir_for(manifest=manifest)
    return [
        EnvEntryRow(
            env=env,
            manifest=manifest,
            install_dir=install_dir.resolve(strict=False),
            installed=install_dir.exists(),
        )
    ]
