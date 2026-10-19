"""``ginkgo plugins`` — the installed plugins and what each contributes."""

from __future__ import annotations

import json

from ginkgo.cli.common import stdout_console
from ginkgo.plugins.registry import plugin_registry

__all__ = ["command_plugins"]


def command_plugins(args) -> int:
    """Handle ``ginkgo plugins`` — list every loaded plugin's contributions."""
    plugins = plugin_registry().plugins
    if args.json:
        payload = [
            {
                "name": plugin.name,
                "contributions": [
                    {"point": contribution.point_name, "key": contribution.key}
                    for contribution in plugin.contributions
                ],
            }
            for plugin in plugins
        ]
        print(json.dumps(payload, indent=2))
        return 0

    rich_console = stdout_console()
    rich_console.print("[bold green]🌿 ginkgo plugins[/]\n")
    for plugin in plugins:
        rich_console.print(f"[bold]{plugin.name}[/]")
        for contribution in plugin.contributions:
            key = contribution.key or "(bare name)"
            rich_console.print(f"  {contribution.point_name} [cyan]{key}[/]")
    return 0
