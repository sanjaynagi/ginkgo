"""``ginkgo plugins`` — the installed plugins and what each contributes."""

from __future__ import annotations

import json

from ginkgo.cli.common import stdout_console
from ginkgo.plugins.registry import describe_key, plugin_registry

__all__ = ["command_plugins"]


def command_plugins(args) -> int:
    """Handle ``ginkgo plugins`` — list each plugin's contributions and any load failure.

    Returns 1 when a plugin failed to load, so a check in CI notices it.
    """
    registry = plugin_registry()
    if args.json:
        payload = {
            "plugins": [
                {
                    "name": plugin.name,
                    "contributions": [
                        {"point": contribution.point_name, "keys": list(contribution.keys)}
                        for contribution in plugin.contributions
                    ],
                }
                for plugin in registry.plugins
            ],
            "failures": [
                {
                    "entry_point": failure.entry_point,
                    "target": failure.target,
                    "reason": failure.reason,
                }
                for failure in registry.failures
            ],
        }
        print(json.dumps(payload, indent=2))
        return 1 if registry.failures else 0

    rich_console = stdout_console()
    rich_console.print("[bold green]🌿 ginkgo plugins[/]\n")
    for plugin in registry.plugins:
        rich_console.print(f"[bold]{plugin.name}[/]")
        for contribution in plugin.contributions:
            keys = ", ".join(key or describe_key(key) for key in contribution.keys)
            rich_console.print(f"  {contribution.point_name} [cyan]{keys}[/]")
    for failure in registry.failures:
        rich_console.print(f"[red]✖[/] {failure.describe()}")
    return 1 if registry.failures else 0
