"""UI command handlers."""

from __future__ import annotations

import sys
import webbrowser

from ginkgo.cli.common import RUNS_ROOT, console
from ginkgo.ui import create_ui_server


def command_ui(args) -> int:
    """Handle ``ginkgo ui``."""
    rich_console = console(sys.stdout)
    server = create_ui_server(
        host=args.host,
        port=args.port,
        runs_root=RUNS_ROOT,
        selected_run_id=args.run_id,
    )
    actual_host, actual_port = server.server_address[:2]
    url = f"http://{actual_host}:{actual_port}/"

    rich_console.print("[bold green]🌿 ginkgo ui[/]\n")
    rich_console.print(f"[cyan]URL:[/] [bold]{url}[/]")
    rich_console.print("[dim]Press Ctrl-C to stop the local UI server.[/]")
    if args.open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        rich_console.print("\n[green]✓[/] Stopped Ginkgo UI")
    finally:
        server.server_close()
    return 0
