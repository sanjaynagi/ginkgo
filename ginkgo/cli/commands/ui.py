"""UI command handlers."""

from __future__ import annotations

import subprocess
import sys
import webbrowser

from ginkgo.cli.common import RUNS_ROOT, console
from ginkgo.ui import create_ui_server


def _open_ui_url(url: str) -> bool:
    """Open the local UI URL in the user's browser.

    Parameters
    ----------
    url : str
        Fully-qualified local UI URL.

    Returns
    -------
    bool
        True when a browser launch was initiated successfully.
    """
    if sys.platform == "darwin":
        result = subprocess.run(  # noqa: S603 - local OS launcher
            ["open", url],
            check=False,
            capture_output=True,
            text=True,
        )
        return result.returncode == 0

    return bool(webbrowser.open(url, new=2))


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
        opened = _open_ui_url(url)
        if not opened:
            rich_console.print("[yellow]⚠[/] Could not open a browser tab automatically.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        rich_console.print("\n[green]✓[/] Stopped Ginkgo UI")
    finally:
        server.server_close()
    return 0
