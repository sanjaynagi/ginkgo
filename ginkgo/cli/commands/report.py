"""``ginkgo report`` — export a static HTML report for a completed run."""

from __future__ import annotations

import subprocess
import sys
import webbrowser
from pathlib import Path

from ginkgo.cli.common import console, resolve_run_dir
from ginkgo.reporting import SizingPolicy, export_report


def command_report(args) -> int:
    """Handle ``ginkgo report``."""
    rich_console = console(sys.stdout)

    try:
        run_dir = resolve_run_dir(args.run_id)
    except FileNotFoundError as exc:
        rich_console.print(f"[red]✖[/] {exc}")
        return 1

    out_dir = _resolve_output_dir(run_dir=run_dir, out=args.out, single_file=args.single_file)
    policy = SizingPolicy(
        embed_full_assets=bool(args.embed_full_assets),
        log_lines=int(args.max_log_lines) if args.max_log_lines else 80,
    )

    try:
        result = export_report(
            run_dir=run_dir,
            out_dir=out_dir,
            policy=policy,
            single_file=bool(args.single_file),
        )
    except ValueError as exc:
        rich_console.print(f"[red]✖[/] {exc}")
        return 1
    except FileExistsError as exc:
        rich_console.print(f"[red]✖[/] {exc}")
        return 1

    rich_console.print("[bold green]🌿 ginkgo report[/]\n")
    rich_console.print(f"[cyan]Run:[/] [bold]{run_dir.name}[/]")
    rich_console.print(f"[cyan]Output:[/] [bold]{result.index_path}[/]")

    if args.open:
        opened = _open_path(result.index_path)
        if not opened:
            rich_console.print("[yellow]⚠[/] Could not open a browser automatically.")

    return 0


def _resolve_output_dir(*, run_dir: Path, out: str | None, single_file: bool) -> Path:
    """Resolve the destination directory for the bundle."""
    if out is not None:
        return Path(out).resolve()
    # Default: <workspace>/.ginkgo/reports/<run-id>/
    workspace = run_dir.parents[1] if len(run_dir.parents) >= 2 else run_dir.parent
    reports_root = workspace / "reports"
    return (reports_root / run_dir.name).resolve()


def _open_path(path: Path) -> bool:
    """Open a local file path in the user's default browser."""
    url = path.resolve().as_uri()
    if sys.platform == "darwin":
        result = subprocess.run(  # noqa: S603 - local OS launcher
            ["open", url], check=False, capture_output=True, text=True
        )
        return result.returncode == 0
    return bool(webbrowser.open(url, new=2))
