"""``ginkgo report`` — export a static HTML report for a completed run."""

from __future__ import annotations

import argparse
import subprocess
import sys
import webbrowser
from pathlib import Path

from rich.text import Text

from ginkgo.cli.common import console, open_run
from ginkgo.formatting import format_bytes
from ginkgo.workspace_layout import WorkspaceLayout
from ginkgo_report.render import export_report
from ginkgo_report.sizing import SizingPolicy

#: A single-file report above this size is too large to pass around comfortably,
#: usually because a notebook page embeds large figures or tables.
LARGE_SINGLE_FILE_BYTES = 25 * 1024 * 1024


def add_report_arguments(parser: argparse.ArgumentParser) -> None:
    """Declare ``ginkgo report``'s arguments."""
    parser.add_argument("run_id", nargs="?")
    parser.add_argument(
        "--out",
        default=None,
        help=(
            "Where to write the report. A bundle is written into this directory "
            "(default: <workspace>/.ginkgo/reports/<run-id>/). With --single-file, "
            "a path ending in .html is the file itself and any other path is a "
            "directory to write index.html into, and any other extension is refused "
            "(default: <workspace>/.ginkgo/reports/<run-id>.html)."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Write into an --out destination that holds files ginkgo did not write, "
            "replacing only the report's own files. Without this, such a "
            "destination is left untouched."
        ),
    )
    parser.add_argument(
        "--single-file",
        action="store_true",
        help=(
            "Emit one HTML file that opens offline, with CSS, fonts, script and "
            "rendered pages inlined; a marimo page is inlined as a static copy of "
            "its outputs. Logs are linked but not included."
        ),
    )
    parser.add_argument(
        "--hide-code",
        action="store_true",
        help=(
            "Leave each cell's code out of the static copies of notebook pages. "
            "By default it is shown, folded above the cell's output."
        ),
    )
    parser.add_argument(
        "--max-log-lines",
        type=int,
        default=80,
        help="Trailing log lines to retain for failed tasks (default: 80).",
    )
    parser.add_argument(
        "--open",
        dest="open",
        action="store_true",
        default=False,
        help="Open the report in the default browser once it is written.",
    )
    parser.add_argument(
        "--no-open",
        dest="open",
        action="store_false",
        help="Do not open the report (the default).",
    )


def command_report(args: argparse.Namespace) -> int:
    """Handle ``ginkgo report``."""
    rich_console = console(sys.stdout)

    try:
        with open_run(args.run_id) as (reader, run_id):
            summary = reader.run(run_id)
            measured_runs = reader.measured_executions(
                task.cache_key
                for task in summary.tasks
                if task.cache_label == "hit" and task.cache_key
            )
    except FileNotFoundError as exc:
        rich_console.print(Text.assemble(("✖ ", "red"), str(exc)))
        return 1
    run_dir = summary.run_dir
    policy = SizingPolicy(log_lines=int(args.max_log_lines) if args.max_log_lines else 80)

    try:
        out = _resolve_destination(run_dir=run_dir, out=args.out, single_file=args.single_file)
        result = export_report(
            summary=summary,
            out=out,
            policy=policy,
            single_file=bool(args.single_file),
            managed_destination=args.out is None,
            force=bool(args.force),
            measured_runs=measured_runs,
            include_code=not args.hide_code,
        )
    except ValueError as exc:
        rich_console.print(Text.assemble(("✖ ", "red"), str(exc)))
        return 1
    except FileExistsError as exc:
        rich_console.print(Text.assemble(("✖ ", "red"), str(exc)))
        rich_console.print("  Pass --force to write into it anyway, or choose another --out.")
        return 1

    rich_console.print("[bold green]🌿 ginkgo report[/]\n")
    rich_console.print(f"[cyan]Run:[/] [bold]{run_dir.name}[/]")
    rich_console.print(f"[cyan]Output:[/] [bold]{result.index_path}[/]", soft_wrap=True)
    if result.omitted_links:
        listed = ", ".join(result.omitted_links)
        rich_console.print(
            f"[yellow]⚠[/] The single-file report links {len(result.omitted_links)} "
            f"file(s) it does not include, so those links will not open: {listed}. "
            "Export without --single-file to include them."
        )
    size = result.index_path.stat().st_size
    if result.single_file and size > LARGE_SINGLE_FILE_BYTES:
        rich_console.print(
            f"[yellow]⚠[/] The single-file report is {format_bytes(size)}, mostly "
            "the rendered pages it embeds. Export without --single-file to keep "
            "them as separate files."
        )

    if args.open:
        opened = _open_path(result.index_path)
        if not opened:
            rich_console.print("[yellow]⚠[/] Could not open a browser automatically.")

    return 0


def _resolve_destination(*, run_dir: Path, out: str | None, single_file: bool) -> Path:
    """Resolve where the report is written: the bundle's directory or the single file.

    A single-file report defaults to ``.ginkgo/reports/<run-id>.html``, beside
    rather than inside the run's bundle directory, so neither replaces the other.

    Raises
    ------
    ValueError
        When a single-file ``out`` names a file that is not HTML, such as
        ``report.txt``, rather than silently making it a directory.
    """
    if out is not None:
        path = Path(out).resolve()
        if not single_file or path.suffix.lower() in {".html", ".htm"}:
            return path
        if path.suffix and not path.is_dir():
            raise ValueError(
                f"--out {out} names a {path.suffix} file, but a single-file report is "
                "HTML: name it with .html, or give a directory to write index.html into"
            )
        return path / "index.html"
    workspace = run_dir.parents[1] if len(run_dir.parents) >= 2 else run_dir.parent
    reports_root = WorkspaceLayout(root=workspace).reports
    if single_file:
        return (reports_root / f"{run_dir.name}.html").resolve()
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
