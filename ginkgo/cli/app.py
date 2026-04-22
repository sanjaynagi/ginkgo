"""CLI parser construction and command dispatch."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from rich.text import Text

from ginkgo.cli.commands.asset import command_asset
from ginkgo.cli.commands.cache import command_cache
from ginkgo.cli.commands.debug import command_debug
from ginkgo.cli.commands.doctor import command_doctor
from ginkgo.cli.commands.env import command_env
from ginkgo.cli.commands.init import command_init
from ginkgo.cli.commands.inspect import command_inspect
from ginkgo.cli.commands.models import command_models
from ginkgo.cli.commands.notebooks import command_notebooks
from ginkgo.cli.commands.report import command_report
from ginkgo.cli.commands.run import command_run
from ginkgo.cli.commands.secrets import command_secrets
from ginkgo.cli.commands.test import command_test
from ginkgo.cli.commands.ui import command_ui
from ginkgo.cli.common import RunMode, console


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``ginkgo`` CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run":
            return command_run(args, output_mode=_run_mode_from_args(args))
        if args.command == "cache":
            return command_cache(args)
        if args.command == "asset":
            return command_asset(args)
        if args.command == "env":
            return command_env(args)
        if args.command == "debug":
            return command_debug(args)
        if args.command == "doctor":
            return command_doctor(args)
        if args.command == "test":
            return command_test(args)
        if args.command == "init":
            return command_init(args)
        if args.command == "inspect":
            return command_inspect(args)
        if args.command == "models":
            return command_models(args)
        if args.command == "notebooks":
            return command_notebooks(args)
        if args.command == "secrets":
            return command_secrets(args)
        if args.command == "ui":
            return command_ui(args)
        if args.command == "report":
            return command_report(args)
    except BaseException as exc:
        rich_console = console(sys.stderr)
        rich_console.print(Text("✖ ", style="bold red"), Text(str(exc)), sep="")
        return 1

    parser.error("missing command")
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ginkgo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("workflow", nargs="?")
    run_parser.add_argument("--config", action="append", default=[])
    run_parser.add_argument("--jobs", type=int, default=None)
    run_parser.add_argument("--cores", type=int, default=None)
    run_parser.add_argument("--memory", type=int, default=None)
    run_parser.add_argument("--dry-run", action="store_true")
    run_parser.add_argument("--verbose", action="store_true")
    run_parser.add_argument("--agent", action="store_true")
    run_parser.add_argument(
        "--trust-workspace",
        action="store_true",
        help="Skip content hashing; use stat-based checks only (faster warm runs)",
    )
    run_parser.add_argument(
        "--profile",
        action="store_true",
        help="Record a coarse runtime phase profile and print it at run end",
    )
    run_parser.add_argument(
        "--executor",
        choices=["local", "k8s", "batch"],
        default="local",
        help="Task execution backend: 'local' (default), 'k8s' for Kubernetes, or 'batch' for GCP Batch",
    )

    cache_parser = subparsers.add_parser("cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)
    cache_subparsers.add_parser("ls")
    clear_parser = cache_subparsers.add_parser("clear")
    clear_parser.add_argument("cache_key")
    explain_parser = cache_subparsers.add_parser("explain")
    explain_parser.add_argument("--run", required=True, dest="run_id")
    prune_parser = cache_subparsers.add_parser("prune")
    prune_parser.add_argument("--older-than", default=None)
    prune_parser.add_argument(
        "--max-size",
        default=None,
        help="Prune oldest entries until total size is at or below the target (e.g. 2GB, 500MB).",
    )
    prune_parser.add_argument(
        "--max-entries",
        type=int,
        default=None,
        help="Prune oldest entries until entry count is at or below this number.",
    )
    prune_parser.add_argument("--dry-run", action="store_true")

    asset_parser = subparsers.add_parser("asset")
    asset_subparsers = asset_parser.add_subparsers(dest="asset_command", required=True)
    asset_subparsers.add_parser("ls")
    asset_versions_parser = asset_subparsers.add_parser("versions")
    asset_versions_parser.add_argument("key")
    asset_inspect_parser = asset_subparsers.add_parser("inspect")
    asset_inspect_parser.add_argument("ref")
    asset_show_parser = asset_subparsers.add_parser("show")
    asset_show_parser.add_argument("ref")

    env_parser = subparsers.add_parser("env")
    env_subparsers = env_parser.add_subparsers(dest="env_command", required=True)
    env_subparsers.add_parser("ls")
    env_clear_parser = env_subparsers.add_parser("clear")
    env_clear_parser.add_argument("env", nargs="?")
    env_clear_parser.add_argument("--all", action="store_true")
    env_clear_parser.add_argument("--dry-run", action="store_true")

    debug_parser = subparsers.add_parser("debug")
    debug_parser.add_argument("run_id", nargs="?")
    debug_parser.add_argument("--json", action="store_true")

    doctor_parser = subparsers.add_parser("doctor")
    doctor_parser.add_argument("workflow", nargs="?")
    doctor_parser.add_argument("--config", action="append", default=[])
    doctor_parser.add_argument("--json", action="store_true")

    test_parser = subparsers.add_parser("test")
    test_parser.add_argument("--dry-run", action="store_true")

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("directory", nargs="?", default=".")
    init_parser.add_argument("--no-skills", action="store_true")
    init_parser.add_argument("--skills-only", action="store_true")
    init_parser.add_argument("--force", action="store_true")

    inspect_parser = subparsers.add_parser("inspect")
    inspect_subparsers = inspect_parser.add_subparsers(dest="inspect_command", required=True)
    inspect_workflow_parser = inspect_subparsers.add_parser("workflow")
    inspect_workflow_parser.add_argument("workflow", nargs="?")
    inspect_workflow_parser.add_argument("--config", action="append", default=[])
    inspect_run_parser = inspect_subparsers.add_parser("run")
    inspect_run_parser.add_argument("run_id")

    models_parser = subparsers.add_parser("models")
    models_parser.add_argument("run_id", nargs="?")

    subparsers.add_parser("notebooks")

    ui_parser = subparsers.add_parser("ui")
    ui_parser.add_argument("run_id", nargs="?")
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=7777)
    ui_parser.add_argument("--open", dest="open", action="store_true", default=True)
    ui_parser.add_argument("--no-open", dest="open", action="store_false")

    report_parser = subparsers.add_parser("report")
    report_parser.add_argument("run_id", nargs="?")
    report_parser.add_argument(
        "--out",
        default=None,
        help="Destination directory (default: <workspace>/.ginkgo/reports/<run-id>/)",
    )
    report_parser.add_argument(
        "--single-file",
        action="store_true",
        help="Emit one HTML file with CSS, fonts, and figures inlined as data URIs.",
    )
    report_parser.add_argument(
        "--embed-full-assets",
        action="store_true",
        help="Copy full artifact bytes into the bundle alongside the rendered previews.",
    )
    report_parser.add_argument(
        "--max-log-lines",
        type=int,
        default=80,
        help="Trailing log lines to retain for failed tasks (default: 80).",
    )
    report_parser.add_argument("--open", dest="open", action="store_true", default=False)
    report_parser.add_argument("--no-open", dest="open", action="store_false")

    secrets_parser = subparsers.add_parser("secrets")
    secrets_subparsers = secrets_parser.add_subparsers(dest="secrets_command", required=True)
    list_parser = secrets_subparsers.add_parser("list")
    list_parser.add_argument("workflow", nargs="?")
    list_parser.add_argument("--config", action="append", default=[])
    validate_parser = secrets_subparsers.add_parser("validate")
    validate_parser.add_argument("workflow", nargs="?")
    validate_parser.add_argument("--config", action="append", default=[])

    return parser


def _run_mode_from_args(args: argparse.Namespace) -> RunMode:
    """Return the run output mode implied by CLI flags."""
    if getattr(args, "agent", False):
        if getattr(args, "verbose", False):
            return "agent_verbose"
        return "agent"
    if getattr(args, "verbose", False):
        return "verbose"
    return "default"
