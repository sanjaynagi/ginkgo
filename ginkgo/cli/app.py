"""CLI parser construction and command dispatch."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from rich.text import Text

from ginkgo.cli.commands.cache import command_cache
from ginkgo.cli.commands.debug import command_debug
from ginkgo.cli.commands.doctor import command_doctor
from ginkgo.cli.commands.env import command_env
from ginkgo.cli.commands.init import command_init
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
        if args.command == "secrets":
            return command_secrets(args)
        if args.command == "ui":
            return command_ui(args)
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

    cache_parser = subparsers.add_parser("cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)
    cache_subparsers.add_parser("ls")
    clear_parser = cache_subparsers.add_parser("clear")
    clear_parser.add_argument("cache_key")
    prune_parser = cache_subparsers.add_parser("prune")
    prune_parser.add_argument("--older-than", required=True)
    prune_parser.add_argument("--dry-run", action="store_true")

    env_parser = subparsers.add_parser("env")
    env_subparsers = env_parser.add_subparsers(dest="env_command", required=True)
    env_subparsers.add_parser("ls")
    env_clear_parser = env_subparsers.add_parser("clear")
    env_clear_parser.add_argument("env", nargs="?")
    env_clear_parser.add_argument("--all", action="store_true")
    env_clear_parser.add_argument("--dry-run", action="store_true")

    debug_parser = subparsers.add_parser("debug")
    debug_parser.add_argument("run_id", nargs="?")

    doctor_parser = subparsers.add_parser("doctor")
    doctor_parser.add_argument("workflow", nargs="?")
    doctor_parser.add_argument("--config", action="append", default=[])

    test_parser = subparsers.add_parser("test")
    test_parser.add_argument("--dry-run", action="store_true")

    init_parser = subparsers.add_parser("init")
    init_parser.add_argument("directory", nargs="?", default=".")
    init_parser.add_argument("--force", action="store_true")

    ui_parser = subparsers.add_parser("ui")
    ui_parser.add_argument("run_id", nargs="?")
    ui_parser.add_argument("--host", default="127.0.0.1")
    ui_parser.add_argument("--port", type=int, default=7777)
    ui_parser.add_argument("--open", dest="open", action="store_true", default=True)
    ui_parser.add_argument("--no-open", dest="open", action="store_false")

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
    if getattr(args, "verbose", False):
        return "verbose"
    return "default"
