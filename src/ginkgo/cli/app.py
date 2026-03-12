"""CLI parser construction and command dispatch."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from ginkgo.cli.commands.cache import command_cache
from ginkgo.cli.commands.debug import command_debug
from ginkgo.cli.commands.run import command_run
from ginkgo.cli.commands.test import command_test
from ginkgo.cli.common import RunMode


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``ginkgo`` CLI."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "run":
            return command_run(args, output_mode=_run_mode_from_args(args))
        if args.command == "cache":
            return command_cache(args)
        if args.command == "debug":
            return command_debug(args)
        if args.command == "test":
            return command_test(args)
    except BaseException as exc:
        print(str(exc), file=sys.stderr)
        return 1

    parser.error("missing command")
    return 2


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ginkgo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("workflow")
    run_parser.add_argument("--config", action="append", default=[])
    run_parser.add_argument("--jobs", type=int, default=None)
    run_parser.add_argument("--cores", type=int, default=None)
    run_parser.add_argument("--verbose", action="store_true")

    cache_parser = subparsers.add_parser("cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)
    cache_subparsers.add_parser("ls")
    clear_parser = cache_subparsers.add_parser("clear")
    clear_parser.add_argument("cache_key")

    debug_parser = subparsers.add_parser("debug")
    debug_parser.add_argument("run_id", nargs="?")

    test_parser = subparsers.add_parser("test")
    test_parser.add_argument("--dry-run", action="store_true")

    return parser


def _run_mode_from_args(args: argparse.Namespace) -> RunMode:
    """Return the run output mode implied by CLI flags."""
    if getattr(args, "verbose", False):
        return "verbose"
    return "default"
