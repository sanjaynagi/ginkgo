"""CLI parser construction and command dispatch."""

from __future__ import annotations

import argparse
import importlib.metadata
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
from ginkgo.cli.commands.run import command_run, command_run_help
from ginkgo.cli.commands.secrets import command_secrets
from ginkgo.cli.commands.test import command_test
from ginkgo.cli.common import RunMode, console
from ginkgo.params import looks_like_flag


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``ginkgo`` CLI."""
    parser, run_parser = _build_parser()

    # Unknown flags and their values are removed before argparse runs. Left in,
    # a parameter's value would be captured by an optional positional: in
    # ``ginkgo run --run-label "wide"`` with the workflow path omitted, argparse
    # binds "wide" to the workflow positional and tries to load it as a module.
    known_argv, extras = _partition_param_extras(
        argv=list(sys.argv[1:] if argv is None else argv),
        known_options=_all_option_strings(parser),
    )
    args = parser.parse_args(known_argv)

    # Commands that import a workflow accept its declared parameters as flags.
    # Every other command must still reject anything it does not recognise.
    if _accepts_workflow_params(args):
        args.param_extras = tuple(extras)
    elif extras:
        parser.error(f"unrecognized arguments: {' '.join(extras)}")
    else:
        args.param_extras = ()

    try:
        if args.command == "run":
            if getattr(args, "show_help", False):
                return command_run_help(args, usage=run_parser.format_help())
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
        if args.command == "report":
            return command_report(args)
    except BaseException as exc:
        rich_console = console(sys.stderr)
        rich_console.print(Text("✖ ", style="bold red"), Text(str(exc)), sep="")
        return 1

    parser.error("missing command")
    return 2


def _ginkgo_version() -> str:
    """Return the installed ginkgo package version, or ``"unknown"`` if unavailable."""
    try:
        return importlib.metadata.version("ginkgo")
    except importlib.metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


def _parser_tree(parser: argparse.ArgumentParser) -> list[argparse.ArgumentParser]:
    """Return *parser* together with every subcommand parser beneath it."""
    found: list[argparse.ArgumentParser] = []
    pending = [parser]
    while pending:
        current = pending.pop()
        found.append(current)
        for action in current._actions:
            choices = getattr(action, "choices", None) or {}
            if isinstance(choices, dict):
                pending.extend(
                    value
                    for value in choices.values()
                    if isinstance(value, argparse.ArgumentParser)
                )
    return found


def _all_option_strings(parser: argparse.ArgumentParser) -> frozenset[str]:
    """Collect every flag the parser tree recognises, including subcommands.

    A token starting with ``-`` that is absent from this set belongs to a
    workflow parameter rather than to ginkgo itself. The set is exact only
    because abbreviation is disabled in :func:`_build_parser`: an abbreviated
    flag argparse would accept is absent here, so it would be misread as a
    parameter.
    """
    found: set[str] = set()
    for current in _parser_tree(parser):
        for action in current._actions:
            found.update(action.option_strings)
    return frozenset(found)


def _partition_param_extras(
    *,
    argv: list[str],
    known_options: frozenset[str],
) -> tuple[list[str], list[str]]:
    """Split *argv* into tokens ginkgo parses and tokens belonging to parameters.

    An unrecognised ``--flag`` takes the following token as its value unless that
    token is itself a flag — the same rule argparse applies to its own optionals,
    so a parameter behaves like every other flag. A negative number is a value
    rather than a flag, so ``--offset -5`` holds together. A boolean parameter
    given immediately before a positional needs the ``--flag=value`` form.

    Parameters
    ----------
    argv : list[str]
        Raw argument vector, without the program name.
    known_options : frozenset[str]
        Every flag the parser tree recognises.

    Returns
    -------
    tuple[list[str], list[str]]
        Tokens for argparse, and tokens belonging to workflow parameters.
    """
    known_argv: list[str] = []
    extras: list[str] = []

    index = 0
    while index < len(argv):
        token = argv[index]
        name = token.split("=", 1)[0]

        if not looks_like_flag(token) or name in known_options:
            known_argv.append(token)
            index += 1
            continue

        extras.append(token)
        index += 1
        if "=" in token:
            continue
        if index < len(argv) and not looks_like_flag(argv[index]):
            extras.append(argv[index])
            index += 1

    return known_argv, extras


def _accepts_workflow_params(args: argparse.Namespace) -> bool:
    """Whether this command imports a workflow and so accepts its parameter flags."""
    if args.command in {"run", "doctor", "secrets"}:
        return True
    return args.command == "inspect" and getattr(args, "inspect_command", None) == "workflow"


def _build_parser() -> tuple[argparse.ArgumentParser, argparse.ArgumentParser]:
    """Build the CLI parser.

    Returns
    -------
    tuple[argparse.ArgumentParser, argparse.ArgumentParser]
        The top-level parser, and the ``run`` subparser. The latter is returned
        so ``ginkgo run <workflow> --help`` can render its own usage text
        alongside the workflow's declared parameters.
    """
    parser = argparse.ArgumentParser(prog="ginkgo")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_ginkgo_version()}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ``run`` handles help itself: argparse's own help action would fire during
    # the first parse and exit before the workflow could be imported, so the
    # declared parameters could never be listed.
    run_parser = subparsers.add_parser("run", add_help=False)
    run_parser.add_argument(
        "-h",
        "--help",
        dest="show_help",
        action="store_true",
        help="Show this message, including the workflow's declared parameters",
    )
    run_parser.add_argument("workflow", nargs="?")
    run_parser.add_argument("--config", action="append", default=[])
    run_parser.add_argument("--jobs", type=int, default=None)
    run_parser.add_argument("--cores", type=int, default=None)
    run_parser.add_argument("--memory", type=int, default=None)
    run_parser.add_argument("--gpus", type=int, default=None)
    run_parser.add_argument(
        "--resource",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Budget for a user-defined resource dimension (repeatable), e.g. --resource api_calls=10",
    )
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
        default="local",
        metavar="NAME",
        help=(
            "Default executor for remote tasks: 'local' (default), a name from "
            "[remote.executors] in ginkgo.toml, or 'k8s'/'batch' for the legacy "
            "[remote.k8s]/[remote.batch] sections. Tasks declaring "
            "executor=... always use the executor they name."
        ),
    )

    cache_parser = subparsers.add_parser("cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)
    cache_subparsers.add_parser("ls")
    clear_parser = cache_subparsers.add_parser("clear")
    clear_parser.add_argument("cache_key")
    explain_parser = cache_subparsers.add_parser("explain")
    explain_parser.add_argument("run_id", nargs="?")
    explain_parser.add_argument(
        "--run",
        dest="run_flag",
        metavar="RUN_ID",
        default=None,
        help="Deprecated alias for the positional run id.",
    )
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

    # Prefix abbreviation is off throughout: an abbreviated ginkgo flag is absent
    # from _all_option_strings, so ``--job 4`` would be partitioned off as a
    # workflow parameter and rejected as undeclared. Abbreviations are not
    # supported, and rejecting them everywhere keeps that consistent.
    for subparser in _parser_tree(parser):
        subparser.allow_abbrev = False

    return parser, run_parser


def _run_mode_from_args(args: argparse.Namespace) -> RunMode:
    """Return the run output mode implied by CLI flags."""
    if getattr(args, "agent", False):
        if getattr(args, "verbose", False):
            return "agent_verbose"
        return "agent"
    if getattr(args, "verbose", False):
        return "verbose"
    return "default"
