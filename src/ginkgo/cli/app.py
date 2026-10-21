"""CLI parser construction and command dispatch."""

from __future__ import annotations

import argparse
import importlib.metadata
import os
from pathlib import Path
import sys
from typing import NoReturn, Sequence

from ginkgo.cli.commands.cache import command_cache, parse_size_bytes
from ginkgo.cli.commands.db import command_db
from ginkgo.cli.commands.debug import command_debug
from ginkgo.cli.commands.doctor import command_doctor
from ginkgo.cli.commands.export import command_export
from ginkgo.cli.commands.history import command_history
from ginkgo.cli.commands.init import command_init
from ginkgo.cli.commands.inspect import command_inspect
from ginkgo.cli.commands.plugins import command_plugins
from ginkgo.cli.commands.query import command_query
from ginkgo.cli.commands.run import command_run, command_run_help
from ginkgo.cli.commands.runs import command_runs
from ginkgo.cli.commands.secrets import command_secrets
from ginkgo.cli.commands.why import command_why
from ginkgo.cli.common import RunMode
from ginkgo.cli.errors import report_failure, report_interrupt, traceback_requested
from ginkgo.params import looks_like_flag, unrecognized_arguments_message
from ginkgo.plugins.api import CliCommand
from ginkgo.plugins.registry import PluginNotFoundError, first_party_provider, plugin_registry
from ginkgo.formatting import parse_duration
from ginkgo.query import RUN_STATUSES, SQL_ROW_LIMIT
from ginkgo.project import project_root
from ginkgo.core.resources import parse_memory


def _memory_arg(value: str) -> int:
    """Parse a --memory value, accepting a bare GiB integer or Kubernetes notation."""
    if value.isdigit():
        return int(value)
    try:
        return parse_memory(value)
    except ValueError as exc:
        # argparse reports a bare "invalid value" for ValueError; this keeps
        # parse_memory's message, which names the accepted notation.
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _duration_arg(value: str) -> str:
    """Validate a duration option (``30d``, ``12h``), keeping it as typed."""
    try:
        parse_duration(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value


def _size_arg(value: str) -> str:
    """Validate a size option (``2GB``, ``500MB``), keeping it as typed."""
    try:
        parse_size_bytes(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    return value


def _non_negative_int(value: str) -> int:
    """Parse a count that may be zero but not negative."""
    try:
        count = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid integer {value!r}") from None
    if count < 0:
        raise argparse.ArgumentTypeError(f"must be at least 0, got {count}")
    return count


_MISSING_ARGS_PREFIX = "the following arguments are required: "


class _GinkgoArgumentParser(argparse.ArgumentParser):
    """Show help when a command group is invoked with no subcommand.

    ``ginkgo cache`` names a group rather than an action, so argparse's bare
    "the following arguments are required" is a worse answer than the group's
    own help. Only a missing *subcommand* is treated that way; every other
    missing argument keeps its precise error.

    ``add_subparsers`` inherits ``parser_class`` from the parser it is called
    on, so each group parser built in :func:`_build_parser` is one of these
    too and answers for itself.
    """

    def error(self, message: str) -> NoReturn:
        if message.startswith(_MISSING_ARGS_PREFIX):
            missing = {name.strip() for name in message[len(_MISSING_ARGS_PREFIX) :].split(",")}
            for action in self._actions:
                if not isinstance(action, argparse._SubParsersAction):
                    continue
                if action.required and (action.metavar or action.dest) in missing:
                    self.print_help(sys.stderr)
                    self.exit(2)

        super().error(message)


# Path-valued arguments, which must be resolved against the invocation
# directory before the working directory moves out from under them.
_PATH_ARGS = ("workflow", "out")
_PATH_LIST_ARGS = ("config",)


def _normalize_working_directory(args: argparse.Namespace) -> None:
    """Move to the project root so every cwd-relative path agrees on where it is.

    Ginkgo's runtime reads the working directory as the project root in about
    forty places: ``WorkspaceLayout.for_cwd()`` puts ``.ginkgo/`` there, config
    layering and environment discovery look for their files there, and the CLI
    renders run paths relative to it. Run from a subdirectory, every one of
    those was wrong in the same way.

    Rather than teach each of them to discover the root, this makes the
    assumption true once: resolve the root and change directory to it, so
    ``Path.cwd()`` *is* the project root for everything downstream. A workflow's
    relative output paths therefore land at the project root wherever ginkgo
    was invoked from, which is what makes the same command reproducible from
    two different directories.

    ``ginkgo init`` is exempt: it creates a project rather than running inside
    one, and its directory argument is relative to where the user stands.
    """
    if args.command == "init":
        return

    root = project_root()
    if root == Path.cwd():
        return

    # Resolved before the chdir, while they still mean what the user typed.
    for name in _PATH_ARGS:
        value = getattr(args, name, None)
        if value:
            setattr(args, name, str(Path(value).resolve()))
    for name in _PATH_LIST_ARGS:
        values = getattr(args, name, None)
        if values:
            setattr(args, name, [str(Path(value).resolve()) for value in values])

    os.chdir(root)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the ``ginkgo`` CLI."""
    # Building the parser runs every plugin command's add_arguments, so a
    # broken plugin is reported like any other failure, not as a traceback.
    try:
        parser, run_parser = _build_parser()
    except Exception as exc:
        return report_failure(exc=exc, stream=sys.stderr, show_traceback=traceback_requested())

    # Unknown flags and their values are removed before argparse runs. Left in,
    # a parameter's value would be captured by an optional positional: in
    # ``ginkgo run --run-label "wide"`` with the workflow path omitted, argparse
    # binds "wide" to the workflow positional and tries to load it as a module.
    known_argv, extras = _partition_param_extras(
        argv=list(sys.argv[1:] if argv is None else argv),
        known_options=_all_option_strings(parser),
    )
    _refuse_uninstalled_command(parser=parser, argv=known_argv)
    args = parser.parse_args(known_argv)

    # Commands that import a workflow accept its declared parameters as flags.
    # Every other command must still reject anything it does not recognise.
    command_parser = _command_parser(parser=parser, args=args)
    args.command_flags = frozenset(
        flag for action in command_parser._actions for flag in action.option_strings
    )
    if _accepts_workflow_params(args):
        args.param_extras = tuple(extras)
    elif extras:
        command_parser.error(
            unrecognized_arguments_message(tokens=extras, known_flags=args.command_flags)
        )
    else:
        args.param_extras = ()

    try:
        _normalize_working_directory(args)

        if args.command == "run":
            if getattr(args, "show_help", False):
                return command_run_help(args, usage=run_parser.format_help())
            return command_run(args, output_mode=_run_mode_from_args(args))
        if args.command == "cache":
            return command_cache(args)
        if args.command == "db":
            return command_db(args)
        if args.command == "debug":
            return command_debug(args)
        if args.command == "doctor":
            return command_doctor(args)
        if args.command == "init":
            return command_init(args)
        if args.command == "inspect":
            return command_inspect(args)
        if args.command == "why":
            return command_why(args)
        if args.command == "plugins":
            return command_plugins(args)
        if args.command == "runs":
            return command_runs(args)
        if args.command == "history":
            return command_history(args)
        if args.command == "query":
            return command_query(args)
        if args.command == "export":
            return command_export(args)
        if args.command == "secrets":
            return command_secrets(args)
        return plugin_registry().get(CliCommand, args.command).run(args)
    # SystemExit is deliberately not caught: argparse and ``--version`` use it
    # to exit with a status they have already chosen.
    except KeyboardInterrupt:
        return report_interrupt(stream=sys.stderr)
    except Exception as exc:
        return report_failure(
            exc=exc,
            stream=sys.stderr,
            show_traceback=traceback_requested(args),
        )


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


def _command_parser(
    *, parser: argparse.ArgumentParser, args: argparse.Namespace
) -> argparse.ArgumentParser:
    """Return the parser of the (sub)command *args* was parsed for.

    Descends through each subcommand the parse chose, so ``ginkgo inspect
    workflow`` answers with the ``workflow`` parser rather than ``inspect``'s.
    """
    current = parser
    while True:
        chosen = next(
            (
                action.choices.get(getattr(args, action.dest, None) or "")
                for action in current._actions
                if isinstance(action, argparse._SubParsersAction)
            ),
            None,
        )
        if chosen is None:
            return current
        current = chosen


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


def _refuse_uninstalled_command(*, parser: argparse.ArgumentParser, argv: Sequence[str]) -> None:
    """Answer a first-party plugin's command with the registry's not-found message.

    Argparse alone would answer ``ginkgo report`` without ginkgo-report with a
    bare "invalid choice". The registry's message names the plugin to install,
    or the plugin that failed to load when it is installed but broken. Any other
    unknown command is left to argparse, which lists the valid choices.
    """
    command = next((token for token in argv if not token.startswith("-")), None)
    if command is None or first_party_provider(CliCommand, command) is None:
        return
    try:
        plugin_registry().get(CliCommand, command)
    except PluginNotFoundError as exc:
        parser.error(str(exc))


def _build_parser() -> tuple[argparse.ArgumentParser, argparse.ArgumentParser]:
    """Build the CLI parser.

    Returns
    -------
    tuple[argparse.ArgumentParser, argparse.ArgumentParser]
        The top-level parser, and the ``run`` subparser. The latter is returned
        so ``ginkgo run <workflow> --help`` can render its own usage text
        alongside the workflow's declared parameters.
    """

    parser = _GinkgoArgumentParser(prog="ginkgo")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_ginkgo_version()}",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ``run`` handles help itself: argparse's own help action would fire during
    # the first parse and exit before the workflow could be imported, so the
    # declared parameters could never be listed.
    run_parser = subparsers.add_parser("run", add_help=False, help="Execute a workflow graph")
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
    run_parser.add_argument(
        "--memory",
        type=_memory_arg,
        default=None,
        metavar="GIB|K8S",
        help="Memory budget, in GiB or Kubernetes notation (e.g. 80, 80Gi, 512Mi).",
    )
    run_parser.add_argument("--gpus", type=int, default=None)
    run_parser.add_argument(
        "--resource",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Budget for a user-defined resource dimension (repeatable), e.g. --resource api_calls=10",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the plan -- waves, cache status, resources -- without executing tasks",
    )
    run_parser.add_argument("--verbose", action="store_true")
    run_parser.add_argument(
        "--agent-output",
        action="store_true",
        help="Print newline-delimited JSON events instead of the live terminal UI",
    )
    run_parser.add_argument(
        "--trust-mtimes",
        action="store_true",
        help="Skip content hashing; use stat-based checks only (faster warm runs)",
    )
    run_parser.add_argument(
        "--rerun",
        action="append",
        default=[],
        metavar="TASK",
        help=(
            "Run this task again rather than use its cached result (repeatable): its "
            "name, or a fan-out display label. Tasks downstream re-run only if its "
            "result changes."
        ),
    )
    run_parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue dispatching after task failures; every task is treated as on_failure='ignore'",
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

    cache_parser = subparsers.add_parser("cache", help="Manage task execution cache")
    cache_subparsers = cache_parser.add_subparsers(dest="cache_command", required=True)
    cache_ls_parser = cache_subparsers.add_parser("ls", help="List cached task entries")
    cache_ls_parser.add_argument(
        "--task",
        default=None,
        help="Only entries written by this task: its name, or a fan-out display label.",
    )
    cache_ls_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table."
    )
    stats_parser = cache_subparsers.add_parser(
        "stats", help="Summarise cache size and hit statistics"
    )
    stats_parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table.")
    clear_parser = cache_subparsers.add_parser("clear", help="Clear cache entries")
    clear_parser.add_argument("cache_key", nargs="?")
    clear_parser.add_argument(
        "--task",
        default=None,
        metavar="NAME",
        help="Remove every entry written by this task: its name, or a fan-out display label.",
    )
    clear_parser.add_argument(
        "--all",
        dest="clear_all",
        action="store_true",
        help="Remove every cache entry, and any cache directory the database has no entry for.",
    )
    clear_parser.add_argument(
        "--orphans",
        action="store_true",
        help="Remove cache directories the database has no entry for.",
    )
    explain_parser = cache_subparsers.add_parser(
        "explain", help="Explain why a task ran or was cached"
    )
    explain_parser.add_argument("run_id", nargs="?")
    explain_parser.add_argument(
        "--run",
        dest="run_flag",
        metavar="RUN_ID",
        default=None,
        help="Deprecated alias for the positional run id.",
    )
    explain_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of formatted text."
    )
    prune_parser = cache_subparsers.add_parser(
        "prune", help="Prune cached artifacts by age or size"
    )
    prune_parser.add_argument(
        "--older-than",
        type=_duration_arg,
        default=None,
        metavar="DURATION",
        help="Prune entries created more than this long ago (30d, 12h, 45m).",
    )
    prune_parser.add_argument(
        "--max-size",
        type=_size_arg,
        default=None,
        help="Prune oldest entries until total size is at or below the target (e.g. 2GB, 500MB).",
    )
    prune_parser.add_argument(
        "--max-entries",
        type=_non_negative_int,
        default=None,
        help="Prune oldest entries until entry count is at or below this number.",
    )
    prune_parser.add_argument(
        "--least-recently-hit",
        action="store_true",
        help="Give up the entries nobody has used lately before the oldest ones.",
    )
    prune_parser.add_argument("--dry-run", action="store_true")

    db_parser = subparsers.add_parser("db", help="Maintain the provenance database")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)
    db_subparsers.add_parser("migrate", help="Create or upgrade the provenance database")
    db_check_parser = db_subparsers.add_parser(
        "check", help="Check database integrity and schema version"
    )
    db_check_parser.add_argument(
        "--verify",
        action="store_true",
        help="Also rehash every stored artifact and report corrupt bytes (reads the whole store).",
    )
    db_check_parser.add_argument(
        "--fix",
        action="store_true",
        help=(
            "First remove what nothing can use: run and cache directories with no ledger row, "
            "cache rows whose bytes are gone, and a write-ahead log without its database."
        ),
    )
    db_subparsers.add_parser("path", help="Print the provenance database path")
    db_subparsers.add_parser("vacuum", help="Rebuild the database, releasing freed space")
    db_prune_parser = db_subparsers.add_parser(
        "prune", help="Delete history the projections no longer need"
    )
    db_prune_parser.add_argument(
        "--events-older-than",
        type=_duration_arg,
        metavar="DURATION",
        help="Delete the raw events of runs that finished more than this ago (30d, 12h, 45m)",
    )
    db_prune_parser.add_argument(
        "--digest-memo-older-than",
        type=_duration_arg,
        metavar="DURATION",
        help="Delete memoised file digests not seen for this long",
    )
    db_prune_parser.add_argument(
        "--staging-older-than",
        type=_duration_arg,
        metavar="DURATION",
        help="Delete staged remote inputs not used for this long, and their bytes",
    )
    db_prune_parser.add_argument("--dry-run", action="store_true")

    debug_parser = subparsers.add_parser("debug", help="Debug failed workflow runs")
    debug_parser.add_argument("run_id", nargs="?")
    debug_parser.add_argument("--json", action="store_true")

    doctor_parser = subparsers.add_parser(
        "doctor", help="Validate environment and workflow configuration"
    )
    doctor_parser.add_argument("workflow", nargs="?")
    doctor_parser.add_argument("--config", action="append", default=[])
    doctor_parser.add_argument("--json", action="store_true")

    init_parser = subparsers.add_parser("init", help="Initialize a new ginkgo project scaffold")
    init_parser.add_argument("directory", nargs="?", default=".")
    init_parser.add_argument(
        "--no-skills",
        action="store_true",
        help="Write the project scaffold without the agent skills.",
    )
    init_parser.add_argument(
        "--skills-only",
        action="store_true",
        help="Write only the agent skills, not the project scaffold.",
    )
    init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite scaffold files that already exist.",
    )

    inspect_parser = subparsers.add_parser("inspect", help="Inspect workflow structure")
    inspect_subparsers = inspect_parser.add_subparsers(dest="inspect_command", required=True)
    inspect_workflow_parser = inspect_subparsers.add_parser(
        "workflow", help="Inspect static workflow graph"
    )
    inspect_workflow_parser.add_argument("workflow", nargs="?")
    inspect_workflow_parser.add_argument("--config", action="append", default=[])

    runs_parser = subparsers.add_parser("runs", help="List and inspect recorded runs")
    runs_subparsers = runs_parser.add_subparsers(dest="runs_command", required=True)
    runs_ls_parser = runs_subparsers.add_parser("ls", help="List recorded runs, newest first")
    runs_ls_parser.add_argument("--workflow", default=None, help="Only runs of this workflow.")
    runs_ls_parser.add_argument(
        "--status", default=None, choices=RUN_STATUSES, help="Only runs with this status."
    )
    runs_ls_parser.add_argument(
        "--since", default=None, metavar="TIMESTAMP", help="Only runs started at or after this."
    )
    runs_ls_parser.add_argument("--limit", type=int, default=20, help="Most runs to list.")
    runs_ls_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table."
    )
    runs_show_parser = runs_subparsers.add_parser("show", help="Show one run and its tasks")
    runs_show_parser.add_argument("run_id", nargs="?")
    runs_show_parser.add_argument(
        "--json", action="store_true", help="Emit the full run manifest as JSON."
    )
    runs_show_parser.add_argument(
        "--all",
        dest="all_tasks",
        action="store_true",
        help="List every task; a large run otherwise lists its first tasks and every failure.",
    )

    history_parser = subparsers.add_parser("history", help="Show every run of one task")
    history_parser.add_argument("task", help="Task name, base name, or fan-out display label.")
    history_parser.add_argument("--limit", type=int, default=20, help="Most runs to list.")
    history_parser.add_argument(
        "--include-unstarted",
        action="store_true",
        help="Also list runs in which the task never started.",
    )
    history_parser.add_argument(
        "--resources",
        action="store_true",
        help="Summarise measured memory and CPU across every run of the task.",
    )
    history_parser.add_argument(
        "--by-label",
        action="store_true",
        help="With --resources, summarise each fan-out branch separately.",
    )
    history_parser.add_argument(
        "--json", action="store_true", help="Emit JSON instead of a table."
    )

    query_parser = subparsers.add_parser(
        "query", help="Run one read-only SQL statement against the provenance database"
    )
    query_parser.add_argument(
        "sql", nargs="?", help="One SELECT. Run --schema to see what there is to select."
    )
    query_parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the tables and their columns instead of running a statement.",
    )
    query_parser.add_argument(
        "--limit", type=int, default=SQL_ROW_LIMIT, help="Most rows to return."
    )
    query_output = query_parser.add_mutually_exclusive_group()
    query_output.add_argument("--json", action="store_true", help="Emit JSON rows.")
    query_output.add_argument("--csv", action="store_true", help="Emit CSV, header first.")

    export_parser = subparsers.add_parser("export", help="Export a run's record")
    export_subparsers = export_parser.add_subparsers(dest="export_command", required=True)
    export_events_parser = export_subparsers.add_parser(
        "events", help="Export a run's ledger events as JSONL"
    )
    export_events_parser.add_argument("run_id", nargs="?")
    export_events_parser.add_argument(
        "--out", default=None, metavar="PATH", help="Write here instead of to stdout."
    )
    export_manifest_parser = export_subparsers.add_parser(
        "manifest", help="Export a run's manifest as YAML"
    )
    export_manifest_parser.add_argument("run_id", nargs="?")
    export_manifest_parser.add_argument(
        "--out", default=None, metavar="PATH", help="Write here instead of to stdout."
    )

    why_parser = subparsers.add_parser("why", help="Trace which task produced a file")
    why_parser.add_argument("target", help="A materialized file path or an artifact id.")
    why_parser.add_argument("--json", action="store_true", help="Emit JSON")

    plugins_parser = subparsers.add_parser(
        "plugins", help="List installed plugins and what they contribute"
    )
    plugins_parser.add_argument("--json", action="store_true", help="Emit JSON")

    secrets_parser = subparsers.add_parser("secrets", help="Manage and validate workflow secrets")
    secrets_subparsers = secrets_parser.add_subparsers(dest="secrets_command", required=True)
    list_parser = secrets_subparsers.add_parser("list", help="List declared secrets")
    list_parser.add_argument("workflow", nargs="?")
    list_parser.add_argument("--config", action="append", default=[])
    validate_parser = secrets_subparsers.add_parser(
        "validate", help="Validate secret availability"
    )
    validate_parser.add_argument("workflow", nargs="?")
    validate_parser.add_argument("--config", action="append", default=[])

    for command in plugin_registry().all(CliCommand):
        # Core's commands are not registry contributions, so the registry
        # cannot refuse this clash; the plugin's command is unreachable.
        if command.name in subparsers.choices:
            print(
                f"warning: a plugin's CLI command {command.name!r} clashes with a core "
                "ginkgo command and is ignored. Run `ginkgo plugins` to find it.",
                file=sys.stderr,
            )
            continue
        command.add_arguments(subparsers.add_parser(command.name, help=command.help))

    # Prefix abbreviation is off throughout: an abbreviated ginkgo flag is absent
    # from _all_option_strings, so ``--job 4`` would be partitioned off as a
    # workflow parameter and rejected as undeclared. Abbreviations are not
    # supported, and rejecting them everywhere keeps that consistent.
    for subparser in _parser_tree(parser):
        subparser.allow_abbrev = False

    return parser, run_parser


def _run_mode_from_args(args: argparse.Namespace) -> RunMode:
    """Return the run output mode implied by CLI flags."""
    if getattr(args, "agent_output", False):
        if getattr(args, "verbose", False):
            return "agent_verbose"
        return "agent"
    if getattr(args, "verbose", False):
        return "verbose"
    return "default"
