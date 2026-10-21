"""Run command handlers."""

from __future__ import annotations

import os

# Suppress gRPC fork-safety warnings on macOS. Must be set before grpc is imported.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

import sys
import time
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Collection, Sequence

from rich.markup import escape

from ginkgo import query
from ginkgo.plugins.api import EnvironmentContext, EventSubscriberContext, EventSubscriberType
from ginkgo.plugins.registry import PluginNotFoundError, plugin_registry
from ginkgo.cli.common import RUNS_ROOT, RunMode, console
from ginkgo.cli.renderers.common import environment_label
from ginkgo.errors import GinkgoError
from ginkgo.formatting import format_duration
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.cli.renderers.jsonl import JsonlEventRenderer, event_line
from ginkgo.cli.errors import IGNORED_FAILURES_EXIT_CODE, UsageError
from ginkgo.cli.renderers.models import (
    FailureDetails,
    CliRenderedPage,
    ResourceRenderState,
    CliRunSummary,
    SkipDetails,
)
from ginkgo.cli.renderers.rich import RichEventRenderer
from ginkgo.cli.renderers.run import CliRunRenderer
from ginkgo.cli.workflow_params import (
    collect_param_help,
    global_param_reads,
    params_table,
    validate_param_extras,
)
from ginkgo.cli.workspace import resolve_envs_workflow_root, resolve_workflow_path
from ginkgo.config import (
    RUNTIME_CONFIG_TABLES,
    config_session,
    load_runtime_config_layers,
    merge_config_layers,
)
from ginkgo.core.expr import record_constructed_calls
from ginkgo.core.resources import (
    ResourceOverrides,
    parse_resource_budget_args,
    resource_budgets_from_config,
)
from ginkgo.core.flow import discover_flow
from ginkgo.core.task import TaskDef
from ginkgo.params import ParamContext
from ginkgo.runtime.backend import EnvironmentRouter
from ginkgo.runtime.evaluator import ConcurrentEvaluator, RootSkippedError
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import display_path, load_module_from_path, task_base_name
from ginkgo.runtime.environment.resources import RunResourceMonitor
from ginkgo.runtime.rundir import (
    RunDir,
    combined_log_tail,
    make_run_id,
    runs_without_ledger_warning,
)
from ginkgo.runtime.diagnostics import (
    WorkflowDiagnostic,
    env_warning_diagnostic,
    environ_read_diagnostics,
    missing_directive_diagnostics,
    path_like_str_param_diagnostics,
    static_notice_diagnostics,
    unreachable_call_diagnostics,
)
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.events import (
    EventBus,
    PhaseTimed,
    RunCompleted,
    RunNotice,
    RunResourcesSampled,
    RunStarted,
    RunValidated,
    TaskNotice,
    event_timestamp,
)
from ginkgo.runtime.run_summary import RunSummary, TaskSummary
from ginkgo.runtime.store_recorder import StoreRecorder
from ginkgo.runtime.task_runners.subworkflow import PARENT_RUN_ID_ENV, PARENT_TASK_ID_ENV
from ginkgo.workspace_layout import WorkspaceLayout


def _ginkgo_version() -> str | None:
    """Return the installed ginkgo version, recorded on every run."""
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("ginkgo")
    except PackageNotFoundError:  # pragma: no cover - editable installs always resolve
        return None


def command_run(args, *, output_mode: RunMode) -> int:
    """Handle ``ginkgo run``.

    Under ``--agent-output`` a failure before the run exists — a missing
    workflow, a rejected parameter or budget, a graph that will not validate —
    is reported on stdout as one ``run_rejected`` event as well, so a consumer
    reading only the stream is not left with nothing. A failure after the run
    exists is already reported there by its ``run_completed`` event.
    """
    run_id = make_run_id()
    try:
        return _run_resolved_workflow(args, output_mode=output_mode, run_id=run_id)
    except Exception as exc:
        if output_mode in {"agent", "agent_verbose"} and not (RUNS_ROOT / run_id).exists():
            sys.stdout.write(
                event_line(
                    {
                        "event": "run_rejected",
                        "v": 1,
                        "ts": event_timestamp(),
                        "run_id": None,
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    }
                )
            )
            sys.stdout.flush()
        raise


def _run_resolved_workflow(args, *, output_mode: RunMode, run_id: str) -> int:
    """Resolve the workflow ``ginkgo run`` names, then run it."""
    resolution = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    )
    return run_workflow(
        run_id=run_id,
        workflow_path=resolution.path,
        workflow_discovered=resolution.discovered,
        config_paths=[Path(path).resolve() for path in args.config],
        jobs=args.jobs,
        cores=args.cores,
        memory=args.memory,
        gpus=args.gpus,
        resource_args=tuple(getattr(args, "resource", ()) or ()),
        dry_run=args.dry_run,
        output_mode=output_mode,
        trust_mtimes=getattr(args, "trust_mtimes", False),
        keep_going=getattr(args, "keep_going", False),
        rerun=tuple(getattr(args, "rerun", ()) or ()),
        executor=getattr(args, "executor", "local"),
        param_extras=getattr(args, "param_extras", ()),
        command_flags=getattr(args, "command_flags", ()),
    )


def command_run_help(args, *, usage: str) -> int:
    """Handle ``ginkgo run --help``.

    Prints the ``run`` usage text, then imports the workflow to list the
    parameters it declares. The import is best-effort: a workflow that cannot be
    imported still gets its usage text, with a warning in place of parameters.

    Parameters
    ----------
    args : argparse.Namespace
        Parsed ``run`` arguments.
    usage : str
        Usage text rendered by the ``run`` subparser.

    Returns
    -------
    int
        Process exit code.
    """
    # Usage and parameter help are printed without markup: their brackets are
    # literal text (``[workflow]``, a user's ``help="[x]"``), not style tags.
    rich_console = console(sys.stdout)
    rich_console.print(usage.rstrip(), highlight=False, markup=False)

    try:
        resolution = resolve_workflow_path(
            project_root=Path.cwd(),
            workflow=args.workflow,
        )
    except BaseException:
        # No workflow resolved, so the usage text alone is the whole answer.
        return 0

    workflow_name = workflow_display_name(
        workflow_path=resolution.path, discovered=resolution.discovered
    )
    try:
        help_lines = collect_param_help(
            workflow_path=resolution.path,
            config_paths=[Path(path).resolve() for path in args.config],
        )
    except BaseException as exc:
        rich_console.print(
            f"\n[yellow]⚠[/] Could not import {escape(workflow_name)} "
            f"to list its parameters: {escape(str(exc))}"
        )
        return 0

    rich_console.print(f"\nparameters declared by {workflow_name}:", highlight=False, markup=False)
    for line in help_lines or ["  (none)"]:
        rich_console.print(line, highlight=False, markup=False)
    return 0


def workflow_display_name(*, workflow_path: Path, discovered: bool) -> str:
    """Return how CLI output names the workflow being run.

    A workflow is shown by its path from the working directory, so
    ``workflow/flow.py`` is not mistaken for a ``flow.py`` beside the user. One
    found by autodiscovery is also marked as discovered, so output never reads
    as though the user typed a path they did not.

    Parameters
    ----------
    workflow_path : Path
        Absolute path to the workflow entrypoint.
    discovered : bool
        Whether the path was discovered rather than given on the command line.

    Returns
    -------
    str
        The display name.
    """
    if not discovered:
        return display_path(workflow_path)
    return f"{display_path(workflow_path)} (auto-discovered)"


def planned_task_rows(evaluator: ConcurrentEvaluator) -> list[tuple[int, str, str, str]]:
    """Return the run table's seed rows for a validated graph.

    Each row is ``(node_id, task_name, label, env_label)``. The label is the
    one the evaluator gave the node at registration, the same one
    ``--dry-run`` labels its plan with and the ledger records, so a fan-out
    branch reads the same everywhere before it is dispatched.
    """
    return [
        (
            node.node_id,
            node.task_def.name,
            node.label,
            environment_label(node.task_def.env),
        )
        for node in sorted(evaluator.task_nodes.values(), key=lambda item: item.node_id)
    ]


def run_workflow(
    *,
    run_id: str | None = None,
    workflow_path: Path,
    workflow_discovered: bool = False,
    config_paths: list[Path],
    jobs: int | None,
    cores: int | None,
    memory: int | None,
    gpus: int | None = None,
    resource_args: Sequence[str] = (),
    dry_run: bool,
    output_mode: RunMode = "default",
    trust_mtimes: bool = False,
    keep_going: bool = False,
    rerun: Sequence[str] = (),
    executor: str = "local",
    plan_preview: bool = True,
    param_extras: Sequence[str] = (),
    command_flags: Collection[str] = (),
) -> int:
    run_id = run_id or make_run_id(workflow_path=workflow_path)
    rich_console = console(sys.stdout)
    workflow_name = escape(
        workflow_display_name(workflow_path=workflow_path, discovered=workflow_discovered)
    )
    # A real run's header carries its id, so it is held back until the run is
    # about to be created: an id printed before a load or validation failure
    # would name a run that never exists.
    if dry_run and output_mode not in {"agent", "agent_verbose"}:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_name}[/] [bold]--dry-run[/]\n"
        )

    load_started = time.perf_counter()
    # The runtime config is loaded before the workflow is imported so that
    # declared parameters resolve against it regardless of whether the workflow
    # calls config() before or after param().
    # Loaded as layers so top-level keys and the [params] table can combine
    # by their own rules, without reading the files twice.
    config_layers = load_runtime_config_layers(
        project_root=Path.cwd(),
        override_paths=config_paths,
    )
    runtime_config = merge_config_layers(config_layers)
    param_config = params_table(config_layers)
    with config_session(
        override_paths=config_paths,
        param_config=param_config,
        cli_extras=param_extras,
    ) as session:
        module = load_module_from_path(workflow_path)
        flow = discover_flow(module)
        with record_constructed_calls() as constructed_calls:
            expr = flow()
        # Validated after the flow body has run, so parameters declared inside
        # it have had their chance to claim a flag. Deliberately outside the
        # construction recorder: parameter resolution mints no task calls.
        validate_param_extras(session, command_flags=command_flags)
        params = session.merged_loaded_values()
        declared_params = session.resolved_params()
        param_sources = session.param_sources()
        declaration_globals = dict(session.declaration_globals)
    # Workers re-import the workflow module, so they need the same inputs to
    # resolve its parameters to the values this run is using.
    param_context = ParamContext(config=param_config, cli_extras=tuple(param_extras))
    runtime_params = dict(runtime_config)
    runtime_params.update(params)
    load_elapsed = time.perf_counter() - load_started

    secret_resolver = build_secret_resolver(
        project_root=Path.cwd(),
        config=runtime_params,
        environ=os.environ,
    )
    backend = EnvironmentRouter(
        context=EnvironmentContext(
            project_root=Path.cwd(),
            workflow_root=resolve_envs_workflow_root(
                project_root=Path.cwd(),
                fallback=workflow_path.parent,
            ),
            config=runtime_config,
            # Only a dry run asks registries about missing images: a real run
            # pulls them anyway, and its pull reports a missing one.
            probe_registry=dry_run,
        )
    )

    # Named executors: --executor picks the run default, and tasks may pin
    # any configured one. Backends are constructed on first dispatch, so a
    # run that never reaches a remote task never builds its client.
    executor_registry = ExecutorRegistry.from_config(runtime_config, default=executor)

    resource_overrides = ResourceOverrides.from_config(runtime_config.get("resources"))
    # CLI --resource flags win over [resources.budgets] per dimension.
    resource_budgets = resource_budgets_from_config(runtime_config.get("resources"))
    resource_budgets.update(parse_resource_budget_args(resource_args))
    # Shared by the validate/dry-run evaluator and the run evaluator below —
    # the two must resolve resources and placement identically.
    evaluator_kwargs: dict[str, Any] = {
        "jobs": jobs,
        "cores": cores,
        "memory": memory,
        "gpus": gpus,
        "resource_overrides": resource_overrides,
        "resource_budgets": resource_budgets or None,
        "backend": backend,
        "executor_registry": executor_registry,
        "secret_resolver": secret_resolver,
        "param_context": param_context,
        "keep_going": keep_going,
        "rerun": frozenset(rerun),
    }
    # A workspace whose ledger was deleted gets a new one from this run, which
    # leaves the older run directories unreadable without saying so. Asked
    # before the evaluator is built, since building it creates the ledger.
    if not WorkspaceLayout.relative().db.is_file():
        ledger_warning = runs_without_ledger_warning(layout=WorkspaceLayout.relative())
        if ledger_warning is not None:
            console(sys.stderr).print(
                f"[yellow]⚠[/] {escape(ledger_warning)}; this run starts a new database."
            )
    evaluator = ConcurrentEvaluator(**evaluator_kwargs)
    # A call the flow never returns is not in the graph, so its side effects
    # never happen: the run otherwise looks like a smaller but healthy one.
    # Warned about before validation, which a consumer of a dropped producer's
    # Out[...] path fails, so the warning names the cause. Held back only for a
    # dry-run plan that renders its own "Dropped" section, unless validation
    # fails before the plan can be rendered.
    plan_reports_dropped = (
        dry_run and plan_preview and output_mode not in {"agent", "agent_verbose"}
    )
    validate_started = time.perf_counter()
    evaluator.build_graph(expr)
    unreachable_calls = evaluator.graph.unreachable_calls(constructed_calls)
    dropped_calls = unreachable_call_diagnostics(calls=unreachable_calls)
    if not plan_reports_dropped:
        _print_dropped_calls(diagnostics=dropped_calls)
    # Every call dropped is not a smaller run but no run at all: nothing the
    # flow asked for would happen, so neither a run nor a dry run may pass.
    if unreachable_calls and not evaluator.task_nodes:
        if plan_reports_dropped:
            _print_dropped_calls(diagnostics=dropped_calls)
        count = len(unreachable_calls)
        raise GinkgoError(
            f"No task would run: every task call in the flow ({count}, named above) was "
            "dropped from the graph. Return the result of each call the flow needs, for "
            "example `return my_task(x=1)` or a list of results."
        )
    try:
        evaluator.validate_graph()
    except Exception:
        if plan_reports_dropped:
            _print_dropped_calls(diagnostics=dropped_calls)
        raise
    validate_elapsed = time.perf_counter() - validate_started

    # Only a dry run probes the container daemon; a stopped one fails just the
    # container tasks that have to run, so it is a warning here, not a failure.
    for warning in evaluator.validator.env_warnings:
        diagnostic = env_warning_diagnostic(warning=warning, workflow_path=workflow_path)
        console(sys.stderr).print(f"[yellow]⚠[/] {escape(diagnostic.message)}")

    # A parameter must reach a task as an argument. One read from a module global
    # is invisible to that task's cache key, so a changed value would silently
    # reuse the previous result. Detection is best-effort, hence a warning.
    for finding in global_param_reads(
        declaration_globals=declaration_globals,
        nodes=evaluator.graph.nodes.values(),
    ):
        console(sys.stderr).print(f"[yellow]⚠[/] {escape(finding.message())}")
    # The same holds for a value read from os.environ inside a task body.
    for diagnostic in environ_read_diagnostics(
        task_defs=[node.task_def for node in evaluator.graph.nodes.values()]
    ):
        console(sys.stderr).print(
            f"[yellow]⚠[/] {escape(diagnostic.message)} {escape(diagnostic.suggestion or '')}"
        )

    # A misspelt --rerun name would otherwise quietly serve every result from
    # the cache. One no task the workflow defines carries is rejected now; one
    # naming a defined task the static graph lacks may still match a task a
    # dynamic expansion returns, so it is checked once the run has finished.
    unmatched = set(rerun) - _rerun_names(evaluator=evaluator)
    defined = _defined_task_names(root=Path.cwd()) if unmatched else set()
    unknown = sorted(name for name in unmatched if name.split("[", 1)[0] not in defined)
    if unknown:
        raise UsageError(
            f"--rerun {', '.join(unknown)} names no task the workflow's modules define. "
            "Name a task, or one call by its label as the run table shows it (`--rerun "
            "'fastq_stats[sample_a]'`). A task whose module only a task body imports "
            "is found once the workflow imports that module too."
        )

    # A parameter named like a path but annotated a bare `str` is content-tracked
    # only when its value names an existing regular file and reads as a path (a
    # separator or a file extension); a directory, a bare word or other non-file
    # path is tracked by its string alone and is never checked for existence
    # (issue #307). One every call fills from an upstream result is left alone.
    # Static and name-based, so it is cheap to check every time, but
    # only printed on --dry-run: unlike the two warnings above it fires once
    # per task definition regardless of how the graph is shaped, so a real
    # run would otherwise repeat it on every invocation.
    # A driver task whose body can never return its directive (a shell task
    # returning a plain string) is flagged the same way, so a dry run catches
    # it before a real run fails there after its upstream tasks have run.
    if dry_run:
        for diagnostic in [
            *path_like_str_param_diagnostics(nodes=evaluator.graph.nodes.values()),
            *missing_directive_diagnostics(
                task_defs=[node.task_def for node in evaluator.graph.nodes.values()]
            ),
            *static_notice_diagnostics(nodes=evaluator.graph.nodes.values()),
        ]:
            # Escaped: the message names `Out[file]`, which Rich would
            # otherwise swallow as a style tag.
            console(sys.stderr).print(f"[yellow]⚠[/] {escape(diagnostic.message)}")

    # Executors named by tasks themselves, which dispatch there regardless of
    # the run default — surfaced in the header so a locally-defaulted run does
    # not look like it stayed on this machine.
    pinned_executors = tuple(
        sorted(
            {
                node.task_def.executor
                for node in evaluator.task_nodes.values()
                if node.task_def.executor is not None
                and node.task_def.executor != executor_registry.default_name
            }
        )
    )
    default_executor = executor_registry.default_name
    dispatch_targets: list[str] = [
        *pinned_executors,
        *([default_executor] if default_executor is not None else []),
    ]
    # Backends are built on first dispatch, so a half-written executor section
    # is caught here rather than after every local task has already run.
    executor_registry.validate_settings(names=dispatch_targets)

    # Code-sync is configured per executor, so a run that dispatches to one
    # without a code table quietly runs its image's baked copy. Warned about
    # for the same reason as the checks above: the run looks healthy.
    # Messages quote config sections, whose brackets Rich would read as markup.
    for message in executor_registry.code_sync_gaps(names=dispatch_targets):
        console(sys.stderr).print(f"[yellow]⚠[/] {escape(message)}")

    task_count = len(evaluator.task_nodes)
    edge_count = sum(len(node.dependency_ids) for node in evaluator.graph.nodes.values())
    env_count = len(
        {node.task_def.env for node in evaluator.task_nodes.values() if node.task_def.env}
    )
    planned_tasks = planned_task_rows(evaluator)

    if dry_run:
        if output_mode in {"agent", "agent_verbose"}:
            bus = EventBus()
            bus.subscribe(JsonlEventRenderer(stream=sys.stdout))
            bus.emit(RunStarted(run_id=run_id, workflow=str(workflow_path)))
            bus.emit(
                RunValidated(
                    run_id=run_id,
                    task_count=task_count,
                    edge_count=edge_count,
                    env_count=env_count,
                )
            )
            # The same warm-cache probe the plan preview runs: a provable
            # input-contract violation must fail the scripted preflight too.
            plan = build_dry_run_plan(
                evaluator=evaluator,
                workflow_label=workflow_path.name,
                unreachable_calls=unreachable_calls,
            )
            for diagnostic in plan.diagnostics:
                bus.emit(
                    TaskNotice(
                        run_id=run_id,
                        task_id=diagnostic.task_id,
                        task_name=diagnostic.task_name,
                        display_label=diagnostic.label,
                        message=diagnostic.message,
                    )
                )
            status = "failed" if plan.diagnostics else "success"
            bus.emit(
                RunCompleted(run_id=run_id, status=status, task_counts={"validated": task_count})
            )
            if plan.diagnostics:
                return 1
        elif plan_preview:
            plan = build_dry_run_plan(
                evaluator=evaluator,
                workflow_label=workflow_path.name,
                unreachable_calls=unreachable_calls,
            )
            render_dry_run_plan(
                plan=plan,
                console=rich_console,
                verbose=output_mode == "verbose",
            )
            if plan.diagnostics:
                # The plan just proved the run would fail; a preflight that
                # says so must also say it to the shell.
                return 1
        else:
            rich_console.print(
                f"[green]✓[/] [bold]{escape(workflow_path.name)}[/] "
                f"[dim](dry-run)[/] [dim]- {task_count} tasks validated[/]"
            )
        return 0

    if output_mode not in {"agent", "agent_verbose"}:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_name}[/] [dim]({run_id})[/]\n"
        )
        rich_console.print(
            f"[cyan]📦[/] Loading workflow...  [green]done[/] ({format_duration(load_elapsed)})"
        )
        rich_console.print(
            f"[green]🌱[/] Building expression tree...  [bold]{task_count}[/] tasks"
        )
        if evaluator.memory is not None:
            rich_console.print(f"[cyan]🧠[/] Memory budget: [bold]{evaluator.memory}[/] GiB")
        if evaluator.gpus:
            rich_console.print(f"[cyan]🎛[/] GPU budget: [bold]{evaluator.gpus}[/]")
        if output_mode == "verbose":
            rich_console.print(
                f"[cyan]🧭[/] Verbose mode: jobs={evaluator.jobs}, cores={evaluator.cores}, "
                f"memory={evaluator.memory if evaluator.memory is not None else 'auto'}, "
                f"gpus={evaluator.gpus}, "
                f"config overlays={len(config_paths)}"
            )
            rich_console.print(f"[cyan]🗂[/] Run directory: {RUNS_ROOT / run_id}\n")
        rich_console.print("")

    run_dir = RunDir.create(run_id=run_id, root=RUNS_ROOT)
    bus = EventBus()
    # Opening the ledger is the first thing that can fail for workspace
    # reasons, and it fails the run: provenance nobody records is provenance
    # nobody can reconstruct.
    recorder = StoreRecorder(path=WorkspaceLayout.relative().db, run_dir=run_dir).start()
    bus.subscribe(recorder)
    resource_monitor = RunResourceMonitor(
        root_pid=os.getpid(),
        sink=lambda resources: bus.emit(RunResourcesSampled(run_id=run_id, resources=resources)),
    )
    warning_console = console(sys.stderr)

    def warn(message: str) -> None:
        # Kept with the run as well as printed: a notification that was not
        # sent is otherwise invisible to anyone reading the run back later.
        warning_console.print(f"[yellow]⚠[/] {escape(message)}")
        bus.emit(RunNotice(run_id=run_id, message=message))

    with ExitStack() as stack:
        stack.callback(recorder.close)
        # Stopped before the recorder closes: stopping emits a final sample.
        stack.callback(resource_monitor.stop)
        reader = stack.enter_context(query.open())
        subscriber_context = EventSubscriberContext(
            config=runtime_params,
            secret_resolver=secret_resolver,
            store=reader.store,
            run_id=run_id,
            run_dir=run_dir.path,
            workflow_path=workflow_path,
            warn=warn,
        )
        _warn_unserved_notifications(config=runtime_params, warn=subscriber_context.warn)
        for subscriber_type in plugin_registry().all(EventSubscriberType):
            subscriber = subscriber_type.build(subscriber_context)
            if subscriber is None:
                continue
            stack.callback(subscriber.close)
            # Registered on the recorder, not the bus: a subscriber may ask
            # the store about what it is reacting to, so it must not run
            # until those events are committed.
            recorder.on_committed(subscriber.handle)
        renderer = None
        if output_mode in {"agent", "agent_verbose"}:
            bus.subscribe(
                JsonlEventRenderer(
                    stream=sys.stdout,
                    include_task_logs=output_mode == "agent_verbose",
                )
            )
        else:
            renderer = CliRunRenderer(
                console=rich_console,
                summary=CliRunSummary(
                    run_id=run_id,
                    mode=output_mode,
                    run_dir=run_dir.path,
                    cores=evaluator.cores,
                    memory=memory,
                    executor_label=(
                        executor_registry.label(executor_registry.default_name)
                        if executor_registry.default_name is not None
                        else "local"
                    ),
                    pinned_executors=pinned_executors,
                ),
                resources=ResourceRenderState(provider=resource_monitor.current_summary),
            )
            bus.subscribe(RichEventRenderer(renderer=renderer))
        evaluator = ConcurrentEvaluator(
            run_dir=run_dir,
            event_bus=bus,
            trust_mtimes=trust_mtimes,
            **evaluator_kwargs,
        )
        if renderer is not None:
            renderer.start(planned_tasks=planned_tasks)
        bus.emit(
            RunStarted(
                run_id=run_id,
                workflow=str(workflow_path),
                jobs=jobs,
                cores=cores,
                memory=memory,
                # Declared parameters layer over the loaded config so the
                # record shows the values the run actually used, including
                # any given on the CLI. The raw [params] table is dropped:
                # recording it alongside would show a config value next to
                # the resolved value that superseded it. So are ginkgo's own
                # tables, which a workflow loading ginkgo.toml also reads.
                params=render_value(
                    {
                        **{
                            key: value
                            for key, value in params.items()
                            if key not in RUNTIME_CONFIG_TABLES
                        },
                        **declared_params,
                    },
                    item_limit=None,
                ),
                param_sources=param_sources,
                ginkgo_version=_ginkgo_version(),
                parent_run_id=os.environ.get(PARENT_RUN_ID_ENV) or None,
                parent_task_id=os.environ.get(PARENT_TASK_ID_ENV) or None,
            )
        )
        # A run that dies before RunCompleted — a graph that will not
        # build, an environment that will not prepare — would otherwise sit
        # in the ledger as 'running' forever, with no manifest.
        stack.callback(_close_unfinished_run, bus=bus, recorder=recorder, run_id=run_id)
        bus.emit(PhaseTimed(run_id=run_id, phase="workflow_load_seconds", seconds=load_elapsed))
        bus.emit(
            PhaseTimed(
                run_id=run_id,
                phase="workflow_validate_seconds",
                seconds=validate_elapsed,
            )
        )
        bus.emit(
            RunValidated(
                run_id=run_id,
                task_count=task_count,
                edge_count=edge_count,
                env_count=env_count,
            )
        )
        resource_monitor.start()
        run_started = time.perf_counter()
        failure: BaseException | None = None
        root_skipped: RootSkippedError | None = None
        try:
            evaluator.evaluate(expr)
            for name in sorted(set(rerun) - _rerun_names(evaluator=evaluator)):
                console(sys.stderr).print(
                    f"[yellow]⚠[/] --rerun {escape(name)} matched no task in this run."
                )
        except RootSkippedError as exc:
            # An expected outcome of a non-fatal failure policy, not a
            # crash: the run drained, it just has no result to show.
            root_skipped = exc
        except BaseException as exc:
            failure = exc

        resource_summary = resource_monitor.stop()
        bus.emit(
            PhaseTimed(
                run_id=run_id,
                phase="workflow_execute_seconds",
                seconds=time.perf_counter() - run_started,
            )
        )
        # The counts describe the tasks, which are already in the ledger;
        # flushing is what makes them readable rather than merely queued.
        recorder.flush()
        # A failure the policy let pass is still a failure: the run is
        # recorded failed whether or not it stopped dispatching.
        ignored_failures = len(evaluator.ignored_failures)
        run_failed = failure is not None or root_skipped is not None or ignored_failures > 0
        # An interrupt is not a failure. It gets its own terminal status so
        # that the ledger can tell "someone pressed Ctrl-C" apart from
        # "the workflow broke" — and so the row closes at all, which is
        # what a run left at 'running' forever never did.
        interrupt = failure if isinstance(failure, KeyboardInterrupt) else None
        bus.emit(
            RunCompleted(
                run_id=run_id,
                status=(
                    "cancelled"
                    if interrupt is not None
                    else ("failed" if run_failed else "success")
                ),
                task_counts=reader.task_status_counts(run_id),
                resources=resource_summary,
                error=_run_error_message(
                    failure=failure,
                    root_skipped=root_skipped,
                    ignored_failures=ignored_failures,
                ),
            )
        )
        run_summary = reader.run(run_id)

        if interrupt is not None:
            # No failure panel: nothing went wrong, the run was stopped.
            # The tally still prints, because what did finish before the
            # interrupt is the one thing worth knowing here.
            if renderer is not None:
                renderer.finish(
                    elapsed=time.perf_counter() - run_started,
                    success=False,
                    cancelled=True,
                    resources=resource_summary,
                    remote_summary=evaluator.remote_stats.summary(),
                    path_tracked_inputs=_count_path_tracked_inputs(run_summary=run_summary),
                )
                print(f"Run directory: {run_dir.path}", file=sys.stderr)
            # Re-raised so the top level prints its interrupt line and
            # exits 130, as it does for an interrupt anywhere else.
            raise interrupt

        if failure is not None:
            if renderer is not None:
                failure_details = _load_failure_details(
                    run_summary=run_summary,
                    renderer=renderer,
                    verbose=output_mode == "verbose",
                )
                renderer.finish(
                    elapsed=time.perf_counter() - run_started,
                    success=False,
                    resources=resource_summary,
                    failure_details=failure_details,
                    skipped=_load_skip_details(
                        run_summary=run_summary,
                        renderer=renderer,
                    ),
                    remote_summary=evaluator.remote_stats.summary(),
                    path_tracked_inputs=_count_path_tracked_inputs(run_summary=run_summary),
                )
                print(f"Run directory: {run_dir.path}", file=sys.stderr)
            raise failure

        if run_failed:
            # Nothing stopped the run, so it is reported like any other
            # finished run: the failures and what they cost, and also the
            # notebooks the surviving branches really did
            # produce. Only the summary line differs from a success.
            if renderer is not None:
                renderer.finish(
                    elapsed=time.perf_counter() - run_started,
                    success=False,
                    resources=resource_summary,
                    failure_details=_load_failure_details(
                        run_summary=run_summary,
                        renderer=renderer,
                        verbose=output_mode == "verbose",
                    ),
                    skipped=_load_skip_details(
                        run_summary=run_summary,
                        renderer=renderer,
                    ),
                    pages=_rendered_pages(
                        run_summary=run_summary,
                        renderer=renderer,
                    ),
                    remote_summary=evaluator.remote_stats.summary(),
                    path_tracked_inputs=_count_path_tracked_inputs(run_summary=run_summary),
                )
            return IGNORED_FAILURES_EXIT_CODE

        if renderer is not None:
            renderer.finish(
                elapsed=time.perf_counter() - run_started,
                success=True,
                resources=resource_summary,
                pages=_rendered_pages(
                    run_summary=run_summary,
                    renderer=renderer,
                ),
                remote_summary=evaluator.remote_stats.summary(),
                path_tracked_inputs=_count_path_tracked_inputs(run_summary=run_summary),
            )
    return 0


def _rerun_names(*, evaluator: ConcurrentEvaluator) -> set[str]:
    """Return every name ``--rerun`` selects one of the graph's tasks by."""
    return {name for node in evaluator.task_nodes.values() for name in node.rerun_names}


def _defined_task_names(*, root: Path) -> set[str]:
    """Return the qualified and base names of every task defined under *root*.

    Read from the modules already imported from the project, the workflow and
    every module it imports, so a task only a dynamic expansion returns is
    counted too.
    """
    names: set[str] = set()
    for module in list(sys.modules.values()):
        module_file = getattr(module, "__file__", None)
        if module_file is None or "site-packages" in Path(module_file).parts:
            continue
        if not Path(module_file).resolve().is_relative_to(root.resolve()):
            continue
        for value in list(vars(module).values()):
            if isinstance(value, TaskDef):
                names |= {value.name, value.base_name}
    return names


def _print_dropped_calls(*, diagnostics: list[WorkflowDiagnostic]) -> None:
    """Warn on stderr about each task call dropped from the graph, with its fix."""
    for diagnostic in diagnostics:
        console(sys.stderr).print(f"[yellow]⚠[/] {escape(diagnostic.message)}")
        if diagnostic.suggestion:
            console(sys.stderr).print(f"  [dim]{escape(diagnostic.suggestion)}[/]")


def _close_unfinished_run(*, bus: EventBus, recorder: StoreRecorder, run_id: str) -> None:
    """Close the run in the ledger if it is unwinding without having completed.

    Registered on the exit stack rather than written into one exception
    handler, so it covers every way out of the run — including the ones nobody
    has thought of yet.
    """
    if recorder.completed:
        return
    exc = sys.exception()
    # An interrupt reaching here — one raised before or after the scheduler
    # loop, where the run's own handling would have caught it — is still an
    # interrupt, and is recorded as one rather than as a failure.
    cancelled = isinstance(exc, KeyboardInterrupt)
    bus.emit(
        RunCompleted(
            run_id=run_id,
            status="cancelled" if cancelled else "failed",
            error=str(exc) if exc is not None else "The run ended before it completed.",
        )
    )


def _run_error_message(
    *,
    failure: BaseException | None,
    root_skipped: RootSkippedError | None,
    ignored_failures: int,
) -> str | None:
    """Return the one line the ledger records as the run's error."""
    if failure is not None:
        return str(failure)
    if root_skipped is not None:
        return str(root_skipped)
    if ignored_failures:
        plural = "task" if ignored_failures == 1 else "tasks"
        return f"{ignored_failures} {plural} failed; the run continued under its failure policy."
    return None


def _load_skip_details(
    *,
    run_summary: RunSummary,
    renderer: CliRunRenderer,
) -> list[SkipDetails]:
    """Load the tasks a failure left without a result.

    A blocking task is named by the label the run gave it, the same way the
    skipped task itself is. Base names alone would count two same-named tasks
    from different modules — or two branches of one fan-out — as one.
    """
    labels = {
        task.task_key: _task_label(task=task, renderer=renderer) for task in run_summary.tasks
    }
    details: list[SkipDetails] = []
    for task in run_summary.tasks:
        if task.status != "skipped":
            continue
        blocker = task.skipped_because or {}
        blocker_name = blocker.get("task_name")
        blocker_label = labels.get(str(blocker.get("task_id"))) or (
            task_base_name(blocker_name)
            if isinstance(blocker_name, str) and blocker_name
            else "a failed task"
        )
        reason = blocker.get("reason")
        if reason == "interrupted":
            cause = "not started: the run was interrupted"
        elif reason == "run_stopped":
            cause = (
                f"not started: the run stopped when {blocker_label} failed"
                if blocker.get("task_id")
                else "not started: the run stopped"
            )
        else:
            cause = f"blocked by {blocker_label}"
        details.append(SkipDetails(task_label=labels[task.task_key], cause=cause))
    return details


def _task_label(*, task: TaskSummary, renderer: CliRunRenderer) -> str:
    """Return the label this run showed for a task, or its base name."""
    return renderer.label_for_node(task.node_id if task.node_id is not None else -1) or (
        task.base_name
    )


def _load_failure_details(
    *,
    run_summary: RunSummary,
    renderer: CliRunRenderer,
    verbose: bool,
) -> list[FailureDetails]:
    """Load failed-task diagnostics from a finished run."""
    run_dir = run_summary.run_dir
    tail_lines = 20 if verbose else 10
    return [
        FailureDetails(
            task_label=_task_label(task=task, renderer=renderer),
            exit_code=task.exit_code,
            log_path=run_dir / task.stderr_log if task.stderr_log is not None else None,
            log_tail=combined_log_tail(
                run_dir=run_dir,
                stdout_log=task.stdout_log,
                stderr_log=task.stderr_log,
                lines=tail_lines,
            ),
            error=task.error,
            failure_kind=task.failure_kind,
            inputs=task.inputs if verbose else None,
            task_kind=task.kind,
            env_label=task.env or "local",
            ignored=task.ignored,
        )
        for task in run_summary.failed_tasks
    ]


def _rendered_pages(
    *,
    run_summary: RunSummary,
    renderer: CliRunRenderer,
) -> list[CliRenderedPage]:
    """List the pages this run's tasks rendered, under their runtime labels."""
    pages: list[CliRenderedPage] = []
    for task in run_summary.tasks:
        html_path = task.rendered_html_absolute(run_dir=run_summary.run_dir)
        if html_path is None:
            continue
        pages.append(
            CliRenderedPage(
                task_label=renderer.label_for_node(task.node_id) or task.label,
                html_path=html_path,
                replayed_from_run_id=(
                    task.rendered_html_run_id
                    if task.rendered_html_run_id not in (None, run_summary.run_id)
                    else None
                ),
            )
        )
    return pages


def _count_path_tracked_inputs(*, run_summary: RunSummary) -> int:
    """Count inputs of executed or cached tasks tracked by path string only.

    Counts every ``(task, parameter)`` pair labelled ``"path"`` — the
    silent-staleness trap of issues #121/#281, where a task input names an
    existing path but is annotated as a plain scalar, so the cache key
    tracks the string rather than the bytes it points at. A task the run
    never started or that only failed or was skipped contributes nothing:
    the label only matters for a task whose result the cache now holds.
    """
    return sum(
        1
        for task in run_summary.tasks
        if task.status in {"succeeded", "cached"} and task.input_trackings
        for label in task.input_trackings.values()
        if label == "path"
    )


def _warn_unserved_notifications(*, config: Any, warn: Callable[[str], None]) -> None:
    """Warn about each ``[notifications.<name>]`` table no installed subscriber reads.

    Without this, notifications configured for a plugin that is not installed
    (``[notifications.slack]`` without ginkgo-slack) would silently never send.
    """
    notifications = config.get("notifications") if isinstance(config, Mapping) else None
    if not isinstance(notifications, Mapping):
        return
    for name in sorted(notifications):
        try:
            plugin_registry().get(EventSubscriberType, name)
        except PluginNotFoundError as exc:
            warn(f"[notifications.{name}] is configured but nothing will send it. {exc}")
