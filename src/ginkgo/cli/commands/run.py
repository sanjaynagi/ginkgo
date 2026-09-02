"""Run command handlers."""

from __future__ import annotations

import os

# Suppress gRPC fork-safety warnings on macOS. Must be set before grpc is imported.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")

import sys
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Sequence

from rich.markup import escape

from ginkgo import query
from ginkgo.cli.common import RUNS_ROOT, RunMode, console, new_table
from ginkgo.cli.renderers.common import environment_label
from ginkgo.formatting import format_duration
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.cli.errors import IGNORED_FAILURES_EXIT_CODE
from ginkgo.cli.renderers.models import (
    CliAssetSummary,
    FailureDetails,
    CliNotebookSummary,
    ResourceRenderState,
    CliRunSummary,
    SkipDetails,
)
from ginkgo.cli.renderers.rich import RichEventRenderer
from ginkgo.cli.renderers.run import CliRunRenderer
from ginkgo.cli.workflow_params import (
    collect_param_declarations,
    global_param_reads,
    params_table,
    validate_param_extras,
)
from ginkgo.cli.workspace import resolve_envs_workflow_root, resolve_workflow_path
from ginkgo.config import (
    PARAMS_CONFIG_KEY,
    config_session,
    load_runtime_config_layers,
    merge_config_layers,
)
from ginkgo.core.expr import display_labels, record_constructed_calls
from ginkgo.core.resources import (
    ResourceOverrides,
    parse_resource_budget_args,
    resource_budgets_from_config,
)
from ginkgo.core.flow import discover_flow
from ginkgo.params import ParamContext, format_param_help
from ginkgo.envs.container import container_backend_from_config
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.backend import CompositeEnvironment, LocalEnvironment
from ginkgo.runtime.evaluator import ConcurrentEvaluator, RootSkippedError
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.resources import RunResourceMonitor
from ginkgo.runtime.rundir import RunDir, combined_log_tail, make_run_id
from ginkgo.runtime.diagnostics import unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.event_values import render_value
from ginkgo.runtime.events import (
    EventBus,
    PhaseTimed,
    RunCompleted,
    RunResourcesSampled,
    RunStarted,
    RunValidated,
    TaskNotice,
)
from ginkgo.runtime.notifications.notifications import build_notification_service
from ginkgo.runtime.profiling import ProfileRecorder
from ginkgo.runtime.run_summary import RunSummary
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
    """Handle ``ginkgo run``."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path
    return run_workflow(
        workflow_path=workflow_path,
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
        profile=getattr(args, "profile", False),
        executor=getattr(args, "executor", "local"),
        param_extras=getattr(args, "param_extras", ()),
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
    rich_console = console(sys.stdout)
    rich_console.print(usage.rstrip(), highlight=False)

    try:
        workflow_path = resolve_workflow_path(
            project_root=Path.cwd(),
            workflow=args.workflow,
        ).path
    except BaseException:
        # No workflow resolved, so the usage text alone is the whole answer.
        return 0

    try:
        declarations = collect_param_declarations(
            workflow_path=workflow_path,
            config_paths=[Path(path).resolve() for path in args.config],
        )
    except BaseException as exc:
        rich_console.print(
            f"\n[yellow]⚠[/] Could not import {workflow_path.name} to list its parameters: {exc}"
        )
        return 0

    rich_console.print(f"\nparameters declared by {workflow_path.name}:", highlight=False)
    for line in format_param_help(declarations) or ["  (none)"]:
        rich_console.print(line, highlight=False)
    return 0


def planned_task_rows(evaluator: ConcurrentEvaluator) -> list[tuple[int, str, str, str]]:
    """Return the run table's seed rows for a validated graph.

    Each row is ``(node_id, task_name, label, env_label)``. The label comes
    from the graph, the same source ``--dry-run`` labels its plan from, so a
    fan-out branch reads the same in both before it is dispatched.
    """
    labels = display_labels({node_id: node.expr for node_id, node in evaluator.task_nodes.items()})
    return [
        (
            node.node_id,
            node.task_def.name,
            labels[node.node_id],
            environment_label(node.task_def.env),
        )
        for node in sorted(evaluator.task_nodes.values(), key=lambda item: item.node_id)
    ]


def run_workflow(
    *,
    workflow_path: Path,
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
    profile: bool = False,
    executor: str = "local",
    plan_preview: bool = True,
    param_extras: Sequence[str] = (),
) -> int:
    profiler = ProfileRecorder(enabled=profile)
    cli_startup_started = time.perf_counter()

    run_id = make_run_id(workflow_path=workflow_path)
    rich_console = console(sys.stdout)
    if dry_run and output_mode not in {"agent", "agent_verbose"}:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_path.name}[/] [bold]--dry-run[/]\n"
        )
    elif output_mode not in {"agent", "agent_verbose"}:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_path.name}[/] [dim]({run_id})[/]\n"
        )

    profiler.record(phase="cli_startup", seconds=time.perf_counter() - cli_startup_started)

    load_started = time.perf_counter()
    # The runtime config is loaded before the workflow is imported so that
    # declared parameters resolve against it regardless of whether the workflow
    # calls config() before or after param().
    with profiler.timed("runtime_config_load"):
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
        with profiler.timed("workflow_module_import"):
            module = load_module_from_path(workflow_path)
        with profiler.timed("flow_construction"):
            flow = discover_flow(module)
            with record_constructed_calls() as constructed_calls:
                expr = flow()
        # Validated after the flow body has run, so parameters declared inside
        # it have had their chance to claim a flag. Deliberately outside the
        # construction recorder: parameter resolution mints no task calls.
        validate_param_extras(session)
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

    registry = PixiRegistry(
        project_root=Path.cwd(),
        workflow_root=resolve_envs_workflow_root(project_root=Path.cwd()),
    )
    secret_resolver = build_secret_resolver(
        project_root=Path.cwd(),
        config=runtime_params,
        environ=os.environ,
    )
    backend = CompositeEnvironment(
        local=LocalEnvironment(pixi_registry=registry),
        container=container_backend_from_config(project_root=Path.cwd(), config=runtime_config),
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
        "profiler": profiler,
        "param_context": param_context,
        "keep_going": keep_going,
    }
    evaluator = ConcurrentEvaluator(
        constructed_calls=tuple(constructed_calls),
        **evaluator_kwargs,
    )
    validate_started = time.perf_counter()
    with profiler.timed("evaluator_validate"):
        evaluator.build_and_validate(expr)
    validate_elapsed = time.perf_counter() - validate_started

    # A parameter must reach a task as an argument. One read from a module global
    # is invisible to that task's cache key, so a changed value would silently
    # reuse the previous result. Detection is best-effort, hence a warning.
    for finding in global_param_reads(
        declaration_globals=declaration_globals,
        evaluator=evaluator,
    ):
        console(sys.stderr).print(f"[yellow]⚠[/] {finding.message()}")

    # A call the flow never returns is not in the graph, so its side effects
    # never happen. Warned about for the same reason: the run otherwise looks
    # like a smaller but healthy one. Suppressed only when the dry-run plan is
    # about to render its own "Dropped" section, which would say it twice.
    plan_reports_dropped = (
        dry_run and plan_preview and output_mode not in {"agent", "agent_verbose"}
    )
    if not plan_reports_dropped:
        for diagnostic in unreachable_call_diagnostics(calls=evaluator.unreachable_calls):
            console(sys.stderr).print(f"[yellow]⚠[/] {diagnostic.message}")

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
    edge_count = sum(len(node.dependency_ids) for node in evaluator.task_nodes.values())
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
            plan = build_dry_run_plan(evaluator=evaluator, workflow_label=workflow_path.name)
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
                f"[green]✓[/] [bold]{workflow_path.name}[/] "
                f"[dim](dry-run)[/] [dim]- {task_count} tasks validated[/]"
            )
        return 0

    if output_mode not in {"agent", "agent_verbose"}:
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
    try:
        with ExitStack() as stack:
            stack.callback(recorder.close)
            reader = stack.enter_context(query.open())
            notification_service = build_notification_service(
                config=runtime_params,
                resolver=secret_resolver,
                store=reader.store,
                run_id=run_id,
                run_dir=run_dir.path,
                workflow_path=workflow_path,
                logger=lambda message: warning_console.print(f"[yellow]⚠[/] {message}"),
            )
            if notification_service is not None:
                stack.callback(notification_service.close)
                # Registered on the recorder, not the bus: the service asks the
                # store which tasks failed, so it must not run until the events
                # it is reacting to are committed.
                recorder.on_committed(notification_service.handle)
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
                    # the resolved value that superseded it.
                    params=render_value(
                        {
                            **{
                                key: value
                                for key, value in params.items()
                                if key != PARAMS_CONFIG_KEY
                            },
                            **declared_params,
                        }
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
            bus.emit(
                PhaseTimed(run_id=run_id, phase="workflow_load_seconds", seconds=load_elapsed)
            )
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
            with profiler.timed("resource_monitor_startup"):
                resource_monitor.start()
            run_started = time.perf_counter()
            failure: BaseException | None = None
            root_skipped: RootSkippedError | None = None
            try:
                evaluator.evaluate(expr)
            except RootSkippedError as exc:
                # An expected outcome of a non-fatal failure policy, not a
                # crash: the run drained, it just has no result to show.
                root_skipped = exc
            except BaseException as exc:
                failure = exc

            with profiler.timed("resource_monitor_shutdown"):
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
            bus.emit(
                RunCompleted(
                    run_id=run_id,
                    status="failed" if run_failed else "success",
                    task_counts=reader.task_status_counts(run_id),
                    resources=resource_summary,
                    error=_run_error_message(
                        failure=failure,
                        root_skipped=root_skipped,
                        ignored_failures=ignored_failures,
                    ),
                )
            )
            with profiler.timed("run_summary_load"):
                run_summary = reader.run(run_id)

            if failure is not None:
                if renderer is not None:
                    failure_details = _load_failure_details(
                        run_summary=run_summary,
                        renderer=renderer,
                        verbose=output_mode == "verbose",
                    )
                    with profiler.timed("renderer_finish"):
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
                        )
                    print(f"Run directory: {run_dir.path}", file=sys.stderr)
                if profiler.enabled:
                    _print_profile_table(console=rich_console, profile=profiler.snapshot())
                raise failure

            if run_failed:
                # Nothing stopped the run, so it is reported like any other
                # finished run — with the failures and the tasks they cost.
                if renderer is not None:
                    with profiler.timed("renderer_finish"):
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
                            remote_summary=evaluator.remote_stats.summary(),
                        )
                    print(f"Run directory: {run_dir.path}", file=sys.stderr)
                if profiler.enabled:
                    _print_profile_table(console=rich_console, profile=profiler.snapshot())
                return IGNORED_FAILURES_EXIT_CODE

            if renderer is not None:
                with profiler.timed("renderer_finish"):
                    renderer.finish(
                        elapsed=time.perf_counter() - run_started,
                        success=True,
                        resources=resource_summary,
                        notebooks=_render_notebooks(
                            run_summary=run_summary,
                            renderer=renderer,
                        ),
                        assets=_render_assets(run_summary=run_summary),
                        remote_summary=evaluator.remote_stats.summary(),
                    )
            if profiler.enabled:
                _print_profile_table(console=rich_console, profile=profiler.snapshot())
    finally:
        resource_monitor.stop()
    return 0


def _close_unfinished_run(*, bus: EventBus, recorder: StoreRecorder, run_id: str) -> None:
    """Fail the run in the ledger if it is unwinding without having completed.

    Registered on the exit stack rather than written into one exception
    handler, so it covers every way out of the run — including the ones nobody
    has thought of yet.
    """
    if recorder.completed:
        return
    exc = sys.exception()
    bus.emit(
        RunCompleted(
            run_id=run_id,
            status="failed",
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
    """Load the tasks an ancestor's failure cost this run."""
    return [
        SkipDetails(
            task_label=(
                renderer.label_for_node(task.node_id if task.node_id is not None else -1)
                or task.name
            ),
            ancestor_label=_base_task_name(
                (task.skipped_because or {}).get("task_name") or "an earlier task"
            ),
        )
        for task in run_summary.tasks
        if task.status == "skipped"
    ]


def _base_task_name(name: str) -> str:
    """Return a task's name without its module prefix."""
    return name.rsplit(".", 1)[-1]


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
            task_label=(
                renderer.label_for_node(task.node_id if task.node_id is not None else -1)
                or task.name
            ),
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


def _print_profile_table(
    *,
    console,
    profile: dict[str, dict[str, float | int]],
) -> None:
    """Print a Rich profile table summarising recorded phase timings."""
    table = new_table("phase")
    table.title = "Runtime Profile"
    table.add_column("seconds", justify="right")
    table.add_column("count", justify="right")

    rows = sorted(
        profile.items(),
        key=lambda item: float(item[1].get("seconds", 0.0)),
        reverse=True,
    )
    for phase, values in rows:
        table.add_row(
            phase,
            f"{float(values.get('seconds', 0.0)):.4f}",
            str(int(values.get("count", 0))),
        )
    console.print("")
    console.print(table)


def _render_notebooks(
    *,
    run_summary: RunSummary,
    renderer: CliRunRenderer,
) -> list[CliNotebookSummary]:
    """Build CLI-renderer notebook rows from a run summary.

    Resolves rendered HTML paths against the run directory, substitutes
    runtime task labels when the renderer has them, and marks rows whose
    artifact an earlier run produced and this run only replayed from cache.
    """
    rows: list[CliNotebookSummary] = []
    for notebook in run_summary.notebooks:
        if notebook.rendered_html is None:
            continue
        html_path = (run_summary.run_dir / notebook.rendered_html).resolve()
        task_summary = next(
            (task for task in run_summary.tasks if task.task_key == notebook.task_key),
            None,
        )
        node_id = task_summary.node_id if task_summary is not None else None
        task_label = (
            renderer.label_for_node(node_id) if isinstance(node_id, int) else None
        ) or notebook.base_name
        rows.append(
            CliNotebookSummary(
                task_label=task_label,
                html_path=html_path,
                render_status=notebook.render_status,
                render_error=notebook.render_error,
                replayed_from_run_id=(
                    notebook.notebook_artifact_run_id
                    if notebook.notebook_artifact_run_id not in (None, run_summary.run_id)
                    else None
                ),
            )
        )
    return rows


def _render_assets(*, run_summary: RunSummary) -> list[CliAssetSummary]:
    """Build CLI-renderer asset rows from a run summary."""
    return [CliAssetSummary(name=asset.name) for asset in run_summary.assets]
