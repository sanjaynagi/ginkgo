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

from ginkgo.cli.common import RUNS_ROOT, RunMode, console
from ginkgo.cli.renderers.common import environment_label
from ginkgo.formatting import format_duration
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.cli.renderers.models import (
    CliAssetSummary,
    FailureDetails,
    CliNotebookSummary,
    ResourceRenderState,
    CliRunSummary,
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
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.resources import RunResourceMonitor
from ginkgo.runtime.caching.provenance import (
    RunProvenanceRecorder,
    combined_log_tail,
    make_run_id,
)
from ginkgo.runtime.diagnostics import unreachable_call_diagnostics
from ginkgo.runtime.dry_run import build_dry_run_plan
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.events import EventBus, RunCompleted, RunStarted, RunValidated
from ginkgo.runtime.notifications.notifications import build_notification_service
from ginkgo.runtime.profiling import ProfileRecorder
from ginkgo.runtime.run_summary import RunSummary


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

    # Machine-readable marker for sub-workflow parent runs to capture the
    # child run id without parsing Rich-formatted output.
    if os.environ.get("GINKGO_CALLED_FROM_PARENT_RUN"):
        sys.stdout.write(f"GINKGO_CHILD_RUN_ID={run_id}\n")
        sys.stdout.flush()

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

    remote_executor = None
    code_bundle_config = None
    if executor == "k8s":
        remote_executor = _build_k8s_executor(runtime_config=runtime_config)
        code_bundle_config = _load_code_bundle_config(runtime_config=runtime_config)
    elif executor == "batch":
        remote_executor = _build_batch_executor(runtime_config=runtime_config)
        code_bundle_config = _load_code_bundle_config(runtime_config=runtime_config)

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
        "remote_executor": remote_executor,
        "code_bundle_config": code_bundle_config,
        "secret_resolver": secret_resolver,
        "profiler": profiler,
        "param_context": param_context,
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
            bus.emit(
                RunCompleted(
                    run_id=run_id, status="success", task_counts={"validated": task_count}
                )
            )
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

    recorder = RunProvenanceRecorder(
        run_id=run_id,
        workflow_path=workflow_path,
        root_dir=RUNS_ROOT,
        jobs=jobs,
        cores=cores,
        memory=memory,
        # Declared parameters layer over the loaded config so the record shows
        # the values the run actually used, including any given on the CLI. The
        # raw [params] table is dropped: recording it alongside would show a
        # config value next to the resolved value that superseded it.
        params={
            **{key: value for key, value in params.items() if key != PARAMS_CONFIG_KEY},
            **declared_params,
        },
        param_sources=param_sources,
    )
    recorder.add_run_timing(phase="workflow_load_seconds", seconds=load_elapsed)
    recorder.add_run_timing(phase="workflow_validate_seconds", seconds=validate_elapsed)
    resource_monitor = RunResourceMonitor(
        root_pid=os.getpid(),
        sink=recorder.update_resources,
    )
    with profiler.timed("resource_monitor_startup"):
        resource_monitor.start()
    warning_console = console(sys.stderr)
    notification_service = build_notification_service(
        config=runtime_params,
        resolver=secret_resolver,
        run_dir=recorder.run_dir,
        workflow_path=workflow_path,
        logger=lambda message: warning_console.print(f"[yellow]⚠[/] {message}"),
    )
    try:
        with ExitStack() as stack:
            events_stream = stack.enter_context(recorder.events_path.open("a", encoding="utf-8"))
            bus = EventBus()
            bus.subscribe(JsonlEventRenderer(stream=events_stream, include_task_logs=True))
            if notification_service is not None:
                bus.subscribe(notification_service.handle)
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
                        run_dir=recorder.run_dir,
                        cores=evaluator.cores,
                        memory=memory,
                        executor=executor,
                    ),
                    resources=ResourceRenderState(provider=resource_monitor.current_summary),
                )
                bus.subscribe(RichEventRenderer(renderer=renderer))
            evaluator = ConcurrentEvaluator(
                provenance=recorder,
                event_bus=bus,
                trust_mtimes=trust_mtimes,
                **evaluator_kwargs,
            )
            if renderer is not None:
                renderer.start(planned_tasks=planned_tasks)
            bus.emit(RunStarted(run_id=run_id, workflow=str(workflow_path)))
            bus.emit(
                RunValidated(
                    run_id=run_id,
                    task_count=task_count,
                    edge_count=edge_count,
                    env_count=env_count,
                )
            )
            run_started = time.perf_counter()
            try:
                evaluator.evaluate(expr)
            except BaseException as exc:
                recorder.add_run_timing(
                    phase="workflow_execute_seconds",
                    seconds=time.perf_counter() - run_started,
                )
                with profiler.timed("resource_monitor_shutdown"):
                    resource_summary = resource_monitor.stop()
                with profiler.timed("provenance_finalize"):
                    recorder.finalize(status="failed", error=str(exc), resources=resource_summary)
                with profiler.timed("manifest_load"):
                    run_summary = RunSummary.load(recorder.run_dir)
                bus.emit(
                    RunCompleted(
                        run_id=run_id,
                        status="failed",
                        task_counts=dict(run_summary.task_counts()),
                        error=str(exc),
                    )
                )
                if renderer is not None:
                    failure_details = _load_failure_details(
                        run_dir=recorder.run_dir,
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
                            remote_summary=evaluator.remote_stats.summary(),
                        )
                    print(f"Run directory: {recorder.run_dir}", file=sys.stderr)
                if profiler.enabled:
                    recorder.set_profile(profile=profiler.snapshot())
                    _print_profile_table(console=rich_console, profile=profiler.snapshot())
                raise

            with profiler.timed("resource_monitor_shutdown"):
                resource_summary = resource_monitor.stop()
            recorder.add_run_timing(
                phase="workflow_execute_seconds",
                seconds=time.perf_counter() - run_started,
            )
            with profiler.timed("provenance_finalize"):
                recorder.finalize(status="succeeded", resources=resource_summary)
            with profiler.timed("manifest_load"):
                run_summary = RunSummary.load(recorder.run_dir)
            bus.emit(
                RunCompleted(
                    run_id=run_id,
                    status="success",
                    task_counts=dict(run_summary.task_counts()),
                )
            )
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
                recorder.set_profile(profile=profiler.snapshot())
                _print_profile_table(console=rich_console, profile=profiler.snapshot())
    finally:
        if notification_service is not None:
            notification_service.close()
    return 0


def _load_failure_details(
    *,
    run_dir: Path,
    run_summary: RunSummary,
    renderer: CliRunRenderer,
    verbose: bool,
) -> list[FailureDetails]:
    """Load failed-task diagnostics from a finished run."""
    details: list[FailureDetails] = []
    tail_lines = 20 if verbose else 10
    for task in run_summary.failed_tasks:
        node_id = task.node_id if task.node_id is not None else -1
        log_tail = combined_log_tail(
            run_dir=run_dir,
            stdout_log=task.stdout_log,
            stderr_log=task.stderr_log,
            lines=tail_lines,
        )
        stderr_path = run_dir / task.stderr_log if isinstance(task.stderr_log, str) else None
        failure_kind = (
            task.failure.get("kind")
            if isinstance(task.failure, dict) and isinstance(task.failure.get("kind"), str)
            else None
        )
        details.append(
            FailureDetails(
                task_label=renderer.label_for_node(node_id) or task.name,
                exit_code=task.exit_code,
                log_path=stderr_path,
                log_tail=log_tail,
                error=task.error,
                failure_kind=failure_kind,
                inputs=task.inputs if verbose else None,
            )
        )
    return details


def _print_profile_table(
    *,
    console,
    profile: dict[str, dict[str, float | int]],
) -> None:
    """Print a Rich profile table summarising recorded phase timings."""
    from rich.table import Table

    table = Table(title="Runtime Profile", show_lines=False)
    table.add_column("phase")
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


def _load_code_bundle_config(*, runtime_config: dict[str, Any]) -> dict[str, Any] | None:
    """Read code-sync configuration from ``[remote.k8s.code]`` or ``[remote.batch.code]``."""
    remote = runtime_config.get("remote", {})
    for section in ("k8s", "batch"):
        backend_config = remote.get(section, {})
        if not isinstance(backend_config, dict):
            continue
        code_config = backend_config.get("code")
        if isinstance(code_config, dict):
            return dict(code_config)
    return None


def _build_k8s_executor(*, runtime_config: dict[str, Any]) -> Any:
    """Construct a ``KubernetesExecutor`` from ``[remote.k8s]`` config."""
    from ginkgo.remote.kubernetes import KubernetesExecutor

    k8s_config = runtime_config.get("remote", {}).get("k8s", {})
    if not isinstance(k8s_config, dict):
        k8s_config = {}

    image = k8s_config.get("image")
    if not image:
        raise ValueError(
            "Kubernetes executor requires an image. Set [remote.k8s] image in ginkgo.toml."
        )

    return KubernetesExecutor(
        namespace=k8s_config.get("namespace", "default"),
        image=image,
        service_account=k8s_config.get("service_account"),
        pull_policy=k8s_config.get("pull_policy", "IfNotPresent"),
        gpu_type=k8s_config.get("gpu_type"),
        node_selector=k8s_config.get("node_selector"),
        tolerations=k8s_config.get("tolerations"),
        ttl_seconds_after_finished=int(k8s_config.get("ttl_seconds_after_finished", 3600)),
        ephemeral_storage=k8s_config.get("ephemeral_storage", "10Gi"),
        backoff_limit=int(k8s_config.get("backoff_limit", 2)),
        fuse_image=k8s_config.get("fuse_image"),
        fuse_annotations=k8s_config.get("fuse_annotations"),
        fuse_privileged=bool(k8s_config.get("fuse_privileged", False)),
    )


def _build_batch_executor(*, runtime_config: dict[str, Any]) -> Any:
    """Construct a ``GCPBatchExecutor`` from ``[remote.batch]`` config."""
    from ginkgo.remote.gcp_batch import GCPBatchExecutor

    batch_config = runtime_config.get("remote", {}).get("batch", {})
    if not isinstance(batch_config, dict):
        batch_config = {}

    project = batch_config.get("project")
    if not project:
        raise ValueError(
            "GCP Batch executor requires a project. Set [remote.batch] project in ginkgo.toml."
        )

    image = batch_config.get("image")
    if not image:
        raise ValueError(
            "GCP Batch executor requires an image. Set [remote.batch] image in ginkgo.toml."
        )

    region = batch_config.get("region", "europe-west2")

    return GCPBatchExecutor(
        project=project,
        region=region,
        image=image,
        service_account=batch_config.get("service_account"),
        gpu_type=batch_config.get("gpu_type"),
        gpu_driver_version=batch_config.get("gpu_driver_version", "LATEST"),
        max_run_duration=batch_config.get("max_run_duration", "3600s"),
        fuse_image=batch_config.get("fuse_image"),
        fuse_privileged=bool(batch_config.get("fuse_privileged", False)),
    )
