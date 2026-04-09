"""Run command handlers."""

from __future__ import annotations

import os
import sys
import time
from contextlib import ExitStack
from pathlib import Path
from types import ModuleType
from typing import Any

from ginkgo.cli.common import RUNS_ROOT, RunMode, console
from ginkgo.cli.renderers.common import _environment_label, _format_duration
from ginkgo.cli.renderers.jsonl import JsonlEventRenderer
from ginkgo.cli.renderers.models import (
    _AssetSummary,
    _FailureDetails,
    _NotebookSummary,
    _ResourceRenderState,
    _RunSummary,
)
from ginkgo.cli.renderers.rich import RichEventRenderer
from ginkgo.cli.renderers.run import _CliRunRenderer
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session, load_runtime_config
from ginkgo.core.flow import FlowDef
from ginkgo.envs.container import ContainerBackend
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.backend import CompositeBackend, LocalBackend
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.resources import RunResourceMonitor
from ginkgo.runtime.caching.provenance import (
    RunProvenanceRecorder,
    load_manifest,
    make_run_id,
    tail_text,
)
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.events import EventBus, RunCompleted, RunStarted, RunValidated
from ginkgo.runtime.notifications.notifications import build_notification_service


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
        dry_run=args.dry_run,
        output_mode=output_mode,
        trust_workspace=getattr(args, "trust_workspace", False),
    )


def run_workflow(
    *,
    workflow_path: Path,
    config_paths: list[Path],
    jobs: int | None,
    cores: int | None,
    memory: int | None,
    dry_run: bool,
    output_mode: RunMode = "default",
    trust_workspace: bool = False,
) -> int:
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

    load_started = time.perf_counter()
    with _config_session(override_paths=config_paths) as session:
        module = load_module_from_path(workflow_path)
        flow = _discover_flow(module)
        expr = flow()
        params = session.merged_loaded_values()
    runtime_config = load_runtime_config(project_root=Path.cwd(), override_paths=config_paths)
    runtime_params = dict(runtime_config)
    runtime_params.update(params)
    load_elapsed = time.perf_counter() - load_started

    registry = PixiRegistry(
        project_root=Path.cwd(),
        workflow_root=workflow_path.parent,
    )
    secret_resolver = build_secret_resolver(
        project_root=Path.cwd(),
        config=runtime_params,
        environ=os.environ,
    )
    backend = CompositeBackend(
        local=LocalBackend(pixi_registry=registry),
        container=ContainerBackend(project_root=Path.cwd()),
    )
    evaluator = _ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        backend=backend,
        secret_resolver=secret_resolver,
    )
    validate_started = time.perf_counter()
    evaluator.validate(expr)
    validate_elapsed = time.perf_counter() - validate_started
    task_count = len(evaluator._nodes)
    edge_count = sum(len(node.dependency_ids) for node in evaluator._nodes.values())
    env_count = len({node.task_def.env for node in evaluator._nodes.values() if node.task_def.env})
    planned_tasks = [
        (node.node_id, node.task_def.name, _environment_label(node.task_def.env))
        for node in sorted(evaluator._nodes.values(), key=lambda item: item.node_id)
    ]

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
        else:
            rich_console.print(
                f"[green]✓[/] [bold]{workflow_path.name}[/] "
                f"[dim](dry-run)[/] [dim]- {task_count} tasks validated[/]"
            )
        return 0

    if output_mode not in {"agent", "agent_verbose"}:
        rich_console.print(
            f"[cyan]📦[/] Loading workflow...  [green]done[/] ({_format_duration(load_elapsed)})"
        )
        rich_console.print(
            f"[green]🌱[/] Building expression tree...  [bold]{task_count}[/] tasks"
        )
        if evaluator.memory is not None:
            rich_console.print(f"[cyan]🧠[/] Memory budget: [bold]{evaluator.memory}[/] GiB")
        if output_mode == "verbose":
            rich_console.print(
                f"[cyan]🧭[/] Verbose mode: jobs={evaluator.jobs}, cores={evaluator.cores}, "
                f"memory={evaluator.memory if evaluator.memory is not None else 'auto'}, "
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
        params=params,
    )
    recorder.add_run_timing(phase="workflow_load_seconds", seconds=load_elapsed)
    recorder.add_run_timing(phase="workflow_validate_seconds", seconds=validate_elapsed)
    resource_monitor = RunResourceMonitor(
        root_pid=os.getpid(),
        sink=recorder.update_resources,
    )
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
                renderer = _CliRunRenderer(
                    console=rich_console,
                    summary=_RunSummary(
                        run_id=run_id,
                        mode=output_mode,
                        run_dir=recorder.run_dir,
                        cores=evaluator.cores,
                        memory=memory,
                    ),
                    resources=_ResourceRenderState(provider=resource_monitor.current_summary),
                )
                bus.subscribe(RichEventRenderer(renderer=renderer))
            evaluator = _ConcurrentEvaluator(
                jobs=jobs,
                cores=cores,
                memory=memory,
                backend=backend,
                provenance=recorder,
                secret_resolver=secret_resolver,
                event_bus=bus,
                trust_workspace=trust_workspace,
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
                resource_summary = resource_monitor.stop()
                recorder.finalize(status="failed", error=str(exc), resources=resource_summary)
                bus.emit(
                    RunCompleted(
                        run_id=run_id,
                        status="failed",
                        task_counts=_task_counts(load_manifest(recorder.run_dir)),
                        error=str(exc),
                    )
                )
                if renderer is not None:
                    failure_details = _load_failure_details(
                        run_dir=recorder.run_dir,
                        renderer=renderer,
                        verbose=output_mode == "verbose",
                    )
                    renderer.finish(
                        elapsed=time.perf_counter() - run_started,
                        success=False,
                        resources=resource_summary,
                        failure_details=failure_details,
                    )
                    print(f"Run directory: {recorder.run_dir}", file=sys.stderr)
                raise

            resource_summary = resource_monitor.stop()
            recorder.add_run_timing(
                phase="workflow_execute_seconds",
                seconds=time.perf_counter() - run_started,
            )
            recorder.finalize(status="succeeded", resources=resource_summary)
            manifest = load_manifest(recorder.run_dir)
            bus.emit(
                RunCompleted(
                    run_id=run_id,
                    status="success",
                    task_counts=_task_counts(manifest),
                )
            )
            if renderer is not None:
                renderer.finish(
                    elapsed=time.perf_counter() - run_started,
                    success=True,
                    resources=resource_summary,
                    notebooks=_load_run_notebooks(
                        run_dir=recorder.run_dir,
                        manifest=manifest,
                        renderer=renderer,
                    ),
                    assets=_load_run_assets(manifest=manifest),
                )
    finally:
        if notification_service is not None:
            notification_service.close()
    return 0


def _load_failure_details(
    *,
    run_dir: Path,
    renderer: _CliRunRenderer,
    verbose: bool,
) -> list[_FailureDetails]:
    """Load failed-task diagnostics from a completed run manifest."""
    manifest = load_manifest(run_dir)
    failed_tasks = sorted(
        (task for task in manifest.get("tasks", {}).values() if task.get("status") == "failed"),
        key=lambda item: int(item.get("node_id", -1)),
    )
    details: list[_FailureDetails] = []
    tail_lines = 20 if verbose else 10
    for task in failed_tasks:
        node_id = int(task.get("node_id", -1))
        log_tail = _combined_log_tail(run_dir, task, lines=tail_lines)
        stderr_rel = task.get("stderr_log")
        stderr_path = run_dir / stderr_rel if isinstance(stderr_rel, str) else None
        details.append(
            _FailureDetails(
                task_label=renderer.label_for_node(node_id) or task.get("task", f"node-{node_id}"),
                exit_code=task.get("exit_code"),
                log_path=stderr_path,
                log_tail=log_tail,
                error=task.get("error"),
                inputs=task.get("inputs") if verbose else None,
            )
        )
    return details


def _combined_log_tail(run_dir: Path, task: dict[str, object], *, lines: int) -> list[str]:
    """Combine stdout and stderr tails for failure display."""
    stdout_rel = task.get("stdout_log")
    stderr_rel = task.get("stderr_log")
    legacy_rel = task.get("log")

    if stdout_rel or stderr_rel:
        combined: list[str] = []
        if isinstance(stdout_rel, str):
            combined.extend(tail_text(run_dir / stdout_rel, lines=lines))
        if isinstance(stderr_rel, str):
            combined.extend(tail_text(run_dir / stderr_rel, lines=lines))
        return combined[-lines:]

    if isinstance(legacy_rel, str):
        return tail_text(run_dir / legacy_rel, lines=lines)
    return []


def _discover_flow(module: ModuleType) -> FlowDef:
    flows = {id(value): value for value in vars(module).values() if isinstance(value, FlowDef)}
    if len(flows) != 1:
        raise RuntimeError(f"Expected exactly one @flow in {module.__file__}, found {len(flows)}")
    return next(iter(flows.values()))


def _load_run_notebooks(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
    renderer: _CliRunRenderer,
) -> list[_NotebookSummary]:
    """Extract notebook HTML artifacts from a completed run manifest.

    Reads ``rendered_html`` from each task entry. Freshly executed notebooks
    record a path relative to ``run_dir``; cache hits replay an absolute
    path pointing at the original run's HTML. ``Path /`` handles both.
    """
    notebooks: list[_NotebookSummary] = []
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return notebooks
    for task in tasks.values():
        if not isinstance(task, dict):
            continue
        rendered_html = task.get("rendered_html")
        if not isinstance(rendered_html, str):
            continue
        html_path = (run_dir / rendered_html).resolve()
        node_id = task.get("node_id")
        task_label = (
            renderer.label_for_node(int(node_id)) if isinstance(node_id, int) else None
        ) or _task_base_name(task.get("task"))
        notebooks.append(_NotebookSummary(task_label=task_label, html_path=html_path))
    return notebooks


def _load_run_assets(*, manifest: dict[str, Any]) -> list[_AssetSummary]:
    """Extract asset materialisations from a completed run manifest."""
    assets: list[_AssetSummary] = []
    seen_keys: set[str] = set()
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return assets
    for task in tasks.values():
        if not isinstance(task, dict):
            continue
        task_assets = task.get("assets")
        if not isinstance(task_assets, list):
            continue
        for asset in task_assets:
            if not isinstance(asset, dict):
                continue
            key = asset.get("asset_key")
            if not isinstance(key, str) or key in seen_keys:
                continue
            seen_keys.add(key)
            name = asset.get("name") or asset.get("asset_key") or key
            assets.append(_AssetSummary(name=str(name)))
    return assets


def _task_base_name(task_name: object) -> str:
    """Return the final dotted segment of a task identifier."""
    text = str(task_name or "unknown")
    return text.rsplit(".", maxsplit=1)[-1]


def _task_counts(manifest: dict[str, object]) -> dict[str, int]:
    """Return task counts by manifest status."""
    counts: dict[str, int] = {}
    tasks = manifest.get("tasks", {})
    if not isinstance(tasks, dict):
        return counts
    for task in tasks.values():
        if not isinstance(task, dict):
            continue
        status = str(task.get("status", "unknown"))
        counts[status] = counts.get(status, 0) + 1
    return counts
