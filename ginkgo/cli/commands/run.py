"""Run command handlers."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import ModuleType

from ginkgo.cli.common import RUNS_ROOT, RunMode, console
from ginkgo.cli.renderers.common import _environment_label, _format_duration
from ginkgo.cli.renderers.models import _FailureDetails, _ResourceRenderState, _RunSummary
from ginkgo.cli.renderers.run import _CliRunRenderer
from ginkgo.cli.workspace import resolve_workflow_path
from ginkgo.config import _config_session
from ginkgo.core.flow import FlowDef
from ginkgo.envs.container import ContainerBackend
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.runtime.backend import CompositeBackend, LocalBackend
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.resources import RunResourceMonitor
from ginkgo.runtime.provenance import RunProvenanceRecorder, load_manifest, make_run_id, tail_text


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
) -> int:
    run_id = make_run_id(workflow_path=workflow_path)
    rich_console = console(sys.stdout)
    if dry_run:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_path.name}[/] [bold]--dry-run[/]\n"
        )
    else:
        rich_console.print(
            f"[bold green]🌿 ginkgo run[/] [bold]{workflow_path.name}[/] [dim]({run_id})[/]\n"
        )

    load_started = time.perf_counter()
    with _config_session(override_paths=config_paths) as session:
        module = load_module_from_path(workflow_path)
        flow = _discover_flow(module)
        expr = flow()
        params = session.merged_loaded_values()
    load_elapsed = time.perf_counter() - load_started

    registry = PixiRegistry(
        project_root=Path.cwd(),
        workflow_root=workflow_path.parent,
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
    )
    evaluator.validate(expr)
    task_count = len(evaluator._nodes)
    planned_tasks = [
        (node.node_id, node.task_def.name, _environment_label(node.task_def.env))
        for node in sorted(evaluator._nodes.values(), key=lambda item: item.node_id)
    ]

    if dry_run:
        rich_console.print(
            f"[green]✓[/] [bold]{workflow_path.name}[/] "
            f"[dim](dry-run)[/] [dim]- {task_count} tasks validated[/]"
        )
        return 0

    rich_console.print(
        f"[cyan]📦[/] Loading workflow...  [green]done[/] ({_format_duration(load_elapsed)})"
    )
    rich_console.print(f"[green]🌱[/] Building expression tree...  [bold]{task_count}[/] tasks")
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
    resource_monitor = RunResourceMonitor(
        root_pid=os.getpid(),
        sink=recorder.update_resources,
    )
    resource_monitor.start()
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
    evaluator = _ConcurrentEvaluator(
        jobs=jobs,
        cores=cores,
        memory=memory,
        backend=backend,
        provenance=recorder,
        _stderr=renderer,
    )
    renderer.start(planned_tasks=planned_tasks)
    run_started = time.perf_counter()
    try:
        evaluator.evaluate(expr)
    except BaseException as exc:
        resource_summary = resource_monitor.stop()
        recorder.finalize(status="failed", error=str(exc), resources=resource_summary)
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
    recorder.finalize(status="succeeded", resources=resource_summary)
    renderer.finish(
        elapsed=time.perf_counter() - run_started,
        success=True,
        resources=resource_summary,
    )
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
                error=task.get("error") if verbose else None,
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
