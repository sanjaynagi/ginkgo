"""Workflow diagnostics collection."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ginkgo.config import config_session
from ginkgo.core.expr import ConstructedCall, record_constructed_calls
from ginkgo.core.flow import discover_flow
from ginkgo.core.task import TaskDef
from ginkgo.core.types import is_str_path_annotation, looks_path_like_param_name
from ginkgo.envs.container import ContainerImageNotFoundError
from ginkgo.envs.pixi import PixiEnvNotFoundError
from ginkgo.errors import failure_location
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.secrets import SecretResolver

UNREACHABLE_CALL_CODE = "unreachable_task_call"
PATH_LIKE_STR_PARAM_CODE = "path_like_str_param"


@dataclass(frozen=True, kw_only=True)
class WorkflowDiagnostic:
    """Structured diagnostic entry."""

    severity: str
    code: str
    message: str
    location: str | None = None
    suggestion: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping."""
        return asdict(self)


def collect_workflow_diagnostics(
    *,
    workflow_path: Path,
    config_paths: list[Path],
    secret_resolver: SecretResolver | None,
    backend_factory: Callable[[], ExecutionEnvironment] | None = None,
    param_extras: Sequence[str] = (),
) -> list[WorkflowDiagnostic]:
    """Collect structured workflow diagnostics.

    Parameters
    ----------
    workflow_path : Path
        Path to the workflow module to validate.
    config_paths : list[Path]
        Config override paths to activate while the workflow is constructed.
    secret_resolver : SecretResolver | None
        Resolver used to check that referenced secrets are available.
    backend_factory : Callable[[], ExecutionEnvironment] | None, optional
        Builds the execution environment used to validate declared task
        ``env`` values. Called inside the diagnostic try/except so that a
        construction failure (bad project layout, unreadable envs/) is
        reported as a diagnostic rather than raised. When ``None``, env
        resolution is not checked.
    param_extras : Sequence[str], optional
        Command-line tokens supplying declared workflow parameters. Required
        parameters need not be supplied: a workflow is still worth diagnosing
        without its inputs.

    Returns
    -------
    list[WorkflowDiagnostic]
        One diagnostic per validation failure; empty when validation passes.
    """
    try:
        from ginkgo.cli.workflow_params import (
            global_param_reads,
            load_param_config,
            validate_param_extras,
        )

        param_config = load_param_config(project_root=Path.cwd(), config_paths=config_paths)
        with config_session(
            override_paths=config_paths,
            param_config=param_config,
            cli_extras=param_extras,
            require_params=False,
        ) as session:
            module = load_module_from_path(workflow_path)
            flow = discover_flow(module)
            with record_constructed_calls() as constructed_calls:
                expr = flow()
            validate_param_extras(session)
        backend = backend_factory() if backend_factory is not None else None
        evaluator = ConcurrentEvaluator(
            secret_resolver=secret_resolver,
            backend=backend,
            constructed_calls=tuple(constructed_calls),
            executor_registry=ExecutorRegistry.for_validation(
                project_root=Path.cwd(),
                config_paths=config_paths,
            ),
        )
        evaluator.build_and_validate(expr)

        # A parameter read from a module global inside a task body is invisible
        # to that task's cache key, so a changed value silently reuses the
        # previous result. Reported as a warning because detection cannot see a
        # read made by a helper the task calls.
        diagnostics = [
            WorkflowDiagnostic(
                severity="warning",
                code="param_read_from_global",
                message=finding.message(),
                location=finding.task_name,
                suggestion=(f"Pass {finding.param_name} into {finding.task_name} as an argument."),
            )
            for finding in global_param_reads(
                declaration_globals=session.declaration_globals,
                evaluator=evaluator,
            )
        ]
        diagnostics.extend(unreachable_call_diagnostics(calls=evaluator.unreachable_calls))
        diagnostics.extend(
            path_like_str_param_diagnostics(
                task_defs=(node.task_def for node in evaluator.task_nodes.values())
            )
        )
        return diagnostics
    except Exception as exc:
        # KeyboardInterrupt and SystemExit are left to propagate: a user who
        # interrupts doctor wants it to stop, not to be told about a diagnostic.
        return [_diagnostic_from_exception(exc=exc, workflow_path=workflow_path)]


def unreachable_call_diagnostics(*, calls: Sequence[ConstructedCall]) -> list[WorkflowDiagnostic]:
    """Build one warning per task call the graph never reached.

    Parameters
    ----------
    calls : Sequence[ConstructedCall]
        Constructed-but-unregistered calls, from
        ``ConcurrentEvaluator.unreachable_calls``.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per dropped call.
    """
    return [
        WorkflowDiagnostic(
            severity="warning",
            code=UNREACHABLE_CALL_CODE,
            message=(
                f"{call.label} is not reachable from the flow return value, so it was "
                "dropped from the graph and will not run."
            ),
            location=call.task_name,
            suggestion=(
                "Return its result from the flow (directly or inside a tuple, list, or dict) "
                "if the task should run."
            ),
        )
        for call in calls
    ]


def path_like_str_param_diagnostics(*, task_defs: Iterable[TaskDef]) -> list[WorkflowDiagnostic]:
    """Build one warning per path-shaped ``str`` parameter across the graph.

    A parameter named like a path (``path``, ``output_dir``, ``report_files``,
    ...) but annotated a bare ``str`` shape (``str``, ``str | None``,
    ``list[str]``, ``tuple[str, ...]``) is tracked by the cache only as a
    string: editing the file it names does not invalidate the task, and
    referencing it from another task's output creates no dependency edge —
    this is issue #307. Purely static and name-based, so it flags every task
    definition in the graph once, regardless of how many calls or branches
    reach it.

    Parameters
    ----------
    task_defs : Iterable[TaskDef]
        Task definitions to check, typically one per node in the built graph
        (``evaluator.task_nodes.values()``'s ``task_def``s). Deduplicated
        internally by :attr:`TaskDef.name`, since the same task definition can
        back many call sites.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per matching parameter, ordered by
        first encounter.
    """
    diagnostics: list[WorkflowDiagnostic] = []
    seen_task_names: set[str] = set()
    for task_def in task_defs:
        if task_def.name in seen_task_names:
            continue
        seen_task_names.add(task_def.name)
        for param_name, annotation in task_def.type_hints.items():
            if param_name == "return":
                continue
            if not looks_path_like_param_name(param_name):
                continue
            if not is_str_path_annotation(annotation):
                continue
            diagnostics.append(
                WorkflowDiagnostic(
                    severity="warning",
                    code=PATH_LIKE_STR_PARAM_CODE,
                    message=(
                        f"{task_def.name}'s `{param_name}` parameter is annotated `str`, so it "
                        "is cache-tracked by its path string alone: editing the file it names "
                        "won't re-run the task, and no dependency edge is created when it comes "
                        "from another task's output. Annotate it `file`/`folder` if the task "
                        "reads that path, or `Out[file]`/`Out[folder]` if it writes to it."
                    ),
                    location=task_def.name,
                    suggestion=(
                        f"Annotate `{param_name}` `file`/`folder` if {task_def.name} reads it, "
                        "or `Out[file]`/`Out[folder]` if it writes it."
                    ),
                )
            )
    return diagnostics


def _diagnostic_from_exception(
    *,
    exc: BaseException,
    workflow_path: Path,
) -> WorkflowDiagnostic:
    """Convert one validation exception into a diagnostic.

    The location is the failing line in the user's own code when the traceback
    reaches it, and the workflow path otherwise — a diagnostic that only names
    the file the user already passed in tells them nothing.
    """
    code = exc.__class__.__name__.upper()
    message = str(exc)
    suggestion = None
    if isinstance(exc, PixiEnvNotFoundError):
        code = "MISSING_ENV"
        suggestion = "Create the environment manifest, or correct the env= name on the task."
    elif isinstance(exc, ContainerImageNotFoundError):
        code = "MISSING_IMAGE"
        suggestion = "Pull the image, correct its name/tag, or adjust [container] pull_policy."
    elif isinstance(exc, RuntimeError) and "Missing secrets:" in message:
        code = "MISSING_SECRET"
        suggestion = "Provide the referenced secret through the configured resolver."
    elif isinstance(exc, TypeError) and "top-level function" in message:
        code = "NON_IMPORTABLE_TASK"
        suggestion = "Define tasks at module scope as plain importable functions."
    elif isinstance(exc, TypeError) and "kind='python'" in message:
        code = "INVALID_ENV_KIND"
        suggestion = "Use shell, notebook, or script task kinds for foreign environments."
    elif isinstance(exc, RuntimeError) and "Expected exactly one @flow" in message:
        code = "FLOW_DISCOVERY_ERROR"
        suggestion = "Keep one unambiguous @flow entrypoint per workflow module."
    elif isinstance(exc, ValueError):
        code = "INVALID_VALUE"

    located = failure_location(exc)
    return WorkflowDiagnostic(
        severity="error",
        code=code,
        message=message,
        location=str(located) if located is not None else str(workflow_path),
        suggestion=suggestion,
    )
