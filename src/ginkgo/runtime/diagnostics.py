"""Workflow diagnostics collection."""

from __future__ import annotations

import ast
import re
from collections.abc import Callable, Collection, Iterable, Sequence
from dataclasses import asdict, dataclass, replace
import inspect
import os
from pathlib import Path
import textwrap
from typing import Any

from ginkgo.config import config_session
from ginkgo.core.directive import DirectiveError
from ginkgo.core.expr import ConstructedCall, record_constructed_calls
from ginkgo.core.flow import discover_flow
from ginkgo.core.task import TaskDef
from ginkgo.core.types import is_str_path_annotation, looks_path_like_param_name
from ginkgo.errors import failure_location
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.graph import TaskGraph, TaskNode
from ginkgo.runtime.placement import Placement
from ginkgo.runtime.task_kinds import task_kind
from ginkgo.runtime.static_directive import static_directive
from ginkgo.runtime.task_validation import TaskValidator, contains_dynamic_expression
from ginkgo.runtime.executor_registry import ExecutorRegistry
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.secrets import SecretResolver

UNREACHABLE_CALL_CODE = "unreachable_task_call"
PATH_LIKE_STR_PARAM_CODE = "path_like_str_param"
MISSING_DIRECTIVE_CODE = "missing_directive_return"
ENVIRON_READ_CODE = "environ_read_in_task"
STATIC_NOTICE_CODE = "task_kind_notice"
_ENVIRON_ATTRIBUTES = frozenset({"environ", "environb", "getenv"})
ENV_WARNING_CONSEQUENCE = (
    "A run fails at the first task in this environment that has to run; "
    "cached tasks do not need it."
)


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
    command_flags: Collection[str] = (),
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
    command_flags : Collection[str], optional
        The command's own flags, offered as suggestions for an unrecognised
        one.

    Returns
    -------
    list[WorkflowDiagnostic]
        One diagnostic per validation failure or warning; empty when validation
        passes cleanly. A failure is the last entry, after any dropped-call
        warnings that may explain it.
    """
    dropped_calls: list[WorkflowDiagnostic] = []
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
            validate_param_extras(session, command_flags=command_flags)
        backend = backend_factory() if backend_factory is not None else None
        graph = TaskGraph.build(expr)
        # Collected before validation and kept if it fails: a consumer of a
        # dropped producer's Out[...] path fails it, and the warning names why.
        dropped_calls = unreachable_call_diagnostics(
            calls=graph.unreachable_calls(constructed_calls)
        )
        validator = TaskValidator(backend=backend, secret_resolver=secret_resolver)
        graph.validate(
            validator=validator,
            placement=Placement(
                executor_registry=ExecutorRegistry.for_validation(
                    project_root=Path.cwd(),
                    config_paths=config_paths,
                )
            ),
        )

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
                nodes=graph.nodes.values(),
            )
        ]
        diagnostics.extend(
            env_warning_diagnostic(warning=warning, workflow_path=workflow_path)
            for warning in validator.env_warnings
        )
        diagnostics.extend(
            environ_read_diagnostics(task_defs=[node.task_def for node in graph.nodes.values()])
        )
        diagnostics.extend(static_notice_diagnostics(nodes=graph.nodes.values()))
        diagnostics.extend(dropped_calls)
        diagnostics.extend(path_like_str_param_diagnostics(nodes=graph.nodes.values()))
        diagnostics.extend(
            missing_directive_diagnostics(
                task_defs=[node.task_def for node in graph.nodes.values()]
            )
        )
        return diagnostics
    except Exception as exc:
        # KeyboardInterrupt and SystemExit are left to propagate: a user who
        # interrupts doctor wants it to stop, not to be told about a diagnostic.
        return [*dropped_calls, _diagnostic_from_exception(exc=exc, workflow_path=workflow_path)]


def env_warning_diagnostic(*, warning: Exception, workflow_path: Path) -> WorkflowDiagnostic:
    """Build the warning for an env problem that fails a task only if it runs.

    A stopped container daemon fails every container task that has to run, but
    a run whose container tasks are all cached never needs it, so doctor and a
    dry run warn rather than fail.
    """
    diagnostic = _diagnostic_from_exception(exc=warning, workflow_path=workflow_path)
    return replace(
        diagnostic,
        severity="warning",
        message=f"{diagnostic.message} {ENV_WARNING_CONSEQUENCE}",
    )


def unreachable_call_diagnostics(*, calls: Sequence[ConstructedCall]) -> list[WorkflowDiagnostic]:
    """Build one warning per task call the graph never reached.

    Parameters
    ----------
    calls : Sequence[ConstructedCall]
        Constructed-but-unregistered calls, from
        ``TaskGraph.unreachable_calls``.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per dropped call.
    """
    return [_unreachable_call_diagnostic(call=call) for call in calls]


def _unreachable_call_diagnostic(*, call: ConstructedCall) -> WorkflowDiagnostic:
    """Build the warning for one dropped call, naming what a partial call lacks."""
    if call.is_partial:
        return WorkflowDiagnostic(
            severity="warning",
            code=UNREACHABLE_CALL_CODE,
            message=(
                f"{call.label} is missing required argument(s): "
                f"{', '.join(call.missing_params)}, so it was dropped from the graph and "
                "will not run."
            ),
            location=call.task_name,
            suggestion=(
                "Pass every required argument, or complete the call with .map() or .product_map()."
            ),
        )
    return WorkflowDiagnostic(
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


def path_like_str_param_diagnostics(*, nodes: Iterable[TaskNode]) -> list[WorkflowDiagnostic]:
    """Build one warning per path-shaped ``str`` parameter across the graph.

    A parameter named like a path (``path``, ``output_dir``, ``report_files``,
    ...) but annotated a bare ``str`` shape (``str``, ``str | None``,
    ``list[str]``, ``tuple[str, ...]``) is only content-tracked when its
    value names an existing regular file and reads as a path, with a
    separator or a file extension (#307 phase 2,
    :func:`~ginkgo.core.types.looks_like_path_string`). A directory, a bare
    word, or any other non-file path is tracked by its string alone, ``file`` and
    ``folder`` additionally validate that the path exists, and a path the
    task writes belongs in ``Out[...]``.

    A parameter every call fills from an upstream task's result is left
    alone: the producer's return annotation decides how that value is
    tracked, and the run checks it when it arrives, so the warning's advice
    would be wrong there. Otherwise the check is name-based and flags each
    task definition once, however many calls or branches reach it.

    Parameters
    ----------
    nodes : Iterable[TaskNode]
        The built graph's nodes. Grouped internally by :attr:`TaskDef.name`,
        since the same task definition can back many call sites.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per matching parameter, ordered by
        first encounter.
    """
    calls: dict[str, list[TaskNode]] = {}
    for node in nodes:
        calls.setdefault(node.task_def.name, []).append(node)

    diagnostics: list[WorkflowDiagnostic] = []
    for task_nodes in calls.values():
        task_def = task_nodes[0].task_def
        task_label = task_def.base_name
        for param_name, annotation in task_def.type_hints.items():
            if param_name == "return":
                continue
            if not looks_path_like_param_name(param_name):
                continue
            if not is_str_path_annotation(annotation):
                continue
            if all(
                contains_dynamic_expression(node.expr.args.get(param_name)) for node in task_nodes
            ):
                continue
            diagnostics.append(
                WorkflowDiagnostic(
                    severity="warning",
                    code=PATH_LIKE_STR_PARAM_CODE,
                    message=(
                        f"{task_label}'s `{param_name}` parameter is annotated `str`, so it "
                        "is only content-tracked when its value names an existing regular "
                        "file and reads as a path (a separator or a file extension): a "
                        "directory, a bare word, or any other non-file path is tracked by its "
                        "string alone, and nothing checks that it exists. Annotate it "
                        "`file`/`folder` if the task reads that path, or `Out[file]`/"
                        "`Out[folder]` if the task writes it."
                    ),
                    location=task_def.name,
                    suggestion=(
                        f"Annotate `{param_name}` `file`/`folder` if {task_label} reads it, "
                        "or `Out[file]`/`Out[folder]` if it writes it."
                    ),
                )
            )
    return diagnostics


def missing_directive_diagnostics(*, task_defs: Iterable[TaskDef]) -> list[WorkflowDiagnostic]:
    """Build one warning per driver task whose body can never return its directive.

    A ``shell`` task must return ``shell(...)``; one whose body only returns a
    string fails when it runs, after its upstream tasks have already run.
    Detection is best-effort and errs towards silence: a task is flagged only
    when every ``return`` in its body yields a literal, an f-string, or
    arithmetic over names and literals, none of which can be a directive. A
    ``return`` of a call or a bare name might be one, so it is never flagged.

    Parameters
    ----------
    task_defs : Iterable[TaskDef]
        Task definitions to check, typically one per node in the built graph.
        Deduplicated internally by :attr:`TaskDef.name`.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per flagged task, ordered by first
        encounter.
    """
    diagnostics: list[WorkflowDiagnostic] = []
    seen_task_names: set[str] = set()
    for task_def in task_defs:
        if task_def.name in seen_task_names:
            continue
        seen_task_names.add(task_def.name)
        kind = task_kind(task_def)
        if kind.directive is None or not _never_returns_directive(task_def.fn):
            continue
        task_label = task_def.base_name
        diagnostics.append(
            WorkflowDiagnostic(
                severity="warning",
                code=MISSING_DIRECTIVE_CODE,
                message=(
                    f'{task_label} is declared @task("{kind.name}") but never returns '
                    f"{kind.call_form}, so it will fail when it runs."
                ),
                location=task_def.name,
                suggestion=(
                    f"Return {kind.call_form} from {task_label}, or declare it @task() if it "
                    "computes its result in Python."
                ),
            )
        )
    return diagnostics


def environ_read_diagnostics(*, task_defs: Iterable[TaskDef]) -> list[WorkflowDiagnostic]:
    """Build one warning per task whose body reads the process environment.

    The cache key covers a task's arguments and source, not ``os.environ``, so
    a task that reads ``MY_SETTING`` keeps serving the result computed with
    its old value. Detection is best-effort: it reads the task body's own
    source for ``os.environ``, ``os.getenv`` and ``os.environb``, and for
    module globals bound to them by ``from os import ...``, but not a read
    made by a helper the task calls.

    Parameters
    ----------
    task_defs : Iterable[TaskDef]
        Task definitions to check. Deduplicated internally by
        :attr:`TaskDef.name`.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per flagged task, ordered by first
        encounter.
    """
    diagnostics: list[WorkflowDiagnostic] = []
    seen_task_names: set[str] = set()
    for task_def in task_defs:
        if task_def.name in seen_task_names:
            continue
        seen_task_names.add(task_def.name)
        if not _reads_environ(task_def.fn):
            continue
        task_label = task_def.base_name
        diagnostics.append(
            WorkflowDiagnostic(
                severity="warning",
                code=ENVIRON_READ_CODE,
                message=(
                    f"Task {task_label!r} reads the process environment (os.environ or "
                    "os.getenv). Cache keys hash task arguments, so changing the variable "
                    "will not re-run this task."
                ),
                location=task_def.name,
                suggestion=(
                    f"Pass the value into {task_label} as an argument, or declare it with "
                    "ginkgo.param and pass that."
                ),
            )
        )
    return diagnostics


def static_notice_diagnostics(*, nodes: Iterable[TaskNode]) -> list[WorkflowDiagnostic]:
    """Build one warning per notice a task kind raises about a task's directive.

    A kind with a ``static_notices`` hook (a notebook's ``mo.cli_args()`` keys)
    is asked about each task whose directive can be worked out from its
    literal arguments without running the body; a task whose directive
    depends on an upstream result is skipped. A directive the constructor
    refuses is left to the dry run's own report.

    Parameters
    ----------
    nodes : Iterable[TaskNode]
        The graph's task nodes.

    Returns
    -------
    list[WorkflowDiagnostic]
        One ``warning``-severity diagnostic per distinct notice, so a fan-out
        of one task reports it once.
    """
    diagnostics: list[WorkflowDiagnostic] = []
    seen: set[str] = set()
    for node in nodes:
        kind = task_kind(node.task_def)
        if kind.static_notices is None or kind.directive is None:
            continue
        literal_args = {
            name: value
            for name, value in node.expr.args.items()
            if not contains_dynamic_expression(value)
        }
        try:
            directive = static_directive(
                fn=node.task_def.fn, resolved_args=literal_args, directive_type=kind.directive
            )
        except DirectiveError:
            continue
        if directive is None:
            continue
        for message in kind.static_notices(
            directive, node.task_def.base_name, list(node.expr.args)
        ):
            if message in seen:
                continue
            seen.add(message)
            diagnostics.append(
                WorkflowDiagnostic(
                    severity="warning",
                    code=STATIC_NOTICE_CODE,
                    message=message,
                    location=node.task_def.name,
                )
            )
    return diagnostics


def _reads_environ(fn: Callable[..., Any]) -> bool:
    """Whether *fn*'s source reads ``os.environ``, ``os.getenv`` or ``os.environb``.

    ``False`` whenever the source cannot be read.
    """
    try:
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    except (OSError, TypeError, SyntaxError):
        return False
    function_globals = getattr(fn, "__globals__", {})
    # environb is POSIX-only.
    readers = {id(os.environ), id(getattr(os, "environb", os.environ)), id(os.getenv)}
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr in _ENVIRON_ATTRIBUTES
            and isinstance(node.value, ast.Name)
            and node.value.id == "os"
        ):
            return True
        if isinstance(node, ast.Name) and id(function_globals.get(node.id)) in readers:
            return True
    return False


def _never_returns_directive(fn: Callable[..., Any]) -> bool:
    """Whether every ``return`` in *fn*'s own body is plainly not a directive.

    ``False`` whenever the source cannot be read or the body is a generator,
    so an unreadable task is never flagged.
    """
    try:
        source = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError):
        return False
    function = next(
        (
            node
            for node in ast.parse(source).body
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        ),
        None,
    )
    if function is None:
        return False

    returns: list[ast.Return] = []
    pending: list[ast.AST] = list(function.body)
    while pending:
        node = pending.pop()
        # A nested function, class or lambda returns for itself, not the task.
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | ast.Lambda):
            continue
        if isinstance(node, ast.Yield | ast.YieldFrom):
            return False
        if isinstance(node, ast.Return):
            returns.append(node)
        pending.extend(ast.iter_child_nodes(node))
    # No return at all returns None, which is no directive either.
    return all(node.value is None or _is_plain_value(node.value) for node in returns)


def _is_plain_value(node: ast.expr) -> bool:
    """Whether *node* is a literal, an f-string, or arithmetic over names and literals."""
    if isinstance(node, ast.Constant | ast.JoinedStr):
        return True
    if isinstance(node, ast.BinOp):
        return all(
            isinstance(operand, ast.Name) or _is_plain_value(operand)
            for operand in (node.left, node.right)
        )
    return False


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
    message = str(exc)
    code, suggestion = _diagnostic_code_and_suggestion(exc=exc, message=message)

    located = failure_location(exc)
    return WorkflowDiagnostic(
        severity="error",
        code=code,
        message=message,
        location=str(located) if located is not None else str(workflow_path),
        suggestion=suggestion,
    )


def _diagnostic_code_and_suggestion(*, exc: BaseException, message: str) -> tuple[str, str | None]:
    """Return the diagnostic code and suggested fix for one validation error.

    An error that knows its diagnostic declares it through ``diagnostic_code``
    and ``diagnostic_suggestion`` class attributes, so this module never imports
    the environment backends that raise them. The rest are recognised here.
    """
    declared_code = getattr(exc, "diagnostic_code", None)
    if isinstance(declared_code, str):
        return declared_code, getattr(exc, "diagnostic_suggestion", None)
    if isinstance(exc, RuntimeError) and "Missing secrets:" in message:
        return "missing_secret", "Provide the referenced secret through the configured resolver."
    if isinstance(exc, TypeError) and "top-level function" in message:
        return "non_importable_task", "Define tasks at module scope as plain importable functions."
    if isinstance(exc, RuntimeError) and "Expected exactly one @flow" in message:
        return "flow_discovery_error", "Keep one unambiguous @flow entrypoint per workflow module."
    if isinstance(exc, ValueError):
        return "invalid_value", None
    return _snake_case(type(exc).__name__), None


def _snake_case(name: str) -> str:
    """Return a CamelCase exception name as a snake_case diagnostic code."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()
