"""Workflow diagnostics collection."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ginkgo.config import config_session
from ginkgo.core.flow import discover_flow
from ginkgo.envs.pixi import PixiEnvNotFoundError
from ginkgo.runtime.backend import ExecutionEnvironment
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.module_loader import load_module_from_path
from ginkgo.runtime.environment.secrets import SecretResolver


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
        from ginkgo.cli.workflow_params import load_param_config, validate_param_extras

        param_config = load_param_config(project_root=Path.cwd(), config_paths=config_paths)
        with config_session(
            override_paths=config_paths,
            param_config=param_config,
            cli_extras=param_extras,
            require_params=False,
        ) as session:
            module = load_module_from_path(workflow_path)
            flow = discover_flow(module)
            expr = flow()
            validate_param_extras(session)
        backend = backend_factory() if backend_factory is not None else None
        evaluator = ConcurrentEvaluator(secret_resolver=secret_resolver, backend=backend)
        evaluator.validate(expr)
        return []
    except BaseException as exc:
        return [_diagnostic_from_exception(exc=exc, workflow_path=workflow_path)]


def _diagnostic_from_exception(
    *,
    exc: BaseException,
    workflow_path: Path,
) -> WorkflowDiagnostic:
    """Convert one validation exception into a diagnostic."""
    code = exc.__class__.__name__.upper()
    message = str(exc)
    suggestion = None
    if isinstance(exc, PixiEnvNotFoundError):
        code = "MISSING_ENV"
        suggestion = "Create the environment manifest, or correct the env= name on the task."
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

    return WorkflowDiagnostic(
        severity="error",
        code=code,
        message=message,
        location=str(workflow_path),
        suggestion=suggestion,
    )
