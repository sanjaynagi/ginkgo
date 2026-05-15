"""Workflow diagnostics collection."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

from ginkgo.config import _config_session
from ginkgo.core.flow import FlowDef
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
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
) -> list[WorkflowDiagnostic]:
    """Collect structured workflow diagnostics."""
    try:
        with _config_session(override_paths=config_paths):
            module = load_module_from_path(workflow_path)
            flow = _discover_flow(module)
            expr = flow()
        evaluator = _ConcurrentEvaluator(secret_resolver=secret_resolver)
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
    if isinstance(exc, RuntimeError) and "Missing secrets:" in message:
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


def _discover_flow(module: ModuleType) -> FlowDef:
    flows = {id(value): value for value in vars(module).values() if isinstance(value, FlowDef)}
    if len(flows) != 1:
        raise RuntimeError(f"Expected exactly one @flow in {module.__file__}, found {len(flows)}")
    return next(iter(flows.values()))
