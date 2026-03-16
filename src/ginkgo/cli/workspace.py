"""Helpers for canonical Ginkgo workspace discovery."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


_IGNORED_DIR_NAMES = {
    ".git",
    ".ginkgo",
    ".pixi",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "node_modules",
}


@dataclass(frozen=True, kw_only=True)
class WorkflowResolution:
    """Resolved workflow metadata for CLI and UI entrypoints.

    Parameters
    ----------
    path : Path
        Absolute path to the resolved workflow entrypoint.
    discovered : bool
        Whether the path was discovered implicitly from the project root.
    """

    path: Path
    discovered: bool


def resolve_workflow_path(*, project_root: Path, workflow: str | None) -> WorkflowResolution:
    """Resolve an explicit workflow path or discover the canonical default."""
    if workflow is not None:
        workflow_path = Path(workflow)
        if not workflow_path.is_absolute():
            workflow_path = project_root / workflow_path
        return WorkflowResolution(path=workflow_path.resolve(), discovered=False)

    discovered = discover_default_workflow(project_root=project_root)
    return WorkflowResolution(path=discovered, discovered=True)


def discover_default_workflow(*, project_root: Path) -> Path:
    """Return the default workflow for the current project root.

    Canonical package workflows are preferred over the legacy root-level
    ``workflow.py``. Legacy projects remain valid when no canonical package
    workflow is present.
    """
    canonical_candidates = canonical_workflow_candidates(project_root=project_root)
    if len(canonical_candidates) == 1:
        return canonical_candidates[0]
    if len(canonical_candidates) > 1:
        candidate_list = "\n".join(
            f"- {candidate.relative_to(project_root)}" for candidate in canonical_candidates
        )
        raise RuntimeError(
            "Found multiple canonical workflow entrypoints. "
            "Pass an explicit workflow path to disambiguate:\n"
            f"{candidate_list}"
        )

    legacy_workflow = project_root / "workflow.py"
    if legacy_workflow.is_file():
        return legacy_workflow.resolve()

    raise FileNotFoundError(
        "No workflow path provided and no canonical workflow was discovered. "
        "Expected either <package>/workflow.py or ./workflow.py from "
        f"{project_root}."
    )


def canonical_workflow_candidates(*, project_root: Path) -> list[Path]:
    """Return direct child package workflow entrypoints under the project root."""
    candidates: list[Path] = []
    for child in sorted(project_root.iterdir(), key=lambda path: path.name):
        if not child.is_dir() or child.name in _IGNORED_DIR_NAMES or child.name.startswith("."):
            continue
        if not (child / "__init__.py").is_file():
            continue
        workflow_path = child / "workflow.py"
        if workflow_path.is_file():
            candidates.append(workflow_path.resolve())
    return candidates


def discover_test_workflows(*, project_root: Path) -> list[Path]:
    """Return canonical or legacy workflow validation files."""
    canonical_dir = project_root / "tests" / "workflows"
    if canonical_dir.is_dir():
        return sorted(path.resolve() for path in canonical_dir.glob("*.py"))

    legacy_dir = project_root / ".tests"
    if legacy_dir.is_dir():
        return sorted(path.resolve() for path in legacy_dir.glob("*.py"))

    return []


def list_workflow_paths(*, project_root: Path) -> list[Path]:
    """Return workflow files that should be offered by local tooling."""
    discovered: dict[Path, None] = {}

    for path in canonical_workflow_candidates(project_root=project_root):
        discovered[path] = None

    legacy_workflow = project_root / "workflow.py"
    if legacy_workflow.is_file():
        discovered[legacy_workflow.resolve()] = None

    for path in sorted(project_root.rglob("*.py")):
        if any(part in _IGNORED_DIR_NAMES for part in path.parts):
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if "@flow" in content:
            discovered[path.resolve()] = None

    return sorted(discovered)
