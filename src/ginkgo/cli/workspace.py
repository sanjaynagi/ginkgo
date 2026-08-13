"""Helpers for canonical Ginkgo workspace discovery."""

from __future__ import annotations


from dataclasses import dataclass
from pathlib import Path

from ginkgo.workspace_layout import DIRECTORY_NAME


#: The entry-file name autodiscovery looks for, at the project root or one
#: directory below it. An explicit path accepts any filename.
_ENTRY_NAME = "flow.py"

_IGNORED_DIR_NAMES = {
    ".git",
    DIRECTORY_NAME,
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
    """Return the workflow to run when no explicit path was given."""
    candidates = canonical_workflow_candidates(project_root=project_root)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        candidate_list = "\n".join(
            f"- {candidate.relative_to(project_root)}" for candidate in candidates
        )
        raise RuntimeError(
            "Found multiple workflow entrypoints. "
            "Pass an explicit workflow path to disambiguate:\n"
            f"{candidate_list}"
        )

    raise FileNotFoundError(
        f"No workflow path provided and no {_ENTRY_NAME} was found in "
        f"{project_root} or its immediate subdirectories. Create "
        f"workflow/{_ENTRY_NAME}, or pass an explicit path: "
        "ginkgo run <path/to/entry.py>."
    )


def canonical_workflow_candidates(*, project_root: Path) -> list[Path]:
    """Return every discoverable workflow entrypoint under the project root.

    An entry file is one named ``flow.py``, sitting either at the project root
    or in one of its immediate subdirectories. The directory name is not
    checked, and ``__init__.py`` is not required — the loader needs it only for
    relative imports, so demanding it here would hide an entry file that runs
    perfectly well.
    """
    candidates: list[Path] = []
    root_entry = project_root / _ENTRY_NAME
    if root_entry.is_file():
        candidates.append(root_entry.resolve())

    for child in sorted(project_root.iterdir(), key=lambda path: path.name):
        if not child.is_dir() or child.name in _IGNORED_DIR_NAMES or child.name.startswith("."):
            continue
        entry_path = child / _ENTRY_NAME
        if entry_path.is_file():
            candidates.append(entry_path.resolve())
    return candidates


def resolve_envs_workflow_root(*, project_root: Path) -> Path | None:
    """Resolve the directory Pixi environment discovery should anchor on.

    Environments live beside the discovered entry file, under
    ``<workflow_root>/envs``. This is independent of which workflow file is
    actually being executed, so a test workflow under ``tests/workflows/`` and
    an ad-hoc entry file elsewhere both resolve the same envs root as the
    project's own ``workflow/flow.py``.

    Parameters
    ----------
    project_root : Path
        Root of the project being run.

    Returns
    -------
    Path | None
        The discovered workflow's parent directory, or ``None`` when no
        workflow can be discovered.
    """
    try:
        return discover_default_workflow(project_root=project_root).parent
    except (FileNotFoundError, RuntimeError):
        return None


def discover_test_workflows(*, project_root: Path) -> list[Path]:
    """Return the workflow validation files ``ginkgo test`` runs."""
    canonical_dir = project_root / "tests" / "workflows"
    if canonical_dir.is_dir():
        return sorted(path.resolve() for path in canonical_dir.glob("*.py"))

    legacy_dir = project_root / ".tests"
    if legacy_dir.is_dir():
        return sorted(path.resolve() for path in legacy_dir.glob("*.py"))

    return []
