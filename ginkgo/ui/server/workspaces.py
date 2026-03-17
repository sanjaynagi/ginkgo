"""Workspace state and discovery helpers for the UI server."""

from __future__ import annotations

import hashlib
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from ginkgo.cli.workspace import list_workflow_paths


class WorkspaceLoadCancelledError(RuntimeError):
    """Raised when the native workspace picker is cancelled."""


@dataclass(frozen=True, kw_only=True)
class WorkspaceRecord:
    """In-memory description of one loaded Ginkgo workspace.

    Parameters
    ----------
    workspace_id : str
        Stable identifier used by the UI and API.
    project_root : Path
        Absolute workspace root directory.
    runs_root : Path
        Run provenance directory for this workspace.
    cache_root : Path
        Cache directory for this workspace.
    label : str
        Human-readable label shown in the UI.
    """

    workspace_id: str
    project_root: Path
    runs_root: Path
    cache_root: Path
    label: str


class WorkspaceRegistry:
    """Track loaded workspaces and the current active workspace."""

    def __init__(self, *, initial_project_root: Path) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, WorkspaceRecord] = {}
        self._active_workspace_id: str | None = None
        initial_record = workspace_record_from_root(initial_project_root.resolve())
        self._records[initial_record.workspace_id] = initial_record
        self._active_workspace_id = initial_record.workspace_id

    def load_workspace(self, *, project_root: Path, activate: bool = True) -> WorkspaceRecord:
        """Register a workspace and optionally make it active."""
        resolved_root = validate_workspace_root(project_root)
        record = workspace_record_from_root(resolved_root)
        with self._lock:
            self._records[record.workspace_id] = record
            if activate or self._active_workspace_id is None:
                self._active_workspace_id = record.workspace_id
        return record

    def list_workspaces(self) -> list[WorkspaceRecord]:
        """Return loaded workspaces in deterministic order."""
        with self._lock:
            return sorted(
                self._records.values(),
                key=lambda record: (record.label.lower(), str(record.project_root)),
            )

    def active_workspace(self) -> WorkspaceRecord | None:
        """Return the active workspace, if one exists."""
        with self._lock:
            if self._active_workspace_id is None:
                return None
            return self._records.get(self._active_workspace_id)

    def active_workspace_id(self) -> str | None:
        """Return the active workspace identifier."""
        with self._lock:
            return self._active_workspace_id

    def workspace(self, workspace_id: str) -> WorkspaceRecord | None:
        """Return one loaded workspace by id."""
        with self._lock:
            return self._records.get(workspace_id)

    def set_active(self, *, workspace_id: str) -> WorkspaceRecord:
        """Set the active workspace."""
        with self._lock:
            record = self._records.get(workspace_id)
            if record is None:
                raise KeyError(workspace_id)
            self._active_workspace_id = workspace_id
            return record


def workspace_record_from_root(project_root: Path) -> WorkspaceRecord:
    """Build workspace metadata from a project root."""
    resolved_root = project_root.resolve()
    workspace_id = hashlib.sha1(str(resolved_root).encode("utf-8")).hexdigest()[:12]
    return WorkspaceRecord(
        workspace_id=workspace_id,
        project_root=resolved_root,
        runs_root=resolved_root / ".ginkgo" / "runs",
        cache_root=resolved_root / ".ginkgo" / "cache",
        label=resolved_root.name or str(resolved_root),
    )


def validate_workspace_root(project_root: Path) -> Path:
    """Validate that a directory looks like a Ginkgo workspace."""
    resolved_root = project_root.expanduser().resolve()
    if not resolved_root.is_dir():
        raise FileNotFoundError(f"Workspace not found: {project_root}")

    has_config = (resolved_root / "ginkgo.toml").is_file()
    has_runtime_state = (resolved_root / ".ginkgo").is_dir()
    has_workflows = bool(list_workflow_paths(project_root=resolved_root))
    if has_config or has_runtime_state or has_workflows:
        return resolved_root

    raise RuntimeError(f"Directory does not look like a Ginkgo workspace: {resolved_root}")


def pick_workspace_folder() -> Path:
    """Open a native folder picker and return the selected directory."""
    if sys.platform == "darwin":
        return _pick_workspace_folder_macos()
    return _pick_workspace_folder_tk()


def _pick_workspace_folder_macos() -> Path:
    """Pick a workspace folder using macOS native dialogs."""
    script = 'POSIX path of (choose folder with prompt "Select a Ginkgo workspace")'
    result = subprocess.run(  # noqa: S603 - trusted local OS dialog
        ["osascript", "-e", script],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise WorkspaceLoadCancelledError("Workspace selection was cancelled.")
    path_text = result.stdout.strip()
    if not path_text:
        raise WorkspaceLoadCancelledError("Workspace selection was cancelled.")
    return Path(path_text)


def _pick_workspace_folder_tk() -> Path:
    """Pick a workspace folder using Tk as a cross-platform fallback."""
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:  # pragma: no cover - platform dependent
        raise RuntimeError("Native folder picker is unavailable on this platform.") from exc

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.askdirectory(title="Select a Ginkgo workspace")
    finally:
        root.destroy()
    if not selected:
        raise WorkspaceLoadCancelledError("Workspace selection was cancelled.")
    return Path(selected)


def resolve_launch_workspace(
    *, registry: WorkspaceRegistry, active_workspace: WorkspaceRecord, workflow: str
) -> tuple[WorkspaceRecord, str]:
    """Resolve which workspace should own a launched workflow."""
    workflow_path = Path(workflow)
    if not workflow_path.is_absolute():
        return active_workspace, workflow

    resolved_workflow = workflow_path.expanduser().resolve()
    if not resolved_workflow.is_file():
        raise FileNotFoundError(f"Workflow not found: {workflow}")

    inferred_root = infer_workflow_project_root(resolved_workflow)
    launch_workspace = registry.load_workspace(project_root=inferred_root, activate=True)
    try:
        workflow_label = str(resolved_workflow.relative_to(launch_workspace.project_root))
    except ValueError:
        workflow_label = str(resolved_workflow)
    return launch_workspace, workflow_label


def infer_workflow_project_root(workflow_path: Path) -> Path:
    """Infer a workflow's owning workspace root from its path."""
    for candidate in (workflow_path.parent, *workflow_path.parents):
        if (candidate / "ginkgo.toml").is_file():
            return candidate
    for candidate in (workflow_path.parent, *workflow_path.parents):
        if (candidate / "pixi.toml").is_file():
            return candidate
    return workflow_path.parent
