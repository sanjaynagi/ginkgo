"""Ownership of the ``.ginkgo/`` directory layout.

Ginkgo keeps its runtime state in a ``.ginkgo/`` directory at the workspace
root, with one subdirectory per concern::

    .ginkgo/
      runs/  cache/  assets/  artifacts/  staging/  fuse/  notebooks/  reports/
      remote-staged.json

:class:`WorkspaceLayout` is the single place that convention is written down.
Every component that needs one of these paths asks the layout for it, so
renaming a directory or relocating the root is one edit rather than a hunt
across six subpackages.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

DIRECTORY_NAME = ".ginkgo"


@dataclass(frozen=True, kw_only=True)
class WorkspaceLayout:
    """The set of paths ginkgo owns inside one workspace.

    Parameters
    ----------
    root : Path
        The ``.ginkgo`` directory itself. May be relative — the CLI holds a
        relative layout so that displayed paths stay workspace-relative.
    """

    root: Path

    @classmethod
    def for_cwd(cls) -> WorkspaceLayout:
        """Return the layout under the current working directory."""
        return cls(root=Path.cwd() / DIRECTORY_NAME)

    @classmethod
    def relative(cls) -> WorkspaceLayout:
        """Return a layout whose paths are relative to the workspace root."""
        return cls(root=Path(DIRECTORY_NAME))

    @classmethod
    def for_workspace(cls, workspace: Path) -> WorkspaceLayout:
        """Return the layout for the workspace rooted at *workspace*."""
        return cls(root=workspace / DIRECTORY_NAME)

    @classmethod
    def containing(cls, path: Path) -> WorkspaceLayout:
        """Return the layout that owns *path*, one of its subdirectories.

        Used where a component holds one root and needs a sibling — an
        artifact store built from a configured cache root, say. Naming the
        assumption here keeps it in one place instead of leaving a bare
        ``.parent`` at each call site.

        Parameters
        ----------
        path : Path
            A direct child of the ``.ginkgo`` directory, such as a cache or
            assets root.
        """
        return cls(root=path.parent)

    @property
    def runs(self) -> Path:
        """Per-run provenance directories."""
        return self.root / "runs"

    @property
    def cache(self) -> Path:
        """Task cache entries."""
        return self.root / "cache"

    @property
    def assets(self) -> Path:
        """Asset catalog."""
        return self.root / "assets"

    @property
    def artifacts(self) -> Path:
        """Content-addressed artifact store."""
        return self.root / "artifacts"

    @property
    def staging(self) -> Path:
        """Downloaded remote inputs."""
        return self.root / "staging"

    @property
    def fuse(self) -> Path:
        """Mount points for streamed remote inputs."""
        return self.root / "fuse"

    @property
    def notebooks(self) -> Path:
        """Notebook artifacts for runs without a provenance directory."""
        return self.root / "notebooks"

    @property
    def reports(self) -> Path:
        """Exported HTML report bundles."""
        return self.root / "reports"

    @property
    def staging_cache_file(self) -> Path:
        """Persisted staging state for remote inputs."""
        return self.root / "remote-staged.json"
