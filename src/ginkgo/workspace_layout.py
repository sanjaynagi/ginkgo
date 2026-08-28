"""Ownership of the ``.ginkgo/`` directory layout.

Ginkgo keeps its runtime state in a ``.ginkgo/`` directory at the workspace
root, with one subdirectory per concern::

    .ginkgo/
      ginkgo.db
      runs/  cache/  assets/  artifacts/  staging/  fuse/  notebooks/  reports/
      remote-staged.json

:class:`WorkspaceLayout` is the single place that convention is written down.
Every component that needs one of these paths asks the layout for it, so
renaming a directory or relocating the root is one edit rather than a hunt
across six subpackages.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DIRECTORY_NAME = ".ginkgo"
GINKGO_DB_ENV_VAR = "GINKGO_DB"
"""Environment variable relocating the provenance ledger away from ``.ginkgo/``."""


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
        """Return the layout under the current working directory.

        The working directory is the project root: the CLI resolves the root
        and changes directory to it before dispatching any command, so a run
        launched from ``workflow/`` still finds the root's ``.ginkgo/``. See
        ``cli/app.py``'s ``_normalize_working_directory``. Code driving ginkgo
        as a library rather than through the CLI does not get that guarantee,
        and should pass an explicit ``root=`` if it is not already at the root.
        """
        return cls(root=Path.cwd() / DIRECTORY_NAME)

    @classmethod
    def relative(cls) -> WorkspaceLayout:
        """Return a layout whose paths are relative to the workspace root."""
        return cls(root=Path(DIRECTORY_NAME))

    @classmethod
    def sibling_of(cls, path: Path) -> WorkspaceLayout:
        """Return the layout whose directories sit alongside *path*.

        Used where a component holds one root and needs another beside it — an
        artifact store built from a configured cache root, say. This does not
        verify that *path* sits inside a ``.ginkgo`` directory, and callers do
        pass roots that do not: a store's ``root=`` is caller-supplied, so a
        test pointing one at a scratch directory gets a layout rooted there.
        That is the same assumption the bare ``.parent`` at each call site
        already made, gathered into one place rather than validated.

        Parameters
        ----------
        path : Path
            A directory whose siblings form the wanted layout, typically a
            cache or assets root.
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
    def db(self) -> Path:
        """The provenance ledger.

        ``GINKGO_DB`` relocates it, and is read here and nowhere else. That
        matters on a shared filesystem, where SQLite's locking is unreliable
        and the fix is to put the database somewhere local.
        """
        override = os.environ.get(GINKGO_DB_ENV_VAR)
        if override:
            return Path(override)
        return self.root / "ginkgo.db"

    @property
    def staging_cache_file(self) -> Path:
        """Persisted staging state for remote inputs."""
        return self.root / "remote-staged.json"
