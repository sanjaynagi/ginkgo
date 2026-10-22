"""Failures the provenance store raises.

Each is a :class:`~ginkgo.errors.GinkgoError` subclass, so the CLI prints
their message and nothing else. Each message therefore has to be the whole
report: which database, what is wrong with it, and the command that fixes it.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo.errors import GinkgoError

__all__ = [
    "CorruptStoreError",
    "StaleWorkspaceError",
    "StoreError",
    "StoreLockedError",
    "UninitializedStoreError",
]


class StoreError(GinkgoError):
    """The provenance store could not be opened, read, or written."""


class StaleWorkspaceError(StoreError):
    """The database was written at a schema version other than this ginkgo's.

    Before 1.0 there are no migrations, so there is no path from another
    version's database to this one. Deleting the workspace is the upgrade: it
    holds derived data, and the runs it describes can be re-run.
    """

    def __init__(self, *, path: Path, found: int, expected: int) -> None:
        super().__init__(
            f"{path} was written by schema version {found}, and this ginkgo uses "
            f"{expected}. Pre-1.0 ginkgo does not migrate between schema versions. "
            f"Delete the workspace ({path.parent}) and run again — it holds "
            "provenance for past runs, not results you cannot rebuild."
        )
        self.path = path
        self.found = found
        self.expected = expected


class UninitializedStoreError(StoreError):
    """A read-only open found the database file but no schema in it yet.

    The window between a first run creating the file and committing its
    schema. A reader cannot create the schema itself, so it says what will.
    """

    def __init__(self, *, path: Path) -> None:
        super().__init__(
            f"{path} has not been initialized yet. Run ginkgo once in this workspace to create it."
        )
        self.path = path


class StoreLockedError(StoreError):
    """Another process held the write lock for longer than the busy timeout."""

    def __init__(self, *, path: Path, timeout_ms: int) -> None:
        super().__init__(
            f"{path} is locked by another process after waiting {timeout_ms} ms. "
            "Wait for the other ginkgo run to finish, or set GINKGO_DB to a "
            "separate database for this run."
        )
        self.path = path
        self.timeout_ms = timeout_ms


class CorruptStoreError(StoreError):
    """The database file is not a readable SQLite database."""

    def __init__(self, *, path: Path, detail: str) -> None:
        super().__init__(
            f"{path} is not a readable ginkgo database ({detail}); it is corrupt or was "
            f"overwritten. Move it aside (`mv {path} {path}.broken`) and run again: ginkgo "
            "starts a new one, without the run history and cache entries the old one "
            "held, so tasks run again rather than coming from the cache."
        )
        self.path = path
