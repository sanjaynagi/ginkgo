"""Failures the provenance store raises.

All three are :class:`~ginkgo.errors.GinkgoError` subclasses, so the CLI prints
their message and nothing else. Each message therefore has to be the whole
report: which database, what is wrong with it, and the command that fixes it.
"""

from __future__ import annotations

from pathlib import Path

from ginkgo.errors import GinkgoError

__all__ = ["SchemaVersionError", "StaleWorkspaceError", "StoreError", "StoreLockedError"]


class StoreError(GinkgoError):
    """The provenance store could not be opened, read, or written."""


class SchemaVersionError(StoreError):
    """The database schema is not the version this ginkgo expects.

    Raised only on read-only opens: a write-mode open migrates instead.
    """

    def __init__(self, *, path: Path, found: int, expected: int) -> None:
        super().__init__(
            f"{path} is at schema version {found}, but this ginkgo expects "
            f"{expected}. Run `ginkgo db migrate` to bring it up to date."
        )
        self.path = path
        self.found = found
        self.expected = expected


class StaleWorkspaceError(StoreError):
    """The database was written by a schema this ginkgo no longer knows.

    Before 1.0 the schema is edited in place rather than migrated, so there is
    no path from an older database to this one. Deleting the workspace is the
    upgrade: it holds derived data, and the runs it describes can be re-run.
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
