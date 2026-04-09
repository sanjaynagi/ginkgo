"""Persistent log of file stats at artifact materialization time.

When Ginkgo produces or restores a file output, the stat metadata (size and
mtime_ns) is recorded alongside the artifact ID.  On subsequent runs,
``artifact_store.matches()`` can check this log before falling back to a
full content hash -- if the stat hasn't changed, the file is known-good.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class _MaterializationRecord:
    """Stat snapshot of a materialized artifact."""

    artifact_id: str
    size: int
    mtime_ns: int


class MaterializationLog:
    """Persistent mapping from output paths to their materialization stats.

    Parameters
    ----------
    path : Path
        Location of the JSON log file (e.g.
        ``.ginkgo/artifacts/materializations.json``).
    """

    def __init__(self, *, path: Path) -> None:
        self._path = path
        self._records: dict[str, _MaterializationRecord] = {}
        self._dirty = False
        self._load()

    def record(self, *, path: Path, artifact_id: str) -> None:
        """Record the current stat of a just-materialized file or directory.

        Parameters
        ----------
        path : Path
            The working-tree path that was materialized.
        artifact_id : str
            The artifact ID that was materialized at *path*.
        """
        resolved = path.resolve()
        if not resolved.exists():
            return
        st = resolved.stat()
        self._records[str(resolved)] = _MaterializationRecord(
            artifact_id=artifact_id,
            size=st.st_size,
            mtime_ns=st.st_mtime_ns,
        )
        self._dirty = True

    def check(self, *, path: Path, artifact_id: str) -> bool:
        """Return True if *path* matches its recorded materialization stat.

        Parameters
        ----------
        path : Path
            The working-tree path to check.
        artifact_id : str
            The expected artifact ID.

        Returns
        -------
        bool
            ``True`` when the current stat matches the recorded
            materialization and the artifact ID matches.
        """
        resolved = path.resolve()
        rec = self._records.get(str(resolved))
        if rec is None or rec.artifact_id != artifact_id:
            return False

        if not resolved.exists():
            return False

        st = resolved.stat()
        return st.st_size == rec.size and st.st_mtime_ns == rec.mtime_ns

    def save(self) -> None:
        """Persist the log to disk atomically."""
        if not self._dirty:
            return

        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {}
        for key, rec in self._records.items():
            payload[key] = {
                "artifact_id": rec.artifact_id,
                "size": rec.size,
                "mtime_ns": rec.mtime_ns,
            }

        # Atomic write via temp file + rename.
        fd, tmp = tempfile.mkstemp(dir=str(self._path.parent), suffix=".tmp", prefix="mat-")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"))
            os.replace(tmp, self._path)
        except BaseException:
            with open(os.devnull, "w"):
                pass
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

        self._dirty = False

    # -- internals -----------------------------------------------------------

    def _load(self) -> None:
        """Load existing records from disk, pruning stale entries."""
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return

        for key, entry in raw.items():
            if not isinstance(entry, dict):
                continue
            artifact_id = entry.get("artifact_id")
            size = entry.get("size")
            mtime_ns = entry.get("mtime_ns")
            if (
                not isinstance(artifact_id, str)
                or not isinstance(size, int)
                or not isinstance(mtime_ns, int)
            ):
                continue
            # Prune entries for paths that no longer exist.
            if not Path(key).exists():
                self._dirty = True
                continue
            self._records[key] = _MaterializationRecord(
                artifact_id=artifact_id,
                size=size,
                mtime_ns=mtime_ns,
            )
