"""The on-disk side of a run: logs, environment locks, and the snapshot.

Everything a run writes that is *bytes* lives here. What happened during the
run is the ledger's business (:mod:`ginkgo.store`); this module owns only the
directory those bytes go in, so neither concern has to know the other's shape.

It also names a run (:func:`make_run_id`) and reads back the logs it wrote
(:func:`tail_text`, :func:`combined_log_tail`).

The layout after a run is::

    runs/<run_id>/
      manifest.yaml   what the run recorded, exported once when it finished
      logs/           per-task stdout and stderr
      envs/           a copy of each Pixi lockfile the run resolved
"""

from __future__ import annotations

import os
import secrets
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

import yaml

__all__ = [
    "RunDir",
    "run_directory_problems",
    "combined_log_tail",
    "make_run_id",
    "manifest_text",
    "tail_text",
    "write_atomic",
    "write_manifest",
]


def run_directory_problems(*, recorded_run_ids: set[str], root: Path) -> list[str]:
    """Return the runs and run directories that have no counterpart under *root*.

    This module owns what a run leaves on disk, so it owns the question of
    whether the ledger and the disk still describe the same set of runs. Both
    directions mean different things: a row with no directory is a run whose
    logs and manifest were deleted — the record survives, the evidence does
    not; a directory with no row is bytes from a database that is gone, which
    nothing will ever read again.

    Parameters
    ----------
    recorded_run_ids : set[str]
        Every run the ledger has a row for.
    root : Path
        The runs root, normally ``WorkspaceLayout.runs``.

    Returns
    -------
    list[str]
        One sentence per problem.
    """
    problems = [
        f"run {run_id} has a row but no run directory"
        for run_id in sorted(recorded_run_ids)
        if not (root / run_id).is_dir()
    ]
    if not root.is_dir():
        return problems
    problems += [
        f"run directory {entry.name} has no row (orphan)"
        for entry in sorted(root.iterdir())
        if entry.is_dir() and entry.name not in recorded_run_ids
    ]
    return problems


def make_run_id(*, workflow_path: str | Path | None = None) -> str:
    """Return a timestamped run identifier."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    token_source = str(Path(workflow_path).resolve()) if workflow_path is not None else timestamp
    discriminator = secrets.token_hex(4)
    suffix = abs(hash((token_source, timestamp, discriminator))) % (16**8)
    return f"{timestamp}_{suffix:08x}"


def tail_text(path: Path, *, lines: int = 50) -> list[str]:
    """Return the last *lines* lines from a text file."""
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8").splitlines()
    return content[-lines:]


def combined_log_tail(
    *,
    run_dir: Path,
    stdout_log: object,
    stderr_log: object,
    lines: int,
) -> list[str]:
    """Combine stdout and stderr tails for failure display.

    Each log argument is the relative path stored on a task record; it
    may be a string path, ``None``, or any other value depending on the
    caller's task representation. Non-string values are ignored, so
    callers can pass either mapping ``.get(...)`` results or dataclass
    attributes without an extra ``isinstance`` check.
    """
    combined: list[str] = []
    if isinstance(stdout_log, str):
        combined.extend(tail_text(run_dir / stdout_log, lines=lines))
    if isinstance(stderr_log, str):
        combined.extend(tail_text(run_dir / stderr_log, lines=lines))
    return combined[-lines:]


@dataclass(kw_only=True)
class RunDir:
    """The directory one run writes its bytes into.

    Construct through :meth:`create`, which is what makes the directories.

    Parameters
    ----------
    run_id : str
        The run this directory belongs to.
    root : Path
        The runs root, normally ``WorkspaceLayout.runs``.
    """

    run_id: str
    root: Path
    _copied_envs: set[str] = field(default_factory=set, init=False, repr=False)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    @classmethod
    def create(cls, *, run_id: str, root: Path) -> RunDir:
        """Create ``root/run_id`` with its ``logs/`` and ``envs/`` subdirectories."""
        run_dir = cls(run_id=run_id, root=Path(root))
        run_dir.logs_dir.mkdir(parents=True, exist_ok=True)
        run_dir.envs_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    @property
    def path(self) -> Path:
        """The run's own directory."""
        return self.root / self.run_id

    @property
    def logs_dir(self) -> Path:
        """Where per-task logs are written."""
        return self.path / "logs"

    @property
    def envs_dir(self) -> Path:
        """Where resolved environment lockfiles are copied."""
        return self.path / "envs"

    @property
    def manifest_path(self) -> Path:
        """Where :meth:`write_manifest` writes. The one home for this filename."""
        return self.path / "manifest.yaml"

    def log_paths_for(self, *, node_id: int, task_name: str) -> tuple[Path, Path]:
        """Return the ``(stdout, stderr)`` log paths for one task node.

        Pure naming: the same node and name always give the same pair, so the
        evaluator can ask for them again without keeping a table of its own.
        """
        stem = f"task_{node_id:04d}_{_slugify(task_name)}"
        return (self.logs_dir / f"{stem}.stdout.log", self.logs_dir / f"{stem}.stderr.log")

    def relative(self, path: Path) -> str:
        """Return *path* relative to the run directory, or unchanged if outside it."""
        try:
            return str(Path(path).relative_to(self.path))
        except ValueError:
            return str(path)

    def copy_env_lock(self, *, env_name: str, lock_path: Path) -> str | None:
        """Copy an environment lockfile in once, and return its relative path.

        Returns ``None`` when the lockfile does not exist or has already been
        copied by an earlier task using the same environment.
        """
        with self._lock:
            if env_name in self._copied_envs or not lock_path.is_file():
                return None
            self._copied_envs.add(env_name)
        destination = self.envs_dir / f"{_slugify(env_name)}.pixi.lock"
        shutil.copy2(lock_path, destination)
        return self.relative(destination)

    def write_manifest(self, manifest: dict[str, Any]) -> Path:
        """Write the run's exported manifest into the run directory."""
        return write_manifest(manifest, path=self.manifest_path)


def _slugify(value: str) -> str:
    """Return *value* reduced to a filesystem-safe stem."""
    slug = "".join(char if char.isalnum() else "_" for char in value).strip("_")
    return slug or "task"


def write_manifest(manifest: dict[str, Any], *, path: Path) -> Path:
    """Write a run's exported manifest to *path* and return it.

    The one place the manifest's destination lives, so the copy ``ginkgo export
    manifest`` writes is byte-identical to the one the run wrote at finalize.
    """
    return write_atomic(manifest_text(manifest), path=path)


def write_atomic(text: str, *, path: Path) -> Path:
    """Write *text* to *path* through a temporary file, and return *path*.

    Written beside its destination and renamed over it, so a reader never sees
    half a file and an interrupted write leaves the previous one intact. Every
    document ginkgo exports goes out this way, whichever command asked for it.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix(path.suffix + ".tmp")
    pending.write_text(text, encoding="utf-8")
    os.replace(pending, path)
    return path


def manifest_text(manifest: dict[str, Any]) -> str:
    """Return the YAML text of a run's exported manifest.

    The format itself, so a manifest printed to a terminal and a manifest on
    disk are the same document.
    """
    return yaml.safe_dump(manifest, sort_keys=False, default_flow_style=False)
