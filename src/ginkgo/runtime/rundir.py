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
      envs/           a copy of each environment lockfile the run resolved
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

from ginkgo.store import open_store
from ginkgo.store.errors import StoreError
from ginkgo.workspace_layout import WorkspaceLayout

__all__ = [
    "RunDir",
    "RUNS_WITHOUT_DIRECTORY_CODE",
    "RUNS_WITHOUT_LEDGER_CODE",
    "orphaned_run_directories",
    "run_directory_problems",
    "runs_without_directory_warning",
    "runs_without_ledger_warning",
    "attempt_log_header",
    "combined_log_tail",
    "make_run_id",
    "manifest_text",
    "tail_text",
    "write_atomic",
    "write_manifest",
]

RUNS_WITHOUT_LEDGER_CODE = "runs_without_ledger"
"""The diagnostic code for run directories the ledger has no row for."""

RUNS_WITHOUT_DIRECTORY_CODE = "runs_without_directory"
"""The diagnostic code for recorded runs whose run directories are gone."""


def orphaned_run_directories(*, recorded_run_ids: set[str], root: Path) -> list[str]:
    """Return the names of run directories under *root* the ledger has no row for.

    Parameters
    ----------
    recorded_run_ids : set[str]
        Every run the ledger has a row for.
    root : Path
        The runs root, normally ``WorkspaceLayout.runs``.

    Returns
    -------
    list[str]
        Directory names, sorted.
    """
    if not root.is_dir():
        return []
    return [
        entry.name
        for entry in sorted(root.iterdir())
        if entry.is_dir() and entry.name not in recorded_run_ids
    ]


def runs_without_ledger_warning(*, layout: WorkspaceLayout) -> str | None:
    """Return the warning for a workspace holding runs its ledger has no row for.

    The ledger is what every read surface reads, so a run directory it has no
    row for is invisible to all of them. With no database at all, the
    workspace answers "no runs", which is the truth for a fresh project and a
    falsehood for one whose database was deleted, filtered out of a backup, or
    copied without ``ginkgo.db``. A database recreated since (the next run
    starts a new one) hides the older directories the same way. ``db check``,
    ``runs ls`` and ``doctor`` all report this answer.

    Parameters
    ----------
    layout : WorkspaceLayout
        The workspace to inspect. Nothing is created: a database that exists
        is opened read-only.

    Returns
    -------
    str | None
        One sentence naming how many run directories have no row, or ``None``
        when there are none (or the database cannot be read, which the
        command reading it reports for itself).
    """
    recorded = _recorded_run_ids(layout=layout)
    if recorded is None and layout.db.is_file():
        return None
    orphaned = orphaned_run_directories(recorded_run_ids=recorded or set(), root=layout.runs)
    if not orphaned:
        return None
    noun = "directory" if len(orphaned) == 1 else "directories"
    if recorded is None:
        return (
            f"{len(orphaned)} run {noun} under {layout.runs} but no database at "
            f"{layout.db}: this workspace has run, and its provenance cannot be read "
            "until the ledger is restored"
        )
    return (
        f"{len(orphaned)} run {noun} under {layout.runs} with no row in {layout.db}: "
        "the database was deleted or replaced after those runs, so ginkgo cannot read "
        "their provenance"
    )


def runs_without_directory_warning(*, layout: WorkspaceLayout) -> str | None:
    """Return the warning for recorded runs whose run directories are gone.

    The ledger still describes those runs, but their logs, manifests and
    rendered pages were deleted, so ``ginkgo debug`` and ``report`` have
    nothing to show for them. ``db check`` fails on it; ``doctor`` warns.

    Parameters
    ----------
    layout : WorkspaceLayout
        The workspace to inspect. A database that exists is opened read-only.

    Returns
    -------
    str | None
        One sentence naming how many runs have no directory, or ``None``.
    """
    recorded = _recorded_run_ids(layout=layout)
    missing = [run_id for run_id in sorted(recorded or ()) if not (layout.runs / run_id).is_dir()]
    if not missing:
        return None
    noun, pronoun = ("run", "its") if len(missing) == 1 else ("runs", "their")
    return (
        f"{len(missing)} {noun} recorded in {layout.db} with no run directory under "
        f"{layout.runs}: {pronoun} logs and manifests were deleted (`ginkgo db check` "
        "lists them)"
    )


def _recorded_run_ids(*, layout: WorkspaceLayout) -> set[str] | None:
    """Return every run the ledger has a row for, or ``None`` without a readable one."""
    if not layout.db.is_file():
        return None
    try:
        with open_store(layout.db, readonly=True) as store:
            return {str(row["run_id"]) for row in store.query("SELECT run_id FROM runs")}
    except StoreError:
        return None


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
    problems += [
        f"run directory {name} has no row (orphan)"
        for name in orphaned_run_directories(recorded_run_ids=recorded_run_ids, root=root)
    ]
    return problems


def make_run_id(*, workflow_path: str | Path | None = None) -> str:
    """Return a timestamped run identifier."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    token_source = str(Path(workflow_path).resolve()) if workflow_path is not None else timestamp
    discriminator = secrets.token_hex(4)
    suffix = abs(hash((token_source, timestamp, discriminator))) % (16**8)
    return f"{timestamp}_{suffix:08x}"


def tail_text(path: Path, *, lines: int | None = 50) -> list[str]:
    """Return the last *lines* lines from a text file, or all of them for ``None``."""
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8").splitlines()
    return content if lines is None else content[-lines:]


_ATTEMPT_HEADER_PREFIX = "--- attempt "


def attempt_log_header(*, attempt: int, attempts: int) -> str:
    """Return the line a retry writes into a task's logs before it starts.

    Every attempt appends to the same log files, so without a marker a log
    tail cannot say which attempt a line came from.
    """
    return f"{_ATTEMPT_HEADER_PREFIX}{attempt}/{attempts} ---\n"


def combined_log_tail(
    *,
    run_dir: Path,
    stdout_log: object,
    stderr_log: object,
    lines: int,
) -> list[str]:
    """Combine the last attempt's stdout and stderr for failure display.

    Only the output after each log's last attempt header is shown: a failure
    is the last attempt's, and earlier attempts' stderr would otherwise crowd
    out the last attempt's stdout. The header itself leads the tail, once.

    Each log argument is the relative path stored on a task record; it
    may be a string path, ``None``, or any other value depending on the
    caller's task representation. Non-string values are ignored, so
    callers can pass either mapping ``.get(...)`` results or dataclass
    attributes without an extra ``isinstance`` check.
    """
    header: str | None = None
    combined: list[str] = []
    for log in (stdout_log, stderr_log):
        if not isinstance(log, str):
            continue
        log_header, segment = _last_attempt(tail_text(run_dir / log, lines=None))
        header = header or log_header
        combined.extend(segment)
    if header is None:
        return combined[-lines:]
    return [header, *combined[max(0, len(combined) - lines + 1) :]]


def _last_attempt(content: list[str]) -> tuple[str | None, list[str]]:
    """Split a log into its last attempt header, if any, and what follows it."""
    for index in range(len(content) - 1, -1, -1):
        if content[index].startswith(_ATTEMPT_HEADER_PREFIX):
            return content[index], content[index + 1 :]
    return None, content


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
        # Named after the environment, keeping the lockfile's own name so its
        # format stays recognisable: envs/analysis.pixi.lock.
        destination = self.envs_dir / f"{_slugify(env_name)}.{lock_path.name}"
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
