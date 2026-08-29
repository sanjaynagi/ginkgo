"""The on-disk side of a run: logs, environment locks, and the snapshot.

Everything a run writes that is *bytes* lives here. What happened during the
run is the ledger's business (:mod:`ginkgo.store`); this module owns only the
directory those bytes go in, so neither concern has to know the other's shape.

The layout after a run is::

    runs/<run_id>/
      manifest.yaml   what the run recorded, exported once when it finished
      logs/           per-task stdout and stderr
      envs/           a copy of each Pixi lockfile the run resolved
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any

import yaml

__all__ = ["RunDir", "manifest_text", "write_manifest"]


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

    The one place the manifest's format lives, so the copy ``ginkgo export
    manifest`` writes is byte-identical to the one the run wrote at finalize.
    Written beside its destination and renamed over it, so a reader never sees
    half a manifest and an interrupted export leaves the previous one intact.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = path.with_suffix(path.suffix + ".tmp")
    pending.write_text(manifest_text(manifest), encoding="utf-8")
    os.replace(pending, path)
    return path


def manifest_text(manifest: dict[str, Any]) -> str:
    """Return the YAML text of a run's exported manifest.

    The format itself, so a manifest printed to a terminal and a manifest on
    disk are the same document.
    """
    return yaml.safe_dump(manifest, sort_keys=False, default_flow_style=False)
