"""Run provenance capture and manifest management."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.value_codec import summarise_value


def make_run_id(*, workflow_path: str | Path | None = None) -> str:
    """Return a timestamped run identifier."""
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    token_source = str(Path(workflow_path).resolve()) if workflow_path is not None else timestamp
    suffix = abs(hash((token_source, timestamp))) % (16**8)
    return f"{timestamp}_{suffix:08x}"


@dataclass(kw_only=True)
class RunProvenanceRecorder:
    """Persist run history and per-task manifest updates."""

    run_id: str
    workflow_path: Path
    root_dir: Path
    jobs: int | None
    cores: int | None
    params: dict[str, Any] = field(default_factory=dict)
    status: str = field(default="running", init=False)
    run_dir: Path = field(init=False)
    manifest_path: Path = field(init=False)
    params_path: Path = field(init=False)
    envs_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    _manifest: dict[str, Any] = field(init=False, repr=False)
    _task_logs: dict[int, tuple[Path, Path]] = field(default_factory=dict, init=False, repr=False)
    _copied_envs: set[str] = field(default_factory=set, init=False, repr=False)

    def __post_init__(self) -> None:
        self.run_dir = self.root_dir / self.run_id
        self.manifest_path = self.run_dir / "manifest.yaml"
        self.params_path = self.run_dir / "params.yaml"
        self.envs_dir = self.run_dir / "envs"
        self.logs_dir = self.run_dir / "logs"
        self.envs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._manifest = {
            "run_id": self.run_id,
            "workflow": str(self.workflow_path),
            "jobs": self.jobs,
            "cores": self.cores,
            "status": self.status,
            "started_at": _timestamp(),
            "tasks": {},
        }
        self.write_params(self.params)
        self._write_manifest()

    def write_params(self, params: dict[str, Any]) -> None:
        """Persist resolved config parameters for the run."""
        self.params = _render_value(params)
        self.params_path.write_text(
            yaml.safe_dump(self.params, sort_keys=True),
            encoding="utf-8",
        )

    def ensure_task(self, *, node_id: int, task_name: str, env: str | None) -> tuple[Path, Path]:
        """Create a manifest entry and log paths for a task node.

        Returns
        -------
        tuple[Path, Path]
            ``(stdout_path, stderr_path)`` for the task.
        """
        tasks = self._manifest["tasks"]
        task_key = _task_key(node_id)
        if task_key not in tasks:
            slug = _slugify(task_name)
            stdout_path = self.logs_dir / f"{task_key}_{slug}.stdout.log"
            stderr_path = self.logs_dir / f"{task_key}_{slug}.stderr.log"
            self._task_logs[node_id] = (stdout_path, stderr_path)
            tasks[task_key] = {
                "task_id": task_key,
                "node_id": node_id,
                "task": task_name,
                "env": env,
                "cached": False,
                "exit_code": None,
                "stdout_log": str(stdout_path.relative_to(self.run_dir)),
                "stderr_log": str(stderr_path.relative_to(self.run_dir)),
                "status": "pending",
            }
            self._write_manifest()
        return self._task_logs[node_id]

    def update_task_inputs(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        resolved_args: dict[str, Any] | None,
        input_hashes: dict[str, Any] | None,
        cache_key: str | None,
        dependency_ids: list[int] | None = None,
        dynamic_dependency_ids: list[int] | None = None,
    ) -> None:
        """Record resolved task inputs and cache identity."""
        self.ensure_task(node_id=node_id, task_name=task_name, env=env)
        task = self._task(node_id)
        if resolved_args is not None:
            task["inputs"] = _render_value(resolved_args)
        if input_hashes is not None:
            task["input_hashes"] = _render_value(input_hashes)
        if cache_key is not None:
            task["cache_key"] = cache_key
        if dependency_ids is not None:
            task["dependency_ids"] = dependency_ids
        if dynamic_dependency_ids is not None:
            task["dynamic_dependency_ids"] = dynamic_dependency_ids
        self._write_manifest()

    def mark_running(self, *, node_id: int, task_name: str, env: str | None) -> None:
        """Mark a task as dispatched."""
        self.ensure_task(node_id=node_id, task_name=task_name, env=env)
        task = self._task(node_id)
        task["status"] = "running"
        task["started_at"] = _timestamp()
        self._write_manifest()

    def mark_cached(self, *, node_id: int, task_name: str, env: str | None, value: Any) -> None:
        """Mark a task as served from cache."""
        self.ensure_task(node_id=node_id, task_name=task_name, env=env)
        task = self._task(node_id)
        task["cached"] = True
        task["exit_code"] = 0
        task["output"] = _render_value(value)
        task["finished_at"] = _timestamp()
        task["status"] = "cached"
        self._write_manifest()

    def mark_succeeded(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        value: Any,
    ) -> None:
        """Mark a task as completed successfully."""
        self.ensure_task(node_id=node_id, task_name=task_name, env=env)
        task = self._task(node_id)
        task["cached"] = False
        task["exit_code"] = 0
        task["output"] = _render_value(value)
        task["finished_at"] = _timestamp()
        task["status"] = "succeeded"
        self._write_manifest()

    def mark_failed(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        exc: BaseException,
    ) -> None:
        """Mark a task as failed."""
        self.ensure_task(node_id=node_id, task_name=task_name, env=env)
        task = self._task(node_id)
        task["cached"] = False
        task["exit_code"] = getattr(exc, "exit_code", 1)
        task["error"] = str(exc)
        task["finished_at"] = _timestamp()
        task["status"] = "failed"
        self._write_manifest()

    def copy_env_lock(self, *, env_name: str, lock_path: Path) -> None:
        """Copy a Pixi lockfile into the run provenance directory once."""
        if env_name in self._copied_envs or not lock_path.is_file():
            return
        destination = self.envs_dir / f"{_slugify(env_name)}.pixi.lock"
        shutil.copy2(lock_path, destination)
        self._copied_envs.add(env_name)

    def finalize(self, *, status: str, error: str | None = None) -> None:
        """Write the final run status."""
        self.status = status
        self._manifest["status"] = status
        self._manifest["finished_at"] = _timestamp()
        if error is not None:
            self._manifest["error"] = error
        self._write_manifest()

    def log_paths_for(self, node_id: int) -> tuple[Path, Path] | None:
        """Return the ``(stdout, stderr)`` log paths for a task node."""
        return self._task_logs.get(node_id)

    def _task(self, node_id: int) -> dict[str, Any]:
        return self._manifest["tasks"][_task_key(node_id)]

    def _write_manifest(self) -> None:
        self.manifest_path.write_text(
            yaml.safe_dump(self._manifest, sort_keys=False),
            encoding="utf-8",
        )


def latest_run_dir(root_dir: Path) -> Path | None:
    """Return the newest run directory by run id."""
    if not root_dir.exists():
        return None
    runs = sorted(
        (path for path in root_dir.iterdir() if path.is_dir()), key=lambda item: item.name
    )
    return runs[-1] if runs else None


def load_manifest(run_dir: Path) -> dict[str, Any]:
    """Load a run manifest from disk."""
    manifest_path = run_dir / "manifest.yaml"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Run manifest not found at {manifest_path}")
    data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"Run manifest must contain a mapping: {manifest_path}")
    return data


def tail_text(path: Path, *, lines: int = 50) -> list[str]:
    """Return the last *lines* lines from a text file."""
    if not path.is_file():
        return []
    content = path.read_text(encoding="utf-8").splitlines()
    return content[-lines:]


def _task_key(node_id: int) -> str:
    return f"task_{node_id:04d}"


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _slugify(value: str) -> str:
    slug = "".join(ch if ch.isalnum() else "_" for ch in value).strip("_")
    return slug or "task"


def _render_value(value: Any) -> Any:
    if isinstance(value, (file, folder, tmp_dir, Path)):
        return str(value)
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_render_value(item) for item in value]
    if isinstance(value, tuple):
        return [_render_value(item) for item in value]
    if isinstance(value, dict):
        return {str(_render_value(key)): _render_value(item) for key, item in value.items()}
    return summarise_value(value)
