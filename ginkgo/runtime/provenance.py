"""Run provenance capture and manifest management."""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

import yaml

from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.secrets import redact_value
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
    memory: int | None = None
    params: dict[str, Any] = field(default_factory=dict)
    status: str = field(default="running", init=False)
    run_dir: Path = field(init=False)
    manifest_path: Path = field(init=False)
    params_path: Path = field(init=False)
    envs_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    events_path: Path = field(init=False)
    _manifest: dict[str, Any] = field(init=False, repr=False)
    _task_logs: dict[int, tuple[Path, Path]] = field(default_factory=dict, init=False, repr=False)
    _copied_envs: set[str] = field(default_factory=set, init=False, repr=False)
    _lock: RLock = field(default_factory=RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        self.run_dir = self.root_dir / self.run_id
        self.manifest_path = self.run_dir / "manifest.yaml"
        self.params_path = self.run_dir / "params.yaml"
        self.envs_dir = self.run_dir / "envs"
        self.logs_dir = self.run_dir / "logs"
        self.events_path = self.run_dir / "events.jsonl"
        self.envs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._manifest = {
            "run_id": self.run_id,
            "workflow": str(self.workflow_path),
            "jobs": self.jobs,
            "cores": self.cores,
            "memory": self.memory,
            "status": self.status,
            "started_at": _timestamp(),
            "resources": _empty_resources(),
            "tasks": {},
        }
        self.write_params(self.params)
        self._write_manifest()

    def write_params(self, params: dict[str, Any]) -> None:
        """Persist resolved config parameters for the run."""
        with self._lock:
            self.params = _render_value(params)
            self.params_path.write_text(
                yaml.safe_dump(self.params, sort_keys=True),
                encoding="utf-8",
            )

    def ensure_task(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        kind: str = "python",
        execution_mode: str = "worker",
        retries: int = 0,
    ) -> tuple[Path, Path]:
        """Create a manifest entry and log paths for a task node.

        Returns
        -------
        tuple[Path, Path]
            ``(stdout_path, stderr_path)`` for the task.
        """
        with self._lock:
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
                    "kind": kind,
                    "execution_mode": execution_mode,
                    "retries": retries,
                    "max_attempts": retries + 1,
                    "attempt": 0,
                    "attempts": 0,
                    "retries_remaining": retries,
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
        kind: str | None = None,
        execution_mode: str | None = None,
        resolved_args: dict[str, Any] | None,
        input_hashes: dict[str, Any] | None,
        cache_key: str | None,
        dependency_ids: list[int] | None = None,
        dynamic_dependency_ids: list[int] | None = None,
    ) -> None:
        """Record resolved task inputs and cache identity."""
        with self._lock:
            self.ensure_task(
                node_id=node_id,
                task_name=task_name,
                env=env,
                kind=kind or "python",
                execution_mode=execution_mode or "worker",
            )
            task = self._task(node_id)
            if kind is not None:
                task["kind"] = kind
            if execution_mode is not None:
                task["execution_mode"] = execution_mode
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

    def mark_running(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        attempt: int,
        retries: int,
    ) -> None:
        """Mark a task as dispatched."""
        with self._lock:
            self.ensure_task(node_id=node_id, task_name=task_name, env=env, retries=retries)
            task = self._task(node_id)
            task["attempt"] = attempt
            task["attempts"] = max(int(task.get("attempts", 0)), attempt)
            task["retries"] = retries
            task["max_attempts"] = retries + 1
            task["retries_remaining"] = max(0, retries - (attempt - 1))
            task["status"] = "running"
            task.setdefault("started_at", _timestamp())
            task["last_started_at"] = _timestamp()
            self._write_manifest()

    def mark_retrying(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        exc: BaseException,
        attempt: int,
        retries_remaining: int,
    ) -> None:
        """Record a failed attempt that will be retried."""
        with self._lock:
            self.ensure_task(node_id=node_id, task_name=task_name, env=env)
            task = self._task(node_id)
            task["cached"] = False
            task["attempt"] = attempt
            task["attempts"] = max(int(task.get("attempts", 0)), attempt)
            task["last_error"] = str(exc)
            task["last_exit_code"] = getattr(exc, "exit_code", 1)
            task["retries_remaining"] = retries_remaining
            task["status"] = "pending"
            task.pop("finished_at", None)
            self._write_manifest()

    def mark_cached(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        value: Any,
        outputs: list[dict[str, Any]] | None = None,
        assets: list[dict[str, Any]] | None = None,
    ) -> None:
        """Mark a task as served from cache."""
        with self._lock:
            self.ensure_task(node_id=node_id, task_name=task_name, env=env)
            task = self._task(node_id)
            task["cached"] = True
            task["exit_code"] = 0
            task["output"] = _render_value(value)
            task["outputs"] = _render_value(outputs or [])
            if assets is not None:
                task["assets"] = _render_value(assets)
            task["finished_at"] = _timestamp()
            task["status"] = "cached"
            task.pop("error", None)
            task.pop("last_error", None)
            task.pop("last_exit_code", None)
            self._write_manifest()

    def mark_succeeded(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        value: Any,
        outputs: list[dict[str, Any]] | None = None,
        assets: list[dict[str, Any]] | None = None,
    ) -> None:
        """Mark a task as completed successfully."""
        with self._lock:
            self.ensure_task(node_id=node_id, task_name=task_name, env=env)
            task = self._task(node_id)
            task["cached"] = False
            task["exit_code"] = 0
            task["output"] = _render_value(value)
            task["outputs"] = _render_value(outputs or [])
            if assets is not None:
                task["assets"] = _render_value(assets)
            task["finished_at"] = _timestamp()
            task["status"] = "succeeded"
            task.pop("error", None)
            task.pop("last_error", None)
            task.pop("last_exit_code", None)
            self._write_manifest()

    def mark_failed(
        self,
        *,
        node_id: int,
        task_name: str,
        env: str | None,
        exc: BaseException,
        failure: dict[str, Any] | None = None,
    ) -> None:
        """Mark a task as failed."""
        with self._lock:
            self.ensure_task(node_id=node_id, task_name=task_name, env=env)
            task = self._task(node_id)
            task["cached"] = False
            task["exit_code"] = getattr(exc, "exit_code", 1)
            task["error"] = str(exc)
            task["failure"] = _render_value(failure or {})
            task["last_error"] = str(exc)
            task["last_exit_code"] = getattr(exc, "exit_code", 1)
            task["retries_remaining"] = 0
            task["finished_at"] = _timestamp()
            task["status"] = "failed"
            self._write_manifest()

    def update_task_extra(self, *, node_id: int, **fields: Any) -> None:
        """Merge additional metadata fields into a task entry."""
        with self._lock:
            task = self._task(node_id)
            task.update(fields)
            self._write_manifest()

    def copy_env_lock(self, *, env_name: str, lock_path: Path) -> None:
        """Copy a Pixi lockfile into the run provenance directory once."""
        with self._lock:
            if env_name in self._copied_envs or not lock_path.is_file():
                return
            destination = self.envs_dir / f"{_slugify(env_name)}.pixi.lock"
            shutil.copy2(lock_path, destination)
            self._copied_envs.add(env_name)

    def update_resources(self, resources: dict[str, Any]) -> None:
        """Persist the latest run-level resource summary."""
        with self._lock:
            self._manifest["resources"] = _render_value(resources)
            self._write_manifest()

    def finalize(
        self,
        *,
        status: str,
        error: str | None = None,
        resources: dict[str, Any] | None = None,
    ) -> None:
        """Write the final run status."""
        with self._lock:
            self.status = status
            self._manifest["status"] = status
            self._manifest["finished_at"] = _timestamp()
            if resources is not None:
                self._manifest["resources"] = _render_value(resources)
            if error is not None:
                self._manifest["error"] = error
            self._write_manifest()

    def log_paths_for(self, node_id: int) -> tuple[Path, Path] | None:
        """Return the ``(stdout, stderr)`` log paths for a task node."""
        with self._lock:
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
    value = redact_value(value)
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


def _empty_resources() -> dict[str, Any]:
    return {
        "status": "pending",
        "scope": "process_tree",
        "sample_count": 0,
        "current": None,
        "peak": None,
        "average": None,
        "updated_at": None,
    }
