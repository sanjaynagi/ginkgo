"""Run provenance capture and manifest management."""

from __future__ import annotations

import json
import secrets
import shutil
import time
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
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
    token_source = str(Path(workflow_path).resolve()) if workflow_path is not None else timestamp
    discriminator = secrets.token_hex(4)
    suffix = abs(hash((token_source, timestamp, discriminator))) % (16**8)
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
            "timings": _empty_timings(),
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
                    "timings": {},
                }
                self._append_event(
                    {
                        "event": "provenance_task_created",
                        "run_id": self.run_id,
                        "task_id": task_key,
                        "task": tasks[task_key],
                    }
                )
            return self._task_logs[node_id]

    def add_run_timing(self, *, phase: str, seconds: float) -> None:
        """Accumulate one run-level timing bucket without writing immediately."""
        if seconds <= 0:
            return
        with self._lock:
            timings = self._manifest.setdefault("timings", _empty_timings())
            run_timings = timings.setdefault("run", {})
            run_timings[phase] = _rounded_seconds(run_timings.get(phase, 0.0) + seconds)
            self._append_event(
                {
                    "event": "provenance_run_timing",
                    "run_id": self.run_id,
                    "phase": phase,
                    "seconds": _rounded_seconds(seconds),
                }
            )

    def add_task_timing(self, *, node_id: int, phase: str, seconds: float) -> None:
        """Accumulate one task timing bucket without writing immediately."""
        if seconds <= 0:
            return
        with self._lock:
            task = self._task(node_id)
            task_timings = task.setdefault("timings", {})
            task_timings[phase] = _rounded_seconds(task_timings.get(phase, 0.0) + seconds)

            timings = self._manifest.setdefault("timings", _empty_timings())
            totals = timings.setdefault("task_phase_totals", {})
            totals[phase] = _rounded_seconds(totals.get(phase, 0.0) + seconds)
            self._append_event(
                {
                    "event": "provenance_task_timing",
                    "run_id": self.run_id,
                    "task_id": _task_key(node_id),
                    "phase": phase,
                    "seconds": _rounded_seconds(seconds),
                }
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    key: task[key]
                    for key in (
                        "kind",
                        "execution_mode",
                        "inputs",
                        "input_hashes",
                        "cache_key",
                        "dependency_ids",
                        "dynamic_dependency_ids",
                    )
                    if key in task
                },
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    "attempt": task["attempt"],
                    "attempts": task["attempts"],
                    "retries": task["retries"],
                    "max_attempts": task["max_attempts"],
                    "retries_remaining": task["retries_remaining"],
                    "status": task["status"],
                    "started_at": task["started_at"],
                    "last_started_at": task["last_started_at"],
                },
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    "cached": task["cached"],
                    "attempt": task["attempt"],
                    "attempts": task["attempts"],
                    "last_error": task["last_error"],
                    "last_exit_code": task["last_exit_code"],
                    "retries_remaining": task["retries_remaining"],
                    "status": task["status"],
                    "finished_at": None,
                },
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    "cached": task["cached"],
                    "exit_code": task["exit_code"],
                    "output": task["output"],
                    "outputs": task["outputs"],
                    "assets": task.get("assets"),
                    "finished_at": task["finished_at"],
                    "status": task["status"],
                    "error": None,
                    "last_error": None,
                    "last_exit_code": None,
                },
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    "cached": task["cached"],
                    "exit_code": task["exit_code"],
                    "output": task["output"],
                    "outputs": task["outputs"],
                    "assets": task.get("assets"),
                    "finished_at": task["finished_at"],
                    "status": task["status"],
                    "error": None,
                    "last_error": None,
                    "last_exit_code": None,
                },
            )

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
            self._append_task_update(
                node_id=node_id,
                fields={
                    "cached": task["cached"],
                    "exit_code": task["exit_code"],
                    "error": task["error"],
                    "failure": task["failure"],
                    "last_error": task["last_error"],
                    "last_exit_code": task["last_exit_code"],
                    "retries_remaining": task["retries_remaining"],
                    "finished_at": task["finished_at"],
                    "status": task["status"],
                },
            )

    def update_task_extra(self, *, node_id: int, **fields: Any) -> None:
        """Merge additional metadata fields into a task entry."""
        with self._lock:
            task = self._task(node_id)
            task.update(fields)
            self._append_task_update(node_id=node_id, fields=fields)

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
            self._append_event(
                {
                    "event": "provenance_run_update",
                    "run_id": self.run_id,
                    "fields": {"resources": self._manifest["resources"]},
                }
            )

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

    def _append_task_update(self, *, node_id: int, fields: dict[str, Any]) -> None:
        task_key = _task_key(node_id)
        self._append_event(
            {
                "event": "provenance_task_updated",
                "run_id": self.run_id,
                "task_id": task_key,
                "fields": _render_value(fields),
            }
        )

    def _append_event(self, payload: dict[str, Any]) -> None:
        started = time.perf_counter()
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")
        self._record_provenance_write(seconds=time.perf_counter() - started)

    def _record_provenance_write(self, *, seconds: float) -> None:
        timings = self._manifest.setdefault("timings", _empty_timings())
        run_timings = timings.setdefault("run", {})
        run_timings["provenance_write_seconds"] = _rounded_seconds(
            run_timings.get("provenance_write_seconds", 0.0) + seconds
        )

    def _write_manifest(self) -> None:
        started = time.perf_counter()
        self.manifest_path.write_text(
            yaml.safe_dump(
                {
                    **self._manifest,
                    "_provenance_event_offset": (
                        self.events_path.stat().st_size if self.events_path.is_file() else 0
                    ),
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        self._record_provenance_write(seconds=time.perf_counter() - started)


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
    event_offset = data.get("_provenance_event_offset", 0)
    if not isinstance(event_offset, int):
        event_offset = 0
    return _replay_provenance_events(run_dir=run_dir, manifest=data, start_offset=event_offset)


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


def _empty_timings() -> dict[str, Any]:
    return {
        "run": {},
        "task_phase_totals": {},
    }


def _rounded_seconds(value: float) -> float:
    return round(float(value), 6)


def _replay_provenance_events(
    *, run_dir: Path, manifest: dict[str, Any], start_offset: int
) -> dict[str, Any]:
    events_path = run_dir / "events.jsonl"
    if not events_path.is_file():
        return manifest

    # Replay append-only provenance patches over the base manifest snapshot.
    manifest.pop("_provenance_event_offset", None)
    tasks = manifest.setdefault("tasks", {})
    timings = manifest.setdefault("timings", _empty_timings())
    run_timings = timings.setdefault("run", {})
    totals = timings.setdefault("task_phase_totals", {})

    with events_path.open("r", encoding="utf-8") as handle:
        if start_offset > 0:
            handle.seek(start_offset)
        lines = handle.read().splitlines()

    for line in lines:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue

        event_name = event.get("event")
        if event_name == "provenance_task_created":
            task_id = event.get("task_id")
            task_payload = event.get("task")
            if isinstance(task_id, str) and isinstance(task_payload, dict):
                tasks[task_id] = dict(task_payload)
            continue

        if event_name == "provenance_task_updated":
            task_id = event.get("task_id")
            fields = event.get("fields")
            if isinstance(task_id, str) and isinstance(fields, dict):
                task = tasks.setdefault(task_id, {})
                for key, value in fields.items():
                    if value is None:
                        task.pop(key, None)
                    else:
                        task[key] = value
            continue

        if event_name == "provenance_run_update":
            fields = event.get("fields")
            if isinstance(fields, dict):
                for key, value in fields.items():
                    if value is None:
                        manifest.pop(key, None)
                    else:
                        manifest[key] = value
            continue

        if event_name == "provenance_run_timing":
            phase = event.get("phase")
            seconds = event.get("seconds")
            if isinstance(phase, str) and isinstance(seconds, int | float):
                run_timings[phase] = _rounded_seconds(run_timings.get(phase, 0.0) + seconds)
            continue

        if event_name == "provenance_task_timing":
            task_id = event.get("task_id")
            phase = event.get("phase")
            seconds = event.get("seconds")
            if (
                isinstance(task_id, str)
                and isinstance(phase, str)
                and isinstance(seconds, int | float)
            ):
                task = tasks.setdefault(task_id, {})
                task_timings = task.setdefault("timings", {})
                task_timings[phase] = _rounded_seconds(task_timings.get(phase, 0.0) + seconds)
                totals[phase] = _rounded_seconds(totals.get(phase, 0.0) + seconds)

    return manifest
