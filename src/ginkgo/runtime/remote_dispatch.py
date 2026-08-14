"""Remote task dispatch: code bundles, artifact store, job handles, polling."""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ginkgo.runtime.caching.digest_registry import DigestRegistry
from ginkgo.runtime.events import TaskRunning, task_id_for_node
from ginkgo.runtime.remote_executor import (
    RemoteDispatchStats,
    RemoteExecutor,
    RemoteJobHandle,
    RemoteJobState,
)

if TYPE_CHECKING:
    from ginkgo.runtime.evaluator import NodeRun


@dataclass(kw_only=True)
class RemoteDispatchManager:
    """Dispatch tasks to a remote executor and track their job handles.

    Owns the remote-only state of a run: the published code bundle, the
    remote artifact store, in-flight job handles, dispatch statistics, and
    the staging cache persisted between runs. The evaluator builds the base
    worker payload (shared with local process-pool dispatch) and hands it
    to :meth:`dispatch`, which augments it with remote transport details
    and returns the watcher future for the evaluator loop to consume.
    """

    executor: RemoteExecutor | None
    code_bundle_config: dict[str, Any] | None
    digests: DigestRegistry
    local_artifact_store: Any
    staging_cache_path: Path
    run_id_provider: Callable[[], str]
    emit_event: Callable[[object], None]
    stats: RemoteDispatchStats = field(default_factory=RemoteDispatchStats)
    _handles: dict[int, RemoteJobHandle] = field(default_factory=dict, init=False, repr=False)
    _code_bundle_meta: dict[str, str] | None = field(default=None, init=False, repr=False)
    _artifact_store: Any = field(default=None, init=False, repr=False)
    _artifact_store_checked: bool = field(default=False, init=False, repr=False)
    _published_artifacts: set[str] = field(default_factory=set, init=False, repr=False)

    def dispatch(
        self,
        *,
        node: NodeRun,
        payload: dict[str, Any],
        gpu_type: str | None,
        watcher: ThreadPoolExecutor,
    ) -> Future[dict[str, Any]]:
        """Submit one task to the remote executor and return its watcher future."""
        self._ensure_code_bundle()
        self._ensure_artifact_store()
        payload["resources"] = {
            "threads": node.threads,
            "memory_gb": node.memory_gb,
            "gpu": node.gpu,
            "gpu_type": gpu_type,
        }
        if self._code_bundle_meta is not None:
            payload["code_bundle"] = self._code_bundle_meta
        if self._artifact_store is not None:
            from ginkgo.runtime.artifacts.remote_arg_transfer import stage_args_for_remote

            payload["args"] = stage_args_for_remote(
                args=payload["args"],
                type_hints=node.task_def.type_hints,
                remote_store=self._artifact_store,
                known_digests=self.digests.known,
                published_artifacts=self._published_artifacts,
            )
            payload["remote_artifact_store"] = {
                "scheme": self._artifact_store.scheme,
                "bucket": self._artifact_store.bucket,
                "prefix": self._artifact_store.prefix,
            }
        assert self.executor is not None  # guaranteed by placement
        handle = self.executor.submit(attempt=payload)
        self.stats.record_submit()
        self._handles[node.node_id] = handle
        return watcher.submit(self._poll_job, handle, node=node)

    def handle_for(self, node_id: int) -> RemoteJobHandle | None:
        """Return the in-flight job handle for a node, if any."""
        return self._handles.get(node_id)

    def pop_handle(self, node_id: int) -> RemoteJobHandle | None:
        """Remove and return the in-flight job handle for a node, if any."""
        return self._handles.pop(node_id, None)

    def cancel_all(self) -> None:
        """Cancel all in-flight remote job handles."""
        for handle in self._handles.values():
            with suppress(Exception):
                handle.cancel()
        self._handles.clear()

    def capture_logs(self, *, node: NodeRun, handle: RemoteJobHandle) -> None:
        """Fetch pod logs and write them to the standard task log paths."""
        try:
            logs = handle.logs_tail(lines=10000)
        except Exception:
            return
        if not logs:
            return

        # Remote workers merge stdout/stderr into one stream — write to both
        # paths so users find tracebacks where they expect them.
        for log_path in (node.stdout_path, node.stderr_path):
            if log_path is not None:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log_path.write_text(logs, encoding="utf-8")

    def save_staging_cache(self) -> None:
        """Persist staging state so the next run skips re-hashing unchanged inputs."""
        if not self.digests.known and not self._published_artifacts:
            return
        from ginkgo.runtime.artifacts.remote_arg_transfer import save_staging_cache

        save_staging_cache(
            cache_path=self.staging_cache_path,
            known_digests=self.digests.known,
            published_artifacts=self._published_artifacts,
        )

    def _poll_job(self, handle: RemoteJobHandle, *, node: NodeRun) -> dict[str, Any]:
        """Poll a remote job handle until it reaches a terminal state.

        Called on a watcher thread — blocks until the remote job finishes.
        Returns the worker result payload for consumption by the evaluator
        loop (same shape as a local ``run_task`` return value).
        """
        poll_interval = 5.0
        max_interval = 30.0
        emitted_running = False
        t_submit = time.monotonic()
        t_running: float | None = None
        while True:
            state = handle.state()
            if state.is_terminal:
                break
            if not emitted_running and state == RemoteJobState.RUNNING:
                emitted_running = True
                t_running = time.monotonic()
                self.emit_event(
                    TaskRunning(
                        run_id=self.run_id_provider(),
                        task_id=task_id_for_node(node.node_id),
                        task_name=node.task_def.name,
                        attempt=node.attempt,
                        display_label=node.display_label,
                        remote_job_id=handle.job_id,
                    )
                )
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, max_interval)

        t_done = time.monotonic()
        pending_s = (t_running or t_done) - t_submit
        running_s = t_done - t_running if t_running else 0.0
        self.stats.record_terminal(state=state)
        self.stats.record_phase_time(pending=pending_s, running=running_s)

        result = handle.result()

        if result.state == RemoteJobState.CANCELLED:
            raise KeyboardInterrupt("Remote job was cancelled")

        if result.state == RemoteJobState.FAILED and not result.payload:
            raise RuntimeError(
                f"Remote job {handle.job_id} failed"
                + (f" (exit code {result.exit_code})" if result.exit_code is not None else "")
                + (f"\n{result.logs}" if result.logs else "")
            )

        payload = result.payload
        if (
            self._artifact_store is not None
            and isinstance(payload, dict)
            and payload.get("ok")
            and payload.get("result_encoding") == "encoded"
            and "result" in payload
        ):
            from ginkgo.runtime.artifacts.remote_arg_transfer import hydrate_result_from_remote

            scratch_dir = self._artifact_store.local._root / "remote-outputs"
            payload["result"] = hydrate_result_from_remote(
                result=payload["result"],
                remote_store=self._artifact_store,
                scratch_dir=scratch_dir,
            )

        return payload

    def _load_staging_cache(self) -> None:
        """Restore persisted staging state from ``.ginkgo/remote-staged.json``."""
        from ginkgo.runtime.artifacts.remote_arg_transfer import load_staging_cache

        digests, published = load_staging_cache(cache_path=self.staging_cache_path)
        self.digests.update(digests)
        self._published_artifacts.update(published)

    def _ensure_code_bundle(self) -> None:
        """Create and publish the code bundle on first remote dispatch.

        Reads ``code_bundle_config`` (from ``[remote.k8s.code]``) to decide
        whether to sync workflow code to the remote backend. The bundle is
        created once per run and reused for all remote tasks.
        """
        if self._code_bundle_meta is not None:
            return
        if self.code_bundle_config is None:
            return
        mode = self.code_bundle_config.get("mode", "baked")
        if mode != "sync":
            return

        package = self.code_bundle_config.get("package")
        if not package:
            raise ValueError(
                "Code-sync mode requires [remote.k8s.code] package to be set "
                'in ginkgo.toml (e.g. package = "my_workflow")'
            )

        from ginkgo.remote.code_bundle import create_code_bundle, publish_code_bundle
        from ginkgo.remote.resolve import resolve_backend
        from ginkgo.core.remote import _parse_uri

        package_path = Path.cwd() / package
        if not package_path.is_dir():
            raise FileNotFoundError(f"Code-sync package directory not found: {package_path}")

        # Determine remote storage from [remote.artifacts] config.
        from ginkgo.config import load_runtime_config

        config = load_runtime_config(project_root=Path.cwd())
        artifacts_config = config.get("remote", {}).get("artifacts", {})
        store_uri = artifacts_config.get("store") if isinstance(artifacts_config, dict) else None
        if store_uri is None:
            raise ValueError(
                "Code-sync mode requires [remote.artifacts] store to be configured in ginkgo.toml"
            )

        parsed = _parse_uri(store_uri)
        backend = resolve_backend(parsed["scheme"])
        prefix = parsed["key"]
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        extra_excludes = self.code_bundle_config.get("exclude")
        if isinstance(extra_excludes, str):
            extra_excludes = [extra_excludes]
        bundle_path, digest = create_code_bundle(
            package_path=package_path,
            extra_excludes=extra_excludes,
        )
        try:
            remote_key = publish_code_bundle(
                backend=backend,
                bucket=parsed["bucket"],
                prefix=prefix,
                bundle_path=bundle_path,
                digest=digest,
            )
        finally:
            Path(bundle_path).unlink(missing_ok=True)

        self._code_bundle_meta = {
            "scheme": parsed["scheme"],
            "bucket": parsed["bucket"],
            "key": remote_key,
            "digest": digest,
            "package": package,
            "package_parent": str(package_path.parent.resolve()),
        }

    def _ensure_artifact_store(self) -> None:
        """Lazily construct a ``RemoteArtifactStore`` from project config.

        Uses the ``[remote.artifacts]`` store URI, wrapping the local
        artifact store already owned by the cache. Called just before
        dispatching a remote task so that ``file`` / ``folder`` inputs
        can be uploaded to the shared object store and hydrated inside
        the worker pod.
        """
        if self._artifact_store_checked:
            return
        self._artifact_store_checked = True
        self._load_staging_cache()
        from ginkgo.runtime.artifacts.remote_artifact_store import (
            load_remote_artifact_store,
        )

        self._artifact_store = load_remote_artifact_store(
            local=self.local_artifact_store,
        )
