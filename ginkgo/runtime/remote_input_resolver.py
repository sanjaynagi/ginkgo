"""Remote input staging for the concurrent evaluator.

The ``RemoteStager`` resolves ``RemoteFileRef``/``RemoteFolderRef``
arguments (and bare remote URI strings under ``file``/``folder``
annotations) into local paths via the worker-local staging cache. It
also owns in-flight deduplication so that two tasks asking for the same
remote ref share a single download.

A small set of pure helpers covers the question "does this argument
need staging before its cache identity is safe?" — used by the cache
hit fast path in the evaluator.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, get_args, get_origin

from ginkgo.config import load_runtime_config
from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef, is_remote_uri
from ginkgo.core.types import file, folder


def remote_value_requires_staging(*, annotation: Any, value: Any) -> bool:
    """Return whether a value must be staged before cache identity is safe."""
    if isinstance(value, RemoteRef):
        return value.version_id is None

    if isinstance(value, str) and is_remote_uri(value):
        return True

    origin = get_origin(annotation)
    if origin in {list, tuple} and isinstance(value, (list, tuple)):
        inner_args = get_args(annotation)
        inner_annotation = inner_args[0] if inner_args else Any
        return any(
            remote_value_requires_staging(annotation=inner_annotation, value=item)
            for item in value
        )

    if origin is dict and isinstance(value, dict):
        key_annotation, value_annotation = (
            get_args(annotation) if len(get_args(annotation)) == 2 else (Any, Any)
        )
        return any(
            remote_value_requires_staging(annotation=key_annotation, value=key)
            or remote_value_requires_staging(annotation=value_annotation, value=item)
            for key, item in value.items()
        )

    if isinstance(value, list | tuple):
        return any(
            remote_value_requires_staging(annotation=annotation, value=item) for item in value
        )

    if isinstance(value, dict):
        return any(
            remote_value_requires_staging(annotation=Any, value=item) for item in value.values()
        )

    return False


def count_remote_inputs(value: Any) -> int:
    """Count nested remote refs and supported remote URI strings."""
    if isinstance(value, RemoteRef):
        return 1
    if isinstance(value, str) and is_remote_uri(value):
        return 1
    if isinstance(value, list | tuple):
        return sum(count_remote_inputs(item) for item in value)
    if isinstance(value, dict):
        return sum(count_remote_inputs(item) for item in value.values())
    return 0


def remote_ref_identity(*, ref: RemoteRef) -> str:
    """Return a stable in-flight identity key for a remote ref."""
    return "|".join(
        [
            type(ref).__name__,
            ref.scheme,
            ref.bucket,
            ref.key,
            ref.version_id or "",
        ]
    )


def resolve_staging_jobs(*, jobs: int) -> int:
    """Return the configured staging concurrency."""
    configured = os.environ.get("GINKGO_STAGING_JOBS")
    if configured:
        return _parse_positive_int(value=configured, label="GINKGO_STAGING_JOBS")

    config = load_runtime_config(project_root=Path.cwd())
    remote_config = config.get("remote", {})
    if isinstance(remote_config, dict) and remote_config.get("staging_jobs") is not None:
        return _parse_positive_int(
            value=remote_config["staging_jobs"],
            label="remote.staging_jobs",
        )

    return max(1, min(jobs, 4))


def _parse_positive_int(*, value: Any, label: str) -> int:
    """Parse a required positive integer config value."""
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def load_remote_publisher() -> Any | None:
    """Load a ``RemotePublisher`` from ``ginkgo.toml`` if configured.

    Looks for a ``[remote] store`` key containing a remote URI string
    (e.g. ``s3://bucket/prefix/``). Returns ``None`` when no remote store
    is configured for the current project.
    """
    config = load_runtime_config(project_root=Path.cwd())
    store_uri = config.get("remote", {}).get("store")
    if store_uri is None:
        return None

    from ginkgo.core.remote import _parse_uri
    from ginkgo.remote.publisher import RemotePublisher
    from ginkgo.remote.resolve import resolve_backend

    parsed = _parse_uri(store_uri)
    backend = resolve_backend(parsed["scheme"])

    # The key from the URI becomes the prefix for all published artifacts.
    prefix = parsed["key"]
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    artifacts_root = Path.cwd() / ".ginkgo" / "artifacts"
    return RemotePublisher(
        backend=backend,
        bucket=parsed["bucket"],
        prefix=prefix,
        scheme=parsed["scheme"],
        local_blobs_dir=artifacts_root / "blobs",
        local_trees_dir=artifacts_root / "trees",
        local_refs_dir=artifacts_root / "refs",
    )


@dataclass(kw_only=True)
class RemoteStager:
    """Stage remote inputs into local paths with in-flight deduplication.

    Parameters
    ----------
    timing_recorder : Callable[..., None] | None
        Optional callback ``timing_recorder(node_id=, phase=, started=)``
        used to record per-task staging duration when provenance is on.
    """

    timing_recorder: Any = None
    _staging_cache: Any = field(default=None, init=False, repr=False)
    _staging_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _staging_inflight: dict[str, Future[Path]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    # Per-task entry points --------------------------------------------------

    def stage_task_inputs(self, *, node: Any) -> dict[str, Any]:
        """Stage remote inputs for one task in the reserved worker slot."""
        assert node.resolved_args is not None
        stage_started = time.perf_counter()
        staged = self.stage_remote_refs(
            task_def=node.task_def,
            resolved_args=node.resolved_args,
        )
        if self.timing_recorder is not None:
            self.timing_recorder(
                node_id=node.node_id,
                phase="remote_stage_seconds",
                started=stage_started,
            )
        return staged

    def cache_lookup_requires_staging(self, *, node: Any) -> bool:
        """Return whether cache identity depends on staging remote inputs."""
        assert node.resolved_args is not None
        for name, value in node.resolved_args.items():
            annotation = node.task_def.type_hints.get(
                name,
                node.task_def.signature.parameters[name].annotation,
            )
            if remote_value_requires_staging(annotation=annotation, value=value):
                return True
        return False

    # Argument-walking helpers ----------------------------------------------

    def stage_remote_refs(
        self,
        *,
        task_def: Any,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Stage every remote reference in a resolved argument map."""
        staged: dict[str, Any] = {}
        for name, value in resolved_args.items():
            annotation = task_def.type_hints.get(
                name,
                task_def.signature.parameters[name].annotation
                if name in task_def.signature.parameters
                else Any,
            )
            staged[name] = self.stage_remote_value(annotation=annotation, value=value)
        return staged

    def stage_remote_value(self, *, annotation: Any, value: Any) -> Any:
        """Stage a single value, recursing into typed containers."""
        # Explicit remote refs.
        if isinstance(value, RemoteFileRef):
            return file(str(self._stage_remote_ref(ref=value)))
        if isinstance(value, RemoteFolderRef):
            return folder(str(self._stage_remote_ref(ref=value)))

        # Annotation-aware coercion: raw URI string → remote ref → staged path.
        if isinstance(value, str) and is_remote_uri(value):
            if annotation is file:
                from ginkgo.core.remote import remote_file

                ref = remote_file(value)
                return file(str(self._stage_remote_ref(ref=ref)))
            if annotation is folder:
                from ginkgo.core.remote import remote_folder

                ref = remote_folder(value)
                return folder(str(self._stage_remote_ref(ref=ref)))

        # Recurse into typed containers.
        origin = get_origin(annotation)
        if origin in {list, tuple} and isinstance(value, (list, tuple)):
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            staged_items = [
                self.stage_remote_value(annotation=inner_annotation, value=item) for item in value
            ]
            return type(value)(staged_items)

        return value

    # Internal: in-flight dedup -----------------------------------------------

    def _stage_remote_ref(self, *, ref: RemoteRef) -> Path:
        """Stage one remote ref with in-flight deduplication."""
        identity = remote_ref_identity(ref=ref)

        with self._staging_lock:
            inflight = self._staging_inflight.get(identity)
            if inflight is None:
                inflight = Future()
                self._staging_inflight[identity] = inflight
                should_stage = True
            else:
                should_stage = False

        if not should_stage:
            return inflight.result()

        try:
            staged_path = self._stage_remote_ref_uncached(ref=ref)
            inflight.set_result(staged_path)
            return staged_path
        except BaseException as exc:
            inflight.set_exception(exc)
            raise
        finally:
            with self._staging_lock:
                self._staging_inflight.pop(identity, None)

    def _stage_remote_ref_uncached(self, *, ref: RemoteRef) -> Path:
        """Stage one remote ref through the worker-local staging cache."""
        cache = self._get_staging_cache()
        if isinstance(ref, RemoteFileRef):
            return cache.stage_file(ref=ref)
        if isinstance(ref, RemoteFolderRef):
            return cache.stage_folder(ref=ref)
        raise TypeError(f"Unsupported remote ref type: {type(ref).__name__}")

    def _get_staging_cache(self) -> Any:
        """Lazily create and return the worker-local staging cache."""
        if self._staging_cache is None:
            from ginkgo.remote.staging import StagingCache

            self._staging_cache = StagingCache()
        return self._staging_cache
