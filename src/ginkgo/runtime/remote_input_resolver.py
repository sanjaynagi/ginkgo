"""Remote input staging for the concurrent evaluator.

The ``RemoteStager`` resolves ``RemoteFileRef``/``RemoteFolderRef``
arguments (and bare remote URI strings under ``file``/``folder``
annotations) into local paths via the worker-local staging cache. It
also owns in-flight deduplication so that two tasks asking for the same
remote ref share a single download.

Before staging, it resolves each remote input's identity from its store's
metadata (one HEAD or LIST), so the evaluator can look the task up in the
cache without downloading anything. :func:`remote_value_needs_identity`
answers whether an argument still needs that.
"""

from __future__ import annotations


import json
import os
import time
from concurrent.futures import Future
from dataclasses import dataclass, field, replace
from pathlib import Path
from threading import Lock
from typing import Any, get_args, get_origin

from ginkgo.config import load_runtime_config
from ginkgo.core.remote import (
    RemoteFileRef,
    RemoteFolderRef,
    RemoteRef,
    is_remote_uri,
    remote_file,
    remote_folder,
)
from ginkgo.core.types import (
    PLAIN_SCALAR_TYPES,
    file,
    folder,
    plain_data_strings,
    rebuild_sequence,
)
from ginkgo.remote.access.doctor import local_streaming_available
from ginkgo.remote.access.protocol import encode_fuse_ref
from ginkgo.remote.access.resolver import (
    AccessConfig,
    TaskAccessPolicy,
    load_access_config,
    resolve_access,
)
from ginkgo.remote.staging import resolve_identity
from ginkgo.runtime.task_kinds import task_kind
from ginkgo.workspace_layout import WorkspaceLayout


def remote_value_needs_identity(*, annotation: Any, value: Any) -> bool:
    """Return whether a value holds a remote input whose identity is not known yet.

    Such an input is resolved, with one HEAD or LIST on its store, before the
    task's cache key can be built. A remote URI string counts only under a
    ``file`` or ``folder`` annotation; anywhere else it is a plain string.
    """
    if type(value) in PLAIN_SCALAR_TYPES:
        return False
    # A large plain value is settled by its distinct strings alone.
    strings = plain_data_strings(value)
    if strings is not None and not any(is_remote_uri(text) for text in strings):
        return False
    if isinstance(value, RemoteRef):
        return value.identity() is None
    if isinstance(value, str):
        return (annotation is file or annotation is folder) and is_remote_uri(value)

    item_annotation, value_annotation = _element_annotations(annotation)
    if isinstance(value, list | tuple):
        return any(
            remote_value_needs_identity(annotation=item_annotation, value=item) for item in value
        )
    if isinstance(value, dict):
        return any(
            remote_value_needs_identity(annotation=value_annotation, value=item)
            for item in value.values()
        )
    return False


def _element_annotations(annotation: Any) -> tuple[Any, Any]:
    """Return the annotation of a typed container's items and of a dict's values.

    An untyped container keeps its own annotation for its items, as the
    walk above always has.
    """
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in {list, tuple}:
        return (args[0] if args else Any), Any
    if origin is dict:
        return Any, (args[1] if len(args) == 2 else Any)
    return annotation, Any


def _param_annotation(*, task_def: Any, name: str) -> Any:
    """Return a task parameter's resolved annotation."""
    return task_def.type_hints.get(name, task_def.signature.parameters[name].annotation)


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
            json.dumps(ref.identity(), sort_keys=True),
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

    from ginkgo.core.remote import parse_remote_uri
    from ginkgo.remote.publisher import RemotePublisher
    from ginkgo.remote.resolve import resolve_backend

    location = parse_remote_uri(store_uri)
    backend = resolve_backend(location.scheme)

    # The key from the URI becomes the prefix for all published artifacts.
    prefix = location.key
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    artifacts_root = WorkspaceLayout.for_cwd().artifacts
    return RemotePublisher(
        backend=backend,
        bucket=location.bucket,
        prefix=prefix,
        scheme=location.scheme,
        local_blobs_dir=artifacts_root / "blobs",
        local_trees_dir=artifacts_root / "trees",
    )


@dataclass(kw_only=True)
class RemoteStager:
    """Stage remote inputs into local paths with in-flight deduplication.

    Parameters
    ----------
    timing_recorder : Callable[..., None] | None
        Optional callback ``timing_recorder(node_id=, phase=, started=)``
        used to record per-task staging duration when provenance is on.
    access_config : AccessConfig | None
        Workflow-level remote-access configuration. ``None`` triggers a
        lazy load from ``ginkgo.toml``.
    """

    timing_recorder: Any = None
    access_config: AccessConfig | None = None
    _staging_cache: Any = field(default=None, init=False, repr=False)
    _staging_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    _staging_inflight: dict[str, Future[Path]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _identities: dict[tuple[str, str], RemoteRef] = field(
        default_factory=dict, init=False, repr=False
    )
    _identity_lock: Lock = field(default_factory=Lock, init=False, repr=False)

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

    def needs_identity(self, *, node: Any) -> bool:
        """Return whether any of the node's remote inputs still needs its identity."""
        assert node.resolved_args is not None
        return any(
            remote_value_needs_identity(
                annotation=_param_annotation(task_def=node.task_def, name=name), value=value
            )
            for name, value in node.resolved_args.items()
        )

    def resolve_task_identities(self, *, node: Any) -> dict[str, Any]:
        """Return the node's arguments with each remote input's identity resolved.

        Runs on the staging pool, since it asks the store: one HEAD per file
        and one LIST per folder. A remote URI string under a ``file`` or
        ``folder`` annotation becomes the ref it names.
        """
        assert node.resolved_args is not None
        return {
            name: self._resolve_identities(
                annotation=_param_annotation(task_def=node.task_def, name=name), value=value
            )
            for name, value in node.resolved_args.items()
        }

    def _resolve_identities(self, *, annotation: Any, value: Any) -> Any:
        """Resolve the remote inputs in one value, leaving everything else as is."""
        if not remote_value_needs_identity(annotation=annotation, value=value):
            return value
        if isinstance(value, RemoteRef):
            return self._resolve_ref(ref=value)
        if isinstance(value, str):
            ref = remote_file(value) if annotation is file else remote_folder(value)
            return self._resolve_ref(ref=ref)

        item_annotation, value_annotation = _element_annotations(annotation)
        if isinstance(value, list | tuple):
            return rebuild_sequence(
                like=value,
                items=[
                    self._resolve_identities(annotation=item_annotation, value=item)
                    for item in value
                ],
            )
        return {
            key: self._resolve_identities(annotation=value_annotation, value=item)
            for key, item in value.items()
        }

    def _resolve_ref(self, *, ref: RemoteRef) -> RemoteRef:
        """Resolve one ref, asking the store once per URI in a run.

        Every task in a run that reads a URI sees the same identity, so they
        all key on, and read, the same version of it.
        """
        if ref.identity() is not None:
            return ref
        key = (type(ref).__name__, ref.uri)
        with self._identity_lock:
            resolved = self._identities.get(key)
        if resolved is None:
            resolved = resolve_identity(ref=ref)
            with self._identity_lock:
                resolved = self._identities.setdefault(key, resolved)
        return replace(resolved, access=ref.access)

    # Argument-walking helpers ----------------------------------------------

    def stage_remote_refs(
        self,
        *,
        task_def: Any,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Stage every remote reference in a resolved argument map.

        Refs resolved to a ``fuse`` policy are emitted as encoded marker
        dicts (see :func:`ginkgo.remote.access.protocol.encode_fuse_ref`)
        and **not** staged locally; the worker mounts them on demand.
        """
        task_policy = TaskAccessPolicy.from_task_def(task_def)
        access_config = self._get_access_config()

        # Fuse mounting is wired for the remote dispatch path (the worker
        # mounts markers via `MountedAccess`) and for local process-pool
        # tasks (hydrated in `run_task` against a task-local mount root).
        # Driver-kind tasks run on the scheduler thread with no mount
        # lifecycle, so they always stage. Local worker tasks stream only
        # when the host has a healthy driver + `/dev/fuse`; that per-scheme
        # check is deferred to `resolve_access` via `require_local_driver`.
        dispatches_remote = bool(
            getattr(task_def, "remote", False)
            or getattr(task_def, "executor", None) is not None
            or getattr(task_def, "gpu", 0) > 0
        )
        # A driver kind's body runs on the scheduler or in a driver subprocess,
        # not through ``run_task`` in the process pool, so it has no local
        # mount lifecycle and always stages remote inputs.
        is_driver_kind = task_kind(task_def).execution_mode == "driver"
        require_local_driver = False
        if not dispatches_remote:
            if is_driver_kind:
                task_policy = TaskAccessPolicy(
                    remote_input_access="stage",
                    streaming_compatible=False,
                    fuse_prefetch=task_policy.fuse_prefetch,
                )
            else:
                require_local_driver = True

        staged: dict[str, Any] = {}
        for name, value in resolved_args.items():
            annotation = task_def.type_hints.get(
                name,
                task_def.signature.parameters[name].annotation
                if name in task_def.signature.parameters
                else Any,
            )
            staged[name] = self.stage_remote_value(
                annotation=annotation,
                value=value,
                task_policy=task_policy,
                access_config=access_config,
                require_local_driver=require_local_driver,
            )
        return staged

    def stage_remote_value(
        self,
        *,
        annotation: Any,
        value: Any,
        task_policy: TaskAccessPolicy | None = None,
        access_config: AccessConfig | None = None,
        require_local_driver: bool = False,
    ) -> Any:
        """Stage a single value, recursing into typed containers.

        When ``require_local_driver`` is ``True`` (local worker tasks),
        fuse selection is gated per scheme on the local host having a
        healthy driver + ``/dev/fuse``; otherwise fuse degrades to
        ``stage``. Remote dispatch leaves this ``False`` and assumes the
        pod carries the drivers.
        """
        task_policy = task_policy or TaskAccessPolicy()
        access_config = access_config or self._get_access_config()

        def _policy_for(ref: RemoteRef) -> str:
            driver_available = (
                local_streaming_available(scheme=ref.scheme) if require_local_driver else True
            )
            return resolve_access(
                ref=ref,
                task_policy=task_policy,
                config=access_config,
                driver_available=driver_available,
            )

        # Explicit remote refs.
        if isinstance(value, (RemoteFileRef, RemoteFolderRef)):
            policy = _policy_for(value)
            if policy.startswith("fuse"):
                return encode_fuse_ref(ref=value, policy=policy)
            if isinstance(value, RemoteFileRef):
                return file(str(self._stage_remote_ref(ref=value)))
            return folder(str(self._stage_remote_ref(ref=value)))

        # Annotation-aware coercion: raw URI string → remote ref → staged path.
        if isinstance(value, str) and is_remote_uri(value):
            if annotation is file:
                from ginkgo.core.remote import remote_file

                ref = remote_file(value)
                policy = _policy_for(ref)
                if policy.startswith("fuse"):
                    return encode_fuse_ref(ref=ref, policy=policy)
                return file(str(self._stage_remote_ref(ref=ref)))
            if annotation is folder:
                from ginkgo.core.remote import remote_folder

                ref = remote_folder(value)
                policy = _policy_for(ref)
                if policy.startswith("fuse"):
                    return encode_fuse_ref(ref=ref, policy=policy)
                return folder(str(self._stage_remote_ref(ref=ref)))

        # Recurse into typed containers.
        origin = get_origin(annotation)
        if origin in {list, tuple} and isinstance(value, (list, tuple)):
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            staged_items = [
                self.stage_remote_value(
                    annotation=inner_annotation,
                    value=item,
                    task_policy=task_policy,
                    access_config=access_config,
                    require_local_driver=require_local_driver,
                )
                for item in value
            ]
            return rebuild_sequence(like=value, items=staged_items)

        return value

    def _get_access_config(self) -> AccessConfig:
        """Return the cached access config, loading it on first use."""
        if self.access_config is None:
            self.access_config = load_access_config()
        return self.access_config

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
