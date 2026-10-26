"""Remote input staging for the concurrent evaluator.

The ``RemoteStager`` resolves ``RemoteFileRef``/``RemoteFolderRef``
arguments (and bare remote URI strings under ``file``/``folder``
annotations) into local paths via the worker-local staging cache.

Before staging, it resolves each remote input's identity from its store's
metadata, so the evaluator can look the task up in the cache without
downloading anything.

The unit of work in both steps is one remote input, not one task: each
unique input is one job on the staging pool, and a task continues once all of
its inputs' jobs are done. A URI that several tasks or list items name is
fetched once.
"""

from __future__ import annotations


import json
import os
import time
from collections import defaultdict
from collections.abc import Callable
from concurrent.futures import CancelledError, Executor, Future, InvalidStateError
from contextlib import suppress
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
    annotation_includes,
    file,
    folder,
    plain_data_strings,
    rebuild_sequence,
)
from ginkgo.remote.access.doctor import local_streaming_available
from ginkgo.remote.access.protocol import encode_remote_input, is_fuse_policy, is_remote_input
from ginkgo.remote.access.resolver import (
    AccessConfig,
    TaskAccessPolicy,
    load_access_config,
    resolve_access,
)
from ginkgo.remote.staging import DEFAULT_STAGING_JOBS, StagingCache, resolve_identities
from ginkgo.runtime.task_kinds import task_kind
from ginkgo.workspace_layout import WorkspaceLayout


def map_remote_inputs(*, annotation: Any, value: Any, fn: Callable[[RemoteRef], Any]) -> Any:
    """Return *value* with each remote input in it replaced by ``fn(ref)``.

    A remote input is a ``RemoteFileRef`` or ``RemoteFolderRef`` anywhere in
    the value's lists, tuples and dict values, or a remote URI string under a
    ``file`` or ``folder`` annotation, which becomes the ref it names. A
    remote-input marker is left as it is. A container none of whose items
    changed is returned as it is, so its type survives.
    """
    if type(value) in PLAIN_SCALAR_TYPES or is_remote_input(value):
        return value
    # A large plain value is settled by its distinct strings alone.
    strings = plain_data_strings(value)
    if strings is not None and not any(is_remote_uri(text) for text in strings):
        return value
    if isinstance(value, RemoteRef):
        return fn(value)
    if isinstance(value, str):
        if not is_remote_uri(value):
            return value
        if annotation_includes(annotation=annotation, expected=file):
            return fn(remote_file(value))
        if annotation_includes(annotation=annotation, expected=folder):
            return fn(remote_folder(value))
        return value

    item_annotation, value_annotation = _element_annotations(annotation)
    if isinstance(value, list | tuple):
        items = [map_remote_inputs(annotation=item_annotation, value=v, fn=fn) for v in value]
        if all(new is old for new, old in zip(items, value)):
            return value
        return rebuild_sequence(like=value, items=items)
    if isinstance(value, dict):
        mapped = {
            key: map_remote_inputs(annotation=value_annotation, value=item, fn=fn)
            for key, item in value.items()
        }
        if all(mapped[key] is item for key, item in value.items()):
            return value
        return mapped
    return value


def _element_annotations(annotation: Any) -> tuple[Any, Any]:
    """Return the annotation of a typed container's items and of a dict's values.

    An untyped container keeps its own annotation for its items.
    """
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in {list, tuple}:
        return (args[0] if args else Any), Any
    if origin is dict:
        return Any, (args[1] if len(args) == 2 else Any)
    return annotation, Any


def _map_args(*, task_def: Any, args: dict[str, Any], fn: Callable[[RemoteRef], Any]) -> dict:
    """Apply :func:`map_remote_inputs` to each argument, under its parameter's annotation."""
    return {
        name: map_remote_inputs(
            annotation=_param_annotation(task_def=task_def, name=name), value=value, fn=fn
        )
        for name, value in args.items()
    }


def _remote_inputs(*, task_def: Any, args: dict[str, Any]) -> list[RemoteRef]:
    """Return every remote input in a task's arguments, in order."""
    found: list[RemoteRef] = []

    def collect(ref: RemoteRef) -> RemoteRef:
        found.append(ref)
        return ref

    _map_args(task_def=task_def, args=args, fn=collect)
    return found


def _param_annotation(*, task_def: Any, name: str) -> Any:
    """Return a task parameter's resolved annotation, or ``Any`` for an unknown name."""
    parameter = task_def.signature.parameters.get(name)
    return task_def.type_hints.get(name, parameter.annotation if parameter else Any)


def _gather(futures: dict[Any, Future[Any]], then: Callable[[dict[Any, Any]], Any]) -> Future[Any]:
    """Return a future of ``then`` applied to every future's result, by key.

    The first of *futures* to fail fails it. ``then`` runs on the thread that
    finishes the last of them, so no thread ever waits for the others.
    """
    gathered: Future[Any] = Future()
    remaining = len(futures)
    lock = Lock()

    def settle(set_outcome: Callable[[Any], None], outcome: Any) -> None:
        with suppress(InvalidStateError):  # cancelled, or failed already
            set_outcome(outcome)

    def finish() -> None:
        try:
            result = then({key: future.result() for key, future in futures.items()})
        except BaseException as exc:
            settle(gathered.set_exception, exc)
        else:
            settle(gathered.set_result, result)

    def on_done(future: Future[Any]) -> None:
        nonlocal remaining
        with lock:
            remaining -= 1
            last = remaining == 0
        error = CancelledError() if future.cancelled() else future.exception()
        if error is not None:
            settle(gathered.set_exception, error)
        elif last:
            finish()

    if not futures:
        finish()
    for future in futures.values():
        future.add_done_callback(on_done)
    return gathered


def _list_group(key: Any, ref: RemoteRef) -> Any:
    """Return the group a ref's identity is resolved with: one LIST serves a group of files.

    Files group by parent prefix. A folder, and a file at a bucket's root
    (rather than list the whole bucket), is a group of its own.
    """
    parent = ref.key.rpartition("/")[0]
    return (ref.scheme, ref.bucket, parent) if isinstance(ref, RemoteFileRef) and parent else key


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


def resolve_staging_jobs() -> int:
    """Return how many store requests staging keeps in flight at once."""
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

    return DEFAULT_STAGING_JOBS


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


@dataclass(frozen=True, kw_only=True)
class StagedInputs:
    """A task's arguments with their remote inputs staged or marked.

    Parameters
    ----------
    args : dict[str, Any]
        The arguments, each remote input replaced by a local path or a marker.
    digests : dict[str, str]
        The digest of each input staged here, by URI (see
        :meth:`ginkgo.remote.staging.StagingCache.digest_of`).
    """

    args: dict[str, Any]
    digests: dict[str, str]


@dataclass(kw_only=True)
class RemoteStager:
    """Stage remote inputs into local paths, fetching each URI once.

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
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)
    # Downloads in flight, by ``remote_ref_identity``: a later task stages
    # through the staging cache again, which finds the bytes there.
    _staging: dict[str, Future[Path]] = field(default_factory=dict, init=False, repr=False)
    # Identities for the whole run, by ref type and URI, so every task reading
    # a URI keys on, and reads, the same version of it.
    _identities: dict[tuple[str, str], Future[RemoteRef]] = field(
        default_factory=dict, init=False, repr=False
    )

    # Per-task entry points --------------------------------------------------

    def needs_identity(self, *, node: Any) -> bool:
        """Return whether any of the node's remote inputs still needs its identity."""
        assert node.resolved_args is not None
        refs = _remote_inputs(task_def=node.task_def, args=node.resolved_args)
        return any(ref.identity() is None for ref in refs)

    def resolve_task_identities(self, *, node: Any, pool: Executor) -> Future[dict[str, Any]]:
        """Return a future of the node's arguments with each remote input's identity resolved.

        Each unresolved URI is asked about once per run, on *pool*: files
        sharing a parent prefix with another of the node's files from one LIST
        of it, any other file with a HEAD, and a folder with a LIST of its
        own. A remote URI string under a ``file`` or ``folder`` annotation
        becomes the ref it names.
        """
        assert node.resolved_args is not None
        refs = {
            _identity_key(ref): ref
            for ref in _remote_inputs(task_def=node.task_def, args=node.resolved_args)
            if ref.identity() is None
        }
        futures = self._fetch_once(
            pool=pool,
            memo=self._identities,
            refs=refs,
            group=_list_group,
            fetch=lambda batch: resolve_identities(refs=batch),
            keep=True,
        )

        def resolved(identities: dict[tuple[str, str], RemoteRef]) -> dict[str, Any]:
            return _map_args(
                task_def=node.task_def,
                args=node.resolved_args,
                fn=lambda ref: (
                    ref
                    if ref.identity() is not None
                    else replace(identities[_identity_key(ref)], access=ref.access)
                ),
            )

        return _gather(futures, resolved)

    def stage_task_inputs(self, *, node: Any, pool: Executor) -> Future[StagedInputs]:
        """Return a future of the node's arguments with every remote input staged or marked.

        A ``fuse`` input, and a ``direct`` one when the task is dispatched to a
        remote executor, becomes a marker dict (see
        :func:`ginkgo.remote.access.protocol.encode_remote_input`) that the
        worker reads itself. Every other input is downloaded on *pool*, one
        job per URI not already in flight.
        """
        assert node.resolved_args is not None
        started = time.perf_counter()
        marker_for = self._marker_for(
            task_def=node.task_def, dispatches_remote=node.executor_name is not None
        )
        refs = {
            remote_ref_identity(ref=ref): ref
            for ref in _remote_inputs(task_def=node.task_def, args=node.resolved_args)
            if marker_for(ref) is None
        }
        futures = self._fetch_once(
            pool=pool,
            memo=self._staging,
            refs=refs,
            group=lambda key, ref: key,
            fetch=lambda batch: [self._stage(ref=ref) for ref in batch],
            keep=False,
        )

        def staged(paths: dict[str, Path]) -> StagedInputs:
            digests: dict[str, str] = {}

            def place(ref: RemoteRef) -> Any:
                marker = marker_for(ref)
                if marker is not None:
                    return marker
                path = paths[remote_ref_identity(ref=ref)]
                digests[ref.uri] = StagingCache.digest_of(path)
                return file(str(path)) if isinstance(ref, RemoteFileRef) else folder(str(path))

            args = _map_args(task_def=node.task_def, args=node.resolved_args, fn=place)
            if self.timing_recorder is not None:
                self.timing_recorder(
                    node_id=node.node_id, phase="remote_stage_seconds", started=started
                )
            return StagedInputs(args=args, digests=digests)

        return _gather(futures, staged)

    def default_access(self, *, task_def: Any) -> str:
        """Return the access mode a task's remote input gets without a hint of its own."""
        return task_def.remote_input_access or self._get_access_config().default

    # Internal ------------------------------------------------------------------

    def _marker_for(
        self, *, task_def: Any, dispatches_remote: bool
    ) -> Callable[[RemoteRef], dict[str, Any] | None]:
        """Return how a task's remote input reaches it: a marker for its worker, or ``None`` to stage.

        A task on this host streams only when the host has a healthy driver
        and ``/dev/fuse`` for the scheme; otherwise fuse falls back to a
        download. A remote task's pod is assumed to carry the drivers.
        """
        task_policy = TaskAccessPolicy.from_task_def(task_def)
        access_config = self._get_access_config()
        # A driver kind's body runs on the scheduler or in a driver
        # subprocess, not through ``run_task``, so it has no mount lifecycle:
        # its inputs are downloaded here.
        if not dispatches_remote and task_kind(task_def).execution_mode == "driver":
            task_policy = TaskAccessPolicy(
                remote_input_access="direct",
                streaming_compatible=False,
                fuse_prefetch=task_policy.fuse_prefetch,
            )

        def marker_for(ref: RemoteRef) -> dict[str, Any] | None:
            policy = resolve_access(
                ref=ref,
                task_policy=task_policy,
                config=access_config,
                known_size=ref.size if isinstance(ref, RemoteFileRef) else None,
                driver_available=dispatches_remote or local_streaming_available(scheme=ref.scheme),
            )
            if is_fuse_policy(policy) or (policy == "direct" and dispatches_remote):
                return encode_remote_input(ref=ref, policy=policy)
            return None

        return marker_for

    def _fetch_once(
        self,
        *,
        pool: Executor,
        memo: dict[Any, Future[Any]],
        refs: dict[Any, RemoteRef],
        group: Callable[[Any, RemoteRef], Any],
        fetch: Callable[[list[RemoteRef]], list[Any]],
        keep: bool,
    ) -> dict[Any, Future[Any]]:
        """Return a future for each of *refs*, submitting work only for those not in *memo*.

        The new refs in each *group* are one ``fetch`` job on *pool*, which
        returns each ref's result or exception in order. A failed ref is forgotten, so a
        retry asks again; with *keep* false, so is one that succeeded.
        """
        with self._lock:
            unclaimed = {key: ref for key, ref in refs.items() if key not in memo}
            for key in unclaimed:
                memo[key] = Future()
            futures = {key: memo[key] for key in refs}

        groups: dict[Any, dict[Any, RemoteRef]] = defaultdict(dict)
        for key, ref in unclaimed.items():
            groups[group(key, ref)][key] = ref
        for batch in groups.values():
            keys = list(batch)

            def settle(job: Future[list[Any]], keys: list[Any] = keys) -> None:
                try:
                    outcomes = job.result()
                except BaseException as exc:  # cancelled, or the fetch itself failed
                    outcomes = [exc] * len(keys)
                with self._lock:
                    for key, outcome in zip(keys, outcomes):
                        if not keep or isinstance(outcome, BaseException):
                            memo.pop(key, None)
                for key, outcome in zip(keys, outcomes):
                    if isinstance(outcome, BaseException):
                        futures[key].set_exception(outcome)
                    else:
                        futures[key].set_result(outcome)

            pool.submit(fetch, list(batch.values())).add_done_callback(settle)
        return futures

    def _get_access_config(self) -> AccessConfig:
        """Return the cached access config, loading it on first use."""
        if self.access_config is None:
            self.access_config = load_access_config()
        return self.access_config

    def _stage(self, *, ref: RemoteRef) -> Path:
        """Stage one remote ref through the worker-local staging cache."""
        cache = self._get_staging_cache()
        if isinstance(ref, RemoteFileRef):
            return cache.stage_file(ref=ref)
        if isinstance(ref, RemoteFolderRef):
            return cache.stage_folder(ref=ref)
        raise TypeError(f"Unsupported remote ref type: {type(ref).__name__}")

    def _get_staging_cache(self) -> Any:
        """Lazily create and return the worker-local staging cache."""
        with self._lock:
            if self._staging_cache is None:
                from ginkgo.remote.staging import StagingCache

                self._staging_cache = StagingCache()
            return self._staging_cache


def _identity_key(ref: RemoteRef) -> tuple[str, str]:
    """Return the key a ref's identity is held under for the run."""
    return (type(ref).__name__, ref.uri)
