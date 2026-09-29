"""Argument staging for remote task execution.

Provides two complementary passes:

- :func:`stage_args_for_remote` (client side): walks task arguments, and for
  each ``file`` / ``folder`` value pointing at an existing local path,
  idempotently uploads the content to a :class:`RemoteArtifactStore` and
  replaces the argument with a structured reference carrying the artifact
  id and original path. Used by the remote-executor payload builder.

- :func:`hydrate_args_from_remote` (worker side): walks task arguments, and
  for each structured reference produced above, downloads the artifact from
  the remote store into a pod-local scratch directory and replaces the
  argument with the local path.

Local execution is untouched — these helpers are only called when a remote
executor is in use.
"""

from __future__ import annotations

import itertools
import uuid
from collections import abc
from pathlib import Path
from types import UnionType
from typing import TYPE_CHECKING, Any, Union, get_args, get_origin

from ginkgo.core.types import (
    annotation_includes,
    file,
    folder,
    pair_elements_with_annotations,
    unwrap_optional_annotation,
)
from ginkgo.remote.access.protocol import (
    is_fuse_ref,
)
from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore
from ginkgo.workspace_layout import WorkspaceLayout

if TYPE_CHECKING:
    from ginkgo.core.task import TaskDef


_REMOTE_FILE_TAG = "__ginkgo_remote_file__"
_REMOTE_FOLDER_TAG = "__ginkgo_remote_folder__"

# Parent directories whose contents are managed content-addressed blobs.
# Paths resolving into these trees are safe to hardlink rather than copy.
_LAYOUT = WorkspaceLayout.relative()
_MANAGED_BLOB_PARENTS = (
    str(_LAYOUT.staging / "blobs"),
    str(_LAYOUT.artifacts / "blobs"),
)


def _is_managed_cas_blob(*, path: Path) -> bool:
    """Return whether ``path`` resolves inside a Ginkgo CAS blob directory."""
    resolved = str(path)
    return any(f"/{marker}/" in resolved for marker in _MANAGED_BLOB_PARENTS)


def _annotation_matches(*, annotation: Any, target: type) -> bool:
    """Return True if an annotation directly or nestedly matches ``target``."""
    if annotation is target:
        return True
    origin = get_origin(annotation)
    if origin is None:
        return False
    return any(_annotation_matches(annotation=arg, target=target) for arg in get_args(annotation))


def stage_args_for_remote(
    *,
    args: dict[str, Any],
    type_hints: dict[str, Any],
    remote_store: RemoteArtifactStore,
    known_digests: dict[str, str] | None = None,
    output_params: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Rewrite ``file`` / ``folder`` arguments into remote-artifact references.

    Parameters
    ----------
    args : dict[str, Any]
        The resolved argument map for the task (already encoded by
        :func:`encode_value` for non-file types).
    type_hints : dict[str, Any]
        Parameter name → type annotation mapping for the task.
    remote_store : RemoteArtifactStore
        Target store to upload artifacts into.
    known_digests : dict[str, str] | None
        Optional path → artifact-id cache from the local artifact store.
        A hit means the artifact exists locally but says nothing about
        whether it has been published to the remote store yet — the store
        answers that from the artifact's own row.
    output_params : frozenset[str]
        Names of ``Out[...]`` parameters. These name paths the task is about
        to *write*, not read, so nothing exists yet to upload — staging them
        here would try to hash a file that does not exist. Left untouched;
        :func:`stage_output_params_for_dispatch` handles them separately.
    """
    known_digests = known_digests or {}
    staged: dict[str, Any] = {}
    for name, value in args.items():
        if name in output_params:
            staged[name] = value
            continue
        annotation = type_hints.get(name, Any)
        staged[name] = _stage_value(
            value=value,
            annotation=annotation,
            remote_store=remote_store,
            known_digests=known_digests,
        )
    return staged


def _stage_value(
    *,
    value: Any,
    annotation: Any,
    remote_store: RemoteArtifactStore,
    known_digests: dict[str, str],
) -> Any:
    """Stage a single argument value, recursing into typed containers."""
    # Fuse-streamed refs bypass CAS entirely — the worker mounts the
    # bucket directly. Pass the marker dict through unchanged.
    if is_fuse_ref(value):
        return value

    # file / folder: upload content, emit a remote reference dict.
    # Inputs may arrive either as raw strings (unencoded) or as the
    # ``{__ginkgo_type__: file/folder, value: <path>}`` dicts produced by
    # ``encode_value``.
    if _annotation_matches(annotation=annotation, target=file):
        path_str = _file_path_from_value(value=value, tag="file")
        if path_str is not None:
            return _stage_path(
                path=Path(path_str),
                tag=_REMOTE_FILE_TAG,
                remote_store=remote_store,
                known_digests=known_digests,
            )
    if _annotation_matches(annotation=annotation, target=folder):
        path_str = _file_path_from_value(value=value, tag="folder")
        if path_str is not None:
            return _stage_path(
                path=Path(path_str),
                tag=_REMOTE_FOLDER_TAG,
                remote_store=remote_store,
                known_digests=known_digests,
            )

    # Recurse into typed containers. Arguments reach here already passed
    # through ``encode_value``, which wraps containers as
    # ``{"__ginkgo_type__": "list"|"tuple"|"dict", "items": [...]}``; a raw
    # list/tuple/dict is accepted too for callers that stage unencoded values.
    encoded_tag = value.get("__ginkgo_type__") if isinstance(value, dict) else None
    if encoded_tag in {"list", "tuple", "dict"}:
        shape = "dict" if encoded_tag == "dict" else "sequence"
    elif isinstance(value, (list, tuple)):
        shape = "sequence"
    elif isinstance(value, dict):
        shape = "dict"
    else:
        return value

    container_annotation = _container_annotation(annotation=annotation, shape=shape)
    if container_annotation is None:
        return value

    def stage(item: Any, item_annotation: Any) -> Any:
        return _stage_value(
            value=item,
            annotation=item_annotation,
            remote_store=remote_store,
            known_digests=known_digests,
        )

    if shape == "sequence":
        items = value["items"] if encoded_tag is not None else value
        staged_items = [
            stage(item, item_annotation)
            for item_annotation, item in pair_elements_with_annotations(
                annotation=container_annotation, value=items
            )
        ]
        if encoded_tag is not None:
            return {**value, "items": staged_items}
        return list(staged_items) if isinstance(value, list) else tuple(staged_items)

    dict_args = get_args(container_annotation)
    key_annotation, value_annotation = dict_args if len(dict_args) == 2 else (Any, Any)
    if encoded_tag == "dict":
        return {
            **value,
            "items": [
                {
                    **entry,
                    "key": stage(entry["key"], key_annotation),
                    "value": stage(entry["value"], value_annotation),
                }
                for entry in value["items"]
            ],
        }
    # A raw dict's keys must stay hashable, so only its values are staged.
    return {key: stage(item, value_annotation) for key, item in value.items()}


_SEQUENCE_ORIGINS = frozenset(
    {list, tuple, abc.Sequence, abc.MutableSequence, abc.Collection, abc.Iterable}
)
_MAPPING_ORIGINS = frozenset({dict, abc.Mapping, abc.MutableMapping})


def _container_annotation(*, annotation: Any, shape: str) -> Any | None:
    """Return the member of *annotation* that describes a container of *shape*.

    Handles optionals and unions (``list[file] | list[str]``) by picking the
    member whose origin matches the value's shape, and abstract collection
    types (``Sequence[file]``, ``Mapping[str, file]``) alongside the concrete
    ones. ``None`` when nothing in the annotation describes such a container,
    in which case the value is passed through unstaged.
    """
    origin = get_origin(annotation)
    if origin in {Union, UnionType}:
        for member in get_args(annotation):
            found = _container_annotation(annotation=member, shape=shape)
            if found is not None:
                return found
        return None
    wanted = _SEQUENCE_ORIGINS if shape == "sequence" else _MAPPING_ORIGINS
    return annotation if origin in wanted else None


def _file_path_from_value(*, value: Any, tag: str) -> str | None:
    """Extract a path string from a raw or encoded file/folder argument."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict) and value.get("__ginkgo_type__") == tag:
        path = value.get("value")
        if isinstance(path, str):
            return path
    return None


def _stage_path(
    *,
    path: Path,
    tag: str,
    remote_store: RemoteArtifactStore,
    known_digests: dict[str, str],
) -> dict[str, str]:
    """Upload a single path and return its remote-reference dict.

    ``known_digests`` records path → artifact-id resolved from the local
    store; on a warm run the store's own materialization rows answer it for
    files this run has not touched. Only for files: a directory's mtime does
    not move when a child's contents change, so the stat guard behind those
    rows cannot tell a stale folder from a fresh one, and a folder is hashed
    rather than recognised.

    A hit guarantees only that the artifact exists locally, so the store is
    asked separately whether it has been published — that fact lives on the
    artifact's row.
    """
    resolved = path.resolve()
    key = str(resolved)
    artifact_id = known_digests.get(key)
    if artifact_id is None and resolved.is_file():
        artifact_id = remote_store.materialized_artifact_id(path=resolved)

    if artifact_id is not None and not remote_store.is_published(artifact_id):
        # Known locally, not yet on the remote: upload the bytes already in the
        # CAS rather than re-hashing and re-sharing the source to arrive back
        # at the same id.
        published = remote_store.publish(artifact_id=artifact_id)
        artifact_id = None if published is None else published.artifact_id

    if artifact_id is None:
        # A source already inside a Ginkgo content-addressed cache
        # (staging or artifact blobs) is immutable by construction, so
        # the store can hardlink it into the artifact blob dir instead
        # of duplicating the bytes. User-supplied paths do not qualify:
        # chmod on a shared inode would make the user's file read-only.
        record = remote_store.store(
            src_path=resolved,
            src_is_readonly=_is_managed_cas_blob(path=resolved),
        )
        artifact_id = record.artifact_id
    known_digests[key] = artifact_id
    return {
        "__ginkgo_type__": tag,
        "artifact_id": artifact_id,
        "path": str(resolved),
    }


def hydrate_args_from_remote(
    *,
    args: dict[str, Any],
    remote_store: RemoteArtifactStore,
    scratch_dir: Path,
) -> dict[str, Any]:
    """Resolve remote-reference dicts into local paths on the worker.

    Parameters
    ----------
    args : dict[str, Any]
        Argument map from the worker payload, possibly containing
        references produced by :func:`stage_args_for_remote`.
    remote_store : RemoteArtifactStore
        Store to retrieve artifacts from. The store is expected to have a
        writable local CAS root (typically under ``scratch_dir``).
    scratch_dir : Path
        Directory into which hydrated inputs are materialised.
    """
    scratch_dir.mkdir(parents=True, exist_ok=True)
    return _hydrate_value(value=args, remote_store=remote_store, scratch_dir=scratch_dir)


def _hydrate_value(*, value: Any, remote_store: RemoteArtifactStore, scratch_dir: Path) -> Any:
    """Recursively hydrate a value, materialising any remote references."""
    if isinstance(value, dict):
        tag = value.get("__ginkgo_type__")
        if tag == _REMOTE_FILE_TAG:
            return _hydrate_reference(
                ref=value,
                remote_store=remote_store,
                scratch_dir=scratch_dir,
                wrap=file,
            )
        if tag == _REMOTE_FOLDER_TAG:
            return _hydrate_reference(
                ref=value,
                remote_store=remote_store,
                scratch_dir=scratch_dir,
                wrap=folder,
            )
        return {
            key: _hydrate_value(value=item, remote_store=remote_store, scratch_dir=scratch_dir)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _hydrate_value(value=item, remote_store=remote_store, scratch_dir=scratch_dir)
            for item in value
        ]
    if isinstance(value, tuple):
        return tuple(
            _hydrate_value(value=item, remote_store=remote_store, scratch_dir=scratch_dir)
            for item in value
        )
    return value


def _hydrate_reference(
    *,
    ref: dict[str, Any],
    remote_store: RemoteArtifactStore,
    scratch_dir: Path,
    wrap: type,
) -> Any:
    """Download one artifact into the scratch dir and return a wrapped path."""
    dest = _materialize_remote_output(ref=ref, remote_store=remote_store, scratch_dir=scratch_dir)
    return wrap(str(dest))


def stage_output_params_for_dispatch(
    *,
    resolved_args: dict[str, Any],
    task_def: TaskDef,
    base_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Rewrite declared ``Out[...]`` paths into worker-local scratch markers.

    Called client side, before dispatch, for a task with ``output_params``
    when a remote artifact store is configured. A remote worker's filesystem
    is not assumed to be the driver's: the task's real absolute output paths
    (e.g. ``/home/user/project/results/x.csv``) would not exist, and could
    not be created, on the worker. Each declared leaf is instead replaced
    with a short relative marker; :func:`resolve_output_param_scratch_paths`
    turns that marker into a real scratch path once the payload reaches the
    worker.

    Walks *resolved_args* (the task's real, pre-encoding argument values —
    an ``Out[...]`` value need not be a ``file`` / ``folder`` instance, only
    a path-like value, so this cannot rely on :func:`encode_value` having
    tagged it) rather than the already-encoded payload, mirroring
    ``declared_output_paths``' own annotation-driven traversal.

    Returns ``{name: encoded_value}`` for just the declared output
    parameters — merge into the worker payload's ``args`` — alongside an
    ordered manifest of ``{"param", "relative", "original", "kind"}``
    entries, one per declared output leaf, that travels with the payload as
    ``output_param_entries`` and is later consumed by
    :func:`stage_output_param_results` (worker) and
    :func:`materialize_output_params_from_remote` (driver).
    """
    from ginkgo.runtime.artifacts.value_codec import encode_value

    manifest: list[dict[str, str]] = []
    counter = itertools.count()
    # One fresh directory per dispatch: two tasks sharing a worker must not
    # collide on "0-out.txt", and a file left by an earlier job must never
    # satisfy this job's "was the output written" check.
    dispatch_dir = uuid.uuid4().hex

    def remap(*, annotation: Any, value: Any, name: str) -> Any:
        if value is None:
            return None
        inner_annotation, _ = unwrap_optional_annotation(annotation)
        origin = get_origin(inner_annotation)
        if origin in {list, tuple} and isinstance(value, (list, tuple)):
            items = [
                remap(annotation=item_annotation, value=item, name=name)
                for item_annotation, item in pair_elements_with_annotations(
                    annotation=inner_annotation, value=value
                )
            ]
            return list(items) if origin is list else tuple(items)
        if isinstance(value, (list, tuple)):
            items = [remap(annotation=inner_annotation, value=item, name=name) for item in value]
            return type(value)(items)

        kind = (
            "file" if annotation_includes(annotation=inner_annotation, expected=file) else "folder"
        )
        original = str(value)
        relative = f"{dispatch_dir}/{next(counter)}-{Path(original).name}"
        manifest.append({"param": name, "relative": relative, "original": original, "kind": kind})
        return (file if kind == "file" else folder)(relative)

    encoded: dict[str, Any] = {}
    for name in sorted(task_def.output_params):
        if name not in resolved_args:
            continue
        annotation = task_def.type_hints.get(name)
        remapped = remap(annotation=annotation, value=resolved_args[name], name=name)
        encoded[name] = encode_value(remapped, base_dir=base_dir)
    return encoded, manifest


def resolve_output_param_scratch_paths(
    *,
    args: dict[str, Any],
    entries: list[dict[str, str]],
    scratch_dir: Path,
) -> dict[str, Any]:
    """Turn worker-local output markers into absolute scratch paths.

    Called on the worker, before the task body runs, so the ``Out[...]``
    parameters it receives are real writable paths under *scratch_dir* —
    each leaf's parent directory is created here, mirroring the driver's
    own pre-execution ``Out[...]`` parent-directory creation.
    """
    param_names = {entry["param"] for entry in entries}
    new_args = dict(args)
    for name in param_names:
        if name in new_args:
            new_args[name] = _resolve_scratch_leaves(value=new_args[name], scratch_dir=scratch_dir)
    return new_args


def _resolve_scratch_leaves(*, value: Any, scratch_dir: Path) -> Any:
    """Recurse through an encoded ``Out[...]`` value, resolving each marker."""
    if not isinstance(value, dict):
        return value
    kind = value.get("__ginkgo_type__")
    if kind in ("file", "folder"):
        dest = scratch_dir / value["value"]
        dest.parent.mkdir(parents=True, exist_ok=True)
        return {"__ginkgo_type__": kind, "value": str(dest)}
    if kind in ("list", "tuple"):
        return {
            **value,
            "items": [
                _resolve_scratch_leaves(value=item, scratch_dir=scratch_dir)
                for item in value.get("items", [])
            ],
        }
    return value


def stage_output_param_results(
    *,
    entries: list[dict[str, str]],
    remote_store: RemoteArtifactStore,
    scratch_dir: Path,
) -> list[dict[str, str]]:
    """Upload each ``Out[...]`` path the task body actually wrote.

    Called on the worker after the task body has run. An entry whose
    scratch path was not written (missing optional output, or a bug the
    driver-side ``validate_declared_outputs_written`` check should catch) is
    silently skipped rather than raised here — the driver runs the same
    check, by the same name and path, once materialisation leaves it either
    present or still missing, so the error is reported exactly as it would
    be for a local task.
    """
    staged: list[dict[str, str]] = []
    for entry in entries:
        path = scratch_dir / entry["relative"]
        is_file_kind = entry["kind"] == "file"
        written = path.is_file() if is_file_kind else (path.exists() and path.is_dir())
        if not written:
            continue
        record = remote_store.store(src_path=path, src_is_readonly=False)
        staged.append(
            {
                "original": entry["original"],
                "artifact_id": record.artifact_id,
                "kind": entry["kind"],
            }
        )
    return staged


def materialize_output_params_from_remote(
    *,
    entries: list[dict[str, str]],
    remote_store: RemoteArtifactStore,
) -> None:
    """Restore each produced ``Out[...]`` output at its declared driver path.

    Called client side once a remote job succeeds, before the task's result
    is finalised — the post-execution "was it written" check and cache-hit
    "outputs present" check both stat the declared path directly, so the
    bytes must already be there. Restores as writable content (not a
    symlink into the CAS) so a remote task's output is indistinguishable
    from one a local task wrote directly, and so the artifact store records
    it as a fresh materialization for future ``--trust-mtimes`` runs.
    """
    for entry in entries:
        dest = Path(entry["original"])
        remote_store.restore(artifact_id=entry["artifact_id"], dest_path=dest)


def stage_result_for_remote(
    *,
    result: Any,
    remote_store: RemoteArtifactStore,
    output_param_paths: dict[str, str] | None = None,
) -> Any:
    """Rewrite file/folder values in an encoded task result into remote refs.

    Called on the worker side after :func:`run_task` returns, to upload
    any ``file`` / ``folder`` outputs to the shared remote artifact store.
    The returned tree has the same shape, but encoded file/folder values
    (``{"__ginkgo_type__": "file"|"folder", ...}``) are replaced with
    remote-reference dicts that the client can hydrate.

    Parameters
    ----------
    result : Any
        The ``result`` payload produced by :func:`encode_value`.
    remote_store : RemoteArtifactStore
        Store to upload produced artifacts into.
    output_param_paths : dict[str, str] | None
        Worker-local scratch path → original driver path, for every declared
        ``Out[...]`` leaf (see :func:`stage_output_params_for_dispatch`). A
        task that returns its own ``Out[...]`` value (explicitly, not just
        via the inferred-return substitution) would otherwise have that
        value re-uploaded and materialised a second time, at a fresh
        artifact-store scratch path rather than the original one —
        :func:`materialize_output_params_from_remote` already restored it
        there. A leaf matching one of these paths is pointed straight at
        its original path instead, exactly as the local (same-filesystem)
        case already does implicitly.
    """
    return _stage_encoded_value(
        value=result, remote_store=remote_store, output_param_paths=output_param_paths or {}
    )


def _stage_encoded_value(
    *,
    value: Any,
    remote_store: RemoteArtifactStore,
    output_param_paths: dict[str, str] | None = None,
) -> Any:
    """Walk an encoded value tree, uploading file/folder leaves to remote."""
    output_param_paths = output_param_paths or {}
    if not isinstance(value, dict):
        return value

    kind = value.get("__ginkgo_type__")
    if kind in ("file", "folder"):
        original = output_param_paths.get(str(Path(value["value"])))
        if original is not None:
            return {"__ginkgo_type__": kind, "value": original}
        tag = _REMOTE_FILE_TAG if kind == "file" else _REMOTE_FOLDER_TAG
        return _stage_encoded_path(path_str=value["value"], tag=tag, remote_store=remote_store)
    if kind in {"list", "tuple"}:
        return {
            **value,
            "items": [
                _stage_encoded_value(
                    value=item, remote_store=remote_store, output_param_paths=output_param_paths
                )
                for item in value.get("items", [])
            ],
        }
    if kind == "dict":
        return {
            **value,
            "items": [
                {
                    "key": _stage_encoded_value(
                        value=item["key"],
                        remote_store=remote_store,
                        output_param_paths=output_param_paths,
                    ),
                    "value": _stage_encoded_value(
                        value=item["value"],
                        remote_store=remote_store,
                        output_param_paths=output_param_paths,
                    ),
                }
                for item in value.get("items", [])
            ],
        }
    if kind == "asset_result":
        return {
            **value,
            "payload": _stage_encoded_value(
                value=value["payload"],
                remote_store=remote_store,
                output_param_paths=output_param_paths,
            ),
        }
    return value


def _stage_encoded_path(
    *, path_str: str, tag: str, remote_store: RemoteArtifactStore
) -> dict[str, Any]:
    """Upload a single pod-local path and return a remote-reference dict."""
    return _stage_path(
        path=Path(path_str),
        tag=tag,
        remote_store=remote_store,
        known_digests={},
    )


def hydrate_result_from_remote(
    *,
    result: Any,
    remote_store: RemoteArtifactStore,
    scratch_dir: Path,
) -> Any:
    """Rewrite remote-reference dicts in an encoded result into local values.

    Called on the client side after the remote worker returns. Downloads
    each referenced artifact into ``scratch_dir`` and replaces the remote
    reference with a regular encoded ``file`` / ``folder`` value, so that
    the evaluator's normal :func:`decode_value` pass works unchanged.

    Parameters
    ----------
    result : Any
        Encoded result payload from the remote worker.
    remote_store : RemoteArtifactStore
        Local-backed remote store used to fetch artifacts.
    scratch_dir : Path
        Directory into which hydrated outputs are materialised.
    """
    scratch_dir.mkdir(parents=True, exist_ok=True)
    return _hydrate_encoded_value(value=result, remote_store=remote_store, scratch_dir=scratch_dir)


def _hydrate_encoded_value(
    *, value: Any, remote_store: RemoteArtifactStore, scratch_dir: Path
) -> Any:
    """Walk an encoded value tree, materialising remote references locally."""
    if not isinstance(value, dict):
        return value

    kind = value.get("__ginkgo_type__")
    if kind == _REMOTE_FILE_TAG:
        local_path = _materialize_remote_output(
            ref=value, remote_store=remote_store, scratch_dir=scratch_dir
        )
        return {"__ginkgo_type__": "file", "value": str(local_path)}
    if kind == _REMOTE_FOLDER_TAG:
        local_path = _materialize_remote_output(
            ref=value, remote_store=remote_store, scratch_dir=scratch_dir
        )
        return {"__ginkgo_type__": "folder", "value": str(local_path)}
    if kind in {"list", "tuple"}:
        return {
            **value,
            "items": [
                _hydrate_encoded_value(
                    value=item, remote_store=remote_store, scratch_dir=scratch_dir
                )
                for item in value.get("items", [])
            ],
        }
    if kind == "dict":
        return {
            **value,
            "items": [
                {
                    "key": _hydrate_encoded_value(
                        value=item["key"],
                        remote_store=remote_store,
                        scratch_dir=scratch_dir,
                    ),
                    "value": _hydrate_encoded_value(
                        value=item["value"],
                        remote_store=remote_store,
                        scratch_dir=scratch_dir,
                    ),
                }
                for item in value.get("items", [])
            ],
        }
    if kind == "asset_result":
        return {
            **value,
            "payload": _hydrate_encoded_value(
                value=value["payload"], remote_store=remote_store, scratch_dir=scratch_dir
            ),
        }
    return value


def _materialize_remote_output(
    *,
    ref: dict[str, Any],
    remote_store: RemoteArtifactStore,
    scratch_dir: Path,
) -> Path:
    """Download a produced artifact into the client scratch dir."""
    artifact_id = ref["artifact_id"]
    original = Path(ref.get("path", artifact_id))
    dest = scratch_dir / artifact_id / original.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        remote_store.retrieve(artifact_id=artifact_id, dest_path=dest)
    return dest


def build_worker_remote_store(
    *,
    scheme: str,
    bucket: str,
    prefix: str,
    local_root: Path,
) -> RemoteArtifactStore:
    """Construct a :class:`RemoteArtifactStore` inside a remote worker.

    The local CAS component is rooted in an ephemeral pod directory, since
    the worker has no pre-existing local store. Its index is in-memory: the
    worker has no workspace database, the pod's disk goes away with the pod,
    and a file there would be rows nothing will ever read.
    """
    from ginkgo.remote.resolve import resolve_backend
    from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
    from ginkgo.runtime.caching.index import CacheIndex

    backend = resolve_backend(scheme)
    local_root.mkdir(parents=True, exist_ok=True)
    local = LocalArtifactStore(root=local_root, index=CacheIndex.in_memory())
    return RemoteArtifactStore(
        local=local,
        backend=backend,
        bucket=bucket,
        prefix=prefix,
        scheme=scheme,
    )
