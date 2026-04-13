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

from pathlib import Path
from typing import Any, get_args, get_origin

from ginkgo.core.types import file, folder
from ginkgo.runtime.artifacts.remote_artifact_store import RemoteArtifactStore


_REMOTE_FILE_TAG = "__ginkgo_remote_file__"
_REMOTE_FOLDER_TAG = "__ginkgo_remote_folder__"


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
    published_artifacts: set[str] | None = None,
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
        whether it has been published to the remote store yet.
    published_artifacts : set[str] | None
        Optional set of artifact ids already uploaded to ``remote_store``
        during this run. Used to avoid redundant remote uploads without
        conflating local-cache state with remote-publish state.
    """
    known_digests = known_digests or {}
    if published_artifacts is None:
        published_artifacts = set()
    staged: dict[str, Any] = {}
    for name, value in args.items():
        annotation = type_hints.get(name, Any)
        staged[name] = _stage_value(
            value=value,
            annotation=annotation,
            remote_store=remote_store,
            known_digests=known_digests,
            published_artifacts=published_artifacts,
        )
    return staged


def _stage_value(
    *,
    value: Any,
    annotation: Any,
    remote_store: RemoteArtifactStore,
    known_digests: dict[str, str],
    published_artifacts: set[str],
) -> Any:
    """Stage a single argument value, recursing into typed containers."""
    # file / folder: upload content, emit a remote reference dict.
    if _annotation_matches(annotation=annotation, target=file) and isinstance(value, str):
        return _stage_path(
            path=Path(value),
            tag=_REMOTE_FILE_TAG,
            remote_store=remote_store,
            known_digests=known_digests,
            published_artifacts=published_artifacts,
        )
    if _annotation_matches(annotation=annotation, target=folder) and isinstance(value, str):
        return _stage_path(
            path=Path(value),
            tag=_REMOTE_FOLDER_TAG,
            remote_store=remote_store,
            known_digests=known_digests,
            published_artifacts=published_artifacts,
        )

    # Recurse into typed containers.
    origin = get_origin(annotation)
    if origin in {list, tuple} and isinstance(value, (list, tuple)):
        inner_args = get_args(annotation)
        inner_annotation = inner_args[0] if inner_args else Any
        staged_items = [
            _stage_value(
                value=item,
                annotation=inner_annotation,
                remote_store=remote_store,
                known_digests=known_digests,
                published_artifacts=published_artifacts,
            )
            for item in value
        ]
        return list(staged_items) if origin is list else tuple(staged_items)

    if origin is dict and isinstance(value, dict):
        dict_args = get_args(annotation)
        value_annotation = dict_args[1] if len(dict_args) == 2 else Any
        return {
            key: _stage_value(
                value=item,
                annotation=value_annotation,
                remote_store=remote_store,
                known_digests=known_digests,
                published_artifacts=published_artifacts,
            )
            for key, item in value.items()
        }

    return value


def _stage_path(
    *,
    path: Path,
    tag: str,
    remote_store: RemoteArtifactStore,
    known_digests: dict[str, str],
    published_artifacts: set[str],
) -> dict[str, str]:
    """Upload a single path and return its remote-reference dict.

    ``known_digests`` records path → artifact-id resolved from the local
    store. A hit there guarantees only that the artifact exists locally,
    not that it has been published to the remote store — the two caches
    must stay separate.
    """
    resolved = path.resolve()
    key = str(resolved)
    artifact_id = known_digests.get(key)
    if artifact_id is None or artifact_id not in published_artifacts:
        record = remote_store.store(src_path=resolved)
        artifact_id = record.artifact_id
        known_digests[key] = artifact_id
        published_artifacts.add(artifact_id)
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
    """Download one artifact into the scratch dir and return its path."""
    artifact_id = ref["artifact_id"]
    # Use the original basename so code that inspects extensions keeps working.
    original = Path(ref.get("path", artifact_id))
    dest = scratch_dir / artifact_id / original.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        remote_store.retrieve(artifact_id=artifact_id, dest_path=dest)
    return wrap(str(dest))


def stage_result_for_remote(
    *,
    result: Any,
    remote_store: RemoteArtifactStore,
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
    """
    return _stage_encoded_value(value=result, remote_store=remote_store)


def _stage_encoded_value(*, value: Any, remote_store: RemoteArtifactStore) -> Any:
    """Walk an encoded value tree, uploading file/folder leaves to remote."""
    if not isinstance(value, dict):
        return value

    kind = value.get("__ginkgo_type__")
    if kind == "file":
        return _stage_encoded_path(
            path_str=value["value"], tag=_REMOTE_FILE_TAG, remote_store=remote_store
        )
    if kind == "folder":
        return _stage_encoded_path(
            path_str=value["value"], tag=_REMOTE_FOLDER_TAG, remote_store=remote_store
        )
    if kind in {"list", "tuple"}:
        return {
            **value,
            "items": [
                _stage_encoded_value(value=item, remote_store=remote_store)
                for item in value.get("items", [])
            ],
        }
    if kind == "dict":
        return {
            **value,
            "items": [
                {
                    "key": _stage_encoded_value(value=item["key"], remote_store=remote_store),
                    "value": _stage_encoded_value(value=item["value"], remote_store=remote_store),
                }
                for item in value.get("items", [])
            ],
        }
    if kind == "asset_result":
        return {
            **value,
            "value": _stage_encoded_value(value=value["value"], remote_store=remote_store),
        }
    return value


def _stage_encoded_path(
    *, path_str: str, tag: str, remote_store: RemoteArtifactStore
) -> dict[str, Any]:
    """Upload a single pod-local path and return a remote-reference dict."""
    resolved = Path(path_str).resolve()
    record = remote_store.store(src_path=resolved)
    return {
        "__ginkgo_type__": tag,
        "artifact_id": record.artifact_id,
        "path": str(resolved),
    }


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
            "value": _hydrate_encoded_value(
                value=value["value"], remote_store=remote_store, scratch_dir=scratch_dir
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
    the worker has no pre-existing local store.
    """
    from ginkgo.remote.resolve import resolve_backend
    from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore

    backend = resolve_backend(scheme)
    local_root.mkdir(parents=True, exist_ok=True)
    local = LocalArtifactStore(root=local_root)
    return RemoteArtifactStore(
        local=local,
        backend=backend,
        bucket=bucket,
        prefix=prefix,
        scheme=scheme,
    )
