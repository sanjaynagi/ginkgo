"""Content-addressed cache support for Ginkgo."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, get_args, get_origin

from ginkgo.core.asset import AssetRef
from ginkgo.core.secret import SecretRef
from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.artifact_model import ArtifactRecord
from ginkgo.runtime.artifact_store import LocalArtifactStore
from ginkgo.runtime.hashing import hash_bytes, hash_directory, hash_file, hash_str
from ginkgo.runtime.secrets import redact_value, secret_identity
from ginkgo.runtime.value_codec import (
    decode_value,
    encode_value,
    hash_value_bytes,
    summarise_value,
)

MISSING = object()


@dataclass(kw_only=True)
class CacheStore:
    """Persistent on-disk cache for resolved task results.

    Parameters
    ----------
    root : Path | None
        Cache root directory. Defaults to ``.ginkgo/cache`` under the current
        working directory.
    backend : TaskBackend | None
        Execution backend used to resolve per-environment identity hashes.
        When ``None``, falls back to looking for a single ``pixi.lock`` in the
        current working directory (pre-Phase-5 behaviour).
    artifact_store : LocalArtifactStore | None
        Shared artifact store for content-addressed binary and file/folder
        artifacts.  Created automatically when ``None``.
    publisher : RemotePublisher | None
        Optional remote publisher for uploading artifacts after local storage.
        When set, artifacts are published to the remote store automatically.
    """

    root: Path | None = None
    backend: Any | None = None  # TaskBackend; typed as Any to avoid circular import
    artifact_store: LocalArtifactStore | None = None
    publisher: Any | None = None  # RemotePublisher; typed as Any to avoid circular import
    _root: Path = field(init=False, repr=False)
    _artifact_store: LocalArtifactStore = field(init=False, repr=False)

    def __post_init__(self) -> None:
        root = self.root if self.root is not None else Path.cwd() / ".ginkgo" / "cache"
        object.__setattr__(self, "_root", Path(root))
        self._root.mkdir(parents=True, exist_ok=True)

        if self.artifact_store is not None:
            object.__setattr__(self, "_artifact_store", self.artifact_store)
        else:
            # Default: sibling directory to the cache root.
            artifacts_root = self._root.parent / "artifacts"
            object.__setattr__(self, "_artifact_store", LocalArtifactStore(root=artifacts_root))

    def build_cache_key(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        extra_source_hash: str | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Build a stable content-addressed cache key for a task call.

        Parameters
        ----------
        task_def : TaskDef
            The task definition.
        resolved_args : dict[str, Any]
            Resolved input argument values.
        extra_source_hash : str | None
            Additional source hash to fold into the cache key. Used by
            notebook and script tasks to incorporate the source hash of the
            underlying notebook or script file, which is not known at
            decoration time.
        """
        input_hashes: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            input_hashes[name] = self._hash_value(annotation=annotation, value=resolved_args[name])

        env_hash = self._env_hash(task_def=task_def)

        # Combine wrapper source hash with optional extra (notebook/script) source hash.
        source_hash = task_def.cache_source_hash
        if extra_source_hash is not None:
            from ginkgo.runtime.hashing import hash_str

            source_hash = hash_str(f"{source_hash}:{extra_source_hash}")

        payload = {
            "env": task_def.env,
            "env_hash": env_hash,
            "inputs": input_hashes,
            "source_hash": source_hash,
            "task": task_def.name,
            "version": task_def.version,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hash_bytes(encoded), input_hashes

    def load(self, *, cache_key: str) -> Any:
        """Load a cached result if present."""
        entry_dir = self._entry_dir(cache_key)
        output_path = entry_dir / "output.json"
        if not output_path.exists():
            return MISSING

        return decode_value(
            json.loads(output_path.read_text(encoding="utf-8")),
            base_dir=entry_dir,
            artifact_store=self._artifact_store,
        )

    def save(
        self,
        *,
        cache_key: str,
        result: Any,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        input_hashes: dict[str, Any],
    ) -> None:
        """Atomically persist a task result and metadata.

        File and folder outputs are copied into the artifact store, while the
        working-tree materialization is left in place as writable content.
        """
        # Always store output artifacts, even if the cache entry already exists.
        artifact_ids = self._store_output_artifacts(result=result, task_def=task_def)

        # Publish artifacts to remote store if a publisher is configured.
        if self.publisher is not None:
            self._publish_artifacts(artifact_ids)

        entry_dir = self._entry_dir(cache_key)
        if not entry_dir.exists():
            temp_dir = Path(tempfile.mkdtemp(prefix=f"{cache_key}.tmp-", dir=self._root))
            try:
                output_path = temp_dir / "output.json"
                output_path.write_text(
                    json.dumps(
                        encode_value(
                            result, base_dir=temp_dir, artifact_store=self._artifact_store
                        ),
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                meta = {
                    "artifact_ids": artifact_ids,
                    "cache_key": cache_key,
                    "env": task_def.env,
                    "function": task_def.name,
                    "inputs": self._serialise_inputs(
                        task_def=task_def, resolved_args=resolved_args
                    ),
                    "input_hashes": input_hashes,
                    "source_hash": task_def.cache_source_hash,
                    "timestamp": datetime.now(UTC).isoformat(),
                    "version": task_def.version,
                }
                (temp_dir / "meta.json").write_text(
                    json.dumps(meta, indent=2, sort_keys=True),
                    encoding="utf-8",
                )

                try:
                    os.replace(temp_dir, entry_dir)
                except FileExistsError:
                    pass
            finally:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)

    def validate_cached_outputs(self, *, cache_key: str, task_def: TaskDef, value: Any) -> bool:
        """Ensure cached file and folder outputs are materialized correctly.

        Returns
        -------
        bool
            ``True`` when all managed outputs either already match their cached
            artifact content or were successfully restored from the artifact
            store. ``False`` if the cached artifact metadata is incomplete or
            a restore fails.
        """
        return_annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        artifact_ids = self._load_artifact_ids(cache_key=cache_key)
        if artifact_ids is None:
            return False
        return self._validate_output_value(
            annotation=return_annotation,
            value=value,
            artifact_ids=artifact_ids,
        )

    def _validate_output_value(
        self,
        *,
        annotation: Any,
        value: Any,
        artifact_ids: dict[str, str],
    ) -> bool:
        """Recursively validate or restore managed file and folder outputs."""
        if isinstance(value, AssetRef):
            return Path(value.artifact_path).exists()

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            for item in value:
                if not self._validate_output_value(
                    annotation=inner_annotation,
                    value=item,
                    artifact_ids=artifact_ids,
                ):
                    return False
            return True

        if isinstance(value, list | tuple):
            for item in value:
                if not self._validate_output_value(
                    annotation=annotation,
                    value=item,
                    artifact_ids=artifact_ids,
                ):
                    return False
            return True

        if annotation is file or isinstance(value, file):
            return self._validate_file_output(Path(str(value)), artifact_ids=artifact_ids)

        if annotation is folder or isinstance(value, folder):
            return self._validate_folder_output(Path(str(value)), artifact_ids=artifact_ids)

        # Non-path types: no output materialization needed.
        return True

    def _validate_file_output(self, path: Path, *, artifact_ids: dict[str, str]) -> bool:
        """Ensure one managed file output matches its cached artifact."""
        artifact_id = artifact_ids.get(str(path))
        if artifact_id is None or not self._artifact_store.exists(artifact_id=artifact_id):
            return False
        if self._artifact_store.matches(artifact_id=artifact_id, path=path):
            return True
        self._artifact_store.restore(artifact_id=artifact_id, dest_path=path)
        return self._artifact_store.matches(artifact_id=artifact_id, path=path)

    def _validate_folder_output(self, path: Path, *, artifact_ids: dict[str, str]) -> bool:
        """Ensure one managed folder output matches its cached artifact."""
        artifact_id = artifact_ids.get(str(path))
        if artifact_id is None or not self._artifact_store.exists(artifact_id=artifact_id):
            return False
        if self._artifact_store.matches(artifact_id=artifact_id, path=path):
            return True
        self._artifact_store.restore(artifact_id=artifact_id, dest_path=path)
        return self._artifact_store.matches(artifact_id=artifact_id, path=path)

    def _load_artifact_ids(self, *, cache_key: str) -> dict[str, str] | None:
        """Load output artifact mappings for one cache entry."""
        meta_path = self._entry_dir(cache_key) / "meta.json"
        if not meta_path.exists():
            return None
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
        artifact_ids = meta.get("artifact_ids")
        if not isinstance(artifact_ids, dict):
            return None
        return {
            str(path): str(artifact_id)
            for path, artifact_id in artifact_ids.items()
            if isinstance(path, str) and isinstance(artifact_id, str)
        }

    def _store_output_artifacts(
        self,
        *,
        result: Any,
        task_def: TaskDef,
    ) -> dict[str, str]:
        """Store file/folder outputs in the artifact store.

        Returns
        -------
        dict[str, str]
            Mapping from output path strings to artifact IDs.
        """
        artifact_ids: dict[str, str] = {}
        return_annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        self._collect_output_artifacts(
            annotation=return_annotation,
            value=result,
            artifact_ids=artifact_ids,
        )
        return artifact_ids

    def _publish_artifacts(self, artifact_ids: dict[str, str]) -> None:
        """Publish stored artifacts to the remote store.

        Loads each artifact record from the refs directory and publishes it
        via the configured publisher.

        Parameters
        ----------
        artifact_ids : dict[str, str]
            Mapping from output path strings to artifact IDs.
        """
        refs_dir = self._artifact_store._refs_dir
        for artifact_id in artifact_ids.values():
            ref_path = refs_dir / f"{artifact_id}.json"
            if ref_path.exists():
                record = ArtifactRecord.from_path(ref_path)
                self.publisher.publish(record=record)

    def _collect_output_artifacts(
        self,
        *,
        annotation: Any,
        value: Any,
        artifact_ids: dict[str, str],
    ) -> None:
        """Recursively walk a result value and store file/folder outputs."""
        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            for item in value:
                self._collect_output_artifacts(
                    annotation=inner_annotation,
                    value=item,
                    artifact_ids=artifact_ids,
                )
            return

        if isinstance(value, list | tuple):
            for item in value:
                self._collect_output_artifacts(
                    annotation=annotation,
                    value=item,
                    artifact_ids=artifact_ids,
                )
            return

        if isinstance(value, AssetRef):
            return

        if annotation is file or isinstance(value, file):
            path = Path(str(value))
            if path.is_symlink():
                # Already a symlink (e.g. from a previous run) — skip.
                return
            if path.is_file():
                record = self._artifact_store.store(src_path=path)
                artifact_ids[str(path)] = record.artifact_id
            return

        if annotation is folder or isinstance(value, folder):
            path = Path(str(value))
            if path.is_symlink():
                return
            if path.is_dir():
                record = self._artifact_store.store(src_path=path)
                artifact_ids[str(path)] = record.artifact_id
            return

    def _entry_dir(self, cache_key: str) -> Path:
        """Return the cache directory for a given key."""
        return self._root / cache_key

    def _env_hash(self, *, task_def: TaskDef) -> dict[str, Any] | None:
        """Return environment identity information for cache-keying."""
        if task_def.env is None:
            return None

        if self.backend is not None:
            lock_digest = self.backend.env_identity(env=task_def.env)
        else:
            # Pre-Phase-5 fallback: single pixi.lock in cwd.
            pixi_lock = Path.cwd() / "pixi.lock"
            lock_digest = self._hash_file_contents(pixi_lock) if pixi_lock.is_file() else None

        # Key name kept as "pixi_lock" for cache-key stability with existing entries.
        return {
            "env": task_def.env,
            "pixi_lock": lock_digest,
        }

    def _hash_value(self, *, annotation: Any, value: Any) -> Any:
        """Hash a concrete value according to its declared Ginkgo type."""
        if annotation is tmp_dir:
            return None
        if isinstance(value, AssetRef):
            return {
                "asset": str(value.key),
                "type": "asset_ref",
                "version_id": value.version_id,
            }
        if isinstance(value, SecretRef):
            return secret_identity(value)

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            return {
                "items": [
                    self._hash_value(annotation=inner_annotation, value=item) for item in value
                ],
                "type": origin.__name__,
            }

        if origin is dict:
            key_annotation, value_annotation = self._dict_annotations(annotation)
            return {
                "items": [
                    {
                        "key": self._hash_value(annotation=key_annotation, value=key),
                        "value": self._hash_value(annotation=value_annotation, value=item),
                    }
                    for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
                ],
                "type": "dict",
            }

        if isinstance(value, list):
            return {
                "items": [self._hash_value(annotation=annotation, value=item) for item in value],
                "type": "list",
            }

        if isinstance(value, tuple):
            return {
                "items": [self._hash_value(annotation=annotation, value=item) for item in value],
                "type": "tuple",
            }

        if annotation is file or isinstance(value, file):
            return {"sha256": self._hash_file_contents(Path(str(value))), "type": "file"}

        if annotation is folder or isinstance(value, folder):
            return {"sha256": self._hash_folder_contents(Path(str(value))), "type": "folder"}

        if isinstance(value, dict):
            return {
                "items": [
                    {
                        "key": self._hash_value(annotation=Any, value=key),
                        "value": self._hash_value(annotation=Any, value=item),
                    }
                    for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
                ],
                "type": "dict",
            }

        if value is None or isinstance(value, (bool, int, float, str)):
            return {
                "sha256": hash_str(repr(value)),
                "type": type(value).__name__,
            }

        codec_name, digest = hash_value_bytes(value)
        return {
            "codec": codec_name,
            "sha256": digest,
            "type": f"{type(value).__module__}.{type(value).__name__}",
        }

    def _serialise_inputs(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Serialize resolved inputs for metadata output."""
        inputs: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            value = redact_value(resolved_args[name])
            inputs[name] = summarise_value(value)
        return inputs

    def _hash_file_contents(self, path: Path) -> str:
        """Return the BLAKE3 digest of a file's contents.

        Follows symlinks so that hashing a symlinked output reads the artifact
        store content transparently.
        """
        return hash_file(path)

    def _hash_folder_contents(self, path: Path) -> str:
        """Return the BLAKE3 digest of a folder's recursive contents."""
        return hash_directory(path)

    def _dict_annotations(self, annotation: Any) -> tuple[Any, Any]:
        """Extract key and value annotations for a mapping annotation."""
        args = get_args(annotation)
        if len(args) == 2:
            return args[0], args[1]
        return Any, Any
