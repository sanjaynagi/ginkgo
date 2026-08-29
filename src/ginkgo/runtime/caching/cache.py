"""Content-addressed cache support for Ginkgo."""

from __future__ import annotations


import json
import os
import shutil
import socket
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, get_args, get_origin

from ginkgo.core.asset import AssetRef
from ginkgo.core.remote import RemoteRef
from ginkgo.core.secret import SecretRef
from ginkgo.core.task import TaskDef
from ginkgo.core.types import (
    annotation_includes,
    file,
    folder,
    is_path_shaped_annotation,
    pair_elements_with_annotations,
    require_path_value,
    tmp_dir,
    unwrap_optional_annotation,
)
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.core.hashing import hash_bytes, hash_directory, hash_file, hash_str
from ginkgo.formatting import now_iso
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.jsonio import dumps_or_none
from ginkgo.runtime.environment.secrets import redact_value, secret_identity
from ginkgo.runtime.artifacts.value_codec import (
    decode_value,
    encode_value,
    hash_value_bytes,
    summarise_value,
)
from ginkgo.workspace_layout import WorkspaceLayout

MISSING = object()


def key_components(meta: dict[str, Any]) -> dict[str, Any]:
    """Split an entry's recorded facts into the labelled components of its key.

    The names mirror the payload :meth:`CacheStore.build_cache_key` hashes, one
    per independent fact, with an ``inputs.<parameter>`` component per input, so
    a diff of two entries can name the component that moved (issue #223). This
    is the one place those labels are written down: ``save`` stores the rows
    ``cache explain`` diffs, so the two cannot drift apart.
    """
    # The payload calls the task's name "task"; the entry calls it "function".
    components: dict[str, Any] = {"task": meta.get("function")}
    for name in ("version", "source_hash", "extra_source_hash", "env"):
        components[name] = meta.get(name)

    env_hash = meta.get("env_hash")
    components["env_hash.pixi_lock"] = (
        env_hash.get("pixi_lock") if isinstance(env_hash, dict) else None
    )

    input_hashes = meta.get("input_hashes")
    if isinstance(input_hashes, dict):
        components.update({f"inputs.{name}": value for name, value in input_hashes.items()})
    return components


class UnresolvedEnvIdentityError(RuntimeError):
    """Raised when a backend cannot identify an environment a task declares."""

    def __init__(self, *, env: str, backend: Any) -> None:
        super().__init__(
            f"{type(backend).__name__} returned no identity for env {env!r}. "
            "A cache key cannot be built from an unresolved environment identity: "
            "it would change as soon as the identity resolved, re-running the task. "
            "env_identity must be a function of the declared environment, knowable "
            "before the environment is materialised."
        )


@dataclass(kw_only=True)
class CacheStore:
    """Persistent on-disk cache for resolved task results.

    Parameters
    ----------
    root : Path | None
        Cache root directory. Defaults to ``.ginkgo/cache`` under the current
        working directory.
    backend : ExecutionEnvironment | None
        Execution environment used to resolve per-environment identity hashes.
        When ``None``, a declared ``env=`` contributes only its own name to the
        key (library use and tests, where no environment is supplied).
    artifact_store : LocalArtifactStore | None
        Shared artifact store for content-addressed binary and file/folder
        artifacts.  Created automatically when ``None``.
    publisher : RemotePublisher | None
        Optional remote publisher for uploading artifacts after local storage.
        When set, artifacts are published to the remote store automatically.
    index : CacheIndex
        The database rows that index the entries. Required, and never opened
        here: which database this is, and whether it may be written, is the
        caller's decision — constructing a cache must not create one.
    """

    index: CacheIndex
    root: Path | None = None
    backend: Any | None = None  # ExecutionEnvironment; typed as Any to avoid circular import
    artifact_store: LocalArtifactStore | None = None
    publisher: Any | None = None  # RemotePublisher; typed as Any to avoid circular import
    hash_memo: HashMemo | None = None
    trust_mtimes: bool = False
    _root: Path = field(init=False, repr=False)
    _artifact_store: LocalArtifactStore = field(init=False, repr=False)
    _seen_env_materializations: set[tuple[str, str]] = field(
        default_factory=set, init=False, repr=False
    )

    def __post_init__(self) -> None:
        root = self.root if self.root is not None else WorkspaceLayout.for_cwd().cache
        object.__setattr__(self, "_root", Path(root))
        self._root.mkdir(parents=True, exist_ok=True)

        if self.artifact_store is not None:
            object.__setattr__(self, "_artifact_store", self.artifact_store)
        else:
            # Default: sibling directory to the cache root.
            object.__setattr__(
                self,
                "_artifact_store",
                LocalArtifactStore(
                    root=WorkspaceLayout.sibling_of(self._root).artifacts,
                    hash_memo=self.hash_memo,
                    index=self.index,
                ),
            )

    def build_cache_key(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        extra_source_hash: str | None = None,
        known_digests: dict[str, str] | None = None,
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
        known_digests : dict[str, str] | None
            Pre-computed content digests for managed file outputs from
            upstream tasks, keyed by resolved absolute path.  When present,
            file inputs whose path appears here skip disk hashing entirely.
        """
        input_hashes: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            input_hashes[name] = self._hash_value(
                annotation=annotation,
                value=resolved_args[name],
                known_digests=known_digests,
                label=f"{task_def.name}.{name}",
            )

        env_hash = self._env_hash(task_def=task_def)

        # Combine wrapper source hash with optional extra (notebook/script) source hash.
        source_hash = task_def.cache_source_hash
        if extra_source_hash is not None:
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

    def load(self, *, cache_key: str, task_def: TaskDef) -> Any:
        """Load a cached result if present and still valid for its environment."""
        entry_dir = self._entry_dir(cache_key)
        output_path = entry_dir / "output.json"
        if not output_path.exists():
            return MISSING

        if not self._env_materialization_matches(cache_key=cache_key, task_def=task_def):
            return MISSING

        return decode_value(
            json.loads(output_path.read_text(encoding="utf-8")),
            base_dir=entry_dir,
            artifact_store=self._artifact_store,
        )

    def has_entry(self, *, cache_key: str, task_def: TaskDef) -> bool:
        """Return whether a usable cache entry exists for the given key.

        Read-only existence check: it does not decode or materialise the
        cached value. Used by the ``--dry-run`` plan preview to predict
        cache hits without running tasks.

        Parameters
        ----------
        cache_key : str
            The cache key to probe.
        task_def : TaskDef
            The task the key belongs to, so a declared environment that has
            drifted since the entry was written counts as no entry.

        Returns
        -------
        bool
            ``True`` if a stored output exists for the key.
        """
        if not (self._entry_dir(cache_key) / "output.json").is_file():
            return False
        return self._env_materialization_matches(cache_key=cache_key, task_def=task_def)

    def save(
        self,
        *,
        cache_key: str,
        result: Any,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        input_hashes: dict[str, Any],
        extra_source_hash: str | None = None,
        extra_meta: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> dict[str, str]:
        """Atomically persist a task result and index it.

        File and folder outputs are copied into the artifact store, while the
        working-tree materialization is left in place as writable content.

        Parameters
        ----------
        extra_source_hash : str | None
            The notebook or script source hash folded into the cache key for
            driver tasks, recorded so `ginkgo cache explain` can name it as
            the component that moved.
        extra_meta : dict[str, Any] | None
            Optional task-kind-specific metadata to persist alongside the
            cache entry. Stored in the entry's ``extra`` column and
            retrievable via :meth:`load_extra_meta`.
        run_id : str | None
            The run saving the entry, recorded on the row.

        Returns
        -------
        dict[str, str]
            Mapping from output path strings to artifact IDs.
        """
        # Always store output artifacts, even if the cache entry already exists.
        records = self._store_output_artifacts(result=result, task_def=task_def)

        # Publish artifacts to remote store if a publisher is configured.
        if self.publisher is not None:
            records = self._publish_artifacts(records)
        artifact_ids = {path: record.artifact_id for path, record in records.items()}

        entry_dir = self._entry_dir(cache_key)
        if not entry_dir.exists():
            temp_dir = Path(tempfile.mkdtemp(prefix=f"{cache_key}.tmp-", dir=self._root))
            try:
                (temp_dir / "output.json").write_text(
                    json.dumps(
                        encode_value(
                            result, base_dir=temp_dir, artifact_store=self._artifact_store
                        ),
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                try:
                    os.replace(temp_dir, entry_dir)
                except FileExistsError:
                    pass
            finally:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)

        # Measured from disk rather than from what was just written, so a
        # re-save over bytes another run wrote records their size, not zero.
        output_path = entry_dir / "output.json"
        size_bytes = output_path.stat().st_size if output_path.is_file() else 0

        materialized_digest = self._materialized_digest(task_def=task_def)
        self._record_env_materialization(task_def=task_def, digest=materialized_digest)

        meta = {
            "cache_key": cache_key,
            "env": task_def.env,
            "env_hash": self._env_hash(task_def=task_def),
            "env_materialized_digest": materialized_digest,
            "extra": extra_meta,
            "extra_source_hash": extra_source_hash,
            "function": task_def.name,
            "inputs": self._serialise_inputs(task_def=task_def, resolved_args=resolved_args),
            "input_hashes": input_hashes,
            "source_hash": task_def.cache_source_hash,
            "created_at": now_iso(),
            "version": task_def.version,
        }
        self.index.record_entry(
            cache_key=cache_key,
            meta=meta,
            artifact_ids=artifact_ids,
            size_bytes=size_bytes,
            run_id=run_id,
        )
        return artifact_ids

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
        artifact_ids = self.load_artifact_ids(cache_key=cache_key)
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

        # An absent optional output has nothing to restore, and its absence is
        # itself the cached result. A None the annotation does not admit is not
        # this walk's to diagnose — validation already rejected it — but the
        # three cache walks agree on the question they ask.
        annotation, admits_none = unwrap_optional_annotation(annotation)
        if value is None:
            return admits_none or not is_path_shaped_annotation(annotation)

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            for item_annotation, item in pair_elements_with_annotations(
                annotation=annotation, value=value
            ):
                if not self._validate_output_value(
                    annotation=item_annotation,
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

    def load_extra_meta(self, *, cache_key: str) -> dict[str, Any] | None:
        """Return task-kind-specific metadata persisted with a cache entry.

        Parameters
        ----------
        cache_key : str
            The content-addressed cache key.

        Returns
        -------
        dict[str, Any] | None
            The dict previously passed as ``extra_meta`` to :meth:`save`,
            or ``None`` when the entry is missing or recorded no extras.
        """
        entry = self.index.entry(cache_key)
        return entry.extra if entry is not None else None

    def load_artifact_ids(self, *, cache_key: str) -> dict[str, str] | None:
        """Return output-path to artifact-ID mappings for one cache entry.

        Parameters
        ----------
        cache_key : str
            The content-addressed cache key.

        Returns
        -------
        dict[str, str] | None
            Mapping of output path to artifact ID, or ``None`` when the
            index has no row for the key.
        """
        entry = self.index.entry(cache_key)
        return entry.artifact_ids if entry is not None else None

    def _store_output_artifacts(
        self,
        *,
        result: Any,
        task_def: TaskDef,
    ) -> dict[str, ArtifactRecord]:
        """Store file/folder outputs in the artifact store.

        Returns
        -------
        dict[str, ArtifactRecord]
            Mapping from output path strings to the records stored for them.
        """
        records: dict[str, ArtifactRecord] = {}
        return_annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        self._collect_output_artifacts(
            annotation=return_annotation,
            value=result,
            records=records,
        )
        return records

    def _publish_artifacts(self, records: dict[str, ArtifactRecord]) -> dict[str, ArtifactRecord]:
        """Publish stored artifacts to the remote store.

        The publisher returns the record with its ``remote_uri`` filled in,
        which is recorded so a later reader knows where the bytes went.

        Parameters
        ----------
        records : dict[str, ArtifactRecord]
            Mapping from output path strings to stored artifact records.

        Returns
        -------
        dict[str, ArtifactRecord]
            The same mapping, with published records replaced.
        """
        publisher = self.publisher
        if publisher is None:
            return records
        published: dict[str, ArtifactRecord] = {}
        for path, record in records.items():
            updated = publisher.publish(record=record)
            if updated.remote_uri != record.remote_uri:
                self.index.record_artifact(updated)
            published[path] = updated
        return published

    def _collect_output_artifacts(
        self,
        *,
        annotation: Any,
        value: Any,
        records: dict[str, ArtifactRecord],
    ) -> None:
        """Recursively walk a result value and store file/folder outputs."""
        # An absent optional output has nothing to store.
        annotation, _ = unwrap_optional_annotation(annotation)
        if value is None:
            return

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            for item_annotation, item in pair_elements_with_annotations(
                annotation=annotation, value=value
            ):
                self._collect_output_artifacts(
                    annotation=item_annotation,
                    value=item,
                    records=records,
                )
            return

        if isinstance(value, list | tuple):
            for item in value:
                self._collect_output_artifacts(
                    annotation=annotation,
                    value=item,
                    records=records,
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
                records[str(path)] = self._artifact_store.store(src_path=path)
            return

        if annotation is folder or isinstance(value, folder):
            path = Path(str(value))
            if path.is_symlink():
                return
            if path.is_dir():
                records[str(path)] = self._artifact_store.store(src_path=path)
            return

    def _entry_dir(self, cache_key: str) -> Path:
        """Return the cache directory for a given key."""
        return self._root / cache_key

    @property
    def artifact_store_view(self) -> LocalArtifactStore:
        """The artifact store these entries' outputs live in.

        Named a view because it is the store the cache was handed or built,
        not a second one: a garbage collector must delete from the same store
        the entries point at.
        """
        return self._artifact_store

    def output_path(self, cache_key: str) -> Path:
        """Return where an entry's bytes live.

        The one statement of that convention: ``db check`` asks the cache
        whether an entry's bytes are there rather than rebuilding the path.
        """
        return self._entry_dir(cache_key) / "output.json"

    def orphan_entry_dirs(self) -> list[Path]:
        """Return entry directories the index has no row for.

        A lost database leaves the bytes behind, and nothing else will ever
        look at them: the key that would find them is only in the row that is
        gone. ``ginkgo db check`` reports these and ``cache clear --orphans``
        removes them, from this one definition of what an orphan is.

        A save in flight is not an orphan. It writes into a temporary directory
        beside the entries and renames it into place, so a concurrent
        ``--orphans`` would otherwise delete the bytes out from under it.
        """
        if not self._root.exists():
            return []
        known = set(self.index.cache_keys())
        return sorted(
            entry
            for entry in self._root.iterdir()
            if entry.is_dir() and entry.name not in known and ".tmp-" not in entry.name
        )

    def integrity_problems(self) -> list[str]:
        """Return the ways the index and the bytes on disk disagree.

        The database is the only cache index, so the two can only drift by
        losing one side: an entry row whose ``output.json`` is gone is a row
        that will never hit, a directory with no row is bytes nothing can find,
        and an artifact a cache entry names but the store does not hold is a
        restore that will fail. Each is reported rather than repaired.

        Returns
        -------
        list[str]
            One sentence per problem, in the order they were checked.
        """
        problems = [
            f"cache entry {key} has a row but no output.json"
            for key in self.index.cache_keys()
            if not self.output_path(key).is_file()
        ]
        problems += [
            f"cache directory {entry.name} has no row (orphan)"
            for entry in self.orphan_entry_dirs()
        ]
        problems += [
            f"cache artifact {artifact_id} is missing from the artifact store"
            for artifact_id in self.index.cache_artifact_ids()
            if not self._artifact_store.exists(artifact_id=artifact_id)
            or not self._artifact_store.artifact_path(artifact_id=artifact_id).exists()
        ]
        return problems

    def _env_hash(self, *, task_def: TaskDef) -> dict[str, Any] | None:
        """Return environment identity information for cache-keying.

        The key is built before the environment is materialised, so an identity
        that is only knowable afterwards would differ between the run that
        installed the environment and every run after it. A backend that cannot
        identify a declared environment is refused rather than folded in as
        ``None`` (issue #194).
        """
        if task_def.env is None:
            return None

        if self.backend is None:
            # No execution environment supplied (library use, tests): the
            # declaration is the whole of the identity available.
            identity = None
        else:
            identity = self.backend.env_identity(env=task_def.env)
            if not identity:
                raise UnresolvedEnvIdentityError(env=task_def.env, backend=self.backend)

        # Key name kept as "pixi_lock" for cache-key stability with existing entries.
        return {
            "env": task_def.env,
            "pixi_lock": identity,
        }

    def _materialized_digest(self, *, task_def: TaskDef) -> str | None:
        """Return the digest of the task's environment as materialised here.

        A pure read. Every lookup path asks for this — ``load``, ``has_entry``,
        the ``--dry-run`` preview — and a read path must not open a write
        transaction, so recording is :meth:`save`'s job.
        """
        if task_def.env is None or self.backend is None:
            return None
        return self.backend.materialized_digest(env=task_def.env)

    def _record_env_materialization(self, *, task_def: TaskDef, digest: str | None) -> None:
        """Record how *task_def*'s declared environment materialised on this host.

        Called from :meth:`save`, which is the one place with both the digest
        and a write in hand. Memoised per process: an entry is saved per task
        and the answer does not move mid-run.
        """
        if digest is None:
            return
        env_hash = dumps_or_none(self._env_hash(task_def=task_def))
        if env_hash is None or (env_hash, digest) in self._seen_env_materializations:
            return
        self._seen_env_materializations.add((env_hash, digest))
        self.index.record_env_materialization(
            env_hash=env_hash, host=socket.gethostname(), materialized_digest=digest
        )

    def _env_materialization_matches(self, *, cache_key: str, task_def: TaskDef) -> bool:
        """Return whether an entry's environment still matches the local one.

        Keys name the *declared* environment, so drift the declaration does not
        record — ``pixi update`` re-solving a lock, a mutable tag repointed
        upstream — leaves the key unchanged. Each entry therefore also carries
        the digest of the environment as materialised when it was written, and a
        candidate hit is checked against the environment on this machine:

        - materialised here and different: the entry was produced against other
          dependencies, so it is a miss;
        - materialised here and the same: a genuine hit;
        - not materialised here: no local evidence either way, and establishing
          any would mean installing or pulling an environment to serve a cache
          hit, so the entry stands.

        Entries written before the digest was recorded have nothing to compare
        and stand too.
        """
        if task_def.env is None or self.backend is None:
            return True

        entry = self.index.entry(cache_key)
        recorded = entry.env_materialized_digest if entry is not None else None
        if recorded is None:
            return True

        current = self._materialized_digest(task_def=task_def)
        if current is None:
            return True

        return current == recorded

    def _hash_value(
        self,
        *,
        annotation: Any,
        value: Any,
        known_digests: dict[str, str] | None = None,
        label: str = "value",
    ) -> Any:
        """Hash a concrete value according to its declared Ginkgo type."""
        if annotation is tmp_dir:
            return None

        # An absent optional output must key differently from a present one,
        # so absence gets its own token rather than collapsing to null.
        annotation, admits_none = unwrap_optional_annotation(annotation)
        if value is None and admits_none:
            return {"type": "absent"}
        if isinstance(value, AssetRef):
            if annotation_includes(annotation=annotation, expected=file):
                return {"sha256": value.content_hash, "type": "file"}
            if annotation_includes(annotation=annotation, expected=folder):
                return {"sha256": value.content_hash, "type": "folder"}
            return {
                "asset": str(value.key),
                "type": "asset_ref",
                "version_id": value.version_id,
            }
        if isinstance(value, RemoteRef):
            if value.version_id is None:
                raise ValueError(
                    "Remote inputs without version_id must be staged before cache lookup."
                )
            return {
                "bucket": value.bucket,
                "key": value.key,
                "scheme": value.scheme,
                "type": type(value).__name__,
                "version_id": value.version_id,
            }
        # Fuse-streamed inputs carry their identity in a marker dict. Hash
        # them the same as the equivalent ``RemoteRef`` so toggling
        # streaming on/off does not perturb cache keys. The ``policy``
        # field is deliberately excluded.
        from ginkgo.remote.access.protocol import FUSE_FILE_TYPE, FUSE_FOLDER_TYPE

        if isinstance(value, dict) and value.get("__ginkgo_type__") in {
            FUSE_FILE_TYPE,
            FUSE_FOLDER_TYPE,
        }:
            type_name = (
                "RemoteFileRef"
                if value["__ginkgo_type__"] == FUSE_FILE_TYPE
                else "RemoteFolderRef"
            )
            return {
                "bucket": value["bucket"],
                "key": value["key"],
                "scheme": value["scheme"],
                "type": type_name,
                "version_id": value.get("version_id"),
            }
        if isinstance(value, SecretRef):
            return secret_identity(value)

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            return {
                "items": [
                    self._hash_value(
                        annotation=item_annotation,
                        value=item,
                        known_digests=known_digests,
                        label=label,
                    )
                    for item_annotation, item in pair_elements_with_annotations(
                        annotation=annotation, value=value
                    )
                ],
                "type": origin.__name__,
            }

        if origin is dict:
            key_annotation, value_annotation = self._dict_annotations(annotation)
            return {
                "items": [
                    {
                        "key": self._hash_value(
                            annotation=key_annotation,
                            value=key,
                            known_digests=known_digests,
                            label=label,
                        ),
                        "value": self._hash_value(
                            annotation=value_annotation,
                            value=item,
                            known_digests=known_digests,
                            label=label,
                        ),
                    }
                    for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
                ],
                "type": "dict",
            }

        if isinstance(value, list):
            return {
                "items": [
                    self._hash_value(
                        annotation=annotation,
                        value=item,
                        known_digests=known_digests,
                        label=label,
                    )
                    for item in value
                ],
                "type": "list",
            }

        if isinstance(value, tuple):
            return {
                "items": [
                    self._hash_value(
                        annotation=annotation,
                        value=item,
                        known_digests=known_digests,
                        label=label,
                    )
                    for item in value
                ],
                "type": "tuple",
            }

        if annotation_includes(annotation=annotation, expected=file) or isinstance(value, file):
            require_path_value(value=value, annotation_label="file", label=label)
            # Use pre-computed digest from upstream task output when available.
            if known_digests is not None:
                resolved_key = str(Path(str(value)).resolve())
                known = known_digests.get(resolved_key)
                if known is not None:
                    return {"sha256": known, "type": "file"}
            return {"sha256": self._hash_file_contents(Path(str(value))), "type": "file"}

        if annotation_includes(annotation=annotation, expected=folder) or isinstance(
            value, folder
        ):
            require_path_value(value=value, annotation_label="folder", label=label)
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
        store content transparently.  Uses run-scoped memoization when
        available.
        """
        if self.hash_memo is not None:
            return self.hash_memo.hash_file(path)
        return hash_file(path)

    def _hash_folder_contents(self, path: Path) -> str:
        """Return the BLAKE3 digest of a folder's recursive contents.

        Uses run-scoped memoization when available.
        """
        if self.hash_memo is not None:
            return self.hash_memo.hash_directory(path)
        return hash_directory(path)

    def stat_fingerprint(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        extra_source_hash: str | None = None,
    ) -> str:
        """Build a stat-based fingerprint for ``--trust-mtimes`` mode.

        Uses file/folder stat metadata instead of content hashes to build
        a fast cache-key surrogate.

        Parameters
        ----------
        task_def : TaskDef
            The task definition.
        resolved_args : dict[str, Any]
            Resolved input argument values.
        extra_source_hash : str | None
            Additional source hash (notebook/script).

        Returns
        -------
        str
            Hex-encoded BLAKE3 digest of the stat-based payload.
        """
        stat_parts: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            stat_parts[name] = self._stat_value(
                annotation=annotation,
                value=resolved_args[name],
                label=f"{task_def.name}.{name}",
            )

        source_hash = task_def.cache_source_hash
        if extra_source_hash is not None:
            source_hash = hash_str(f"{source_hash}:{extra_source_hash}")

        payload = {
            "inputs": stat_parts,
            "source_hash": source_hash,
            "task": task_def.name,
            "version": task_def.version,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hash_bytes(encoded)

    def _stat_value(self, *, annotation: Any, value: Any, label: str = "value") -> Any:
        """Build a stat-based representation for a value (no content reading)."""
        if annotation is tmp_dir:
            return None

        if isinstance(value, RemoteRef):
            if value.version_id is None:
                return {
                    "bucket": value.bucket,
                    "key": value.key,
                    "scheme": value.scheme,
                    "type": type(value).__name__,
                    "unversioned": True,
                }
            return {
                "bucket": value.bucket,
                "key": value.key,
                "scheme": value.scheme,
                "type": type(value).__name__,
                "version_id": value.version_id,
            }

        # Fuse marker dicts stand in for a RemoteRef at stat-index time.
        from ginkgo.remote.access.protocol import FUSE_FILE_TYPE, FUSE_FOLDER_TYPE

        if isinstance(value, dict) and value.get("__ginkgo_type__") in {
            FUSE_FILE_TYPE,
            FUSE_FOLDER_TYPE,
        }:
            type_name = (
                "RemoteFileRef"
                if value["__ginkgo_type__"] == FUSE_FILE_TYPE
                else "RemoteFolderRef"
            )
            stat_entry = {
                "bucket": value["bucket"],
                "key": value["key"],
                "scheme": value["scheme"],
                "type": type_name,
            }
            if value.get("version_id") is None:
                stat_entry["unversioned"] = True
            else:
                stat_entry["version_id"] = value["version_id"]
            return stat_entry

        if annotation is file or isinstance(value, file):
            path = Path(str(value)).resolve()
            if path.is_file():
                st = path.stat()
                return {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "type": "file"}
            return {"type": "file", "missing": True}

        if annotation is folder or isinstance(value, folder):
            path = Path(str(value)).resolve()
            if path.is_dir():
                parts: list[str] = []
                for child in sorted(
                    path.rglob("*"),
                    key=lambda p: str(p.relative_to(path)),
                ):
                    rel = child.relative_to(path).as_posix()
                    if child.is_dir():
                        parts.append(f"D:{rel}")
                    else:
                        st = child.stat()
                        parts.append(f"F:{rel}:{st.st_size}:{st.st_mtime_ns}")
                return {"fingerprint": hash_str("\n".join(parts)), "type": "folder"}
            return {"type": "folder", "missing": True}

        # For non-path types, use the same hash as the content-addressed path.
        return self._hash_value(annotation=annotation, value=value, label=label)

    def _dict_annotations(self, annotation: Any) -> tuple[Any, Any]:
        """Extract key and value annotations for a mapping annotation."""
        args = get_args(annotation)
        if len(args) == 2:
            return args[0], args[1]
        return Any, Any
