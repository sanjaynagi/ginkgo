"""Content-addressed cache support for Ginkgo."""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, get_args, get_origin

from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.core.remote import RemoteRef
from ginkgo.core.secret import SecretRef
from ginkgo.core.task import TaskDef
from ginkgo.core.types import (
    PLAIN_SCALAR_TYPES,
    annotation_includes,
    file,
    folder,
    holds_only_plain_scalars,
    is_path_shaped_annotation,
    is_untracked_annotation,
    looks_like_path_string,
    pair_elements_with_annotations,
    require_path_value,
    tmp_dir,
    unwrap_optional_annotation,
)
from ginkgo.runtime.artifacts.artifact_model import ArtifactRecord
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.caching.hash_memo import HashMemo
from ginkgo.runtime.task_validation import (
    declared_output_paths,
    is_content_trackable_path_shape,
    is_content_trackable_path_value,
    label_input_value,
)
from ginkgo.core.hashing import hash_bytes, hash_directory, hash_file, hash_str
from ginkgo.formatting import now_iso
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.store.jsonio import dumps_or_none
from ginkgo.runtime.environment.secrets import redact_value, secret_identity
from ginkgo.runtime.artifacts.value_codec import (
    check_encoded,
    decode_value,
    encode_value,
    encoded_artifact_ids,
    hash_value_bytes,
    summarise_value,
)
from ginkgo.workspace_layout import WorkspaceLayout


logger = logging.getLogger(__name__)

_UNREADABLE_ENTRY_ERRORS = (OSError, ValueError, KeyError, TypeError, ImportError, AttributeError)
"""What reading a cache entry raises when the entry cannot be used.

``TypeError`` covers the codec's own refusal; the import and attribute errors
come from unpickling a value whose task or class has been renamed or removed
since it was stored.
"""

_KEY_FORMAT = 2
"""Version of what a cache entry holds, folded into every key.

Bumped when an entry written by an older version can no longer be trusted, so
those entries miss once instead. Format 2 stores a dynamic task's returned
expressions rather than the value they resolved to.
"""

MISSING = object()


@dataclass(frozen=True)
class DynamicTemplate:
    """The task expressions a dynamic task returned, as its cache entry holds them.

    A task that returns task expressions is complete only once they have run.
    Its entry keeps the expressions rather than the value they resolved to, so
    a hit replays them and each child is looked up in the cache on its own:
    a child whose code or inputs changed re-runs, even though the parent's
    key did not move. The template is pickled whole, so an expression shared
    between two places in it stays one task.

    Parameters
    ----------
    template : Any
        The returned value: an expression, or a nested container holding
        them.
    """

    template: Any


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


def diff_key_components(*, current: dict[str, Any], prior: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the cache-key components that differ between two entries."""
    differences: list[dict[str, Any]] = []
    for name in sorted(set(current) | set(prior)):
        # A component absent from one side is a parameter that came or went.
        if name not in current:
            differences.append({"component": name, "status": "removed", "prior": prior[name]})
        elif name not in prior:
            differences.append({"component": name, "status": "added", "current": current[name]})
        elif current[name] != prior[name]:
            differences.append(
                {
                    "component": name,
                    "status": "changed",
                    "current": current[name],
                    "prior": prior[name],
                }
            )
    return differences


def coarse_reasons(components: list[dict[str, Any]]) -> list[str]:
    """Return the summary reason codes implied by a component diff.

    A moved component no code covers falls back to ``cache_key_changed``; the
    component list names it either way.
    """
    moved = {str(component["component"]) for component in components}
    reasons = []
    if moved & {"source_hash", "extra_source_hash"}:
        reasons.append("source_hash_changed")
    if "version" in moved:
        reasons.append("version_bump")
    if moved & {"env", "env_hash.pixi_lock"}:
        reasons.append("env_changed")
    if any(name.startswith("inputs") for name in moved):
        reasons.append("input_changed")
    return reasons or ["cache_key_changed"]


class UnresolvedEnvIdentityError(GinkgoError, RuntimeError):
    """Raised when a backend cannot identify an environment a task declares."""

    failure_kind: ClassVar[FailureKind] = "env_mismatch"

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
                label=f"{task_def.base_name}.{name}",
                is_output=name in task_def.output_params,
            )

        env_hash = self._env_hash(task_def=task_def)

        # Combine wrapper source hash with optional extra (notebook/script) source hash.
        source_hash = task_def.cache_source_hash
        if extra_source_hash is not None:
            source_hash = hash_str(f"{source_hash}:{extra_source_hash}")

        payload = {
            "env": task_def.env,
            "env_hash": env_hash,
            "format": _KEY_FORMAT,
            "inputs": input_hashes,
            "source_hash": source_hash,
            "task": task_def.name,
            "version": task_def.version,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hash_bytes(encoded), input_hashes

    def miss_reasons(
        self,
        *,
        cache_key: str,
        task_def: TaskDef,
        input_hashes: dict[str, Any],
        extra_source_hash: str | None = None,
    ) -> list[str]:
        """Return why a key built by :meth:`build_cache_key` would miss.

        The key's components are compared with whichever of the task's recent
        entries differs from them least: the newest entry of a fanned-out task
        is usually another branch, which would report inputs that only differ
        between branches. ``ginkgo cache explain`` falls back to the same choice.

        Parameters
        ----------
        cache_key : str
            The key that misses.
        task_def : TaskDef
            The task it was built for.
        input_hashes : dict[str, Any]
            The input hashes :meth:`build_cache_key` returned with it.
        extra_source_hash : str | None
            The directive source hash folded into it, if any.

        Returns
        -------
        list[str]
            ``cache explain`` reason codes: ``no_prior_entry`` when the task
            has no entry to compare with, otherwise those
            :func:`coarse_reasons` gives.
        """
        priors = self.index.recent_entries(function=task_def.name, excluding=cache_key)
        if not priors:
            return ["no_prior_entry"]
        current = key_components(
            {
                "function": task_def.name,
                "version": task_def.version,
                "source_hash": task_def.cache_source_hash,
                "extra_source_hash": extra_source_hash,
                "env": task_def.env,
                "env_hash": self._env_hash(task_def=task_def),
                "input_hashes": input_hashes,
            }
        )
        diffs = [
            diff_key_components(current=current, prior=key_components(prior.as_meta()))
            for prior in priors
        ]
        # min() keeps the first of equals, and the entries come newest first.
        return coarse_reasons(min(diffs, key=lambda diff: len(diff)))

    def label_inputs(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> dict[str, str]:
        """Return each input parameter's cache-tracking label.

        Metadata only, for every parameter :meth:`build_cache_key` considers —
        ``tmp_dir`` included, labelled ``"untracked"`` there exactly as it is
        excluded from the key — but never folded into the key itself, so
        computing or storing it cannot perturb it. ``ginkgo cache explain`` and
        the run summary read this to name *how* an input is tracked: by
        file/folder content, by a remote ref's version id, by its path string only
        (the silent-staleness trap of issues #121/#281), by its own value, by
        an ``Out[...]`` parameter's declared path (by design), or not at all.
        See :func:`~ginkgo.runtime.task_validation.label_input_value` for the
        categories.
        """
        labels: dict[str, str] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                labels[name] = "untracked"
                continue
            labels[name] = label_input_value(
                annotation=annotation,
                value=resolved_args[name],
                is_output=name in task_def.output_params,
            )
        return labels

    def load(self, *, cache_key: str, task_def: TaskDef) -> Any:
        """Load a cached result if present and still valid for its environment.

        An entry is present only when the index holds its row: the bytes
        under the cache root of a key the database no longer knows (a lost
        ``ginkgo.db``, say) are stranded, never served.
        """
        entry_dir = self._entry_dir(cache_key)
        output_path = entry_dir / "output.json"
        if not output_path.exists() or not self.index.has_entry(cache_key):
            return MISSING

        if not self._env_materialization_matches(cache_key=cache_key, task_def=task_def):
            return MISSING

        # An entry this codec cannot read — cut short, written by a codec this
        # version no longer has, or removed by a concurrent run mid-read — is a
        # miss. Loading never deletes it: a dry run loads too, and ``save``
        # replaces it once the task has actually run.
        try:
            return decode_value(
                json.loads(output_path.read_text(encoding="utf-8")),
                base_dir=entry_dir,
                artifact_store=self._artifact_store,
            )
        except _UNREADABLE_ENTRY_ERRORS as exc:
            logger.warning("Ignoring unreadable cache entry %s: %s", cache_key, exc)
            return MISSING

    @staticmethod
    def _entry_readable(entry_dir: Path) -> bool:
        """Return whether an entry's ``output.json`` is one this codec can decode."""
        try:
            check_encoded(json.loads((entry_dir / "output.json").read_text(encoding="utf-8")))
        except _UNREADABLE_ENTRY_ERRORS:
            return False
        return True

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
            ``True`` if a stored output exists for the key and the index
            holds its row, as :meth:`load` requires.
        """
        if not (self._entry_dir(cache_key) / "output.json").is_file():
            return False
        if not self.index.has_entry(cache_key):
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
        result : Any
            The task's result, or the :class:`DynamicTemplate` of a task that
            returned task expressions.
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
        # A dynamic task's returned outputs are its children's, which stored
        # their own; its declared Out[...] paths are its own.
        records = self._store_output_artifacts(
            result=result, task_def=task_def, resolved_args=resolved_args
        )

        # Publish artifacts to remote store if a publisher is configured.
        if self.publisher is not None:
            records = self._publish_artifacts(records)
        artifact_ids = {path: record.artifact_id for path, record in records.items()}

        entry_dir = self._entry_dir(cache_key)
        if entry_dir.exists() and not self._entry_readable(entry_dir):
            # An unreadable entry would otherwise outlive every re-run.
            shutil.rmtree(entry_dir, ignore_errors=True)
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
                except OSError:
                    # A concurrent run published this entry first. Renaming a
                    # directory onto a non-empty one fails with ENOTEMPTY or
                    # EEXIST depending on the platform; either way its entry
                    # holds the same result, so keep it.
                    if not entry_dir.exists():
                        raise
            finally:
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)

        # Measured from disk rather than from what was just written, so a
        # re-save over bytes another run wrote records their size, not zero.
        # A large value (a DataFrame's Parquet, say) lives in the artifact
        # store and is counted with the entry that names it.
        output_path = entry_dir / "output.json"
        size_bytes = output_path.stat().st_size if output_path.is_file() else 0
        for artifact_id in self._value_artifact_ids(entry_dir=entry_dir):
            record = self._artifact_store.load_record(artifact_id=artifact_id)
            size_bytes += record.size if record is not None else 0

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

    def validate_cached_outputs(
        self,
        *,
        cache_key: str,
        task_def: TaskDef,
        value: Any,
        resolved_args: dict[str, Any],
    ) -> bool:
        """Ensure cached file and folder outputs are materialized correctly.

        Covers the returned outputs (none for a :class:`DynamicTemplate`,
        whose children restore their own) and every declared ``Out[...]``
        path, whatever the return annotation.

        Returns
        -------
        bool
            ``True`` when all managed outputs either already match their cached
            artifact content or were successfully restored from the artifact
            store. ``False`` if the cached artifact metadata is incomplete or
            a restore fails.
        """
        artifact_ids = self.load_artifact_ids(cache_key=cache_key)
        if artifact_ids is None:
            return False
        if not isinstance(value, DynamicTemplate) and not self._validate_output_value(
            annotation=task_def.effective_return_annotation,
            value=value,
            artifact_ids=artifact_ids,
        ):
            return False
        return all(
            self._validate_output_value(
                annotation=file if kind == "file" else folder,
                value=path,
                artifact_ids=artifact_ids,
            )
            for _, path, kind in declared_output_paths(
                task_def=task_def, resolved_args=resolved_args
            )
        )

    def _validate_output_value(
        self,
        *,
        annotation: Any,
        value: Any,
        artifact_ids: dict[str, str],
    ) -> bool:
        """Recursively validate or restore managed file and folder outputs."""
        # An absent optional output has nothing to restore, and its absence is
        # itself the cached result. A None the annotation does not admit is not
        # this walk's to diagnose — validation already rejected it — but the
        # three cache walks agree on the question they ask.
        annotation, admits_none = unwrap_optional_annotation(annotation)
        if value is None:
            return admits_none or not is_path_shaped_annotation(annotation)
        if holds_only_plain_scalars(value) and not is_path_shaped_annotation(annotation):
            return True

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
            self._artifact_store.restock(artifact_id=artifact_id, path=path)
            return True
        self._artifact_store.restore(artifact_id=artifact_id, dest_path=path)
        return self._artifact_store.matches(artifact_id=artifact_id, path=path)

    def _validate_folder_output(self, path: Path, *, artifact_ids: dict[str, str]) -> bool:
        """Ensure one managed folder output matches its cached artifact."""
        artifact_id = artifact_ids.get(str(path))
        if artifact_id is None or not self._artifact_store.exists(artifact_id=artifact_id):
            return False
        if self._artifact_store.matches(artifact_id=artifact_id, path=path):
            self._artifact_store.restock(artifact_id=artifact_id, path=path)
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
        resolved_args: dict[str, Any],
    ) -> dict[str, ArtifactRecord]:
        """Store the file/folder outputs a task returned or declared ``Out[...]``.

        Every declared output is stored whatever the return annotation says,
        so a hit restores it just as it restores a returned ``file``.

        Returns
        -------
        dict[str, ArtifactRecord]
            Mapping from output path strings to the records stored for them.
        """
        records: dict[str, ArtifactRecord] = {}
        if not isinstance(result, DynamicTemplate):
            self._collect_output_artifacts(
                annotation=task_def.effective_return_annotation,
                value=result,
                records=records,
            )
        for _, path, kind in declared_output_paths(task_def=task_def, resolved_args=resolved_args):
            self._collect_output_artifacts(
                annotation=file if kind == "file" else folder, value=path, records=records
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

    def value_artifact_ids(self) -> set[str]:
        """Return every artifact an entry's encoded value names.

        These are not among the entries' ``cache_artifacts`` rows, which
        record output paths; a garbage collector must keep them too. Reads
        every ``output.json``, so it is for maintenance, not the run path.
        """
        return set().union(
            *(
                self._value_artifact_ids(entry_dir=self._entry_dir(key))
                for key in self.index.cache_keys()
            )
        )

    def _value_artifact_ids(self, *, entry_dir: Path) -> set[str]:
        """Return the artifacts one entry's encoded value names, none if unreadable."""
        try:
            encoded = json.loads((entry_dir / "output.json").read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return set()
        return encoded_artifact_ids(encoded)

    def unusable_entry_keys(self) -> list[str]:
        """Return the entries that can only miss: bytes gone, unreadable, or incomplete.

        An entry whose ``output.json`` is missing or cannot be decoded, or
        that names an artifact the store no longer holds, would be treated as
        a miss on every lookup. Removing it loses nothing.
        """
        unusable = []
        for key in self.index.cache_keys():
            entry_dir = self._entry_dir(key)
            entry = self.index.entry(key)
            artifact_ids = {
                *self._value_artifact_ids(entry_dir=entry_dir),
                *(entry.artifact_ids.values() if entry is not None else ()),
            }
            if not self._entry_readable(entry_dir) or any(
                not self._artifact_store.artifact_path(artifact_id=artifact_id).exists()
                for artifact_id in artifact_ids
            ):
                unusable.append(key)
        return unusable

    def integrity_problems(self) -> list[str]:
        """Return the ways the index and the bytes on disk disagree.

        The database is the only cache index, so the two can only drift by
        losing one side: an entry row whose ``output.json`` is gone is a row
        that will never hit, a directory with no row is bytes nothing can find,
        and an artifact a cache entry names but the store does not hold is a
        restore that will fail. An ``output.json`` that cannot be decoded — cut
        short by a full disk, or written by a ginkgo whose encoding has since
        moved on — will miss on every load. Each is reported rather than
        repaired.

        Returns
        -------
        list[str]
            One sentence per problem, in the order they were checked.
        """
        problems: list[str] = []
        for key in self.index.cache_keys():
            output_path = self.output_path(key)
            if not output_path.is_file():
                problems.append(f"cache entry {key} has a row but no output.json")
                continue
            try:
                encoded = json.loads(output_path.read_text(encoding="utf-8"))
                check_encoded(encoded)
            except _UNREADABLE_ENTRY_ERRORS as exc:
                problems.append(f"cache entry {key} has an unreadable output.json: {exc}")
                continue
            problems += [
                f"cache entry {key} names value artifact {artifact_id}, which is missing "
                "from the artifact store"
                for artifact_id in sorted(encoded_artifact_ids(encoded))
                if not self._artifact_store.artifact_path(artifact_id=artifact_id).exists()
            ]
        problems += [
            f"cache directory {entry.name} is not referenced by the ledger "
            "(the ledger may have been reset)"
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

    def content_tracked_input_digests(
        self, *, task_def: TaskDef, resolved_args: dict[str, Any]
    ) -> dict[tuple[str, str], str | None]:
        """Return ``{(parameter, resolved path): digest}`` for plain-``str`` inputs.

        Covers the inputs the root-input rule content-hashes in
        :meth:`build_cache_key` (not ``file``/``folder`` inputs, which a task
        is not expected to write), plus the ones that would be hashed if
        their file existed, recorded with ``None`` so a task creating the
        file is noticed too. Digests come from the same memoised hashing, so
        taking them right after the key costs no extra reads. Each path is
        keyed with the parameter that carried it, so a warning about it can
        name what to re-annotate.
        """
        digests: dict[tuple[str, str], str | None] = {}
        for name, parameter in task_def.signature.parameters.items():
            if name in task_def.output_params or name not in resolved_args:
                continue
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            for path in _content_trackable_paths(annotation=annotation, value=resolved_args[name]):
                resolved = str(Path(path).resolve())
                if (name, resolved) in digests:
                    continue
                candidate = Path(resolved)
                digests[(name, resolved)] = (
                    self._hash_file_contents(candidate) if candidate.is_file() else None
                )
        return digests

    def written_inputs(self, *, before: dict[tuple[str, str], str | None]) -> set[tuple[str, str]]:
        """Return the ``(parameter, path)`` keys of *before* whose file the task changed."""
        written: set[tuple[str, str]] = set()
        for (parameter, path), digest in before.items():
            candidate = Path(path)
            after = self._hash_file_contents(candidate) if candidate.is_file() else None
            if after != digest:
                written.add((parameter, path))
        return written

    def _hash_value(
        self,
        *,
        annotation: Any,
        value: Any,
        known_digests: dict[str, str] | None = None,
        label: str = "value",
        is_output: bool = False,
    ) -> Any:
        """Hash a concrete value according to its declared Ginkgo type.

        An ``Out[...]`` parameter (``is_output``) contributes its path
        string only, exactly as a plain ``str`` scalar would — never file or
        folder content, since the path names what this task is about to
        write, not something it reads.
        """
        if annotation is tmp_dir:
            return None

        # An absent optional output must key differently from a present one,
        # so absence gets its own token rather than collapsing to null.
        annotation, admits_none = unwrap_optional_annotation(annotation)
        if value is None and admits_none:
            return {"type": "absent"}
        if is_output:
            return self._hash_path_string_leaf(value=value)
        if is_untracked_annotation(annotation):
            return self._hash_path_string_leaf(value=value)
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
            return self._hash_sequence(
                type_name=origin.__name__,
                pairs=pair_elements_with_annotations(annotation=annotation, value=value),
                container_annotation=annotation,
                known_digests=known_digests,
                label=label,
            )

        if origin is dict:
            key_annotation, value_annotation = self._dict_annotations(annotation)
            return self._hash_mapping(
                value=value,
                key_annotation=key_annotation,
                value_annotation=value_annotation,
                container_annotation=annotation,
                known_digests=known_digests,
                label=label,
            )

        if isinstance(value, (list, tuple)):
            return self._hash_sequence(
                type_name="list" if isinstance(value, list) else "tuple",
                pairs=[(annotation, item) for item in value],
                container_annotation=annotation,
                known_digests=known_digests,
                label=label,
            )

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
            return self._hash_mapping(
                value=value,
                key_annotation=Any,
                value_annotation=Any,
                container_annotation=Any,
                known_digests=known_digests,
                label=label,
            )

        # Root-input path hashing (#307 phase 2, closes #121/#281): a value
        # that is not ``Out[...]``, not ``tmp_dir``/``untracked``, and not
        # already annotated ``file``/``folder`` (those branches above already
        # handled it) is nonetheless content-hashed, like a ``file`` input,
        # when it is a str/PathLike naming an existing *regular file* that
        # reads as a path — see ``is_content_trackable_path_value`` for the
        # exact eligibility rule. A directory never qualifies here; it falls
        # through to the plain scalar branch below, keyed by its path string,
        # same as before this feature.
        if is_content_trackable_path_value(annotation=annotation, value=value):
            resolved_key = str(Path(str(value)).resolve())
            if known_digests is not None:
                known = known_digests.get(resolved_key)
                if known is not None:
                    return {"sha256": known, "type": "file"}
            return {"sha256": self._hash_file_contents(Path(str(value))), "type": "file"}

        # Exact types only, as ``encode_value`` stores them: a subclass such as
        # ``np.float64`` is hashed by its bytes, so the fresh value and the one
        # restored from the cache hash the same.
        if value is None or type(value) in (bool, int, float, str):
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

    def _hash_path_string_leaf(self, *, value: Any) -> Any:
        """Hash a value as a plain path string only, recursively.

        Mirrors the plain-scalar branch of :meth:`_hash_value` exactly (same
        ``repr``-based digest), so it contributes to the cache key only when
        the declared path itself changes, never when the file or directory
        at that path does. Used for an ``Out[...]`` parameter (the path
        names what the task is about to write) and for an ``untracked``-
        annotated parameter (a deliberate opt-out of content tracking).
        """
        if isinstance(value, (list, tuple)):
            return {
                "items": [self._hash_path_string_leaf(value=item) for item in value],
                "type": type(value).__name__,
            }
        return {"sha256": hash_str(repr(value)), "type": type(value).__name__}

    def _hash_sequence(
        self,
        *,
        type_name: str,
        pairs: list[tuple[Any, Any]],
        container_annotation: Any,
        known_digests: dict[str, str] | None,
        label: str,
    ) -> dict[str, Any]:
        """Hash a list or tuple from its ``(annotation, element)`` pairs.

        See :meth:`_hash_elements` for when it is one digest and when it keeps
        an entry per element.
        """
        items, names_path = self._hash_elements(
            pairs=pairs,
            container_annotation=container_annotation,
            known_digests=known_digests,
            label=label,
        )
        if not names_path:
            return {"sha256": _digest_of_tokens(items), "type": type_name}
        return {"items": items, "type": type_name}

    def _hash_mapping(
        self,
        *,
        value: dict[Any, Any],
        key_annotation: Any,
        value_annotation: Any,
        container_annotation: Any,
        known_digests: dict[str, str] | None,
        label: str,
    ) -> dict[str, Any]:
        """Hash a dict, its items ordered by the ``repr`` of their keys."""
        pairs: list[tuple[Any, Any]] = []
        for key, item in sorted(value.items(), key=lambda pair: repr(pair[0])):
            pairs.extend([(key_annotation, key), (value_annotation, item)])
        items, names_path = self._hash_elements(
            pairs=pairs,
            container_annotation=container_annotation,
            known_digests=known_digests,
            label=label,
        )
        if not names_path:
            return {"sha256": _digest_of_tokens(items), "type": "dict"}
        return {
            "items": [
                {"key": items[index], "value": items[index + 1]}
                for index in range(0, len(items), 2)
            ],
            "type": "dict",
        }

    def _hash_elements(
        self,
        *,
        pairs: list[tuple[Any, Any]],
        container_annotation: Any,
        known_digests: dict[str, str] | None,
        label: str,
    ) -> tuple[list[Any], bool]:
        """Return a container's element hashes and whether one names a path.

        A container whose elements hold no file, folder or remote object is
        hashed as one digest of them: an entry per element would make a
        200,000-number list a 200,000-entry key component, stored with the
        cache entry and in every run's ledger. Only a container that does hold
        one keeps an entry per element, so ``cache explain`` and the ledger
        can name the file that moved.

        A plain scalar, or a string that cannot name a path, stands for itself
        as its ``repr`` without going through :meth:`_hash_value`; it gets the
        usual hash only when the container keeps an entry per element.
        """
        plain_strings = not is_path_shaped_annotation(container_annotation)
        tokens = [
            repr(item)
            if _hashes_as_repr(value=item, plain_strings=plain_strings)
            else self._hash_value(
                annotation=item_annotation,
                value=item,
                known_digests=known_digests,
                label=label,
            )
            for item_annotation, item in pairs
        ]
        if not any(_names_tracked_path(token) for token in tokens):
            return tokens, False
        items = [
            self._hash_value(annotation=item_annotation, value=item, label=label)
            if isinstance(token, str)
            else token
            for (item_annotation, item), token in zip(pairs, tokens, strict=True)
        ]
        return items, True

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
                label=f"{task_def.base_name}.{name}",
                is_output=name in task_def.output_params,
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

    def _stat_value(
        self,
        *,
        annotation: Any,
        value: Any,
        label: str = "value",
        is_output: bool = False,
    ) -> Any:
        """Build a stat-based representation for a value (no content reading).

        An ``Out[...]`` parameter (``is_output``) is never stat'd — like the
        content-addressed key, it contributes its path string only. Same for
        an ``untracked``-annotated parameter.
        """
        if annotation is tmp_dir:
            return None
        if is_output:
            return self._hash_path_string_leaf(value=value)
        if is_untracked_annotation(annotation):
            return self._hash_path_string_leaf(value=value)

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

        # Root-input path stat fingerprint (#307 phase 2), mirroring the
        # ``file``/``folder`` branches above rather than reading content: a
        # value eligible for root-input content hashing under the normal key
        # (see ``is_content_trackable_path_value``) is fingerprinted by stat,
        # not by digest, so ``--trust-mtimes`` stays a fast surrogate.
        if is_content_trackable_path_value(annotation=annotation, value=value):
            st = Path(str(value)).resolve().stat()
            return {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "type": "file"}

        # For non-path types, use the same hash as the content-addressed path.
        return self._hash_value(annotation=annotation, value=value, label=label)

    def _dict_annotations(self, annotation: Any) -> tuple[Any, Any]:
        """Extract key and value annotations for a mapping annotation."""
        args = get_args(annotation)
        if len(args) == 2:
            return args[0], args[1]
        return Any, Any


def _content_trackable_paths(*, annotation: Any, value: Any) -> list[str]:
    """Return the path strings in *value* the root-input rule would content-hash.

    Walks lists, tuples and dict values the way :meth:`CacheStore._hash_value`
    does, pairing each element with its own annotation. Includes paths whose
    file does not exist yet, so a task that creates one can be noticed.
    """
    if type(value) in PLAIN_SCALAR_TYPES:
        return []
    annotation, _ = unwrap_optional_annotation(annotation)
    if isinstance(value, (list, tuple)):
        if get_origin(annotation) in {list, tuple}:
            pairs = pair_elements_with_annotations(annotation=annotation, value=value)
        else:
            pairs = [(Any, item) for item in value]
        return [
            path
            for item_annotation, item in pairs
            for path in _content_trackable_paths(annotation=item_annotation, value=item)
        ]
    if isinstance(value, dict):
        dict_args = get_args(annotation) if get_origin(annotation) is dict else ()
        value_annotation = dict_args[1] if len(dict_args) == 2 else Any
        return [
            path
            for item in value.values()
            for path in _content_trackable_paths(annotation=value_annotation, value=item)
        ]
    if is_content_trackable_path_shape(annotation=annotation, value=value):
        return [str(value)]
    return []


def _hashes_as_repr(*, value: Any, plain_strings: bool) -> bool:
    """Return whether a container element is keyed by its ``repr`` alone.

    True for a plain scalar, and, when the container's annotation binds no
    element to a path (``plain_strings``), for a string that does not read as
    one: :meth:`CacheStore._hash_value` keys either by its ``repr`` too.
    """
    if type(value) in PLAIN_SCALAR_TYPES:
        return True
    return plain_strings and type(value) is str and not looks_like_path_string(value)


def _names_tracked_path(element_hash: Any) -> bool:
    """Return whether an element's hash is, or holds, a file, folder or remote object."""
    if not isinstance(element_hash, dict):
        return False
    return (
        "items" in element_hash
        or element_hash.get("type") in {"file", "folder"}
        or "bucket" in element_hash
    )


def _digest_of_tokens(tokens: list[Any]) -> str:
    """Return the one digest of a container's element hashes."""
    return hash_str(json.dumps(tokens, sort_keys=True, separators=(",", ":")))
