"""Asset registration glue between the cache and asset stores.

When a task returns one or more :class:`AssetResult` sentinels, the
evaluator stores each payload into the artifact store, registers an
immutable :class:`AssetVersion` in the local asset catalog, and replaces
every sentinel with an :class:`AssetRef` so downstream tasks see the
resolved reference.

Dispatch is kind-keyed through
:data:`~ginkgo.runtime.artifacts.asset_kinds.ASSET_KINDS`. File assets
go through a dedicated path because their content is copied from a
user-supplied source path; every other kind is serialised by the kind's
registered serializer and then stored as bytes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from ginkgo.core.asset import (
    AssetKey,
    AssetRef,
    AssetResult,
    AssetVersion,
    asset_ref_from_version,
    collect_asset_refs,
    make_asset_version,
    make_asset_version_id,
)
from ginkgo.core.hashing import hash_str
from ginkgo.errors import GinkgoError
from ginkgo.runtime.artifacts.asset_kinds import (
    WRAPPER_KINDS,
    get_kind_spec,
    is_path_backed_payload,
)
from ginkgo.runtime.artifacts.asset_serialization import (
    AssetSerializationError,
    SerializedAsset,
    serialize_asset,
)
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.caching.index import CacheEntry
from ginkgo.runtime.events import AssetMaterialized, GinkgoEvent, task_id_for_node


logger = logging.getLogger(__name__)

ASSET_GROUP_METADATA_KEY = "ginkgo_group"
ASSET_CAPTION_METADATA_KEY = "ginkgo_caption"
ASSET_CHECKS_METADATA_KEY = "ginkgo_checks"


class AssetCheckError(GinkgoError, RuntimeError):
    """Raised when an asset check cannot verify a wrapped payload."""


def asset_key_for_result(*, name: str, kind: str) -> AssetKey:
    """Build one asset key for a supported asset result.

    Parameters
    ----------
    name : str
        Local asset name.
    kind : str
        Asset kind. Any registered kind is accepted.

    Returns
    -------
    AssetKey
    """
    # Validate via the registry; falls back to ValueError for unknown kinds.
    get_kind_spec(kind)
    return AssetKey(namespace=kind, name=name)


def render_asset_ref(*, asset_ref: AssetRef) -> dict[str, Any]:
    """Render one asset reference for provenance and event payloads."""
    return {
        "artifact_id": asset_ref.artifact_id,
        "artifact_path": asset_ref.artifact_path,
        "asset_key": str(asset_ref.key),
        "content_hash": asset_ref.content_hash,
        "kind": asset_ref.kind,
        "metadata": dict(asset_ref.metadata),
        "name": asset_ref.name,
        "namespace": asset_ref.namespace,
        "version_id": asset_ref.version_id,
    }


def asset_index_for(*, value: Any) -> list[dict[str, Any]]:
    """Return rendered asset summaries for one task result value."""
    return [render_asset_ref(asset_ref=asset_ref) for asset_ref in collect_asset_refs(value)]


@dataclass(kw_only=True)
class _AssetRegistrationState:
    """Per-task state used while assigning keys to non-file asset outputs.

    An explicit ``name=`` is the asset name verbatim, for every kind. Only
    unnamed outputs get a generated name: file-kind assets default to the
    task function's name, and every other kind keeps a per-kind counter so
    unnamed outputs get deterministic indexed names
    (``<task>.<kind>[<index>]``).
    """

    kind_counters: dict[str, int] = field(default_factory=dict)
    used_names: set[tuple[str, str]] = field(default_factory=set)

    def reserve_name(self, *, result: AssetResult, task_name: str) -> str:
        """Return the local asset name for a result, enforcing uniqueness.

        Uniqueness is scoped by ``(kind, name)`` within the task, so the
        task name stays out of the user-visible key.
        """
        kind = result.kind
        if result.name is not None:
            key = (kind, result.name)
            if key in self.used_names:
                raise ValueError(
                    f"duplicate wrapped asset name in task {task_name!r}: "
                    f"kind={kind} name={result.name!r}"
                )
            self.used_names.add(key)
            return result.name

        index = self.kind_counters.get(kind, 0)
        self.kind_counters[kind] = index + 1
        return f"{task_name}.{kind}[{index}]"


@dataclass(kw_only=True)
class AssetRegistrar:
    """Materialise asset sentinels in a task result into asset references.

    Parameters
    ----------
    cache_store : CacheStore
        Provides access to the underlying artifact store for content
        storage.
    asset_store : AssetStore
        Local asset catalog where new versions and lineage edges are
        recorded.
    run_id_provider : Callable[[], str]
        Returns the active run id at registration time.
    live_payloads : LivePayloadRegistry | None
        Optional in-process cache that lets downstream tasks consume
        wrapped outputs without a disk round-trip.
    emit_event : Callable[[GinkgoEvent], None] | None
        Where ``AssetMaterialized`` is announced. The catalog row is written
        directly by :class:`AssetStore`; the event is the ledger's record that
        the version came into being, and what a live reader watches.
    """

    cache_store: CacheStore
    asset_store: AssetStore
    run_id_provider: Callable[[], str]
    live_payloads: LivePayloadRegistry | None = None
    emit_event: Callable[[GinkgoEvent], None] | None = None

    def materialize_results(self, *, node: Any, value: Any) -> Any:
        """Register nested asset sentinels and replace them with asset refs.

        Mutates ``node.asset_versions`` to record every newly registered
        version so the scheduler can later persist them.
        """
        node.asset_versions = []
        parent_refs = self._parent_asset_refs(node=node)

        # Walk once to validate wrapped-asset name uniqueness before
        # serialising anything, so a duplicate leaves no partial catalog
        # state.
        state = _AssetRegistrationState()
        self._validate_wrapped_names(node=node, value=value, state=state)

        # Reset counters — the mutating walk needs its own fresh indices
        # so the validation pass does not inflate them.
        state = _AssetRegistrationState()
        return self._replace_asset_results(
            node=node,
            value=value,
            parent_refs=parent_refs,
            state=state,
        )

    def _validate_wrapped_names(
        self,
        *,
        node: Any,
        value: Any,
        state: _AssetRegistrationState,
    ) -> None:
        """Pre-walk sentinels and enforce name uniqueness for non-file kinds."""
        if isinstance(value, AssetResult):
            if value.kind in WRAPPER_KINDS:
                state.reserve_name(result=value, task_name=node.task_def.fn.__name__)
            return
        if isinstance(value, list | tuple):
            for item in value:
                self._validate_wrapped_names(node=node, value=item, state=state)
            return
        if isinstance(value, dict):
            for item in value.values():
                self._validate_wrapped_names(node=node, value=item, state=state)

    def _replace_asset_results(
        self,
        *,
        node: Any,
        value: Any,
        parent_refs: list[AssetRef],
        state: _AssetRegistrationState,
    ) -> Any:
        """Recursively replace nested asset sentinels with asset refs."""
        if isinstance(value, AssetResult):
            asset_ref, asset_version = self._register_asset(
                node=node,
                result=value,
                parent_refs=parent_refs,
                state=state,
            )
            node.asset_versions.append(asset_version)
            return asset_ref

        if isinstance(value, list):
            return [
                self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    state=state,
                )
                for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    state=state,
                )
                for item in value
            )

        if isinstance(value, dict):
            return {
                key: self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    state=state,
                )
                for key, item in value.items()
            }

        return value

    def _register_asset(
        self,
        *,
        node: Any,
        result: AssetResult,
        parent_refs: list[AssetRef],
        state: _AssetRegistrationState,
    ) -> tuple[AssetRef, AssetVersion]:
        """Register one asset result through its kind-specific path."""
        task_fn_name = node.task_def.fn.__name__
        spec = get_kind_spec(result.kind)

        # 1. Resolve the local asset name using the kind's strategy.
        if spec.default_name_strategy == "task_name":
            asset_name = result.name or task_fn_name
            version_metadata = _metadata_with_group(metadata=result.metadata, result=result)
            # File assets carry no serializer; the registrar copies bytes
            # directly from the declared source path.
            record = self._store_file_content(node=node, result=result)
        else:
            asset_name = state.reserve_name(result=result, task_name=task_fn_name)
            index = _current_index_for(state=state, result=result)
            serialized: SerializedAsset = serialize_asset(result=result, index=index)
            record = self.cache_store._artifact_store.store_bytes(
                data=serialized.data,
                extension=serialized.extension,
            )
            version_metadata = _metadata_with_group(metadata=serialized.metadata, result=result)

        # 2. Verify the stored payload before publishing a catalog version.
        check_outcomes = _check_outcomes(result=result)
        if check_outcomes:
            version_metadata[ASSET_CHECKS_METADATA_KEY] = check_outcomes

        # 3. Build and register the immutable version record.
        version = make_asset_version(
            key=asset_key_for_result(name=asset_name, kind=result.kind),
            kind=result.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self.run_id_provider(),
            producer_task=node.task_def.name,
            metadata=version_metadata,
        )
        self.asset_store.register_version(
            version=version,
            parents=parent_refs,
            code_version=_code_version(node=node),
            task_id=task_id_for_node(node.node_id),
            cache_key=getattr(node, "cache_key", None),
        )
        asset_ref = asset_ref_from_version(
            version=version,
            artifact_path=self.cache_store._artifact_store.artifact_path(
                artifact_id=record.artifact_id
            ),
        )
        self._announce(node=node, version=version, parents=parent_refs)

        # 4. Cache live payloads for in-process downstream consumers.
        # File assets don't benefit (consumers get a path either way); fig
        # payloads are binary blobs that are rarely consumed as live Python
        # objects — skipping them aligns the registry with the evaluator's
        # rehydrate-on-receive set. Path-backed payloads (a CSV given to
        # ``table()``) are skipped too: the on-disk loader returns the
        # deserialised object, so caching the raw path would make a live hit
        # and a loader fallback disagree about what a ref rehydrates to.
        if (
            self.live_payloads is not None
            and spec.rehydrate_on_receive
            and result.kind != "fig"
            and not is_path_backed_payload(
                kind=result.kind,
                sub_kind=result.sub_kind,
                payload=result.payload,
            )
        ):
            self.live_payloads.put(
                artifact_id=record.artifact_id,
                payload=result.payload,
            )

        return asset_ref, version

    def reassert_cached_versions(self, *, value: Any, cache_key: str) -> list[AssetRef]:
        """Re-establish catalog rows for the asset versions a cache hit replayed.

        A task that executes registers a row for every version it materializes;
        a task that hits the cache registers nothing, and hands its consumers
        refs rebuilt from ``output.json``. Where the catalog no longer holds
        those versions — rebuilt, restored or lost behind an intact cache — the
        consumer's lineage silently drops them and the artifact collector stops
        protecting their bytes (issue #263). The producer repairs that on its
        own hit rather than leaving a consumer to write another task's rows.

        The repair is one-way: a version the catalog already knows is left
        alone, so a healthy warm run costs one lookup per replayed asset,
        reads nothing that only a repair needs, and never trades a
        fully-attributed row for a partial one.

        A repair that fails is contained. The workspace is then left exactly
        as it was — the state this repair exists to correct, and one the
        consumer still warns about — so failing the cache hit as well would
        turn an incomplete catalog into a broken run.

        Parameters
        ----------
        value : Any
            The replayed cached result, whose nested refs name the versions.
        cache_key : str
            The entry the value came from, which is what the row can be
            attributed from.

        Returns
        -------
        list[AssetRef]
            The refs whose rows were missing and have now been written.
        """
        refs = collect_asset_refs(value)
        if not refs:
            return []
        try:
            return self._reassert_missing(refs=refs, cache_key=cache_key)
        except Exception as exc:
            logger.warning(
                "Could not re-assert the catalog rows for the assets cache entry %s "
                "replayed (%s); lineage through them will be incomplete",
                cache_key,
                exc,
            )
            return []

    def _reassert_missing(self, *, refs: list[AssetRef], cache_key: str) -> list[AssetRef]:
        """Write a catalog row for each replayed ref the catalog does not hold."""
        missing = [ref for ref in refs if self.asset_store.version_by_id(ref.version_id) is None]
        if not missing:
            return []
        # Only a repair needs to know who wrote the entry, so only a repair
        # reads it.
        entry = self.cache_store.index.entry(cache_key)
        written: list[AssetRef] = []
        for ref in missing:
            run_id, producer_task = _proven_producer(ref=ref, entry=entry)
            if self.asset_store.reassert_version(
                ref=ref,
                run_id=run_id,
                producer_task=producer_task,
                created_at=None if entry is None else entry["created_at"],
                cache_key=cache_key,
            ):
                written.append(ref)
        return written

    def _announce(
        self,
        *,
        node: Any,
        version: AssetVersion,
        parents: list[AssetRef],
    ) -> None:
        """Record in the ledger that one asset version was materialized."""
        if self.emit_event is None:
            return
        self.emit_event(
            AssetMaterialized(
                run_id=self.run_id_provider(),
                task_id=task_id_for_node(node.node_id),
                task_name=node.task_def.name,
                asset_key=str(version.key),
                version_id=version.version_id,
                kind=version.kind,
                artifact_id=version.artifact_id,
                content_hash=version.content_hash,
                cache_key=getattr(node, "cache_key", None),
                metadata=dict(version.metadata),
                parents=[render_asset_ref(asset_ref=parent) for parent in parents],
            )
        )

    def _store_file_content(self, *, node: Any, result: AssetResult) -> Any:
        """Copy the file-kind asset's source bytes into the artifact store."""
        source_path = result.path
        if not source_path.is_file():
            raise FileNotFoundError(
                f"{node.task_def.name}.return asset file must exist: {str(source_path)!r}"
            )
        return self.cache_store._artifact_store.store(src_path=source_path)

    def _parent_asset_refs(self, *, node: Any) -> list[AssetRef]:
        """Collect unique upstream asset references consumed by one task.

        The resolved arguments still hold a ref wherever the parameter binds a
        path. Where it binds the payload instead, the evaluator rehydrated the
        ref into a DataFrame before the task ever saw it, and the identity
        survives only on ``node.asset_inputs`` — recorded at resolution time
        for exactly this reason (issue #253). Both are read, so lineage does
        not depend on how a consumer chose to annotate its parameter.

        A parameter can bind several assets at once — the fan-in shape — and
        every one of them is a parent, so each parameter's whole list is read
        rather than its first entry (issue #264).
        """
        if node.resolved_args is None:
            return []
        unique: dict[tuple[str, str, str], AssetRef] = {}
        for asset_ref in collect_asset_refs(node.resolved_args):
            unique[(asset_ref.namespace, asset_ref.name, asset_ref.version_id)] = asset_ref
        for param, declared in node.asset_inputs.items():
            for entry in declared:
                self._add_declared_parent(unique=unique, param=param, node=node, declared=entry)
        return list(unique.values())

    def _add_declared_parent(
        self,
        *,
        unique: dict[tuple[str, str, str], AssetRef],
        param: str,
        node: Any,
        declared: dict[str, Any],
    ) -> None:
        """Add one ``asset_inputs`` entry to the collected parents, if the catalog knows it."""
        version_id = declared["version_id"]
        version = self.asset_store.version_by_id(version_id)
        if version is None:
            # The catalog has no row for a version the evaluator resolved
            # moments ago. Nothing downstream can be traced through it, and
            # that is a registration bug rather than a shape lineage should
            # quietly accept.
            logger.warning(
                "Task %s consumed asset version %s through %r, "
                "which the catalog has no row for; lineage will not record it",
                node.task_def.name,
                version_id,
                param,
            )
            return
        key = (version.key.namespace, version.key.name, version.version_id)
        unique.setdefault(
            key,
            asset_ref_from_version(
                version=version,
                artifact_path=self.cache_store.artifact_store_view.artifact_path(
                    artifact_id=version.artifact_id
                ),
            ),
        )


def _proven_producer(*, ref: AssetRef, entry: CacheEntry | None) -> tuple[str | None, str | None]:
    """Return the run and task that produced a replayed version, when provable.

    An entry's ``created_run_id`` and ``function`` describe the run and the
    task that *wrote the entry*, which is not the same claim as the one a
    catalog row makes. A task that passes an input's ref straight back out
    writes an entry replaying a version some other task produced, and taking
    the entry at its word would have that row name the wrong producer.

    A version id settles it: it hashes over the key, the content and the
    producing run, so a recomputation that lands on the ref's own id proves
    the entry's run minted this version, and with it that the entry's function
    is the task that did. Where the recomputation disagrees, both halves are
    withheld — a row that says nothing about its provenance is worth more than
    one that says something false.

    Returns
    -------
    tuple[str | None, str | None]
        The producing run id and task name, or ``(None, None)``.
    """
    if entry is None:
        return None, None
    run_id = entry["created_run_id"]
    if not run_id:
        return None, None
    minted = make_asset_version_id(key=ref.key, content_hash=ref.content_hash, run_id=str(run_id))
    if minted != ref.version_id:
        return None, None
    return str(run_id), entry["function"]


def _current_index_for(
    *,
    state: _AssetRegistrationState,
    result: AssetResult,
) -> int:
    """Return the positional index most recently assigned for ``result.kind``.

    Used only for error-message attribution: named results never need an
    index, and unnamed results have just incremented the counter inside
    :meth:`_AssetRegistrationState.reserve_name`.
    """
    if result.name is not None:
        return -1
    return max(0, state.kind_counters.get(result.kind, 1) - 1)


def _code_version(*, node: Any) -> str | None:
    """Return the identity of the code that produced a task's assets.

    The task's own source hash, folded together with the driver file's for a
    notebook or script task, so that two versions built by the same code agree
    and a change to either half of it shows.
    """
    source_hash = node.task_def.cache_source_hash
    extra = getattr(node, "extra_source_hash", None)
    if source_hash is None:
        return None
    return hash_str(f"{source_hash}\n{extra}") if extra else str(source_hash)


def _metadata_with_group(*, metadata: dict[str, Any], result: AssetResult) -> dict[str, Any]:
    """Return version metadata including report presentation labels."""
    version_metadata = dict(metadata)
    if result.group is not None:
        version_metadata[ASSET_GROUP_METADATA_KEY] = result.group
    if result.caption is not None:
        version_metadata[ASSET_CAPTION_METADATA_KEY] = result.caption
    return version_metadata


def _check_outcomes(*, result: AssetResult) -> list[dict[str, bool | str]]:
    """Run asset checks and return their serialisable passing outcomes."""
    outcomes: list[dict[str, bool | str]] = []
    for check in result.checks:
        check_name = getattr(check, "__name__", type(check).__name__)
        if not callable(check):
            raise AssetCheckError(
                f"Asset check {check_name!r} for {result.kind!r} asset is not callable."
            )

        try:
            passed = check(result.payload)
        except Exception as exc:
            raise AssetCheckError(
                f"Asset check {check_name!r} raised an exception for {result.kind!r} asset."
            ) from exc

        if not isinstance(passed, bool):
            raise AssetCheckError(
                f"Asset check {check_name!r} for {result.kind!r} asset must return bool, "
                f"got {type(passed).__name__}."
            )
        if not passed:
            raise AssetCheckError(f"Asset check {check_name!r} failed for {result.kind!r} asset.")
        outcomes.append({"name": check_name, "passed": passed})
    return outcomes


# Re-export for callers that used to import from asset_registration.
__all__ = [
    "AssetRegistrar",
    "ASSET_CAPTION_METADATA_KEY",
    "ASSET_CHECKS_METADATA_KEY",
    "ASSET_GROUP_METADATA_KEY",
    "AssetCheckError",
    "AssetSerializationError",
    "asset_index_for",
    "asset_key_for_result",
    "render_asset_ref",
]
