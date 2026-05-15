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
)
from ginkgo.runtime.artifacts.asset_kinds import WRAPPER_KINDS, get_kind_spec
from ginkgo.runtime.artifacts.asset_serialization import (
    AssetSerializationError,
    SerializedAsset,
    serialize_asset,
)
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry
from ginkgo.runtime.caching.cache import CacheStore


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

    File-kind assets default to the task function's name; every other
    kind keeps a per-kind counter so unnamed outputs get deterministic
    indexed names (``<task>.<kind>[<index>]``).
    """

    kind_counters: dict[str, int] = field(default_factory=dict)
    used_names: set[tuple[str, str]] = field(default_factory=set)

    def reserve_name(self, *, result: AssetResult, task_name: str) -> str:
        """Return the local asset name for a result, enforcing uniqueness."""
        kind = result.kind
        if result.name is not None:
            local = f"{task_name}.{result.name}"
            key = (kind, local)
            if key in self.used_names:
                raise ValueError(
                    f"duplicate wrapped asset name in task {task_name!r}: "
                    f"kind={kind} name={result.name!r}"
                )
            self.used_names.add(key)
            return local

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
    """

    cache_store: CacheStore
    asset_store: AssetStore
    run_id_provider: Callable[[], str]
    live_payloads: LivePayloadRegistry | None = None

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
            version_metadata = dict(result.metadata)
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
            version_metadata = dict(serialized.metadata)

        # 2. Build and register the immutable version record.
        version = make_asset_version(
            key=asset_key_for_result(name=asset_name, kind=result.kind),
            kind=result.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self.run_id_provider(),
            producer_task=node.task_def.name,
            metadata=version_metadata,
        )
        self.asset_store.register_version(version=version)
        asset_ref = asset_ref_from_version(
            version=version,
            artifact_path=self.cache_store._artifact_store.artifact_path(
                artifact_id=record.artifact_id
            ),
        )
        if parent_refs:
            self.asset_store.record_lineage(child=asset_ref, parents=parent_refs)

        # 3. Cache live payloads for in-process downstream consumers.
        # File assets don't benefit (consumers get a path either way); fig
        # payloads are binary blobs that are rarely consumed as live Python
        # objects — skipping them aligns the registry with the evaluator's
        # rehydrate-on-receive set.
        if self.live_payloads is not None and spec.rehydrate_on_receive and result.kind != "fig":
            self.live_payloads.put(
                artifact_id=record.artifact_id,
                payload=result.payload,
            )

        return asset_ref, version

    def _store_file_content(self, *, node: Any, result: AssetResult) -> Any:
        """Copy the file-kind asset's source bytes into the artifact store."""
        source_path = result.path
        if not source_path.is_file():
            raise FileNotFoundError(
                f"{node.task_def.name}.return asset file must exist: {str(source_path)!r}"
            )
        return self.cache_store._artifact_store.store(src_path=source_path)

    def _parent_asset_refs(self, *, node: Any) -> list[AssetRef]:
        """Collect unique upstream asset references consumed by one task."""
        if node.resolved_args is None:
            return []
        unique: dict[tuple[str, str, str], AssetRef] = {}
        for asset_ref in collect_asset_refs(node.resolved_args):
            unique[(asset_ref.namespace, asset_ref.name, asset_ref.version_id)] = asset_ref
        return list(unique.values())


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


# Re-export for callers that used to import from asset_registration.
__all__ = [
    "AssetRegistrar",
    "AssetSerializationError",
    "asset_index_for",
    "asset_key_for_result",
    "render_asset_ref",
]
