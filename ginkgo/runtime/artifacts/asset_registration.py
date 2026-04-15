"""Asset registration glue between the cache and asset stores.

When a task returns ``AssetResult`` or wrapped asset sentinels
(``table``/``array``/``fig``/``text``), the evaluator stores the content
into the artifact store, registers an immutable ``AssetVersion`` in the
local asset catalog, and replaces each sentinel with an ``AssetRef`` so
downstream tasks see the resolved reference.
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
from ginkgo.core.wrappers import WrappedResult
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.artifacts.live_payloads import LivePayloadRegistry
from ginkgo.runtime.artifacts.wrapper_serialization import (
    SerializedWrapper,
    WrapperSerializationError,
    serialize_wrapper,
)
from ginkgo.runtime.caching.cache import CacheStore


_WRAPPER_NAMESPACES = {"table", "array", "fig", "text", "model"}


def asset_key_for_result(*, name: str, kind: str) -> AssetKey:
    """Build one asset key for a supported asset result.

    Parameters
    ----------
    name : str
        Local asset name.
    kind : str
        Asset kind. Supports ``"file"`` plus the wrapped kinds
        (``"table"`` / ``"array"`` / ``"fig"`` / ``"text"`` / ``"model"``).

    Returns
    -------
    AssetKey
    """
    if kind == "file":
        return AssetKey(namespace="file", name=name)
    if kind in _WRAPPER_NAMESPACES:
        return AssetKey(namespace=kind, name=name)
    raise ValueError(f"Unsupported asset kind: {kind!r}")


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
class _WrapperRegistrationState:
    """Per-task state used while assigning keys to wrapped outputs."""

    kind_counters: dict[str, int] = field(default_factory=dict)
    used_names: set[tuple[str, str]] = field(default_factory=set)

    def reserve_name(self, *, wrapper: WrappedResult, task_name: str) -> str:
        """Return the local asset name for a wrapper, enforcing uniqueness."""
        kind = wrapper.kind
        if wrapper.name is not None:
            local = f"{task_name}.{wrapper.name}"
            key = (kind, local)
            if key in self.used_names:
                raise ValueError(
                    f"duplicate wrapped asset name in task {task_name!r}: "
                    f"kind={kind} name={wrapper.name!r}"
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
        Provides access to the underlying artifact store for content storage.
    asset_store : AssetStore
        Local asset catalog where new versions and lineage edges are recorded.
    run_id_provider : Callable[[], str]
        Returns the active run id at registration time.
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

        # Walk once to validate wrapper-name uniqueness before serialising
        # anything, so a duplicate leaves no partial catalog state.
        wrapper_state = _WrapperRegistrationState()
        self._validate_wrapper_names(node=node, value=value, state=wrapper_state)

        # Reset counters — the mutating walk needs its own fresh indices so
        # the validation pass does not inflate them.
        wrapper_state = _WrapperRegistrationState()
        return self._replace_asset_results(
            node=node,
            value=value,
            parent_refs=parent_refs,
            wrapper_state=wrapper_state,
        )

    def _validate_wrapper_names(
        self,
        *,
        node: Any,
        value: Any,
        state: _WrapperRegistrationState,
    ) -> None:
        """Pre-walk wrapper sentinels and enforce name uniqueness."""
        if isinstance(value, WrappedResult):
            state.reserve_name(wrapper=value, task_name=node.task_def.fn.__name__)
            return
        if isinstance(value, list | tuple):
            for item in value:
                self._validate_wrapper_names(node=node, value=item, state=state)
            return
        if isinstance(value, dict):
            for item in value.values():
                self._validate_wrapper_names(node=node, value=item, state=state)

    def _replace_asset_results(
        self,
        *,
        node: Any,
        value: Any,
        parent_refs: list[AssetRef],
        wrapper_state: _WrapperRegistrationState,
    ) -> Any:
        """Recursively replace nested asset sentinels with asset refs."""
        if isinstance(value, AssetResult):
            asset_ref, asset_version = self._register_asset_result(
                node=node,
                asset_result=value,
                parent_refs=parent_refs,
            )
            node.asset_versions.append(asset_version)
            return asset_ref

        if isinstance(value, WrappedResult):
            asset_ref, asset_version = self._register_wrapped_result(
                node=node,
                wrapper=value,
                parent_refs=parent_refs,
                wrapper_state=wrapper_state,
            )
            node.asset_versions.append(asset_version)
            return asset_ref

        if isinstance(value, list):
            return [
                self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    wrapper_state=wrapper_state,
                )
                for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    wrapper_state=wrapper_state,
                )
                for item in value
            )

        if isinstance(value, dict):
            return {
                key: self._replace_asset_results(
                    node=node,
                    value=item,
                    parent_refs=parent_refs,
                    wrapper_state=wrapper_state,
                )
                for key, item in value.items()
            }

        return value

    def _register_asset_result(
        self,
        *,
        node: Any,
        asset_result: AssetResult,
        parent_refs: list[AssetRef],
    ) -> tuple[AssetRef, AssetVersion]:
        """Store one file asset and register its immutable catalog version."""
        source_path = asset_result.path
        if not source_path.is_file():
            raise FileNotFoundError(
                f"{node.task_def.name}.return asset file must exist: {str(source_path)!r}"
            )

        record = self.cache_store._artifact_store.store(src_path=source_path)

        asset_name = asset_result.name or node.task_def.fn.__name__
        version = make_asset_version(
            key=asset_key_for_result(name=asset_name, kind=asset_result.kind),
            kind=asset_result.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self.run_id_provider(),
            producer_task=node.task_def.name,
            metadata=asset_result.metadata,
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
        return asset_ref, version

    def _register_wrapped_result(
        self,
        *,
        node: Any,
        wrapper: WrappedResult,
        parent_refs: list[AssetRef],
        wrapper_state: _WrapperRegistrationState,
    ) -> tuple[AssetRef, AssetVersion]:
        """Serialise one wrapped payload and register its catalog version."""
        task_fn_name = node.task_def.fn.__name__
        local_name = wrapper_state.reserve_name(wrapper=wrapper, task_name=task_fn_name)

        wrapper_index = _current_wrapper_index(
            state=wrapper_state, kind=wrapper.kind, wrapper=wrapper
        )
        serialized: SerializedWrapper
        try:
            serialized = serialize_wrapper(wrapper=wrapper, wrapper_index=wrapper_index)
        except WrapperSerializationError:
            # Already structured — propagate without touching catalog state.
            raise

        record = self.cache_store._artifact_store.store_bytes(
            data=serialized.data,
            extension=serialized.extension,
        )

        metadata = dict(serialized.metadata)
        version = make_asset_version(
            key=asset_key_for_result(name=local_name, kind=wrapper.kind),
            kind=wrapper.kind,
            artifact_id=record.artifact_id,
            content_hash=record.digest_hex,
            run_id=self.run_id_provider(),
            producer_task=node.task_def.name,
            metadata=metadata,
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

        # Cache the producer's live Python object so downstream tasks
        # running in the same evaluator process can rehydrate without a
        # disk round-trip. Falls back to the on-disk loader otherwise.
        if self.live_payloads is not None:
            self.live_payloads.put(
                artifact_id=record.artifact_id,
                payload=wrapper.payload,
            )

        return asset_ref, version

    def _parent_asset_refs(self, *, node: Any) -> list[AssetRef]:
        """Collect unique upstream asset references consumed by one task."""
        if node.resolved_args is None:
            return []
        unique: dict[tuple[str, str, str], AssetRef] = {}
        for asset_ref in collect_asset_refs(node.resolved_args):
            unique[(asset_ref.namespace, asset_ref.name, asset_ref.version_id)] = asset_ref
        return list(unique.values())


def _current_wrapper_index(
    *,
    state: _WrapperRegistrationState,
    kind: str,
    wrapper: WrappedResult,
) -> int:
    """Return the positional index most recently assigned for ``kind``.

    Used only for error-message attribution: names that were already
    reserved never land here, and unnamed wrappers have just incremented
    the counter inside :meth:`_WrapperRegistrationState.reserve_name`.
    """
    if wrapper.name is not None:
        return -1
    # reserve_name incremented the counter to ``index + 1``; the wrapper
    # that was just registered occupied ``index``.
    return max(0, state.kind_counters.get(kind, 1) - 1)
