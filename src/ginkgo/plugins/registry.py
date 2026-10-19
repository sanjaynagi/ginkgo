"""Discover plugins and look up their contributions.

Core's own plugin is always loaded first. Other plugins are found through the
``ginkgo.plugins`` entry-point group, each naming a module attribute that is a
:class:`~ginkgo.plugins.api.Plugin`. Discovery happens once per process, on the
first lookup, so a run that never needs a plugin never imports one.

A plugin that fails to load is skipped and recorded rather than fatal: one
broken third-party plugin must not stop core, or every other plugin, from
working. The failure is reported where it matters — by ``ginkgo plugins``, and
in the error a lookup raises when nothing provides the key it asked for.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import TypeVar

from ginkgo.core.directive import ExecutionDirective
from ginkgo.errors import GinkgoError
from ginkgo.plugins.api import (
    CliCommand,
    Contribution,
    EventSubscriberType,
    ExecutorType,
    MountDriverType,
    ObjectStoreType,
    Plugin,
    TaskKind,
)

__all__ = [
    "ENTRY_POINT_GROUP",
    "PluginConflictError",
    "PluginLoadFailure",
    "PluginNotFoundError",
    "PluginRegistry",
    "first_party_provider",
    "plugin_registry",
    "use_plugin_registry",
]

ENTRY_POINT_GROUP = "ginkgo.plugins"

_FIRST_PARTY_PROVIDERS: dict[tuple[type, str], str] = {
    (ExecutorType, "k8s"): "ginkgo-k8s",
    (ExecutorType, "batch"): "ginkgo-gcp",
    (EventSubscriberType, "slack"): "ginkgo-slack",
    (ObjectStoreType, "s3"): "ginkgo-aws",
    (ObjectStoreType, "gs"): "ginkgo-gcp",
    (ObjectStoreType, "oci"): "ginkgo-oci",
    (MountDriverType, "s3"): "ginkgo-aws",
    (MountDriverType, "gs"): "ginkgo-gcp",
    (MountDriverType, "oci"): "ginkgo-oci",
    (CliCommand, "report"): "ginkgo-report",
}
"""Which first-party distribution provides a key core does not ship, for hints."""


def first_party_provider(point: type[Contribution], key: str) -> str | None:
    """Return the first-party distribution that contributes *key* to *point*, if any."""
    return _FIRST_PARTY_PROVIDERS.get((point, key))


C = TypeVar("C", bound=Contribution)


class PluginConflictError(GinkgoError, RuntimeError):
    """Raised when two plugins contribute the same key to one extension point."""


class PluginNotFoundError(GinkgoError, LookupError):
    """Raised when no loaded plugin contributes the key asked for."""


@dataclass(frozen=True, kw_only=True)
class PluginLoadFailure:
    """An entry point that did not load to a ``Plugin``.

    Parameters
    ----------
    entry_point : str
        The entry point's name.
    target : str
        What it points at, e.g. ``"ginkgo_k8s:PLUGIN"``.
    reason : str
        Why it was skipped.
    """

    entry_point: str
    target: str
    reason: str

    def describe(self) -> str:
        """Return one line naming the entry point and the reason."""
        return f"{self.entry_point} ({self.target}): {self.reason}"


class PluginRegistry:
    """The loaded plugins, indexed by extension point and key.

    Parameters
    ----------
    plugins : tuple[Plugin, ...]
        Every loaded plugin, core's own first.
    failures : tuple[PluginLoadFailure, ...]
        Entry points that were skipped because they failed to load.

    Raises
    ------
    PluginConflictError
        If two plugins contribute the same key to one extension point, or two
        task kinds return the same directive type.
    """

    def __init__(
        self,
        *,
        plugins: tuple[Plugin, ...],
        failures: tuple[PluginLoadFailure, ...] = (),
    ) -> None:
        self.plugins = plugins
        self.failures = failures
        self._index: dict[tuple[type, str], tuple[Contribution, str]] = {}
        self._directive_kinds: dict[type[ExecutionDirective], TaskKind] = {}
        for plugin in plugins:
            for contribution in plugin.contributions:
                self._add(contribution=contribution, owner=plugin.name)

    def _add(self, *, contribution: Contribution, owner: str) -> None:
        """Index one contribution under each of its keys, refusing clashes."""
        for key in contribution.keys:
            slot = (type(contribution), key)
            if slot in self._index:
                _, other = self._index[slot]
                raise PluginConflictError(
                    f"Plugins {other!r} and {owner!r} both contribute "
                    f"{contribution.point_name} {key!r}. Uninstall one of them."
                )
            self._index[slot] = (contribution, owner)
        if isinstance(contribution, TaskKind) and contribution.directive is not None:
            claimed = self._directive_kinds.get(contribution.directive)
            if claimed is not None:
                raise PluginConflictError(
                    f"Task kinds {claimed.name!r} and {contribution.name!r} both return "
                    f"{contribution.directive.__name__}; each kind needs its own directive."
                )
            self._directive_kinds[contribution.directive] = contribution

    @classmethod
    def discover(cls) -> PluginRegistry:
        """Load core's plugin and every loadable plugin in the entry-point group.

        Entry points load in name order. One that fails to import, is not a
        ``Plugin``, or claims a key an earlier plugin already holds is skipped
        and recorded in ``failures``.
        """
        from ginkgo.plugins.builtin import BUILTIN

        loaded = [BUILTIN]
        failures: list[PluginLoadFailure] = []
        for entry_point in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda ep: ep.name):
            try:
                plugin = entry_point.load()
            except Exception as exc:
                failures.append(
                    PluginLoadFailure(
                        entry_point=entry_point.name,
                        target=entry_point.value,
                        reason=f"failed to load: {exc}",
                    )
                )
                continue
            if not isinstance(plugin, Plugin):
                failures.append(
                    PluginLoadFailure(
                        entry_point=entry_point.name,
                        target=entry_point.value,
                        reason=f"is a {type(plugin).__name__}, not a ginkgo Plugin",
                    )
                )
                continue
            # A plugin that clashes with one already loaded is skipped like one
            # that fails to import, so it cannot take core or the others down.
            try:
                cls(plugins=(*loaded, plugin))
            except PluginConflictError as exc:
                failures.append(
                    PluginLoadFailure(
                        entry_point=entry_point.name,
                        target=entry_point.value,
                        reason=str(exc),
                    )
                )
                continue
            loaded.append(plugin)
        return cls(plugins=tuple(loaded), failures=tuple(failures))

    def get(self, point: type[C], key: str) -> C:
        """Return the contribution to *point* registered under *key*.

        Raises
        ------
        PluginNotFoundError
            If no loaded plugin contributes it. The message lists what is
            available, the first-party package that provides the key if there
            is one, and any plugin that failed to load.
        """
        found = self._index.get((point, key))
        if found is None:
            raise PluginNotFoundError(self._not_found_message(point=point, key=key))
        contribution, _ = found
        assert isinstance(contribution, point)
        return contribution

    def all(self, point: type[C]) -> list[C]:
        """Return every contribution to *point*, once each, in load order."""
        seen: list[C] = []
        for (kind, _), (contribution, _) in self._index.items():
            if kind is point and isinstance(contribution, point) and contribution not in seen:
                seen.append(contribution)
        return seen

    def has(self, point: type[Contribution], key: str) -> bool:
        """Return whether a loaded plugin contributes *key* to *point*."""
        return (point, key) in self._index

    def keys(self, point: type[Contribution]) -> list[str]:
        """Return the keys registered for *point*, sorted."""
        return sorted(key for kind, key in self._index if kind is point)

    def kind_for_directive(self, directive_type: type[ExecutionDirective]) -> TaskKind | None:
        """Return the task kind that returns *directive_type*, if any."""
        return self._directive_kinds.get(directive_type)

    def _not_found_message(self, *, point: type[Contribution], key: str) -> str:
        available = ", ".join(repr(name) for name in self.keys(point)) or "none"
        message = (
            f"No installed plugin provides {point.point_name} {key!r}. Available: {available}."
        )
        provider = first_party_provider(point, key)
        if provider is not None:
            message += f" Install {provider} to add it."
        if self.failures:
            skipped = "; ".join(failure.describe() for failure in self.failures)
            message += f" Plugins that failed to load: {skipped}."
        return message


_active: PluginRegistry | None = None


def plugin_registry() -> PluginRegistry:
    """Return the process's plugin registry, discovering it on first use."""
    global _active
    if _active is None:
        _active = PluginRegistry.discover()
    return _active


@contextmanager
def use_plugin_registry(registry: PluginRegistry) -> Iterator[PluginRegistry]:
    """Make *registry* the process's registry for the duration of the block.

    For tests and embedders that need a known set of plugins regardless of
    what is installed. The previous registry is restored on exit.
    """
    global _active
    previous = _active
    _active = registry
    try:
        yield registry
    finally:
        _active = previous
