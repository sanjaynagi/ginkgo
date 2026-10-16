"""Discover plugins and look up their contributions.

Core's own plugin is always loaded first. Other plugins are found through the
``ginkgo.plugins`` entry-point group, each naming a module attribute that is a
:class:`~ginkgo.plugins.api.Plugin`. Discovery happens once per process, on the
first lookup, so a run that never needs a plugin never imports one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import TypeVar

from ginkgo.errors import GinkgoError
from ginkgo.plugins.api import Contribution, Plugin

__all__ = [
    "ENTRY_POINT_GROUP",
    "PluginConflictError",
    "PluginLoadError",
    "PluginNotFoundError",
    "PluginRegistry",
    "plugin_registry",
]

ENTRY_POINT_GROUP = "ginkgo.plugins"

C = TypeVar("C", bound=Contribution)


class PluginConflictError(GinkgoError, RuntimeError):
    """Raised when two plugins contribute the same key to one extension point."""


class PluginLoadError(GinkgoError, RuntimeError):
    """Raised when an entry point does not load to a ``Plugin``."""


class PluginNotFoundError(GinkgoError, ValueError):
    """Raised when no installed plugin contributes the key asked for."""


@dataclass(frozen=True)
class PluginRegistry:
    """The installed plugins, indexed by extension point and key.

    Parameters
    ----------
    plugins : tuple[Plugin, ...]
        Every loaded plugin, core's own first.
    """

    plugins: tuple[Plugin, ...]
    _index: dict[tuple[type, str], tuple[Contribution, str]] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        index: dict[tuple[type, str], tuple[Contribution, str]] = {}
        for plugin in self.plugins:
            for contribution in plugin.contributions:
                slot = (type(contribution), contribution.key)
                if slot in index:
                    _, owner = index[slot]
                    raise PluginConflictError(
                        f"Plugins {owner!r} and {plugin.name!r} both contribute "
                        f"{contribution.point_name} {contribution.key!r}. "
                        "Uninstall one of them."
                    )
                index[slot] = (contribution, plugin.name)
        object.__setattr__(self, "_index", index)

    @classmethod
    def discover(cls) -> PluginRegistry:
        """Load core's plugin and every plugin in the entry-point group.

        Raises
        ------
        PluginLoadError
            If an entry point fails to import or is not a ``Plugin``.
        PluginConflictError
            If two plugins claim the same key.
        """
        from ginkgo.plugins.builtin import BUILTIN

        loaded = [BUILTIN]
        for entry_point in sorted(entry_points(group=ENTRY_POINT_GROUP), key=lambda ep: ep.name):
            try:
                plugin = entry_point.load()
            except Exception as exc:
                raise PluginLoadError(
                    f"Plugin entry point {entry_point.name!r} ({entry_point.value}) "
                    f"failed to load: {exc}"
                ) from exc
            if not isinstance(plugin, Plugin):
                raise PluginLoadError(
                    f"Plugin entry point {entry_point.name!r} ({entry_point.value}) "
                    f"is a {type(plugin).__name__}, not a ginkgo Plugin."
                )
            loaded.append(plugin)
        return cls(plugins=tuple(loaded))

    def get(self, point: type[C], key: str) -> C:
        """Return the contribution to *point* registered under *key*.

        Raises
        ------
        PluginNotFoundError
            If no installed plugin contributes it.
        """
        found = self._index.get((point, key))
        if found is None:
            available = ", ".join(repr(name) for name in self.keys(point)) or "none"
            raise PluginNotFoundError(
                f"No installed plugin provides {point.point_name} {key!r}. Available: {available}."
            )
        contribution, _ = found
        assert isinstance(contribution, point)
        return contribution

    def all(self, point: type[C]) -> list[C]:
        """Return every contribution to *point*, in load order."""
        return [
            contribution
            for (kind, _), (contribution, _) in self._index.items()
            if kind is point and isinstance(contribution, point)
        ]

    def keys(self, point: type[Contribution]) -> list[str]:
        """Return the keys registered for *point*, sorted."""
        return sorted(key for kind, key in self._index if kind is point)


_active: PluginRegistry | None = None


def plugin_registry() -> PluginRegistry:
    """Return the process's plugin registry, discovering it on first use."""
    global _active
    if _active is None:
        _active = PluginRegistry.discover()
    return _active
