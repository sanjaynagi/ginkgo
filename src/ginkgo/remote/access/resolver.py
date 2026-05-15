"""Per-input access-policy resolver.

Layers (highest precedence first):

1. Explicit ``access=...`` on the ref (``remote_file(..., access="fuse")``).
2. Task decorator default (``@task(remote_input_access="fuse")``).
3. Config default (``[remote.access] default = "stage" | "fuse"``).
4. Auto-enable heuristic (off by default; gated on size threshold,
   driver availability, doctor probe freshness).

Returns a canonical policy string — one of:

- ``"stage"`` — download via :class:`StagedAccess`.
- ``"fuse"`` — user / config opted in explicitly.
- ``"fuse (auto)"`` — auto-enable heuristic fired.
- ``"stage (fallback)"`` — used by the worker after a mount failure; not
  produced by this resolver directly.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from ginkgo.core.remote import RemoteRef


@dataclass(kw_only=True)
class AccessConfig:
    """Resolved config snapshot consumed by :func:`resolve_access`.

    Parameters
    ----------
    default : str
        Default access mode when nothing else decides (``"stage"`` in the
        typical config).
    auto_fuse : bool
        Whether the auto-enable heuristic is active.
    auto_fuse_min_bytes : int
        Byte threshold at which auto-fuse promotes a ref.
    pattern_defaults : tuple[tuple[str, str], ...]
        List of ``(glob, mode)`` pairs. First match wins over ``default``.
    doctor_ok : bool
        Whether the cluster passed the FUSE doctor probe recently.
    """

    default: str = "stage"
    auto_fuse: bool = False
    auto_fuse_min_bytes: int = 2 * 1024**3
    pattern_defaults: tuple[tuple[str, str], ...] = ()
    doctor_ok: bool = True

    @classmethod
    def from_mapping(cls, mapping: dict[str, Any] | None) -> AccessConfig:
        """Build an :class:`AccessConfig` from a ``[remote.access]`` dict."""
        if not mapping:
            return cls()
        default = str(mapping.get("default", "stage"))
        auto_fuse = bool(mapping.get("auto_fuse", False))
        auto_fuse_min_bytes = int(mapping.get("auto_fuse_min_bytes", 2 * 1024**3))

        raw_patterns = mapping.get("default_for_pattern", ()) or ()
        pattern_defaults: list[tuple[str, str]] = []
        for entry in raw_patterns:
            if not isinstance(entry, dict):
                continue
            glob = entry.get("glob")
            mode = entry.get("access")
            if isinstance(glob, str) and isinstance(mode, str):
                pattern_defaults.append((glob, mode))
        return cls(
            default=default,
            auto_fuse=auto_fuse,
            auto_fuse_min_bytes=auto_fuse_min_bytes,
            pattern_defaults=tuple(pattern_defaults),
            doctor_ok=True,
        )


@dataclass(kw_only=True)
class TaskAccessPolicy:
    """Task-level streaming opt-ins extracted from the decorator.

    Parameters
    ----------
    remote_input_access : str | None
        Per-task default mode (``"stage"`` / ``"fuse"`` / ``None``).
    streaming_compatible : bool
        ``False`` when the task declares it cannot tolerate streaming;
        blocks fuse auto-promotion and explicit fuse selection.
    fuse_prefetch : dict[str, str]
        ``glob → strategy`` map for predictive prefetch. Plumbed
        through events only; no driver currently consumes them.
    """

    remote_input_access: str | None = None
    streaming_compatible: bool = True
    fuse_prefetch: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_task_def(cls, task_def: Any) -> TaskAccessPolicy:
        """Build a policy view from a :class:`TaskDef` instance."""
        prefetch_items = getattr(task_def, "fuse_prefetch", ()) or ()
        return cls(
            remote_input_access=getattr(task_def, "remote_input_access", None),
            streaming_compatible=getattr(task_def, "streaming_compatible", True),
            fuse_prefetch=dict(prefetch_items),
        )


def resolve_access(
    *,
    ref: RemoteRef,
    task_policy: TaskAccessPolicy | None = None,
    config: AccessConfig | None = None,
    known_size: int | None = None,
    driver_available: bool = True,
) -> str:
    """Resolve the access policy for a single ref.

    Parameters
    ----------
    ref : RemoteRef
        The reference under consideration.
    task_policy : TaskAccessPolicy | None
        Extracted from the TaskDef; ``None`` treats the task as having no
        declared preference.
    config : AccessConfig | None
        ``[remote.access]`` config; ``None`` uses dataclass defaults.
    known_size : int | None
        Object size in bytes (from a prior HEAD call) used by the
        auto-enable heuristic. ``None`` disables size-gated promotion.
    driver_available : bool
        Whether the scheme's driver is considered available. When
        ``False``, fuse selections degrade to ``"stage"``.

    Returns
    -------
    str
        Resolved policy string.
    """
    config = config or AccessConfig()
    task_policy = task_policy or TaskAccessPolicy()

    # 1. Explicit ref.access always wins (bypass streaming_compatible).
    if ref.access == "stage":
        return "stage"
    if ref.access == "fuse":
        if not task_policy.streaming_compatible:
            return "stage"
        return "fuse" if driver_available else "stage"

    # 2. Task decorator default.
    if task_policy.remote_input_access is not None:
        mode = task_policy.remote_input_access
        if mode == "fuse":
            if not task_policy.streaming_compatible:
                return "stage"
            return "fuse" if driver_available else "stage"
        return mode  # "stage"

    # 3. Pattern-based config default.
    pattern_mode = _match_pattern(ref=ref, patterns=config.pattern_defaults)
    if pattern_mode is not None:
        if pattern_mode == "fuse":
            if not task_policy.streaming_compatible or not driver_available:
                return "stage"
            return "fuse"
        return pattern_mode

    # 4. Auto-enable heuristic.
    if (
        config.auto_fuse
        and task_policy.streaming_compatible
        and driver_available
        and config.doctor_ok
        and known_size is not None
        and known_size >= config.auto_fuse_min_bytes
    ):
        return "fuse (auto)"

    # 5. Config default.
    return config.default


def _match_pattern(
    *,
    ref: RemoteRef,
    patterns: Iterable[tuple[str, str]],
) -> str | None:
    """Return the first matching pattern's mode, or ``None``."""
    for glob, mode in patterns:
        if fnmatch.fnmatch(ref.key, glob) or fnmatch.fnmatch(ref.uri, glob):
            return mode
    return None


def load_access_config(
    *,
    project_root: Path | None = None,
    doctor_ok: bool = True,
) -> AccessConfig:
    """Load ``[remote.access]`` from the runtime config.

    Parameters
    ----------
    project_root : Path | None
        Directory containing ``ginkgo.toml``. Defaults to the current
        working directory.
    doctor_ok : bool
        Whether the cluster has a recent passing FUSE doctor probe.

    Returns
    -------
    AccessConfig
    """
    from ginkgo.config import load_runtime_config

    root = project_root if project_root is not None else Path.cwd()
    try:
        config = load_runtime_config(project_root=root)
    except Exception:  # noqa: BLE001
        config = {}
    remote = config.get("remote", {}) if isinstance(config, dict) else {}
    access = remote.get("access") if isinstance(remote, dict) else None
    parsed = AccessConfig.from_mapping(access if isinstance(access, dict) else None)
    if doctor_ok is False:
        parsed = AccessConfig(
            default=parsed.default,
            auto_fuse=parsed.auto_fuse,
            auto_fuse_min_bytes=parsed.auto_fuse_min_bytes,
            pattern_defaults=parsed.pattern_defaults,
            doctor_ok=False,
        )
    return parsed
