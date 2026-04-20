"""Worker-side hydration for fuse-streamed refs.

The driver emits :data:`FUSE_FILE_TYPE` / :data:`FUSE_FOLDER_TYPE` marker
dicts in the task payload for inputs whose resolved access policy is
``fuse``. This module walks the payload, mounts each referenced bucket
once, and replaces the markers with ``file`` / ``folder`` values
pointing at the mount.
"""

from __future__ import annotations

from typing import Any

from ginkgo.core.types import file, folder
from ginkgo.remote.access.mounted import MountedAccess
from ginkgo.remote.access.protocol import (
    FUSE_FILE_TYPE,
    FUSE_FOLDER_TYPE,
    decode_fuse_ref,
)
from ginkgo.remote.access.staged import StagedAccess


def hydrate_fuse_refs(
    *,
    args: dict[str, Any],
    mounted_access: MountedAccess | None = None,
    fallback_access: StagedAccess | None = None,
) -> tuple[dict[str, Any], MountedAccess | None]:
    """Replace fuse marker dicts in ``args`` with local mount paths.

    Parameters
    ----------
    args : dict[str, Any]
        Worker-side payload args, potentially containing fuse markers.
    mounted_access : MountedAccess | None
        Strategy instance used to mount referenced buckets. A default
        :class:`MountedAccess` is constructed on first need.
    fallback_access : StagedAccess | None
        Strategy used when mounting fails. When ``None`` a
        :class:`StagedAccess` with policy ``"stage (fallback)"`` is
        lazily constructed.

    Returns
    -------
    tuple[dict[str, Any], MountedAccess | None]
        The rewritten args and the (possibly newly-created)
        :class:`MountedAccess` instance so the caller can read stats and
        later call :meth:`MountedAccess.close`.
    """
    state = _Hydrator(
        mounted_access=mounted_access,
        fallback_access=fallback_access,
    )
    rewritten = state.walk(args)
    return rewritten, state.mounted_access


class _Hydrator:
    def __init__(
        self,
        *,
        mounted_access: MountedAccess | None,
        fallback_access: StagedAccess | None,
    ) -> None:
        self.mounted_access = mounted_access
        self.fallback_access = fallback_access

    def walk(self, value: Any) -> Any:
        if isinstance(value, dict):
            tag = value.get("__ginkgo_type__")
            if tag in {FUSE_FILE_TYPE, FUSE_FOLDER_TYPE}:
                return self._hydrate_fuse(value=value)
            return {key: self.walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.walk(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self.walk(item) for item in value)
        return value

    def _hydrate_fuse(self, *, value: dict[str, Any]) -> Any:
        ref, _policy = decode_fuse_ref(value)
        try:
            if self.mounted_access is None:
                self.mounted_access = MountedAccess()
            if value["__ginkgo_type__"] == FUSE_FILE_TYPE:
                mount_path = self.mounted_access.materialize_file(ref=ref)  # type: ignore[arg-type]
                return file(str(mount_path))
            mount_path = self.mounted_access.materialize_folder(ref=ref)  # type: ignore[arg-type]
            return folder(str(mount_path))
        except Exception as exc:  # noqa: BLE001
            # Fall back to staged download of the single ref.
            if self.fallback_access is None:
                self.fallback_access = StagedAccess(policy="stage (fallback)")
            if self.mounted_access is not None:
                self.mounted_access.stats().fallback_reason = (
                    (self.mounted_access.stats().fallback_reason or "") + f" {exc}"
                ).strip()
            if value["__ginkgo_type__"] == FUSE_FILE_TYPE:
                path = self.fallback_access.materialize_file(ref=ref)  # type: ignore[arg-type]
                return file(str(path))
            path = self.fallback_access.materialize_folder(ref=ref)  # type: ignore[arg-type]
            return folder(str(path))
