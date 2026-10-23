"""Worker-side hydration for remote inputs the worker reads itself.

The driver emits :data:`REMOTE_FILE_TYPE` / :data:`REMOTE_FOLDER_TYPE` marker
dicts in the task payload instead of staging those inputs. This module walks
the payload and replaces each marker with a ``file`` / ``folder`` value: a
``direct`` input is downloaded from its source, a ``fuse`` input is mounted
(its bucket once), falling back to a download if the mount fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ginkgo.core.remote import RemoteFileRef, RemoteRef
from ginkgo.core.types import file, folder
from ginkgo.remote.access.mounted import MountedAccess
from ginkgo.remote.access.protocol import decode_remote_input, is_fuse_policy, is_remote_input
from ginkgo.remote.access.staged import StagedAccess
from ginkgo.remote.staging import StagingCache
from ginkgo.store.sqlite import MEMORY


@dataclass(kw_only=True)
class Hydration:
    """What hydrating a payload's remote inputs produced.

    Parameters
    ----------
    args : dict[str, Any]
        The arguments, with every marker replaced by a local path.
    mounted_access : MountedAccess | None
        The mounts made, for the caller to read stats from and close.
    digests : dict[str, str]
        The digest of each input downloaded rather than mounted, by URI.
    """

    args: dict[str, Any]
    mounted_access: MountedAccess | None
    digests: dict[str, str]


def hydrate_remote_inputs(
    *,
    args: dict[str, Any],
    staging_root: Path | None = None,
    mounted_access: MountedAccess | None = None,
    staged_access: StagedAccess | None = None,
) -> Hydration:
    """Replace remote-input markers in ``args`` with local paths.

    Parameters
    ----------
    args : dict[str, Any]
        Worker-side payload args, potentially containing remote-input markers.
    staging_root : Path | None
        Where downloaded inputs go. ``None`` uses the default staging root.
    mounted_access : MountedAccess | None
        Strategy used to mount buckets for ``fuse`` inputs. A default
        :class:`MountedAccess` is constructed on first need.
    staged_access : StagedAccess | None
        Strategy used to download ``direct`` inputs, and ``fuse`` inputs
        whose mount failed. Constructed on first need when ``None``.

    Returns
    -------
    Hydration
        The rewritten args, the mounts made, and the downloads' digests.
    """
    state = _Hydrator(
        staging_root=staging_root,
        mounted_access=mounted_access,
        staged_access=staged_access,
    )
    rewritten = state.walk(args)
    return Hydration(
        args=rewritten,
        mounted_access=state.mounted_access,
        digests=state.staged_access.digests() if state.staged_access is not None else {},
    )


class _Hydrator:
    def __init__(
        self,
        *,
        staging_root: Path | None,
        mounted_access: MountedAccess | None,
        staged_access: StagedAccess | None,
    ) -> None:
        self.staging_root = staging_root
        self.mounted_access = mounted_access
        self.staged_access = staged_access

    def walk(self, value: Any) -> Any:
        if is_remote_input(value):
            return self._hydrate(value=value)
        if isinstance(value, dict):
            return {key: self.walk(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.walk(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self.walk(item) for item in value)
        return value

    def _hydrate(self, *, value: dict[str, Any]) -> Any:
        ref, policy = decode_remote_input(value)
        if not is_fuse_policy(policy):
            return self._download(ref=ref)
        try:
            if self.mounted_access is None:
                self.mounted_access = MountedAccess()
            if isinstance(ref, RemoteFileRef):
                return file(str(self.mounted_access.materialize_file(ref=ref)))
            return folder(str(self.mounted_access.materialize_folder(ref=ref)))  # type: ignore[arg-type]
        except Exception as exc:  # noqa: BLE001
            # Fall back to downloading this one input.
            if self.mounted_access is not None:
                self.mounted_access.stats().fallback_reason = (
                    (self.mounted_access.stats().fallback_reason or "") + f" {exc}"
                ).strip()
            return self._download(ref=ref)

    def _download(self, *, ref: RemoteRef) -> Any:
        """Download one input from its source; staging checks its identity."""
        if self.staged_access is None:
            # In a worker: the pod has nowhere to write an index that
            # anything would read back, so the rows go to memory.
            self.staged_access = StagedAccess(
                policy="direct",
                cache=StagingCache(root=self.staging_root, db_path=MEMORY),
            )
        try:
            if isinstance(ref, RemoteFileRef):
                return file(str(self.staged_access.materialize_file(ref=ref)))
            return folder(str(self.staged_access.materialize_folder(ref=ref)))  # type: ignore[arg-type]
        except PermissionError as exc:
            raise PermissionError(
                f"The worker cannot read {ref.uri}: {exc}. Give it read access to the "
                'bucket, or use access="stage" so the driver downloads it instead.'
            ) from exc
