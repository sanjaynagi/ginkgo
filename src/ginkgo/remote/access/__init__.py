"""Remote input access strategies.

Provides a uniform protocol (:class:`RemoteInputAccess`) for materialising
a remote object inside a worker pod, with two concrete strategies:

- :class:`StagedAccess` — downloads the full object into local disk before
  the task runs. This is the historical behaviour and the default.
- :class:`MountedAccess` — mounts the remote prefix through a FUSE driver
  (``gcsfuse``, ``mountpoint-s3``, ``rclone mount``) and exposes the
  mount path as a normal filesystem path. Bytes are pulled on demand.

Both strategies yield plain local paths so the task body is unchanged.
Per-input policy is resolved via :func:`resolve_access` which layers
constructor argument → task decorator → config default → auto-enable
heuristic.
"""

from __future__ import annotations

from ginkgo.remote.access.mounted import MountedAccess
from ginkgo.remote.access.protocol import (
    FUSE_FILE_TYPE,
    FUSE_FOLDER_TYPE,
    AccessStats,
    PerInputStats,
    RemoteInputAccess,
    decode_fuse_ref,
    encode_fuse_ref,
    is_fuse_ref,
)
from ginkgo.remote.access.resolver import (
    AccessConfig,
    TaskAccessPolicy,
    load_access_config,
    resolve_access,
)
from ginkgo.remote.access.staged import StagedAccess

__all__ = [
    "AccessConfig",
    "AccessStats",
    "FUSE_FILE_TYPE",
    "FUSE_FOLDER_TYPE",
    "MountedAccess",
    "PerInputStats",
    "RemoteInputAccess",
    "StagedAccess",
    "TaskAccessPolicy",
    "decode_fuse_ref",
    "encode_fuse_ref",
    "is_fuse_ref",
    "load_access_config",
    "resolve_access",
]
