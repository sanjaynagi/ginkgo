"""Remote input access strategies.

Provides a uniform protocol (:class:`RemoteInputAccess`) for materialising
a remote object inside a worker pod, with two concrete strategies:

- :class:`StagedAccess` — downloads the full object into local disk before
  the task runs: on the driver for ``stage``, or in the worker for ``direct``,
  the default for a task dispatched to a remote executor.
- :class:`MountedAccess` — mounts the remote prefix through a FUSE driver
  (``gcsfuse``, ``mountpoint-s3``, ``rclone mount``) and exposes the
  mount path as a normal filesystem path. Bytes are pulled on demand.

Both strategies yield plain local paths so the task body is unchanged.
Per-input policy is resolved via :func:`resolve_access` which layers
constructor argument → task decorator → config default → auto-enable
heuristic.
"""

from __future__ import annotations

from ginkgo.remote.access.doctor import local_streaming_available
from ginkgo.remote.access.mounted import MountedAccess
from ginkgo.remote.access.protocol import (
    REMOTE_FILE_TYPE,
    REMOTE_FOLDER_TYPE,
    AccessStats,
    PerInputStats,
    RemoteInputAccess,
    decode_remote_input,
    encode_remote_input,
    is_fuse_input,
    is_remote_input,
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
    "MountedAccess",
    "PerInputStats",
    "REMOTE_FILE_TYPE",
    "REMOTE_FOLDER_TYPE",
    "RemoteInputAccess",
    "StagedAccess",
    "TaskAccessPolicy",
    "decode_remote_input",
    "encode_remote_input",
    "is_fuse_input",
    "is_remote_input",
    "load_access_config",
    "local_streaming_available",
    "resolve_access",
]
