"""Protocol and statistics for remote-input access strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef, RemoteRef

# Payload tags for fuse-streamed inputs. Driver side emits these instead
# of staging the object locally; worker side detects them and routes
# through :class:`MountedAccess` to produce a local mount path.
FUSE_FILE_TYPE = "fuse_file"
FUSE_FOLDER_TYPE = "fuse_folder"


def encode_fuse_ref(*, ref: RemoteRef, policy: str) -> dict[str, Any]:
    """Serialise a fuse-marked ref into a JSON-safe payload dict.

    Parameters
    ----------
    ref : RemoteRef
        The reference being streamed.
    policy : str
        Resolved policy string (``"fuse"`` or ``"fuse (auto)"``).
    """
    tag = FUSE_FILE_TYPE if isinstance(ref, RemoteFileRef) else FUSE_FOLDER_TYPE
    return {
        "__ginkgo_type__": tag,
        "uri": ref.uri,
        "scheme": ref.scheme,
        "bucket": ref.bucket,
        "key": ref.key,
        "namespace": ref.namespace,
        "version_id": ref.version_id,
        "policy": policy,
    }


def is_fuse_ref(value: Any) -> bool:
    """Return True when ``value`` is a dict marking a fuse-streamed ref."""
    return isinstance(value, dict) and value.get("__ginkgo_type__") in {
        FUSE_FILE_TYPE,
        FUSE_FOLDER_TYPE,
    }


def decode_fuse_ref(value: dict[str, Any]) -> tuple[RemoteRef, str]:
    """Rehydrate a fuse payload dict into a :class:`RemoteRef` + policy."""
    from ginkgo.core.remote import RemoteFileRef, RemoteFolderRef

    cls = RemoteFileRef if value["__ginkgo_type__"] == FUSE_FILE_TYPE else RemoteFolderRef
    ref = cls(
        uri=value["uri"],
        scheme=value["scheme"],
        bucket=value["bucket"],
        key=value["key"],
        namespace=value.get("namespace"),
        version_id=value.get("version_id"),
        access="fuse",
    )
    return ref, value.get("policy", "fuse")


@dataclass(kw_only=True)
class PerInputStats:
    """Counters for a single streamed input.

    Parameters
    ----------
    uri : str
        Remote URI the stats belong to.
    bytes_read : int
        Total bytes consumed by the task.
    range_requests : int
        Number of byte-range requests issued against the remote store.
    cache_hits : int
        Reads served from the driver's read-through cache.
    cache_bytes : int
        Bytes served from the driver's read-through cache.
    prefetch_hits : int
        Reads satisfied by predictive prefetch.
    """

    uri: str
    bytes_read: int = 0
    range_requests: int = 0
    cache_hits: int = 0
    cache_bytes: int = 0
    prefetch_hits: int = 0


@dataclass(kw_only=True)
class AccessStats:
    """Aggregate statistics for a single task's remote-input access.

    Parameters
    ----------
    policy : str
        Canonical policy string (``"stage"``, ``"fuse"``,
        ``"fuse (auto)"``, ``"stage (fallback)"``).
    mount_seconds : float
        Wall time spent mounting (fuse only).
    unmount_seconds : float
        Wall time spent unmounting (fuse only).
    per_input : dict[str, PerInputStats]
        Per-URI counters.
    fallback_reason : str | None
        Populated when the mount failed and the strategy fell back to
        staged downloads.
    """

    policy: str
    mount_seconds: float = 0.0
    unmount_seconds: float = 0.0
    per_input: dict[str, PerInputStats] = field(default_factory=dict)
    fallback_reason: str | None = None

    def to_dict(self) -> dict:
        """Return a JSON-serialisable view of the stats."""
        return {
            "policy": self.policy,
            "mount_seconds": self.mount_seconds,
            "unmount_seconds": self.unmount_seconds,
            "fallback_reason": self.fallback_reason,
            "per_input": {
                uri: {
                    "bytes_read": stats.bytes_read,
                    "range_requests": stats.range_requests,
                    "cache_hits": stats.cache_hits,
                    "cache_bytes": stats.cache_bytes,
                    "prefetch_hits": stats.prefetch_hits,
                }
                for uri, stats in self.per_input.items()
            },
        }


@runtime_checkable
class RemoteInputAccess(Protocol):
    """Strategy interface for materialising remote inputs on a worker.

    Implementations are stateful within a single worker / task invocation:
    ``materialize_*`` yields a local path, ``release`` tears down any
    transient resources, and ``stats`` returns the recorded counters.
    """

    def materialize_file(self, *, ref: RemoteFileRef) -> Path:
        """Return a local path to the remote file contents."""
        ...

    def materialize_folder(self, *, ref: RemoteFolderRef) -> Path:
        """Return a local directory path to the remote prefix contents."""
        ...

    def release(self, *, paths: Iterable[Path]) -> None:
        """Release any transient resources tied to materialised paths."""
        ...

    def stats(self) -> AccessStats:
        """Return recorded access statistics for this strategy instance."""
        ...
