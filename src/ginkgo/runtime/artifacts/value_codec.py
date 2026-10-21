"""Value codecs shared by process transport and cache storage."""

from __future__ import annotations

import base64
import io
import logging
import pickle
import sys
import types
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, ClassVar, TYPE_CHECKING

import numpy as np
import pandas as pd

from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.runtime.module_loader import (
    CACHE_IDENTITY_MODULE,
    import_workflow_attribute,
    workflow_module_reference,
)

if TYPE_CHECKING:
    from ginkgo.runtime.artifacts.artifact_store import ArtifactStore

logger = logging.getLogger(__name__)

INLINE_BYTES_LIMIT = 256 * 1024


class CodecError(GinkgoError, TypeError):
    """Raised when a value cannot be encoded by Ginkgo."""

    failure_kind: ClassVar[FailureKind] = "serialization_error"


def encode_value(
    value: Any,
    *,
    base_dir: Path,
    artifact_store: ArtifactStore | None = None,
    inline_limit: int = INLINE_BYTES_LIMIT,
) -> Any:
    """Encode a Python value into a JSON-safe payload with optional artifacts.

    Parameters
    ----------
    value : Any
        The value to encode.
    base_dir : Path
        Directory for ephemeral artifact storage (used by process transport).
    artifact_store : ArtifactStore | None
        When provided, large binary payloads are stored through the artifact
        store instead of writing to *base_dir*.  Used by cache persistence.
    inline_limit : int
        Byte threshold below which binary payloads are base64-inlined.
    """
    # file/folder/tmp_dir are str subclasses, so they must be checked
    # before the generic str branch below short-circuits them into bare
    # strings.
    if isinstance(value, file):
        return _encode_path_marker(kind="file", value=value)

    if isinstance(value, folder):
        return _encode_path_marker(kind="folder", value=value)

    if isinstance(value, tmp_dir):
        return {"__ginkgo_type__": "tmp_dir", "value": str(value)}

    # Exact types only: a subclass such as ``np.float64`` would otherwise be
    # stored as JSON and come back from the cache as a plain ``float``, so a
    # consumer would see a different value, and hash a different input, on
    # a cache hit than on a fresh run.
    if value is None or type(value) in (bool, int, float, str):
        return value

    # Exact containers only, for the same reason: a ``namedtuple``,
    # ``defaultdict`` or ``OrderedDict`` would come back as the plain builtin,
    # so it is pickled whole instead.
    if type(value) is list:
        return {
            "__ginkgo_type__": "list",
            "items": [
                encode_value(
                    item,
                    base_dir=base_dir,
                    artifact_store=artifact_store,
                    inline_limit=inline_limit,
                )
                for item in value
            ],
        }

    if type(value) is tuple:
        return {
            "__ginkgo_type__": "tuple",
            "items": [
                encode_value(
                    item,
                    base_dir=base_dir,
                    artifact_store=artifact_store,
                    inline_limit=inline_limit,
                )
                for item in value
            ],
        }

    if type(value) is dict:
        # Fuse-streamed marker dicts produced by the driver-side resolver
        # are JSON-safe already; pass them through untouched so the worker
        # receives the original payload unchanged.
        from ginkgo.remote.access.protocol import is_fuse_ref

        if is_fuse_ref(value):
            return value
        return {
            "__ginkgo_type__": "dict",
            "items": [
                {
                    "key": encode_value(
                        key,
                        base_dir=base_dir,
                        artifact_store=artifact_store,
                        inline_limit=inline_limit,
                    ),
                    "value": encode_value(
                        item,
                        base_dir=base_dir,
                        artifact_store=artifact_store,
                        inline_limit=inline_limit,
                    ),
                }
                for key, item in value.items()
            ],
        }

    codec_name, data, extension = _encode_bytes(value)
    return _encode_binary_payload(
        codec_name=codec_name,
        data=data,
        extension=extension,
        base_dir=base_dir,
        artifact_store=artifact_store,
        inline_limit=inline_limit,
    )


def decode_value(
    payload: Any,
    *,
    base_dir: Path,
    artifact_store: ArtifactStore | None = None,
) -> Any:
    """Restore a Python value from an encoded payload.

    Parameters
    ----------
    payload : Any
        Encoded payload from :func:`encode_value`.
    base_dir : Path
        Base directory for resolving ephemeral artifact paths.
    artifact_store : ArtifactStore | None
        When provided, artifact-backed binary payloads are read through the
        store instead of from *base_dir*.
    """
    if not isinstance(payload, dict):
        return payload

    kind = payload.get("__ginkgo_type__")
    if kind == "file":
        return _decode_path_marker(marker_cls=file, payload=payload)
    if kind == "folder":
        return _decode_path_marker(marker_cls=folder, payload=payload)
    if kind == "tmp_dir":
        return tmp_dir(payload["value"])
    # Only a dict item carries a tag; a scalar item is its own value, so a long
    # list of numbers decodes without a call per element.
    if kind == "list":
        return [
            decode_value(item, base_dir=base_dir, artifact_store=artifact_store)
            if type(item) is dict
            else item
            for item in payload["items"]
        ]
    if kind == "tuple":
        return tuple(
            decode_value(item, base_dir=base_dir, artifact_store=artifact_store)
            if type(item) is dict
            else item
            for item in payload["items"]
        )
    if kind == "dict":
        return {
            decode_value(
                item["key"], base_dir=base_dir, artifact_store=artifact_store
            ): decode_value(
                item["value"],
                base_dir=base_dir,
                artifact_store=artifact_store,
            )
            for item in payload["items"]
        }
    if kind == "binary":
        return _decode_binary_payload(
            payload=payload, base_dir=base_dir, artifact_store=artifact_store
        )

    _reject_unknown_tag(payload)
    return payload


def check_encoded(payload: Any) -> None:
    """Raise if :func:`decode_value` could not read *payload*, without reading bytes.

    Walks the encoded structure and checks each tag, so a cache entry written
    by a codec this version no longer has is diagnosable without loading the
    artifacts behind it.

    Parameters
    ----------
    payload : Any
        Encoded payload from :func:`encode_value`.

    Raises
    ------
    CodecError
        If the payload carries a tag this codec cannot decode.
    """
    if not isinstance(payload, dict):
        return
    kind = payload.get("__ginkgo_type__")
    if kind in {"list", "tuple"}:
        for item in payload["items"]:
            check_encoded(item)
        return
    if kind == "dict":
        for item in payload["items"]:
            check_encoded(item["key"])
            check_encoded(item["value"])
        return
    if kind in {"file", "folder", "tmp_dir", "binary"}:
        return
    _reject_unknown_tag(payload)


def _reject_unknown_tag(payload: dict[str, Any]) -> None:
    """Raise for a tag no decoder branch handled.

    Fuse markers pass through, mirroring ``encode_value``; the worker hydrates
    them after decoding. Any other tag was written by a codec this version no
    longer has, and returning it as a raw dict would hand the task a value it
    never produced.
    """
    from ginkgo.remote.access.protocol import is_fuse_ref

    kind = payload.get("__ginkgo_type__")
    if kind is not None and not is_fuse_ref(payload):
        raise CodecError(f"Unknown encoded value type: {kind!r}")


def _encode_path_marker(*, kind: str, value: file | folder) -> dict[str, Any]:
    """Encode a ``file``/``folder`` marker."""
    return {"__ginkgo_type__": kind, "value": str(value)}


def _decode_path_marker(*, marker_cls: type[file] | type[folder], payload: dict[str, Any]) -> Any:
    """Decode a ``file``/``folder`` marker."""
    return marker_cls(payload["value"])


SUMMARY_ITEM_LIMIT = 20
"""How many items without a path a container summary keeps.

A summary describes a value; it is not a copy of it. Items naming a
``file`` or ``folder`` are always kept, because they are what provenance
links to artifacts.
"""


def summarise_value(value: Any) -> Any:
    """Return a compact metadata view of a value for cache manifests.

    A list, tuple or dict keeps its first :data:`SUMMARY_ITEM_LIMIT` items
    plus every item naming a file or folder, records how many it left out
    under ``omitted``, and its full ``length``.
    """
    # file/folder are str subclasses — check before the generic str branch.
    if isinstance(value, file):
        return {"type": "file", "value": str(value)}
    if isinstance(value, folder):
        return {"type": "folder", "value": str(value)}
    if isinstance(value, tmp_dir):
        return {"type": "tmp_dir", "value": str(value)}

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (list, tuple)):
        return {
            "type": _summary_type_name(value),
            **_capped_items(value, summarise=summarise_value),
            "length": len(value),
        }
    if isinstance(value, dict):
        pairs = sorted(value.items(), key=lambda pair: repr(pair[0]))
        return {
            "type": _summary_type_name(value),
            **_capped_items(pairs, summarise=_summarise_pair),
            "length": len(value),
        }

    if isinstance(value, np.ndarray):
        return {
            "type": "numpy.ndarray",
            "dtype": str(value.dtype),
            "shape": list(value.shape),
        }

    if isinstance(value, pd.DataFrame):
        return {
            "type": "pandas.DataFrame",
            "columns": list(map(str, value.columns)),
            "rows": int(len(value.index)),
        }

    return {"type": f"{type(value).__module__}.{type(value).__name__}"}


def _summary_type_name(value: Any) -> str:
    """Return a container's summary type: the builtin's name, or its subclass's."""
    if type(value) in (list, tuple, dict):
        return type(value).__name__
    return f"{type(value).__module__}.{type(value).__name__}"


def _summarise_pair(pair: tuple[Any, Any]) -> dict[str, Any]:
    """Summarise one dict item."""
    key, item = pair
    return {"key": summarise_value(key), "value": summarise_value(item)}


def _capped_items(items: Iterable[Any], *, summarise: Callable[[Any], Any]) -> dict[str, Any]:
    """Summarise the first items and every one naming a path; count the rest.

    An item past the limit is only summarised when it holds a path, so a
    large container costs a cheap scan rather than a summary per item.
    """
    kept: list[Any] = []
    omitted = 0
    for item in items:
        if len(kept) < SUMMARY_ITEM_LIMIT or _holds_a_path(item):
            kept.append(summarise(item))
        else:
            omitted += 1
    return {"items": kept, "omitted": omitted} if omitted else {"items": kept}


def _holds_a_path(value: Any) -> bool:
    """Return whether a value is, or contains, a file or folder."""
    if isinstance(value, (file, folder)):
        return True
    if isinstance(value, (list, tuple)):
        return any(_holds_a_path(item) for item in value)
    if isinstance(value, dict):
        return any(_holds_a_path(key) or _holds_a_path(item) for key, item in value.items())
    return False


def hash_value_bytes(value: Any) -> tuple[str, str]:
    """Return the codec name and BLAKE3 digest for a Python value."""
    if isinstance(value, np.ndarray) and value.dtype.hasobject is False:
        return "numpy.ndarray", _hash_numpy_array(value)
    if isinstance(value, pd.DataFrame):
        try:
            return "pandas.DataFrame", _hash_pandas_dataframe(value)
        except Exception:
            # Fall back to the existing serialized-byte path for frames pandas
            # cannot hash directly (for example some exotic object payloads).
            pass

    from ginkgo.core.hashing import hash_bytes

    codec_name, data, _extension = _encode_bytes(value, for_identity=True)
    digest = hash_bytes(data)
    return codec_name, digest


def ensure_serializable(value: Any, *, label: str) -> None:
    """Validate that a value can be encoded by the Ginkgo codec registry."""
    if value is None or isinstance(
        value,
        (bool, int, float, str, file, folder, tmp_dir),
    ):
        return

    # Mirrors ``encode_value``: only exact containers are encoded item by item.
    if type(value) in (list, tuple):
        for index, item in enumerate(value):
            ensure_serializable(item, label=f"{label}[{index}]")
        return

    if type(value) is dict:
        for key, item in value.items():
            ensure_serializable(key, label=f"{label}.key")
            ensure_serializable(item, label=f"{label}[{key!r}]")
        return

    try:
        _encode_bytes(value)
    except Exception as exc:  # pragma: no cover - error path varies by value type
        raise CodecError(
            f"{label} cannot be serialized for process execution: "
            f"{type(value).__module__}.{type(value).__name__}"
        ) from exc


def _encode_binary_payload(
    *,
    codec_name: str,
    data: bytes,
    extension: str,
    base_dir: Path,
    artifact_store: ArtifactStore | None = None,
    inline_limit: int,
) -> dict[str, Any]:
    if len(data) <= inline_limit:
        return {
            "__ginkgo_type__": "binary",
            "codec": codec_name,
            "storage": "inline",
            "data": base64.b64encode(data).decode("ascii"),
        }

    # Route through the artifact store when available (cache persistence).
    if artifact_store is not None:
        record = artifact_store.store_bytes(data=data, extension=extension)
        return {
            "__ginkgo_type__": "binary",
            "artifact_id": record.artifact_id,
            "codec": codec_name,
            "storage": "artifact_store",
        }

    # Ephemeral transport fallback: write to base_dir/artifacts/.
    from ginkgo.core.hashing import hash_bytes

    artifacts_dir = base_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    digest = hash_bytes(data)
    artifact_name = f"{digest}.{extension}"
    artifact_path = artifacts_dir / artifact_name
    if not artifact_path.exists():
        artifact_path.write_bytes(data)

    return {
        "__ginkgo_type__": "binary",
        "artifact": f"artifacts/{artifact_name}",
        "codec": codec_name,
        "sha256": digest,
        "storage": "artifact",
    }


def _decode_binary_payload(
    *,
    payload: dict[str, Any],
    base_dir: Path,
    artifact_store: ArtifactStore | None = None,
) -> Any:
    storage = payload["storage"]

    if storage == "inline":
        data = base64.b64decode(payload["data"])
    elif storage == "artifact_store" and artifact_store is not None:
        data = artifact_store.read_bytes(artifact_id=payload["artifact_id"])
    else:
        # Ephemeral transport or legacy cache entries.
        data = (base_dir / payload["artifact"]).read_bytes()

    return _decode_bytes(codec_name=payload["codec"], data=data)


def _encode_bytes(value: Any, *, for_identity: bool = False) -> tuple[str, bytes, str]:
    """Return the codec name, bytes and file extension for a binary value.

    Parameters
    ----------
    value : Any
        The value to encode.
    for_identity : bool
        Whether the bytes are only hashed, never decoded. Classes and
        functions of a single-file workflow are then recorded under one
        location-independent name, so a renamed or copied workflow hashes its
        values the same.
    """
    if isinstance(value, np.ndarray):
        buffer = io.BytesIO()
        np.save(buffer, value, allow_pickle=True)
        return "numpy.npy", buffer.getvalue(), "npy"

    if isinstance(value, pd.DataFrame):
        parquet_bytes = _try_encode_dataframe_parquet(value)
        if parquet_bytes is not None:
            return "pandas.parquet", parquet_bytes, "parquet"

    buffer = io.BytesIO()
    try:
        _WorkflowPickler(buffer, for_identity=for_identity).dump(value)
    except (pickle.PicklingError, TypeError, AttributeError) as exc:
        raise CodecError(
            f"Cannot pickle a {type(value).__module__}.{type(value).__qualname__} value: "
            f"{exc}. Values other than plain lists, tuples, dicts and scalars are "
            "pickled, so their classes must be importable: defined at module "
            "level, not inside a function."
        ) from exc
    return "python.pickle", buffer.getvalue(), "pkl"


class _WorkflowPickler(pickle.Pickler):
    """Pickle that records single-file workflow classes by a portable reference.

    Such a class lives in a module named after the workflow's absolute path in
    this process (see :data:`~ginkgo.runtime.module_loader.USER_MODULE_PREFIX`),
    which a worker or a later run cannot import. It is recorded instead as a
    call to :func:`~ginkgo.runtime.module_loader.import_workflow_attribute`,
    which finds the workflow again wherever the value is decoded.
    """

    def __init__(self, file: io.BytesIO, *, for_identity: bool) -> None:
        super().__init__(file, protocol=5)
        self._for_identity = for_identity

    def reducer_override(self, obj: Any) -> Any:
        if not isinstance(obj, (type, types.FunctionType)):
            return NotImplemented
        module_name = getattr(obj, "__module__", None)
        if not isinstance(module_name, str):
            return NotImplemented
        reference = workflow_module_reference(module_name)
        if reference is None:
            return NotImplemented
        # Only what pickle itself could find by name: a lambda or a class
        # defined inside a function keeps pickle's own error.
        qualname = obj.__qualname__
        found: Any = sys.modules[module_name]
        for part in qualname.split("."):
            found = getattr(found, part, None)
        if found is not obj:
            return NotImplemented
        if self._for_identity:
            reference = CACHE_IDENTITY_MODULE
        return import_workflow_attribute, (reference, qualname)


def _decode_bytes(*, codec_name: str, data: bytes) -> Any:
    if codec_name == "numpy.npy":
        return np.load(io.BytesIO(data), allow_pickle=True)

    if codec_name == "pandas.parquet":
        return pd.read_parquet(io.BytesIO(data))

    if codec_name == "python.pickle":
        return pickle.loads(data)

    raise CodecError(f"Unknown value codec: {codec_name}")


def _try_encode_dataframe_parquet(value: Any) -> bytes | None:
    """Encode a DataFrame to Parquet bytes, or return None if it cannot.

    Returning ``None`` makes the caller fall back to pickle, which is not
    portable across pandas/Python versions. The Parquet failure is logged
    rather than swallowed silently so the format downgrade is diagnosable.
    """
    # ``index=None`` stores a ``RangeIndex`` as metadata so it decodes as a
    # ``RangeIndex``; ``index=True`` would materialise it as an ``Index``,
    # which hashes differently from the frame that was encoded.
    buffer = io.BytesIO()
    try:
        value.to_parquet(buffer, index=None)
    except Exception:
        logger.warning(
            "DataFrame could not be encoded as Parquet; falling back to "
            "pickle (not portable across pandas/Python versions).",
            exc_info=True,
        )
        return None
    return buffer.getvalue()


def _hash_numpy_array(value: Any) -> str:
    """Return a stable BLAKE3 digest for a non-object NumPy array."""
    from ginkgo.core.hashing import new_hasher

    normalized = np.ascontiguousarray(value)
    hasher = new_hasher()

    # Include array metadata so equal bytes with different logical arrays diverge.
    hasher.update(normalized.dtype.str.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(str(normalized.ndim).encode("ascii"))
    hasher.update(b"\0")
    for dim in normalized.shape:
        hasher.update(str(dim).encode("ascii"))
        hasher.update(b"\0")

    hasher.update(memoryview(normalized).cast("B"))
    return hasher.hexdigest()


def _hash_pandas_dataframe(value: pd.DataFrame) -> str:
    """Return a stable BLAKE3 digest for a pandas DataFrame."""
    from ginkgo.core.hashing import new_hasher

    hasher = new_hasher()

    # Include structural metadata so equivalent row hashes with different
    # labels, dtype declarations, or axis metadata still diverge.
    metadata = (
        tuple(value.columns.tolist()),
        tuple(map(str, value.dtypes.tolist())),
        tuple(value.columns.names),
        tuple(value.index.names),
        type(value.index).__module__,
        type(value.index).__qualname__,
        tuple(map(str, getattr(value.index, "dtypes", [value.index.dtype]))),
    )
    hasher.update(pickle.dumps(metadata, protocol=5))

    row_hashes = pd.util.hash_pandas_object(value, index=True, categorize=True)
    normalized = np.ascontiguousarray(row_hashes.to_numpy(dtype=np.uint64, copy=False))
    hasher.update(memoryview(normalized).cast("B"))
    return hasher.hexdigest()
