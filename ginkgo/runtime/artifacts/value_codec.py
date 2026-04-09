"""Value codecs shared by process transport and cache storage."""

from __future__ import annotations

import base64
import io
import pickle
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from ginkgo.core.asset import AssetRef, AssetResult
from ginkgo.core.types import file, folder, tmp_dir

if TYPE_CHECKING:
    from ginkgo.runtime.artifacts.artifact_store import ArtifactStore

INLINE_BYTES_LIMIT = 256 * 1024


class CodecError(TypeError):
    """Raised when a value cannot be encoded by Ginkgo."""


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
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, file):
        return {"__ginkgo_type__": "file", "value": str(value)}

    if isinstance(value, folder):
        return {"__ginkgo_type__": "folder", "value": str(value)}

    if isinstance(value, AssetRef):
        return {"__ginkgo_type__": "asset_ref", "value": value.to_dict()}

    if isinstance(value, AssetResult):
        return {
            "__ginkgo_type__": "asset_result",
            "name": value.name,
            "kind": value.kind,
            "metadata": dict(value.metadata),
            "value": encode_value(
                value.value,
                base_dir=base_dir,
                artifact_store=artifact_store,
                inline_limit=inline_limit,
            ),
        }

    if isinstance(value, tmp_dir):
        return {"__ginkgo_type__": "tmp_dir", "value": str(value)}

    if isinstance(value, list):
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

    if isinstance(value, tuple):
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

    if isinstance(value, dict):
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
        return file(payload["value"])
    if kind == "folder":
        return folder(payload["value"])
    if kind == "asset_ref":
        return AssetRef.from_dict(payload["value"])
    if kind == "asset_result":
        return AssetResult(
            value=decode_value(
                payload["value"],
                base_dir=base_dir,
                artifact_store=artifact_store,
            ),
            name=payload.get("name"),
            kind=str(payload.get("kind", "file")),
            metadata=dict(payload.get("metadata", {})),
        )
    if kind == "tmp_dir":
        return tmp_dir(payload["value"])
    if kind == "list":
        return [
            decode_value(item, base_dir=base_dir, artifact_store=artifact_store)
            for item in payload["items"]
        ]
    if kind == "tuple":
        return tuple(
            decode_value(item, base_dir=base_dir, artifact_store=artifact_store)
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
    return payload


def summarise_value(value: Any) -> Any:
    """Return a compact metadata view of a value for cache manifests."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, file):
        return {"type": "file", "value": str(value)}
    if isinstance(value, folder):
        return {"type": "folder", "value": str(value)}
    if isinstance(value, AssetRef):
        return {
            "type": "asset_ref",
            "asset": str(value.key),
            "version_id": value.version_id,
            "artifact_id": value.artifact_id,
        }
    if isinstance(value, AssetResult):
        return {
            "type": "asset_result",
            "kind": value.kind,
            "name": value.name,
        }
    if isinstance(value, tmp_dir):
        return {"type": "tmp_dir", "value": str(value)}
    if isinstance(value, list):
        return {
            "type": "list",
            "items": [summarise_value(item) for item in value],
            "length": len(value),
        }
    if isinstance(value, tuple):
        return {
            "type": "tuple",
            "items": [summarise_value(item) for item in value],
            "length": len(value),
        }
    if isinstance(value, dict):
        return {
            "type": "dict",
            "items": [
                {
                    "key": summarise_value(key),
                    "value": summarise_value(item),
                }
                for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
            ],
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


def hash_value_bytes(value: Any) -> tuple[str, str]:
    """Return the codec name and BLAKE3 digest for a Python value."""
    if isinstance(value, AssetRef):
        from ginkgo.runtime.caching.hashing import hash_str

        return "ginkgo.asset_ref", hash_str(value.version_id)
    if isinstance(value, np.ndarray) and value.dtype.hasobject is False:
        return "numpy.ndarray", _hash_numpy_array(value)
    if isinstance(value, pd.DataFrame):
        try:
            return "pandas.DataFrame", _hash_pandas_dataframe(value)
        except Exception:
            # Fall back to the existing serialized-byte path for frames pandas
            # cannot hash directly (for example some exotic object payloads).
            pass

    from ginkgo.runtime.caching.hashing import hash_bytes

    codec_name, data, _extension = _encode_bytes(value)
    digest = hash_bytes(data)
    return codec_name, digest


def ensure_serializable(value: Any, *, label: str) -> None:
    """Validate that a value can be encoded by the Ginkgo codec registry."""
    if value is None or isinstance(
        value,
        (bool, int, float, str, file, folder, tmp_dir, AssetRef, AssetResult),
    ):
        return

    if isinstance(value, list | tuple):
        for index, item in enumerate(value):
            ensure_serializable(item, label=f"{label}[{index}]")
        return

    if isinstance(value, dict):
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
    from ginkgo.runtime.caching.hashing import hash_bytes

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


def _encode_bytes(value: Any) -> tuple[str, bytes, str]:
    if isinstance(value, np.ndarray):
        buffer = io.BytesIO()
        np.save(buffer, value, allow_pickle=True)
        return "numpy.npy", buffer.getvalue(), "npy"

    if isinstance(value, pd.DataFrame):
        parquet_bytes = _try_encode_dataframe_parquet(value)
        if parquet_bytes is not None:
            return "pandas.parquet", parquet_bytes, "parquet"
        return "python.pickle", pickle.dumps(value, protocol=5), "pkl"

    return "python.pickle", pickle.dumps(value, protocol=5), "pkl"


def _decode_bytes(*, codec_name: str, data: bytes) -> Any:
    if codec_name == "numpy.npy":
        return np.load(io.BytesIO(data), allow_pickle=True)

    if codec_name == "pandas.parquet":
        return pd.read_parquet(io.BytesIO(data))

    if codec_name == "python.pickle":
        return pickle.loads(data)

    raise CodecError(f"Unknown value codec: {codec_name}")


def _try_encode_dataframe_parquet(value: Any) -> bytes | None:
    buffer = io.BytesIO()
    try:
        value.to_parquet(buffer, index=True)
    except Exception:
        return None
    return buffer.getvalue()


def _hash_numpy_array(value: Any) -> str:
    """Return a stable BLAKE3 digest for a non-object NumPy array."""
    from ginkgo.runtime.caching.hashing import new_hasher

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
    from ginkgo.runtime.caching.hashing import new_hasher

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
