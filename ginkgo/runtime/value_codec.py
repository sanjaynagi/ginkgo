"""Value codecs shared by process transport and cache storage."""

from __future__ import annotations

import base64
import hashlib
import io
import pickle
from pathlib import Path
from typing import Any

from ginkgo.core.types import file, folder, tmp_dir

INLINE_BYTES_LIMIT = 256 * 1024


class CodecError(TypeError):
    """Raised when a value cannot be encoded by Ginkgo."""


def encode_value(
    value: Any,
    *,
    base_dir: Path,
    inline_limit: int = INLINE_BYTES_LIMIT,
) -> Any:
    """Encode a Python value into a JSON-safe payload with optional artifacts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, file):
        return {"__ginkgo_type__": "file", "value": str(value)}

    if isinstance(value, folder):
        return {"__ginkgo_type__": "folder", "value": str(value)}

    if isinstance(value, tmp_dir):
        return {"__ginkgo_type__": "tmp_dir", "value": str(value)}

    if isinstance(value, list):
        return {
            "__ginkgo_type__": "list",
            "items": [
                encode_value(item, base_dir=base_dir, inline_limit=inline_limit) for item in value
            ],
        }

    if isinstance(value, tuple):
        return {
            "__ginkgo_type__": "tuple",
            "items": [
                encode_value(item, base_dir=base_dir, inline_limit=inline_limit) for item in value
            ],
        }

    if isinstance(value, dict):
        return {
            "__ginkgo_type__": "dict",
            "items": [
                {
                    "key": encode_value(key, base_dir=base_dir, inline_limit=inline_limit),
                    "value": encode_value(item, base_dir=base_dir, inline_limit=inline_limit),
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
        inline_limit=inline_limit,
    )


def decode_value(payload: Any, *, base_dir: Path) -> Any:
    """Restore a Python value from an encoded payload."""
    if not isinstance(payload, dict):
        return payload

    kind = payload.get("__ginkgo_type__")
    if kind == "file":
        return file(payload["value"])
    if kind == "folder":
        return folder(payload["value"])
    if kind == "tmp_dir":
        return tmp_dir(payload["value"])
    if kind == "list":
        return [decode_value(item, base_dir=base_dir) for item in payload["items"]]
    if kind == "tuple":
        return tuple(decode_value(item, base_dir=base_dir) for item in payload["items"])
    if kind == "dict":
        return {
            decode_value(item["key"], base_dir=base_dir): decode_value(
                item["value"], base_dir=base_dir
            )
            for item in payload["items"]
        }
    if kind == "binary":
        return _decode_binary_payload(payload=payload, base_dir=base_dir)
    return payload


def summarise_value(value: Any) -> Any:
    """Return a compact metadata view of a value for cache manifests."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    if isinstance(value, file):
        return {"type": "file", "value": str(value)}
    if isinstance(value, folder):
        return {"type": "folder", "value": str(value)}
    if isinstance(value, tmp_dir):
        return {"type": "tmp_dir", "value": str(value)}
    if isinstance(value, list):
        return {"type": "list", "length": len(value)}
    if isinstance(value, tuple):
        return {"type": "tuple", "length": len(value)}
    if isinstance(value, dict):
        return {"type": "dict", "length": len(value)}

    try:
        import numpy as np
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        np = None

    if np is not None and isinstance(value, np.ndarray):
        return {
            "type": "numpy.ndarray",
            "dtype": str(value.dtype),
            "shape": list(value.shape),
        }

    try:
        import pandas as pd
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        pd = None

    if pd is not None and isinstance(value, pd.DataFrame):
        return {
            "type": "pandas.DataFrame",
            "columns": list(map(str, value.columns)),
            "rows": int(len(value.index)),
        }

    return {"type": f"{type(value).__module__}.{type(value).__name__}"}


def hash_value_bytes(value: Any) -> tuple[str, str]:
    """Return the codec name and SHA-256 digest for a Python value."""
    codec_name, data, _extension = _encode_bytes(value)
    digest = hashlib.sha256(data).hexdigest()
    return codec_name, digest


def ensure_serializable(value: Any, *, label: str) -> None:
    """Validate that a value can be encoded by the Ginkgo codec registry."""
    if value is None or isinstance(value, (bool, int, float, str, file, folder, tmp_dir)):
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
    inline_limit: int,
) -> dict[str, Any]:
    if len(data) <= inline_limit:
        return {
            "__ginkgo_type__": "binary",
            "codec": codec_name,
            "storage": "inline",
            "data": base64.b64encode(data).decode("ascii"),
        }

    artifacts_dir = base_dir / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(data).hexdigest()
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


def _decode_binary_payload(*, payload: dict[str, Any], base_dir: Path) -> Any:
    if payload["storage"] == "inline":
        data = base64.b64decode(payload["data"])
    else:
        data = (base_dir / payload["artifact"]).read_bytes()
    return _decode_bytes(codec_name=payload["codec"], data=data)


def _encode_bytes(value: Any) -> tuple[str, bytes, str]:
    try:
        import numpy as np
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        np = None

    if np is not None and isinstance(value, np.ndarray):
        buffer = io.BytesIO()
        np.save(buffer, value, allow_pickle=True)
        return "numpy.npy", buffer.getvalue(), "npy"

    try:
        import pandas as pd
    except ModuleNotFoundError:  # pragma: no cover - optional dependency
        pd = None

    if pd is not None and isinstance(value, pd.DataFrame):
        parquet_bytes = _try_encode_dataframe_parquet(value)
        if parquet_bytes is not None:
            return "pandas.parquet", parquet_bytes, "parquet"
        return "python.pickle", pickle.dumps(value, protocol=5), "pkl"

    return "python.pickle", pickle.dumps(value, protocol=5), "pkl"


def _decode_bytes(*, codec_name: str, data: bytes) -> Any:
    if codec_name == "numpy.npy":
        try:
            import numpy as np
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise CodecError("numpy is required to decode ndarray values") from exc
        return np.load(io.BytesIO(data), allow_pickle=True)

    if codec_name == "pandas.parquet":
        try:
            import pandas as pd
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise CodecError("pandas is required to decode DataFrame values") from exc
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
