"""Content-addressed cache support for Ginkgo."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_args, get_origin

from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.value_codec import (
    decode_value,
    encode_value,
    hash_value_bytes,
    summarise_value,
)

if TYPE_CHECKING:
    from ginkgo.envs.pixi import PixiRegistry


MISSING = object()


@dataclass(kw_only=True)
class CacheStore:
    """Persistent on-disk cache for resolved task results.

    Parameters
    ----------
    root : Path | None
        Cache root directory. Defaults to ``.ginkgo/cache`` under the current
        working directory.
    pixi_registry : PixiRegistry | None
        Registry used to resolve per-environment lockfile hashes. When
        ``None``, falls back to looking for a single ``pixi.lock`` in the
        current working directory (pre-Phase-5 behaviour).
    """

    root: Path | None = None
    pixi_registry: Any | None = None  # PixiRegistry; typed as Any to avoid circular import
    _root: Path = field(init=False, repr=False)

    def __post_init__(self) -> None:
        root = self.root if self.root is not None else Path.cwd() / ".ginkgo" / "cache"
        object.__setattr__(self, "_root", Path(root))
        self._root.mkdir(parents=True, exist_ok=True)

    def build_cache_key(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Build a stable content-addressed cache key for a task call."""
        input_hashes: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            input_hashes[name] = self._hash_value(annotation=annotation, value=resolved_args[name])

        env_hash = self._env_hash(task_def=task_def)
        payload = {
            "env": task_def.env,
            "env_hash": env_hash,
            "inputs": input_hashes,
            "task": task_def.name,
            "version": task_def.version,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest(), input_hashes

    def load(self, *, cache_key: str) -> Any:
        """Load a cached result if present."""
        entry_dir = self._entry_dir(cache_key)
        output_path = entry_dir / "output.json"
        if not output_path.exists():
            return MISSING

        return decode_value(
            json.loads(output_path.read_text(encoding="utf-8")),
            base_dir=entry_dir,
        )

    def save(
        self,
        *,
        cache_key: str,
        result: Any,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
        input_hashes: dict[str, Any],
    ) -> None:
        """Atomically persist a task result and metadata."""
        entry_dir = self._entry_dir(cache_key)
        if entry_dir.exists():
            return

        temp_dir = Path(tempfile.mkdtemp(prefix=f"{cache_key}.tmp-", dir=self._root))
        try:
            output_path = temp_dir / "output.json"
            output_path.write_text(
                json.dumps(encode_value(result, base_dir=temp_dir), sort_keys=True),
                encoding="utf-8",
            )

            meta = {
                "cache_key": cache_key,
                "env": task_def.env,
                "function": task_def.name,
                "inputs": self._serialise_inputs(task_def=task_def, resolved_args=resolved_args),
                "input_hashes": input_hashes,
                "timestamp": datetime.now(UTC).isoformat(),
                "version": task_def.version,
            }
            (temp_dir / "meta.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True),
                encoding="utf-8",
            )

            try:
                os.replace(temp_dir, entry_dir)
            except FileExistsError:
                pass
        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

    def _entry_dir(self, cache_key: str) -> Path:
        """Return the cache directory for a given key."""
        return self._root / cache_key

    def _env_hash(self, *, task_def: TaskDef) -> dict[str, Any] | None:
        """Return environment identity information for cache-keying."""
        if task_def.env is None:
            return None

        if self.pixi_registry is not None:
            lock_digest = self.pixi_registry.lock_hash(env=task_def.env)
        else:
            # Pre-Phase-5 fallback: single pixi.lock in cwd.
            pixi_lock = Path.cwd() / "pixi.lock"
            lock_digest = self._hash_file_contents(pixi_lock) if pixi_lock.is_file() else None

        return {
            "env": task_def.env,
            "pixi_lock": lock_digest,
        }

    def _hash_value(self, *, annotation: Any, value: Any) -> Any:
        """Hash a concrete value according to its declared Ginkgo type."""
        if annotation is tmp_dir:
            return None

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_args = get_args(annotation)
            inner_annotation = inner_args[0] if inner_args else Any
            return {
                "items": [
                    self._hash_value(annotation=inner_annotation, value=item) for item in value
                ],
                "type": origin.__name__,
            }

        if origin is dict:
            key_annotation, value_annotation = self._dict_annotations(annotation)
            return {
                "items": [
                    {
                        "key": self._hash_value(annotation=key_annotation, value=key),
                        "value": self._hash_value(annotation=value_annotation, value=item),
                    }
                    for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
                ],
                "type": "dict",
            }

        if annotation is file or isinstance(value, file):
            return {"sha256": self._hash_file_contents(Path(str(value))), "type": "file"}

        if annotation is folder or isinstance(value, folder):
            return {"sha256": self._hash_folder_contents(Path(str(value))), "type": "folder"}

        if isinstance(value, list):
            return {
                "items": [self._hash_value(annotation=Any, value=item) for item in value],
                "type": "list",
            }

        if isinstance(value, tuple):
            return {
                "items": [self._hash_value(annotation=Any, value=item) for item in value],
                "type": "tuple",
            }

        if isinstance(value, dict):
            return {
                "items": [
                    {
                        "key": self._hash_value(annotation=Any, value=key),
                        "value": self._hash_value(annotation=Any, value=item),
                    }
                    for key, item in sorted(value.items(), key=lambda pair: repr(pair[0]))
                ],
                "type": "dict",
            }

        if value is None or isinstance(value, (bool, int, float, str)):
            return {
                "sha256": hashlib.sha256(repr(value).encode("utf-8")).hexdigest(),
                "type": type(value).__name__,
            }

        codec_name, digest = hash_value_bytes(value)
        return {
            "codec": codec_name,
            "sha256": digest,
            "type": f"{type(value).__module__}.{type(value).__name__}",
        }

    def _serialise_inputs(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> dict[str, Any]:
        """Serialize resolved inputs for metadata output."""
        inputs: dict[str, Any] = {}
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir:
                continue
            inputs[name] = summarise_value(resolved_args[name])
        return inputs

    def _hash_file_contents(self, path: Path) -> str:
        """Return the SHA-256 digest of a file's contents."""
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _hash_folder_contents(self, path: Path) -> str:
        """Return the SHA-256 digest of a folder's recursive contents."""
        digest = hashlib.sha256()

        for child in sorted(path.rglob("*"), key=lambda item: str(item.relative_to(path))):
            rel = str(child.relative_to(path))
            if child.is_dir():
                digest.update(f"D:{rel}".encode("utf-8"))
                continue

            digest.update(f"F:{rel}".encode("utf-8"))
            digest.update(self._hash_file_contents(child).encode("utf-8"))

        return digest.hexdigest()

    def _dict_annotations(self, annotation: Any) -> tuple[Any, Any]:
        """Extract key and value annotations for a mapping annotation."""
        args = get_args(annotation)
        if len(args) == 2:
            return args[0], args[1]
        return Any, Any
