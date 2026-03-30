"""File-backed asset catalog metadata store."""

from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import yaml

from ginkgo.core.asset import AssetKey, AssetRef, AssetVersion


@dataclass(frozen=True, kw_only=True)
class AssetLineage:
    """Lineage edge set for one asset version.

    Parameters
    ----------
    child : AssetRef
        Produced asset reference.
    parents : tuple[AssetRef, ...]
        Upstream asset references consumed to build the child.
    """

    child: AssetRef
    parents: tuple[AssetRef, ...]


class AssetStore:
    """Local file-backed asset metadata catalog.

    Parameters
    ----------
    root : Path | None
        Catalog root directory. Defaults to ``.ginkgo/assets`` under the
        current working directory.
    """

    def __init__(self, *, root: Path | None = None) -> None:
        self._root = root if root is not None else Path.cwd() / ".ginkgo" / "assets"
        self._root.mkdir(parents=True, exist_ok=True)

    def register_version(self, *, version: AssetVersion) -> AssetVersion:
        """Persist one immutable asset version and update the asset index.

        Parameters
        ----------
        version : AssetVersion
            Version metadata to persist.

        Returns
        -------
        AssetVersion
        """
        version_dir = self._version_dir(version.key, version.version_id)
        version_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_yaml(version_dir / "meta.yaml", version.to_dict())

        index = self._load_index(version.key)
        versions = [str(item) for item in index.get("versions", [])]
        if version.version_id not in versions:
            versions.append(version.version_id)
        index.update(
            {
                "key": version.key.to_dict(),
                "latest_version_id": version.version_id,
                "versions": versions,
            }
        )
        self._write_index(version.key, index)
        return version

    def get_version(self, *, key: AssetKey, version_id: str) -> AssetVersion:
        """Load one specific asset version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        version_id : str
            Immutable version identifier.

        Returns
        -------
        AssetVersion
        """
        path = self._version_dir(key, version_id) / "meta.yaml"
        if not path.is_file():
            raise FileNotFoundError(f"Unknown asset version {key}@{version_id}")
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return AssetVersion.from_dict(data)

    def get_latest_version(self, *, key: AssetKey) -> AssetVersion | None:
        """Return the latest version for an asset key, if any."""
        index = self._load_index(key)
        version_id = index.get("latest_version_id")
        if not version_id:
            return None
        return self.get_version(key=key, version_id=str(version_id))

    def list_versions(self, *, key: AssetKey) -> list[AssetVersion]:
        """Return all versions for one asset key."""
        index = self._load_index(key)
        versions = [str(item) for item in index.get("versions", [])]
        return [self.get_version(key=key, version_id=version_id) for version_id in versions]

    def list_asset_keys(self) -> list[AssetKey]:
        """Return all asset keys currently present in the catalog."""
        keys: list[AssetKey] = []
        for namespace_dir in sorted(path for path in self._root.iterdir() if path.is_dir()):
            for asset_dir in sorted(path for path in namespace_dir.iterdir() if path.is_dir()):
                index_path = asset_dir / "index.yaml"
                if not index_path.is_file():
                    continue
                data = yaml.safe_load(index_path.read_text(encoding="utf-8")) or {}
                key_data = data.get("key")
                if isinstance(key_data, dict):
                    keys.append(AssetKey.from_dict(key_data))
                    continue
                keys.append(AssetKey(namespace=namespace_dir.name, name=unquote(asset_dir.name)))
        return keys

    def set_alias(self, *, key: AssetKey, alias: str, version_id: str) -> None:
        """Point one alias at a specific asset version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        alias : str
            Alias label.
        version_id : str
            Target version identifier.
        """
        self.get_version(key=key, version_id=version_id)
        index = self._load_index(key)
        aliases = dict(index.get("aliases", {}))
        aliases[alias] = version_id
        index["aliases"] = aliases
        self._write_index(key, index)

    def resolve_version(
        self,
        *,
        key: AssetKey,
        selector: str | None = None,
    ) -> AssetVersion:
        """Resolve a version selector to one concrete version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        selector : str | None
            Explicit version id, alias, or ``None`` for latest.

        Returns
        -------
        AssetVersion
        """
        index = self._load_index(key)
        if selector is None:
            latest = index.get("latest_version_id")
            if latest is None:
                raise FileNotFoundError(f"No versions registered for asset {key}")
            return self.get_version(key=key, version_id=str(latest))

        aliases = dict(index.get("aliases", {}))
        resolved = str(aliases.get(selector, selector))
        return self.get_version(key=key, version_id=resolved)

    def record_lineage(self, *, child: AssetRef, parents: list[AssetRef]) -> None:
        """Persist lineage edges for one produced asset.

        Parameters
        ----------
        child : AssetRef
            Produced asset reference.
        parents : list[AssetRef]
            Upstream asset references consumed by the producing task.
        """
        index = self._load_index(child.key)
        lineage = dict(index.get("lineage", {}))
        rendered_parents = [parent.to_dict() for parent in parents]
        if lineage.get(child.version_id) == rendered_parents:
            return
        lineage[child.version_id] = rendered_parents
        index["lineage"] = lineage
        self._write_index(child.key, index)

    def lineage_for(self, *, key: AssetKey, version_id: str) -> AssetLineage | None:
        """Return recorded lineage for one asset version.

        Parameters
        ----------
        key : AssetKey
            Asset identity.
        version_id : str
            Target version identifier.

        Returns
        -------
        AssetLineage | None
        """
        version = self.get_version(key=key, version_id=version_id)
        index = self._load_index(key)
        lineage = dict(index.get("lineage", {}))
        parents_data = lineage.get(version_id)
        if parents_data is None:
            return None
        child = AssetRef.from_dict(
            {
                "key": version.key.to_dict(),
                "version_id": version.version_id,
                "kind": version.kind,
                "artifact_id": version.artifact_id,
                "content_hash": version.content_hash,
                "artifact_path": "",
                "metadata": version.metadata,
            }
        )
        parents = tuple(AssetRef.from_dict(parent) for parent in parents_data)
        return AssetLineage(child=child, parents=parents)

    def _index_path(self, key: AssetKey) -> Path:
        return self._asset_dir(key) / "index.yaml"

    def _asset_dir(self, key: AssetKey) -> Path:
        return self._root / key.namespace / quote(key.name, safe="")

    def _version_dir(self, key: AssetKey, version_id: str) -> Path:
        return self._asset_dir(key) / "versions" / f"v-{version_id}"

    def _load_index(self, key: AssetKey) -> dict[str, Any]:
        path = self._index_path(key)
        if not path.is_file():
            return {
                "aliases": {},
                "key": key.to_dict(),
                "latest_version_id": None,
                "lineage": {},
                "versions": [],
            }
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise TypeError(f"Asset index must contain a mapping: {path}")
        data.setdefault("aliases", {})
        data.setdefault("key", key.to_dict())
        data.setdefault("latest_version_id", None)
        data.setdefault("lineage", {})
        data.setdefault("versions", [])
        return data

    def _write_index(self, key: AssetKey, index: dict[str, Any]) -> None:
        asset_dir = self._asset_dir(key)
        asset_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_yaml(self._index_path(key), index)


def _atomic_write_yaml(path: Path, payload: dict[str, Any]) -> None:
    """Atomically persist one YAML payload."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
        temp_path = Path(handle.name)
    temp_path.replace(path)
