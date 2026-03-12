"""Workflow configuration loading.

Loads TOML or YAML config files and returns plain dicts. Schema validation and
multi-file layering are deferred to a later phase.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
import tomllib
from typing import Any, Iterator, Sequence

import yaml


@dataclass
class _ConfigSession:
    """Tracks config overrides and loaded values within a CLI import session."""

    override_paths: list[Path] = field(default_factory=list)
    loaded_values: list[dict[str, Any]] = field(default_factory=list)

    def merged_loaded_values(self) -> dict[str, Any]:
        """Return all loaded config mappings merged in load order."""
        return _merge_top_level_dicts(self.loaded_values)


_CONFIG_SESSIONS: list[_ConfigSession] = []


def config(path: str | Path) -> dict[str, Any]:
    """Load a TOML or YAML configuration file.

    Parameters
    ----------
    path : str | Path
        Path to the configuration file.

    Returns
    -------
    dict[str, Any]
        The parsed configuration as a nested dict.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    """
    session = _CONFIG_SESSIONS[-1] if _CONFIG_SESSIONS else None
    if session is not None and session.override_paths:
        data = _merge_top_level_dicts(_load_config_mapping(item) for item in session.override_paths)
    else:
        data = _load_config_mapping(path)

    if session is not None:
        session.loaded_values.append(deepcopy(data))

    return data


@contextmanager
def _config_session(
    *,
    override_paths: Sequence[str | Path] | None = None,
) -> Iterator[_ConfigSession]:
    """Temporarily override config loading for CLI-driven workflow imports."""
    session = _ConfigSession(
        override_paths=[Path(path).resolve() for path in override_paths or ()],
    )
    _CONFIG_SESSIONS.append(session)
    try:
        yield session
    finally:
        popped = _CONFIG_SESSIONS.pop()
        assert popped is session


def _load_config_mapping(path: str | Path) -> dict[str, Any]:
    """Load a single TOML or YAML config file and require a top-level mapping."""
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix == ".toml":
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    elif suffix in {".yaml", ".yml"}:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    else:
        raise ValueError(
            f"Unsupported config format for {path}. Expected .toml, .yaml, or .yml"
        )

    if not isinstance(data, dict):
        raise TypeError(f"Config file must contain a top-level mapping, got {type(data).__name__}")

    return data


def _merge_top_level_dicts(mappings: Sequence[dict[str, Any]] | Iterator[dict[str, Any]]) -> dict[str, Any]:
    """Shallow-merge top-level config keys with last-write-wins semantics."""
    merged: dict[str, Any] = {}
    for mapping in mappings:
        merged.update(deepcopy(mapping))
    return merged
