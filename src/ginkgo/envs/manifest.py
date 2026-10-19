"""Finding a Pixi manifest in a directory.

Core uses this for the project's own environment: the interpreter check
compares the running interpreter with the project manifest. Task environments
are ginkgo-pixi's; it uses the same lookup for ``envs/<name>/``.
"""

from __future__ import annotations

import tomllib
from pathlib import Path


def find_pixi_manifest(directory: Path) -> Path | None:
    """Return the Pixi manifest in *directory*, or None if there is none.

    Prefers ``pixi.toml``; falls back to a ``pyproject.toml`` that carries a
    ``[tool.pixi]`` section.

    Parameters
    ----------
    directory : Path
        A project root, or a named environment's directory (``envs/<name>/``).

    Returns
    -------
    Path | None
        Path to the manifest file, or ``None`` when neither manifest exists.
    """
    pixi_toml = directory / "pixi.toml"
    if pixi_toml.is_file():
        return pixi_toml
    pyproject = directory / "pyproject.toml"
    if is_pixi_pyproject(pyproject):
        return pyproject
    return None


def is_pixi_pyproject(path: Path) -> bool:
    """Return whether *path* is a ``pyproject.toml`` carrying ``[tool.pixi]``.

    Parameters
    ----------
    path : Path
        Candidate manifest path.

    Returns
    -------
    bool
        True only when *path* is a readable ``pyproject.toml`` whose parsed
        contents contain a ``[tool.pixi]`` table. Unreadable or malformed TOML
        is treated as a non-match rather than an error.
    """
    if path.name != "pyproject.toml" or not path.is_file():
        return False
    try:
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return False
    tool = data.get("tool")
    return isinstance(tool, dict) and "pixi" in tool
