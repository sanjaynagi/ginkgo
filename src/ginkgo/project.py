"""Discovery of the project root directory.

A ginkgo project is rooted at the directory holding its configuration file
(``ginkgo.toml``, or the YAML equivalents). Workflow files, ``.ginkgo/``
runtime state, and config overrides are all located relative to that
directory, so where it is needs one answer rather than one per caller.

:func:`find_project_root` is that answer: walk upward from a starting
directory to the nearest project marker. :func:`project_root` is the
user-facing form, walking up from the current working directory, so a
workflow can name a path relative to the project rather than to wherever
``ginkgo`` happened to be invoked from.
"""

from __future__ import annotations

from pathlib import Path

PROJECT_CONFIG_NAMES = ("ginkgo.toml", "ginkgo.yaml", "ginkgo.yml")
"""Configuration file names that mark a directory as a project root."""


def find_project_root(start_dir: Path) -> Path | None:
    """Walk upward from *start_dir* to the nearest project root.

    Parameters
    ----------
    start_dir : Path
        Directory to start from. Searched before its parents, so a directory
        that is itself a project root is returned unchanged.

    Returns
    -------
    Path | None
        The nearest ancestor of *start_dir* (inclusive) holding one of
        :data:`PROJECT_CONFIG_NAMES`, or ``None`` if there is no such
        directory up to the filesystem root.
    """
    for candidate in (start_dir, *start_dir.parents):
        for config_name in PROJECT_CONFIG_NAMES:
            if (candidate / config_name).is_file():
                return candidate
    return None


def project_root() -> Path:
    """Return the root directory of the project containing the current directory.

    Walks upward from the current working directory to the nearest directory
    holding a ginkgo configuration file, so this resolves to the same
    directory whether a workflow is run from the project root or from a
    subdirectory such as ``workflow/``.

    The starting point is deliberately the working directory rather than the
    calling module's location: a result that depended on which file asked
    would be harder to predict than one that depends on where the command
    was run.

    Returns
    -------
    Path
        An absolute path to the project root. A project configuration file is
        optional, so when no marker is found this falls back to the current
        working directory — the same directory the rest of ginkgo already
        treats as the project root.
    """
    cwd = Path.cwd()
    return find_project_root(cwd) or cwd
