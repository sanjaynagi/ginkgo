"""Whether the interpreter running ginkgo can import what the project declares.

Python tasks cannot declare ``env=`` — they execute in the interpreter the CLI
runs from — so a project's Pixi manifest *is* the environment its Python and
notebook task bodies need. Install the CLI globally (the curl installer, ``uv
tool install``) and run it inside such a project and the two part company: the
manifest is right, the interpreter is wrong, and the only symptom is a bare
``ModuleNotFoundError``.

This module is the detector for that mismatch. It stays deliberately modest:
it compares the module names the workflow's own sources *actually import*
against what the running interpreter can find, and ignores the manifest's
dependency names entirely — a distribution name is not an import name
(``pyyaml`` installs ``yaml``), and guessing that mapping would trade a
precise answer for a noisy one. The manifest is consulted only for whether
this is a project with a declared environment at all, and for the ``pixi
run`` command that would use it.

:class:`InterpreterMismatch` carries the finding. It exposes ``severity``,
``code``, ``message`` and ``suggestion`` so ``ginkgo doctor`` can render it
beside its other diagnostics, and ``hint_lines`` so the same explanation can
be attached where a failure is rendered.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from ginkgo.envs.pixi import _env_manifest

_IMPORT_PATTERN = re.compile(r"^\s*(?:import|from)\s+([A-Za-z_][\w.]*)", re.MULTILINE)
"""Top-level ``import x`` / ``from x import y`` statements, x captured.

Leading whitespace is allowed so a deferred import inside a function body is
still seen. A relative import (``from .modules import prep``) cannot match,
because the name has to start with a letter or underscore.
"""

_MISSING_MODULE_PATTERN = re.compile(r"No module named '([\w.]+)'")

_SKIPPED_DIR_NAMES = frozenset({"envs", "scripts", "tests"})
"""Directories whose imports do not describe the CLI's own interpreter.

``envs/`` holds manifests for *other* environments; ``scripts/`` runs under a
script task's declared env; ``tests/`` imports test tooling the project has no
reason to declare for a run. Hidden directories (``.pixi/``, ``.ginkgo/``) are
skipped too, so a materialized environment is never mistaken for source.
"""

_SOURCE_SUFFIXES = (".py", ".ipynb")

_FALLBACK_COMMAND = "pixi run ginkgo run"


@dataclass(frozen=True, kw_only=True)
class InterpreterMismatch:
    """A project manifest whose environment the running interpreter is not.

    Attributes
    ----------
    manifest : Path
        The project's Pixi manifest.
    interpreter : Path
        The interpreter ginkgo is running from.
    missing : tuple[str, ...]
        Module names the workflow imports that this interpreter cannot find,
        sorted.
    project_command : str
        The command that would run ginkgo inside the manifest's environment.
    """

    manifest: Path
    interpreter: Path
    missing: tuple[str, ...]
    project_command: str

    severity: ClassVar[str] = "error"
    code: ClassVar[str] = "interpreter_env_mismatch"

    @property
    def message(self) -> str:
        """One line naming the manifest and what this interpreter cannot import."""
        return (
            f"This project declares its environment in {self.manifest.name}, but the ginkgo "
            f"you are running cannot import: {', '.join(self.missing)}"
        )

    @property
    def suggestion(self) -> str:
        """The interpreter, the manifest, and the command that pairs them."""
        return "\n".join(self.hint_lines[1:])

    @property
    def hint_lines(self) -> tuple[str, ...]:
        """The whole explanation, message first, for rendering beside a failure."""
        return (
            self.message,
            f"  running: {self.interpreter}",
            f"  project: {self.manifest}  ({self.project_command} uses this)",
            f"  Try: {self.project_command}",
        )


def find_project_manifest(*, project_root: Path) -> Path | None:
    """Return the project's Pixi manifest, or ``None`` when it has none.

    Parameters
    ----------
    project_root : Path
        Directory to look in — the project root, not searched upward.

    Returns
    -------
    Path | None
        ``pixi.toml``, or a ``pyproject.toml`` carrying ``[tool.pixi]``, in
        *project_root*. ``None`` means this is not a project with a declared
        environment, and every check here stays quiet.
    """
    return _env_manifest(project_root)


def workflow_import_roots(*, workflow_path: Path) -> tuple[str, ...]:
    """Return the third-party root modules the workflow's sources import.

    Scans the Python files and notebook code cells beside *workflow_path*,
    which is where the bodies that run in the CLI's interpreter live. Standard
    library modules and the project's own packages are dropped, so what
    remains is what the project's environment has to supply.

    Parameters
    ----------
    workflow_path : Path
        The workflow module. Its directory is the scan root.

    Returns
    -------
    tuple[str, ...]
        Sorted root module names.
    """
    scan_root = workflow_path.parent
    roots = {
        match.group(1).split(".")[0]
        for text in _iter_source_texts(scan_root=scan_root)
        for match in _IMPORT_PATTERN.finditer(text)
    }
    return tuple(
        sorted(
            root
            for root in roots
            if root not in sys.stdlib_module_names
            and root != "__future__"
            and not _is_local_module(name=root, scan_root=scan_root)
        )
    )


def unimportable(*, names: tuple[str, ...]) -> tuple[str, ...]:
    """Return the *names* the running interpreter cannot import, in order.

    Uses :func:`importlib.util.find_spec`, so nothing is executed. A name
    whose lookup raises is reported as missing: a module whose parent package
    is absent, or whose finder errors, is not one a task body can import
    either.
    """
    return tuple(name for name in names if not _is_importable(name))


def detect_import_mismatch(
    *,
    workflow_path: Path,
    project_root: Path,
) -> InterpreterMismatch | None:
    """Report a project whose declared environment the interpreter is not.

    Parameters
    ----------
    workflow_path : Path
        The workflow whose sources are scanned for imports.
    project_root : Path
        Where the project manifest is looked for.

    Returns
    -------
    InterpreterMismatch | None
        ``None`` — the quiet answer — when the project declares no manifest,
        or when this interpreter can import everything the workflow does.
        That covers running under ``pixi run``, where the interpreter *is* the
        manifest's environment, without a special case.
    """
    manifest = find_project_manifest(project_root=project_root)
    if manifest is None:
        return None
    missing = unimportable(names=workflow_import_roots(workflow_path=workflow_path))
    if not missing:
        return None
    return _mismatch(manifest=manifest, missing=missing)


def import_failure_mismatch(
    *,
    message: str | None,
    project_root: Path,
) -> InterpreterMismatch | None:
    """Explain a ``ModuleNotFoundError`` *message* as an interpreter mismatch.

    Parameters
    ----------
    message : str | None
        A failure message, as recorded for a failed task or carried by a
        raised exception. Anything that does not name a missing module is
        ignored.
    project_root : Path
        Where the project manifest is looked for.

    Returns
    -------
    InterpreterMismatch | None
        ``None`` when the message names no module, when the project declares
        no manifest, or when the named module is in fact importable here —
        a failure from some other interpreter, which this explanation would
        misdescribe.
    """
    if message is None:
        return None
    match = _MISSING_MODULE_PATTERN.search(message)
    if match is None:
        return None
    manifest = find_project_manifest(project_root=project_root)
    if manifest is None:
        return None
    root = match.group(1).split(".")[0]
    if _is_importable(root):
        return None
    return _mismatch(manifest=manifest, missing=(root,))


def _mismatch(*, manifest: Path, missing: tuple[str, ...]) -> InterpreterMismatch:
    """Build the finding for *missing* modules against *manifest*."""
    return InterpreterMismatch(
        manifest=manifest,
        interpreter=Path(sys.executable),
        missing=missing,
        project_command=_project_command(manifest=manifest),
    )


def _is_importable(name: str) -> bool:
    """Whether the running interpreter can find the module *name*."""
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def _iter_source_texts(*, scan_root: Path) -> list[str]:
    """Return the source text of every scanned file under *scan_root*.

    A notebook contributes the concatenated source of its code cells: a
    notebook task's body executes in the CLI's interpreter, so its imports
    count the same as a Python task's.

    Skipped directories are pruned as the walk descends rather than filtered
    afterwards: a materialized ``.pixi/`` holds an entire environment, and
    walking into it would cost more than the check is worth.
    """
    texts: list[str] = []
    for dirpath, dirnames, filenames in os.walk(scan_root):
        dirnames[:] = sorted(
            name
            for name in dirnames
            if not name.startswith(".") and name not in _SKIPPED_DIR_NAMES
        )
        for filename in sorted(filenames):
            path = Path(dirpath) / filename
            if path.suffix not in _SOURCE_SUFFIXES:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            texts.append(_notebook_code(text) if path.suffix == ".ipynb" else text)
    return texts


def _notebook_code(text: str) -> str:
    """Return the concatenated code cells of a notebook, or ``""`` if unreadable."""
    try:
        cells = json.loads(text)["cells"]
    except (ValueError, KeyError, TypeError):
        return ""
    return "\n".join(
        "".join(cell.get("source", ()))
        for cell in cells
        if isinstance(cell, dict) and cell.get("cell_type") == "code"
    )


def _is_local_module(*, name: str, scan_root: Path) -> bool:
    """Whether *name* names a module the project itself provides.

    An import of the workflow's own package (``from workflow.modules import
    prep``) is satisfied by the project tree, not by the environment, so it is
    not evidence of anything about the interpreter.
    """
    for directory in (scan_root, scan_root.parent):
        if (directory / name).is_dir() or (directory / f"{name}.py").is_file():
            return True
    return False


def _project_command(*, manifest: Path) -> str:
    """Return the ``pixi run`` command that runs ginkgo from *manifest*.

    Prefers a task the manifest already declares as plain ``ginkgo run``, so
    the hint names the command the project's own README does — the scaffold
    declares it as ``run``. Anything else, including a task that only dry-runs,
    falls back to ``pixi run ginkgo run``, which works in any Pixi project.
    """
    for name, command in _manifest_tasks(manifest=manifest).items():
        if _task_command(command).strip() == "ginkgo run":
            return f"pixi run {name}"
    return _FALLBACK_COMMAND


def _manifest_tasks(*, manifest: Path) -> dict[str, Any]:
    """Return the manifest's ``tasks`` table, empty when absent or unreadable."""
    try:
        with manifest.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    if manifest.name == "pyproject.toml":
        pixi = data.get("tool", {})
        data = pixi.get("pixi", {}) if isinstance(pixi, dict) else {}
    tasks = data.get("tasks") if isinstance(data, dict) else None
    return tasks if isinstance(tasks, dict) else {}


def _task_command(command: Any) -> str:
    """Return the command string of a Pixi task, which may be a table."""
    if isinstance(command, str):
        return command
    if isinstance(command, dict):
        cmd = command.get("cmd")
        if isinstance(cmd, str):
            return cmd
        if isinstance(cmd, list):
            return " ".join(str(part) for part in cmd)
    return ""
