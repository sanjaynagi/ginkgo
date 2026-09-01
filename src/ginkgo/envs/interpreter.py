"""Whether the interpreter running ginkgo can import what the project declares.

Python tasks cannot declare ``env=`` — they execute in the interpreter the CLI
runs from — so a project's Pixi manifest *is* the environment its Python and
notebook task bodies need. Install the CLI globally (the curl installer, ``uv
tool install``) and run it inside such a project and the two part company: the
manifest is right, the interpreter is wrong, and the only symptom is a bare
``ModuleNotFoundError``.

This module is the detector for that. It stays deliberately modest: it
compares the module names the workflow's own sources *actually import* against
what the running interpreter can find, and ignores the manifest's dependency
names entirely — a distribution name is not an import name (``pyyaml``
installs ``yaml``), and guessing that mapping would trade a precise answer for
a noisy one. Imports are read from the parsed syntax tree, so an import
written inside a docstring, a string literal, an ``if TYPE_CHECKING:`` block,
or a ``try``/``except ImportError`` guard is not one the environment has to
supply, and does not count.

An unimportable module has two quite different causes, and the two findings
here keep them apart:

- :class:`InterpreterMismatch` — the running interpreter is *not* the
  manifest's environment. Re-running under ``pixi run`` fixes it.
- :class:`MissingDependency` — the running interpreter *is* the manifest's
  environment, and it genuinely lacks the package. Only editing the manifest
  fixes it; ``pixi run`` would change nothing.

Both expose ``severity``, ``code``, ``message`` and ``suggestion`` so ``ginkgo
doctor`` can render them beside its other diagnostics, and ``hint_lines`` so
the same explanation can be attached where a failure is rendered.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import re
import sys
import tomllib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

from ginkgo.envs.pixi import _env_manifest

_MISSING_MODULE_PATTERN = re.compile(r"No module named '([\w.]+)'")

_SKIPPED_DIR_NAMES = frozenset({"envs", "scripts", "tests"})
"""Directories whose imports do not describe the CLI's own interpreter.

``envs/`` holds manifests for *other* environments; ``scripts/`` runs under a
script task's declared env; ``tests/`` imports test tooling the project has no
reason to declare for a run. Hidden directories (``.pixi/``, ``.ginkgo/``) are
skipped too, so a materialized environment is never mistaken for source.
"""

_SOURCE_SUFFIXES = (".py", ".ipynb")

_GUARDED_IMPORT_ERRORS = frozenset({"ImportError", "ModuleNotFoundError"})
"""Exceptions whose handler marks the imports in a ``try`` body as optional."""

_FALLBACK_COMMAND = "pixi run ginkgo run"


@dataclass(frozen=True, kw_only=True)
class EnvironmentFinding:
    """Modules the workflow imports that the running interpreter cannot find.

    The base of the two findings this module reports. Subclasses differ only
    in what they say, which is what the reader needs: the two causes have
    different fixes.

    Attributes
    ----------
    manifest : Path
        The project's Pixi manifest.
    interpreter : Path
        The interpreter ginkgo is running from.
    missing : tuple[str, ...]
        Module names the workflow imports that this interpreter cannot find,
        sorted.
    """

    manifest: Path
    interpreter: Path
    missing: tuple[str, ...]

    severity: ClassVar[str] = "error"
    code: ClassVar[str] = "environment_import_failure"

    @property
    def message(self) -> str:
        """One line naming what this interpreter cannot import."""
        raise NotImplementedError

    @property
    def detail_lines(self) -> tuple[str, ...]:
        """The indented lines below the message: where things are, and the fix."""
        raise NotImplementedError

    @property
    def suggestion(self) -> str:
        """The detail lines as one block, for ``doctor``'s dim second paragraph."""
        return "\n".join(self.detail_lines)

    @property
    def hint_lines(self) -> tuple[str, ...]:
        """The whole explanation, message first, for rendering beside a failure."""
        return (self.message, *self.detail_lines)


@dataclass(frozen=True, kw_only=True)
class InterpreterMismatch(EnvironmentFinding):
    """A project manifest whose environment the running interpreter is not.

    Attributes
    ----------
    project_command : str
        The command that would run ginkgo inside the manifest's environment.
    """

    project_command: str

    code: ClassVar[str] = "interpreter_env_mismatch"

    @property
    def message(self) -> str:
        """One line naming the manifest and what this interpreter cannot import."""
        return (
            f"This project declares its environment in {self.manifest.name}, but the ginkgo "
            f"you are running cannot import: {', '.join(self.missing)}"
        )

    @property
    def detail_lines(self) -> tuple[str, ...]:
        """The interpreter, the manifest, and the command that pairs them."""
        return (
            f"  running: {self.interpreter}",
            f"  project: {self.manifest}  ({self.project_command} uses this)",
            f"  Try: {self.project_command}",
        )


@dataclass(frozen=True, kw_only=True)
class MissingDependency(EnvironmentFinding):
    """The project's own environment cannot import what the workflow imports.

    Reported when ginkgo *is* running from the manifest's environment, where
    the interpreter is right and the manifest is the thing that is short. The
    ``pixi run`` advice would be a no-op here, so it is deliberately absent.
    """

    code: ClassVar[str] = "missing_dependency"

    @property
    def message(self) -> str:
        """One line naming the imports the project's own environment lacks."""
        subject = "that" if len(self.missing) == 1 else "those"
        return (
            f"This workflow imports {', '.join(self.missing)}, but the project's environment "
            f"cannot import {subject}"
        )

    @property
    def detail_lines(self) -> tuple[str, ...]:
        """The environment in use, the manifest to edit, and the fix."""
        return (
            f"  environment: {self.interpreter}",
            f"  project: {self.manifest}",
            f"  Try: add {', '.join(self.missing)} to {self.manifest.name}, then pixi install",
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


def source_import_roots(*, text: str) -> frozenset[str]:
    """Return the root module names *text* imports at run time.

    Parses *text* and walks the tree, so only real ``import`` statements
    count. Four classes of import are deliberately left out, because none of
    them means the environment has to supply the module:

    - anything inside a string — a docstring example, a README snippet
      embedded in a literal;
    - imports under ``if TYPE_CHECKING:``, which never execute;
    - imports inside a ``try`` whose handlers catch ``ImportError`` or
      ``ModuleNotFoundError``, the standard optional-dependency shape;
    - relative imports (``from .modules import prep``), which the project
      satisfies itself.

    Text that does not parse contributes nothing: a scan is advisory, and a
    file ginkgo cannot read is not evidence about the environment.
    """
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return frozenset()
    collector = _ImportCollector()
    collector.visit(tree)
    return frozenset(collector.roots)


class _ImportCollector(ast.NodeVisitor):
    """Collect the root module names of imports that actually execute."""

    def __init__(self) -> None:
        self.roots: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802 - ast visitor protocol
        """Record the root of each ``import a.b`` name."""
        for alias in node.names:
            self.roots.add(alias.name.split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802 - ast visitor protocol
        """Record the root of an absolute ``from a.b import c``."""
        if node.level == 0 and node.module:
            self.roots.add(node.module.split(".")[0])

    def visit_If(self, node: ast.If) -> None:  # noqa: N802 - ast visitor protocol
        """Skip the body of ``if TYPE_CHECKING:``; those imports never run."""
        if _is_type_checking_test(node.test):
            for statement in node.orelse:
                self.visit(statement)
            return
        self.generic_visit(node)

    def visit_Try(self, node: ast.Try) -> None:  # noqa: N802 - ast visitor protocol
        """Skip a ``try`` body guarded against ``ImportError``.

        An import written that way is optional by construction — the code
        already has an answer for its absence — so a missing module there says
        nothing about the interpreter. ``else``/``finally`` still count: they
        are not the guarded part.
        """
        if any(_handles_import_error(handler) for handler in node.handlers):
            for statement in (*node.orelse, *node.finalbody):
                self.visit(statement)
            return
        self.generic_visit(node)


def _is_type_checking_test(test: ast.expr) -> bool:
    """Whether an ``if`` test is ``TYPE_CHECKING`` or ``typing.TYPE_CHECKING``."""
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _handles_import_error(handler: ast.ExceptHandler) -> bool:
    """Whether an ``except`` clause catches a missing import.

    A bare ``except:`` counts too: it swallows ``ImportError`` along with
    everything else, so the import is no less optional.
    """
    if handler.type is None:
        return True
    caught = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    return any(isinstance(item, ast.Name) and item.id in _GUARDED_IMPORT_ERRORS for item in caught)


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
    roots: set[str] = set()
    for text in _iter_source_texts(scan_root=scan_root):
        roots |= source_import_roots(text=text)
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


def running_in_manifest_environment(*, manifest: Path) -> bool:
    """Whether this interpreter is one of *manifest*'s own Pixi environments.

    Two independent signals, either of which settles it:

    - ``sys.prefix`` sits under the manifest's ``.pixi/`` directory, where
      Pixi materializes a workspace's environments;
    - ``PIXI_PROJECT_MANIFEST``, which ``pixi run`` exports, names this very
      manifest — which covers a workspace whose environments live elsewhere.

    A false answer here is the interesting one: it means the running
    interpreter and the project's declared environment are different things,
    and ``pixi run`` is the advice that closes the gap.
    """
    declared = os.environ.get("PIXI_PROJECT_MANIFEST")
    if declared and _same_path(Path(declared), manifest):
        return True
    try:
        Path(sys.prefix).resolve().relative_to((manifest.parent / ".pixi").resolve())
    except (OSError, ValueError):
        return False
    return True


def detect_import_problem(
    *,
    workflow_path: Path,
    project_root: Path,
) -> EnvironmentFinding | None:
    """Report imports the running interpreter cannot satisfy, and say why.

    Parameters
    ----------
    workflow_path : Path
        The workflow whose sources are scanned for imports.
    project_root : Path
        Where the project manifest is looked for.

    Returns
    -------
    EnvironmentFinding | None
        :class:`InterpreterMismatch` when the interpreter is not the
        manifest's environment, :class:`MissingDependency` when it is and the
        package is simply absent, and ``None`` — the quiet answer — when the
        project declares no manifest, or when this interpreter can import
        everything the workflow does.
    """
    manifest = find_project_manifest(project_root=project_root)
    if manifest is None:
        return None
    missing = unimportable(names=workflow_import_roots(workflow_path=workflow_path))
    if not missing:
        return None
    return _finding(manifest=manifest, missing=missing)


def explain_import_failure(
    *,
    message: str | None,
    project_root: Path,
) -> EnvironmentFinding | None:
    """Explain a ``ModuleNotFoundError`` *message* against the project manifest.

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
    EnvironmentFinding | None
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
    return _finding(manifest=manifest, missing=(root,))


def _finding(*, manifest: Path, missing: tuple[str, ...]) -> EnvironmentFinding:
    """Build the finding for *missing* modules, choosing it by where we run."""
    interpreter = Path(sys.executable)
    if running_in_manifest_environment(manifest=manifest):
        return MissingDependency(manifest=manifest, interpreter=interpreter, missing=missing)
    return InterpreterMismatch(
        manifest=manifest,
        interpreter=interpreter,
        missing=missing,
        project_command=_project_command(manifest=manifest),
    )


def _same_path(left: Path, right: Path) -> bool:
    """Whether two paths resolve to the same file, tolerating a bad path."""
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return False


def _is_importable(name: str) -> bool:
    """Whether the running interpreter can find the module *name*.

    Every exception is swallowed, not just ``ImportError``: a third-party
    meta-path finder runs arbitrary code during the lookup and can raise
    anything, and a diagnostic that crashes the command it is diagnosing is
    worse than no diagnostic.
    """
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


def _iter_source_texts(*, scan_root: Path) -> Iterator[str]:
    """Yield each separately parseable source text under *scan_root*.

    A Python file yields once. A notebook yields once per code cell: cells are
    parsed apart so a cell carrying an IPython magic — which is not Python —
    costs only itself, not the rest of the notebook.

    Skipped directories are pruned as the walk descends rather than filtered
    afterwards: a materialized ``.pixi/`` holds an entire environment, and
    walking into it would cost more than the check is worth. Texts are yielded
    rather than collected, so a large project tree is never held in memory at
    once.
    """
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
            if path.suffix == ".ipynb":
                yield from _notebook_code_cells(text)
            else:
                yield text


def _notebook_code_cells(text: str) -> Iterator[str]:
    """Yield the source of each code cell of a notebook, nothing if unreadable."""
    try:
        cells = json.loads(text)["cells"]
    except (ValueError, KeyError, TypeError):
        return
    for cell in cells:
        if isinstance(cell, dict) and cell.get("cell_type") == "code":
            yield "".join(cell.get("source", ()))


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
