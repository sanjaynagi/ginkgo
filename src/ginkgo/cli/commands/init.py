"""Project scaffolding command."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata, resources
from importlib.resources.abc import Traversable
import json
import shutil
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

from ginkgo.cli.common import console


_TEMPLATE_PROJECT_NAME = "ginkgo-init-template"

#: Fixed name of the scaffolded workflow package. Kept constant across projects
#: so the layout is predictable and the project root is never shadowed by a
#: like-named package directory.
PACKAGE_NAME = "workflow"

#: Repository the scaffolded project installs ginkgo from.
GINKGO_REPO_URL = "https://github.com/sanjaynagi/ginkgo.git"

#: Commit the starter scaffold is validated against, used when the running
#: install does not record one of its own.
#:
#: This constant exists only because no release currently carries what the
#: templates use: they call ``ginkgo.param``, which landed after v0.2.0, so
#: pinning either existing tag would scaffold a project that installs cleanly
#: and then cannot run. Whoever cuts a release that carries everything the
#: templates use should delete this and pin that tag instead.
#:
#: Until then, bump it when the templates start needing newer ginkgo.
#: ``test_fallback_pin_can_still_run_the_scaffold_it_pins`` fails when this
#: commit no longer carries every ginkgo name the templates reach for.
FALLBACK_GINKGO_REV = "2471fcd00bc606f6ad6702151452fb0e6ea76396"  # pragma: allowlist secret


@dataclass(frozen=True, kw_only=True)
class TemplateContext:
    """Values rendered into scaffold templates."""

    project_name: str
    workflow_relpath: str
    modules_relpath: str
    envs_relpath: str
    notebooks_relpath: str
    scripts_relpath: str
    tests_relpath: str
    ginkgo_rev: str


def _template_root(*, group: str):
    """Return the packaged template root for one scaffold group."""
    return resources.files("ginkgo.templates.init").joinpath(group)


def _render_template_content(*, content: str, context: TemplateContext) -> str:
    """Return template content with project and package substitutions applied."""
    rendered = content.replace(_TEMPLATE_PROJECT_NAME, context.project_name)
    replacements = {
        "{{ project_name }}": context.project_name,
        "{{ workflow_relpath }}": context.workflow_relpath,
        "{{ modules_relpath }}": context.modules_relpath,
        "{{ envs_relpath }}": context.envs_relpath,
        "{{ notebooks_relpath }}": context.notebooks_relpath,
        "{{ scripts_relpath }}": context.scripts_relpath,
        "{{ tests_relpath }}": context.tests_relpath,
        "{{ ginkgo_rev }}": context.ginkgo_rev,
    }
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    return rendered


def _iter_template_files(
    template_root: Traversable,
) -> Iterable[tuple[Traversable, PurePosixPath]]:
    """Yield packaged template files and their relative paths."""
    yield from _iter_template_files_from_dir(
        current_dir=template_root,
        relative_dir=PurePosixPath(),
    )


def _iter_template_files_from_dir(
    *,
    current_dir: Traversable,
    relative_dir: PurePosixPath,
) -> Iterable[tuple[Traversable, PurePosixPath]]:
    """Yield packaged template files from one directory subtree."""
    for child in sorted(current_dir.iterdir(), key=lambda path: path.name):
        child_relative = relative_dir / child.name
        if child.is_file():
            yield child, child_relative
            continue
        yield from _iter_template_files_from_dir(
            current_dir=child,
            relative_dir=child_relative,
        )


def _template_files(
    *, template_root: Traversable, destination_prefix: Path | None = None
) -> list[tuple[Traversable, Path]]:
    """Return ``(source, relative_dest)`` pairs for the starter template."""
    files: list[tuple[Traversable, Path]] = []
    for source_path, relative_path in _iter_template_files(template_root):
        rendered_path = Path(*relative_path.parts)
        if destination_prefix is not None:
            rendered_path = destination_prefix / rendered_path
        files.append(
            (
                source_path,
                rendered_path,
            )
        )
    return files


def _normalized_repo_url(url: str) -> str:
    """Return a repository URL stripped of the variations that do not identify it."""
    return url.removeprefix("git+").rstrip("/").removesuffix(".git").casefold()


def _installed_ginkgo_rev() -> str | None:
    """Return the ginkgo commit the running CLI was installed from, if recorded.

    Installers write ``direct_url.json`` when a distribution comes from a URL,
    including the commit a git requirement resolved to. Editable and local
    installs record a directory instead of a commit, and a commit from some
    other repository would not resolve for whoever installs the scaffold, so
    both of those cases return ``None``.
    """
    try:
        raw = metadata.distribution("ginkgo").read_text("direct_url.json")
        direct_url = json.loads(raw) if raw else {}
    except (metadata.PackageNotFoundError, OSError, ValueError):  # pragma: no cover
        return None

    if _normalized_repo_url(str(direct_url.get("url", ""))) != _normalized_repo_url(
        GINKGO_REPO_URL
    ):
        return None
    commit_id = (direct_url.get("vcs_info") or {}).get("commit_id")
    return str(commit_id) if commit_id else None


def _ginkgo_rev() -> str:
    """Return the ginkgo commit a scaffolded project should pin itself to.

    A scaffolded project runs its Python tasks in the interpreter its own
    environment provides, so that environment installs ginkgo itself. Pinning
    keeps the orchestrator reproducible, and preferring the running install's
    own commit keeps a scaffold consistent with the CLI that wrote it.
    """
    return _installed_ginkgo_rev() or FALLBACK_GINKGO_REV


def _template_context(*, root: Path) -> TemplateContext:
    """Return the scaffold render context for one project root."""
    package_dir = Path(PACKAGE_NAME)
    return TemplateContext(
        ginkgo_rev=_ginkgo_rev(),
        project_name=root.name,
        workflow_relpath=str(package_dir / "flow.py"),
        modules_relpath=str(package_dir / "modules"),
        envs_relpath=str(package_dir / "envs"),
        notebooks_relpath=str(package_dir / "notebooks"),
        scripts_relpath=str(package_dir / "scripts"),
        tests_relpath="tests/workflows",
    )


def _selected_template_files(*, args) -> list[tuple[Traversable, Path]]:
    """Return the template files selected by CLI flags."""
    if args.no_skills and args.skills_only:
        raise ValueError("Cannot combine --no-skills with --skills-only.")

    groups = ["skills"] if args.skills_only else ["base", "skills"]
    if args.no_skills:
        groups = ["base"]

    files: list[tuple[Traversable, Path]] = []
    for group in groups:
        destination_prefix = Path("skills") if group == "skills" else None
        files.extend(
            _template_files(
                template_root=_template_root(group=group),
                destination_prefix=destination_prefix,
            )
        )
    return files


def command_init(args) -> int:
    """Handle ``ginkgo init``."""
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rich_console = console(sys.stdout)
    context = _template_context(root=root)
    files = _selected_template_files(args=args)

    conflicts = [
        root / relative_path for _, relative_path in files if (root / relative_path).exists()
    ]
    if conflicts and not args.force:
        conflict_list = "\n".join(str(path.relative_to(root)) for path in conflicts)
        raise FileExistsError(
            f"Refusing to overwrite existing scaffold files without --force:\n{conflict_list}"
        )

    written_paths: list[Path] = []

    # Copy the starter template file-by-file so path and content substitutions stay explicit.
    for source_path, relative_path in files:
        destination = root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            content = source_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            with resources.as_file(source_path) as materialized_source:
                shutil.copy2(materialized_source, destination)
        else:
            destination.write_text(
                _render_template_content(
                    content=content,
                    context=context,
                ),
                encoding="utf-8",
            )
        written_paths.append(destination)

    rich_console.print(f"[bold green]🌿 ginkgo init[/] [bold]{root.name}[/]\n")
    rich_console.print(f"[green]✓[/] Initialized project scaffold at [bold]{root}[/]")
    rich_console.print("[cyan]Created:[/]")
    for path in written_paths:
        rich_console.print(f"  [green]•[/] {path.relative_to(root)}")
    rich_console.print(
        f"\n[dim]Your workflow lives in[/] [bold]{context.workflow_relpath}[/]"
        f"[dim]; tasks live in[/] [bold]{context.modules_relpath}/[/]"
    )
    rich_console.print(
        "[dim]Next steps:[/] [bold]cd[/] "
        f"[bold]{root.name}[/] and run [bold]ginkgo test --dry-run[/]"
    )
    return 0
