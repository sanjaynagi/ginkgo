"""Project scaffolding command."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from importlib.resources.abc import Traversable
import shutil
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

from ginkgo.cli.common import console


_TEMPLATE_PROJECT_NAME = "ginkgo-init-template"
_TEMPLATE_PACKAGE_NAME = "ginkgo_init_template"


@dataclass(frozen=True, kw_only=True)
class TemplateContext:
    """Values rendered into scaffold templates."""

    project_name: str
    package_name: str
    workflow_relpath: str
    modules_relpath: str
    envs_relpath: str
    tests_relpath: str


def _package_name_for_directory(*, root: Path) -> str:
    """Return a stable importable package name for the scaffolded project."""
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in root.name)
    normalized = normalized.strip("_") or "ginkgo_project"
    if normalized[0].isdigit():
        normalized = f"project_{normalized}"
    return normalized


def _template_root(*, group: str):
    """Return the packaged template root for one scaffold group."""
    return resources.files("ginkgo.templates.init").joinpath(group)


def _render_template_path(*, relative_path: PurePosixPath, package_name: str) -> Path:
    """Return the destination-relative path for one template file."""
    rendered_parts = [
        part.replace(_TEMPLATE_PACKAGE_NAME, package_name) for part in relative_path.parts
    ]
    return Path(*rendered_parts)


def _render_template_content(*, content: str, context: TemplateContext) -> str:
    """Return template content with project and package substitutions applied."""
    rendered = content.replace(_TEMPLATE_PROJECT_NAME, context.project_name).replace(
        _TEMPLATE_PACKAGE_NAME,
        context.package_name,
    )
    replacements = {
        "{{ project_name }}": context.project_name,
        "{{ package_name }}": context.package_name,
        "{{ workflow_relpath }}": context.workflow_relpath,
        "{{ modules_relpath }}": context.modules_relpath,
        "{{ envs_relpath }}": context.envs_relpath,
        "{{ tests_relpath }}": context.tests_relpath,
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
    *, template_root: Traversable, package_name: str, destination_prefix: Path | None = None
) -> list[tuple[Traversable, Path]]:
    """Return ``(source, relative_dest)`` pairs for the starter template."""
    files: list[tuple[Traversable, Path]] = []
    for source_path, relative_path in _iter_template_files(template_root):
        rendered_path = _render_template_path(
            relative_path=relative_path, package_name=package_name
        )
        if destination_prefix is not None:
            rendered_path = destination_prefix / rendered_path
        files.append(
            (
                source_path,
                rendered_path,
            )
        )
    return files


def _template_context(*, root: Path, package_name: str) -> TemplateContext:
    """Return the scaffold render context for one project root."""
    package_dir = Path(package_name)
    return TemplateContext(
        project_name=root.name,
        package_name=package_name,
        workflow_relpath=str(package_dir / "workflow.py"),
        modules_relpath=str(package_dir / "modules"),
        envs_relpath=str(package_dir / "envs"),
        tests_relpath="tests/workflows",
    )


def _selected_template_files(*, package_name: str, args) -> list[tuple[Traversable, Path]]:
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
                package_name=package_name,
                destination_prefix=destination_prefix,
            )
        )
    return files


def command_init(args) -> int:
    """Handle ``ginkgo init``."""
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rich_console = console(sys.stdout)
    package_name = _package_name_for_directory(root=root)
    context = _template_context(root=root, package_name=package_name)
    files = _selected_template_files(package_name=package_name, args=args)

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
        "\n[dim]Next steps:[/] [bold]cd[/] "
        f"[bold]{root.name}[/] and run [bold]ginkgo test --dry-run[/]"
    )
    return 0
