"""Project scaffolding command."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

from ginkgo.cli.common import console


_TEMPLATE_PROJECT_NAME = "ginkgo-init-template"
_TEMPLATE_PACKAGE_NAME = "ginkgo_init_template"


def _package_name_for_directory(*, root: Path) -> str:
    """Return a stable importable package name for the scaffolded project."""
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in root.name)
    normalized = normalized.strip("_") or "ginkgo_project"
    if normalized[0].isdigit():
        normalized = f"project_{normalized}"
    return normalized


def _starter_template_root() -> Path:
    """Return the repository starter-template root."""
    return Path(__file__).resolve().parents[3] / "examples" / "init"


def _render_template_path(*, relative_path: Path, package_name: str) -> Path:
    """Return the destination-relative path for one template file."""
    rendered_parts = [
        part.replace(_TEMPLATE_PACKAGE_NAME, package_name) for part in relative_path.parts
    ]
    return Path(*rendered_parts)


def _render_template_content(*, content: str, project_name: str, package_name: str) -> str:
    """Return template content with project and package substitutions applied."""
    return content.replace(_TEMPLATE_PROJECT_NAME, project_name).replace(
        _TEMPLATE_PACKAGE_NAME,
        package_name,
    )


def _template_files(*, template_root: Path, package_name: str) -> list[tuple[Path, Path]]:
    """Return ``(source, relative_dest)`` pairs for the starter template."""
    files: list[tuple[Path, Path]] = []
    for source_path in sorted(path for path in template_root.rglob("*") if path.is_file()):
        relative_path = source_path.relative_to(template_root)
        files.append(
            (
                source_path,
                _render_template_path(relative_path=relative_path, package_name=package_name),
            )
        )
    return files


def command_init(args) -> int:
    """Handle ``ginkgo init``."""
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rich_console = console(sys.stdout)
    package_name = _package_name_for_directory(root=root)
    template_root = _starter_template_root()
    files = _template_files(template_root=template_root, package_name=package_name)

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
            shutil.copy2(source_path, destination)
        else:
            destination.write_text(
                _render_template_content(
                    content=content,
                    project_name=root.name,
                    package_name=package_name,
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
