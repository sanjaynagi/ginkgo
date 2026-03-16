"""Project scaffolding command."""

from __future__ import annotations

import sys
from pathlib import Path

from ginkgo.cli.common import console


_PACKAGE_WORKFLOW_TEMPLATE = """from {package_name}.modules.reporting import main

__all__ = ["main"]
"""


_MODULE_TEMPLATE = """from pathlib import Path

import ginkgo
from ginkgo import flow, task

cfg = ginkgo.config("ginkgo.toml")


@task()
def write_summary(message: str, output_path: str) -> str:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(message, encoding="utf-8")
    return output_path


@flow
def main():
    return write_summary(
        message=cfg["message"],
        output_path="results/summary.txt",
    )
"""

_ROOT_PIXI_TEMPLATE = """[workspace]
name = "{project_name}"
channels = ["conda-forge"]
platforms = ["osx-arm64", "linux-64"]

[dependencies]
python = ">=3.11"
"""

_CONFIG_TEMPLATE = """message = "hello from ginkgo"
"""

_TEST_TEMPLATE = """from ginkgo import flow, task


@task()
def noop(value: str) -> str:
    return value


@flow
def main():
    return noop(value="dry-run-ok")
"""

_ENV_TEMPLATE = """[workspace]
name = "analysis_tools"
channels = ["conda-forge"]
platforms = ["osx-arm64", "linux-64"]

[dependencies]
python = ">=3.11"
"""

_AGENT_TEMPLATE = """# Project Agent Notes

This project uses Ginkgo for reproducible workflow execution.

## Expectations

- Keep `{package_name}/workflow.py` focused on flow entrypoints and graph wiring.
- Put reusable task implementations under `{package_name}/modules/`.
- Define tasks at module scope with `@task()`.
- Use explicit task inputs and deterministic output paths.
- Use `file`, `folder`, and `tmp_dir` annotations when path semantics matter.
- Use `shell_task(...)` for command-line tools and always provide an explicit `output`.
- Bump `version=` when task logic changes in a cache-relevant way.
- Prefer `.map()` for fan-out and normal downstream tasks for fan-in.
- Declare `env=` when a task depends on a reproducible Pixi environment.
- Keep task environment manifests under `{package_name}/envs/`.
"""


def _package_name_for_directory(*, root: Path) -> str:
    """Return a stable importable package name for the scaffolded project."""
    normalized = "".join(ch.lower() if ch.isalnum() else "_" for ch in root.name)
    normalized = normalized.strip("_") or "ginkgo_project"
    if normalized[0].isdigit():
        normalized = f"project_{normalized}"
    return normalized


def _root_workflow_template(*, package_name: str) -> str:
    """Return the canonical workflow entrypoint template."""
    return _PACKAGE_WORKFLOW_TEMPLATE.format(package_name=package_name)


def command_init(args) -> int:
    """Handle ``ginkgo init``."""
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rich_console = console(sys.stdout)
    package_name = _package_name_for_directory(root=root)

    files = {
        root / "pixi.toml": _ROOT_PIXI_TEMPLATE.format(project_name=root.name),
        root / "ginkgo.toml": _CONFIG_TEMPLATE,
        root / package_name / "__init__.py": "",
        root / package_name / "workflow.py": _root_workflow_template(package_name=package_name),
        root / package_name / "modules" / "__init__.py": "",
        root / package_name / "modules" / "reporting.py": _MODULE_TEMPLATE,
        root / package_name / "envs" / "analysis_tools" / "pixi.toml": _ENV_TEMPLATE,
        root / "tests" / "workflows" / "smoke.py": _TEST_TEMPLATE,
        root / "agents.ginkgo.md": _AGENT_TEMPLATE.format(package_name=package_name),
    }

    conflicts = [path for path in files if path.exists()]
    if conflicts and not args.force:
        conflict_list = "\n".join(str(path.relative_to(root)) for path in conflicts)
        raise FileExistsError(
            f"Refusing to overwrite existing scaffold files without --force:\n{conflict_list}"
        )

    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    rich_console.print(f"[bold green]🌿 ginkgo init[/] [bold]{root.name}[/]\n")
    rich_console.print(f"[green]✓[/] Initialized project scaffold at [bold]{root}[/]")
    rich_console.print("[cyan]Created:[/]")
    for path in files:
        rich_console.print(f"  [green]•[/] {path.relative_to(root)}")
    rich_console.print(
        "\n[dim]Next steps:[/] [bold]cd[/] "
        f"[bold]{root.name}[/] and run [bold]ginkgo test --dry-run[/]"
    )
    return 0
