"""Project scaffolding command."""

from __future__ import annotations

import sys
from pathlib import Path

from ginkgo.cli.common import console


_WORKFLOW_TEMPLATE = """from pathlib import Path

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

- Define tasks at module scope with `@task()`.
- Keep one `@flow` entrypoint per workflow module for CLI discovery.
- Use explicit task inputs and deterministic output paths.
- Use `file`, `folder`, and `tmp_dir` annotations when path semantics matter.
- Use `shell_task(...)` for command-line tools and always provide an explicit `output`.
- Bump `version=` when task logic changes in a cache-relevant way.
- Prefer `.map()` for fan-out and normal downstream tasks for fan-in.
- Declare `env=` when a task depends on a reproducible Pixi environment.
"""


def command_init(args) -> int:
    """Handle ``ginkgo init``."""
    root = Path(args.directory).resolve()
    root.mkdir(parents=True, exist_ok=True)
    rich_console = console(sys.stdout)

    files = {
        root / "workflow.py": _WORKFLOW_TEMPLATE,
        root / "ginkgo.toml": _CONFIG_TEMPLATE,
        root / ".tests" / "smoke.py": _TEST_TEMPLATE,
        root / "envs" / "analysis_tools" / "pixi.toml": _ENV_TEMPLATE,
        root / "agents.ginkgo.md": _AGENT_TEMPLATE,
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
