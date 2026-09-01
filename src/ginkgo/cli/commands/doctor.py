"""Doctor command handlers."""

from __future__ import annotations

import os
import json
from pathlib import Path
import sys

from rich.markup import escape

from ginkgo.cli.common import console
from ginkgo.cli.workspace import resolve_envs_workflow_root, resolve_workflow_path
from ginkgo.config import load_runtime_config
from ginkgo.envs.container import container_backend_from_config
from ginkgo.envs.interpreter import EnvironmentFinding, detect_import_problem
from ginkgo.envs.pixi import PixiRegistry
from ginkgo.remote.access.doctor import AccessDiagnostic, collect_access_diagnostics
from ginkgo.runtime.backend import CompositeEnvironment, LocalEnvironment
from ginkgo.runtime.diagnostics import collect_workflow_diagnostics
from ginkgo.runtime.environment.secrets import build_secret_resolver


def command_doctor(args) -> int:
    """Handle ``ginkgo doctor``."""
    workflow_path = resolve_workflow_path(
        project_root=Path.cwd(),
        workflow=args.workflow,
    ).path
    # Read the runtime config the way ``run`` does. A config *session* only
    # accumulates values as the workflow module calls ``config(path)`` during
    # import, which happens later, inside collect_workflow_diagnostics -- so
    # reading a session here yielded an empty mapping, and every setting below
    # silently fell back to its default.
    config = load_runtime_config(
        project_root=Path.cwd(),
        override_paths=[Path(path).resolve() for path in args.config],
    )

    # Same environment pair that ``run`` builds, so doctor reaches the
    # declared-env check and searches the env directories the run will use --
    # the canonical package's, not those beside whichever file is being checked.
    # Validation only resolves manifests and probes PATH; nothing is built or
    # installed. Built inside collect_workflow_diagnostics's try/except so
    # construction failures surface as a diagnostic, not a crash.
    def build_backend() -> CompositeEnvironment:
        return CompositeEnvironment(
            local=LocalEnvironment(
                pixi_registry=PixiRegistry(
                    project_root=Path.cwd(),
                    workflow_root=resolve_envs_workflow_root(project_root=Path.cwd()),
                )
            ),
            container=container_backend_from_config(project_root=Path.cwd(), config=config),
        )

    diagnostics = collect_workflow_diagnostics(
        workflow_path=workflow_path,
        config_paths=[Path(path).resolve() for path in args.config],
        secret_resolver=build_secret_resolver(
            project_root=Path.cwd(),
            config=config,
            environ=os.environ,
        ),
        backend_factory=build_backend,
        param_extras=getattr(args, "param_extras", ()),
    )

    # Additional FUSE-streaming probes. These produce their own diagnostic
    # shape; normalise into the workflow diagnostic format for rendering.
    access_diagnostics = collect_access_diagnostics(
        project_root=Path.cwd(),
        executor_configs=_extract_executor_configs(config=config),
    )

    # A Python task body runs in this very interpreter and cannot declare
    # ``env=``, so the project manifest is the environment it needs. When the
    # two have parted company -- a globally installed CLI run inside a pixi
    # project -- the only symptom is a bare ModuleNotFoundError from whichever
    # task imports first. Reported here, where it is cheap to see. Running
    # from the project's own environment gets the other finding: the same
    # missing import, but a fix that edits the manifest instead.
    import_problem = detect_import_problem(workflow_path=workflow_path, project_root=Path.cwd())
    environment_diagnostics: list[AccessDiagnostic | EnvironmentFinding] = [
        *([] if import_problem is None else [import_problem]),
        *access_diagnostics,
    ]

    if args.json:
        combined = [item.to_payload() for item in diagnostics]
        combined.extend(
            {
                "severity": item.severity,
                "code": item.code,
                "message": item.message,
                "location": None,
                "suggestion": item.suggestion,
            }
            for item in environment_diagnostics
        )
        has_errors = any(
            item.severity == "error" for item in (*diagnostics, *environment_diagnostics)
        )
        print(
            json.dumps(
                {"ok": not has_errors, "diagnostics": combined},
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if not has_errors else 1

    rich_console_out = console(sys.stdout)
    rich_console_err = console(sys.stderr)

    workflow_errors = [item for item in diagnostics if item.severity == "error"]
    if not workflow_errors:
        rich_console_out.print("[bold green]🌿 ginkgo doctor[/]\n")
        rich_console_out.print("[green]✓[/] Workflow validation passed")
    # Diagnostic text quotes config sections and task declarations, so it can
    # contain square brackets ("[remote.executors.<name>]") that Rich would
    # otherwise parse as a style tag and silently drop.
    for item in diagnostics:
        marker = {"error": "[red]✖[/]", "warning": "[yellow]⚠[/]"}.get(item.severity, "[cyan]ℹ[/]")
        target = rich_console_err if item.severity == "error" else rich_console_out
        target.print(f"{marker} {item.code}: {escape(item.message)}")
        if item.location:
            # Printed whole: a wrapped path cannot be clicked or copied.
            target.print(f"[dim]  at {escape(item.location)}[/]", soft_wrap=True)
        if item.suggestion:
            target.print(f"[dim]{escape(item.suggestion)}[/]")

    for item in environment_diagnostics:
        marker = {"error": "[red]✖[/]", "warning": "[yellow]![/]"}.get(item.severity, "[cyan]ℹ[/]")
        target = rich_console_err if item.severity == "error" else rich_console_out
        target.print(f"{marker} {item.code}: {escape(item.message)}")
        if item.suggestion:
            target.print(f"[dim]{escape(item.suggestion)}[/]")

    has_errors = bool(workflow_errors) or any(
        item.severity == "error" for item in environment_diagnostics
    )
    return 1 if has_errors else 0


def _extract_executor_configs(*, config: dict) -> dict[str, dict]:
    """Return every configured executor section, keyed by its config path.

    A task may be pinned to any configured executor, so the FUSE probes
    have to see all of them: diagnosing only one section would clear a run
    whose other executors cannot mount. Keys are the section names as they
    appear in ``ginkgo.toml`` (``"[remote.k8s]"``,
    ``"[remote.executors.gpu-k8s]"``) so a diagnostic can name the section
    the user has to edit.
    """
    remote = config.get("remote") if isinstance(config, dict) else None
    if not isinstance(remote, dict):
        return {}
    sections: dict[str, dict] = {}
    for type_name in ("k8s", "batch"):
        section = remote.get(type_name)
        if isinstance(section, dict):
            sections[f"[remote.{type_name}]"] = section
    named = remote.get("executors")
    if isinstance(named, dict):
        for name, table in named.items():
            if isinstance(table, dict):
                sections[f"[remote.executors.{name}]"] = table
    return sections
