"""Core's own plugin: the implementations ginkgo ships with.

Registered through the same registry as any installed plugin, so core has one
dispatch path rather than built-ins plus extensions. The registry loads it
directly, ahead of the entry-point group, so it never depends on package
metadata being installed.
"""

from __future__ import annotations

from ginkgo.core.notebook import NotebookDirective
from ginkgo.core.script import ScriptDirective
from ginkgo.core.shell import ShellDirective
from ginkgo.core.subworkflow import SubWorkflowDirective
from ginkgo.envs.container import build_container_environment
from ginkgo.envs.pixi import build_pixi_environment
from ginkgo.plugins.api import (
    EnvironmentType,
    Plugin,
    SecretBackendType,
    TaskKind,
)
from ginkgo.runtime.environment.secrets import build_env_backends
from ginkgo.runtime.task_runners.notebook import build_notebook_runner
from ginkgo.runtime.task_runners.script import build_script_runner
from ginkgo.runtime.task_runners.shell import build_shell_runner
from ginkgo.runtime.task_runners.subworkflow import build_subworkflow_runner

__all__ = ["BUILTIN"]

BUILTIN = Plugin(
    name="ginkgo",
    contributions=(
        SecretBackendType(name="env", build=build_env_backends),
        EnvironmentType(name="pixi", schemes=("",), label="pixi", build=build_pixi_environment),
        EnvironmentType(
            name="container",
            schemes=("docker", "oci"),
            label="container",
            build=build_container_environment,
        ),
        TaskKind(
            name="python",
            directive=None,
            call_form="",
            execution_mode="worker",
            allows_env=False,
            remote_capable=True,
        ),
        TaskKind(
            name="shell",
            directive=ShellDirective,
            call_form="shell(...)",
            execution_mode="driver",
            allows_env=True,
            remote_capable=False,
            exit_code_retries=True,
            runner=build_shell_runner,
        ),
        TaskKind(
            name="script",
            directive=ScriptDirective,
            call_form="script(...)",
            execution_mode="driver",
            allows_env=True,
            remote_capable=False,
            eager_source_hash=True,
            runner=build_script_runner,
        ),
        TaskKind(
            name="notebook",
            directive=NotebookDirective,
            call_form="notebook(...)",
            execution_mode="driver",
            allows_env=True,
            remote_capable=False,
            eager_source_hash=True,
            runner=build_notebook_runner,
        ),
        TaskKind(
            name="subworkflow",
            directive=SubWorkflowDirective,
            call_form="subworkflow(...)",
            execution_mode="worker",
            allows_env=False,
            remote_capable=False,
            runner=build_subworkflow_runner,
        ),
    ),
)
"""Everything core contributes, in the order the registry indexes it."""
