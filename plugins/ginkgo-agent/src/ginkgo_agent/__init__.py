"""ginkgo-agent: coding-agent CLIs (Codex, Claude Code) as tasks.

``@task("agent")`` bodies return ``agent(brief=...)``. Each attempt runs the CLI
that ``[agents]`` in ``ginkgo.toml`` names, in a fresh workspace, and the task's
value is that workspace.
"""

from __future__ import annotations

from ginkgo.plugins.api import Plugin, TaskKind
from ginkgo_agent.directive import AgentDirective, agent
from ginkgo_agent.runner import (
    AgentExitError,
    AgentTaskError,
    AgentTimeoutError,
    build_agent_runner,
)

__all__ = [
    "PLUGIN",
    "AgentDirective",
    "AgentExitError",
    "AgentTaskError",
    "AgentTimeoutError",
    "agent",
]

PLUGIN = Plugin(
    name="ginkgo-agent",
    contributions=(
        TaskKind(
            name="agent",
            directive=AgentDirective,
            call_form="agent(...)",
            execution_mode="driver",
            allows_env=False,
            remote_capable=False,
            eager_source_hash=True,
            exit_code_retries=True,
            runner=build_agent_runner,
        ),
    ),
)
