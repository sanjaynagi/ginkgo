"""The ``[agents]`` table of ``ginkgo.toml``: which agent CLI a run calls.

The runner is chosen by config rather than in the workflow, so one workflow runs
under Codex or Claude Code unchanged::

    [agents]
    runner = "codex"        # or "claude", or "stub"
    model = "gpt-5-codex"   # optional; the CLI's default otherwise
    command = "codex"       # optional; the executable, default the runner's name

``stub`` runs ``command`` as a script, for testing a workflow without calling a
model. Each of these joins an agent task's cache key, with the CLI's version.
"""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ginkgo.config import current_session, load_runtime_config
from ginkgo.core.directive import DirectiveError

RUNNERS = ("codex", "claude", "stub")

# Isolation is how far the agent can see past its inputs and workspace. Only
# "none" is built: the CLI's own sandbox is all there is.
ISOLATIONS = ("none",)


@dataclass(frozen=True, kw_only=True)
class AgentSettings:
    """How a run calls its agent CLI.

    Parameters
    ----------
    runner : str
        One of :data:`RUNNERS`.
    command : str
        The executable as an absolute path when it can be found: the runner's
        name on ``PATH`` by default, or the stub's script. The agent runs in its
        workspace, so a path relative to the project would not resolve there.
    model : str | None
        The model to ask for, or ``None`` for the CLI's default.
    isolation : str
        One of :data:`ISOLATIONS`.
    """

    runner: str
    command: str
    model: str | None = None
    isolation: str = "none"


def load_settings() -> AgentSettings:
    """Read ``[agents]`` from the run's config, ``--config`` files included.

    Raises
    ------
    DirectiveError
        If the table names an unknown runner or isolation, or ``stub`` without
        a command.
    """
    session = current_session()
    config = load_runtime_config(
        project_root=Path.cwd(),
        override_paths=session.override_paths if session is not None else None,
    )
    return settings_from_table(config.get("agents"))


def settings_from_table(table: Any) -> AgentSettings:
    """Build :class:`AgentSettings` from an ``[agents]`` table, or ``None``."""
    table = table if isinstance(table, Mapping) else {}
    runner = str(table.get("runner", "codex"))
    if runner not in RUNNERS:
        raise DirectiveError(
            f"[agents] runner = {runner!r} is not a known runner; use one of {', '.join(RUNNERS)}."
        )
    command = table.get("command")
    if runner == "stub" and not command:
        raise DirectiveError('[agents] runner = "stub" needs command = "<path to the script>".')
    isolation = str(table.get("isolation", "none"))
    if isolation not in ISOLATIONS:
        raise DirectiveError(
            f"[agents] isolation = {isolation!r} is not available yet; only "
            f"{', '.join(repr(name) for name in ISOLATIONS)} is."
        )
    model = table.get("model")
    return AgentSettings(
        runner=runner,
        command=_resolve_command(runner=runner, command=str(command) if command else runner),
        model=str(model) if model else None,
        isolation=isolation,
    )


def _resolve_command(*, runner: str, command: str) -> str:
    """Return *command* as an absolute path, or unchanged if it cannot be found.

    A stub's script is a path; a CLI is looked up on ``PATH`` unless given as a
    path. A command that cannot be found is left for :func:`cli_version` to
    report.
    """
    expanded = str(Path(command).expanduser())
    if runner == "stub":
        return str(Path(expanded).resolve())
    found = shutil.which(expanded)
    return str(Path(found).resolve()) if found is not None else expanded
