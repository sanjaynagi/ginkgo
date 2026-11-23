"""``agent()``: run a coding-agent CLI as a task.

Called from inside a ``@task("agent")`` body, ``agent()`` returns an
``AgentDirective``, which ginkgo-agent's runner executes. The body runs while
the task is prepared, so what the directive resolves joins the cache key: the
runner, model and CLI version from ``[agents]``, and the brief's and schema's
contents. The task's arguments are hashed as any task's are, which is why every
path in ``reads`` should be one of them.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import final

from jsonschema import SchemaError
from jsonschema.validators import validator_for

from ginkgo.core.directive import DirectiveError, ExecutionDirective
from ginkgo.core.hashing import hash_file, hash_str
from ginkgo_agent.clis import cli_version
from ginkgo_agent.settings import AgentSettings, load_settings


@final
@dataclass(frozen=True)
class AgentDirective(ExecutionDirective):
    """A request to run one agent attempt.

    Parameters
    ----------
    brief : Path
        The prompt, sent on stdin with the attempt's paths appended.
    reads : tuple[Path, ...]
        The paths the agent reads. They must not change while it runs.
    output_schema : Path | None
        A JSON Schema the final message must validate against.
    network : bool
        Whether the agent may use the network.
    timeout_minutes : float | None
        How long the CLI may run before it is stopped and the task fails.
    settings : AgentSettings
        The CLI, from ``[agents]``.
    cli_version : str
        The CLI's version, recorded and part of the cache key.
    source_hash : str
        Folded into the cache key.
    """

    brief: Path
    reads: tuple[Path, ...]
    output_schema: Path | None
    network: bool
    timeout_minutes: float | None
    settings: AgentSettings
    cli_version: str
    source_hash: str


def agent(
    *,
    brief: str | Path,
    reads: Sequence[str | Path] = (),
    output_schema: str | Path | None = None,
    network: bool = False,
    timeout_minutes: float | None = 60,
) -> AgentDirective:
    """Run the configured agent CLI on a brief, in a fresh workspace.

    The task's value is the attempt's workspace, a folder holding what the
    agent wrote and its final message as ``final_message.json`` (with an
    ``output_schema``) or ``final_message.txt``.

    Parameters
    ----------
    brief : str | Path
        The prompt file.
    reads : Sequence[str | Path]
        Paths the agent reads, listed in its prompt. Pass each as a task
        argument too, so its contents join the cache key.
    output_schema : str | Path | None
        A JSON Schema the agent's final message must satisfy.
    network : bool
        Whether the agent may use the network.
    timeout_minutes : float | None
        Stop the agent and fail the task after this long; ``None`` for no limit.

    Returns
    -------
    AgentDirective

    Raises
    ------
    FileNotFoundError
        If the brief, a read or the schema does not exist.
    DirectiveError
        If the schema is not a valid JSON Schema, ``[agents]`` is wrong, or the
        CLI cannot be run.
    """
    brief_path = _existing(brief, what="Agent brief")
    read_paths = tuple(_existing(read, what="Agent read") for read in reads)
    schema_path = None
    if output_schema is not None:
        schema_path = _existing(output_schema, what="Agent output schema")
        _check_schema(schema_path)
    if timeout_minutes is not None and timeout_minutes <= 0:
        raise DirectiveError(f"agent(timeout_minutes={timeout_minutes!r}) must be positive.")

    settings = load_settings()
    version = cli_version(runner=settings.runner, command=settings.command)
    identity = {
        "runner": settings.runner,
        "cli_version": version,
        "model": settings.model,
        "isolation": settings.isolation,
        "network": network,
        "brief": hash_file(brief_path),
        "output_schema": hash_file(schema_path) if schema_path is not None else None,
    }
    return AgentDirective(
        brief=brief_path,
        reads=read_paths,
        output_schema=schema_path,
        network=network,
        timeout_minutes=timeout_minutes,
        settings=settings,
        cli_version=version,
        source_hash=hash_str(json.dumps(identity, sort_keys=True)),
    )


def _existing(path: str | Path, *, what: str) -> Path:
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"{what} not found: {str(resolved)!r}")
    return resolved


def _check_schema(path: Path) -> None:
    """Refuse a schema file that is not JSON, or not a valid JSON Schema."""
    try:
        schema = json.loads(path.read_text(encoding="utf-8"))
        validator_for(schema).check_schema(schema)
    except (json.JSONDecodeError, SchemaError) as exc:
        raise DirectiveError(
            f"Agent output schema {str(path)!r} is not a valid JSON Schema: {exc}"
        )
