"""How each agent CLI is called, and how its final message and usage are read back.

Every runner gets the same contract from :class:`AttemptPaths`: it runs with the
workspace as its working directory, takes the prompt on stdin, writes its event
stream (JSONL) to ``transcript.jsonl``, and leaves its final message in
``final_message``.

- ``codex``: ``codex exec`` in its ``workspace-write`` sandbox, which confines
  writes to the workspace and turns the network off unless asked for. Reads are
  not confined.
- ``claude``: ``claude -p`` with edits accepted in the workspace and each read
  added as a directory, with edits there refused. Claude Code has no network
  switch, so ``network=False`` refuses its web tools only.
- ``stub``: a script, given the paths in ``GINKGO_AGENT_*`` variables, for
  testing a workflow without calling a model.
"""

from __future__ import annotations

import json
import shlex
import subprocess
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

from ginkgo.core.directive import DirectiveError
from ginkgo.core.hashing import hash_file
from ginkgo_agent.settings import AgentSettings


@dataclass(frozen=True, kw_only=True)
class AttemptPaths:
    """The files of one attempt, all under its attempt directory.

    Parameters
    ----------
    root : Path
        The attempt directory.
    """

    root: Path

    @property
    def workspace(self) -> Path:
        """The agent's working directory, and the only place it should write."""
        return self.root / "workspace"

    @property
    def prompt(self) -> Path:
        """The brief, with the paths of this attempt appended, as sent on stdin."""
        return self.root / "prompt.md"

    @property
    def transcript(self) -> Path:
        """The CLI's event stream, one JSON object per line."""
        return self.root / "transcript.jsonl"

    @property
    def final_message(self) -> Path:
        """The agent's final message, as the CLI left it."""
        return self.root / "final_message.txt"


@cache
def cli_version(*, runner: str, command: str) -> str:
    """Return what identifies the CLI's behaviour, for the cache key.

    The CLI's ``--version`` output, or for ``stub`` a hash of its script, so
    editing the stub re-runs its tasks. Memoised: every branch of a fan-out asks.

    Raises
    ------
    DirectiveError
        If the command cannot be found or run.
    """
    if runner == "stub":
        script = Path(command).expanduser()
        if not script.is_file():
            raise DirectiveError(f"[agents] command = {command!r}: no such stub script.")
        return f"stub {hash_file(script.resolve())}"
    try:
        completed = subprocess.run(
            [command, "--version"], capture_output=True, text=True, timeout=60, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise DirectiveError(
            f"Cannot run the {runner} CLI {command!r} to read its version: {exc}. "
            'Install it, or set [agents] command = "<path>" in ginkgo.toml.'
        ) from exc
    version = completed.stdout.strip().splitlines()
    if completed.returncode != 0 or not version:
        raise DirectiveError(
            f"`{command} --version` failed (exit {completed.returncode}): "
            f"{completed.stderr.strip() or 'no output'}"
        )
    return version[0]


def build_command(
    *,
    settings: AgentSettings,
    paths: AttemptPaths,
    reads: tuple[Path, ...],
    output_schema: Path | None,
    network: bool,
) -> tuple[str, dict[str, str]]:
    """Return the shell command for one attempt, and the variables it needs.

    The command runs in the workspace, reads the prompt on stdin and writes the
    event stream to the transcript; ``bash -o pipefail`` runs it.
    """
    q = shlex.quote
    ws = q(str(paths.workspace))
    redirect = f"< {q(str(paths.prompt))} > {q(str(paths.transcript))}"
    if settings.runner == "codex":
        parts = [
            q(settings.command),
            "exec",
            "--sandbox workspace-write",
            f"--cd {ws}",
            "--skip-git-repo-check",
            "--ephemeral",
            "--json",
            f"-o {q(str(paths.final_message))}",
        ]
        if output_schema is not None:
            parts.append(f"--output-schema {q(str(output_schema))}")
        if settings.model is not None:
            parts.append(f"-m {q(settings.model)}")
        if network:
            parts.append("-c sandbox_workspace_write.network_access=true")
        parts.append("-")
        return f"cd {ws} && {' '.join(parts)} {redirect}", {}

    if settings.runner == "claude":
        parts = [
            q(settings.command),
            "-p",
            "--output-format stream-json",
            "--verbose",
            "--permission-mode acceptEdits",
            "--no-session-persistence",
        ]
        if settings.model is not None:
            parts.append(f"--model {q(settings.model)}")
        if output_schema is not None:
            schema_text = json.dumps(json.loads(output_schema.read_text(encoding="utf-8")))
            parts.append(f"--json-schema {q(schema_text)}")
        refused: list[str] = []
        for read in reads:
            directory = read if read.is_dir() else read.parent
            parts.append(f"--add-dir {q(str(directory))}")
            refused.extend(
                [f"Edit({read}/**)", f"Write({read}/**)", f"Edit({read})", f"Write({read})"]
            )
        if not network:
            refused.extend(["WebFetch", "WebSearch"])
        if refused:
            parts.append("--disallowedTools " + " ".join(q(rule) for rule in refused))
        return f"cd {ws} && {' '.join(parts)} {redirect}", {}

    env = {
        "GINKGO_AGENT_WORKSPACE": str(paths.workspace),
        "GINKGO_AGENT_FINAL_MESSAGE": str(paths.final_message),
        "GINKGO_AGENT_READS": "\n".join(str(read) for read in reads),
        "GINKGO_AGENT_OUTPUT_SCHEMA": str(output_schema) if output_schema is not None else "",
        "GINKGO_AGENT_MODEL": settings.model or "",
        "GINKGO_AGENT_NETWORK": "1" if network else "0",
    }
    script = q(str(Path(settings.command).expanduser().resolve()))
    return f"cd {ws} && {script} {redirect}", env


def read_final_message(*, settings: AgentSettings, paths: AttemptPaths) -> str | None:
    """Return the agent's final message, or ``None`` if it left none.

    Claude Code reports it in the last ``result`` event of its stream, its
    structured output when a schema was given; it is written to
    ``final_message`` so every runner's attempt keeps it in the same place.
    """
    if settings.runner == "claude":
        message = _claude_result(paths.transcript)
        if message is not None:
            paths.final_message.write_text(message, encoding="utf-8")
    if not paths.final_message.is_file():
        return None
    return paths.final_message.read_text(encoding="utf-8")


def _claude_result(transcript: Path) -> str | None:
    """Return the final message of Claude Code's stream, if it reached one."""
    result: str | None = None
    for event in iter_events(transcript):
        if event.get("type") != "result":
            continue
        structured = event.get("structured_output")
        if structured is not None:
            result = json.dumps(structured)
        elif isinstance(event.get("result"), str):
            result = event["result"]
    return result


def usage_totals(transcript: Path) -> dict[str, float]:
    """Sum the numbers in each event's top-level ``usage``, and any cost.

    Codex reports usage per turn (``turn.completed``) and Claude Code once, in
    its ``result`` event, so summing top-level ``usage`` counts each once.
    Usage nested deeper, inside a message, is a per-message copy and is skipped.
    """
    totals: dict[str, float] = {}
    for event in iter_events(transcript):
        usage = event.get("usage")
        if isinstance(usage, dict):
            for key, value in usage.items():
                if isinstance(value, int | float) and not isinstance(value, bool):
                    totals[key] = totals.get(key, 0) + value
        cost = event.get("total_cost_usd")
        if isinstance(cost, int | float) and not isinstance(cost, bool):
            totals["cost_usd"] = totals.get("cost_usd", 0) + cost
    return totals


def iter_events(transcript: Path) -> list[dict[str, Any]]:
    """Return the JSON objects of a JSONL transcript, skipping any other line."""
    if not transcript.is_file():
        return []
    events: list[dict[str, Any]] = []
    for line in transcript.read_text(encoding="utf-8", errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events
