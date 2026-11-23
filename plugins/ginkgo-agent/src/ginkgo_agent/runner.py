"""Running an ``agent()`` task: one CLI call per attempt, in a fresh workspace.

Each attempt gets its own directory in the run's ``agents/`` directory, so a
retry or a later ``--rerun`` never overwrites an earlier attempt::

    agents/task_0007_attempt_1/
        prompt.md            the brief, with this attempt's paths appended
        transcript.jsonl     the CLI's event stream
        final_message.txt    the agent's final message
        workspace/           its working directory: the task's value

The attempt fails on a timeout, a non-zero exit, a final message that does not
match the schema, or a change to any path in ``reads``. What the attempt cost
(runner, model, CLI version, token usage, seconds) is recorded against the task.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from jsonschema import ValidationError
from jsonschema.validators import validator_for

from ginkgo.core.types import is_path_shaped_annotation
from ginkgo.errors import CommandExitError, FailureKind, GinkgoError
from ginkgo.plugins.api import RunnerContext
from ginkgo.runtime.rundir import RunDir
from ginkgo.runtime.task_runners.driver import DriverTaskRunner
from ginkgo.workspace_layout import WorkspaceLayout
from ginkgo_agent.clis import AttemptPaths, build_command, read_final_message, usage_totals
from ginkgo_agent.directive import AgentDirective

if TYPE_CHECKING:
    from ginkgo.plugins.api import TaskInvocation


class AgentTaskError(GinkgoError, RuntimeError):
    """An agent attempt failed. ``retry_on=AgentTaskError`` retries any of them."""

    failure_kind: ClassVar[FailureKind] = "output_validation_error"


class AgentExitError(CommandExitError, AgentTaskError):
    """The agent CLI exited non-zero."""

    failure_kind: ClassVar[FailureKind] = "command_error"

    def __init__(self, *, task_name: str, exit_code: int, output: str, transcript: Path) -> None:
        self.exit_code = exit_code
        details = f"Agent task {task_name} failed: the CLI exited with code {exit_code}."
        if output.strip():
            details = f"{details}\n{output.strip()}"
        super().__init__(f"{details}\nIts event stream is in {transcript}.")


class AgentTimeoutError(AgentTaskError):
    """The agent CLI ran past ``timeout_minutes`` and was stopped."""

    failure_kind: ClassVar[FailureKind] = "command_error"


@dataclass(kw_only=True)
class AgentRunner(DriverTaskRunner):
    """Run agent tasks.

    Parameters
    ----------
    run_dir : RunDir | None
        The run directory, which holds the attempts under ``agents/``. ``None``
        when evaluating without one; attempts then go to ``.ginkgo/agents/``.
    annotate : Callable
        Records the attempt's runner, usage and directory against the task.
    notice : Callable
        Warns about a read that is not one of the task's arguments.
    """

    run_dir: RunDir | None
    annotate: Callable[..., None]
    notice: Callable[..., None]
    _warned: set[tuple[str, Path]] = field(default_factory=set, init=False, repr=False)

    def run(self, *, node: TaskInvocation, directive: AgentDirective) -> Any:
        """Run one attempt and return its workspace."""
        name = node.task_def.base_name
        # Absolute: the command runs in the workspace, and the CLI's run
        # directory is relative to the project root.
        paths = AttemptPaths(root=self._attempt_dir(node=node).resolve())
        paths.workspace.mkdir(parents=True)
        self._warn_untracked_reads(node=node, directive=directive)
        paths.prompt.write_text(_prompt(directive=directive, paths=paths), encoding="utf-8")
        cmd, env = build_command(
            settings=directive.settings,
            paths=paths,
            reads=directive.reads,
            output_schema=directive.output_schema,
            network=directive.network,
        )

        before = {read: _fingerprint(read) for read in directive.reads}
        timeout = directive.timeout_minutes * 60 if directive.timeout_minutes is not None else None
        started = time.monotonic()
        try:
            completed = self.shell_runner.run_logged_command(
                node=node, cmd=cmd, extra_env=env, timeout=timeout
            )
        except subprocess.TimeoutExpired as exc:
            raise AgentTimeoutError(
                f"Agent task {name} was stopped after {directive.timeout_minutes:g} minutes. "
                f"Its event stream so far is in {paths.transcript}."
            ) from exc
        finally:
            self._record(node=node, directive=directive, paths=paths, started=started)

        if completed.returncode != 0:
            raise AgentExitError(
                task_name=name,
                exit_code=completed.returncode,
                output=completed.stderr or "",
                transcript=paths.transcript,
            )
        changed = [str(read) for read in directive.reads if _fingerprint(read) != before[read]]
        if changed:
            raise AgentTaskError(
                f"Agent task {name} changed a path it was given to read: {', '.join(changed)}. "
                "An agent writes only in its workspace."
            )
        self._keep_final_message(name=name, directive=directive, paths=paths)
        return self.validator.coerce_return_value(
            task_def=node.task_def, value=str(paths.workspace)
        )

    def _attempt_dir(self, *, node: TaskInvocation) -> Path:
        """Return a new directory for this attempt."""
        if self.run_dir is None:
            root = WorkspaceLayout.for_cwd().root / "agents"
            root.mkdir(parents=True, exist_ok=True)
            return Path(tempfile.mkdtemp(prefix=f"task_{node.node_id:04d}_", dir=root))
        path = self.run_dir.path / "agents" / f"task_{node.node_id:04d}_attempt_{node.attempt}"
        if path.exists():
            shutil.rmtree(path)
        return path

    def _warn_untracked_reads(self, *, node: TaskInvocation, directive: AgentDirective) -> None:
        """Warn about a read outside every ``file`` or ``folder`` argument.

        Only those arguments are hashed by content, so a change to any other
        read would not re-run the task. Each task and read is named once a run.
        """
        assert node.resolved_args is not None
        hints = node.task_def.type_hints
        arguments = [
            Path(path).resolve()
            for name, value in node.resolved_args.items()
            if is_path_shaped_annotation(hints.get(name))
            for path in _path_values([value])
        ]
        for read in directive.reads:
            warned = (node.task_def.base_name, read)
            if warned in self._warned:
                continue
            if not any(read == arg or read.is_relative_to(arg) for arg in arguments):
                self._warned.add(warned)
                self.notice(
                    node=node,
                    message=(
                        f"{node.task_def.base_name} reads {read}, which is not one of its "
                        "arguments, so a change to it will not re-run the task. Pass it as a "
                        "`file` or `folder` argument."
                    ),
                )

    def _keep_final_message(
        self, *, name: str, directive: AgentDirective, paths: AttemptPaths
    ) -> None:
        """Check the final message against the schema, and copy it into the workspace."""
        message = read_final_message(settings=directive.settings, paths=paths)
        if directive.output_schema is None:
            if message is not None:
                (paths.workspace / "final_message.txt").write_text(message, encoding="utf-8")
            return
        if message is None:
            raise AgentTaskError(
                f"Agent task {name} finished without a final message to check against "
                f"{directive.output_schema}."
            )
        try:
            report = json.loads(message)
        except json.JSONDecodeError as exc:
            raise AgentTaskError(
                f"Agent task {name}'s final message is not JSON ({exc}), so it cannot match "
                f"{directive.output_schema}. It is in {paths.final_message}."
            ) from exc
        schema = json.loads(directive.output_schema.read_text(encoding="utf-8"))
        try:
            validator_for(schema)(schema).validate(report)
        except ValidationError as exc:
            location = "/".join(str(part) for part in exc.absolute_path) or "the top level"
            raise AgentTaskError(
                f"Agent task {name}'s final message does not match {directive.output_schema}: "
                f"{exc.message} (at {location}). It is in {paths.final_message}."
            ) from exc
        (paths.workspace / "final_message.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )

    def _record(
        self,
        *,
        node: TaskInvocation,
        directive: AgentDirective,
        paths: AttemptPaths,
        started: float,
    ) -> None:
        """Record what the attempt ran on and cost, and leave it for a cache hit."""
        fields: dict[str, Any] = {
            "agent_runner": directive.settings.runner,
            "agent_model": directive.settings.model,
            "agent_cli_version": directive.cli_version,
            "agent_seconds": round(time.monotonic() - started, 3),
            "agent_usage": usage_totals(paths.transcript),
        }
        if self.run_dir is not None:
            fields["agent_attempt_dir"] = str(paths.root.relative_to(self.run_dir.path.resolve()))
            fields["agent_attempt_run_id"] = self.run_dir.run_id
        self.annotate(node=node, fields=fields)
        node.runner_extras = fields


def _prompt(*, directive: AgentDirective, paths: AttemptPaths) -> str:
    """Return the brief with this attempt's workspace and inputs appended."""
    lines = [
        directive.brief.read_text(encoding="utf-8").rstrip(),
        "",
        "---",
        "",
        f"Your working directory is {paths.workspace}. Write files only there.",
    ]
    if directive.reads:
        lines += ["", "Read these inputs, and do not change them:"]
        lines += [f"- {read}" for read in directive.reads]
    if directive.output_schema is not None:
        lines += ["", "Your final message must be JSON that matches the output schema."]
    return "\n".join(lines) + "\n"


def _fingerprint(path: Path) -> tuple[tuple[str, int, int], ...]:
    """Return each entry's relative path, size and mtime under *path*, cheaply.

    Directories count, so adding or removing an empty one is a change, and
    symbolic links are followed, as the agent would follow them.
    """
    if path.is_file():
        stat = path.stat()
        return (("", stat.st_size, stat.st_mtime_ns),)
    entries = []
    for root, dirs, files in os.walk(path, followlinks=True):
        for name in [*dirs, *files]:
            child = Path(root) / name
            try:
                stat = child.stat()
            except FileNotFoundError:  # a broken link
                stat = child.lstat()
            entries.append((str(child.relative_to(path)), stat.st_size, stat.st_mtime_ns))
    return tuple(sorted(entries))


def _path_values(values: Any) -> Iterator[str]:
    """Yield every string among the argument values, looking inside containers."""
    for value in values:
        if isinstance(value, str):
            yield value
        elif isinstance(value, list | tuple | set):
            yield from _path_values(value)
        elif isinstance(value, dict):
            yield from _path_values(value.values())


def build_agent_runner(context: RunnerContext) -> AgentRunner:
    """Build the agent runner for a run."""
    return AgentRunner(
        shell_runner=context.shell_runner,
        validator=context.validator,
        run_dir=context.run_dir,
        annotate=context.annotate,
        notice=context.notice,
    )
