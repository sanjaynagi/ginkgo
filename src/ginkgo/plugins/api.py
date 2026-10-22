"""The plugin contract: a ``Plugin`` and the extension points it contributes to.

A plugin is a named bundle of contributions. Each contribution is a frozen
spec for exactly one extension point, registered under the keys config or user
code uses to select it — an executor ``type``, a URI scheme, a secret backend
name. Core looks a contribution up by key and calls the spec's ``build``; it
never names a concrete implementation.

Every ``build`` takes one argument: a frozen context object for the points that
need several inputs, so a field can be added without breaking plugins.

The set of extension points is fixed. Adding one is a deliberate change to
core, not something a plugin can do, so every dispatch site stays typed and
greppable.

The API is provisional until 1.0: first-party plugins pin the exact core
version, and the specs here may change in step with them.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol

if TYPE_CHECKING:
    from ginkgo.core.directive import ExecutionDirective
    from ginkgo.core.remote import RemoteLocation
    from ginkgo.core.task import TaskDef
    from ginkgo.remote.access.drivers.base import MountDriver
    from ginkgo.remote.backend import ObjectStore
    from ginkgo.runtime.backend import ExecutionEnvironment
    from ginkgo.runtime.environment.secrets import SecretBackend, SecretResolver
    from ginkgo.runtime.events import GinkgoEvent
    from ginkgo.runtime.remote_executor import RemoteExecutor
    from ginkgo.runtime.rundir import RunDir
    from ginkgo.runtime.task_runners.shell import ShellRunner
    from ginkgo.runtime.task_validation import TaskValidator
    from ginkgo.store.protocol import ProvenanceStore

__all__ = [
    "Annotate",
    "CliCommand",
    "Contribution",
    "EnvironmentContext",
    "EnvironmentType",
    "EventSubscriber",
    "EventSubscriberCheckContext",
    "EventSubscriberConfigProblem",
    "EventSubscriberContext",
    "EventSubscriberType",
    "ExecutorType",
    "MountDriverType",
    "Notice",
    "ObjectStoreContext",
    "ObjectStoreType",
    "Plugin",
    "RunnerContext",
    "SecretBackendContext",
    "SecretBackendType",
    "TaskInvocation",
    "TaskKind",
    "TaskRunner",
]


# ----- Executors -------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class ExecutorType:
    """A remote executor backend, selected by ``type`` in an executor table.

    Parameters
    ----------
    name : str
        The ``type`` value in ``[remote.executors.<name>]``.
    label : str
        Human-readable backend name for run headers and error messages.
    required_settings : tuple[str, ...]
        Settings the backend cannot be built without, checked before any task
        runs.
    build : Callable[[Mapping[str, Any]], RemoteExecutor]
        Construct the backend from the executor table's settings.
    """

    point_name: ClassVar[str] = "executor type"

    name: str
    label: str
    required_settings: tuple[str, ...]
    build: Callable[[Mapping[str, Any]], RemoteExecutor]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the executor ``type``."""
        return (self.name,)


# ----- Object stores and FUSE drivers ----------------------------------------


@dataclass(frozen=True, kw_only=True)
class ObjectStoreContext:
    """What an object store is built from.

    Parameters
    ----------
    config : Mapping[str, Any]
        Merged runtime config; each store reads its own table.
    region : str | None
        A region override from the caller, if any.
    """

    config: Mapping[str, Any]
    region: str | None = None


@dataclass(frozen=True, kw_only=True)
class ObjectStoreType:
    """An object store for one URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"s3"``.
    parse_uri : Callable[[str], RemoteLocation]
        Split one of the scheme's URIs into bucket and key, raising
        ``ValueError`` if it is malformed. Stores laid out as
        ``scheme://bucket/key`` pass :func:`ginkgo.core.remote.parse_bucket_key_uri`.
    build : Callable[[ObjectStoreContext], ObjectStore]
        Construct the store.
    """

    point_name: ClassVar[str] = "object store for scheme"

    scheme: str
    parse_uri: Callable[[str], RemoteLocation]
    build: Callable[[ObjectStoreContext], ObjectStore]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the URI scheme."""
        return (self.scheme,)


@dataclass(frozen=True, kw_only=True)
class MountDriverType:
    """A FUSE driver that mounts buckets of one URI scheme.

    Parameters
    ----------
    scheme : str
        URI scheme, e.g. ``"gs"``.
    build : Callable[[], MountDriver]
        Construct the driver.
    """

    point_name: ClassVar[str] = "FUSE driver for scheme"

    scheme: str
    build: Callable[[], MountDriver]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the URI scheme."""
        return (self.scheme,)


# ----- Secrets ---------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class SecretBackendContext:
    """What a secret backend chain is built from.

    Parameters
    ----------
    project_root : Path
        The workflow project's root directory.
    config : Mapping[str, Any]
        Merged runtime config; each backend reads its own table.
    environ : Mapping[str, str]
        The process environment.
    """

    project_root: Path
    config: Mapping[str, Any]
    environ: Mapping[str, str]


@dataclass(frozen=True, kw_only=True)
class SecretBackendType:
    """A secret backend, selected by ``secret(name, backend=...)``.

    Built only when a run first resolves a secret naming it.

    Parameters
    ----------
    name : str
        The backend name a ``SecretRef`` carries.
    build : Callable[[SecretBackendContext], tuple[SecretBackend, ...]]
        Return the chain of backends tried in order for this name.
    """

    point_name: ClassVar[str] = "secret backend"

    name: str
    build: Callable[[SecretBackendContext], tuple[SecretBackend, ...]]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the backend name."""
        return (self.name,)


# ----- Run observers ---------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class EventSubscriberContext:
    """What a run hands an event subscriber when building it.

    Parameters
    ----------
    config : Mapping[str, Any] | None
        Merged runtime config; each subscriber reads its own table.
    secret_resolver : SecretResolver | None
        The run's secret resolver.
    store : ProvenanceStore
        A read connection to the ledger.
    run_id : str
        The run being observed.
    run_dir : Path
        The run's directory.
    workflow_path : Path
        The workflow file being run.
    warn : Callable[[str], None]
        Report a non-fatal problem to the user. The message is also recorded
        with the run as a notice, which ``ginkgo runs show`` and reports
        display, so it may be called from ``close`` after the run has ended.
    """

    config: Mapping[str, Any] | None
    secret_resolver: SecretResolver | None
    store: ProvenanceStore
    run_id: str
    run_dir: Path
    workflow_path: Path
    warn: Callable[[str], None]


class EventSubscriber(Protocol):
    """Receives a run's events once they are committed to the ledger."""

    def handle(self, event: GinkgoEvent) -> None:
        """React to one committed event."""
        ...

    def close(self) -> None:
        """Finish outstanding work at the end of the run."""
        ...


@dataclass(frozen=True, kw_only=True)
class EventSubscriberConfigProblem:
    """One problem an event subscriber's ``check`` finds in its config.

    Parameters
    ----------
    message : str
        What is wrong, and how to fix it.
    blocking : bool
        Whether the problem leaves the subscriber sending nothing. ``ginkgo
        doctor`` reports a blocking problem as an error and any other, such as
        a setting the subscriber ignores while still sending, as a warning.
    """

    message: str
    blocking: bool = True


@dataclass(frozen=True, kw_only=True)
class EventSubscriberCheckContext:
    """What ``ginkgo doctor`` hands an event subscriber's config check.

    Parameters
    ----------
    config : Mapping[str, Any]
        Merged runtime config; each subscriber reads its own table.
    secret_resolver : SecretResolver | None
        The secret resolver a run would use.
    """

    config: Mapping[str, Any]
    secret_resolver: SecretResolver | None


@dataclass(frozen=True, kw_only=True)
class EventSubscriberType:
    """A run observer, such as a notification channel.

    Parameters
    ----------
    name : str
        Identifies the subscriber, e.g. ``"slack"``. Its config is the
        ``[notifications.<name>]`` table.
    build : Callable[[EventSubscriberContext], EventSubscriber | None]
        Return a subscriber for this run, or ``None`` when its config leaves
        it disabled.
    check : Callable[[EventSubscriberCheckContext], Sequence[EventSubscriberConfigProblem]] | None
        Return one problem per thing wrong with the subscriber's config, so
        ``ginkgo doctor`` reports what a run would only warn about. ``None``
        when there is nothing to check.
    """

    point_name: ClassVar[str] = "event subscriber"

    name: str
    build: Callable[[EventSubscriberContext], EventSubscriber | None]
    check: (
        Callable[[EventSubscriberCheckContext], Sequence[EventSubscriberConfigProblem]] | None
    ) = None

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the subscriber name."""
        return (self.name,)


# ----- Execution environments ------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class EnvironmentContext:
    """What a run hands an environment backend when building it.

    Parameters
    ----------
    project_root : Path
        The workflow project's root directory.
    workflow_root : Path | None
        Where named environment manifests are searched, when known.
    config : Mapping[str, Any]
        Merged runtime config; each backend reads its own table.
    probe_registry : bool
        Whether validation may make probes a real run skips: asking the
        container daemon whether it is reachable, and image registries about
        missing images. Only a dry run and ``ginkgo doctor`` set it.
    """

    project_root: Path
    workflow_root: Path | None
    config: Mapping[str, Any]
    probe_registry: bool = False


@dataclass(frozen=True, kw_only=True)
class EnvironmentType:
    """An execution environment for ``env=`` values of some URI schemes.

    Parameters
    ----------
    name : str
        Identifies the backend, e.g. ``"container"``. One instance per name is
        built per run, shared by all of its schemes.
    schemes : tuple[str, ...]
        The schemes of the ``env=`` values it serves, e.g. ``("docker",
        "oci")`` for ``"docker://image:tag"``. The empty string claims bare
        names with no scheme, such as a Pixi environment name.
    label : str
        Short name for the backend in run tables and provenance.
    build : Callable[[EnvironmentContext], ExecutionEnvironment]
        Construct the backend for a run.
    """

    point_name: ClassVar[str] = "environment for scheme"

    name: str
    schemes: tuple[str, ...]
    label: str
    build: Callable[[EnvironmentContext], ExecutionEnvironment]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: every scheme the environment serves."""
        return self.schemes


# ----- Task kinds ------------------------------------------------------------


class TaskInvocation(Protocol):
    """One attempt of a task, as a runner sees it.

    Everything but ``runner_extras`` is read-only. A runner records in
    ``runner_extras`` what it wants back on a cache hit.
    """

    @property
    def node_id(self) -> int:
        """The task's node id in the run's graph."""
        ...

    @property
    def task_def(self) -> TaskDef:
        """The task's definition."""
        ...

    @property
    def attempt(self) -> int:
        """The attempt number, from 1."""
        ...

    @property
    def display_label(self) -> str | None:
        """The task's display label, if it has one."""
        ...

    @property
    def resolved_args(self) -> dict[str, Any] | None:
        """The task's arguments, with secrets still as references."""
        ...

    @property
    def execution_args(self) -> dict[str, Any] | None:
        """The task's arguments, with secrets resolved."""
        ...

    @property
    def stdout_path(self) -> Path | None:
        """Where the task's stdout is logged."""
        ...

    @property
    def stderr_path(self) -> Path | None:
        """Where the task's stderr is logged."""
        ...

    @property
    def threads(self) -> int:
        """The threads the task was granted."""
        ...

    @property
    def secret_values(self) -> tuple[str, ...]:
        """Resolved secret values, to redact from logs and errors."""
        ...

    runner_extras: dict[str, Any] | None


class Annotate(Protocol):
    """Record open-ended facts against a task, as a ``TaskAnnotated`` event."""

    def __call__(self, *, node: TaskInvocation, fields: dict[str, Any]) -> None: ...


class Notice(Protocol):
    """Report a task-scoped problem to the user, as a ``TaskNotice`` event."""

    def __call__(self, *, node: TaskInvocation, message: str) -> None: ...


@dataclass(frozen=True, kw_only=True)
class RunnerContext:
    """The evaluator services a task runner may use.

    Parameters
    ----------
    validator : TaskValidator
        Input and contract validation.
    shell_runner : ShellRunner
        Logged subprocess execution, shared by every runner so an interrupt
        terminates all of their children.
    run_dir : RunDir | None
        The run directory; ``None`` for dry runs and validation.
    annotate : Annotate
        Records facts against a task.
    notice : Notice
        Shows the user a task-scoped warning that does not fail the task.
    run_id : Callable[[], str]
        Returns the current run id.
    db_path : Path
        The provenance database.
    """

    validator: TaskValidator
    shell_runner: ShellRunner
    run_dir: RunDir | None
    annotate: Annotate
    notice: Notice
    run_id: Callable[[], str]
    db_path: Path


class TaskRunner(Protocol):
    """Executes the directive a task body returned.

    Anything a runner wants back on a cache hit, it puts in
    ``node.runner_extras`` while running: the evaluator saves that with the
    cache entry and records it against the task again when a later run hits
    the cache, so a runner never handles cache hits itself.
    """

    def run(self, *, node: TaskInvocation, directive: Any) -> Any:
        """Execute *directive* for *node* and return the task's result."""
        ...


@dataclass(frozen=True, kw_only=True)
class TaskKind:
    """A task kind, selected by ``@task(kind=...)``.

    Parameters
    ----------
    name : str
        The ``kind`` value.
    directive : type[ExecutionDirective] | None
        The directive the task body returns, which the runner executes. A task
        of this kind must return this directive and no other, and no two kinds
        may share one. ``None`` for a kind whose body's return value is the
        result.
    call_form : str
        How the body builds its directive, for messages, e.g. ``"shell(...)"``.
        Empty exactly when ``directive`` is ``None``.
    execution_mode : {"driver", "worker"}
        Whether the body runs on the scheduler or in a worker process.
    allows_env : bool
        Whether the kind may declare ``env=``.
    remote_capable : bool
        Whether the kind can be dispatched to a remote executor.
    eager_source_hash : bool
        Whether the body is evaluated while the task is prepared, so the
        directive's ``source_hash`` (a notebook or script file) joins the cache
        key.
    exit_code_retries : bool
        Whether ``retry_on_exit_codes`` applies.
    user_built_command : bool
        Whether the body formats its own command line (``shell(cmd=...)``),
        which ginkgo cannot quote. The kind's ``file``, ``folder`` and
        ``Out[...]`` paths must then not contain spaces.
    runner : Callable[[RunnerContext], TaskRunner] | None
        Build the runner for a run. ``None`` exactly when ``directive`` is.
    static_notices : Callable[[ExecutionDirective, str, Sequence[str]], Sequence[str]] | None
        Check a directive before anything runs, given the task's name and the
        names of the arguments it passes, and return warnings about what the
        run would get wrong. ``doctor`` and ``--dry-run`` call it for each
        task whose directive they can work out without running the body.

    Raises
    ------
    ValueError
        If ``directive``, ``runner`` and ``call_form`` disagree about whether
        the kind returns a directive.
    """

    point_name: ClassVar[str] = "task kind"

    name: str
    directive: type[ExecutionDirective] | None
    call_form: str
    execution_mode: Literal["driver", "worker"]
    allows_env: bool
    remote_capable: bool
    eager_source_hash: bool = False
    exit_code_retries: bool = False
    user_built_command: bool = False
    runner: Callable[[RunnerContext], TaskRunner] | None = None
    static_notices: Callable[[ExecutionDirective, str, Sequence[str]], Sequence[str]] | None = None

    def __post_init__(self) -> None:
        returns_directive = self.directive is not None
        if (self.runner is not None) != returns_directive or bool(
            self.call_form
        ) != returns_directive:
            raise ValueError(
                f"Task kind {self.name!r} must set directive, runner and call_form "
                "together, or none of them."
            )

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the kind name."""
        return (self.name,)


# ----- CLI commands ----------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class CliCommand:
    """A ``ginkgo <name>`` subcommand.

    Core's own commands are wired directly in the CLI; a plugin command is
    added after them, and one that reuses a core command's name is ignored with
    a warning.

    The CLI builds its parser, and so imports every plugin module, on every
    invocation: a plugin module should import cheaply. ``run`` executes with
    the working directory at the project root; the CLI resolves only the
    ``workflow``, ``out`` and ``config`` arguments against the directory the
    user ran it from, so any other path argument is relative to the root.

    Parameters
    ----------
    name : str
        The subcommand, e.g. ``"report"``.
    help : str
        One line for ``ginkgo --help``.
    add_arguments : Callable[[argparse.ArgumentParser], None]
        Declare the command's arguments on its subparser.
    run : Callable[[argparse.Namespace], int]
        Execute the command and return its exit status.
    """

    point_name: ClassVar[str] = "CLI command"

    name: str
    help: str
    add_arguments: Callable[[argparse.ArgumentParser], None]
    run: Callable[[argparse.Namespace], int]

    @property
    def keys(self) -> tuple[str, ...]:
        """The lookup keys: the subcommand name."""
        return (self.name,)


Contribution = (
    ExecutorType
    | ObjectStoreType
    | MountDriverType
    | SecretBackendType
    | EventSubscriberType
    | EnvironmentType
    | TaskKind
    | CliCommand
)
"""Every extension point a plugin can contribute to."""


@dataclass(frozen=True, kw_only=True)
class Plugin:
    """A named bundle of contributions, exposed through an entry point.

    Parameters
    ----------
    name : str
        Distribution-style name, e.g. ``"ginkgo-k8s"``.
    contributions : tuple[Contribution, ...]
        What the plugin adds.
    """

    name: str
    contributions: tuple[Contribution, ...]
