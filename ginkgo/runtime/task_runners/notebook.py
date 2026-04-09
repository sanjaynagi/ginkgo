"""Notebook and script task execution.

The ``NotebookRunner`` runs both ``NotebookExpr`` and ``ScriptExpr`` driver
tasks. It owns the per-run notebook artifact layout, the managed-kernel
preparation flow, the command builders, and the manifest extras that surface
notebook outputs in the run summary.
"""

from __future__ import annotations

import inspect
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Any, Callable

import yaml

from ginkgo.core.notebook import NotebookExpr
from ginkgo.core.script import ScriptExpr
from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.backend import TaskBackend
from ginkgo.runtime.caching.cache import CacheStore
from ginkgo.runtime.notebook_kernels import (
    ExecutionCommand,
    NotebookCommandBuilder,
    NotebookKernelManager,
    build_jupyter_env_prefix,
)
from ginkgo.runtime.environment.secrets import redact_text
from ginkgo.runtime.task_runners.shell import (
    ShellRunner,
    ShellTaskError,
    iter_output_values,
    remove_declared_output,
)
from ginkgo.runtime.task_validation import TaskValidator


# ----- Exceptions -----------------------------------------------------------


class NotebookTaskError(RuntimeError):
    """Notebook task execution failure."""

    def __init__(
        self,
        *,
        task_name: str,
        phase: str,
        cmd: str,
        exit_code: int,
        output: str,
    ) -> None:
        self.exit_code = exit_code
        details = (
            f"Notebook task {task_name} failed during {phase} with exit code {exit_code}: {cmd}"
        )
        if output:
            details = f"{details}\n{output.strip()}"
        super().__init__(details)


# ----- Helpers --------------------------------------------------------------


def serialize_notebook_value(value: Any) -> Any:
    """Convert runtime values into YAML/CLI-safe notebook parameters."""
    if isinstance(value, Path | file | folder | tmp_dir):
        return str(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list):
        return [serialize_notebook_value(item) for item in value]
    if isinstance(value, tuple):
        return [serialize_notebook_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(serialize_notebook_value(key)): serialize_notebook_value(item)
            for key, item in value.items()
        }
    return value


def stringify_notebook_argument(value: Any) -> str:
    """Render one notebook argument for a CLI invocation."""
    serialized = serialize_notebook_value(value)
    if isinstance(serialized, str):
        return serialized
    return json.dumps(serialized, sort_keys=True)


def relativize_to_run_dir(*, run_dir: Path, path: Path) -> str:
    """Return a run-relative path when possible."""
    try:
        return str(path.relative_to(run_dir))
    except ValueError:
        return str(path)


def escape_html(value: str) -> str:
    """Escape plain text for a tiny fallback HTML page."""
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def first_label_param_name(*, task_def: TaskDef) -> str | None:
    """Return the first user-declared parameter name for CLI labeling."""
    for name, parameter in task_def.signature.parameters.items():
        annotation = task_def.type_hints.get(name, parameter.annotation)
        if annotation is tmp_dir:
            continue
        return name
    return None


def render_label_value(value: Any) -> str | None:
    """Return a compact string for a mapped-task display label."""
    if isinstance(value, Path):
        text = value.name or str(value)
    elif isinstance(value, (str, int, float, bool)):
        text = str(value)
    else:
        text = repr(value)

    compact = " ".join(text.split()).strip()
    if not compact:
        return None
    if len(compact) > 24:
        compact = f"{compact[:21]}..."
    return compact


def fn_description(fn: Any) -> str | None:
    """Return the function docstring (used as the notebook description)."""
    return inspect.getdoc(fn)


# ----- Internal types -------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class NotebookArtifacts:
    """Run-scoped artifact locations for one notebook task."""

    root_dir: Path
    html_path: Path
    executed_path: Path | None
    params_path: Path


@dataclass(kw_only=True, frozen=True)
class _NotebookCommandBuilder(NotebookCommandBuilder):
    """Build Python subprocess invocations for notebook helper commands."""

    backend: TaskBackend | None

    def command_for_python(self, *, env: str | None, args: list[str]) -> ExecutionCommand:
        """Return a subprocess invocation for one Python command."""
        # Use the current interpreter directly when no task env is declared.
        if env is None:
            argv = [sys.executable, *args]
            return ExecutionCommand(
                argv=argv,
                use_shell=False,
                display=" ".join(shlex.quote(part) for part in argv),
            )

        if self.backend is None:
            raise RuntimeError(
                f"Notebook task environment {env!r} requires a backend, but none is configured."
            )

        # Reuse the backend shell wrapper so env-backed notebook helpers run inside Pixi.
        cmd = " ".join(shlex.quote(part) for part in ["python", *args])
        return ExecutionCommand(
            argv=self.backend.exec_argv(env=env, cmd=cmd),
            use_shell=False,
            display=cmd,
        )


# ----- Notebook runner ------------------------------------------------------


@dataclass(kw_only=True)
class NotebookRunner:
    """Execute notebook and script driver tasks.

    Parameters
    ----------
    backend : TaskBackend | None
        Execution backend for environment-isolated notebook helpers.
    shell_runner : ShellRunner
        Provides ``run_logged_command`` and the underlying subprocess
        primitives.
    validator : TaskValidator
        Used to coerce return values for notebook and script outputs.
    cache_store : CacheStore
        Cache backing store; consulted to replay notebook manifest extras
        on a cache hit.
    provenance : Any | None
        Run provenance recorder. May be ``None`` in dry-run / validation
        contexts; manifest writes are then no-ops.
    notice_emitter : Callable
        Callback used to surface kernel-installation notices.
    runtime_root_factory : Callable[[], Path]
        Lazily resolves the on-disk runtime root for shared notebook files.
    """

    backend: TaskBackend | None
    shell_runner: ShellRunner
    validator: TaskValidator
    cache_store: CacheStore
    provenance: Any | None
    notice_emitter: Callable[[Any, str], None]
    runtime_root_factory: Callable[[], Path]
    _kernel_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    # Public driver entry points ---------------------------------------------

    def run_notebook(self, *, node: Any, notebook_expr: NotebookExpr) -> Any:
        """Execute a notebook task from a ``NotebookExpr`` sentinel.

        Determines the notebook backend from the file extension, runs
        execution, renders HTML, validates any declared outputs, and
        returns the appropriate result value.
        """
        assert node.execution_args is not None
        notebook_path = notebook_expr.path
        notebook_kind = "ipynb" if notebook_path.suffix.lower() == ".ipynb" else "marimo"
        user_log_path = Path(notebook_expr.log) if notebook_expr.log is not None else None
        description = fn_description(node.task_def.fn)

        artifacts = self._notebook_artifacts(node=node, notebook_kind=notebook_kind)
        self._prepare_notebook_artifacts(artifacts=artifacts)
        if notebook_expr.outputs is not None:
            for output_path in iter_output_values(notebook_expr.outputs):
                remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
        kernel_spec = (
            self._managed_notebook_kernel(node=node) if notebook_kind == "ipynb" else None
        )
        self._record_notebook_manifest(
            node=node,
            notebook_kind=notebook_kind,
            notebook_path=notebook_path,
            notebook_description=description,
            executed_path=artifacts.executed_path,
            rendered_html=artifacts.html_path,
            render_status="pending",
            render_error=None,
            managed_kernel_name=kernel_spec.name if kernel_spec is not None else None,
        )

        # Build and run the execution command.
        if notebook_kind == "ipynb":
            command = self._build_ipynb_execute_command(
                notebook_path=notebook_path,
                executed_path=artifacts.executed_path,
                params_path=artifacts.params_path,
                resolved_args=node.execution_args,
                kernel_name=kernel_spec.name if kernel_spec is not None else "",
                jupyter_path=kernel_spec.jupyter_path if kernel_spec is not None else Path(),
            )
            executed_artifact = artifacts.executed_path
        else:
            command = self._build_marimo_execute_command(
                notebook_path=notebook_path,
                resolved_args=node.execution_args,
            )
            executed_artifact = None

        completed = self.shell_runner.run_logged_command(
            node=node, cmd=command, user_log_path=user_log_path
        )
        if completed.returncode != 0:
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="not_started",
                render_error=None,
                managed_kernel_name=kernel_spec.name if kernel_spec is not None else None,
            )
            raise NotebookTaskError(
                task_name=node.task_def.name,
                phase="execute",
                cmd=command,
                exit_code=completed.returncode,
                output=(completed.stdout or "") + (completed.stderr or ""),
            )

        # Render notebook to HTML.
        render_command = self._build_notebook_render_command(
            notebook_path=notebook_path,
            notebook_kind=notebook_kind,
            executed_path=artifacts.executed_path,
            html_path=artifacts.html_path,
        )
        render_result = self.shell_runner.run_logged_command(node=node, cmd=render_command)
        if render_result.returncode != 0 or not artifacts.html_path.is_file():
            render_error = self._render_notebook_failure_page(
                html_path=artifacts.html_path,
                task_name=node.task_def.name,
                error_output=(render_result.stdout or "") + (render_result.stderr or ""),
            )
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="failed",
                render_error=render_error,
                managed_kernel_name=kernel_spec.name if kernel_spec is not None else None,
            )
        else:
            self._record_notebook_manifest(
                node=node,
                notebook_kind=notebook_kind,
                notebook_path=notebook_path,
                notebook_description=description,
                executed_path=executed_artifact,
                rendered_html=artifacts.html_path,
                render_status="succeeded",
                render_error=None,
                managed_kernel_name=kernel_spec.name if kernel_spec is not None else None,
            )

        # Validate and return declared outputs, or fall back to HTML artifact.
        if notebook_expr.outputs is None:
            return self.validator.coerce_return_value(
                task_def=node.task_def, value=str(artifacts.html_path)
            )
        return self._validate_and_return_outputs(
            task_name=node.task_def.name,
            task_def=node.task_def,
            outputs=notebook_expr.outputs,
        )

    def run_script(self, *, node: Any, script_expr: ScriptExpr) -> Any:
        """Execute a script task, forwarding task inputs as CLI arguments."""
        assert node.execution_args is not None
        user_log_path = Path(script_expr.log) if script_expr.log is not None else None
        if script_expr.outputs is not None:
            for output_path in iter_output_values(script_expr.outputs):
                remove_declared_output(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)

        # Resolve the interpreter: use sys.executable for Python to stay in the same env.
        interpreter_cmd = (
            shlex.quote(sys.executable)
            if script_expr.interpreter == "python"
            else shlex.quote(script_expr.interpreter)
        )

        # Build command: interpreter script_path --arg-name value ...
        cmd_parts = [interpreter_cmd, shlex.quote(str(script_expr.path))]
        for name, value in node.execution_args.items():
            option = f"--{name.replace('_', '-')}"
            cmd_parts.extend(
                [shlex.quote(option), shlex.quote(stringify_notebook_argument(value))]
            )
        cmd = " ".join(cmd_parts)

        completed = self.shell_runner.run_logged_command(
            node=node, cmd=cmd, user_log_path=user_log_path
        )
        combined_output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode != 0:
            raise ShellTaskError(
                task_name=node.task_def.name,
                cmd=redact_text(text=cmd, secret_values=node.secret_values),
                exit_code=completed.returncode,
                output=combined_output,
                log=script_expr.log,
            )

        if script_expr.outputs is None:
            return None
        return self._validate_and_return_outputs(
            task_name=node.task_def.name,
            task_def=node.task_def,
            outputs=script_expr.outputs,
        )

    # Cache replay -----------------------------------------------------------

    def replay_cached_extras(self, *, node: Any, cache_key: str) -> None:
        """Replay notebook manifest extras stored on a previous cache save.

        Notebook tasks render an HTML artifact whose path is not part of the
        cached return value. To keep cache hits visible in the run summary
        and downstream tooling, we persist the notebook manifest extras
        alongside the cache entry on save and re-apply them here on hit.
        """
        if self.provenance is None or node.task_def.kind != "notebook":
            return
        cached_extra = self.cache_store.load_extra_meta(cache_key=cache_key)
        if cached_extra is None:
            return
        notebook_extras = cached_extra.get("notebook_extras")
        if not isinstance(notebook_extras, dict):
            return
        self.provenance.update_task_extra(node_id=node.node_id, **notebook_extras)

    # Private helpers --------------------------------------------------------

    def _validate_and_return_outputs(
        self,
        *,
        task_name: str,
        task_def: TaskDef,
        outputs: Any,
    ) -> Any:
        """Validate declared output paths exist and return coerced value."""
        output_paths = iter_output_values(outputs)
        missing = [str(path) for path in output_paths if not path.exists()]
        if missing:
            label = missing[0] if len(missing) == 1 else missing
            raise FileNotFoundError(
                f"Task {task_name} completed but did not create declared output {label!r}"
            )
        return self.validator.coerce_return_value(task_def=task_def, value=outputs)

    def _notebook_artifacts(self, *, node: Any, notebook_kind: str) -> NotebookArtifacts:
        """Return deterministic artifact paths for one notebook task."""
        task_key = f"task_{node.node_id:04d}"
        root_dir = (
            self.provenance.run_dir / "notebooks"
            if self.provenance is not None
            else Path.cwd() / ".ginkgo" / "notebooks"
        )
        root_dir.mkdir(parents=True, exist_ok=True)
        executed_path = root_dir / f"{task_key}.ipynb" if notebook_kind == "ipynb" else None
        return NotebookArtifacts(
            root_dir=root_dir,
            html_path=root_dir / f"{task_key}.html",
            executed_path=executed_path,
            params_path=root_dir / f"{task_key}.params.yaml",
        )

    def _prepare_notebook_artifacts(self, *, artifacts: NotebookArtifacts) -> None:
        """Clear stale notebook artifacts before a fresh execution attempt."""
        artifacts.root_dir.mkdir(parents=True, exist_ok=True)
        for path in (artifacts.html_path, artifacts.executed_path, artifacts.params_path):
            if path is None:
                continue
            if path.exists():
                path.unlink()

    def _managed_notebook_kernel(self, *, node: Any) -> Any:
        """Prepare and return the managed kernelspec for one notebook task."""
        env = node.task_def.env
        env_identity = None
        if env is not None and self.backend is not None:
            env_identity = self.backend.env_identity(env=env)

        manager = NotebookKernelManager(
            runtime_root=self.runtime_root_factory(),
            command_builder=_NotebookCommandBuilder(backend=self.backend),
        )
        # Serialize cold-start kernel preparation so concurrent notebook tasks in the
        # same env do not race to install the same managed kernelspec twice.
        with self._kernel_lock:
            return manager.ensure_kernel(
                env=env,
                env_identity=env_identity,
                run_command=self._run_execution_command,
                on_installing=lambda spec: self.notice_emitter(
                    node, f"📦 Installing ipykernel for {spec.env_label}..."
                ),
            )

    def _run_execution_command(
        self,
        command: ExecutionCommand,
    ) -> subprocess.CompletedProcess[str]:
        """Run one notebook helper command without attaching task logs."""
        completed, _ = self.shell_runner._call_run_subprocess(
            argv=command.argv,
            use_shell=command.use_shell,
            on_stdout=lambda _chunk: None,
            on_stderr=lambda _chunk: None,
        )
        return completed

    def _build_ipynb_execute_command(
        self,
        *,
        notebook_path: Path,
        executed_path: Path | None,
        params_path: Path,
        resolved_args: dict[str, Any],
        kernel_name: str,
        jupyter_path: Path,
    ) -> str:
        """Build the Papermill execution command for one Jupyter notebook."""
        if executed_path is None:
            raise RuntimeError("ipynb notebooks require an executed output path")
        params_path.write_text(
            yaml.safe_dump(
                serialize_notebook_value(resolved_args),
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return " ".join(
            [
                build_jupyter_env_prefix(jupyter_path=jupyter_path),
                shlex.quote(sys.executable),
                "-m",
                "papermill",
                shlex.quote(str(notebook_path)),
                shlex.quote(str(executed_path)),
                "-f",
                shlex.quote(str(params_path)),
                "-k",
                shlex.quote(kernel_name),
            ]
        )

    def _build_marimo_execute_command(
        self,
        *,
        notebook_path: Path,
        resolved_args: dict[str, Any],
    ) -> str:
        """Build the command used to execute one marimo notebook script."""
        args: list[str] = [shlex.quote(sys.executable), shlex.quote(str(notebook_path))]
        for name, value in resolved_args.items():
            option = f"--{name.replace('_', '-')}"
            args.extend([shlex.quote(option), shlex.quote(stringify_notebook_argument(value))])
        return " ".join(args)

    def _build_notebook_render_command(
        self,
        *,
        notebook_path: Path,
        notebook_kind: str,
        executed_path: Path | None,
        html_path: Path,
    ) -> str:
        """Build the HTML render command for one notebook task."""
        if notebook_kind == "ipynb":
            if executed_path is None:
                raise RuntimeError("ipynb notebooks require an executed output path")
            return " ".join(
                [
                    shlex.quote(sys.executable),
                    "-m",
                    "jupyter",
                    "nbconvert",
                    "--to",
                    "html",
                    "--output",
                    shlex.quote(html_path.stem),
                    "--output-dir",
                    shlex.quote(str(html_path.parent)),
                    shlex.quote(str(executed_path)),
                ]
            )

        return " ".join(
            [
                shlex.quote(sys.executable),
                "-m",
                "marimo",
                "export",
                "html",
                shlex.quote(str(notebook_path)),
                "-o",
                shlex.quote(str(html_path)),
            ]
        )

    def _record_notebook_manifest(
        self,
        *,
        node: Any,
        notebook_kind: str,
        notebook_path: Path,
        notebook_description: str | None,
        executed_path: Path | None,
        rendered_html: Path,
        render_status: str,
        render_error: str | None,
        managed_kernel_name: str | None = None,
    ) -> None:
        """Persist notebook-specific metadata to the task manifest."""
        if self.provenance is None:
            return
        extra: dict[str, Any] = {
            "task_type": "notebook",
            "notebook_kind": notebook_kind,
            "notebook_path": str(notebook_path),
            "notebook_description": notebook_description,
            "render_status": render_status,
            "rendered_html": relativize_to_run_dir(
                run_dir=self.provenance.run_dir,
                path=rendered_html,
            ),
        }
        if executed_path is not None:
            extra["executed_notebook"] = relativize_to_run_dir(
                run_dir=self.provenance.run_dir,
                path=executed_path,
            )
        if render_error is not None:
            extra["render_error"] = render_error
        elif render_status != "failed":
            extra["render_error"] = None
        if managed_kernel_name is not None:
            extra["managed_kernel_name"] = managed_kernel_name
        self.provenance.update_task_extra(node_id=node.node_id, **extra)

        # Stash an absolute-path version of the extras on the node so that
        # _complete_node can persist them in the cache entry. Replaying these
        # on a future cache hit lets us populate the new run's manifest with
        # the rendered HTML pointer even when the task body is skipped.
        cache_extras = dict(extra)
        cache_extras["rendered_html"] = str(rendered_html)
        if executed_path is not None:
            cache_extras["executed_notebook"] = str(executed_path)
        node.notebook_extras = cache_extras

    def _render_notebook_failure_page(
        self,
        *,
        html_path: Path,
        task_name: str,
        error_output: str,
    ) -> str:
        """Write a fallback HTML page when notebook rendering fails."""
        html_path.parent.mkdir(parents=True, exist_ok=True)
        message = error_output.strip() or "Notebook HTML export failed."
        html_path.write_text(
            "\n".join(
                [
                    "<html><body>",
                    f"<h1>{task_name}</h1>",
                    "<p>Notebook execution succeeded, but HTML export failed.</p>",
                    "<pre>",
                    escape_html(message),
                    "</pre>",
                    "</body></html>",
                ]
            ),
            encoding="utf-8",
        )
        return message
