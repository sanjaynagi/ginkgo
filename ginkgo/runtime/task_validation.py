"""Task contract validation and return-value coercion.

This module isolates the rules that decide whether a task definition,
its resolved inputs, and its return value are well-formed. The logic was
previously inlined in ``_ConcurrentEvaluator`` — extracting it lets the
scheduler stay focused on graph and lifecycle management while keeping
validation independently testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, get_args, get_origin

from ginkgo.core.asset import AssetRef
from ginkgo.core.expr import Expr, ExprList, OutputIndex
from ginkgo.core.remote import RemoteRef, is_remote_uri
from ginkgo.core.secret import SecretRef
from ginkgo.core.shell import ShellExpr
from ginkgo.core.task import TaskDef
from ginkgo.core.types import file, folder, tmp_dir
from ginkgo.runtime.backend import TaskBackend
from ginkgo.runtime.environment.secrets import SecretResolver, collect_secret_refs
from ginkgo.runtime.artifacts.value_codec import CodecError, ensure_serializable


def is_path_annotation(annotation: Any) -> bool:
    """Return whether an annotation is a pathlib path type."""
    return isinstance(annotation, type) and issubclass(annotation, Path)


def is_remote_path_value(value: Any) -> bool:
    """Return whether a value is a remote reference or supported remote URI."""
    if isinstance(value, RemoteRef):
        return True
    return isinstance(value, str) and is_remote_uri(value)


def contains_dynamic_expression(value: Any) -> bool:
    """Return whether a nested value contains unresolved expressions."""
    if isinstance(value, (Expr, ExprList, OutputIndex)):
        return True
    if isinstance(value, list | tuple):
        return any(contains_dynamic_expression(item) for item in value)
    if isinstance(value, dict):
        return any(
            contains_dynamic_expression(key) or contains_dynamic_expression(item)
            for key, item in value.items()
        )
    return False


@dataclass(kw_only=True)
class TaskValidator:
    """Validate task definitions, inputs, and return values.

    Parameters
    ----------
    backend : TaskBackend | None
        Execution backend used to validate declared task environments.
    secret_resolver : SecretResolver | None
        Resolver used to verify declared secret references resolve.
    """

    backend: TaskBackend | None = None
    secret_resolver: SecretResolver | None = None

    # Static graph validation -------------------------------------------------

    def validate_declared_envs(self, *, nodes: Iterable[Any]) -> None:
        """Raise before any work starts if a declared env cannot be resolved.

        Foreign execution environments only support shell-like tasks.
        """
        node_list = list(nodes)
        for node in node_list:
            if node.task_def.env is not None and node.task_def.kind not in {
                "notebook",
                "script",
                "shell",
            }:
                raise TypeError(
                    f"{node.task_def.name} uses env {node.task_def.env!r} "
                    "but is declared with kind='python'. Foreign environments "
                    "only support driver tasks — use @task('shell'), "
                    "@task('notebook'), or @task('script')."
                )

        if self.backend is None:
            return

        env_names: set[str] = {
            node.task_def.env for node in node_list if node.task_def.env is not None
        }
        if env_names:
            self.backend.validate_envs(env_names=env_names)

    def validate_declared_secrets(self, *, nodes: Iterable[Any]) -> None:
        """Raise if any statically declared secrets are missing."""
        if self.secret_resolver is None:
            return

        missing: list[SecretRef] = []
        seen: set[SecretRef] = set()
        for node in nodes:
            for ref in collect_secret_refs(node.expr.args):
                if ref in seen:
                    continue
                seen.add(ref)
                try:
                    self.secret_resolver.resolve(ref=ref)
                except BaseException:
                    missing.append(ref)

        if missing:
            rendered = ", ".join(f"{ref.backend}:{ref.name}" for ref in sorted(missing, key=str))
            raise RuntimeError(f"Missing secrets: {rendered}")

    # Per-node contract validation -------------------------------------------

    def validate_task_contract(
        self,
        *,
        task_def: TaskDef,
        execution_args: dict[str, Any],
    ) -> None:
        """Validate that a task can run safely under its declared contract."""
        self.validate_task_preconditions(task_def=task_def, resolved_args=execution_args)

    def validate_task_preconditions(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> None:
        """Validate top-level importability and serializable input types."""
        self.validate_task_importable(task_def=task_def)
        for name, value in resolved_args.items():
            if task_def.type_hints.get(name) is tmp_dir:
                continue
            self.validate_process_safe_value(
                value=value,
                label=f"{task_def.name}.{name}",
            )

    def validate_static_inputs(self, *, node: Any) -> None:
        """Validate literal-only task inputs during dry-run mode."""
        for name, parameter in node.task_def.signature.parameters.items():
            annotation = node.task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir or name not in node.expr.args:
                continue
            value = node.expr.args[name]
            if contains_dynamic_expression(value):
                continue
            if collect_secret_refs(value):
                continue
            self.validate_annotated_value(
                annotation=annotation,
                value=value,
                label=f"{node.task_def.name}.{name}",
            )

    def validate_task_importable(self, *, task_def: TaskDef) -> None:
        """Require Python tasks to be plain top-level importable functions."""
        # Imported lazily to avoid a hard cycle with the module loader.
        from ginkgo.runtime.module_loader import load_module, resolve_module_file

        fn = task_def.fn
        if fn.__qualname__ != fn.__name__:
            raise TypeError(
                f"{task_def.name} is not a top-level function. "
                "Define tasks at module scope for process execution."
            )

        if fn.__closure__:
            raise TypeError(
                f"{task_def.name} closes over local state. "
                "Pass required values as task arguments instead."
            )

        module = load_module(fn.__module__, module_file=resolve_module_file(fn.__module__))
        imported = getattr(module, fn.__name__, None)
        if imported is not fn and getattr(imported, "fn", None) is not fn:
            raise TypeError(
                f"{task_def.name} is not importable by module path. "
                "Define tasks as plain module-level functions."
            )

    def validate_process_safe_value(self, *, value: Any, label: str) -> None:
        """Reject values that are not supported across process and cache boundaries."""
        if isinstance(value, (Expr, ExprList, ShellExpr, SecretRef)):
            return
        if collect_secret_refs(value):
            return
        try:
            ensure_serializable(value, label=label)
        except CodecError as exc:
            raise TypeError(str(exc)) from exc

    def validate_inputs(
        self,
        *,
        task_def: TaskDef,
        resolved_args: dict[str, Any],
    ) -> None:
        """Validate resolved task inputs against Ginkgo path types."""
        for name, parameter in task_def.signature.parameters.items():
            annotation = task_def.type_hints.get(name, parameter.annotation)
            if annotation is tmp_dir or name not in resolved_args:
                continue
            self.validate_annotated_value(
                annotation=annotation,
                value=resolved_args[name],
                label=f"{task_def.name}.{name}",
            )

    def validate_return_value(self, *, task_def: TaskDef, value: Any) -> None:
        """Validate a task return value when it uses a Ginkgo path type."""
        annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        self.validate_annotated_value(
            annotation=annotation,
            value=value,
            label=f"{task_def.name}.return",
        )

    def validate_annotated_value(
        self,
        *,
        annotation: Any,
        value: Any,
        label: str,
    ) -> None:
        """Validate a value for direct and container-wrapped Ginkgo types."""
        if annotation in {None, Any}:
            return

        origin = get_origin(annotation)
        if origin in {list, tuple}:
            inner_annotations = get_args(annotation)
            inner_annotation = inner_annotations[0] if inner_annotations else Any
            for index, item in enumerate(value):
                self.validate_annotated_value(
                    annotation=inner_annotation,
                    value=item,
                    label=f"{label}[{index}]",
                )
            return

        if isinstance(value, list | tuple):
            for index, item in enumerate(value):
                self.validate_annotated_value(
                    annotation=annotation,
                    value=item,
                    label=f"{label}[{index}]",
                )
            return

        if annotation is file:
            if isinstance(value, AssetRef) and value.kind == "file":
                return
            if is_remote_path_value(value):
                return
            self._validate_file_path(path=value, label=label)
            return

        if annotation is folder:
            if isinstance(value, AssetRef) and value.kind == "folder":
                return
            if is_remote_path_value(value):
                return
            self._validate_folder_path(path=value, label=label)
            return

        if annotation is tmp_dir:
            self._validate_tmp_dir_path(path=value, label=label)
            return

        if is_path_annotation(annotation):
            if not Path(value).exists():
                raise FileNotFoundError(f"{label} must exist: {str(value)!r}")

    def _validate_file_path(self, *, path: Any, label: str) -> None:
        """Validate a concrete file path argument or return value."""
        path_str = str(path)
        if " " in path_str:
            raise ValueError(f"{label} must not contain spaces: {path_str!r}")

        if not Path(path_str).is_file():
            raise FileNotFoundError(f"{label} must exist and be a file: {path_str!r}")

    def _validate_folder_path(self, *, path: Any, label: str) -> None:
        """Validate a concrete folder path argument or return value."""
        path_str = str(path)
        if " " in path_str:
            raise ValueError(f"{label} must not contain spaces: {path_str!r}")

        path_obj = Path(path_str)
        if not path_obj.exists() or not path_obj.is_dir():
            raise FileNotFoundError(f"{label} must exist and be a directory: {path_str!r}")

    def _validate_tmp_dir_path(self, *, path: Any, label: str) -> None:
        """Validate an auto-created scratch directory."""
        path_obj = Path(str(path))
        if not path_obj.exists() or not path_obj.is_dir():
            raise FileNotFoundError(f"{label} tmp_dir does not exist: {str(path)!r}")

    # Coercion ----------------------------------------------------------------

    def coerce_return_value(self, *, task_def: TaskDef, value: Any) -> Any:
        """Coerce string returns into the declared Ginkgo path marker type."""
        annotation = task_def.type_hints.get("return", task_def.signature.return_annotation)
        return self.coerce_annotated_value(annotation=annotation, value=value)

    def coerce_annotated_value(self, *, annotation: Any, value: Any) -> Any:
        """Coerce values recursively for direct and container-wrapped path types."""
        if annotation in {None, Any}:
            return value

        origin = get_origin(annotation)
        if origin is list and isinstance(value, list):
            inner_annotations = get_args(annotation)
            inner_annotation = inner_annotations[0] if inner_annotations else Any
            return [
                self.coerce_annotated_value(annotation=inner_annotation, value=item)
                for item in value
            ]

        if origin is tuple and isinstance(value, tuple):
            inner_annotations = get_args(annotation)
            if len(inner_annotations) == 2 and inner_annotations[1] is Ellipsis:
                inner_annotation = inner_annotations[0]
                return tuple(
                    self.coerce_annotated_value(annotation=inner_annotation, value=item)
                    for item in value
                )

            if inner_annotations and len(inner_annotations) == len(value):
                return tuple(
                    self.coerce_annotated_value(annotation=item_annotation, value=item)
                    for item_annotation, item in zip(inner_annotations, value, strict=True)
                )

            inner_annotation = inner_annotations[0] if inner_annotations else Any
            return tuple(
                self.coerce_annotated_value(annotation=inner_annotation, value=item)
                for item in value
            )

        if isinstance(value, list):
            return [
                self.coerce_annotated_value(annotation=annotation, value=item) for item in value
            ]

        if isinstance(value, tuple):
            return tuple(
                self.coerce_annotated_value(annotation=annotation, value=item) for item in value
            )

        if annotation in {file, folder, tmp_dir} and isinstance(value, str):
            return annotation(value)

        if is_path_annotation(annotation) and isinstance(value, str):
            return Path(value)

        return value
