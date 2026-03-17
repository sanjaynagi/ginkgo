"""The ``@task`` decorator and supporting classes.

A ``@task()``-decorated function does not execute when called.  Instead it
returns an ``Expr[T]`` (full call) or a ``PartialCall`` (subset of required
arguments), enabling lazy expression tree construction.
"""

from __future__ import annotations

import inspect
from importlib import import_module
from dataclasses import dataclass, field
from typing import Any, Callable, get_type_hints

from ginkgo.core.expr import Expr, ExprList
from ginkgo.core.types import tmp_dir

_TASK_KINDS = frozenset({"python", "shell"})


@dataclass(frozen=True)
class TaskDef:
    """Wraps a user function so that calls produce expression nodes.

    Parameters
    ----------
    fn : Callable
        The original user function.
    env : str | None
        Pixi environment name (or Docker URI in future).
    version : int
        Cache-busting version tag.
    retries : int
        Additional retry attempts after the initial execution.
    kind : str
        Execution contract for the task body.
    """

    fn: Callable[..., Any]
    env: str | None = None
    version: int = 1
    retries: int = 0
    kind: str = "python"
    _signature: inspect.Signature = field(init=False, repr=False)
    _type_hints: dict[str, Any] = field(init=False, repr=False)
    _required_params: frozenset[str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ValueError("retries must be at least 0")
        if self.kind not in _TASK_KINDS:
            supported = ", ".join(sorted(_TASK_KINDS))
            raise ValueError(f"kind must be one of {{{supported}}}, got {self.kind!r}")

        sig = inspect.signature(self.fn)
        hints = get_type_hints(self.fn)
        required = frozenset(
            name
            for name, param in sig.parameters.items()
            if (param.default is inspect.Parameter.empty and hints.get(name) is not tmp_dir)
        )
        # frozen dataclass — use object.__setattr__ for post-init
        object.__setattr__(self, "_signature", sig)
        object.__setattr__(self, "_type_hints", hints)
        object.__setattr__(self, "_required_params", required)

    @property
    def name(self) -> str:
        """Fully qualified name of the wrapped function."""
        module = getattr(self.fn, "__module__", None) or ""
        return f"{module}.{self.fn.__qualname__}"

    @property
    def required_params(self) -> frozenset[str]:
        """Parameter names that have no default value."""
        return self._required_params

    @property
    def execution_mode(self) -> str:
        """Return whether the task body runs on the driver or a worker."""
        if self.kind == "shell":
            return "driver"
        return "worker"

    @property
    def all_params(self) -> dict[str, inspect.Parameter]:
        """All parameters from the function signature."""
        return dict(self._signature.parameters)

    @property
    def signature(self) -> inspect.Signature:
        """The inspected function signature."""
        return self._signature

    @property
    def type_hints(self) -> dict[str, Any]:
        """Resolved runtime type hints for the wrapped function."""
        return dict(self._type_hints)

    def __call__(self, **kwargs: Any) -> Expr | PartialCall:
        """Build an ``Expr`` (all required args supplied) or ``PartialCall``.

        Parameters
        ----------
        **kwargs
            Keyword arguments for the task.  If all required parameters are
            covered, returns ``Expr``.  Otherwise returns ``PartialCall``.

        Returns
        -------
        Expr | PartialCall
        """
        supplied = set(kwargs.keys())

        # Validate that all supplied args are valid parameter names
        valid_params = set(self.all_params.keys())
        unknown = supplied - valid_params
        if unknown:
            raise TypeError(
                f"{self.fn.__name__}() got unexpected keyword arguments: "
                f"{', '.join(sorted(unknown))}"
            )

        managed = {name for name, annotation in self._type_hints.items() if annotation is tmp_dir}
        supplied_managed = supplied & managed
        if supplied_managed:
            raise TypeError(
                f"{self.fn.__name__}() arguments are auto-managed by ginkgo: "
                f"{', '.join(sorted(supplied_managed))}"
            )

        if self._required_params <= supplied:
            # All required params supplied — produce an Expr
            return Expr(task_def=self, args=kwargs, mapped=False)

        # Partial call — some required params are missing
        return PartialCall(task_def=self, fixed_args=kwargs)

    def __reduce__(self) -> tuple[Callable[..., TaskDef], tuple[str, str]]:
        """Serialize task definitions by their module-level binding."""
        return (_load_taskdef, (self.fn.__module__, self.fn.__name__))


@dataclass(frozen=True)
class PartialCall:
    """A partially applied task call, awaiting ``.map()`` for remaining args.

    Parameters
    ----------
    task_def : TaskDef
        The task definition.
    fixed_args : dict[str, object]
        Arguments already supplied.
    """

    task_def: TaskDef
    fixed_args: dict[str, object] = field(default_factory=dict)

    def map(self, **varying: Any) -> ExprList:
        """Fan-out: produce one ``Expr`` per element by zipping varying columns.

        All varying argument columns must be the same length.

        Parameters
        ----------
        **varying
            Keyword arguments where each value is an iterable (list, Series,
            or ``ExprList``) of per-element values.

        Returns
        -------
        ExprList
            One ``Expr`` per element in the varying columns.

        Raises
        ------
        ValueError
            If varying columns have different lengths or no varying args given.
        TypeError
            If a varying argument name is not a valid parameter.
        """
        if not varying:
            raise ValueError("map() requires at least one varying argument")

        # Validate varying arg names against function signature
        valid_params = set(self.task_def.all_params.keys())
        unknown = set(varying.keys()) - valid_params
        if unknown:
            raise TypeError(
                f"{self.task_def.fn.__name__}() got unexpected keyword arguments: "
                f"{', '.join(sorted(unknown))}"
            )

        managed = {
            name for name, annotation in self.task_def.type_hints.items() if annotation is tmp_dir
        }
        supplied_managed = set(varying.keys()) & managed
        if supplied_managed:
            raise TypeError(
                f"{self.task_def.fn.__name__}() arguments are auto-managed by ginkgo: "
                f"{', '.join(sorted(supplied_managed))}"
            )

        # Materialise columns and check lengths match
        columns: dict[str, list] = {}
        length: int | None = None
        for key, col in varying.items():
            items = list(col)
            if length is None:
                length = len(items)
            elif len(items) != length:
                raise ValueError(
                    f"map() columns have mismatched lengths: expected {length}, "
                    f"got {len(items)} for '{key}'"
                )
            columns[key] = items

        assert length is not None  # guaranteed by the non-empty check above

        # Build one Expr per row
        exprs: list[Expr] = []
        for i in range(length):
            row_args = dict(self.fixed_args)
            for key, items in columns.items():
                row_args[key] = items[i]
            exprs.append(Expr(task_def=self.task_def, args=row_args, mapped=True))

        return ExprList(exprs=exprs)


def task(
    *,
    env: str | None = None,
    version: int = 1,
    retries: int = 0,
    kind: str = "python",
) -> Callable[[Callable[..., Any]], TaskDef]:
    """Decorator that turns a function into a lazy task definition.

    Parameters
    ----------
    env : str | None
        Pixi environment name.  If ``None``, the task runs in the current env.
    version : int
        Cache-busting version tag.  Bump when task logic changes.
    retries : int
        Additional retry attempts after the initial execution.
    kind : str
        Execution contract for the task body. Use ``"shell"`` for
        scheduler-evaluated shell spec builders.

    Returns
    -------
    Callable
        A decorator that wraps the function in a ``TaskDef``.
    """

    def decorator(fn: Callable[..., Any]) -> TaskDef:
        return TaskDef(fn=fn, env=env, version=version, retries=retries, kind=kind)

    return decorator


def _load_taskdef(module_name: str, task_name: str) -> TaskDef:
    """Load a task definition from its module-level binding."""
    module = import_module(module_name)
    task_def = getattr(module, task_name)
    if not isinstance(task_def, TaskDef):
        raise TypeError(f"{module_name}.{task_name} is not a ginkgo task")
    return task_def
