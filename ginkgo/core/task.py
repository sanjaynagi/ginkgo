"""The ``@task`` decorator and supporting classes.

A ``@task()``-decorated function does not execute when called.  Instead it
returns an ``Expr[T]`` (full call) or a ``PartialCall`` (subset of required
arguments), enabling lazy expression tree construction.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from importlib import import_module
from itertools import product
from typing import Any, Callable, Literal, get_type_hints

from ginkgo.core.expr import Expr, ExprList
from ginkgo.core.types import tmp_dir

_TASK_KINDS = frozenset({"notebook", "python", "script", "shell"})
_FanOutMode = Literal["zip", "product"]


@dataclass(frozen=True)
class TaskDef:
    """Wraps a user function so that calls produce expression nodes.

    Parameters
    ----------
    fn : Callable
        The original user function.
    env : str | None
        Foreign execution environment for shell tasks.
    version : int
        Cache-busting version tag.
    retries : int
        Additional retry attempts after the initial execution.
    kind : str
        Execution contract for the task body.
    threads : int
        Static CPU footprint for the scheduler. Used as the task's core
        budget against ``--cores`` and made available to the task body when
        the function signature declares a ``threads`` parameter. Shell tasks
        also receive ``GINKGO_THREADS=<n>`` in the subprocess environment.
    export_thread_env : bool
        When ``True``, shell tasks additionally receive ``OMP_NUM_THREADS``,
        ``MKL_NUM_THREADS``, ``OPENBLAS_NUM_THREADS``, and
        ``NUMEXPR_NUM_THREADS`` set to the declared thread count. Default is
        ``False`` so existing tool configuration is not silently overridden.
    """

    fn: Callable[..., Any]
    env: str | None = None
    version: int = 1
    retries: int = 0
    kind: str = "python"
    threads: int = 1
    export_thread_env: bool = False
    _signature: inspect.Signature = field(init=False, repr=False)
    _type_hints: dict[str, Any] = field(init=False, repr=False)
    _required_params: frozenset[str] = field(init=False, repr=False)
    _source_hash: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ValueError("retries must be at least 0")
        if self.kind not in _TASK_KINDS:
            supported = ", ".join(sorted(_TASK_KINDS))
            raise ValueError(f"kind must be one of {{{supported}}}, got {self.kind!r}")
        if self.threads < 1:
            raise ValueError(f"threads must be at least 1, got {self.threads}")

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
        object.__setattr__(self, "_source_hash", _compute_source_hash(self.fn))

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
        if self.kind in {"notebook", "script", "shell"}:
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

    @property
    def source_hash(self) -> str:
        """BLAKE3 digest of the task function's source code."""
        return self._source_hash

    @property
    def cache_source_hash(self) -> str:
        """Digest used for cache invalidation.

        For notebook and script tasks, the source file hash is incorporated
        at execution time via the ``NotebookExpr``/``ScriptExpr`` sentinel.
        """
        return self._source_hash

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

        if "threads" in supplied:
            import warnings

            warnings.warn(
                f"{self.fn.__name__}(): passing 'threads' as a function argument has no "
                "scheduler effect. Declare the static thread count on the decorator "
                "instead, e.g. @task(threads=N).",
                stacklevel=2,
            )

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

    def map(self, *, max_concurrent: int | None = None, **varying: Any) -> ExprList:
        """Fan-out: produce one ``Expr`` per element by zipping varying columns.

        All varying argument columns must be the same length.

        Parameters
        ----------
        max_concurrent : int | None
            When set, the scheduler will run at most this many generated
            branches concurrently, independently of ``--jobs`` and
            ``--cores`` limits. Use this to throttle classes of work that
            should not run in parallel (e.g. model training).
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
        return _fan_out_partial_call(
            partial_call=self,
            varying=varying,
            mode="zip",
            max_concurrent=max_concurrent,
        )

    def product_map(self, *, max_concurrent: int | None = None, **varying: Any) -> ExprList:
        """Fan-out: produce one ``Expr`` per Cartesian combination."""
        return _fan_out_partial_call(
            partial_call=self,
            varying=varying,
            mode="product",
            max_concurrent=max_concurrent,
        )


def _next_concurrency_group_id(task_def: TaskDef) -> str:
    """Return one process-unique concurrency group identifier."""
    global _concurrency_group_counter
    _concurrency_group_counter += 1
    return f"map:{task_def.name}:{_concurrency_group_counter}"


def _validate_max_concurrent(*, max_concurrent: int | None, function_name: str) -> None:
    """Reject non-positive ``max_concurrent`` values."""
    if max_concurrent is None:
        return
    if not isinstance(max_concurrent, int) or isinstance(max_concurrent, bool):
        raise TypeError(
            f"{function_name}() max_concurrent must be an integer, got "
            f"{type(max_concurrent).__name__}"
        )
    if max_concurrent < 1:
        raise ValueError(f"{function_name}() max_concurrent must be at least 1")


def _fan_out_partial_call(
    *,
    partial_call: PartialCall,
    varying: dict[str, Any],
    mode: _FanOutMode,
    max_concurrent: int | None = None,
) -> ExprList:
    """Build an ``ExprList`` from one partially-applied task."""
    function_name = _fan_out_function_name(mode=mode)
    _validate_max_concurrent(max_concurrent=max_concurrent, function_name=function_name)
    columns = _materialize_varying_columns(
        task_def=partial_call.task_def,
        varying=varying,
        function_name=function_name,
    )
    rows = _build_varying_rows(columns=columns, mode=mode, function_name=function_name)
    varying_keys = tuple(columns.keys())
    group_id = (
        _next_concurrency_group_id(partial_call.task_def) if max_concurrent is not None else None
    )
    exprs = [
        Expr(
            task_def=partial_call.task_def,
            args={**partial_call.fixed_args, **row},
            mapped=True,
            display_label_parts=_label_parts_for_row(
                task_def=partial_call.task_def,
                row=row,
                mode=mode,
                varying_keys=varying_keys,
            ),
            concurrency_group=group_id,
            concurrency_group_limit=max_concurrent,
        )
        for row in rows
    ]
    return ExprList(exprs=exprs, task_def=partial_call.task_def)


def _fan_out_expr_list(
    *,
    expr_list: ExprList,
    varying: dict[str, Any],
    mode: _FanOutMode,
    max_concurrent: int | None = None,
) -> ExprList:
    """Extend each existing branch with additional fan-out rows."""
    function_name = _fan_out_function_name(mode=mode)
    _validate_max_concurrent(max_concurrent=max_concurrent, function_name=function_name)
    task_def = _expr_list_task_def(expr_list=expr_list, function_name=function_name)
    columns = _materialize_varying_columns(
        task_def=task_def,
        varying=varying,
        function_name=function_name,
    )
    rows = _build_varying_rows(columns=columns, mode=mode, function_name=function_name)
    varying_keys = tuple(columns.keys())
    group_id = _next_concurrency_group_id(task_def) if max_concurrent is not None else None
    exprs = [
        Expr(
            task_def=task_def,
            args={**base_expr.args, **row},
            mapped=True,
            display_label_parts=(
                *base_expr.display_label_parts,
                *_label_parts_for_row(
                    task_def=task_def,
                    row=row,
                    mode=mode,
                    varying_keys=varying_keys,
                ),
            ),
            concurrency_group=(
                group_id if max_concurrent is not None else base_expr.concurrency_group
            ),
            concurrency_group_limit=(
                max_concurrent if max_concurrent is not None else base_expr.concurrency_group_limit
            ),
        )
        for base_expr in expr_list
        for row in rows
    ]
    return ExprList(exprs=exprs, task_def=task_def)


_concurrency_group_counter: int = 0


def _expr_list_task_def(*, expr_list: ExprList, function_name: str) -> TaskDef:
    """Return the shared task definition for one fan-out expression list."""
    if expr_list.task_def is not None:
        return expr_list.task_def
    if not expr_list.exprs:
        raise ValueError(
            f"{function_name}() cannot extend an empty ExprList without task metadata"
        )

    task_def = expr_list.exprs[0].task_def
    if any(expr.task_def is not task_def for expr in expr_list.exprs[1:]):
        raise TypeError(f"{function_name}() requires all ExprList elements to share one task")
    return task_def


def _materialize_varying_columns(
    *,
    task_def: TaskDef,
    varying: dict[str, Any],
    function_name: str,
) -> dict[str, list[Any]]:
    """Validate and materialize varying columns for fan-out."""
    if not varying:
        raise ValueError(f"{function_name}() requires at least one varying argument")

    valid_params = set(task_def.all_params.keys())
    unknown = set(varying.keys()) - valid_params
    if unknown:
        raise TypeError(
            f"{task_def.fn.__name__}() got unexpected keyword arguments: "
            f"{', '.join(sorted(unknown))}"
        )

    managed = {name for name, annotation in task_def.type_hints.items() if annotation is tmp_dir}
    supplied_managed = set(varying.keys()) & managed
    if supplied_managed:
        raise TypeError(
            f"{task_def.fn.__name__}() arguments are auto-managed by ginkgo: "
            f"{', '.join(sorted(supplied_managed))}"
        )

    if "threads" in varying:
        import warnings

        warnings.warn(
            f"{task_def.fn.__name__}(): passing 'threads' as a fan-out argument has no "
            "scheduler effect. Declare the static thread count on the decorator instead, "
            "e.g. @task(threads=N).",
            stacklevel=3,
        )

    return {key: list(column) for key, column in varying.items()}


def _build_varying_rows(
    *,
    columns: dict[str, list[Any]],
    mode: _FanOutMode,
    function_name: str,
) -> list[dict[str, Any]]:
    """Build row dictionaries for one fan-out call."""
    if mode == "zip":
        lengths = {len(items) for items in columns.values()}
        if len(lengths) > 1:
            expected_length = len(next(iter(columns.values())))
            mismatch_key, mismatch_items = next(
                (key, items) for key, items in columns.items() if len(items) != expected_length
            )
            raise ValueError(
                f"{function_name}() columns have mismatched lengths: expected {expected_length}, "
                f"got {len(mismatch_items)} for '{mismatch_key}'"
            )
        return [
            dict(zip(columns.keys(), values, strict=True))
            for values in zip(*columns.values(), strict=True)
        ]

    return [
        dict(zip(columns.keys(), values, strict=True)) for values in product(*columns.values())
    ]


def _fan_out_function_name(*, mode: _FanOutMode) -> str:
    """Return the public method name for one fan-out mode."""
    if mode == "zip":
        return "map"
    return "product_map"


def _label_parts_for_row(
    *,
    task_def: TaskDef,
    row: dict[str, Any],
    mode: _FanOutMode,
    varying_keys: tuple[str, ...],
) -> tuple[str, ...]:
    """Return display-label fragments for one fan-out row."""
    if not varying_keys:
        return ()

    if mode == "zip":
        first_key = varying_keys[0]
        rendered = _render_label_value(row.get(first_key))
        if rendered is None:
            return ()
        return (rendered,)

    parts: list[str] = []
    valid_params = set(task_def.all_params.keys())
    for key in varying_keys:
        if key not in valid_params:
            continue
        rendered = _render_label_value(row.get(key))
        if rendered is None:
            continue
        parts.append(f"{key}={rendered}")
    return tuple(parts)


def _render_label_value(value: Any) -> str | None:
    """Render one concise label-safe value."""
    if isinstance(value, Expr):
        return None
    if isinstance(value, ExprList):
        return None
    if value is None:
        return None
    return str(value)


def task(
    _kind: str | None = None,
    /,
    *,
    env: str | None = None,
    version: int = 1,
    retries: int = 0,
    kind: str = "python",
    threads: int = 1,
    export_thread_env: bool = False,
) -> Callable[[Callable[..., Any]], TaskDef]:
    """Decorator that turns a function into a lazy task definition.

    The task kind may be given as the first positional argument or via the
    ``kind`` keyword.  ``@task("shell")``, ``@task("notebook")``, and
    ``@task("script")`` are the preferred short forms.

    Parameters
    ----------
    _kind : str | None
        Task kind as a positional argument. When provided, takes precedence
        over the ``kind`` keyword.
    env : str | None
        Foreign execution environment for shell tasks. If ``None``, the task
        runs in the current environment.
    version : int
        Cache-busting version tag.  Bump when task logic changes.
    retries : int
        Additional retry attempts after the initial execution.
    kind : str
        Execution contract for the task body. Ignored when ``_kind`` is given.
    threads : int
        Static CPU footprint for the scheduler. The task body receives the
        same value when its function signature declares a ``threads``
        parameter; shell tasks also see ``GINKGO_THREADS=<n>`` in the
        subprocess environment.
    export_thread_env : bool
        Export common BLAS/OpenMP thread environment variables
        (``OMP_NUM_THREADS``, ``MKL_NUM_THREADS``, ``OPENBLAS_NUM_THREADS``,
        ``NUMEXPR_NUM_THREADS``) to shell-task subprocesses. Default
        ``False``.

    Returns
    -------
    Callable
        A decorator that wraps the function in a ``TaskDef``.

    Raises
    ------
    ValueError
        If both a positional kind and a non-default ``kind`` keyword are
        supplied and they differ.
    """
    resolved_kind = _kind if _kind is not None else kind
    if _kind is not None and kind != "python" and _kind != kind:
        raise ValueError(f"task kind specified twice: positional {_kind!r} and keyword {kind!r}")

    def decorator(fn: Callable[..., Any]) -> TaskDef:
        return TaskDef(
            fn=fn,
            env=env,
            version=version,
            retries=retries,
            kind=resolved_kind,
            threads=threads,
            export_thread_env=export_thread_env,
        )

    return decorator


def _compute_source_hash(fn: Callable[..., Any]) -> str:
    """Return the BLAKE3 digest of a function's source code.

    Parameters
    ----------
    fn : Callable
        The function to hash.

    Returns
    -------
    str
        Hex-encoded BLAKE3 digest.

    Raises
    ------
    ValueError
        If the source cannot be extracted (lambdas, dynamic functions).
    """
    from ginkgo.runtime.caching.hashing import hash_str

    try:
        source = inspect.getsource(fn)
    except OSError as exc:
        raise ValueError(
            f"Cannot extract source for task '{fn.__qualname__}'. "
            "Tasks must be defined as named, top-level functions."
        ) from exc
    return hash_str(source)


def _load_taskdef(module_name: str, task_name: str) -> TaskDef:
    """Load a task definition from its module-level binding."""
    module = import_module(module_name)
    task_def = getattr(module, task_name)
    if not isinstance(task_def, TaskDef):
        raise TypeError(f"{module_name}.{task_name} is not a ginkgo task")
    return task_def
