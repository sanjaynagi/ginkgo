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

import re

from ginkgo.core.expr import Expr, ExprList, record_call, supersede_call
from ginkgo.core.resources import Resources
from ginkgo.core.source_hash import compute_source_hash
from ginkgo.core.types import tmp_dir
from ginkgo.wildcards import ExpandedTemplate, PerBranch

_TASK_KINDS = frozenset({"notebook", "python", "script", "shell", "subworkflow"})
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
    retry_on : type[BaseException] | tuple[type[BaseException], ...] | None
        When set, only retry failures matching these exception classes.
        ``None`` (default) retries every failure up to ``retries``.
    retry_backoff : float
        Base delay in seconds between retry attempts. ``0.0`` (default)
        reruns immediately.
    retry_backoff_multiplier : float
        Exponential factor applied to the base delay. Delay for attempt
        *k* (1-indexed) is ``retry_backoff * retry_backoff_multiplier ** (k - 1)``,
        capped at ``retry_backoff_max``.
    retry_backoff_max : float
        Upper bound on the computed retry delay, in seconds.
    retry_on_exit_codes : tuple[int, ...] | None
        Shell-task only. When set, only retry failures whose exit code is
        in this tuple. Ignored for non-shell tasks.
    priority : int
        Relative scheduling priority. When several tasks are ready at the
        same time, higher-priority tasks are dispatched first. Range is
        ``[-1000, 1000]``; default ``0``.
    kind : str
        Execution contract for the task body.
    resources : Resources
        Declarative resource requirements (threads, memory, gpu, gpu_type).
        States what the task needs; placement is decided separately by the
        evaluator. ``threads`` is made available to the task body when the
        function signature declares a ``threads`` parameter, and shell tasks
        receive ``GINKGO_THREADS=<n>`` in the subprocess environment.
    remote : bool
        When ``True``, dispatch this task to the remote executor. Requires
        an executor to be configured via ``--executor``.
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
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None = None
    retry_backoff: float = 0.0
    retry_backoff_multiplier: float = 2.0
    retry_backoff_max: float = 60.0
    retry_on_exit_codes: tuple[int, ...] | None = None
    priority: int = 0
    kind: str = "python"
    resources: Resources = field(default_factory=Resources)
    remote: bool = False
    export_thread_env: bool = False
    remote_input_access: str | None = None
    streaming_compatible: bool = True
    fuse_prefetch: tuple[tuple[str, str], ...] = ()
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
        if self.retry_backoff < 0:
            raise ValueError(f"retry_backoff must be at least 0, got {self.retry_backoff}")
        if self.retry_backoff_multiplier < 1:
            raise ValueError(
                f"retry_backoff_multiplier must be at least 1, got {self.retry_backoff_multiplier}"
            )
        if self.retry_backoff_max < 0:
            raise ValueError(f"retry_backoff_max must be at least 0, got {self.retry_backoff_max}")
        _validate_retry_on(self.retry_on)
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError(f"priority must be an integer, got {type(self.priority).__name__}")
        if abs(self.priority) > 1000:
            raise ValueError(f"priority must be in range [-1000, 1000], got {self.priority}")
        if self.retry_on_exit_codes is not None:
            if self.kind != "shell":
                raise ValueError("retry_on_exit_codes is only valid for shell tasks")
            for code in self.retry_on_exit_codes:
                if not isinstance(code, int) or isinstance(code, bool):
                    raise ValueError(f"retry_on_exit_codes must be integers, got {code!r}")

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
        object.__setattr__(self, "_source_hash", compute_source_hash(self.fn))

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
        """BLAKE3 digest of task source and local imported modules."""
        return self._source_hash

    @property
    def threads(self) -> int:
        """Declared CPU footprint (see :class:`Resources`)."""
        return self.resources.threads

    @property
    def memory(self) -> str | None:
        """Declared memory footprint string (see :class:`Resources`)."""
        return self.resources.memory

    @property
    def memory_gb(self) -> int:
        """Parsed memory footprint in whole GiB (0 when unset)."""
        return self.resources.memory_gb

    @property
    def gpu(self) -> int:
        """Declared GPU count (see :class:`Resources`)."""
        return self.resources.gpu

    @property
    def gpu_type(self) -> str | None:
        """Declared accelerator type (see :class:`Resources`)."""
        return self.resources.gpu_type

    @property
    def cache_source_hash(self) -> str:
        """Digest used for cache invalidation.

        For notebook and script tasks, the source file hash is incorporated
        at execution time via the ``NotebookDirective``/``ScriptDirective``.
        """
        return self._source_hash

    def should_retry_exception(self, *, exc: BaseException) -> bool:
        """Return whether ``exc`` matches the configured retry policy."""
        if self.retry_on is None and self.retry_on_exit_codes is None:
            return True

        class_match = self.retry_on is None or isinstance(exc, self.retry_on)
        if self.retry_on_exit_codes is not None:
            exit_code = getattr(exc, "exit_code", None)
            code_match = isinstance(exit_code, int) and exit_code in self.retry_on_exit_codes
        else:
            code_match = True

        if self.retry_on is not None and self.retry_on_exit_codes is not None:
            return class_match and code_match
        return class_match and code_match

    def retry_delay_seconds(self, *, attempt: int) -> float:
        """Return the retry delay to wait before ``attempt`` (1-indexed)."""
        if self.retry_backoff <= 0 or attempt < 1:
            return 0.0
        delay = self.retry_backoff * (self.retry_backoff_multiplier ** (attempt - 1))
        return min(delay, self.retry_backoff_max)

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
            expr = Expr(task_def=self, args=kwargs, mapped=False)
            record_call(expr)
            return expr

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
            or ``ExprList``) of per-element values, or a ``per_branch()``
            template rendered from each row's own values.

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
        """Fan-out: produce one ``Expr`` per Cartesian combination.

        Every varying list is an axis of the grid. Arguments that are a
        *function* of the grid cell — output paths above all — must be
        passed as ``per_branch("...{arg}...")`` templates, which are
        rendered from each cell's own values. ``expand()`` output is
        rejected here: it is already one value per cell, so crossing it
        with the axes it came from would mislabel every branch.
        """
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
    varying_args = _materialize_varying_columns(
        task_def=partial_call.task_def,
        varying=varying,
        mode=mode,
        function_name=function_name,
    )
    columns = varying_args.columns
    rows = _build_varying_rows(columns=columns, mode=mode, function_name=function_name)
    varying_keys = tuple(columns.keys())
    group_id = (
        _next_concurrency_group_id(partial_call.task_def) if max_concurrent is not None else None
    )
    exprs = [
        Expr(
            task_def=partial_call.task_def,
            args=_branch_args(
                base_args=partial_call.fixed_args,
                row=row,
                derived=varying_args.derived,
                task_def=partial_call.task_def,
                function_name=function_name,
            ),
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
    expr_list = ExprList(exprs=exprs, task_def=partial_call.task_def)
    record_call(expr_list)
    return expr_list


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
    varying_args = _materialize_varying_columns(
        task_def=task_def,
        varying=varying,
        mode=mode,
        function_name=function_name,
    )
    columns = varying_args.columns
    rows = _build_varying_rows(columns=columns, mode=mode, function_name=function_name)
    varying_keys = tuple(columns.keys())
    group_id = _next_concurrency_group_id(task_def) if max_concurrent is not None else None
    exprs = [
        Expr(
            task_def=task_def,
            args=_branch_args(
                base_args=base_expr.args,
                row=row,
                derived=varying_args.derived,
                task_def=task_def,
                function_name=function_name,
            ),
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
    # The base branches are rebuilt here, so their original call is no longer
    # part of the graph and must not be reported as dropped.
    supersede_call(expr_list)
    extended = ExprList(exprs=exprs, task_def=task_def)
    record_call(extended)
    return extended


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


@dataclass(frozen=True)
class _VaryingArgs:
    """The two kinds of varying argument one fan-out call can receive.

    Parameters
    ----------
    columns : dict[str, list[Any]]
        Value columns that generate branches — zipped or crossed by mode.
    derived : dict[str, PerBranch]
        Templates rendered once per generated branch from that branch's
        own values, so they never generate branches of their own.
    """

    columns: dict[str, list[Any]]
    derived: dict[str, PerBranch]


def _materialize_varying_columns(
    *,
    task_def: TaskDef,
    varying: dict[str, Any],
    mode: _FanOutMode,
    function_name: str,
) -> _VaryingArgs:
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

    derived = {key: value for key, value in varying.items() if isinstance(value, PerBranch)}
    axis_keys = [
        key
        for key, value in varying.items()
        if not isinstance(value, PerBranch | ExpandedTemplate)
    ]

    columns: dict[str, list[Any]] = {}
    for key, value in varying.items():
        if key in derived:
            continue
        if isinstance(value, str | bytes):
            raise TypeError(
                f"{function_name}() argument {key!r} is a {type(value).__name__}, which would "
                f"fan out over its individual characters. Pass a list of values, or pass "
                f"{key}={value!r} as a fixed argument on the {task_def.fn.__name__}() call."
            )
        if mode == "product" and isinstance(value, ExpandedTemplate):
            raise ValueError(
                _expanded_template_in_product_message(key=key, column=value, axis_keys=axis_keys)
            )
        columns[key] = list(value)

    if not columns:
        raise ValueError(
            f"{function_name}() received only per_branch() arguments "
            f"({', '.join(sorted(derived))}), which derive from other varying arguments rather "
            "than generating branches. Add at least one varying list."
        )

    _validate_per_branch_templates(
        task_def=task_def,
        derived=derived,
        function_name=function_name,
    )
    return _VaryingArgs(columns=columns, derived=derived)


def _expanded_template_in_product_message(
    *,
    key: str,
    column: ExpandedTemplate,
    axis_keys: list[str],
) -> str:
    """Explain why an expanded template cannot be a ``product_map()`` axis."""
    suggestion = column.as_per_branch_template(axis_keys)
    return (
        f"product_map() argument {key!r} was built by {column.function_name}"
        f"({column.template!r}), which already returns one value per combination of its "
        f"wildcards, not an axis to sweep. Crossing it with the grid would produce one branch "
        f"per (grid cell, {key}) pair, so branches would carry {key} values that contradict "
        f"their other arguments. Derive it per branch instead:\n"
        f"    {key}=per_branch({suggestion!r})\n"
        "spelling the placeholders as this call's own argument names "
        f"({', '.join(axis_keys) or 'none varying'}); per_branch() renders once per grid cell "
        f"from that cell's values. {column.function_name}() output remains correct with .map(), "
        "where every column is consumed row by row."
    )


def _validate_per_branch_templates(
    *,
    task_def: TaskDef,
    derived: dict[str, PerBranch],
    function_name: str,
) -> None:
    """Reject ``per_branch()`` templates that name arguments the task lacks."""
    available = set(task_def.all_params.keys())
    for key, template in derived.items():
        unknown = [name for name in template.placeholder_names() if name not in available]
        if unknown:
            raise ValueError(
                f"{function_name}() argument {key!r}: per_branch({template.template!r}) "
                f"references {', '.join(sorted(unknown))}, which "
                f"{task_def.fn.__name__}() does not take. Available: "
                f"{', '.join(sorted(available))}."
            )


def _branch_args(
    *,
    base_args: dict[str, Any],
    row: dict[str, Any],
    derived: dict[str, PerBranch],
    task_def: TaskDef,
    function_name: str,
) -> dict[str, Any]:
    """Return one branch's arguments, rendering per-branch templates last."""
    args: dict[str, Any] = {**base_args, **row}
    for key, template in derived.items():
        _require_renderable(
            values=args,
            key=key,
            template=template,
            task_def=task_def,
            function_name=function_name,
        )
        args[key] = template.render(args)
    return args


def _require_renderable(
    *,
    values: dict[str, Any],
    key: str,
    template: PerBranch,
    task_def: TaskDef,
    function_name: str,
) -> None:
    """Check that one branch can supply every value a template asks for."""
    for name in template.placeholder_names():
        if name not in values:
            raise ValueError(
                f"{function_name}() argument {key!r}: per_branch({template.template!r}) "
                f"references {name!r}, which this branch does not set. Pass it as a varying "
                f"argument to {function_name}() or as a fixed argument on the "
                f"{task_def.fn.__name__}() call."
            )
        if isinstance(values[name], Expr | ExprList):
            raise ValueError(
                f"{function_name}() argument {key!r}: per_branch({template.template!r}) "
                f"references {name!r}, which is a task result with no value until the run "
                "reaches it. Reference plain values only."
            )


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


_MAX_ZIP_LABEL_LENGTH = 24
_PATH_LIKE_PATTERN = re.compile(r"[/\\]|\.[A-Za-z][A-Za-z0-9]{0,4}$")


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
        return _zip_label_parts_for_row(row=row, varying_keys=varying_keys)

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


def _zip_label_parts_for_row(
    *,
    row: dict[str, Any],
    varying_keys: tuple[str, ...],
) -> tuple[str, ...]:
    """Pick the most distinguishing label for one zip fan-out row.

    Prefers the first varying value that renders as a short, non-path
    scalar. When every varying value is an ``Expr``/``ExprList`` (the
    normal shape for a chained downstream task), inherit the label of
    the upstream branch that produced it, since the zip position ties
    each row back to exactly one producing branch. Falls back to the
    first key's rendered value, matching the previous behaviour.
    """
    for key in varying_keys:
        rendered = _render_label_value(row.get(key))
        if rendered is not None and _is_short_scalar_label(rendered):
            return (rendered,)

    for key in varying_keys:
        value = row.get(key)
        if isinstance(value, Expr) and value.display_label_parts:
            return value.display_label_parts

    first_key = varying_keys[0]
    rendered = _render_label_value(row.get(first_key))
    if rendered is None:
        return ()
    return (rendered,)


def _is_short_scalar_label(rendered: str) -> bool:
    """Return whether a rendered value reads as a short, non-path scalar."""
    if len(rendered) > _MAX_ZIP_LABEL_LENGTH:
        return False
    return not _PATH_LIKE_PATTERN.search(rendered)


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
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None = None,
    retry_backoff: float = 0.0,
    retry_backoff_multiplier: float = 2.0,
    retry_backoff_max: float = 60.0,
    retry_on_exit_codes: tuple[int, ...] | None = None,
    priority: int = 0,
    kind: str = "python",
    threads: int = 1,
    memory: str | None = None,
    gpu: int = 0,
    gpu_type: str | None = None,
    memory_retry_multiplier: float = 1.0,
    resources: dict[str, int] | None = None,
    remote: bool = False,
    export_thread_env: bool = False,
    remote_input_access: str | None = None,
    streaming_compatible: bool = True,
    fuse_prefetch: dict[str, str] | None = None,
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
    retry_on : type[BaseException] | tuple[type[BaseException], ...] | None
        Narrow retries to specific exception classes. ``None`` retries any
        failure.
    retry_backoff : float
        Base delay (seconds) before each retry. ``0.0`` disables the delay.
    retry_backoff_multiplier : float
        Exponential factor applied between attempts.
    retry_backoff_max : float
        Upper bound on the computed delay, in seconds.
    retry_on_exit_codes : tuple[int, ...] | None
        Shell-task only. Narrow retries to specific exit codes.
    priority : int
        Relative scheduling priority. Higher runs first among ready tasks.
        Range ``[-1000, 1000]``; default ``0``.
    kind : str
        Execution contract for the task body. Ignored when ``_kind`` is given.
    threads : int
        Static CPU footprint for the scheduler. The task body receives the
        same value when its function signature declares a ``threads``
        parameter; shell tasks also see ``GINKGO_THREADS=<n>`` in the
        subprocess environment.
    memory : str | None
        Static memory footprint for the scheduler in Kubernetes resource
        notation (e.g. ``"4Gi"``, ``"512Mi"``).
    gpu : int
        Number of GPUs the task requires. Locally this reserves against the
        ``--gpus`` budget; a requirement the local budget cannot satisfy is
        dispatched to the remote executor when one is configured, and is a
        build error otherwise. Remote executors map the count to the
        appropriate accelerator resource (e.g. ``nvidia.com/gpu`` on
        Kubernetes).
    gpu_type : str | None
        Accelerator type for remote execution (e.g. ``"nvidia-tesla-t4"``).
        Overrides the executor-level default. Requires ``gpu > 0``.
    memory_retry_multiplier : float
        Factor applied to ``memory`` on each retry attempt, capped at the
        run's ``--memory`` budget. Use with ``retries`` for OOM-prone tasks
        (e.g. ``memory="16Gi", retries=2, memory_retry_multiplier=2`` runs
        attempts at 16, 32, and 64 GiB). Requires ``memory``.
    resources : dict[str, int] | None
        User-defined resource demands (e.g. ``{"api_calls": 2}``), scheduled
        against run-level budgets from ``[resources.budgets]`` config or
        repeated ``--resource name=value`` flags. Dimensions without a
        configured budget are unconstrained. Counted wherever the task runs,
        including remote executors.
    remote : bool
        When ``True``, dispatch this task to the remote executor. Requires
        an executor to be configured via ``--executor``.
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

    if remote_input_access is not None and remote_input_access not in {"stage", "fuse"}:
        raise ValueError(
            f"remote_input_access must be 'stage' or 'fuse', got {remote_input_access!r}"
        )

    prefetch_map = tuple(sorted((fuse_prefetch or {}).items()))

    def decorator(fn: Callable[..., Any]) -> TaskDef:
        return TaskDef(
            fn=fn,
            env=env,
            version=version,
            retries=retries,
            retry_on=retry_on,
            retry_backoff=retry_backoff,
            retry_backoff_multiplier=retry_backoff_multiplier,
            retry_backoff_max=retry_backoff_max,
            retry_on_exit_codes=retry_on_exit_codes,
            priority=priority,
            kind=resolved_kind,
            resources=Resources(
                threads=threads,
                memory=memory,
                gpu=gpu,
                gpu_type=gpu_type,
                memory_retry_multiplier=memory_retry_multiplier,
                custom=dict(resources or {}),
            ),
            remote=remote,
            export_thread_env=export_thread_env,
            remote_input_access=remote_input_access,
            streaming_compatible=streaming_compatible,
            fuse_prefetch=prefetch_map,
        )

    return decorator


def _validate_retry_on(
    retry_on: type[BaseException] | tuple[type[BaseException], ...] | None,
) -> None:
    """Validate that ``retry_on`` names only exception classes."""
    if retry_on is None:
        return
    candidates: tuple[Any, ...] = retry_on if isinstance(retry_on, tuple) else (retry_on,)
    for candidate in candidates:
        if not (isinstance(candidate, type) and issubclass(candidate, BaseException)):
            raise ValueError(
                "retry_on must be an exception class or tuple of exception "
                f"classes, got {candidate!r}"
            )


def _load_taskdef(module_name: str, task_name: str) -> TaskDef:
    """Load a task definition from its module-level binding."""
    module = import_module(module_name)
    task_def = getattr(module, task_name)
    if not isinstance(task_def, TaskDef):
        raise TypeError(f"{module_name}.{task_name} is not a ginkgo task")
    return task_def
