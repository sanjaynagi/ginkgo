"""Core expression tree nodes for lazy workflow evaluation.

Every ``@task``-decorated function, when called, returns an ``Expr[T]`` rather
than executing. The evaluator recursively resolves these nodes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from types import UnionType
from typing import TYPE_CHECKING, Any, Generic, TypeVar, get_args

if TYPE_CHECKING:
    from ginkgo.core.task import PartialCall, TaskDef

T = TypeVar("T")


def _refused_value_operation(action: str) -> Callable[..., Any]:
    """Build an ``Expr`` operator that refuses *action* with a task-named error.

    Parameters
    ----------
    action : str
        What the operator does, completing "cannot be ...".

    Returns
    -------
    Callable[..., Any]
        A method that raises :meth:`Expr._deferred_value_error` whatever its
        operands.
    """

    def refuse(self: Expr, *operands: object) -> Any:
        raise self._deferred_value_error(action)

    refuse.__doc__ = f"Refuse being {action}: the value does not exist until the task runs."
    return refuse


@dataclass(frozen=True)
class Expr(Generic[T]):
    """An opaque node representing a deferred computation.

    A call is not the tuple its task's return annotation describes, so
    unpacking, indexing, and ``len()`` all refuse, and point at :attr:`output`
    instead.

    Parameters
    ----------
    task_def : TaskDef
        The task definition that produced this expression.
    args : dict[str, object]
        Mapping of parameter names to argument values.  Values may be concrete
        Python objects or nested ``Expr`` / ``ExprList`` instances that must be
        resolved before this task can execute.
    """

    task_def: TaskDef
    args: dict[str, object] = field(default_factory=dict)
    mapped: bool = False
    display_label_parts: tuple[str, ...] = field(default_factory=tuple, repr=False)
    concurrency_group: str | None = field(default=None, repr=False)
    concurrency_group_limit: int | None = field(default=None, repr=False)

    @property
    def output(self) -> _OutputProxy:
        """Return a proxy for indexing into this expression's tuple result."""
        return _OutputProxy(self)

    @property
    def display_label(self) -> str:
        """Return the label under which this call is reported to the user.

        Built from ``display_label_parts``, which fan-out fixes at
        graph-build time, so the label is the same before dispatch as
        after it.
        """
        base_name = self.task_def.base_name
        if self.display_label_parts:
            return f"{base_name}[{','.join(self.display_label_parts)}]"
        return base_name

    def __iter__(self) -> Iterator[Any]:
        """Refuse iteration — and unpacking, which is iteration — with advice."""
        raise self._deferred_result_error("unpacked or iterated")

    def __getitem__(self, index: object) -> Any:
        """Refuse subscripting, pointing at ``.output`` instead."""
        raise self._deferred_result_error("indexed")

    def __len__(self) -> int:
        """Refuse ``len()``: the number of outputs is not known until the run."""
        raise self._deferred_result_error("measured with len()")

    def __bool__(self) -> bool:
        """Report a call as truthy.

        Truthiness falls back to :meth:`__len__` when a type defines no
        ``__bool__``, and that one raises. A constructed call is a real object,
        so ``if expr:`` must stay true rather than inherit the refusal.
        """
        return True

    def __eq__(self, other: object) -> bool:
        """Compare two calls by identity; refuse comparing a call with a value.

        A call compared with a value would otherwise be silently unequal, so
        ``if expr == 5:`` would always take its ``else`` branch. Two calls are
        equal only when they are the same object: comparing their arguments
        would compare any nested call with a plain value, and so raise.
        """
        if not isinstance(other, Expr):
            raise self._deferred_value_error("compared")
        return self is other

    def __format__(self, format_spec: str) -> str:
        """Render as ``str()`` does; refuse a format spec, which needs the value."""
        if format_spec:
            raise self._deferred_value_error("formatted")
        return str(self)

    def __getattr__(self, name: str) -> Any:
        """Refuse a missing attribute, explaining the ones users reach for.

        Only reached when normal lookup fails. ``.map()`` belongs to a call
        that leaves its varying arguments unset, and an ``Out[...]``
        parameter's value is selected through :attr:`output`. Underscore names
        are refused plainly: copy, pickle and dataclass internals probe them,
        sometimes on an instance whose fields are not yet set.
        """
        task_def = self.__dict__.get("task_def")
        if name.startswith("_") or task_def is None:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        fn_name = task_def.fn.__name__
        if name in ("map", "product_map"):
            raise AttributeError(
                f"{fn_name}(...) was given every required argument, so it is one call rather "
                f"than a fan-out, and has no .{name}(). To fan out, leave the varying "
                f"parameters unset on the call and pass them to .{name}(): "
                f"{fn_name}(fixed=...).{name}(varying=[...])"
            )
        if name in task_def.output_params:
            raise AttributeError(
                f"{fn_name}() returns one deferred result, which has no attribute {name!r}. "
                f"{name!r} is one of its Out[...] parameters: select it with "
                f"r.output[{name!r}]"
            )
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def _deferred_result_error(self, action: str, *, remedy: str | None = None) -> TypeError:
        """Build the error explaining why *action* cannot work on a deferred call.

        Parameters
        ----------
        action : str
            What the user tried to do, as a past participle that completes
            "cannot be ...".
        remedy : str | None
            The sentence telling the user what to do instead. Defaults to
            selecting the call's outputs by position.

        Returns
        -------
        TypeError
            The error to raise. A ``TypeError`` because that is what Python's
            unpacking, subscripting, ``len()``, comparison, and arithmetic
            protocols promise, and because the CLI reports the user's own line
            for it.
        """
        # The task's own function name, not TaskDef.name, which carries the
        # hashed synthetic module prefix of the loaded workflow.
        name = self.task_def.fn.__name__
        if remedy is None:
            remedy = (
                "Select its outputs by position instead: "
                f"r = {name}(...); a, b = r.output[0], r.output[1]"
            )
        return TypeError(
            f"{name}() returns one deferred result, which cannot be {action} while the "
            f"flow is being built. {remedy}"
        )

    def _deferred_value_error(self, action: str) -> TypeError:
        """Build the error for an operation that needs the call's actual value."""
        return self._deferred_result_error(
            action,
            remedy=(
                "Its value exists only once the task has run: pass the call to another "
                "task and do the work there."
            ),
        )

    # Operators that need the value itself. Without these a comparison or
    # conversion would raise Python's bare "not supported for 'Expr'" error.
    __lt__ = __le__ = __gt__ = __ge__ = _refused_value_operation("compared")
    __int__ = __float__ = __complex__ = __index__ = __round__ = _refused_value_operation(
        "converted to a number"
    )
    _refuse_arithmetic = _refused_value_operation("used in arithmetic")
    __add__ = __radd__ = __sub__ = __rsub__ = __mul__ = __rmul__ = _refuse_arithmetic
    __truediv__ = __rtruediv__ = __floordiv__ = __rfloordiv__ = _refuse_arithmetic
    __mod__ = __rmod__ = __divmod__ = __rdivmod__ = __pow__ = __rpow__ = _refuse_arithmetic
    __matmul__ = __rmatmul__ = __neg__ = __pos__ = __abs__ = _refuse_arithmetic
    __and__ = __rand__ = __or__ = __ror__ = __xor__ = __rxor__ = _refuse_arithmetic
    __lshift__ = __rlshift__ = __rshift__ = __rrshift__ = __invert__ = _refuse_arithmetic
    del _refuse_arithmetic

    def __repr__(self) -> str:
        arg_strs = []
        for k, v in self.args.items():
            if isinstance(v, (Expr, ExprList)):
                arg_strs.append(f"{k}=<{type(v).__name__}>")
            else:
                arg_strs.append(f"{k}={v!r}")
        joined = ", ".join(arg_strs)
        return f"Expr({self.task_def.base_name}({joined}))"


@dataclass(frozen=True)
class OutputIndex:
    """Deferred index into a tuple-returning expression.

    Created by ``expr.output[i]``.  The evaluator resolves the upstream
    ``Expr`` and then indexes into the concrete result.

    Parameters
    ----------
    expr : Expr
        The upstream expression whose result is a tuple.
    index : int
        The positional index into the result tuple.
    """

    expr: Expr
    index: int

    def __repr__(self) -> str:
        return f"OutputIndex({self.expr!r}, {self.index})"


@dataclass(frozen=True)
class OutputName:
    """Deferred named selection into an ``Out[...]`` parameter's value.

    Created by ``expr.output["name"]``. Unlike ``OutputIndex``, which selects
    from the task's *return* value, this resolves to the named ``Out[...]``
    parameter's own resolved argument (coerced to ``file``/``folder``) — so it
    works whether that task's return is explicit or inferred.

    Parameters
    ----------
    expr : Expr
        The upstream expression whose task declares an ``Out[...]``
        parameter named *name*.
    name : str
        The ``Out[...]`` parameter's name.
    """

    expr: Expr
    name: str

    def __repr__(self) -> str:
        return f"OutputName({self.expr!r}, {self.name!r})"


class _OutputProxy:
    """Proxy returned by ``Expr.output`` and ``ExprList.output``.

    Supports ``__getitem__`` to create deferred index or named selections
    into a task's outputs.
    """

    def __init__(self, source: Expr | ExprList) -> None:
        self._source = source

    def __getitem__(self, index: int | str) -> OutputIndex | OutputName | ExprList:
        """Select element *index*, or ``Out[...]`` parameter *index*, per result.

        Parameters
        ----------
        index : int | str
            A positional index into the result tuple, or the name of one of
            the task's ``Out[...]`` parameters.

        Returns
        -------
        OutputIndex | OutputName
            When the source is a single ``Expr``.
        ExprList
            When the source is an ``ExprList``, returns a new ``ExprList``
            whose elements are ``OutputIndex``/``OutputName`` wrappers.
        """
        if isinstance(index, str):
            task_def = self._source.task_def
            if task_def is not None:
                _validate_output_name(task_def=task_def, name=index)
            if isinstance(self._source, Expr):
                return OutputName(expr=self._source, name=index)
            return ExprList(
                exprs=[OutputName(expr=e, name=index) for e in self._source],
                task_def=self._source.task_def,
            )

        task_def = self._source.task_def
        if task_def is not None:
            _validate_output_index(task_def=task_def, index=index)
        if isinstance(self._source, Expr):
            return OutputIndex(expr=self._source, index=index)

        # ExprList — wrap each constituent Expr.
        return ExprList(
            exprs=[OutputIndex(expr=e, index=index) for e in self._source],
            task_def=self._source.task_def,
        )


def _validate_output_index(*, task_def: TaskDef, index: int) -> None:
    """Raise a clear error if the task's declared return holds one value.

    Indexing a single ``str`` or path result would otherwise select one of
    its characters at run time, and the run would succeed with it.
    """
    from pathlib import Path

    from ginkgo.core.types import file, folder

    annotation = task_def.effective_return_annotation
    members = get_args(annotation) if isinstance(annotation, UnionType) else (annotation,)
    single_values = {file, folder, Path, str, int, float, bool, None, type(None)}
    if not members or not all(member in single_values for member in members):
        return
    raise TypeError(
        f"{task_def.fn.__name__}() returns a single value, so it has no output [{index}]. "
        "Pass the result itself, or select an Out[...] parameter by name with "
        '.output["name"].'
    )


def _validate_output_name(*, task_def: TaskDef, name: str) -> None:
    """Raise a clear error if *name* is not one of the task's Out[...] params.

    Validated eagerly, at flow-construction time (``.output["name"]`` itself),
    rather than deferred to the run.
    """
    if name in task_def.output_params:
        return
    known = ", ".join(sorted(task_def.output_params)) or "(none)"
    raise KeyError(
        f"{task_def.fn.__name__}() has no Out[...] parameter named {name!r}. "
        f"Its Out[...] parameters are: {known}."
    )


@dataclass(frozen=True)
class ExprList(Generic[T]):
    """A typed list of independent ``Expr[T]`` nodes produced by fan-out.

    The scheduler evaluates all elements in parallel.  When passed as an
    argument to a downstream task expecting ``list[T]``, the evaluator resolves
    all constituent expressions before executing the consumer.

    Parameters
    ----------
    exprs : list[Expr[T]]
        The individual expression nodes.
    task_def : TaskDef | None
        Optional originating task definition for empty or chained fan-out.
    """

    exprs: list[Expr[T]] = field(default_factory=list)
    task_def: TaskDef | None = field(default=None, repr=False)

    @property
    def output(self) -> _OutputProxy:
        """Return a proxy for indexing into each element's tuple result."""
        return _OutputProxy(self)

    def __len__(self) -> int:
        return len(self.exprs)

    def __getitem__(self, index: int) -> Expr[T]:
        return self.exprs[index]

    def __iter__(self):
        return iter(self.exprs)

    def map(self, *, max_concurrent: int | None = None, **varying: Any) -> ExprList[T]:
        """Extend each existing branch by zipping new varying arguments.

        Parameters
        ----------
        max_concurrent : int | None
            When set, the scheduler will run at most this many of the
            generated branches concurrently. Independent of the global
            ``--jobs`` and ``--cores`` budgets.
        **varying
            Per-branch keyword arguments.
        """
        from ginkgo.core.task import _fan_out_expr_list

        return _fan_out_expr_list(
            expr_list=self,
            varying=varying,
            mode="zip",
            max_concurrent=max_concurrent,
        )

    def product_map(self, *, max_concurrent: int | None = None, **varying: Any) -> ExprList[T]:
        """Extend each existing branch across Cartesian varying arguments.

        Parameters
        ----------
        max_concurrent : int | None
            When set, the scheduler will run at most this many of the
            generated branches concurrently.
        **varying
            Per-branch keyword arguments.
        """
        from ginkgo.core.task import _fan_out_expr_list

        return _fan_out_expr_list(
            expr_list=self,
            varying=varying,
            mode="product",
            max_concurrent=max_concurrent,
        )


@dataclass(frozen=True, kw_only=True)
class ConstructedCall:
    """A task call minted while a flow body was executing.

    Recorded so the evaluator can tell which calls the graph walk reached and
    which were constructed and then discarded.

    Parameters
    ----------
    value : Expr | ExprList | PartialCall
        The object the call handed back to the flow body.
    exprs : tuple[Expr, ...]
        The expressions the call produced — one for a plain call, one per
        branch for a fan-out. Empty only for a partial call, which produces
        none until ``.map()`` or ``.product_map()`` completes it.
    missing_params : tuple[str, ...]
        Required parameters a partial call left unsupplied; empty for a
        complete call.
    """

    value: Expr | ExprList | PartialCall
    exprs: tuple[Expr, ...]
    missing_params: tuple[str, ...] = ()

    @property
    def is_partial(self) -> bool:
        """Whether the call omitted required arguments."""
        return bool(self.missing_params)

    @property
    def task_name(self) -> str:
        """Fully qualified name of the called task."""
        if self.is_partial:
            return self.value.task_def.name
        return self.exprs[0].task_def.name

    @property
    def label(self) -> str:
        """Short human-readable form, e.g. ``analyse()`` or ``analyse() × 4``."""
        base = f"{self.task_name.rsplit('.', 1)[-1]}()"
        return base if len(self.exprs) <= 1 else f"{base} × {len(self.exprs)}"


_active_call_log: ContextVar[list[ConstructedCall] | None] = ContextVar(
    "ginkgo_construction_log",
    default=None,
)


@contextmanager
def record_constructed_calls() -> Iterator[list[ConstructedCall]]:
    """Record every task call constructed inside the block.

    Recording is inactive outside this context manager, so expressions built by
    library users or minted inside running tasks cost nothing.

    Yields
    ------
    list[ConstructedCall]
        The log, filled as the block executes.
    """
    log: list[ConstructedCall] = []
    token = _active_call_log.set(log)
    try:
        yield log
    finally:
        _active_call_log.reset(token)


def record_call(value: Expr | ExprList) -> None:
    """Append a constructed task call to the active log, if any."""
    log = _active_call_log.get()
    if log is None:
        return
    exprs = (value,) if isinstance(value, Expr) else tuple(value)
    # A fan-out over an empty sequence produces no branches, so there is no
    # call to drop and nothing to report.
    if not exprs:
        return
    log.append(ConstructedCall(value=value, exprs=exprs))


def record_partial_call(partial_call: PartialCall) -> None:
    """Append a call that omitted required arguments to the active log, if any.

    It enters the graph only through the fan-out that completes it, which
    supersedes this entry, so one still logged once the flow body has run was
    discarded.
    """
    log = _active_call_log.get()
    if log is None:
        return
    log.append(
        ConstructedCall(
            value=partial_call,
            exprs=(),
            missing_params=partial_call.missing_params,
        )
    )


def supersede_call(value: Expr | ExprList | PartialCall) -> None:
    """Drop a logged call that a fan-out has replaced.

    ``ExprList.map`` rebuilds every branch, so the expressions of the list it
    was called on are legitimately unreachable and must not be reported. A
    partial call completed by ``.map()`` or ``.product_map()`` is likewise
    replaced by the branches it produced.
    """
    log = _active_call_log.get()
    if log is None:
        return
    for index, call in enumerate(log):
        if call.value is value:
            del log[index]
            return
