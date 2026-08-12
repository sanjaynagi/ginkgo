"""Core expression tree nodes for lazy workflow evaluation.

Every ``@task``-decorated function, when called, returns an ``Expr[T]`` rather
than executing. The evaluator recursively resolves these nodes.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from ginkgo.core.task import TaskDef

T = TypeVar("T")


@dataclass(frozen=True)
class Expr(Generic[T]):
    """An opaque node representing a deferred computation.

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

    def __repr__(self) -> str:
        arg_strs = []
        for k, v in self.args.items():
            if isinstance(v, (Expr, ExprList)):
                arg_strs.append(f"{k}=<{type(v).__name__}>")
            else:
                arg_strs.append(f"{k}={v!r}")
        joined = ", ".join(arg_strs)
        return f"Expr({self.task_def.name}({joined}))"


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


class _OutputProxy:
    """Proxy returned by ``Expr.output`` and ``ExprList.output``.

    Supports ``__getitem__`` to create deferred index selections into
    tuple-returning task results.
    """

    def __init__(self, source: Expr | ExprList) -> None:
        self._source = source

    def __getitem__(self, index: int) -> OutputIndex | ExprList:
        """Select element *index* from each tuple result.

        Parameters
        ----------
        index : int
            Positional index into the result tuple.

        Returns
        -------
        OutputIndex
            When the source is a single ``Expr``.
        ExprList
            When the source is an ``ExprList``, returns a new ``ExprList``
            whose elements are ``OutputIndex`` wrappers.
        """
        if isinstance(self._source, Expr):
            return OutputIndex(expr=self._source, index=index)

        # ExprList — wrap each constituent Expr.
        return ExprList(
            exprs=[OutputIndex(expr=e, index=index) for e in self._source],
            task_def=self._source.task_def,
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
    value : Expr | ExprList
        The object the call handed back to the flow body.
    exprs : tuple[Expr, ...]
        The expressions the call produced — one for a plain call, one per
        branch for a fan-out. Never empty.
    """

    value: Expr | ExprList
    exprs: tuple[Expr, ...]

    @property
    def task_name(self) -> str:
        """Fully qualified name of the called task."""
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


def supersede_call(value: Expr | ExprList) -> None:
    """Drop a logged call that a chained fan-out has replaced.

    ``ExprList.map`` rebuilds every branch, so the expressions of the list it
    was called on are legitimately unreachable and must not be reported.
    """
    log = _active_call_log.get()
    if log is None:
        return
    for index, call in enumerate(log):
        if call.value is value:
            del log[index]
            return
