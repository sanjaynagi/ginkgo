"""Core expression tree nodes for lazy workflow evaluation.

Every ``@task``-decorated function, when called, returns an ``Expr[T]`` rather
than executing. The evaluator recursively resolves these nodes.
"""

from __future__ import annotations

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
