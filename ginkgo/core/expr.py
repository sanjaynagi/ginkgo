"""Core expression tree nodes for lazy workflow evaluation.

Every ``@task``-decorated function, when called, returns an ``Expr[T]`` rather
than executing.  The evaluator (Phase 2) recursively resolves these nodes.
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

    def __len__(self) -> int:
        return len(self.exprs)

    def __getitem__(self, index: int) -> Expr[T]:
        return self.exprs[index]

    def __iter__(self):
        return iter(self.exprs)

    def map(self, **varying: Any) -> ExprList[T]:
        """Extend each existing branch by zipping new varying arguments."""
        from ginkgo.core.task import _fan_out_expr_list

        return _fan_out_expr_list(expr_list=self, varying=varying, mode="zip")

    def product_map(self, **varying: Any) -> ExprList[T]:
        """Extend each existing branch across Cartesian varying arguments."""
        from ginkgo.core.task import _fan_out_expr_list

        return _fan_out_expr_list(expr_list=self, varying=varying, mode="product")
