"""The ``@flow`` decorator for marking pipeline entry points.

A ``@flow``-decorated function is the entry point for a workflow.  When called,
it executes its body to build the expression tree, then returns the resulting
``Expr`` or ``ExprList``.  No task execution happens during this phase.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class FlowDef:
    """A wrapper around a flow function.

    Calling a ``FlowDef`` executes the flow body (building the expression tree)
    and returns whatever the flow function returns.

    Parameters
    ----------
    fn : Callable
        The original flow function.
    """

    fn: Callable[..., Any]

    @property
    def name(self) -> str:
        """Fully qualified name of the wrapped function."""
        module = getattr(self.fn, "__module__", None) or ""
        return f"{module}.{self.fn.__qualname__}"

    def __call__(self, **kwargs: Any) -> Any:
        """Execute the flow body to build the expression tree.

        Parameters
        ----------
        **kwargs
            Arguments passed to the flow function.

        Returns
        -------
        Any
            The expression tree (typically ``Expr`` or ``ExprList``) built by
            the flow body.
        """
        return self.fn(**kwargs)


def flow(fn: Callable[..., Any]) -> FlowDef:
    """Decorator that marks a function as a flow (pipeline entry point).

    Unlike ``@task()``, ``@flow`` is used without parentheses.

    Parameters
    ----------
    fn : Callable
        The flow function.

    Returns
    -------
    FlowDef
    """
    return FlowDef(fn=fn)
