"""Execution directive base type."""


class ExecutionDirective:
    """A value a task body returns to request further execution.

    Each concrete directive type carries the parameters the evaluator needs
    to dispatch the appropriate runner. Core's own directive types
    (ShellDirective, ScriptDirective, SubWorkflowDirective) subclass this, as
    do those a task-kind plugin brings.
    """
