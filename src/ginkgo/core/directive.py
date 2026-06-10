"""Execution directive base type."""


class ExecutionDirective:
    """A value a task body returns to request further execution.

    Each concrete directive type carries the parameters the evaluator needs
    to dispatch the appropriate runner. The four built-in directive types
    (ShellDirective, NotebookDirective, ScriptDirective, SubWorkflowDirective) subclass this.
    """
