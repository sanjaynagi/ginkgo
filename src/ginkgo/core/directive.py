"""Execution directive base type."""

from ginkgo.errors import GinkgoError


class ExecutionDirective:
    """A value a task body returns to request further execution.

    Each concrete directive type carries the parameters the evaluator needs
    to dispatch the appropriate runner. Core's own directive types
    (ShellDirective, ScriptDirective, SubWorkflowDirective) subclass this, as
    do those a task-kind plugin brings.
    """


class DirectiveError(GinkgoError, ValueError):
    """A directive request that is wrong whatever state the run is in.

    Such as a file type the task's kind cannot run. Unlike a missing file, which
    an upstream task may yet write, it fails every run, so ``ginkgo run
    --dry-run`` reports it when it predicts the directive instead of treating
    the task as unknown.
    """
