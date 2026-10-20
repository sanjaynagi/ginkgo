"""Advisory notices about how a task is declared or how it ran.

Each function returns the notice text, or nothing, and never changes what
runs. The evaluator emits what they return as ``TaskNotice`` events.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ginkgo.core.expr import Expr, ExprList, OutputIndex, OutputName
from ginkgo.core.task import TaskDef
from ginkgo.runtime.task_validation import is_untracked_directory_value


def untracked_directory_notices(
    *,
    task_def: TaskDef,
    args: Mapping[str, Any],
    resolved_args: Mapping[str, Any],
    warned: set[tuple[str, str, str]],
) -> list[str]:
    """Warn when a directory crosses a task boundary without content tracking.

    Fires only for arguments resolved from an upstream expression in the
    graph: those are the ones where the producer can rewrite the directory's
    contents while the consumer's cache key, built from the path string
    alone, stays put. A same-shaped upstream *file* path is content-hashed,
    so it is not warned about. Containers are walked in step with their
    resolved counterparts, so a path arriving inside ``inputs=[a, b]`` is
    checked as one passed directly.

    Parameters
    ----------
    task_def : TaskDef
        The consuming task.
    args : Mapping[str, Any]
        The task's unresolved arguments, as its expression holds them.
    resolved_args : Mapping[str, Any]
        The same arguments, resolved.
    warned : set[tuple[str, str, str]]
        ``(producer, consumer, parameter)`` already warned about in this run.
        Each new warning is added, so fan-out branches report once. Checked
        before the filesystem probe, so a fan-out costs one stat.

    Returns
    -------
    list[str]
        One notice per newly warned producer and parameter.
    """
    notices: list[str] = []
    for name, unresolved in args.items():
        annotation = task_def.type_hints.get(name)
        for producer, resolved in _upstream_values(unresolved, resolved_args.get(name)):
            warning_key = (producer, task_def.name, name)
            if warning_key in warned:
                continue
            if not is_untracked_directory_value(annotation=annotation, value=resolved):
                continue
            warned.add(warning_key)
            producer_base = producer.rsplit(".", 1)[-1]
            notices.append(
                f"{producer_base} returns a path to a directory as 'str', so '{name}' "
                "is cached on the path only and content changes will not invalidate this "
                f"task. Annotate {producer_base}'s return '-> folder' and "
                f"'{name}: folder'."
            )
    return notices


def access_fallback_notice(access_stats: Mapping[str, Any]) -> str | None:
    """Return a notice when a requested FUSE mount fell back to staging.

    ``access_stats["fallback_reason"]`` is set by
    :class:`~ginkgo.remote.access.mounted.MountedAccess` and the worker
    hydration layer when a mount could not be established (missing driver,
    no ``/dev/fuse``, permission denied). Without the notice, a task that
    declared ``access="fuse"`` would silently pay staging costs.
    """
    reason = access_stats.get("fallback_reason")
    if not reason:
        return None
    return f"FUSE access fell back to staging: {reason}"


def written_str_inputs_notice(written: Sequence[tuple[str, str]]) -> str | None:
    """Return a notice when a task wrote files it received as plain ``str`` paths.

    Such a path is content-hashed as an input, so the task invalidates its own
    cache entry and re-runs on every run until it is re-annotated.

    Parameters
    ----------
    written : Sequence[tuple[str, str]]
        ``(parameter, path)`` for each such path the task wrote.
    """
    if not written:
        return None
    paths = ", ".join(f"{path} (parameter '{parameter}')" for parameter, path in written)
    parameters = ", ".join(sorted({f"'{parameter}'" for parameter, _ in written}))
    return (
        f"wrote {paths}, which it received as a plain `str` path, so the "
        "file is tracked as an input and this task will re-run every time. "
        f"Annotate {parameters} `Out[file]` if the task writes it, or "
        "`untracked` to key it by its path only."
    )


def _upstream_values(unresolved: Any, resolved: Any) -> list[tuple[str, Any]]:
    """Pair each upstream expression in one argument with its resolved value.

    Returns ``(producer task name, resolved value)`` for every expression the
    argument carries, at any depth of list, tuple or dict.
    """
    if isinstance(unresolved, ExprList) and isinstance(resolved, list | tuple):
        unresolved = list(unresolved)

    if isinstance(unresolved, list | tuple) and isinstance(resolved, list | tuple):
        return [
            pair
            for item, item_resolved in zip(unresolved, resolved)
            for pair in _upstream_values(item, item_resolved)
        ]

    if isinstance(unresolved, dict) and isinstance(resolved, dict):
        # A key that is itself an expression resolves to a different key, so
        # its value cannot be paired up.
        return [
            pair
            for key, item in unresolved.items()
            if key in resolved
            for pair in _upstream_values(item, resolved[key])
        ]

    producer = _producer_task_name(unresolved)
    return [] if producer is None else [(producer, resolved)]


def _producer_task_name(value: Any) -> str | None:
    """Return the name of the task an unresolved argument came from, if any."""
    if isinstance(value, (OutputIndex, OutputName)):
        return _producer_task_name(value.expr)
    if isinstance(value, Expr):
        return value.task_def.name
    return None
