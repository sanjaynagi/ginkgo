"""Detection of paths that cross task boundaries without a dependency edge.

A path handed to a task as a literal ``str`` is a value with no provenance:
it contributes only its own characters to the consumer's cache key, and it
creates no edge in the graph. Two defects follow from that one fact, and this
module holds what both need to decide.

Where a path is written by one task and read by another with no ordering
between them, the two run concurrently and the reader can observe a
half-written or missing file (#280). Where a path is not written by any task
in the graph, it is an untracked input: editing it on disk does not
invalidate the consumer, which then serves a stale result forever (#281).

Two checks divide the work by what they can know and when:

- :func:`shared_literal_path_findings` runs before anything executes. It needs
  no filesystem and no run: where one literal path is an argument to two nodes
  with no dependency path between them, that is unordered concurrent access to
  one path, whichever node writes. Direction does not have to be known, which
  is what makes the check cheap and sound. It cannot see a path a task
  computes inside its own body.
- The evaluator's end-of-run pass sees resolved values and the set of paths
  tasks actually produced, so it decides the cases this module's static check
  cannot reach. It shares :func:`ancestor_ids` with it, so both answer
  "is there a dependency path between these two nodes?" the same way.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import PurePath
from typing import Any

from ginkgo.core.expr import Expr, ExprList, OutputIndex
from ginkgo.core.remote import is_remote_uri


# Longer than any real path, and long enough that a rehydrated text asset's
# contents cannot be mistaken for one.
_MAX_PATH_LEN = 4096


def looks_like_path_string(value: Any) -> bool:
    """Return whether a value is a string shaped like a filesystem path.

    Distinguishes a path from an ordinary string argument without touching
    the filesystem, so a check can run before anything has been written.
    ``is_path_like`` cannot serve here: it is true of every ``str``, which
    would make a shared sample label look like a shared path.

    A separator settles it. Failing that, a suffix does, unless the suffix is
    numeric — ``"2.1"`` is a version, not a file.

    Parameters
    ----------
    value : Any
        The candidate argument value.

    Returns
    -------
    bool
        ``True`` when the value is shaped like a path.
    """
    if not isinstance(value, (str, PurePath, os.PathLike)):
        return False
    text = str(value)
    if not text or len(text) > _MAX_PATH_LEN:
        return False
    if is_remote_uri(text):
        return False
    if "/" in text or os.sep in text:
        return True
    suffix = PurePath(text).suffix
    return len(suffix) > 1 and not suffix[1:].isdigit()


def ancestor_ids(nodes: Mapping[int, Any]) -> dict[int, frozenset[int]]:
    """Return every node's transitive dependencies.

    Reachability, not adjacency, is what says whether two nodes are ordered.
    The starter template proves why: ``write_summary`` reads seed paths its
    ``write_seed_card`` nodes wrote, and the two are connected only through
    the normalize/brief/package chain between them. A direct-edge test would
    call that a race.

    Parameters
    ----------
    nodes : Mapping[int, Any]
        Graph nodes by id, each carrying ``dependency_ids``.

    Returns
    -------
    dict[int, frozenset[int]]
        Every node id mapped to the ids it transitively depends on.
    """
    resolved: dict[int, frozenset[int]] = {}

    def walk(node_id: int, seen: frozenset[int]) -> frozenset[int]:
        cached = resolved.get(node_id)
        if cached is not None:
            return cached
        # A cycle is rejected elsewhere, at build time; guard anyway so this
        # helper cannot be the thing that hangs.
        if node_id in seen:
            return frozenset()
        node = nodes.get(node_id)
        if node is None:
            return frozenset()
        found: set[int] = set()
        for dependency_id in node.dependency_ids:
            found.add(dependency_id)
            found |= walk(dependency_id, seen | {node_id})
        answer = frozenset(found)
        resolved[node_id] = answer
        return answer

    return {node_id: walk(node_id, frozenset()) for node_id in nodes}


def are_ordered(*, ancestors: Mapping[int, frozenset[int]], left: int, right: int) -> bool:
    """Return whether a dependency path runs between two nodes, either way."""
    return right in ancestors.get(left, frozenset()) or left in ancestors.get(right, frozenset())


def literal_path_arguments(value: Any) -> Iterator[str]:
    """Yield every path-shaped literal reachable from an unresolved argument.

    Deferred expressions are skipped rather than descended into for their own
    sake: a value that arrives through an ``Expr`` carries a dependency edge
    already, which is the thing whose absence this module looks for. An
    ``ExprList`` is walked because a fan-out list mixes literals and
    expressions element by element.

    Parameters
    ----------
    value : Any
        One unresolved argument from ``Expr.args``.

    Yields
    ------
    str
        Each path-shaped literal, at any container depth.
    """
    if isinstance(value, (Expr, OutputIndex)):
        return
    if isinstance(value, ExprList):
        for item in value:
            yield from literal_path_arguments(item)
        return
    if isinstance(value, (list, tuple, set, frozenset)):
        for item in value:
            yield from literal_path_arguments(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from literal_path_arguments(item)
        return
    if looks_like_path_string(value):
        yield str(value)


@dataclass(frozen=True, kw_only=True)
class SharedPathFinding:
    """One literal path two unordered tasks both take as an argument.

    Parameters
    ----------
    path : str
        The shared literal path, as written at both call sites.
    first_task : str
        Fully-qualified name of one of the two tasks.
    first_parameter : str
        The parameter carrying the path in ``first_task``.
    second_task : str
        Fully-qualified name of the other task.
    second_parameter : str
        The parameter carrying the path in ``second_task``.
    """

    path: str
    first_task: str
    first_parameter: str
    second_task: str
    second_parameter: str

    def message(self) -> str:
        """Return the finding phrased so a reader can see why it matters."""
        return (
            f"{self.first_task.rsplit('.', 1)[-1]}.{self.first_parameter} and "
            f"{self.second_task.rsplit('.', 1)[-1]}.{self.second_parameter} are both "
            f"given the path {self.path!r}, and nothing orders the two tasks, so they "
            "run at the same time. Whichever one writes, the other can read the file "
            "half-written or missing, and the result it returns is cached as if it "
            "were correct."
        )

    def suggestion(self) -> str:
        """Return the fix, in terms of the value that would carry the edge."""
        return (
            f"Have the task that writes {self.path!r} return it as `-> file`, and pass "
            "that returned value to the other task instead of the path string. The "
            "edge that ordering needs comes from the value, not from the path."
        )


def shared_literal_path_findings(nodes: Mapping[int, Any]) -> list[SharedPathFinding]:
    """Report every literal path two unordered nodes both receive.

    Statically decidable: no filesystem access, no execution, no ledger. It
    catches the shape the starter template makes idiomatic — an output path
    passed to the producer as a literal argument, and the same literal passed
    to a consumer — and cannot catch a path a task computes in its own body,
    which the evaluator's end-of-run pass covers instead.

    Parameters
    ----------
    nodes : Mapping[int, Any]
        Graph nodes by id, each carrying ``expr``, ``task_def`` and
        ``dependency_ids``.

    Returns
    -------
    list[SharedPathFinding]
        One finding per unordered pair of nodes sharing a path, ordered by
        path then task name so output is stable.
    """
    sites: dict[str, dict[int, str]] = {}
    for node_id, node in nodes.items():
        for parameter, argument in node.expr.args.items():
            for path in literal_path_arguments(argument):
                # First parameter wins for a node that takes one path twice;
                # the pair is what matters, not which slot named it.
                sites.setdefault(path, {}).setdefault(node_id, parameter)

    ancestors = ancestor_ids(nodes)
    findings: list[SharedPathFinding] = []
    for path, by_node in sites.items():
        if len(by_node) < 2:
            continue
        node_ids = sorted(by_node)
        for index, left in enumerate(node_ids):
            for right in node_ids[index + 1 :]:
                if are_ordered(ancestors=ancestors, left=left, right=right):
                    continue
                findings.append(
                    SharedPathFinding(
                        path=path,
                        first_task=nodes[left].task_def.name,
                        first_parameter=by_node[left],
                        second_task=nodes[right].task_def.name,
                        second_parameter=by_node[right],
                    )
                )

    return sorted(findings, key=lambda f: (f.path, f.first_task, f.second_task))


__all__ = [
    "SharedPathFinding",
    "ancestor_ids",
    "are_ordered",
    "literal_path_arguments",
    "looks_like_path_string",
    "shared_literal_path_findings",
]
