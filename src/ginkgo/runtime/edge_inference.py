"""Infer dependency edges between task nodes from ``Out[...]`` paths.

Phase 2 of issue #307. A literal path string passed between two tasks — one
that writes it (``Out[file]``/``Out[folder]``), another that reads it under
any annotation — creates no dependency edge today, so the two tasks can run
concurrently in the same wave: the consumer may read a missing or
half-written file, and the wrong result gets cached permanently (#280).

This module is pure and stateless: it only inspects literal argument values
already attached to a node's :class:`~ginkgo.core.expr.Expr` (never resolves
anything, never touches the filesystem beyond normalising a path string), and
returns the edges to add. Deciding what to do with those edges — merging them
into a node's dependency ids, detecting cycles, handling nodes already in
flight during dynamic graph expansion — is the caller's job (see
``ConcurrentEvaluator._infer_and_apply_edges`` in ``evaluator.py``).

Only *literal* path values are considered. A path computed at runtime from an
upstream value already carries an edge through the normal ``Expr`` graph; a
path that will only be known once some other task has run (an unresolved
``Expr``/``OutputIndex``/``OutputName``) cannot be matched yet and is simply
skipped — it is not a bug, just not yet knowable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, get_origin

from ginkgo.core.remote import is_remote_uri
from ginkgo.core.task import TaskDef
from ginkgo.core.types import (
    annotation_includes,
    file,
    folder,
    is_path_like,
    pair_elements_with_annotations,
    is_untracked_annotation,
    tmp_dir,
    untracked,
    unwrap_optional_annotation,
)
from ginkgo.errors import GinkgoError
from ginkgo.runtime.task_validation import contains_dynamic_expression


def normalize_path(path: str) -> str:
    """Return *path* normalised the way the runtime resolves relative paths.

    Relative to the process's current working directory, matching how path
    arguments are otherwise interpreted at execution time.
    """
    return os.path.normpath(os.path.abspath(path))


@dataclass(frozen=True)
class ProducedPath:
    """One literal path a node's ``Out[...]`` parameter declares it writes."""

    node_id: int
    task_name: str
    param: str
    path: str
    kind: str  # "file" | "folder"


@dataclass(frozen=True)
class ConsumedPath:
    """One literal path-like value a node's (non-output) parameter receives."""

    node_id: int
    task_name: str
    param: str
    path: str
    kind: str | None  # "file" | "folder" | None (untyped)


class DuplicateOutputPathError(GinkgoError, ValueError):
    """Raised when two different nodes declare overlapping ``Out[...]`` paths."""

    def __init__(self, *, first: ProducedPath, second: ProducedPath) -> None:
        self.first = first
        self.second = second
        if first.path == second.path:
            detail = f"both declare {first.path!r} as `Out[...]`"
        else:
            detail = (
                f"{first.task_name!r}'s `Out[...]` path {first.path!r} and "
                f"{second.task_name!r}'s `Out[...]` path {second.path!r} overlap "
                "(one is inside the other's declared Out[folder])"
            )
        super().__init__(
            f"{first.task_name!r} (parameter {first.param!r}) and "
            f"{second.task_name!r} (parameter {second.param!r}) cannot both write "
            f"the same location: {detail}. Two tasks cannot declare the same "
            "output path."
        )


class OutputOverwritesInputError(GinkgoError, ValueError):
    """Raised when a node declares an ``Out[...]`` path over one of its own inputs.

    Running it would overwrite the input, and the cache would then record the
    damaged file as the input of a successful run.
    """

    def __init__(self, *, produced: ProducedPath, consumed: ConsumedPath) -> None:
        self.produced = produced
        self.consumed = consumed
        if produced.path == consumed.path:
            detail = f"both name {produced.path!r}"
        elif os.path.commonpath([produced.path, consumed.path]) == produced.path:
            detail = f"input {consumed.path!r} is inside `Out[...]` path {produced.path!r}"
        else:
            detail = f"`Out[...]` path {produced.path!r} is inside input {consumed.path!r}"
        super().__init__(
            f"{produced.task_name!r} (parameter {produced.param!r}) would write over its "
            f"own input (parameter {consumed.param!r}): {detail}. A task cannot declare "
            "an output path that overlaps a path it reads; write to a new path instead."
        )


def reject_self_overwrite(*, produced: list[ProducedPath], consumed: list[ConsumedPath]) -> None:
    """Refuse one node whose ``Out[...]`` paths overlap its own ``file``/``folder`` inputs.

    Overlap is the same path, or one path inside the other. Untyped consumed
    values are skipped: a plain ``str`` may be a label that happens to match a
    path, such as a sample name equal to that sample's output folder.

    Parameters
    ----------
    produced : list[ProducedPath]
        The node's literal ``Out[...]`` paths.
    consumed : list[ConsumedPath]
        The node's literal input paths.

    Raises
    ------
    OutputOverwritesInputError
        On the first overlapping pair.
    """
    for entry in consumed:
        if entry.kind is None:
            continue
        for output in produced:
            if os.path.commonpath([output.path, entry.path]) in {output.path, entry.path}:
                raise OutputOverwritesInputError(produced=output, consumed=entry)


def _ancestors(path: str):
    """Yield every proper ancestor directory of a normalised absolute *path*."""
    current = path
    while True:
        parent = os.path.dirname(current)
        if parent == current:
            return
        yield parent
        current = parent


def _walk_output_value(
    *,
    annotation: Any,
    value: Any,
    param: str,
    node_id: int,
    task_name: str,
    entries: list[ProducedPath],
) -> None:
    if value is None or contains_dynamic_expression(value):
        return

    annotation, _ = unwrap_optional_annotation(annotation)
    origin = get_origin(annotation)
    if origin in {list, tuple}:
        for item_annotation, item in pair_elements_with_annotations(
            annotation=annotation, value=value
        ):
            _walk_output_value(
                annotation=item_annotation,
                value=item,
                param=param,
                node_id=node_id,
                task_name=task_name,
                entries=entries,
            )
        return

    if isinstance(value, (list, tuple)):
        for item in value:
            _walk_output_value(
                annotation=annotation,
                value=item,
                param=param,
                node_id=node_id,
                task_name=task_name,
                entries=entries,
            )
        return

    if not is_path_like(value):
        return
    text = str(value)
    if not text or is_remote_uri(text):
        return

    kind = "file" if annotation_includes(annotation=annotation, expected=file) else "folder"
    entries.append(
        ProducedPath(
            node_id=node_id,
            task_name=task_name,
            param=param,
            path=normalize_path(text),
            kind=kind,
        )
    )


def collect_produced_paths(
    *, node_id: int, task_name: str, task_def: TaskDef, args: dict[str, Any]
) -> list[ProducedPath]:
    """Return the literal paths this node's ``Out[...]`` parameters name.

    Non-literal values (arguments still carrying an unresolved ``Expr``) are
    skipped: those paths are not known until the upstream task has run, so
    they cannot be matched against anything yet.
    """
    entries: list[ProducedPath] = []
    for name in task_def.output_params_in_order:
        if name not in args:
            continue
        annotation = task_def.type_hints.get(name)
        _walk_output_value(
            annotation=annotation,
            value=args[name],
            param=name,
            node_id=node_id,
            task_name=task_name,
            entries=entries,
        )
    return entries


def _walk_consumed_value(
    *,
    annotation: Any,
    value: Any,
    param: str,
    node_id: int,
    task_name: str,
    entries: list[ConsumedPath],
) -> None:
    if value is None or contains_dynamic_expression(value):
        return

    annotation, _ = unwrap_optional_annotation(annotation)
    origin = get_origin(annotation)
    if origin in {list, tuple}:
        for item_annotation, item in pair_elements_with_annotations(
            annotation=annotation, value=value
        ):
            _walk_consumed_value(
                annotation=item_annotation,
                value=item,
                param=param,
                node_id=node_id,
                task_name=task_name,
                entries=entries,
            )
        return

    if isinstance(value, (list, tuple)):
        for item in value:
            _walk_consumed_value(
                annotation=annotation,
                value=item,
                param=param,
                node_id=node_id,
                task_name=task_name,
                entries=entries,
            )
        return

    if isinstance(value, dict):
        for item in value.values():
            _walk_consumed_value(
                annotation=annotation,
                value=item,
                param=param,
                node_id=node_id,
                task_name=task_name,
                entries=entries,
            )
        return

    if not is_path_like(value) or isinstance(value, untracked):
        return
    text = str(value)
    if not text or is_remote_uri(text):
        return

    kind: str | None = None
    if annotation_includes(annotation=annotation, expected=file):
        kind = "file"
    elif annotation_includes(annotation=annotation, expected=folder):
        kind = "folder"

    entries.append(
        ConsumedPath(
            node_id=node_id,
            task_name=task_name,
            param=param,
            path=normalize_path(text),
            kind=kind,
        )
    )


def collect_consumed_paths(
    *, node_id: int, task_name: str, task_def: TaskDef, args: dict[str, Any]
) -> list[ConsumedPath]:
    """Return every literal path-like value a node's non-output parameters receive.

    ``tmp_dir``-annotated parameters are excluded (they name a scratch
    directory ginkgo manages itself, never another node's output), and so are
    ``untracked`` ones (the author opted that path out of tracking).
    ``Out[...]`` parameters are excluded — a node never depends on itself for
    a path it writes.
    """
    entries: list[ConsumedPath] = []
    output_params = task_def.output_params
    for name, parameter in task_def.signature.parameters.items():
        if name in output_params or name not in args:
            continue
        annotation = task_def.type_hints.get(name, parameter.annotation)
        # A scratch directory ginkgo manages, or a path the author declared
        # ``untracked`` on purpose: neither is a read of another node's output.
        if annotation is tmp_dir or is_untracked_annotation(annotation):
            continue
        _walk_consumed_value(
            annotation=annotation,
            value=args[name],
            param=name,
            node_id=node_id,
            task_name=task_name,
            entries=entries,
        )
    return entries


class PathIndex:
    """Incremental index of produced and consumed paths across a run's graph.

    Matching is done by dictionary lookups on a path and its ancestor
    directories, never by comparing every consumer with every producer, so the
    cost per path is proportional to its depth rather than to the size of the
    graph. That keeps a wide ``.map()`` fan-out (thousands of branches, each
    declaring its own ``Out[...]`` path) linear to register.
    """

    def __init__(self) -> None:
        self.produced_exact: dict[str, ProducedPath] = {}
        self._produced_folders: dict[str, ProducedPath] = {}
        self._produced_under: dict[str, list[ProducedPath]] = {}
        self._consumed_exact: dict[str, list[ConsumedPath]] = {}
        self._consumed_folders: dict[str, list[ConsumedPath]] = {}
        self._consumed_under: dict[str, list[ConsumedPath]] = {}

    def add_produced(self, entries: list[ProducedPath]) -> None:
        """Index *entries*, raising on an overlap with another node's output.

        Raises
        ------
        DuplicateOutputPathError
            When two different nodes declare the same path, or one declares a
            path inside another's ``Out[folder]``.
        """
        for entry in entries:
            existing = self.produced_exact.get(entry.path)
            if existing is not None and existing.node_id != entry.node_id:
                raise DuplicateOutputPathError(first=existing, second=entry)
            for ancestor in _ancestors(entry.path):
                outer = self.produced_exact.get(ancestor)
                if outer is not None and outer.node_id != entry.node_id:
                    raise DuplicateOutputPathError(first=outer, second=entry)
            for inner in self._produced_under.get(entry.path, ()):
                if inner.node_id != entry.node_id:
                    raise DuplicateOutputPathError(first=inner, second=entry)

            self.produced_exact.setdefault(entry.path, entry)
            if entry.kind == "folder":
                self._produced_folders.setdefault(entry.path, entry)
            for ancestor in _ancestors(entry.path):
                self._produced_under.setdefault(ancestor, []).append(entry)

    def add_consumed(self, entries: list[ConsumedPath]) -> None:
        """Index consumer *entries* so later producers can find them."""
        for entry in entries:
            self._consumed_exact.setdefault(entry.path, []).append(entry)
            if entry.kind == "folder":
                self._consumed_folders.setdefault(entry.path, []).append(entry)
            for ancestor in _ancestors(entry.path):
                self._consumed_under.setdefault(ancestor, []).append(entry)

    def producers_for(self, consumer: ConsumedPath) -> list[ProducedPath]:
        """Return the indexed producers whose ``Out[...]`` path *consumer* reads.

        A match is: the paths are identical, the consumer's path is inside a
        produced ``Out[folder]``, or the producer's path is inside the
        consumer's path and the consumer parameter is annotated ``folder``.
        A node never matches its own produced paths.
        """
        found: list[ProducedPath] = []
        exact = self.produced_exact.get(consumer.path)
        if exact is not None:
            found.append(exact)
        for ancestor in _ancestors(consumer.path):
            outer = self._produced_folders.get(ancestor)
            if outer is not None:
                found.append(outer)
        if consumer.kind == "folder":
            found.extend(self._produced_under.get(consumer.path, ()))
        return [entry for entry in found if entry.node_id != consumer.node_id]

    def consumers_for(self, producer: ProducedPath) -> list[ConsumedPath]:
        """Return the indexed consumers that read *producer*'s path (same rule)."""
        found: list[ConsumedPath] = list(self._consumed_exact.get(producer.path, ()))
        if producer.kind == "folder":
            found.extend(self._consumed_under.get(producer.path, ()))
        for ancestor in _ancestors(producer.path):
            found.extend(self._consumed_folders.get(ancestor, ()))
        return [entry for entry in found if entry.node_id != producer.node_id]
