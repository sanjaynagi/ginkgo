"""Keyed grouping and joins over metadata-aware collections (issue #97).

Real workflows commonly carry sample metadata (sample, library, lane,
treatment, ...) alongside task outputs, and need to group or join those
outputs by that metadata — merge all lanes of a library, join a QC report
back onto its sample. Doing this with plain Python dictionaries means
manual index bookkeeping and no help from ginkgo's fan-out machinery.

``KeyedExprList`` pairs an ``ExprList`` with aligned metadata dictionaries so
grouping and joining read like the metadata table they describe, while still
compiling straight down to ordinary ``Expr``/``ExprList`` graph nodes: every
call built here is an ordinary :class:`~ginkgo.core.expr.Expr`, so caching,
provenance, resources, and retries all work exactly as they do for
hand-written fan-out.

This is a first, intentionally narrow slice of the design in issue #97:
keys are static, supplied by the flow author up front. Runtime-discovered
keys (grouping decided by what a task returns, not what the flow author
already knows) are deliberately out of scope here — see the issue for that
follow-up.
"""

from __future__ import annotations

from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from ginkgo.core.expr import Expr, ExprList, record_call
from ginkgo.core.task import PartialCall, TaskDef
from ginkgo.core.types import tmp_dir

T = TypeVar("T")

# A join's pairing of one left and one right expression for a shared key.
Pair = tuple[Expr[Any], Expr[Any]]

_HASHABLE_SCALAR_TYPES = (str, int, float, bool, tuple)


def keyed(
    exprs: ExprList[T] | Sequence[Expr[T]],
    keys: Sequence[Mapping[str, Hashable]],
) -> KeyedExprList[T]:
    """Pair an ``ExprList`` with metadata keys, aligned 1:1 by position.

    Parameters
    ----------
    exprs : ExprList[T] | Sequence[Expr[T]]
        The expressions to key, in the order the keys describe them.
    keys : Sequence[Mapping[str, Hashable]]
        One metadata mapping per expression. Values must be hashable scalars
        (``str``, ``int``, ``float``, ``bool``, or ``tuple``) — the kind of
        value a samplesheet column holds.

    Returns
    -------
    KeyedExprList[T]
        The keyed collection.

    Raises
    ------
    ValueError
        If ``exprs`` and ``keys`` have different lengths.
    TypeError
        If a key is not a string-keyed mapping, or a key value is not a
        hashable scalar.

    Examples
    --------
    >>> reads = filter_fastq().map(sample=["S1", "S2"], lane=["L1", "L1"])
    >>> samples = keyed(reads, [{"sample": "S1", "lane": "L1"}, {"sample": "S2", "lane": "L1"}])
    """
    expr_tuple = tuple(exprs)
    key_tuple = tuple(keys)
    if len(expr_tuple) != len(key_tuple):
        raise ValueError(
            f"keyed() received {len(expr_tuple)} expression(s) but {len(key_tuple)} key(s); "
            "they must be aligned 1:1"
        )
    validated_keys = tuple(_validate_key(key, index=i) for i, key in enumerate(key_tuple))
    return KeyedExprList(_exprs=expr_tuple, _keys=validated_keys)


def _validate_key(key: Mapping[str, Any], *, index: int) -> dict[str, Hashable]:
    """Validate one metadata mapping and return a plain, defensively-copied dict."""
    if not isinstance(key, Mapping):
        raise TypeError(f"keyed() key {index} must be a mapping, got {type(key).__name__}")
    validated: dict[str, Hashable] = {}
    for field_name, value in key.items():
        if not isinstance(field_name, str):
            raise TypeError(f"keyed() key {index} has a non-string field name: {field_name!r}")
        if isinstance(value, (Expr, ExprList)) or not isinstance(value, _HASHABLE_SCALAR_TYPES):
            raise TypeError(
                f"keyed() key {index} field {field_name!r} must be a hashable scalar "
                f"(str, int, float, bool, or tuple), got {type(value).__name__}"
            )
        validated[field_name] = value
    return validated


def _resolve_task(
    task_partial: TaskDef | PartialCall,
    fixed: dict[str, Any],
) -> tuple[TaskDef, dict[str, Any]]:
    """Return the task definition and combined fixed arguments for a call."""
    if isinstance(task_partial, PartialCall):
        return task_partial.task_def, {**task_partial.fixed_args, **fixed}
    if isinstance(task_partial, TaskDef):
        return task_partial, dict(fixed)
    raise TypeError(
        "expected a task (TaskDef) or a partially-applied call (PartialCall), got "
        f"{type(task_partial).__name__}"
    )


def _label_parts_for_key(key: Mapping[str, Hashable]) -> tuple[str, ...]:
    """Build ``Expr.display_label_parts`` from a metadata key, e.g. ``sample=S1``."""
    return tuple(f"{field_name}={value}" for field_name, value in key.items())


def _construct_expr(
    *,
    task_def: TaskDef,
    call_args: dict[str, Any],
    display_label_parts: tuple[str, ...],
) -> Expr:
    """Build one ``Expr`` for a keyed call, validating arguments as ``TaskDef.__call__`` does."""
    valid_params = set(task_def.all_params)
    unknown = set(call_args) - valid_params
    if unknown:
        raise TypeError(
            f"{task_def.fn.__name__}() got unexpected keyword arguments: "
            f"{', '.join(sorted(unknown))}"
        )
    managed = {name for name, annotation in task_def.type_hints.items() if annotation is tmp_dir}
    supplied_managed = set(call_args) & managed
    if supplied_managed:
        raise TypeError(
            f"{task_def.fn.__name__}() arguments are auto-managed by ginkgo: "
            f"{', '.join(sorted(supplied_managed))}"
        )
    missing = task_def.required_params - set(call_args)
    if missing:
        raise TypeError(
            f"{task_def.fn.__name__}() missing required arguments: {', '.join(sorted(missing))}"
        )
    return Expr(
        task_def=task_def, args=call_args, mapped=True, display_label_parts=display_label_parts
    )


def _merge_key_into_call_args(
    *,
    call_args: dict[str, Any],
    key: Mapping[str, Hashable],
    task_def: TaskDef,
) -> None:
    """Fill in a task's own declared parameters from a group/element's key.

    A fixed argument the caller supplied explicitly always wins over a value
    implied by the key, so a key field can be overridden per call.
    """
    valid_params = set(task_def.all_params)
    for field_name, value in key.items():
        if field_name in valid_params and field_name not in call_args:
            call_args[field_name] = value


@dataclass(frozen=True)
class _Group:
    """One group produced by :meth:`KeyedExprList.group_by`.

    Parameters
    ----------
    key : dict[str, Hashable]
        The group's key, one value per grouped field.
    members : tuple[Expr, ...]
        The group's members, in first-appearance order.
    """

    key: dict[str, Hashable]
    members: tuple[Expr, ...]


@dataclass(frozen=True)
class KeyedGroups:
    """The result of :meth:`KeyedExprList.group_by`: groups in first-appearance order.

    Parameters
    ----------
    fields : tuple[str, ...]
        The fields the collection was grouped by.
    groups : tuple[_Group, ...]
        One entry per distinct combination of ``fields``, in the order that
        combination first appeared in the source collection. Member order
        within a group follows the source collection's order too.
    """

    fields: tuple[str, ...]
    groups: tuple[_Group, ...]

    def __len__(self) -> int:
        return len(self.groups)

    def __iter__(self):
        return iter(self.groups)

    @property
    def keys(self) -> list[dict[str, Hashable]]:
        """Return each group's key, in group order."""
        return [dict(group.key) for group in self.groups]

    def map_groups(
        self,
        task_partial: TaskDef | PartialCall,
        *,
        param: str,
        **fixed: Any,
    ) -> KeyedExprList[Any]:
        """Call a task once per group, passing the group's members as a list.

        Parameters
        ----------
        task_partial : TaskDef | PartialCall
            The task to call — either a bare task (``merge_lanes``) or one
            with some arguments already fixed (``merge_lanes(min_reads=10)``).
        param : str
            The task's parameter that receives the group's members, as a
            plain ``list`` of that group's ``Expr`` values.
        **fixed
            Additional arguments fixed for every group's call. These take
            precedence over any same-named group-key value.

        Returns
        -------
        KeyedExprList[Any]
            One call per group, keyed by ``fields``.

        Notes
        -----
        If the task declares a parameter with the same name as one of the
        grouped ``fields`` (e.g. a ``sample`` parameter when grouping by
        ``sample``), that group's key value is passed automatically —
        useful for naming outputs after the group — unless the caller
        already fixed that parameter explicitly.
        """
        task_def, base_fixed = _resolve_task(task_partial, fixed)
        exprs: list[Expr] = []
        keys: list[dict[str, Hashable]] = []
        for group in self.groups:
            call_args = dict(base_fixed)
            call_args[param] = list(group.members)
            _merge_key_into_call_args(call_args=call_args, key=group.key, task_def=task_def)
            expr = _construct_expr(
                task_def=task_def,
                call_args=call_args,
                display_label_parts=_label_parts_for_key(group.key),
            )
            exprs.append(expr)
            keys.append(dict(group.key))
        result = ExprList(exprs=exprs, task_def=task_def)
        record_call(result)
        return KeyedExprList(_exprs=tuple(exprs), _keys=tuple(keys))


def _index_by_fields(
    exprs: tuple[Any, ...],
    keys: tuple[dict[str, Hashable], ...],
    fields: tuple[str, ...],
    *,
    side: str,
) -> dict[tuple[Hashable, ...], Any]:
    """Index expressions by their value on ``fields``, rejecting duplicates."""
    index: dict[tuple[Hashable, ...], Any] = {}
    for expr, key in zip(exprs, keys):
        missing = [field_name for field_name in fields if field_name not in key]
        if missing:
            raise KeyError(f"join() field(s) {missing} missing from {side} key {dict(key)!r}")
        group_key = tuple(key[field_name] for field_name in fields)
        if group_key in index:
            raise ValueError(
                f"join() found a duplicate {side} key on field(s) "
                f"{', '.join(fields)}: {dict(zip(fields, group_key))!r}. join() requires "
                f"unique keys on both sides — group_by() first if duplicates are expected."
            )
        index[group_key] = expr
    return index


@dataclass(frozen=True)
class KeyedExprList(Generic[T]):
    """An ``ExprList`` paired 1:1 with metadata keys.

    Built by :func:`keyed` (or ``ExprList.with_keys()``), and by
    :meth:`KeyedGroups.map_groups`, :meth:`KeyedExprList.map`, and
    :meth:`KeyedExprList.join`, which all keep the pairing intact across a
    task call. Every element is an ordinary :class:`~ginkgo.core.expr.Expr`
    (or, after :meth:`join`, a ``(left, right)`` pair of them) — there is no
    new graph-node type, so cache, provenance, resource, and retry semantics
    are exactly the ones the evaluator already gives ``Expr``.

    Iteration and indexing follow the underlying elements' order, which is
    always: first-appearance order of a grouped key, and otherwise the order
    of the collection the keys came from.
    """

    _exprs: tuple[Any, ...]
    _keys: tuple[dict[str, Hashable], ...]

    def __len__(self) -> int:
        return len(self._exprs)

    def __iter__(self):
        return iter(self._exprs)

    def __getitem__(self, index: int) -> Any:
        return self._exprs[index]

    @property
    def keys(self) -> list[dict[str, Hashable]]:
        """Return each element's metadata key, as plain dicts, in order."""
        return [dict(key) for key in self._keys]

    def unkey(self) -> ExprList[T]:
        """Drop the keys and return the plain ``ExprList``.

        Raises
        ------
        TypeError
            If this collection came from :meth:`join`, whose elements are
            ``(left, right)`` pairs rather than single expressions — pass
            those straight to ``.map()`` instead of unkeying them.
        """
        if self._exprs and not all(isinstance(expr, Expr) for expr in self._exprs):
            raise TypeError(
                "unkey() requires single-expression elements; this collection holds "
                "joined (left, right) pairs — use .map() to consume both sides instead"
            )
        task_def = self._exprs[0].task_def if self._exprs else None
        return ExprList(exprs=list(self._exprs), task_def=task_def)

    def group_by(self, *fields: str) -> KeyedGroups:
        """Group by one or more key fields, preserving first-appearance order.

        Parameters
        ----------
        *fields : str
            Key field names to group by, e.g. ``group_by("sample", "library")``.

        Returns
        -------
        KeyedGroups
            Groups in first-appearance order, each with members in their
            original order.

        Raises
        ------
        ValueError
            If no fields are given.
        KeyError
            If an element's key is missing one of ``fields``.
        """
        if not fields:
            raise ValueError("group_by() requires at least one field")
        order: list[tuple[Hashable, ...]] = []
        members: dict[tuple[Hashable, ...], list[Expr]] = {}
        for expr, key in zip(self._exprs, self._keys):
            missing = [field_name for field_name in fields if field_name not in key]
            if missing:
                raise KeyError(f"group_by() field(s) {missing} missing from key {dict(key)!r}")
            group_key = tuple(key[field_name] for field_name in fields)
            if group_key not in members:
                members[group_key] = []
                order.append(group_key)
            members[group_key].append(expr)
        groups = tuple(
            _Group(key=dict(zip(fields, group_key)), members=tuple(members[group_key]))
            for group_key in order
        )
        return KeyedGroups(fields=tuple(fields), groups=groups)

    def map(
        self,
        task_partial: TaskDef | PartialCall,
        *,
        param: str | Sequence[str],
        **fixed: Any,
    ) -> KeyedExprList[Any]:
        """Call a task once per element, retaining each element's key.

        Parameters
        ----------
        task_partial : TaskDef | PartialCall
            The task to call.
        param : str | Sequence[str]
            The parameter that receives each element. For a plain keyed
            collection this is a single name. For the result of
            :meth:`join`, whose elements are ``(left, right)`` pairs, pass
            two names, e.g. ``param=("sample_bam", "sample_qc")`` — they are
            zipped against the pair positionally, exactly as ``.map()``
            zips ``ExprList`` columns elsewhere.
        **fixed
            Additional arguments fixed for every call. These take
            precedence over any same-named key value.

        Returns
        -------
        KeyedExprList[Any]
            One call per element, with the same keys as this collection.
        """
        task_def, base_fixed = _resolve_task(task_partial, fixed)
        exprs: list[Expr] = []
        for member, key in zip(self._exprs, self._keys):
            call_args = dict(base_fixed)
            if isinstance(param, str):
                call_args[param] = member
            else:
                names = tuple(param)
                if not isinstance(member, tuple) or len(member) != len(names):
                    raise TypeError(
                        f"map() param={param!r} names {len(names)} parameters, but this "
                        f"collection's elements are not {len(names)}-tuples: {member!r}"
                    )
                call_args.update(dict(zip(names, member)))
            _merge_key_into_call_args(call_args=call_args, key=key, task_def=task_def)
            expr = _construct_expr(
                task_def=task_def,
                call_args=call_args,
                display_label_parts=_label_parts_for_key(key),
            )
            exprs.append(expr)
        result = ExprList(exprs=exprs, task_def=task_def)
        record_call(result)
        return KeyedExprList(_exprs=tuple(exprs), _keys=self._keys)

    def join(self, other: KeyedExprList[Any], *, on: str | Sequence[str]) -> KeyedExprList[Pair]:
        """Inner-join two keyed collections on explicit fields.

        Strict by default: every key on either side must have exactly one
        match on the other, so this is not a filtering join — a key present
        on only one side is an error, not a silent drop. Use
        :meth:`group_by` first if either side legitimately has duplicate
        keys to merge before joining.

        Parameters
        ----------
        other : KeyedExprList[Any]
            The collection to join against.
        on : str | Sequence[str]
            The field(s) to join on. Must be present in both collections'
            keys.

        Returns
        -------
        KeyedExprList[Pair]
            One ``(left, right)`` pair per matched key, in this collection's
            order, keyed by ``on`` alone (metadata outside ``on`` is not
            carried over — pass it through the task call itself if needed).

        Raises
        ------
        ValueError
            If ``on`` is empty, or a key is duplicated on either side, or a
            key is present on only one side.
        KeyError
            If an element's key is missing one of the ``on`` fields.
        """
        fields = (on,) if isinstance(on, str) else tuple(on)
        if not fields:
            raise ValueError("join() requires at least one field in 'on'")
        left_index = _index_by_fields(self._exprs, self._keys, fields, side="left")
        right_index = _index_by_fields(other._exprs, other._keys, fields, side="right")

        left_only = set(left_index) - set(right_index)
        if left_only:
            raise ValueError(
                "join() found key(s) present only on the left side: "
                f"{[dict(zip(fields, k)) for k in sorted(left_only, key=repr)]!r}"
            )
        right_only = set(right_index) - set(left_index)
        if right_only:
            raise ValueError(
                "join() found key(s) present only on the right side: "
                f"{[dict(zip(fields, k)) for k in sorted(right_only, key=repr)]!r}"
            )

        pairs = tuple((left_index[group_key], right_index[group_key]) for group_key in left_index)
        keys = tuple(dict(zip(fields, group_key)) for group_key in left_index)
        return KeyedExprList(_exprs=pairs, _keys=keys)
