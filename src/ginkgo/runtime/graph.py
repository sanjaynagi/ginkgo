"""The static task graph a workflow's expressions describe.

:class:`TaskGraph` registers the task expressions reachable from a value,
labels them, infers ``Out[...]`` path edges between them (#280/#307), rejects
cycles, and validates what can be checked before anything runs. It knows
nothing about scheduling: the evaluator owns one and keeps each node's run
state beside it, while ``inspect``, ``secrets`` and ``doctor`` use one alone.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from ginkgo.core.expr import ConstructedCall, Expr, ExprList, OutputIndex, OutputName
from ginkgo.core.task import PartialCall, TaskDef
from ginkgo.errors import FailureKind, GinkgoError
from ginkgo.runtime.edge_inference import (
    ConsumedPath,
    PathIndex,
    ProducedPath,
    collect_consumed_paths,
    collect_produced_paths,
    reject_self_overwrite,
)
from ginkgo.runtime.task_kinds import task_kind

if TYPE_CHECKING:
    from ginkgo.runtime.placement import Placement
    from ginkgo.runtime.task_validation import TaskValidator


class CycleError(GinkgoError, RuntimeError):
    """Raised when the expression graph contains a dependency cycle."""

    failure_kind: ClassVar[FailureKind] = "cycle_detected"

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        rendered = " -> ".join(cycle)
        super().__init__(f"Detected cycle in workflow graph: {rendered}")


class IncompleteCallError(GinkgoError, TypeError):
    """Raised when a task call missing required arguments reaches the graph.

    Such a call is a :class:`~ginkgo.core.task.PartialCall`, which only
    ``.map()`` or ``.product_map()`` can complete. Returned from the flow or
    passed to another task, it would otherwise run nothing or reach a worker
    as an ordinary value.

    Parameters
    ----------
    partial_call : PartialCall
        The incomplete call.
    consumer : str | None
        Short name of the task it was passed to, or ``None`` when the flow
        returned it.
    """

    diagnostic_code: ClassVar[str] = "incomplete_call"
    diagnostic_suggestion: ClassVar[str] = (
        "Pass every required argument, or complete the call with .map() or .product_map()."
    )

    def __init__(self, *, partial_call: PartialCall, consumer: str | None) -> None:
        name = partial_call.task_def.fn.__name__
        missing = ", ".join(partial_call.missing_params)
        where = "returned from the flow" if consumer is None else f"passed to {consumer}()"
        super().__init__(
            f"{name}() is missing required argument(s): {missing}, but was {where}. "
            "Pass every required argument, or complete the call with .map() or "
            ".product_map()."
        )


@dataclass(eq=False, kw_only=True)
class TaskNode:
    """One task in the graph.

    Only :class:`TaskGraph` changes a node, and only its edges: an inferred
    edge can be added after the node is registered, when a later dynamic
    expansion registers the task that writes a path the node reads.
    Instances hash and compare by identity.
    """

    node_id: int
    expr: Expr
    dependency_ids: frozenset[int]
    """Every task this one waits on, declared or inferred."""
    inferred_dependency_ids: frozenset[int] = frozenset()
    """The subset of :attr:`dependency_ids` inferred from ``Out[...]`` paths
    rather than declared through the ``Expr`` graph."""
    display_label: str | None = None
    """The label given at registration; ``None`` when it is the bare task name."""

    @property
    def task_def(self) -> TaskDef:
        """The task definition for the node."""
        return self.expr.task_def

    @property
    def label(self) -> str:
        """The name the node is shown under: its display label or bare task name."""
        return self.display_label or self.task_def.base_name


@dataclass(frozen=True, kw_only=True)
class Registration:
    """What one :meth:`TaskGraph.register` call added.

    Parameters
    ----------
    dependency_ids : set[int]
        The nodes the registered value refers to directly.
    new_node_ids : list[int]
        Nodes this call created, in ascending id order.
    """

    dependency_ids: set[int]
    new_node_ids: list[int]


@dataclass(kw_only=True)
class TaskGraph:
    """The task nodes reachable from a workflow's value, with their edges."""

    nodes: dict[int, TaskNode] = field(default_factory=dict)
    """Every registered node, in registration order: a node after the nodes
    of its arguments, so dependencies come first."""
    next_node_id: int = 0
    """The id the next registered node takes."""
    new_node_ids: list[int] = field(default_factory=list)
    """The nodes the latest :meth:`register` call added, in registration
    order, kept when it raised."""
    _expr_nodes: dict[int, int] = field(default_factory=dict, repr=False)
    _path_index: PathIndex = field(default_factory=PathIndex, repr=False)
    """Literal produced and consumed paths across every registration, so a
    dynamic expansion is matched against everything seen so far."""
    _dynamic_parent_ids: dict[int, int] = field(default_factory=dict, repr=False)
    """Node id -> id of the task whose dynamic expansion registered it."""
    _display_label_counts: Counter[str] = field(default_factory=Counter, repr=False)
    """The highest ordinal each requested label has been given so far."""
    _display_labels: set[str] = field(default_factory=set, repr=False)
    """Every label a node is shown under, bare task names included."""

    @classmethod
    def build(cls, value: Any) -> TaskGraph:
        """Return the graph of the task nodes reachable from *value*."""
        graph = cls()
        graph.register(value)
        return graph

    def register(
        self,
        value: Any,
        *,
        expanding_node_id: int | None = None,
        node_state: Callable[[int], str] = lambda node_id: "pending",
    ) -> Registration:
        """Register the task nodes reachable from *value* and infer their edges.

        Parameters
        ----------
        value : Any
            An expression, or a nested container of them.
        expanding_node_id : int | None
            The task whose result *value* is, for a dynamic expansion.
        node_state : Callable[[int], str]
            The scheduling state of a node registered earlier. A new
            ``Out[...]`` producer cannot be made a dependency of a reader that
            is no longer ``"pending"``.

        Returns
        -------
        Registration
            The nodes *value* refers to, and the nodes this call created.
            When this raises, the nodes it created stay registered, and
            :attr:`new_node_ids` lists them.

        Raises
        ------
        CycleError
            If the expressions, or an inferred edge, close a cycle.
        IncompleteCallError
            If a partial call reaches the graph.
        """
        self.new_node_ids = []
        dependency_ids = self._register_value(value)
        new_node_ids = list(self.new_node_ids)
        self._infer_edges(
            new_node_ids=new_node_ids,
            expanding_node_id=expanding_node_id,
            node_state=node_state,
        )
        return Registration(dependency_ids=dependency_ids, new_node_ids=new_node_ids)

    def node_id_for(self, expr: Expr) -> int:
        """Return the id of the node registered for *expr*."""
        return self._expr_nodes[id(expr)]

    def unreachable_calls(self, calls: Sequence[ConstructedCall]) -> list[ConstructedCall]:
        """Return the task calls among *calls* the graph never reached.

        A discarded partial call has no expressions, so it is always among
        them.
        """
        return [
            call for call in calls if not any(id(expr) in self._expr_nodes for expr in call.exprs)
        ]

    def validate(self, *, validator: TaskValidator, placement: Placement) -> None:
        """Validate each node's import, environment, inputs and placement.

        Raises
        ------
        Exception
            The first validation failure, as the validator or placement
            raises it.
        """
        nodes = list(self.nodes.values())
        validator.validate_declared_envs(nodes=nodes)
        validator.validate_declared_secrets(nodes=nodes)

        # A `file`/`folder` input that does not exist yet is normally a static
        # error, but it is not when the path is one of this graph's own
        # Out[...] outputs (#280): a real run would produce it before this
        # node runs, since an edge was inferred for it.
        produced_paths = set(self._path_index.produced_exact)
        for node in nodes:
            validator.validate_task_importable(task_def=node.task_def)
            validator.validate_static_inputs(node=node, produced_paths=produced_paths)
            # Placement is static per task definition; resolving it here
            # surfaces misconfiguration (remote=True or an unsatisfiable GPU
            # requirement without a usable executor) before anything runs.
            placement.resolve(task_def=node.task_def)

    # Registration ------------------------------------------------------------

    def _register_value(
        self,
        value: Any,
        *,
        expr_stack: tuple[int, ...] = (),
        task_path: tuple[str, ...] = (),
    ) -> set[int]:
        """Register all task nodes reachable from a nested value."""
        if isinstance(value, (OutputIndex, OutputName)):
            return self._register_value(value.expr, expr_stack=expr_stack, task_path=task_path)

        if isinstance(value, Expr):
            return {self._register_expr(value, expr_stack=expr_stack, task_path=task_path)}

        # Only a fan-out may complete a partial call; one reached here would
        # otherwise run nothing (as the root) or reach a worker as a value.
        if isinstance(value, PartialCall):
            consumer = task_path[-1].rsplit(".", 1)[-1] if task_path else None
            raise IncompleteCallError(partial_call=value, consumer=consumer)

        items: Iterable[Any]
        if isinstance(value, ExprList | list | tuple):
            items = value
        elif isinstance(value, dict):
            items = (item for pair in value.items() for item in pair)
        else:
            return set()
        dependencies: set[int] = set()
        for item in items:
            dependencies |= self._register_value(item, expr_stack=expr_stack, task_path=task_path)
        return dependencies

    def _register_expr(
        self,
        expr: Expr,
        *,
        expr_stack: tuple[int, ...],
        task_path: tuple[str, ...],
    ) -> int:
        """Register a task expression node once per object identity."""
        expr_id = id(expr)
        if expr_id in expr_stack:
            cycle_start = expr_stack.index(expr_id)
            cycle = list(task_path[cycle_start:]) + [expr.task_def.base_name]
            raise CycleError(cycle)

        if expr_id in self._expr_nodes:
            return self._expr_nodes[expr_id]
        _validate_task_kind(task_def=expr.task_def)

        # The id and label are taken before the arguments are walked, so a
        # node's arguments take later ids and ordinals follow ascending id.
        node_id = self.next_node_id
        self.next_node_id += 1
        display_label = self._next_display_label(expr=expr)

        next_expr_stack = (*expr_stack, expr_id)
        next_task_path = (*task_path, expr.task_def.base_name)
        dependency_ids: set[int] = set()
        for value in expr.args.values():
            dependency_ids |= self._register_value(
                value, expr_stack=next_expr_stack, task_path=next_task_path
            )
        # Added after its arguments, so nodes stay in dependency order and a
        # scheduler pass over them resolves a whole cached chain at once.
        self.nodes[node_id] = TaskNode(
            node_id=node_id,
            expr=expr,
            dependency_ids=frozenset(dependency_ids),
            display_label=display_label,
        )
        self._expr_nodes[expr_id] = node_id
        self.new_node_ids.append(node_id)
        return node_id

    def _next_display_label(self, *, expr: Expr) -> str | None:
        """Return the display label for the node being registered for *expr*.

        The expression's own label (its task name plus any fan-out values)
        takes an ordinal when an earlier node already holds it, counted across
        the static graph and every dynamic expansion. The second plain
        ``child(...)`` call is ``child[2]`` in the live table, the dry run,
        the ledger and ``history`` alike.

        Returns
        -------
        str | None
            The label, or ``None`` when it is the bare task name.
        """
        label = self._claim_display_label(expr.display_label)
        return label if label != expr.task_def.base_name else None

    def relabel(self, *, current: str, wanted: str) -> str:
        """Give up a node's *current* label and return a unique form of *wanted*.

        For a mapped branch named only once its arguments resolve: the label
        it is given then must not repeat one another node already holds.

        Parameters
        ----------
        current : str
            The label the node was registered under, released first so a
            branch that wants back the label it holds keeps it.
        wanted : str
            The label the node should be shown under.

        Returns
        -------
        str
            *wanted*, or *wanted* with an ordinal when another node holds it.
        """
        self._display_labels.discard(current)
        return self._claim_display_label(wanted)

    def _claim_display_label(self, label: str) -> str:
        """Return *label*, or the next free ordinal form of it, and hold it."""
        candidate = label
        while candidate in self._display_labels:
            count = max(self._display_label_counts[label], 1) + 1
            self._display_label_counts[label] = count
            candidate = f"{label}[{count}]"
        self._display_labels.add(candidate)
        return candidate

    # Edge inference ----------------------------------------------------------

    def _infer_edges(
        self,
        *,
        new_node_ids: list[int],
        expanding_node_id: int | None,
        node_state: Callable[[int], str],
    ) -> None:
        """Infer ``Out[...]`` path dependency edges for newly registered nodes.

        1. Collect the literal produced (``Out[...]``) and consumed path
           values of the new nodes.
        2. Reject two nodes declaring the same, or an overlapping, ``Out``
           path, and one node declaring an ``Out`` path that overlaps its own
           ``file``/``folder`` input.
        3. Match new consumers against every producer known so far, and add
           the producer as a dependency.
        4. Match new producers against consumers from *earlier*
           registrations, the only way a dynamically registered node can
           matter retroactively. A still-pending old consumer gets the edge;
           one already dispatched can no longer be made to wait, so this
           raises rather than let the race happen silently. The expanding
           task and its own dynamic ancestors are exempt: a task that
           receives a folder and returns children writing inside it is the
           normal fan-out shape, and it already waits on those children
           through its dynamic template.
        5. Detect any cycle the new edges created.
        """
        if not new_node_ids:
            return

        expanding_lineage: set[int] = set()
        if expanding_node_id is not None:
            for node_id in new_node_ids:
                self._dynamic_parent_ids[node_id] = expanding_node_id
            ancestor: int | None = expanding_node_id
            while ancestor is not None:
                expanding_lineage.add(ancestor)
                ancestor = self._dynamic_parent_ids.get(ancestor)

        new_produced: list[ProducedPath] = []
        new_consumed: list[ConsumedPath] = []
        for node_id in new_node_ids:
            node = self.nodes[node_id]
            node_produced = collect_produced_paths(
                node_id=node_id,
                task_name=node.task_def.base_name,
                task_def=node.task_def,
                args=node.expr.args,
            )
            node_consumed = collect_consumed_paths(
                node_id=node_id,
                task_name=node.task_def.base_name,
                task_def=node.task_def,
                args=node.expr.args,
            )
            reject_self_overwrite(produced=node_produced, consumed=node_consumed)
            new_produced.extend(node_produced)
            new_consumed.extend(node_consumed)

        index = self._path_index
        index.add_produced(new_produced)
        edges_added = False

        # New consumers against every producer indexed so far (new included).
        for consumer in new_consumed:
            producer_ids = {entry.node_id for entry in index.producers_for(consumer)}
            edges_added |= self._add_inferred_dependencies(
                node_id=consumer.node_id, producer_ids=producer_ids
            )

        # New producers against consumers registered earlier: the
        # dynamic-expansion case, where the reader may already be in flight.
        for producer in new_produced:
            for consumer in index.consumers_for(producer):
                if consumer.node_id in expanding_lineage:
                    continue
                consumer_state = node_state(consumer.node_id)
                if consumer_state != "pending":
                    producer_name = self.nodes[producer.node_id].task_def.base_name
                    consumer_name = self.nodes[consumer.node_id].task_def.base_name
                    raise RuntimeError(
                        f"{producer_name!r} was just registered declaring "
                        f"`Out[...]` path {producer.path!r}, but {consumer_name!r} "
                        f"already read that path (state={consumer_state!r}) before the "
                        "edge could be inferred. Pass the path through the task graph "
                        "(the producer's return value or an `.output[...]` reference) "
                        "instead of a literal string so the dependency is explicit."
                    )
                edges_added |= self._add_inferred_dependencies(
                    node_id=consumer.node_id, producer_ids={producer.node_id}
                )

        index.add_consumed(new_consumed)

        # Registration already rejects cycles in the expression graph, so only
        # an inferred edge can close one; skip the walk when none was added.
        if edges_added:
            cycle = self._find_cycle()
            if cycle is not None:
                raise CycleError([self.nodes[node_id].task_def.base_name for node_id in cycle])

    def _add_inferred_dependencies(self, *, node_id: int, producer_ids: set[int]) -> bool:
        """Add inferred producer ids to a node's dependencies; return whether any were new."""
        node = self.nodes[node_id]
        producer_ids = producer_ids - {node_id}
        if not producer_ids:
            return False
        node.dependency_ids |= producer_ids
        node.inferred_dependency_ids |= producer_ids
        return True

    def _find_cycle(self) -> list[int] | None:
        """Return a cycle among the nodes' ``dependency_ids``, if any.

        Iterative depth-first search, so a long linear chain of tasks cannot
        exhaust the interpreter's recursion limit.
        """
        WHITE, GRAY, BLACK = 0, 1, 2
        color: dict[int, int] = dict.fromkeys(self.nodes, WHITE)
        for root in sorted(self.nodes):
            if color[root] != WHITE:
                continue
            path: list[int] = [root]
            stack = [iter(sorted(self.nodes[root].dependency_ids))]
            color[root] = GRAY
            while stack:
                dep_id = next(stack[-1], None)
                if dep_id is None:
                    color[path.pop()] = BLACK
                    stack.pop()
                    continue
                state = color.get(dep_id, BLACK)
                if state == GRAY:
                    return [*path[path.index(dep_id) :], dep_id]
                if state == WHITE:
                    color[dep_id] = GRAY
                    path.append(dep_id)
                    stack.append(iter(sorted(self.nodes[dep_id].dependency_ids)))
        return None


def _validate_task_kind(*, task_def: TaskDef) -> None:
    """Raise if *task_def*'s kind is not usable as declared.

    Raises
    ------
    PluginNotFoundError
        If no installed plugin provides the kind.
    ValueError
        If the task declares ``retry_on_exit_codes`` for a kind without exit
        codes to retry on.
    """
    kind = task_kind(task_def)
    if task_def.retry_on_exit_codes is not None and not kind.exit_code_retries:
        raise ValueError(
            f"{task_def.base_name} declares retry_on_exit_codes, which kind={kind.name!r} "
            "does not support"
        )
