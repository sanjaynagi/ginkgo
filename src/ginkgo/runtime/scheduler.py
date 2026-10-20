"""Task selection policy for constrained concurrent execution."""

from __future__ import annotations

import signal
from collections import Counter
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from threading import current_thread, main_thread
from typing import Iterable

from ortools.sat.python import cp_model

_GUARDED_SIGNALS = (signal.SIGINT, signal.SIGTERM)


@contextmanager
def _signal_handlers_preserved() -> Iterator[None]:
    """Restore this process's signal handlers after CP-SAT has solved.

    CP-SAT installs its own interrupt handler for the duration of a solve and
    puts back ``SIG_DFL`` rather than what it found. Every solve therefore
    disarms whatever handler the caller installed, and the scheduler solves on
    every dispatch — so an interrupt arriving after the first wave killed the
    process outright, before the run could record that it was cancelled.

    Only meaningful on the main thread: :func:`signal.signal` refuses to run
    anywhere else, and there is nothing to restore there anyway.
    """
    if current_thread() is not main_thread():
        yield
        return

    previous = {signum: signal.getsignal(signum) for signum in _GUARDED_SIGNALS}
    try:
        yield
    finally:
        # Re-installed unconditionally. CP-SAT reaches past the ``signal``
        # module to change the disposition, so Python's own record of the
        # handler still names it and comparing the two would see no change
        # to undo. ``getsignal`` returns None only for a handler installed
        # from C, which there is no way to put back.
        for signum, handler in previous.items():
            if handler is not None:
                signal.signal(signum, handler)


@dataclass(frozen=True)
class SchedulableTask:
    """A ready task candidate for the resource scheduler.

    Parameters
    ----------
    node_id : int
        Internal evaluator node identifier.
    threads : int
        Core footprint for the task.
    memory_gb : int
        Declared memory footprint for the task in GiB.
    gpu : int
        Declared local GPU footprint for the task. Remote-placed tasks
        report 0 — their GPUs are satisfied by the executor.
    priority : int
        Declared scheduling priority. Higher values run first when multiple
        ready tasks contend for the same resources. Default is ``0``.
    concurrency_group : str | None
        Optional named concurrency group. When set, the scheduler will
        respect the corresponding entry in ``available_group_slots`` when
        selecting tasks to dispatch.
    concurrency_group_limit : int | None
        How many tasks of the group may run at once, if it is limited.
    custom : Mapping[str, int]
        User-defined resource demands, constrained per dimension by
        ``custom_budgets``. Dimensions absent from the budgets mapping are
        unconstrained. Unlike the built-in dimensions, custom demands are
        not zeroed for remote-placed tasks — their budgets are run-level.
    """

    node_id: int
    threads: int
    memory_gb: int
    gpu: int = 0
    priority: int = 0
    concurrency_group: str | None = None
    concurrency_group_limit: int | None = None
    custom: Mapping[str, int] = field(default_factory=dict)


@dataclass(kw_only=True)
class ResourceLedger:
    """The run's resource budgets, and what the tasks in flight hold of them.

    A task's footprint is reserved when it is handed to an executor and
    released when that work finishes, so what is free is always known
    without re-adding up the tasks in flight.

    Parameters
    ----------
    jobs : int
        How many tasks may be in flight at once.
    cores : int
        The local core budget.
    memory : int | None
        The local memory budget in GiB, or ``None`` when memory is not
        scheduled.
    gpus : int
        The local GPU budget.
    custom_budgets : Mapping[str, int] | None
        Run-level budgets for user-defined resource dimensions.
    """

    jobs: int
    cores: int
    memory: int | None = None
    gpus: int = 0
    custom_budgets: Mapping[str, int] | None = None
    _held: dict[int, SchedulableTask] = field(default_factory=dict, repr=False)
    _cores: int = field(default=0, repr=False)
    _memory: int = field(default=0, repr=False)
    _gpus: int = field(default=0, repr=False)
    _groups: Counter[str] = field(default_factory=Counter, repr=False)
    _custom: Counter[str] = field(default_factory=Counter, repr=False)

    def reserve(self, task: SchedulableTask) -> None:
        """Hold *task*'s footprint while its work is in flight."""
        if task.node_id in self._held:
            raise RuntimeError(f"node {task.node_id} already holds resources")
        self._held[task.node_id] = task
        self._cores += task.threads
        self._memory += task.memory_gb
        self._gpus += task.gpu
        if task.concurrency_group is not None:
            self._groups[task.concurrency_group] += 1
        self._custom.update(task.custom)

    def release(self, node_id: int) -> None:
        """Give back what the node's finished work held."""
        task = self._held.pop(node_id)
        self._cores -= task.threads
        self._memory -= task.memory_gb
        self._gpus -= task.gpu
        if task.concurrency_group is not None:
            self._groups[task.concurrency_group] -= 1
        self._custom.subtract(task.custom)

    def select(self, ready_tasks: list[SchedulableTask]) -> list[int]:
        """Select the ready tasks to dispatch within what is still free.

        A concurrency group's limit is read from the ready tasks that
        declare it; every task of the group in flight counts against it.
        """
        group_slots = {
            task.concurrency_group: max(
                0, task.concurrency_group_limit - self._groups[task.concurrency_group]
            )
            for task in ready_tasks
            if task.concurrency_group is not None and task.concurrency_group_limit is not None
        }
        custom = (
            None
            if not self.custom_budgets
            else {
                dimension: budget - self._custom[dimension]
                for dimension, budget in self.custom_budgets.items()
            }
        )
        return select_dispatch_subset(
            ready_tasks=ready_tasks,
            jobs=self.jobs - len(self._held),
            cores=self.cores - self._cores,
            memory=None if self.memory is None else self.memory - self._memory,
            gpus=self.gpus - self._gpus,
            available_group_slots=group_slots,
            custom_budgets=custom,
        )


def select_dispatch_subset(
    *,
    ready_tasks: Iterable[SchedulableTask],
    jobs: int,
    cores: int,
    memory: int | None = None,
    gpus: int = 0,
    available_group_slots: dict[str, int] | None = None,
    custom_budgets: Mapping[str, int] | None = None,
) -> list[int]:
    """Select a feasible subset of ready tasks to dispatch.

    Parameters
    ----------
    ready_tasks : Iterable[SchedulableTask]
        Ready tasks to choose from.
    jobs : int
        Maximum number of tasks to dispatch in this cycle.
    cores : int
        Available core budget for this cycle.
    memory : int | None
        Available memory budget in GiB for this cycle, or ``None`` when
        memory-aware scheduling is disabled.
    gpus : int
        Available local GPU budget for this cycle.
    available_group_slots : dict[str, int] | None
        Remaining concurrency-group budgets after accounting for in-flight
        tasks. The scheduler enforces ``sum(selected in group) <= slot``
        for each group present in this mapping.
    custom_budgets : Mapping[str, int] | None
        Remaining budget per user-defined resource dimension after
        accounting for in-flight tasks. The scheduler enforces
        ``sum(selected demand) <= budget`` for each dimension present in
        this mapping; dimensions tasks request but the mapping omits are
        unconstrained.

    Returns
    -------
    list[int]
        The selected task identifiers.
    """
    tasks = list(ready_tasks)
    # Only a missing jobs slot blocks everything. An exhausted core/memory/gpu
    # budget must not short-circuit: remote-placed tasks carry a zero local
    # footprint and stay dispatchable; the CP-SAT constraints handle the rest.
    if jobs <= 0 or not tasks:
        return []

    return _select_with_cp_sat(
        tasks=tasks,
        jobs=jobs,
        cores=cores,
        memory=memory,
        gpus=gpus,
        available_group_slots=available_group_slots or {},
        custom_budgets=custom_budgets or {},
    )


def _select_with_cp_sat(
    *,
    tasks: list[SchedulableTask],
    jobs: int,
    cores: int,
    memory: int | None,
    gpus: int,
    available_group_slots: dict[str, int],
    custom_budgets: Mapping[str, int],
) -> list[int]:
    """Select tasks using OR-Tools CP-SAT when available."""
    model = cp_model.CpModel()
    selected = {task.node_id: model.NewBoolVar(f"task_{task.node_id}") for task in tasks}

    model.Add(sum(selected.values()) <= jobs)
    model.Add(sum(task.threads * selected[task.node_id] for task in tasks) <= max(0, cores))
    if memory is not None:
        model.Add(sum(task.memory_gb * selected[task.node_id] for task in tasks) <= max(0, memory))
    model.Add(sum(task.gpu * selected[task.node_id] for task in tasks) <= max(0, gpus))

    # One weighted-sum constraint per budgeted user-defined dimension.
    for dimension, budget in custom_budgets.items():
        model.Add(
            sum(task.custom.get(dimension, 0) * selected[task.node_id] for task in tasks)
            <= max(0, int(budget))
        )

    # Per-group concurrency limits — each named group consumes one slot per
    # selected task; tasks already in flight have already been deducted from
    # the caller-provided remaining budget.
    grouped: dict[str, list[SchedulableTask]] = {}
    for task in tasks:
        if task.concurrency_group is None:
            continue
        grouped.setdefault(task.concurrency_group, []).append(task)
    for group_id, group_tasks in grouped.items():
        slot = available_group_slots.get(group_id)
        if slot is None:
            continue
        model.Add(sum(selected[task.node_id] for task in group_tasks) <= max(0, int(slot)))

    total_selected = sum(selected.values())
    total_cores = sum(task.threads * selected[task.node_id] for task in tasks)
    priority_sum = sum(task.priority * selected[task.node_id] for task in tasks)
    order_bias = sum(
        (len(tasks) - index) * selected[task.node_id] for index, task in enumerate(tasks)
    )
    # Weights establish a strict lexicographic preference:
    # (1) dispatch as many tasks as possible,
    # (2) fill the core budget,
    # (3) prefer higher-priority tasks when (1) and (2) are tied,
    # (4) break remaining ties in declaration order.
    model.Maximize(
        total_selected * 10_000_000 + total_cores * 10_000 + priority_sum * 100 + order_bias
    )

    solver = cp_model.CpSolver()
    with _signal_handlers_preserved():
        status = solver.Solve(model)
    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return []

    return [task.node_id for task in tasks if solver.Value(selected[task.node_id])]
