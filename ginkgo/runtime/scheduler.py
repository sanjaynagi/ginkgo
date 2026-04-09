"""Task selection policy for constrained concurrent execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from ortools.sat.python import cp_model


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
    concurrency_group : str | None
        Optional named concurrency group. When set, the scheduler will
        respect the corresponding entry in ``available_group_slots`` when
        selecting tasks to dispatch.
    """

    node_id: int
    threads: int
    memory_gb: int
    concurrency_group: str | None = None


def select_dispatch_subset(
    *,
    ready_tasks: Iterable[SchedulableTask],
    jobs: int,
    cores: int,
    memory: int | None = None,
    available_group_slots: dict[str, int] | None = None,
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
    available_group_slots : dict[str, int] | None
        Remaining concurrency-group budgets after accounting for in-flight
        tasks. The scheduler enforces ``sum(selected in group) <= slot``
        for each group present in this mapping.

    Returns
    -------
    list[int]
        The selected task identifiers.
    """
    tasks = list(ready_tasks)
    if jobs <= 0 or cores <= 0 or (memory is not None and memory < 0) or not tasks:
        return []

    return _select_with_cp_sat(
        tasks=tasks,
        jobs=jobs,
        cores=cores,
        memory=memory,
        available_group_slots=available_group_slots or {},
    )


def _select_with_cp_sat(
    *,
    tasks: list[SchedulableTask],
    jobs: int,
    cores: int,
    memory: int | None,
    available_group_slots: dict[str, int],
) -> list[int]:
    """Select tasks using OR-Tools CP-SAT when available."""
    model = cp_model.CpModel()
    selected = {task.node_id: model.NewBoolVar(f"task_{task.node_id}") for task in tasks}

    model.Add(sum(selected.values()) <= jobs)
    model.Add(sum(task.threads * selected[task.node_id] for task in tasks) <= cores)
    if memory is not None:
        model.Add(sum(task.memory_gb * selected[task.node_id] for task in tasks) <= memory)

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
    order_bias = sum(
        (len(tasks) - index) * selected[task.node_id] for index, task in enumerate(tasks)
    )
    model.Maximize(total_selected * 100000 + total_cores * 100 + order_bias)

    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return []

    return [task.node_id for task in tasks if solver.Value(selected[task.node_id])]
