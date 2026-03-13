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
    task_id : int
        Internal task identifier.
    threads : int
        Core footprint for the task.
    """

    task_id: int
    threads: int


def select_dispatch_subset(
    *,
    ready_tasks: Iterable[SchedulableTask],
    jobs: int,
    cores: int,
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

    Returns
    -------
    list[int]
        The selected task identifiers.
    """
    tasks = list(ready_tasks)
    if jobs <= 0 or cores <= 0 or not tasks:
        return []

    return _select_with_cp_sat(tasks=tasks, jobs=jobs, cores=cores)


def _select_with_cp_sat(
    *,
    tasks: list[SchedulableTask],
    jobs: int,
    cores: int,
) -> list[int]:
    """Select tasks using OR-Tools CP-SAT when available."""
    model = cp_model.CpModel()
    selected = {task.task_id: model.NewBoolVar(f"task_{task.task_id}") for task in tasks}

    model.Add(sum(selected.values()) <= jobs)
    model.Add(sum(task.threads * selected[task.task_id] for task in tasks) <= cores)

    total_selected = sum(selected.values())
    total_cores = sum(task.threads * selected[task.task_id] for task in tasks)
    order_bias = sum(
        (len(tasks) - index) * selected[task.task_id] for index, task in enumerate(tasks)
    )
    model.Maximize(total_selected * 100000 + total_cores * 100 + order_bias)

    solver = cp_model.CpSolver()
    status = solver.Solve(model)
    if status not in {cp_model.OPTIMAL, cp_model.FEASIBLE}:
        return []

    return [task.task_id for task in tasks if solver.Value(selected[task.task_id])]
