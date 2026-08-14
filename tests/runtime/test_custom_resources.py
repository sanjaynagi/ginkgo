"""User-defined resource dimension tests (custom budgets)."""

from __future__ import annotations

import time

import pytest

from ginkgo import evaluate, flow, task
from ginkgo.core.resources import (
    ResourceOverrides,
    Resources,
    parse_resource_budget_args,
    resource_budgets_from_config,
)
from ginkgo.runtime.dry_run import PlannedTask, PlanWave, _summarise_resources
from ginkgo.runtime.scheduler import SchedulableTask, select_dispatch_subset


class TestResourcesCustom:
    def test_defaults_to_empty(self) -> None:
        assert Resources().custom == {}

    def test_positive_demands_accepted(self) -> None:
        assert Resources(custom={"api_calls": 2, "db": 1}).custom == {"api_calls": 2, "db": 1}

    def test_reserved_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="collides with a built-in resource field"):
            Resources(custom={"threads": 2})

    def test_zero_demand_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be a positive integer"):
            Resources(custom={"api_calls": 0})

    def test_bool_demand_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be a positive integer"):
            Resources(custom={"api_calls": True})

    def test_non_int_demand_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be a positive integer"):
            Resources(custom={"api_calls": "2"})


class TestTaskDecorator:
    def test_resources_kwarg_populates_custom(self) -> None:
        @task(resources={"api_calls": 2})
        def fetch() -> int:
            return 1

        assert fetch.resources.custom == {"api_calls": 2}


class TestBudgetParsing:
    def test_cli_args_parse(self) -> None:
        assert parse_resource_budget_args(["api_calls=10", "db=2"]) == {"api_calls": 10, "db": 2}

    def test_cli_missing_separator_rejected(self) -> None:
        with pytest.raises(ValueError, match="expected name=value"):
            parse_resource_budget_args(["api_calls"])

    def test_cli_non_positive_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            parse_resource_budget_args(["api_calls=0"])

    def test_cli_non_int_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            parse_resource_budget_args(["api_calls=ten"])

    def test_cli_reserved_name_points_at_run_option(self) -> None:
        with pytest.raises(ValueError, match="dedicated --gpus run option"):
            parse_resource_budget_args(["gpu=2"])
        with pytest.raises(ValueError, match="dedicated --cores run option"):
            parse_resource_budget_args(["threads=2"])

    def test_cli_unbudgetable_builtin_rejected(self) -> None:
        with pytest.raises(ValueError, match="has no run-level budget"):
            parse_resource_budget_args(["gpu_type=1"])

    def test_config_budgets_parse(self) -> None:
        assert resource_budgets_from_config({"budgets": {"api_calls": 10}}) == {"api_calls": 10}

    def test_config_absent_is_empty(self) -> None:
        assert resource_budgets_from_config(None) == {}
        assert resource_budgets_from_config({}) == {}

    def test_config_non_table_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"\[resources.budgets\] must be a table"):
            resource_budgets_from_config({"budgets": 10})

    def test_config_reserved_name_points_at_run_option(self) -> None:
        with pytest.raises(ValueError, match="dedicated --memory run option"):
            resource_budgets_from_config({"budgets": {"memory": 4}})

    def test_config_non_positive_rejected(self) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            resource_budgets_from_config({"budgets": {"api_calls": 0}})


class TestOverridesCustom:
    def test_override_replaces_custom_demands(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"fetch": {"custom": {"api_calls": 5}}}}
        )
        base = Resources(custom={"api_calls": 2})
        merged = overrides.apply(task_name="module.fetch", base=base)
        assert merged.custom == {"api_calls": 5}


class TestSchedulerCustomBudget:
    def test_budget_caps_weighted_demand(self) -> None:
        ready = [
            SchedulableTask(node_id=i, threads=1, memory_gb=0, custom={"api_calls": 2})
            for i in range(5)
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=10,
            cores=10,
            custom_budgets={"api_calls": 4},
        )
        assert len(selected) == 2

    def test_zero_remaining_budget_blocks_dispatch(self) -> None:
        ready = [SchedulableTask(node_id=1, threads=1, memory_gb=0, custom={"api_calls": 1})]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=4,
            cores=4,
            custom_budgets={"api_calls": 0},
        )
        assert selected == []

    def test_unrelated_tasks_unaffected_by_budget(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=1, memory_gb=0, custom={"api_calls": 1}),
            SchedulableTask(node_id=2, threads=1, memory_gb=0, custom={"api_calls": 1}),
            SchedulableTask(node_id=3, threads=1, memory_gb=0),
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=10,
            cores=10,
            custom_budgets={"api_calls": 1},
        )
        assert len([n for n in selected if n in {1, 2}]) == 1
        assert 3 in selected

    def test_independent_dimensions_constrain_separately(self) -> None:
        ready = [
            SchedulableTask(node_id=1, threads=1, memory_gb=0, custom={"api_calls": 1, "db": 1}),
            SchedulableTask(node_id=2, threads=1, memory_gb=0, custom={"api_calls": 1}),
            SchedulableTask(node_id=3, threads=1, memory_gb=0, custom={"db": 1}),
        ]
        selected = select_dispatch_subset(
            ready_tasks=ready,
            jobs=10,
            cores=10,
            custom_budgets={"api_calls": 1, "db": 1},
        )
        # Task 1 consumes both budgets, so it excludes both others; the
        # count-maximizing objective must instead pick tasks 2 and 3.
        assert sorted(selected) == [2, 3]

    def test_unbudgeted_dimension_unconstrained(self) -> None:
        ready = [
            SchedulableTask(node_id=i, threads=1, memory_gb=0, custom={"api_calls": 2})
            for i in range(5)
        ]
        selected = select_dispatch_subset(ready_tasks=ready, jobs=10, cores=10)
        assert len(selected) == 5


class TestDryRunSummary:
    def test_custom_demands_are_totalled(self) -> None:
        def planned(node_id: int, custom: dict[str, int]) -> PlannedTask:
            return PlannedTask(
                node_id=node_id,
                base_name="fetch",
                label="fetch()",
                kind="python",
                env=None,
                mapped=False,
                threads=1,
                memory_gb=0,
                gpu=0,
                custom=custom,
                cache_status="will_run",
            )

        waves = [
            PlanWave(index=1, tasks=[planned(0, {"api_calls": 2})]),
            PlanWave(index=2, tasks=[planned(1, {"api_calls": 1, "db": 1}), planned(2, {})]),
        ]
        assert _summarise_resources(waves).custom_totals == {"api_calls": 3, "db": 1}


# ----- End-to-end evaluator tests ---------------------------------------------


@task(resources={"api_calls": 1})
def _timed_api_call(item: str) -> dict[str, float]:
    started = time.perf_counter()
    # Long enough that concurrently dispatched branches reliably overlap
    # even when process-pool workers spawn slowly on a loaded machine.
    time.sleep(0.3)
    ended = time.perf_counter()
    return {"start": started, "end": ended}


@task(resources={"api_calls": 4})
def _hungry_api_task() -> int:
    return 1


@flow
def _api_flow(items: list[str]):
    return _timed_api_call().map(item=items)


def _peak_overlap(records: list[dict[str, float]]) -> int:
    points: list[tuple[float, int]] = []
    for record in records:
        points.append((record["start"], 1))
        points.append((record["end"], -1))
    points.sort(key=lambda item: (item[0], item[1]))
    active = 0
    peak = 0
    for _, delta in points:
        active += delta
        peak = max(peak, active)
    return peak


class TestEvaluatorCustomBudget:
    def test_budget_serializes_contending_tasks(self) -> None:
        items = [f"item_{i}" for i in range(4)]
        records = evaluate(
            _api_flow(items=items),
            jobs=4,
            cores=4,
            resource_budgets={"api_calls": 1},
        )
        assert len(records) == 4
        assert _peak_overlap(records) == 1

    def test_no_budget_leaves_dimension_unconstrained(self) -> None:
        items = [f"item_{i}" for i in range(4)]
        records = evaluate(_api_flow(items=items), jobs=4, cores=4)
        assert _peak_overlap(records) >= 2

    def test_demand_exceeding_budget_fails_fast(self) -> None:
        with pytest.raises(ValueError, match="requires 4 api_calls but only 2 are available"):
            evaluate(_hungry_api_task(), jobs=1, resource_budgets={"api_calls": 2})
