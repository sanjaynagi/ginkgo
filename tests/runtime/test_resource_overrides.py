"""Site resource overrides and retry memory escalation tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import task
from ginkgo.core.resources import ResourceOverrides, Resources
from ginkgo.runtime.evaluator import ConcurrentEvaluator


@task(threads=2, memory="4Gi")
def sized_step(x: int) -> int:
    return x


@task(retries=1, memory="4Gi", memory_retry_multiplier=2)
def flaky_step(marker: str) -> str:
    path = Path(marker)
    if not path.exists():
        path.write_text("attempted", encoding="utf-8")
        raise RuntimeError("first attempt fails")
    return "ok"


class TestResourceOverridesParsing:
    def test_empty_config(self) -> None:
        assert ResourceOverrides.from_config(None).selectors == ()
        assert ResourceOverrides.from_config({}).selectors == ()

    def test_unknown_key_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown keys"):
            ResourceOverrides.from_config({"overrides": {"sized_step": {"cpus": 4}}})

    def test_non_table_selector_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be a table"):
            ResourceOverrides.from_config({"overrides": {"sized_step": 4}})


class TestResourceOverridesMatching:
    def test_short_name_match(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_step": {"threads": 8, "memory": "16Gi"}}}
        )
        merged = overrides.apply(task_name="pkg.module.sized_step", base=Resources(threads=2))
        assert merged.threads == 8
        assert merged.memory_gb == 16

    def test_fully_qualified_match(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"pkg.module.sized_step": {"threads": 8}}}
        )
        assert overrides.apply(task_name="pkg.module.sized_step", base=Resources()).threads == 8

    def test_unmatched_returns_base(self) -> None:
        overrides = ResourceOverrides.from_config({"overrides": {"other": {"threads": 8}}})
        base = Resources(threads=2)
        assert overrides.apply(task_name="pkg.sized_step", base=base) is base

    def test_omitted_keys_keep_declared_values(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_step": {"memory": "16Gi"}}}
        )
        merged = overrides.apply(
            task_name="pkg.sized_step", base=Resources(threads=4, memory="4Gi")
        )
        assert merged.threads == 4
        assert merged.memory_gb == 16

    def test_glob_match(self) -> None:
        overrides = ResourceOverrides.from_config({"overrides": {"sized_*": {"threads": 8}}})
        assert overrides.apply(task_name="pkg.sized_step", base=Resources()).threads == 8

    def test_exact_beats_glob(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_*": {"threads": 8}, "sized_step": {"threads": 16}}}
        )
        assert overrides.apply(task_name="pkg.sized_step", base=Resources()).threads == 16

    def test_first_glob_wins(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_*": {"threads": 8}, "*_step": {"threads": 16}}}
        )
        assert overrides.apply(task_name="pkg.sized_step", base=Resources()).threads == 8

    def test_invalid_merge_names_the_task(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_step": {"gpu_type": "nvidia-tesla-t4"}}}
        )
        with pytest.raises(ValueError, match="invalid resource override for pkg.sized_step"):
            overrides.apply(task_name="pkg.sized_step", base=Resources())


class TestEffectiveResources:
    def test_override_applies_to_scheduling(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_step": {"threads": 8, "memory": "16Gi"}}}
        )
        evaluator = ConcurrentEvaluator(jobs=1, resource_overrides=overrides)
        effective = evaluator._effective_resources(task_def=sized_step)
        assert effective.threads == 8
        assert effective.memory_gb == 16

    def test_no_overrides_returns_declaration(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1)
        assert evaluator._effective_resources(task_def=sized_step) is sized_step.resources


class TestMemoryEscalation:
    def test_multiplier_math(self) -> None:
        resources = Resources(memory="4Gi", memory_retry_multiplier=2)
        assert resources.memory_gb_for_attempt(0) == 4
        assert resources.memory_gb_for_attempt(1) == 8
        assert resources.memory_gb_for_attempt(2) == 16

    def test_default_multiplier_is_flat(self) -> None:
        resources = Resources(memory="4Gi")
        assert resources.memory_gb_for_attempt(3) == 4

    def test_multiplier_below_one_rejected(self) -> None:
        with pytest.raises(ValueError, match="memory_retry_multiplier must be at least 1"):
            Resources(memory="4Gi", memory_retry_multiplier=0.5)

    def test_multiplier_requires_memory(self) -> None:
        with pytest.raises(ValueError, match="requires memory to be set"):
            Resources(memory_retry_multiplier=2)

    def test_retry_escalates_memory(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, memory=32)
        result = evaluator.evaluate(flaky_step(marker="escalation.marker"))
        assert result == "ok"
        (node,) = evaluator.task_nodes.values()
        assert node.attempt == 2
        assert node.memory_gb == 8

    def test_escalation_clamped_to_local_budget(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, memory=6)
        result = evaluator.evaluate(flaky_step(marker="clamped.marker"))
        assert result == "ok"
        (node,) = evaluator.task_nodes.values()
        assert node.memory_gb == 6
