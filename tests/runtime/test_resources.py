"""Declared task resources: parsing, site overrides, budgets, escalation, and what a task sees."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import evaluate, shell, task
from ginkgo.core.resources import (
    ResourceOverrides,
    Resources,
    parse_memory,
    parse_resource_budget_args,
    resource_budgets_from_config,
)
from ginkgo.runtime.dry_run import PlannedTask, PlanWave, _summarise_resources
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.task_runners.shell import build_shell_subprocess_env
from tests.conftest import Ledger


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        (None, 0),
        ("4Gi", 4),
        ("2.5Gi", 3),  # binary Gi, rounds up
        ("512Mi", 1),
        ("1Ti", 1024),
        ("8G", 8),  # decimal G
        ("1000M", 1),
        ("1048576Ki", 1),
        ("  4Gi  ", 4),
    ],
)
def test_parse_memory(spec: str | None, expected: int) -> None:
    assert parse_memory(spec) == expected


@pytest.mark.parametrize("spec", ["4Tb", "4096", "", "-4Gi"])
def test_parse_memory_rejects(spec: str) -> None:
    with pytest.raises(ValueError, match="Invalid memory specification"):
        parse_memory(spec)


def test_the_task_decorator_declares_resources() -> None:
    @task(threads=4, memory="8Gi", resources={"api_calls": 2})
    def step() -> int:
        return 1

    assert (step.threads, step.memory, step.memory_gb) == (4, "8Gi", 8)
    assert step.resources.custom == {"api_calls": 2}


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"memory": "lots"}, "Invalid memory specification"),
        ({"memory": 64}, "memory must be a string .* memory='64Gi'"),
    ],
)
def test_the_task_decorator_rejects_bad_memory(kwargs, match) -> None:
    with pytest.raises(ValueError, match=match):
        task(**kwargs)(lambda: 1)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"custom": {"threads": 2}}, "collides with a built-in resource field"),
        ({"custom": {"api_calls": 0}}, "must be a positive integer"),
        ({"custom": {"api_calls": True}}, "must be a positive integer"),
        ({"custom": {"api_calls": "2"}}, "must be a positive integer"),
        ({"gpu_type": "nvidia-tesla-t4"}, "gpu_type requires gpu > 0"),
        ({"threads": 0}, "threads must be at least 1"),
        ({"memory": "4Gi", "memory_retry_multiplier": 0.5}, "multiplier must be at least 1"),
        ({"memory_retry_multiplier": 2}, "requires memory to be set"),
    ],
)
def test_resources_reject_invalid_declarations(kwargs, match) -> None:
    with pytest.raises(ValueError, match=match):
        Resources(**kwargs)


class TestBudgets:
    def test_budgets_parse_from_the_cli_and_config(self) -> None:
        assert parse_resource_budget_args(["api_calls=10", "db=2"]) == {"api_calls": 10, "db": 2}
        assert resource_budgets_from_config({"budgets": {"api_calls": 10}}) == {"api_calls": 10}
        assert resource_budgets_from_config(None) == resource_budgets_from_config({}) == {}

    @pytest.mark.parametrize(
        ("arg", "match"),
        [
            ("api_calls", "expected name=value"),
            ("api_calls=0", "positive integer"),
            ("api_calls=ten", "positive integer"),
            ("gpu=2", "dedicated --gpus run option"),
            ("threads=2", "dedicated --cores run option"),
            ("gpu_type=1", "has no run-level budget"),
        ],
    )
    def test_a_bad_cli_budget_is_rejected(self, arg: str, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            parse_resource_budget_args([arg])

    @pytest.mark.parametrize(
        ("config", "match"),
        [
            ({"budgets": 10}, r"\[resources.budgets\] must be a table"),
            ({"budgets": {"memory": 4}}, "dedicated --memory run option"),
            ({"budgets": {"api_calls": 0}}, "positive integer"),
        ],
    )
    def test_a_bad_config_budget_is_rejected(self, config, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            resource_budgets_from_config(config)

    def test_the_dry_run_totals_custom_demands(self) -> None:
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


class TestOverrides:
    @pytest.mark.parametrize(
        ("config", "match"),
        [
            ("big", r"\[resources\] must be a table"),
            ({"overrides": {"sized_step": 4}}, "must be a table"),
            ({"overrides": {"sized_step": {"cpus": 4}}}, "unknown keys"),
        ],
    )
    def test_a_malformed_config_is_rejected(self, config, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            ResourceOverrides.from_config(config)

    @pytest.mark.parametrize(
        ("overrides", "threads"),
        [
            ({"sized_step": {"threads": 8}}, 8),
            ({"pkg.module.sized_step": {"threads": 8}}, 8),
            ({"sized_*": {"threads": 8}}, 8),
            ({"sized_*": {"threads": 8}, "sized_step": {"threads": 16}}, 16),  # exact beats glob
            ({"sized_*": {"threads": 8}, "*_step": {"threads": 16}}, 8),  # first glob wins
            ({"other": {"threads": 8}}, 2),
        ],
    )
    def test_the_matching_selector_sets_threads(self, overrides, threads: int) -> None:
        merged = ResourceOverrides.from_config({"overrides": overrides}).apply(
            task_name="pkg.module.sized_step", base=Resources(threads=2, memory="4Gi")
        )
        assert merged.threads == threads
        assert merged.memory_gb == 4  # omitted keys keep the declaration

    def test_an_override_replaces_memory_and_custom_demands(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"fetch": {"memory": "16Gi", "custom": {"api_calls": 5}}}}
        )
        merged = overrides.apply(
            task_name="m.fetch", base=Resources(memory="4Gi", custom={"api_calls": 2})
        )
        assert (merged.memory_gb, merged.custom) == (16, {"api_calls": 5})

    def test_an_invalid_merge_names_the_task(self) -> None:
        overrides = ResourceOverrides.from_config(
            {"overrides": {"sized_step": {"gpu_type": "nvidia-tesla-t4"}}}
        )
        with pytest.raises(ValueError, match="invalid resource override for pkg.sized_step"):
            overrides.apply(task_name="pkg.sized_step", base=Resources())

    def test_an_override_in_a_config_file_reaches_the_run(self) -> None:
        Path("workflow.py").write_text(
            "from pathlib import Path\n"
            "from ginkgo import flow, task\n\n"
            "@task(threads=1)\n"
            "def report_threads(threads: int = 1) -> int:\n"
            '    Path("threads.txt").write_text(str(threads), encoding="utf-8")\n'
            "    return threads\n\n"
            "@flow\n"
            "def main():\n"
            "    return report_threads()\n",
            encoding="utf-8",
        )
        Path("overrides.toml").write_text(
            "[resources.overrides.report_threads]\nthreads = 3\n", encoding="utf-8"
        )

        from ginkgo.cli.commands.run import run_workflow

        exit_code = run_workflow(
            workflow_path=Path("workflow.py").resolve(),
            config_paths=[Path("overrides.toml").resolve()],
            jobs=1,
            cores=4,
            memory=None,
            dry_run=False,
        )
        assert exit_code == 0
        assert Path("threads.txt").read_text(encoding="utf-8") == "3"


class TestMemoryEscalation:
    def test_the_multiplier_compounds_per_attempt(self) -> None:
        resources = Resources(memory="4Gi", memory_retry_multiplier=2)
        assert [resources.memory_gb_for_attempt(n) for n in range(3)] == [4, 8, 16]
        assert Resources(memory="4Gi").memory_gb_for_attempt(3) == 4

    @pytest.mark.parametrize(("budget", "effective"), [(32, 8), (6, 6)])
    def test_a_retry_escalates_within_the_budget_and_records_both(
        self, tmp_path: Path, budget: int, effective: int
    ) -> None:
        """The declaration and the escalated budget the retry ran under are both recorded."""
        ledger = Ledger.start(root=tmp_path)
        try:
            evaluator = ConcurrentEvaluator(
                jobs=1, memory=budget, run_dir=ledger.run_dir, event_bus=ledger.bus
            )
            assert evaluator.evaluate(flaky_step(marker=str(tmp_path / "marker"))) == "ok"
            (task_summary,) = ledger.finish().tasks
        finally:
            ledger.close()

        (node,) = evaluator.task_nodes.values()
        assert (node.attempt, node.memory_gb) == (2, effective)
        declared = task_summary.resource_usage["declared"]
        assert (declared["memory_gb"], declared["effective_memory_gb"]) == (4, effective)


@task(retries=1, memory="4Gi", memory_retry_multiplier=2)
def flaky_step(marker: str) -> str:
    path = Path(marker)
    if not path.exists():
        path.write_text("attempted", encoding="utf-8")
        raise RuntimeError("first attempt fails")
    return "ok"


# ----- What a running task sees of its threads -------------------------------


@task(threads=3)
def _report_threads(threads: int = 1) -> int:
    return threads


@task("shell", threads=7)
def _write_threads(out_path: str) -> str:
    return shell(cmd=f'sh -c "echo $GINKGO_THREADS > {out_path}"', output=out_path)


@task("shell", threads=5)
def _shell_default() -> str:
    return shell(cmd="true", output="out.txt")


@task("shell", threads=6, export_thread_env=True)
def _shell_export() -> str:
    return shell(cmd="true", output="out.txt")


def test_declared_threads_reach_the_python_and_shell_task(tmp_path: Path) -> None:
    out_path = tmp_path / "threads.txt"

    assert evaluate([_report_threads(), _write_threads(out_path=str(out_path))], cores=8)[0] == 3
    assert out_path.read_text(encoding="utf-8").strip() == "7"


def test_the_shell_environment_exports_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GINKGO_TEST_SENTINEL", "sentinel-value")

    default = build_shell_subprocess_env(task_def=_shell_default)
    exported = build_shell_subprocess_env(task_def=_shell_export)

    assert default["GINKGO_THREADS"] == "5"
    assert default.get("OMP_NUM_THREADS", "") != "5"
    assert default["GINKGO_TEST_SENTINEL"] == "sentinel-value"
    for name in ("GINKGO_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"):
        assert exported[name] == "6"
    assert exported["NUMEXPR_NUM_THREADS"] == "6"
