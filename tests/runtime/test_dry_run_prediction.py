"""A warm-cache dry run predicts what the run will do, and says why.

R2: script tasks are predicted from a statically evaluated directive.
P4: a hand edit inside an upstream folder output does not fool the probe.
M3: a task that will run carries the reason its key misses.
M6: a task over the run's core or memory budget is a diagnostic.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from rich.console import Console

import ginkgo
from ginkgo import Out, file, folder, task
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.runtime.dry_run import DryRunPlan, build_dry_run_plan
from ginkgo.runtime.evaluator import ConcurrentEvaluator

SCRIPT = """\
import argparse
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--out")
parser.add_argument("--tag")
args = parser.parse_args()
Path(args.out).write_text(args.tag)
"""


@task(kind="script")
def write_tag(tag: str, out: Out[file]) -> file:
    return ginkgo.script("write_tag.py", output=out)


@task(kind="script")
def write_tag_dynamically(tag: str, out: Out[file]) -> file:
    path = "write_tag.py"
    return ginkgo.script(path, output=out)


@task()
def read_tag(src: file) -> str:
    return Path(src).read_text()


@task()
def make_dir(out: Out[folder]) -> folder:
    Path(out).mkdir(parents=True, exist_ok=True)
    (Path(out) / "a.txt").write_text("a")
    return out


@task()
def list_dir(d: folder) -> list[str]:
    return sorted(p.name for p in Path(d).iterdir())


@task()
def double(n: int) -> int:
    return n * 2


@task(threads=8, memory="64Gi")
def big() -> int:
    return 1


@task()
def read_parts(n: int) -> list:
    return [read_tag(src=f"part{i}.txt") for i in range(n)]


@task()
def total_length(parts: list) -> int:
    return sum(len(part) for part in parts)


def _plan(expr, **evaluator_options) -> DryRunPlan:
    evaluator = ConcurrentEvaluator(**evaluator_options)
    evaluator.build_graph(expr)
    evaluator.validate_graph()
    return build_dry_run_plan(evaluator=evaluator, workflow_label="workflow.py")


def _by_label(plan: DryRunPlan) -> dict[str, tuple[str, tuple[str, ...]]]:
    return {
        task.label: (task.cache_status, task.run_reasons)
        for wave in plan.waves
        for task in wave.tasks
    }


def _tasks(plan: DryRunPlan) -> dict[str, tuple[str, tuple[str, ...]]]:
    return {
        task.base_name: (task.cache_status, task.run_reasons)
        for wave in plan.waves
        for task in wave.tasks
    }


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "write_tag.py").write_text(SCRIPT)
    return tmp_path


class TestScriptTasks:
    def test_a_warm_script_task_and_its_consumer_are_cached(self, project: Path) -> None:
        ginkgo.evaluate(read_tag(src=write_tag(tag="v1", out="tag.txt")))

        plan = _plan(read_tag(src=write_tag(tag="v1", out="tag.txt")))

        assert _tasks(plan) == {"write_tag": ("cached", ()), "read_tag": ("cached", ())}

    def test_an_edited_script_will_run_because_its_source_changed(self, project: Path) -> None:
        ginkgo.evaluate(write_tag(tag="v1", out="tag.txt"))
        (project / "write_tag.py").write_text(SCRIPT + "# edited\n")

        plan = _plan(read_tag(src=write_tag(tag="v1", out="tag.txt")))

        assert _tasks(plan) == {
            "write_tag": ("will_run", ("source_hash_changed",)),
            "read_tag": ("unknown", ("upstream_pending",)),
        }

    def test_a_body_dry_run_cannot_evaluate_stays_unknown(self, project: Path) -> None:
        ginkgo.evaluate(write_tag_dynamically(tag="v1", out="tag.txt"))

        plan = _plan(write_tag_dynamically(tag="v1", out="tag.txt"))

        assert _tasks(plan) == {"write_tag_dynamically": ("unknown", ())}


class TestRunReasons:
    def test_a_task_never_run_has_no_prior_entry(self, project: Path) -> None:
        plan = _plan(double(n=1))

        assert _tasks(plan) == {"double": ("will_run", ("no_prior_entry",))}

    def test_a_new_argument_is_an_input_change(self, project: Path) -> None:
        ginkgo.evaluate(double(n=1))

        plan = _plan(double(n=2))

        assert _tasks(plan) == {"double": ("will_run", ("input_changed",))}

    def test_the_reason_is_rendered_and_grouped_across_a_fan_out(self, project: Path) -> None:
        console = Console(record=True, width=200)
        plan = _plan(double().map(n=list(range(10))))

        render_dry_run_plan(plan=plan, console=console, verbose=False)

        assert "double × 10   10 will run (10 no prior entry)" in console.export_text()

        render_dry_run_plan(plan=_plan(double(n=1)), console=console, verbose=False)

        assert "[will run: no prior entry]" in console.export_text()

    def test_a_task_after_a_will_run_task_says_what_decides_it(self, project: Path) -> None:
        """NB5-2: not a bare unknown — it waits on its upstream task's output."""
        ginkgo.evaluate(write_tag(tag="v1", out="tag.txt"))
        (project / "write_tag.py").write_text(SCRIPT + "# edited\n")
        console = Console(record=True, width=200)

        render_dry_run_plan(
            plan=_plan(read_tag(src=write_tag(tag="v1", out="tag.txt"))),
            console=console,
            verbose=False,
        )

        text = console.export_text()
        assert "[unknown: after upstream]" in text
        assert "served from cache if that task's output comes out unchanged" in text


class TestFolderOutputs:
    def test_a_hand_edit_inside_an_upstream_folder_is_restored_as_the_run_would(
        self, project: Path
    ) -> None:
        """The run restores the cached folder before ``list_dir`` hashes it (P4)."""
        ginkgo.evaluate(list_dir(d=make_dir(out="results/d")))
        (project / "results" / "d" / "extra.txt").write_text("hand edit")

        plan = _plan(list_dir(d=make_dir(out="results/d")))

        assert _tasks(plan) == {"make_dir": ("cached", ()), "list_dir": ("cached", ())}
        # The probe read a stored copy and left the working tree alone.
        assert (project / "results" / "d" / "extra.txt").read_text() == "hand edit"


class TestResourceBudgets:
    def test_a_task_over_the_core_and_memory_budget_is_a_diagnostic(self, project: Path) -> None:
        plan = _plan(big(), cores=2, memory=4)

        assert [diagnostic.message for diagnostic in plan.diagnostics] == [
            f"{big.name} requires 8 cores but only 2 are available"
        ]

    def test_memory_over_budget_is_reported_when_cores_fit(self, project: Path) -> None:
        plan = _plan(big(), cores=8, memory=4)

        assert [diagnostic.message for diagnostic in plan.diagnostics] == [
            f"{big.name} requires 64 GiB but only 4 GiB are available"
        ]

    def test_a_task_within_budget_is_clean(self, project: Path) -> None:
        assert _plan(big(), cores=8, memory=64).diagnostics == ()

    def test_a_cached_task_over_budget_is_clean(self, project: Path) -> None:
        """MG5-5: the run serves it from cache and never holds it to the budget."""
        ginkgo.evaluate(big(), cores=8, memory=64)

        assert _plan(big(), cores=2, memory=4).diagnostics == ()


class TestDynamicTasks:
    """G1: a cached dynamic task's children are probed, each on its own."""

    @pytest.fixture
    def parts(self, project: Path) -> Path:
        (project / "part0.txt").write_text("a")
        (project / "part1.txt").write_text("bb")
        ginkgo.evaluate(total_length(parts=read_parts(n=2)))
        return project

    def test_a_warm_dynamic_task_and_its_children_are_cached(self, parts: Path) -> None:
        plan = _plan(total_length(parts=read_parts(n=2)))

        assert _by_label(plan) == {
            "read_parts": ("cached", ()),
            "read_tag[part0.txt]": ("cached", ()),
            "read_tag[part1.txt]": ("cached", ()),
            "total_length": ("cached", ()),
        }
        assert plan.task_count == 4

    def test_a_stale_child_will_run_under_its_cached_parent(self, parts: Path) -> None:
        (parts / "part1.txt").write_text("changed")

        plan = _plan(total_length(parts=read_parts(n=2)))

        assert _by_label(plan) == {
            "read_parts": ("cached", ()),
            "read_tag[part0.txt]": ("cached", ()),
            "read_tag[part1.txt]": ("will_run", ("input_changed",)),
            "total_length": ("unknown", ("upstream_pending",)),
        }
        # Children come after the task that returns them, and its dependents
        # after the children.
        assert [[task.label for task in wave.tasks] for wave in plan.waves] == [
            ["read_parts"],
            ["read_tag[part0.txt]", "read_tag[part1.txt]"],
            ["total_length"],
        ]

    def test_rerun_marks_the_named_task_will_run(self, parts: Path) -> None:
        plan = _plan(total_length(parts=read_parts(n=2)), rerun=frozenset({"read_tag[part1.txt]"}))

        assert _by_label(plan)["read_tag[part1.txt]"] == ("will_run", ("rerun_requested",))
        assert _by_label(plan)["read_tag[part0.txt]"] == ("cached", ())
