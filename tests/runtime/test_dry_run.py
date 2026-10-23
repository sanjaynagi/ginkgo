"""A dry run predicts what the run will do and why, without running a task body.

- #343: a consumer wired to a cached producer through ``.output["name"]`` is probed.
- R2: script tasks are predicted from a statically evaluated directive.
- P4: a hand edit inside an upstream folder output does not fool the probe.
- M3: a task that will run carries the reason its key misses.
- M6: a task over the run's core or memory budget is a diagnostic.
- G1: a cached dynamic task's children are probed, each on its own.
- #232: once an upstream is cached, its consumer's inputs are validated.
- #294: an unexpected probe error is reported, not swallowed.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from rich.console import Console

import ginkgo
from ginkgo import Out, file, folder, shell, task, untracked
from ginkgo.cli.app import main as cli_main
from ginkgo.cli.renderers.dry_run import render_dry_run_plan
from ginkgo.core.expr import record_constructed_calls
from ginkgo.runtime.caching.cache import CacheStore
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


@task()
def make(n: int, data: Out[file], log: Out[file]) -> int:
    Path(data).parent.mkdir(parents=True, exist_ok=True)
    Path(data).write_text(str(n))
    Path(log).write_text("ok")
    return n


@task()
def use(p: file) -> int:
    return len(Path(p).read_text())


@task()
def collect(logs: list[file]) -> int:
    return sum(len(Path(p).read_text()) for p in logs)


@task("shell")
def make_shell(out: Out[file]):
    return shell(cmd=f"mkdir -p out && echo made > {out}")


@task()
def produce_file(output_path: str) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("payload")
    return output_path


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


@task(threads=4)
def busy(i: int) -> int:
    return i


@task(threads=8, memory="64Gi")
def big() -> int:
    return 1


@task()
def fan_out(n: int) -> list[int]:
    return [busy(i=i) for i in range(n)]


@task()
def read_parts(n: int) -> list:
    return [read_tag(src=f"part{i}.txt") for i in range(n)]


@task()
def total_length(parts: list) -> int:
    return sum(len(part) for part in parts)


@task()
def produce_summary(output_path: untracked) -> object:
    """Return a DataFrame; ``untracked`` keeps the staging path out of the key."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north", "south"], "count": [10, 20]}).to_csv(out, index=False)
    return pd.read_csv(out)


@task()
def consume_as_file(summary: file) -> str:
    return str(summary)


@task()
def consume_as_object(summary: object) -> str:
    return str(type(summary))


@task(kind="script")
def consume_in_script(summary: file, output_path: str) -> file:
    return ginkgo.script(path="analyse.py", output=output_path)


def single_flow():
    return use(p=make(n=1, data="out/d.txt", log="out/l.txt").output["data"])


def mapped_flow():
    runs = make().map(
        n=[1, 2], data=["out/d1.txt", "out/d2.txt"], log=["out/l1.txt", "out/l2.txt"]
    )
    return collect(logs=runs.output["log"])


def summary():
    return produce_summary(output_path="results/summary.csv")


def _plan(build, **evaluator_options) -> DryRunPlan:
    """Plan *build* (an expression, or a flow body to record calls from)."""
    with record_constructed_calls() as calls:
        expr = build() if callable(build) else build
    evaluator = ConcurrentEvaluator(**evaluator_options)
    evaluator.build_graph(expr)
    evaluator.validate_graph()
    return build_dry_run_plan(
        evaluator=evaluator,
        workflow_label="workflow.py",
        unreachable_calls=evaluator.graph.unreachable_calls(calls),
    )


def _status(plan: DryRunPlan) -> dict[str, tuple[str, tuple[str, ...]]]:
    return {
        task.label: (task.cache_status, task.run_reasons) for w in plan.waves for task in w.tasks
    }


def _rendered(plan: DryRunPlan, *, verbose: bool = False) -> str:
    console = Console(record=True, width=200)
    render_dry_run_plan(plan=plan, console=console, verbose=verbose)
    return console.export_text()


CACHED = ("cached", ())


@pytest.fixture
def project(tmp_path: Path) -> Path:
    (tmp_path / "write_tag.py").write_text(SCRIPT)
    (tmp_path / "part0.txt").write_text("a")
    (tmp_path / "part1.txt").write_text("bb")
    return tmp_path


@pytest.fixture
def warm(project: Path) -> Path:
    """Run every upstream the warm-cache tests plan against, in one evaluation."""
    ginkgo.evaluate(
        [
            single_flow(),
            mapped_flow(),
            make(n=3, data="out/x.txt", log="out/y.txt"),
            use(p=make_shell(out="out/s.txt")),
            read_tag(src=write_tag(tag="v1", out="tag.txt")),
            write_tag_dynamically(tag="v1", out="tag2.txt"),
            list_dir(d=make_dir(out="results/d")),
            use(p=produce_file(output_path="results/out.txt")),
            double(n=1),
            total_length(parts=read_parts(n=2)),
            summary(),
            big(),
        ],
        cores=8,
        memory=64,
    )
    return project


class TestWarmCache:
    def test_cached_tasks_are_predicted_and_their_consumers_validated(self, warm: Path) -> None:
        for flow in (single_flow, mapped_flow):
            plan = _plan(flow)
            assert plan.probe_failures == ()
            assert set(_status(plan).values()) == {CACHED}
        assert _status(_plan(read_tag(src=write_tag(tag="v1", out="tag.txt")))) == {
            "write_tag": CACHED,
            "read_tag": CACHED,
        }
        # A body the dry run cannot evaluate statically makes no claim.
        assert _status(_plan(write_tag_dynamically(tag="v1", out="tag2.txt"))) == {
            "write_tag_dynamically": ("unknown", ())
        }
        # A consumer of a cached producer with new inputs will run.
        plan = _plan(use(p=make(n=3, data="out/x.txt", log="out/y.txt").output["data"]))
        assert _status(plan) == {"make": CACHED, "use": ("will_run", ("input_changed",))}
        # The run serves a cached task from cache and never holds it to the budget.
        assert _plan(big(), cores=2, memory=4).diagnostics == ()

        # Validation: a cached value bound to a `file` parameter is a diagnostic.
        plan = _plan(lambda: consume_as_file(summary=summary()))
        (diagnostic,) = plan.diagnostics
        assert diagnostic.label.startswith("consume_as_file")
        assert "annotated `file` but received a pandas.DataFrame" in diagnostic.message
        assert _status(plan)["produce_summary"] == CACHED
        assert _status(plan)["consume_as_file"][0] == "will_run"
        text = _rendered(plan)
        assert "Problems — this run would fail" in text
        assert "DataFrame" in text

        # A driver task's inputs validate before its directive is considered,
        # and each failing task gets its own diagnostic.
        def both():
            shared = summary()
            return [
                consume_as_file(summary=shared),
                consume_in_script(summary=shared, output_path="out.txt"),
            ]

        plan = _plan(both)
        assert sorted(d.label.split("(")[0] for d in plan.diagnostics) == [
            "consume_as_file",
            "consume_in_script",
        ]
        assert _status(plan)["consume_in_script"][0] == "will_run"
        assert _plan(lambda: consume_as_object(summary=summary())).diagnostics == ()

    def test_changes_since_the_run_are_predicted_as_the_run_would_see_them(
        self, warm: Path
    ) -> None:
        # A cached dynamic task's children are probed, each on its own.
        plan = _plan(total_length(parts=read_parts(n=2)))
        assert _status(plan) == {
            "read_parts": CACHED,
            "read_tag[part0.txt]": CACHED,
            "read_tag[part1.txt]": CACHED,
            "total_length": CACHED,
        }
        assert plan.task_count == 4

        rerun = _status(
            _plan(total_length(parts=read_parts(n=2)), rerun=frozenset({"read_tag[part1.txt]"}))
        )
        assert rerun["read_tag[part1.txt]"] == ("will_run", ("rerun_requested",))
        assert rerun["read_tag[part0.txt]"] == CACHED

        (warm / "part1.txt").write_text("changed")
        plan = _plan(total_length(parts=read_parts(n=2)))
        assert _status(plan) == {
            "read_parts": CACHED,
            "read_tag[part0.txt]": CACHED,
            "read_tag[part1.txt]": ("will_run", ("input_changed",)),
            "total_length": ("unknown", ("upstream_pending",)),
        }
        # Children come after the task that returns them, its dependents after them.
        assert [[task.label for task in wave.tasks] for wave in plan.waves] == [
            ["read_parts"],
            ["read_tag[part0.txt]", "read_tag[part1.txt]"],
            ["total_length"],
        ]

        assert _status(_plan(double(n=2))) == {"double": ("will_run", ("input_changed",))}

        # The run restores a cached output before its consumer reads it.
        Path("out/s.txt").write_text("rewritten")
        assert set(_status(_plan(use(p=make_shell(out="out/s.txt")))).values()) == {CACHED}
        (warm / "results" / "d" / "extra.txt").write_text("hand edit")
        assert set(_status(_plan(list_dir(d=make_dir(out="results/d")))).values()) == {CACHED}
        # The probe read a stored copy and left the working tree alone.
        assert (warm / "results" / "d" / "extra.txt").read_text() == "hand edit"

        # A deleted-but-restorable output is an honest "cannot know", not a failure.
        Path("results/out.txt").unlink()
        plan = _plan(use(p=produce_file(output_path="results/out.txt")))
        assert (plan.probe_failures, plan.diagnostics) == ((), ())
        assert _status(plan)["use"][0] == "unknown"

        (warm / "write_tag.py").write_text(SCRIPT + "# edited\n")
        plan = _plan(read_tag(src=write_tag(tag="v1", out="tag.txt")))
        assert _status(plan) == {
            "write_tag": ("will_run", ("source_hash_changed",)),
            "read_tag": ("unknown", ("upstream_pending",)),
        }
        text = _rendered(plan)
        assert "[unknown: after upstream]" in text
        assert "served from cache if that task's output comes out unchanged" in text


class TestColdCache:
    def test_a_never_run_task_has_no_prior_entry_grouped_across_a_fan_out(
        self, project: Path
    ) -> None:
        assert _status(_plan(double(n=1))) == {"double": ("will_run", ("no_prior_entry",))}
        assert "[will run: no prior entry]" in _rendered(_plan(double(n=1)))
        text = _rendered(_plan(double().map(n=list(range(10)))))
        assert "double × 10   10 will run (10 no prior entry)" in text

    def test_a_consumer_of_an_uncached_upstream_proves_nothing(self, project: Path) -> None:
        plan = _plan(lambda: consume_as_file(summary=summary()))
        assert plan.diagnostics == ()
        assert _status(plan)["consume_as_file"][0] == "unknown"

    @pytest.mark.parametrize(
        ("cores", "memory", "messages"),
        [
            (2, 4, ["requires 8 cores but only 2 are available"]),
            (8, 4, ["requires 64 GiB but only 4 GiB are available"]),
            (8, 64, []),
        ],
    )
    def test_a_task_over_the_run_budget_is_a_diagnostic(
        self, project: Path, cores: int, memory: int, messages: list[str]
    ) -> None:
        plan = _plan(big(), cores=cores, memory=memory)
        assert [d.message for d in plan.diagnostics] == [f"{big.name} {m}" for m in messages]

    def test_the_resource_summary_reports_requested_threads_not_cores(self, project: Path) -> None:
        text = _rendered(_plan(lambda: busy().map(i=[1, 2, 3])))
        assert "12 threads requested at peak (wave 1)" in text
        assert "cores peak" not in text

        for name in ("a.txt", "b.txt", "c.txt"):
            Path(name).write_text(name)
        text = _rendered(_plan(lambda: use().map(p=["a.txt", "b.txt", "c.txt"])))
        assert "up to 3 tasks at once, one thread each (wave 1)" in text
        assert "threads requested" not in text

    def test_a_task_that_calls_tasks_is_named_as_adding_tasks_at_run_time(
        self, project: Path
    ) -> None:
        plan = _plan(lambda: fan_out(n=3))
        assert plan.fan_out_labels == ("fan_out",)
        assert "fan_out calls tasks in its body" in _rendered(plan)
        assert _plan(single_flow).fan_out_labels == ()


class TestProbeFailures:
    def test_an_unexpected_error_is_recorded_and_degrades_to_unknown(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clean = _plan(produce_file(output_path="results/out.txt"))
        assert clean.probe_failures == ()
        assert "Cache status could not be determined" not in _rendered(clean, verbose=True)

        def boom(self, *, node):
            raise RuntimeError("resolver exploded")

        monkeypatch.setattr(ConcurrentEvaluator, "resolve_probe_args", boom)
        plan = _plan(produce_file(output_path="results/out.txt"))

        (failure,) = plan.probe_failures
        assert failure.label.startswith("produce_file")
        assert failure.task_name.endswith("produce_file")
        assert (failure.stage, failure.exception_type) == ("resolve_args", "RuntimeError")
        assert failure.message == "resolver exploded"
        assert _status(plan)["produce_file"][0] == "unknown"
        # Probe failures stay out of the diagnostics that fail the command.
        assert plan.diagnostics == ()

        text = _rendered(plan)
        assert "Cache status could not be determined for 1 task" in text
        assert "resolver exploded" not in text
        assert "Problems" not in text
        verbose = _rendered(plan, verbose=True)
        assert "RuntimeError: resolver exploded" in verbose
        assert "resolve_args" in verbose

    def test_a_cache_key_error_is_recorded_at_its_stage(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(self, **kwargs):
            raise KeyError("missing_input")

        monkeypatch.setattr(CacheStore, "build_cache_key", boom)

        (failure,) = _plan(produce_file(output_path="results/out.txt")).probe_failures
        assert (failure.stage, failure.exception_type) == ("cache_key", "KeyError")
        assert "missing_input" in failure.message


SWITCHED_WORKFLOW = """
from pathlib import Path

import pandas as pd
from ginkgo import file, flow, param, task, untracked

mode = param("mode", default="produce")


@task()
def produce_summary(output_path: untracked) -> object:
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"site": ["north"], "count": [1]}).to_csv(out, index=False)
    return pd.read_csv(out)


@task()
def consume_as_file(summary: file) -> str:
    return str(summary)


@flow
def main():
    summary = produce_summary(output_path="results/summary.csv")
    if mode == "produce":
        return summary
    return consume_as_file(summary=summary)
"""


def test_a_dry_run_that_proves_a_failure_exits_nonzero(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    Path("wf.py").write_text(SWITCHED_WORKFLOW.strip() + "\n", encoding="utf-8")
    dry_run = ["run", "wf.py", "--dry-run", "--mode", "full"]

    assert cli_main(dry_run) == 0  # a cold cache proves nothing
    assert cli_main(["run", "wf.py"]) == 0
    capsys.readouterr()

    assert cli_main(dry_run) == 1
    assert "Problems — this run would fail" in capsys.readouterr().out

    # The scripted mode must not report success on a run the plan refuses.
    assert cli_main([*dry_run, "--agent-output"]) == 1
    out = capsys.readouterr().out
    assert "task_notice" in out
    assert '"status": "failed"' in out or '"status":"failed"' in out
