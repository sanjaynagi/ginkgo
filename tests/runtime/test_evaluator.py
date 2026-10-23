"""Unit tests for the evaluator runtime.

Each ``evaluate`` that runs a python task starts a fresh worker process, so
tests here fold related checks into one evaluation where they can.
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ginkgo import (
    Out,
    ScriptDirective,
    SubWorkflowDirective,
    evaluate,
    file,
    folder,
    script,
    secret,
    shell,
    task,
    tmp_dir,
    untracked,
)
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.environment.secrets import build_secret_resolver
from ginkgo.runtime.evaluator import ConcurrentEvaluator, RootSkippedError
from ginkgo.runtime.events import (
    TaskCacheHit,
    TaskCompleted,
    TaskNotice,
    TaskPlanned,
    TaskRetrying,
    TaskSkipped,
)
from ginkgo.runtime.graph import CycleError
from ginkgo.runtime.task_runners.shell import ShellTaskError
from tests._vw_support import append_line
from tests.conftest import EventCollector, Ledger, ShellEnvironment


def _lines(path: str | Path) -> list[str]:
    return Path(path).read_text(encoding="utf-8").splitlines()


def _wait_for(marker_path: str, *, then: float = 0.0) -> None:
    """Block until *marker_path* exists, then sleep *then* seconds more."""
    for _ in range(1000):
        if Path(marker_path).exists():
            break
        time.sleep(0.01)
    time.sleep(then)


@task("script")
def python_script_task(*, script_path: str, output_path: str) -> Path:
    return script(script_path, output=output_path)


@task("script", env="test_env")
def python_script_env_task(*, script_path: str, output_path: str) -> Path:
    return script(script_path, output=output_path)


@task()
def add_one_task(x: int) -> int:
    return x + 1


@task()
def logged_work_task(x: int, log_path: untracked) -> int:
    append_line(log_path, f"work:{x}")
    return x + 1


@task()
def fit_site_species_trend_task(site: str, species: str) -> str:
    return f"{site}:{species}"


@task()
def label_source_task(x: int) -> int:
    return x


@task()
def label_branch_task(x: int) -> int:
    return x * 10


@task()
def sum_keyword_pair_task(*, left: int, right: int) -> int:
    return left + right


@task()
def make_pair_task(x: int) -> tuple[int, int]:
    return (x * 10, x * 100)


@task()
def sum_pair_task(a: int, b: int) -> int:
    return a + b


@task(retries=2)
def flaky_retry_task(marker_path: untracked, log_path: untracked) -> str:
    append_line(log_path, "attempt")
    marker = Path(marker_path)
    if not marker.exists():
        marker.write_text("1", encoding="utf-8")
        raise RuntimeError("transient failure")
    return "ok"


@task(retries=2)
def always_fail_retry_task(log_path: untracked) -> str:
    append_line(log_path, "attempt")
    raise RuntimeError("still broken")


@task()
def always_fail_once_task(log_path: untracked) -> str:
    append_line(log_path, "attempt")
    raise RuntimeError("no retry configured")


@task(on_failure="ignore")
def ignored_failure_task(log_path: untracked) -> int:
    append_line(log_path, "ignored-attempt")
    raise RuntimeError("bad input")


@task(on_failure="ignore", retries=2)
def ignored_failure_with_retries_task(log_path: untracked) -> int:
    append_line(log_path, "attempt")
    raise RuntimeError("still broken")


@task()
def double_task(value: int, log_path: untracked) -> int:
    append_line(log_path, f"double:{value}")
    return value * 2


@task()
def expand_into_a_failure_task(log_path: untracked) -> object:
    append_line(log_path, "expanded")
    return [logged_work_task(x=1, log_path=log_path), ignored_failure_task(log_path=log_path)]


@task()
def fatal_failure_marker_task(marker_path: untracked, log_path: untracked) -> int:
    append_line(log_path, "fatal")
    Path(marker_path).write_text("failed", encoding="utf-8")
    raise RuntimeError("fatal boom")


@task(on_failure="ignore")
def ignored_failure_after_marker_task(marker_path: untracked, log_path: untracked) -> int:
    _wait_for(marker_path)
    append_line(log_path, "late")
    raise RuntimeError("late boom")


@task(retries=2)
def retrying_failure_after_marker_task(marker_path: untracked, log_path: untracked) -> int:
    # Let the fatal failure land first, so this one arrives at a stopping run.
    _wait_for(marker_path, then=0.3)
    append_line(log_path, "late")
    raise RuntimeError("late boom")


@task()
def work_after_marker_task(marker_path: untracked, log_path: untracked) -> int:
    # Finish after the fatal failure, while the stopping run drains.
    _wait_for(marker_path, then=0.3)
    append_line(log_path, "late-work")
    return 1


@task(retries=2, retry_backoff=60.0)
def slow_retry_failure_task(marker_path: untracked, log_path: untracked) -> int:
    append_line(log_path, "retrier")
    Path(marker_path).write_text("failed", encoding="utf-8")
    raise RuntimeError("flaky boom")


@task()
def fatal_failure_after_marker_task(marker_path: untracked, log_path: untracked) -> int:
    # Fail once the other task's retry is scheduled.
    _wait_for(marker_path, then=0.3)
    append_line(log_path, "fatal")
    raise RuntimeError("fatal boom")


@task()
def join_pair_task(left: int, right: int, log_path: untracked) -> int:
    append_line(log_path, "joined")
    return left + right


@task(retries=2, retry_on=IOError)
def retry_only_ioerror_task(log_path: untracked, kind: str) -> str:
    append_line(log_path, "attempt")
    if kind == "ioerror":
        raise IOError("transient io")
    raise ValueError("unexpected value")


@task(retries=3, retry_backoff=0.05, retry_backoff_multiplier=2.0)
def flaky_with_backoff_task(marker_path: untracked, log_path: untracked) -> str:
    append_line(log_path, f"attempt:{time.monotonic():.6f}")
    marker = Path(marker_path)
    failures = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
    if failures < 2:
        marker.write_text(str(failures + 1), encoding="utf-8")
        raise RuntimeError("still transient")
    return "ok"


@task(kind="shell", retries=2, retry_on_exit_codes=(7,))
def shell_retry_on_exit_code_task(marker_path: untracked, output_path: str) -> file:
    return shell(
        cmd=f"[ -f {marker_path} ] || {{ touch {marker_path}; exit 7; }}; printf x > {output_path}",
        output=output_path,
    )


@task(kind="shell", retries=2, retry_on_exit_codes=(137,))
def shell_retry_skip_mismatched_exit_code_task(output_path: str, log_path: untracked) -> file:
    return shell(cmd=f"echo attempt >> {log_path}; exit 7", output=output_path)


@task(kind="shell", retries=2)
def flaky_shell_task(marker_path: untracked, output_path: str, log_path: untracked) -> file:
    return shell(
        cmd=(
            f"if [ ! -f {marker_path} ]; then touch {marker_path}; "
            "printf 'transient shell failure\\n' 1>&2; exit 7; "
            f"fi; printf 'captured stdout\\n'; printf 'payload' > {output_path}"
        ),
        output=output_path,
        log=log_path,
    )


@task(kind="shell")
def shell_command_task(command: str, output_path: str) -> file:
    return shell(cmd=command.format(out=output_path), output=output_path)


@task(kind="shell")
def shell_write_multiple_outputs_task(
    output_one: Out[file], output_two: Out[file], marker_path: untracked
) -> tuple[file, file]:
    return shell(
        cmd=(
            f"printf 'left' > {output_one}; printf 'right' > {output_two}; "
            f"printf 'run\\n' >> {marker_path}"
        ),
        output=(output_one, output_two),
    )


@task(kind="shell")
def shell_write_list_outputs_task(output_one: str, output_two: str) -> list[file]:
    return shell(
        cmd=f"printf 'left' > {output_one}; printf 'right' > {output_two}",
        output=[output_one, output_two],
    )


@task(kind="shell")
def shell_missing_multiple_outputs_task(output_one: str, output_two: str) -> tuple[file, file]:
    return shell(cmd=f"printf 'left' > {output_one}", output=(output_one, output_two))


@task(kind="shell")
def shell_returns_script_directive_task(output_path: str) -> file:
    return script("copy.py", output=output_path)


@task()
def python_returns_shell_directive_task(output_path: str) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


@task()
def python_returns_script_directive_task(output_path: str) -> None:
    return ScriptDirective(
        path=Path("/fake.py"), output=None, log=None, interpreter="python", source_hash="test"
    )


@task()
def python_returns_subworkflow_directive_task(output_path: str) -> None:
    return SubWorkflowDirective(path="fake/workflow.py")


@task(kind="shell")
def shell_returns_plain_value_task(output_path: str) -> str:
    return "plain-value"


@task()
def read_text_task(path: file) -> str:
    return Path(path).read_text()


@task()
def write_multiple_files_with_scalar_annotation(output_paths: list[str]) -> file:
    for index, output_path in enumerate(output_paths):
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(f"payload-{index}", encoding="utf-8")
    return output_paths


@task()
def list_dir_task(path: folder) -> list[str]:
    return sorted(child.name for child in Path(path).iterdir())


@task()
def write_spaced_output(dest: Out[file]) -> file:
    Path(dest).write_text("ok", encoding="utf-8")
    return dest


@task(kind="shell")
def touch_via_shell(dest: Out[file]) -> file:
    return shell(cmd=f"touch {dest}", output=dest)


@task()
def write_scratch_task(report_path: str, scratch: tmp_dir, fail: bool = False) -> str:
    (Path(scratch) / "marker.txt").write_text("ok", encoding="utf-8")
    Path(report_path).write_text(str(scratch), encoding="utf-8")
    if fail:
        raise RuntimeError("boom")
    return str(scratch)


@task()
def array_task(start: int) -> object:
    return np.arange(start, start + 3)


@task()
def sum_array_task(values: object) -> int:
    return int(np.asarray(values).sum())


@task()
def dataframe_task(log_path: untracked, start: int) -> object:
    append_line(log_path, f"df:{start}")
    return pd.DataFrame({"sample": [start, start + 1], "value": [start * 2, start * 2 + 1]})


@task()
def logged_dataframe_total_task(log_path: untracked, df: object) -> int:
    append_line(log_path, "total")
    return int(df["value"].sum())


@task()
def passthrough_task(value: object | None = None) -> object:
    return value


@task()
def reveal_secret_task(secret_value: str, output_path: Out[file]) -> file:
    print(f"stdout:{secret_value}")
    print(f"stderr:{secret_value}", file=sys.stderr)
    Path(output_path).write_text(secret_value, encoding="utf-8")
    return file(output_path)


@task("shell")
def echo_secret_task(secret_value: str, output_path: Out[file]):
    return shell(cmd=f"echo shell-secret:{secret_value}; echo {secret_value} >&2; exit 3")


@task()
def fail_with_secret_task(secret_value: str) -> str:
    print(f"task-secret:{secret_value}")
    raise RuntimeError(f"failure:{secret_value}")


@task()
def build_dynamic_cycle_task() -> object:
    first = passthrough_task()
    second = passthrough_task(value=first)
    first.args["value"] = second
    return first


@task()
def _read_path(*, path: file) -> str:
    return str(path)


@task()
def _write_path(*, output_path: Out[file]) -> file:
    return file(output_path)


def _ledger_evaluator(tmp_path: Path, **kwargs) -> tuple[Ledger, ConcurrentEvaluator]:
    ledger = Ledger.start(root=tmp_path)
    kwargs.setdefault("jobs", 1)
    kwargs.setdefault("cores", kwargs["jobs"])
    return ledger, ConcurrentEvaluator(run_dir=ledger.run_dir, event_bus=ledger.bus, **kwargs)


class TestEvaluate:
    def test_containers_and_output_indexes_resolve(self):
        pair = make_pair_task(x=3)
        pairs = make_pair_task().map(x=[1, 2])
        result = evaluate(
            {
                "single": add_one_task(x=1),
                "list": [add_one_task(x=2), add_one_task(x=3)],
                "tuple": (add_one_task(x=4), "literal"),
                "indexed": sum_pair_task(a=pair.output[0], b=pair.output[1]),
                "one": add_one_task(x=pair.output[0]),
                "mapped": sum_pair_task().map(a=pairs.output[0], b=pairs.output[1]),
            }
        )

        assert result == {
            "single": 2,
            "list": [3, 4],
            "tuple": (5, "literal"),
            "indexed": 330,
            "one": 31,
            "mapped": [110, 220],
        }

    def test_two_axis_zip_map_display_label_reaches_the_store(self, tmp_path: Path) -> None:
        """Issue #286: a two-axis zip fan-out's label lands in the ledger, per branch."""
        ledger, evaluator = _ledger_evaluator(tmp_path)
        # The grid is crossed by hand, so the first axis alone cannot tell branches apart.
        result = evaluator.evaluate(
            fit_site_species_trend_task().map(
                site=["north_fen", "north_fen", "south_bog", "south_bog"],
                species=["sedge", "sphagnum", "sedge", "sphagnum"],
            )
        )
        ledger.finish()

        assert result[1] == "north_fen:sphagnum"
        assert [row["display_label"] for row in ledger.tasks().values()] == [
            "fit_site_species_trend_task[north_fen,sedge]",
            "fit_site_species_trend_task[north_fen,sphagnum]",
            "fit_site_species_trend_task[south_bog,sedge]",
            "fit_site_species_trend_task[south_bog,sphagnum]",
        ]
        ledger.close()

    def test_branches_labelled_at_prepare_never_repeat_a_label(self, tmp_path: Path) -> None:
        """A branch over upstream results is named by its resolved value, and
        that name must not collide with a sibling fan-out's."""
        ledger, evaluator = _ledger_evaluator(tmp_path)
        expr = [
            label_branch_task().map(x=[1, 2]),
            label_branch_task().map(x=[label_source_task(x=1), label_source_task(x=2)]),
        ]

        assert evaluator.evaluate(expr) == [[10, 20], [10, 20]]
        summary = ledger.finish()
        ledger.close()

        labels = [task.label for task in summary.tasks if task.base_name == "label_branch_task"]
        assert len(labels) == len(set(labels)) == 4

    def test_script_task_with_env_uses_env_python_not_scheduler_interpreter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue #120: a declared env= controls which Python runs the script."""
        script_path = tmp_path / "fit.py"
        script_path.write_text("# placeholder script\n", encoding="utf-8")
        output_path = tmp_path / "out.txt"
        calls: list[str] = []

        def fake_run_subprocess(*, argv: list[str], **kwargs: Any):
            calls.append(str(argv))
            output_path.write_text("done\n", encoding="utf-8")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")

        evaluator = ConcurrentEvaluator(jobs=1, cores=1, backend=ShellEnvironment())
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)
        evaluator.evaluate(
            python_script_env_task(script_path=str(script_path), output_path=str(output_path))
        )

        [argv] = calls
        assert sys.executable not in argv
        assert "python" in argv

    def test_local_task_fails_immediately_at_runtime(self):
        @task()
        def local_task(x: int) -> int:
            return x + 1

        with pytest.raises(TypeError, match="top-level function"):
            evaluate(local_task(x=1))

    @pytest.mark.parametrize(
        ("make", "match"),
        [
            (
                shell_returns_script_directive_task,
                r"kind='shell' and must return shell\(\.\.\.\), not ScriptDirective\. "
                r'Use @task\("script"\)',
            ),
            (python_returns_shell_directive_task, r'ShellDirective.*Use @task\("shell"\)'),
            (python_returns_script_directive_task, r'ScriptDirective.*Use @task\("script"\)'),
            (
                python_returns_subworkflow_directive_task,
                r'SubWorkflowDirective.*Use @task\("subworkflow"\)',
            ),
            (shell_returns_plain_value_task, r"must return shell\(\.\.\.\) or dynamic"),
        ],
        ids=["shell->script", "python->shell", "python->script", "python->subworkflow", "plain"],
    )
    def test_a_task_must_return_its_own_kinds_directive(self, tmp_path: Path, make, match):
        Path("copy.py").write_text("", encoding="utf-8")
        with pytest.raises(TypeError, match=match):
            evaluate(make(output_path=str(tmp_path / "out")))

    def test_numpy_arrays_cross_the_python_task_boundary(self):
        result = evaluate([array_task(start=2), sum_array_task(values=array_task(start=2))])

        assert np.array_equal(result[0], np.array([2, 3, 4]))
        assert result[1] == 9

    def test_a_dataframe_flows_downstream_and_its_consumer_stays_cached_on_replay(self):
        # The producer's default RangeIndex must survive the cache round trip,
        # or the replayed frame hashes differently and the consumer re-runs.
        log_path = "dataframe-chain.log"

        def build() -> object:
            return logged_dataframe_total_task(
                log_path=log_path, df=dataframe_task(log_path=log_path, start=5)
            )

        assert evaluate(build()) == evaluate(build()) == 21
        assert _lines(log_path) == ["df:5", "total"]

    def test_cached_upstream_tasks_do_not_deadlock_newly_unblocked_downstream(
        self, event_collector: EventCollector
    ) -> None:
        log_path = "fanin-cache.log"
        expr = sum_keyword_pair_task(
            left=logged_work_task(x=1, log_path=log_path),
            right=logged_work_task(x=2, log_path=log_path),
        )

        assert evaluate(expr) == 5
        assert evaluate(expr, event_bus=event_collector.bus) == 5

        # The second run reuses every cached result, so each body ran once.
        assert sorted(_lines(log_path)) == ["work:1", "work:2"]
        assert len([e for e in event_collector.events if isinstance(e, TaskCacheHit)]) == 3
        assert (Path(".ginkgo") / "cache").exists()

    @pytest.mark.parametrize(
        "wrap", [lambda expr: expr, lambda expr: {"nested": [expr]}], ids=["direct", "nested"]
    )
    def test_cycles_are_rejected(self, wrap):
        first = passthrough_task()
        first.args["value"] = wrap(passthrough_task(value=first))
        with pytest.raises(
            CycleError,
            match="Detected cycle in workflow graph: passthrough_task -> passthrough_task",
        ):
            evaluate(first)

    def test_cycles_from_dynamic_returned_expressions_are_rejected(self):
        with pytest.raises(CycleError, match="Detected cycle in workflow graph"):
            evaluate(build_dynamic_cycle_task())


class TestRetries:
    def test_a_transient_failure_is_retried_recorded_and_then_cached(
        self, tmp_path: Path, event_collector: EventCollector
    ) -> None:
        """A7: a retried task's failed attempts are not lost behind its success."""
        ledger, evaluator = _ledger_evaluator(tmp_path)
        ledger.bus.subscribe(event_collector.bus.emit)
        expr = flaky_retry_task(marker_path="flaky.marker", log_path="flaky.log")

        assert evaluator.evaluate(expr) == "ok"

        assert _lines("flaky.log") == ["attempt", "attempt"]
        retries = [e for e in event_collector.events if isinstance(e, TaskRetrying)]
        assert [e.attempt for e in retries] == [1]
        planned = [e for e in event_collector.events if isinstance(e, TaskPlanned)]
        assert len(planned) == len(event_collector.started()) == 2
        history = ledger.finish().to_payload()["tasks"][0]["attempt_history"]
        ledger.close()
        assert [(entry["attempt"], entry["status"]) for entry in history] == [
            (1, "failed"),
            (2, "succeeded"),
        ]
        assert history[0]["failure"]["message"] == "RuntimeError: transient failure"

        rerun = EventCollector()
        assert evaluate(expr, event_bus=rerun.bus) == "ok"
        assert _lines("flaky.log") == ["attempt", "attempt"]
        assert rerun.cached()

    def test_the_final_attempt_fails_and_each_retry_is_marked_in_the_logs(
        self, tmp_path: Path
    ) -> None:
        ledger, evaluator = _ledger_evaluator(tmp_path)

        with pytest.raises(RuntimeError, match="still broken"):
            evaluator.evaluate(always_fail_retry_task(log_path="marked.log"))
        ledger.close()

        assert _lines("marked.log") == ["attempt"] * 3
        node = evaluator.task_nodes[0]
        for path in (node.stdout_path, node.stderr_path):
            markers = [line for line in _lines(path) if line.startswith("--- attempt")]
            assert markers == ["--- attempt 2/3 ---", "--- attempt 3/3 ---"]

    def test_retry_on_narrows_by_exception_class(self):
        with pytest.raises(IOError, match="transient io"):
            evaluate(retry_only_ioerror_task(log_path="io.log", kind="ioerror"))
        assert _lines("io.log") == ["attempt"] * 3

        with pytest.raises(ValueError, match="unexpected value"):
            evaluate(retry_only_ioerror_task(log_path="value.log", kind="value"))
        assert _lines("value.log") == ["attempt"]

    def test_retry_backoff_delays_attempts(self):
        assert evaluate(flaky_with_backoff_task(marker_path="b.marker", log_path="b.log")) == "ok"

        # Two retries with base=0.05s and multiplier=2: delays of 0.05s then 0.10s.
        stamps = [float(line.split(":", 1)[1]) for line in _lines("b.log")]
        assert len(stamps) == 3
        assert stamps[1] - stamps[0] >= 0.04
        assert stamps[2] - stamps[1] >= 0.09

    def test_shell_retries_follow_retry_on_exit_codes(self, tmp_path: Path):
        output = tmp_path / "retried.out"
        evaluate(
            shell_retry_on_exit_code_task(
                marker_path=str(tmp_path / "marker"), output_path=str(output)
            )
        )
        assert output.read_text(encoding="utf-8") == "x"

        # Exit 7 is not 137: the task fails without a retry.
        log = tmp_path / "attempts.log"
        with pytest.raises(ShellTaskError):
            evaluate(
                shell_retry_skip_mismatched_exit_code_task(
                    output_path=str(tmp_path / "never.out"), log_path=str(log)
                )
            )
        assert _lines(log) == ["attempt"]

    def test_a_shell_task_retries_a_transient_failure_and_logs_both_attempts(self, tmp_path: Path):
        output, log = tmp_path / "flaky.txt", tmp_path / "logs" / "flaky.log"

        result = evaluate(
            flaky_shell_task(
                marker_path=str(tmp_path / "marker"), output_path=str(output), log_path=str(log)
            )
        )

        assert result == file(str(output))
        assert output.read_text(encoding="utf-8") == "payload"
        assert "transient shell failure" in log.read_text(encoding="utf-8")
        assert "captured stdout" in log.read_text(encoding="utf-8")


class TestFailurePolicy:
    """``on_failure="ignore"`` and ``keep_going``: what a failure stops."""

    def test_fail_fast_stops_the_tasks_downstream_of_the_failure(self) -> None:
        with pytest.raises(RuntimeError, match="no retry configured"):
            evaluate(read_text_task(path=always_fail_once_task(log_path="ff.log")))

        # No retries by default, and the downstream task never ran.
        assert _lines("ff.log") == ["attempt"]

    def test_the_missing_result_names_the_failed_branch_by_its_label(self) -> None:
        """RP4-5: the message names the label, not an internal task id."""
        with pytest.raises(RootSkippedError) as excinfo:
            ConcurrentEvaluator(jobs=1, cores=1).evaluate(
                ignored_failure_task().map(log_path=["first.log", "second.log"])
            )

        assert "ignored_failure_task[first.log]" in str(excinfo.value)
        assert "task_0" not in str(excinfo.value)

    def test_dependents_of_an_ignored_failure_are_skipped_down_the_chain(
        self, event_collector: EventCollector
    ) -> None:
        log_path = "ignore-cascade.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1, event_bus=event_collector.bus)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                double_task(
                    value=double_task(
                        value=ignored_failure_task(log_path=log_path), log_path=log_path
                    ),
                    log_path=log_path,
                )
            )

        assert _lines(log_path) == ["ignored-attempt"]
        skipped = [node for node in evaluator.task_nodes.values() if node.state == "skipped"]
        assert len(skipped) == 2
        # A skip names the failure behind the skips above it.
        assert {node.blocked_by.task_def.name for node in skipped} == {ignored_failure_task.name}
        events = [e for e in event_collector.events if isinstance(e, TaskSkipped)]
        assert [e.blocked_by_task_name for e in events] == [ignored_failure_task.name] * 2
        assert not [
            e
            for e in event_collector.events
            if isinstance(e, TaskCompleted) and e.task_name == double_task.name
        ]

    def test_an_ignored_failure_skips_only_the_join_below_it(
        self, event_collector: EventCollector
    ) -> None:
        log_path = "ignore-diamond.log"
        evaluator = ConcurrentEvaluator(jobs=2, cores=2, event_bus=event_collector.bus)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                [
                    join_pair_task(
                        left=ignored_failure_task(log_path=log_path),
                        right=logged_work_task(x=1, log_path=log_path),
                        log_path=log_path,
                    ),
                    logged_work_task(x=2, log_path=log_path),
                ]
            )

        # The sibling branches finish; only the join below the failure is skipped.
        assert sorted(_lines(log_path)) == ["ignored-attempt", "work:1", "work:2"]
        states = {node.task_def.name: node.state for node in evaluator.task_nodes.values()}
        assert states == {
            ignored_failure_task.name: "failed",
            logged_work_task.name: "completed",
            join_pair_task.name: "skipped",
        }
        assert [node.task_def.name for node, _ in evaluator.ignored_failures] == [
            ignored_failure_task.name
        ]
        assert [event.ignored for event in event_collector.failed()] == [True]

    def test_retries_are_exhausted_before_a_failure_is_ignored(self) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, cores=1)

        with pytest.raises(RootSkippedError, match="ignored_failure_with_retries_task"):
            evaluator.evaluate(ignored_failure_with_retries_task(log_path="retries.log"))

        assert _lines("retries.log") == ["attempt"] * 3
        assert len(evaluator.ignored_failures) == 1

    def test_a_task_skipped_by_its_own_expansion_reports_the_work_it_did(
        self, tmp_path: Path, event_collector: EventCollector
    ) -> None:
        """A task whose body returns task expressions has run by the time it
        waits on them, so a failure inside its expansion leaves it started,
        attempted, logged — and resultless. Nothing may claim it did not run."""
        ledger, evaluator = _ledger_evaluator(tmp_path)
        ledger.bus.subscribe(event_collector.bus.emit)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(expand_into_a_failure_task(log_path="expansion.log"))

        parent = evaluator.task_nodes[0]
        assert parent.state == "skipped"
        assert parent.blocked_by.task_def.name == ignored_failure_task.name
        assert "expanded" in _lines("expansion.log")
        assert [event.task_name for event in event_collector.started()] == [
            expand_into_a_failure_task.name,
            logged_work_task.name,
            ignored_failure_task.name,
        ]

        row = ledger.finish(status="failed").tasks[0]
        ledger.close()
        assert (row.status, row.started, row.attempts, row.cache_label) == (
            "skipped",
            True,
            1,
            "miss",
        )
        assert row.started_at is not None
        # The attempt it made is reported, not erased to "—" like a task that never started.
        assert row.attempts_label != "—"
        assert row.skipped_because == {
            "reason": "upstream_failed",
            "task_id": "task_0002",
            "task_name": ignored_failure_task.name,
        }

    def test_work_landing_after_the_run_stopped_is_not_called_ignored_or_retried(
        self, event_collector: EventCollector
    ) -> None:
        """The ignored flag records what happened, not what the policy would have done."""
        log_path, marker_path = "late-failure.log", "fatal.marker"
        evaluator = ConcurrentEvaluator(jobs=3, cores=3, event_bus=event_collector.bus)

        with pytest.raises(RuntimeError, match="fatal boom"):
            evaluator.evaluate(
                [
                    fatal_failure_marker_task(marker_path=marker_path, log_path=log_path),
                    ignored_failure_after_marker_task(marker_path=marker_path, log_path=log_path),
                    retrying_failure_after_marker_task(marker_path=marker_path, log_path=log_path),
                ]
            )

        assert sorted(_lines(log_path)) == ["fatal", "late", "late"]
        assert evaluator.ignored_failures == ()
        failed = event_collector.failed()
        assert len(failed) == 3
        assert not any(event.ignored for event in failed)
        assert _notices(event_collector, retrying_failure_after_marker_task) == [
            "retry 1/2 not attempted: the run is stopping after an earlier failure"
        ]

    def test_a_fail_fast_stop_leaves_no_task_pending(
        self, event_collector: EventCollector
    ) -> None:
        log_path, marker_path = "fail-fast-terminal.log", "fatal.marker"
        evaluator = ConcurrentEvaluator(jobs=2, cores=2, event_bus=event_collector.bus)

        with pytest.raises(RuntimeError, match="fatal boom"):
            evaluator.evaluate(
                [
                    double_task(
                        value=fatal_failure_marker_task(
                            marker_path=marker_path, log_path=log_path
                        ),
                        log_path=log_path,
                    ),
                    double_task(
                        value=work_after_marker_task(marker_path=marker_path, log_path=log_path),
                        log_path=log_path,
                    ),
                ]
            )

        assert sorted(_lines(log_path)) == ["fatal", "late-work"]
        assert sorted(node.state for node in evaluator.task_nodes.values()) == [
            "completed",
            "failed",
            "skipped",
            "skipped",
        ]
        skipped = [event for event in event_collector.events if isinstance(event, TaskSkipped)]
        assert sorted(event.reason for event in skipped) == ["run_stopped", "upstream_failed"]
        assert {event.blocked_by_task_name for event in skipped} == {
            fatal_failure_marker_task.name
        }

    def test_a_scheduled_retry_abandoned_by_a_stop_is_recorded_failed(
        self, tmp_path: Path, event_collector: EventCollector
    ) -> None:
        log_path, marker_path = "abandoned-scheduled-retry.log", "retrier.marker"
        ledger, evaluator = _ledger_evaluator(tmp_path, jobs=2)
        ledger.bus.subscribe(event_collector.bus.emit)

        with pytest.raises(RuntimeError, match="fatal boom"):
            evaluator.evaluate(
                [
                    slow_retry_failure_task(marker_path=marker_path, log_path=log_path),
                    fatal_failure_after_marker_task(marker_path=marker_path, log_path=log_path),
                ]
            )

        assert evaluator.task_nodes[0].state == "failed"
        assert _notices(event_collector, slow_retry_failure_task) == [
            "retry 1/2 not attempted: the run is stopping after an earlier failure"
        ]
        row = ledger.finish(status="failed").tasks[0]
        ledger.close()
        assert (row.status, row.attempts) == ("failed", 1)
        assert row.failure["message"] == "RuntimeError: flaky boom"

    def test_an_interrupt_skips_every_task_that_never_started(
        self, event_collector: EventCollector
    ) -> None:
        evaluator = ConcurrentEvaluator(jobs=1, cores=1, event_bus=event_collector.bus)
        evaluator._register(double_task(value=logged_work_task(x=1, log_path="x"), log_path="x"))

        evaluator._close_unfinished_nodes(reason=KeyboardInterrupt("Interrupted by SIGINT"))

        skipped = [event for event in event_collector.events if isinstance(event, TaskSkipped)]
        assert [(event.reason, event.blocked_by_task_id) for event in skipped] == [
            ("interrupted", None),
            ("interrupted", None),
        ]

    def test_keep_going_ignores_a_fail_fast_task(self) -> None:
        log_path = "keep-going.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1, keep_going=True)

        with pytest.raises(RootSkippedError, match="always_fail_once_task"):
            evaluator.evaluate(
                [
                    read_text_task(path=always_fail_once_task(log_path=log_path)),
                    logged_work_task(x=1, log_path=log_path),
                ]
            )

        assert sorted(_lines(log_path)) == ["attempt", "work:1"]
        assert len(evaluator.ignored_failures) == 1


def _notices(collector: EventCollector, task_def) -> list[str]:
    return [
        event.message
        for event in collector.events
        if isinstance(event, TaskNotice) and event.task_name == task_def.name
    ]


class TestShellTask:
    @pytest.mark.parametrize(
        "command",
        ["nosuchtool | cat > {out}", "false\necho ok > {out}", "false; echo ok > {out}"],
        ids=["failing-pipeline-stage", "failing-line", "failing-statement"],
    )
    def test_shell_task_fails_on_any_failing_command(self, tmp_path: Path, command: str):
        with pytest.raises(ShellTaskError):
            evaluate(shell_command_task(command=command, output_path=str(tmp_path / "out.txt")))

    def test_a_failed_shell_task_is_named_and_leaves_no_partial_output(self, tmp_path: Path):
        output = tmp_path / "out.txt"
        with pytest.raises(ShellTaskError, match=r"^Shell task shell_command_task failed"):
            evaluate(
                shell_command_task(command="echo partial > {out}; exit 3", output_path=str(output))
            )
        assert not output.exists()

    def test_a_failed_script_task_says_script(self, tmp_path: Path):
        script_path = tmp_path / "fail.py"
        script_path.write_text("raise SystemExit(4)\n", encoding="utf-8")

        with pytest.raises(ShellTaskError, match=r"^Script task python_script_task failed"):
            evaluate(
                python_script_task(
                    script_path=str(script_path), output_path=str(tmp_path / "out.txt")
                )
            )

    def test_shell_task_runs_under_bash(self, tmp_path: Path):
        output = tmp_path / "out.txt"
        evaluate(shell_command_task(command="cat <(echo bash) > {out}", output_path=str(output)))
        assert output.read_text(encoding="utf-8") == "bash\n"

    def test_shell_task_requires_its_declared_outputs_to_exist(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="did not create output"):
            evaluate(shell_command_task(command="true", output_path=str(tmp_path / "gone.txt")))

        missing = tmp_path / "results" / "sample_2.fastq.gz"
        with pytest.raises(FileNotFoundError, match=str(missing)):
            evaluate(
                shell_missing_multiple_outputs_task(
                    output_one=str(tmp_path / "results" / "sample_1.fastq.gz"),
                    output_two=str(missing),
                )
            )

    def test_multiple_outputs_create_parent_dirs_and_restore_from_cache(self, tmp_path: Path):
        one = tmp_path / "results" / "reads" / "sample_1.fastq.gz"
        two = tmp_path / "results" / "reads" / "sample_2.fastq.gz"
        marker = tmp_path / "command.log"
        expr = shell_write_multiple_outputs_task(
            output_one=str(one), output_two=str(two), marker_path=str(marker)
        )

        first = evaluate(expr)
        assert first == evaluate(expr) == (file(str(one)), file(str(two)))
        assert (one.read_text(), two.read_text()) == ("left", "right")
        assert _lines(marker) == ["run"]

    def test_list_outputs_from_shell_and_python_tasks(self, tmp_path: Path):
        one, two = tmp_path / "list" / "s1.fastq.gz", tmp_path / "list" / "s2.fastq.gz"
        expected = [file(str(one)), file(str(two))]

        assert evaluate(
            shell_write_list_outputs_task(output_one=str(one), output_two=str(two))
        ) == (expected)
        # A python task may return a list for a scalar ``file`` annotation, cached too.
        py = [str(tmp_path / "py" / "s1.fastq.gz"), str(tmp_path / "py" / "s2.fastq.gz")]
        expr = write_multiple_files_with_scalar_annotation(output_paths=py)
        assert evaluate(expr) == evaluate(expr) == [file(path) for path in py]


def test_run_subprocess_tracks_the_process_only_while_it_runs() -> None:
    runner = ConcurrentEvaluator()._shell_runner

    completed = runner.run_subprocess(argv=["sh", "-c", "echo out; echo err >&2"])

    assert (completed.returncode, completed.stdout, completed.stderr) == (0, "out\n", "err\n")
    assert runner._active_subprocesses == {}


def test_a_worker_payload_carries_no_import_roots(tmp_path: Path) -> None:
    evaluator = ConcurrentEvaluator()
    (node_id,) = evaluator._register(add_one_task(x=1))
    node = evaluator._nodes[node_id]
    node.transport_path = tmp_path / "transport"
    node.transport_path.mkdir()
    node.resolved_args = node.execution_args = {"x": 1}

    payload = evaluator._build_worker_payload(node=node)

    assert "ginkgo_import_roots" not in payload
    assert "sys_path" not in payload


class TestSecrets:
    def _evaluator(self, tmp_path: Path, token: str) -> tuple[Ledger, ConcurrentEvaluator]:
        resolver = build_secret_resolver(
            project_root=tmp_path, config={}, environ={"API_TOKEN": token}
        )
        return _ledger_evaluator(tmp_path, secret_resolver=resolver)

    def test_secret_rotation_keeps_cache_key_stable_and_redacts_manifest(
        self, tmp_path: Path
    ) -> None:
        expr = reveal_secret_task(secret_value=secret("API_TOKEN"), output_path="result.txt")
        ledger, evaluator = self._evaluator(tmp_path, "first-token")

        first_result = evaluator.evaluate(expr)
        first_task = ledger.finish().tasks[0]
        ledger.close()
        with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
            entry = index.entry(first_task.cache_key)

        assert Path(first_result).read_text(encoding="utf-8") == "first-token"
        assert first_task.inputs["secret_value"]["redacted"] is True
        assert "first-token" not in ledger.run_dir.manifest_path.read_text(encoding="utf-8")
        assert "first-token" not in str(entry["inputs"])

        rotated, evaluator = self._evaluator(tmp_path, "rotated-token")
        second_result = evaluator.evaluate(expr)
        [second_task] = rotated.tasks().values()
        rotated.close()

        assert second_task["status"] == "cached"
        assert second_task["cache_key"] == first_task.cache_key
        assert Path(second_result).read_text(encoding="utf-8") == "first-token"

    def test_secret_values_are_redacted_from_logs_and_errors(self, tmp_path: Path) -> None:
        ledger, evaluator = self._evaluator(tmp_path, "super-secret")

        with pytest.raises(RuntimeError, match="failure:\\[REDACTED\\]"):
            evaluator.evaluate(fail_with_secret_task(secret_value=secret("API_TOKEN")))

        [task] = ledger.tasks().values()
        stdout_log = (ledger.path / task["stdout_log"]).read_text(encoding="utf-8")
        stderr_log = (ledger.path / task["stderr_log"]).read_text(encoding="utf-8")
        assert "[REDACTED]" in stdout_log
        assert "super-secret" not in stdout_log + stderr_log
        ledger.finish()
        ledger.close()
        assert "super-secret" not in ledger.run_dir.manifest_path.read_text(encoding="utf-8")

    def test_a_shell_task_never_writes_a_secret_to_its_logs(self, tmp_path: Path) -> None:
        ledger, evaluator = self._evaluator(tmp_path, "super-secret")

        with pytest.raises(ShellTaskError):
            evaluator.evaluate(
                echo_secret_task(secret_value=secret("API_TOKEN"), output_path="out.txt")
            )

        [task] = ledger.tasks().values()
        ledger.close()
        stdout_log = (ledger.path / task["stdout_log"]).read_text(encoding="utf-8")
        stderr_log = (ledger.path / task["stderr_log"]).read_text(encoding="utf-8")
        assert "shell-secret:[REDACTED]" in stdout_log
        assert "super-secret" not in stdout_log + stderr_log


class TestValidation:
    @pytest.mark.parametrize(
        ("make", "kind"), [(read_text_task, "file"), (list_dir_task, "folder")]
    )
    def test_a_missing_input_suggests_out_for_a_path_the_task_writes(
        self, tmp_path: Path, make, kind: str
    ) -> None:
        """N2: ``file``/``folder`` declare reads; a written path wants ``Out[...]``."""
        with pytest.raises(
            FileNotFoundError, match=rf"annotate the parameter `Out\[{kind}\]`"
        ) as err:
            evaluate(make(path=str(tmp_path / "missing")))
        assert "must exist and be a" in str(err.value)

    @pytest.mark.parametrize(
        ("make", "kind"), [(read_text_task, "file"), (list_dir_task, "folder")]
    )
    def test_a_list_for_one_path_is_rejected_before_the_run(
        self, tmp_path: Path, make, kind: str
    ) -> None:
        """N3: each element existing does not make a list one path."""
        paths = [tmp_path / "a", tmp_path / "b"]
        for path in paths:
            path.write_text("") if kind == "file" else path.mkdir()
        evaluator = ConcurrentEvaluator()
        evaluator.build_graph(make(path=[str(path) for path in paths]))

        with pytest.raises(TypeError, match=f"annotated '{kind}' but was given a list"):
            evaluator.validate_graph()

    def test_python_tasks_accept_paths_with_spaces_but_shell_outputs_do_not(self, tmp_path: Path):
        spaced_dir = tmp_path / "has space"
        spaced_dir.mkdir()
        (spaced_dir / "a b.txt").write_text("")

        listed, written = evaluate(
            [
                list_dir_task(path=str(spaced_dir)),
                write_spaced_output(dest=str(tmp_path / "out space" / "out put.txt")),
            ]
        )
        assert listed == ["a b.txt"]
        assert Path(str(written)).read_text(encoding="utf-8") == "ok"

        with pytest.raises(ValueError, match="must not contain spaces"):
            evaluate(touch_via_shell(dest=str(tmp_path / "out put.txt")))


@pytest.mark.parametrize("fail", [False, True])
def test_a_tmp_dir_is_removed_on_success_and_kept_on_failure(fail: bool):
    report_path = Path("scratch-path.txt")

    if fail:
        with pytest.raises(RuntimeError, match="boom"):
            evaluate(write_scratch_task(report_path=str(report_path), fail=True))
    else:
        assert evaluate(write_scratch_task(report_path=str(report_path))) == (
            report_path.read_text(encoding="utf-8")
        )

    scratch_path = Path(report_path.read_text(encoding="utf-8"))
    assert scratch_path.exists() == fail
    shutil.rmtree(scratch_path, ignore_errors=True)


def test_nodes_a_failed_expansion_added_still_get_run_state(tmp_path: Path) -> None:
    """An inferred edge may already point at them, so none is left without state."""
    pending, started = str(tmp_path / "a.txt"), str(tmp_path / "b.txt")
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph([_read_path(path=pending), _read_path(path=started)])
    evaluator.task_nodes[1].state = "running"

    with pytest.raises(RuntimeError, match="already read that path"):
        evaluator._register([_write_path(output_path=pending), _write_path(output_path=started)])

    assert set(evaluator.task_nodes) == set(evaluator.graph.nodes)


def test_a_deadlock_names_each_unresolved_task_and_its_waits() -> None:
    evaluator = ConcurrentEvaluator()
    evaluator.build_graph(sum_keyword_pair_task(left=add_one_task(x=1), right=add_one_task(x=2)))
    evaluator.task_nodes[1].state = "completed"

    assert str(evaluator._deadlock_error()).splitlines() == [
        "Scheduler reached a deadlock with unresolved tasks:",
        "  add_one_task[2] (pending) waits on: nothing",
        "  sum_keyword_pair_task (pending) waits on: add_one_task[2]",
    ]
