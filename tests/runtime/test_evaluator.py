"""Unit tests for the evaluator runtime."""

from concurrent.futures import Future, ProcessPoolExecutor
from pathlib import Path
import shutil
import subprocess
import sys
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
from ginkgo.runtime.evaluator import CycleError, ConcurrentEvaluator, RootSkippedError
from ginkgo.runtime.executors import Executors
from tests.conftest import EventCollector, ShellEnvironment
from ginkgo.store.sqlite import open_store
from ginkgo.runtime.events import (
    TaskCompleted,
    TaskFailed,
    TaskSkipped,
)
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.runtime.rundir import make_run_id

from tests.conftest import Ledger
from ginkgo.runtime.environment.secrets import build_secret_resolver
from tests._vw_support import append_line


@task("script")
def python_script_task(*, script_path: str, output_path: str) -> Path:
    """Run a Python script by path, writing to output_path."""
    return script(script_path, output=output_path)


@task("script", env="test_env")
def python_script_env_task(*, script_path: str, output_path: str) -> Path:
    """Run a Python script inside a declared task environment."""
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
def sum_keyword_pair_task(*, left: int, right: int) -> int:
    return left + right


@task(retries=2)
def flaky_retry_task(marker_path: untracked, log_path: untracked) -> str:
    append_line(log_path, "attempt")
    marker = Path(marker_path)
    failures = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
    if failures < 1:
        marker.write_text(str(failures + 1), encoding="utf-8")
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
    return [
        logged_work_task(x=1, log_path=log_path),
        ignored_failure_task(log_path=log_path),
    ]


@task()
def fatal_failure_marker_task(marker_path: untracked, log_path: untracked) -> int:
    append_line(log_path, "fatal")
    Path(marker_path).write_text("failed", encoding="utf-8")
    raise RuntimeError("fatal boom")


@task(on_failure="ignore")
def ignored_failure_after_marker_task(marker_path: untracked, log_path: untracked) -> int:
    import time as _time

    for _ in range(1000):
        if Path(marker_path).exists():
            break
        _time.sleep(0.01)
    append_line(log_path, "late")
    raise RuntimeError("late boom")


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
    import time as _time

    append_line(log_path, f"attempt:{_time.monotonic():.6f}")
    marker = Path(marker_path)
    failures = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
    if failures < 2:
        marker.write_text(str(failures + 1), encoding="utf-8")
        raise RuntimeError("still transient")
    return "ok"


@task(kind="shell", retries=2, retry_on_exit_codes=(7,))
def shell_retry_on_exit_code_task(
    marker_path: untracked, output_path: str, log_path: untracked
) -> file:
    return shell(
        cmd=(
            f"if [ ! -f {marker_path} ]; then "
            f"touch {marker_path}; "
            "exit 7; "
            f"fi; printf 'payload' > {output_path}"
        ),
        output=output_path,
        log=log_path,
    )


@task(kind="shell", retries=2, retry_on_exit_codes=(137,))
def shell_retry_skip_mismatched_exit_code_task(output_path: str, log_path: untracked) -> file:
    return shell(cmd="exit 7", output=output_path, log=log_path)


@task(kind="shell")
def shell_write_output_task(output_path: str, log_path: untracked) -> file:
    return shell(
        cmd=(
            "printf 'captured stdout\\n'; "
            "printf 'captured stderr\\n' 1>&2; "
            f"printf 'payload' > {output_path}"
        ),
        output=output_path,
        log=log_path,
    )


@task(kind="shell")
def shell_missing_output_task(output_path: str) -> file:
    return shell(cmd="true", output=output_path)


@task(kind="shell")
def shell_write_multiple_outputs_task(
    output_one: Out[file],
    output_two: Out[file],
    marker_path: untracked,
) -> tuple[file, file]:
    return shell(
        cmd=(
            f"printf 'left' > {output_one}; "
            f"printf 'right' > {output_two}; "
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
    return shell(
        cmd=f"printf 'left' > {output_one}",
        output=(output_one, output_two),
    )


@task(kind="shell", retries=2)
def flaky_shell_task(marker_path: untracked, output_path: str, log_path: untracked) -> file:
    return shell(
        cmd=(
            f"if [ ! -f {marker_path} ]; then "
            f"touch {marker_path}; "
            "printf 'transient shell failure\\n' 1>&2; "
            "exit 7; "
            f"fi; printf 'payload' > {output_path}"
        ),
        output=output_path,
        log=log_path,
    )


@task(kind="shell")
def shell_returns_script_directive_task(output_path: str) -> file:
    return script("copy.py", output=output_path)


@task()
def python_returns_shell_directive_task(output_path: str) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


@task()
def python_returns_script_directive_task() -> None:
    from pathlib import Path

    return ScriptDirective(
        path=Path("/fake.py"), output=None, log=None, interpreter="python", source_hash="test"
    )


@task()
def python_returns_subworkflow_directive_task() -> None:
    return SubWorkflowDirective(path="fake/workflow.py")


@task(kind="shell")
def shell_returns_plain_value_task() -> str:
    return "plain-value"


@task()
def read_text_task(path: file) -> str:
    return Path(path).read_text()


@task()
def write_multiple_files_with_scalar_annotation(output_paths: list[str]) -> file:
    for index, output_path in enumerate(output_paths):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"payload-{index}", encoding="utf-8")
    return output_paths


@task()
def list_dir_task(path: folder) -> list[str]:
    return sorted(child.name for child in Path(path).iterdir())


@task()
def write_scratch_task(report_path: str, scratch: tmp_dir) -> str:
    scratch_path = Path(scratch)
    (scratch_path / "marker.txt").write_text("ok", encoding="utf-8")
    Path(report_path).write_text(str(scratch_path), encoding="utf-8")
    return str(scratch_path)


@task()
def fail_in_scratch_task(report_path: str, scratch: tmp_dir) -> None:
    scratch_path = Path(scratch)
    (scratch_path / "marker.txt").write_text("boom", encoding="utf-8")
    Path(report_path).write_text(str(scratch_path), encoding="utf-8")
    raise RuntimeError("boom")


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
def dataframe_total_task(df: object) -> int:
    return int(df["value"].sum())


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
def make_pair_task(x: int) -> tuple[int, int]:
    return (x * 10, x * 100)


@task()
def sum_pair_task(a: int, b: int) -> int:
    return a + b


class TestEvaluate:
    def test_evaluate_resolves_nested_containers(self):
        result = evaluate(
            {
                "single": add_one_task(x=1),
                "list": [add_one_task(x=2), add_one_task(x=3)],
                "tuple": (add_one_task(x=4), "literal"),
            }
        )

        assert result == {
            "single": 2,
            "list": [3, 4],
            "tuple": (5, "literal"),
        }

    def test_output_index_selects_tuple_element(self):
        pair = make_pair_task(x=3)
        result = evaluate(sum_pair_task(a=pair.output[0], b=pair.output[1]))
        assert result == 30 + 300

    def test_output_index_single_element(self):
        pair = make_pair_task(x=5)
        result = evaluate(add_one_task(x=pair.output[0]))
        assert result == 51

    def test_output_index_with_fan_out(self):
        pairs = make_pair_task().map(x=[1, 2, 3])
        result = evaluate(
            sum_pair_task().map(
                a=pairs.output[0],
                b=pairs.output[1],
            )
        )
        assert result == [10 + 100, 20 + 200, 30 + 300]

    def test_two_axis_zip_map_display_label_reaches_the_store(self, tmp_path: Path) -> None:
        """Issue #286: a two-axis zip fan-out's per-branch label must land in
        the ledger's ``tasks.display_label`` column, distinct per branch."""
        recorder = Ledger.start(
            root=tmp_path, run_id=make_run_id(workflow_path=tmp_path / "workflow.py")
        )
        # The grid is crossed by hand, so the first axis repeats and cannot
        # tell the branches apart on its own.
        expr_list = fit_site_species_trend_task().map(
            site=["north_fen", "north_fen", "south_bog", "south_bog"],
            species=["sedge", "sphagnum", "sedge", "sphagnum"],
        )
        evaluator = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            run_dir=recorder.run_dir,
            event_bus=recorder.bus,
        )
        result = evaluator.evaluate(expr_list)
        summary = recorder.finish()

        assert result == [
            "north_fen:sedge",
            "north_fen:sphagnum",
            "south_bog:sedge",
            "south_bog:sphagnum",
        ]
        expected = [
            "fit_site_species_trend_task[north_fen,sedge]",
            "fit_site_species_trend_task[north_fen,sphagnum]",
            "fit_site_species_trend_task[south_bog,sedge]",
            "fit_site_species_trend_task[south_bog,sphagnum]",
        ]
        assert {task.display_label for task in summary.tasks} == set(expected)

        with open_store(recorder.db, readonly=True) as store:
            rows = store.query(
                "SELECT display_label FROM tasks WHERE run_id = ? ORDER BY task_id",
                (recorder.run_id,),
            )
        stored_labels = [row["display_label"] for row in rows]
        assert stored_labels == expected
        assert all(label is not None for label in stored_labels)

    def test_second_run_is_served_from_cache(self) -> None:
        log_path = "work-events.log"

        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3
        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3

        # The second run reuses the cached result, so the task body only ran once.
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["work:2"]
        assert (Path(".ginkgo") / "cache").exists()

    def test_script_task_runs_and_validates_outputs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        script_path = tmp_path / "fit.py"
        script_path.write_text("# placeholder script\n", encoding="utf-8")
        output_path = tmp_path / "out.txt"
        expr = python_script_task(script_path=str(script_path), output_path=str(output_path))

        def fake_run_subprocess(
            *,
            argv: str | list[str],
            use_shell: bool,
            on_stdout: Any = None,
            on_stderr: Any = None,
            **kwargs: Any,
        ) -> subprocess.CompletedProcess[str]:
            # Simulate the script creating its declared output.
            output_path.write_text("done\n", encoding="utf-8")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")

        evaluator = ConcurrentEvaluator(jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)
        result = evaluator.evaluate(expr)

        assert Path(result).is_file()

    def test_script_task_with_env_uses_env_python_not_scheduler_interpreter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A declared env= must control which Python runs the script.

        Regression test for the scheduler's own sys.executable being
        hardcoded into the command, which silently ignored env= for
        Python scripts (issue #120).
        """
        script_path = tmp_path / "fit.py"
        script_path.write_text("# placeholder script\n", encoding="utf-8")
        output_path = tmp_path / "out.txt"
        expr = python_script_env_task(script_path=str(script_path), output_path=str(output_path))

        calls: list[str] = []

        def fake_run_subprocess(
            *,
            argv: str | list[str],
            use_shell: bool,
            on_stdout: Any = None,
            on_stderr: Any = None,
            **kwargs: Any,
        ) -> subprocess.CompletedProcess[str]:
            calls.append(str(argv))
            output_path.write_text("done\n", encoding="utf-8")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")

        evaluator = ConcurrentEvaluator(jobs=1, cores=1, backend=ShellEnvironment())
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)
        result = evaluator.evaluate(expr)

        assert Path(result).is_file()
        assert len(calls) == 1
        assert sys.executable not in calls[0]
        assert "python" in calls[0]

    def test_local_task_fails_immediately_at_runtime(self):
        @task()
        def local_task(x: int) -> int:
            return x + 1

        with pytest.raises(TypeError, match="top-level function"):
            evaluate(local_task(x=1))

    def test_a_task_must_return_its_own_kinds_directive(self, tmp_path: Path) -> None:
        Path("copy.py").write_text("", encoding="utf-8")

        with pytest.raises(
            TypeError,
            match=r"kind='shell' and must return shell\(\.\.\.\), not ScriptDirective\. "
            r"Use @task\(kind='script'\)",
        ):
            evaluate(shell_returns_script_directive_task(output_path=str(tmp_path / "out")))

    def test_python_tasks_must_not_return_shell_directives(self, tmp_path: Path) -> None:
        output = tmp_path / "payload.txt"

        with pytest.raises(TypeError, match=r"ShellDirective.*Use @task\(kind='shell'\)"):
            evaluate(python_returns_shell_directive_task(output_path=str(output)))

    def test_python_tasks_must_not_return_script_directives(self) -> None:
        with pytest.raises(TypeError, match=r"ScriptDirective.*Use @task\(kind='script'\)"):
            evaluate(python_returns_script_directive_task())

    def test_python_tasks_must_not_return_subworkflow_directives(self) -> None:
        with pytest.raises(
            TypeError, match=r"SubWorkflowDirective.*Use @task\(kind='subworkflow'\)"
        ):
            evaluate(python_returns_subworkflow_directive_task())

    def test_shell_tasks_must_return_shell_payloads_or_dynamic_exprs(self) -> None:
        with pytest.raises(TypeError, match="must return shell\\(\\.\\.\\.\\) or dynamic"):
            evaluate(shell_returns_plain_value_task())

    def test_numpy_array_can_cross_the_python_task_boundary(self):
        result = evaluate(array_task(start=2))

        assert np.array_equal(result, np.array([2, 3, 4]))

    def test_numpy_array_can_flow_into_downstream_python_task(self):
        result = evaluate(sum_array_task(values=array_task(start=2)))

        assert result == 9

    def test_dataframe_results_are_cached_and_restored(
        self,
        event_collector: EventCollector,
    ):
        log_path = "dataframe-events.log"

        first = evaluate(dataframe_task(log_path=log_path, start=3))

        second = evaluate(
            dataframe_task(log_path=log_path, start=3), event_bus=event_collector.bus
        )

        assert first.equals(second)
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["df:3"]
        assert event_collector.cached()

    def test_dataframe_can_flow_into_downstream_python_task(self):
        result = evaluate(dataframe_total_task(df=dataframe_task(log_path="df.log", start=4)))

        assert result == 17

    def test_dataframe_consumer_stays_cached_when_producer_is_replayed(self):
        # The producer's default RangeIndex must survive the cache round trip,
        # or the replayed frame hashes differently and the consumer re-runs.
        log_path = "dataframe-chain.log"

        def build() -> object:
            return logged_dataframe_total_task(
                log_path=log_path, df=dataframe_task(log_path=log_path, start=5)
            )

        first = evaluate(build())
        second = evaluate(build())

        assert first == second == 21
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["df:5", "total"]

    def test_validate_rejects_direct_expression_cycles(self):
        first = passthrough_task()
        second = passthrough_task(value=first)
        first.args["value"] = second

        evaluator = ConcurrentEvaluator()
        with pytest.raises(
            CycleError,
            match="Detected cycle in workflow graph: test_evaluator.passthrough_task -> "
            "test_evaluator.passthrough_task -> test_evaluator.passthrough_task",
        ):
            evaluator.build_and_validate(first)

    def test_evaluate_rejects_cycles_nested_inside_containers(self):
        first = passthrough_task()
        second = passthrough_task(value=[first])
        first.args["value"] = {"nested": second}

        with pytest.raises(CycleError, match="Detected cycle in workflow graph"):
            evaluate(first)

    def test_evaluate_rejects_cycles_from_dynamic_returned_expressions(self):
        with pytest.raises(CycleError, match="Detected cycle in workflow graph"):
            evaluate(build_dynamic_cycle_task())

    def test_task_retries_are_disabled_by_default(self):
        log_path = "default-no-retry.log"

        with pytest.raises(RuntimeError, match="no retry configured"):
            evaluate(always_fail_once_task(log_path=log_path))

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["attempt"]

    def test_task_retries_allow_transient_python_failures_and_cache_success(
        self,
        event_collector: EventCollector,
    ):
        marker_path = "flaky.marker"
        log_path = "flaky.log"

        first_collector = EventCollector()
        assert (
            evaluate(
                flaky_retry_task(marker_path=marker_path, log_path=log_path),
                event_bus=first_collector.bus,
            )
            == "ok"
        )

        from ginkgo.runtime.events import TaskRetrying

        retry_events = [e for e in first_collector.events if isinstance(e, TaskRetrying)]
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["attempt", "attempt"]
        assert retry_events
        assert any(e.attempt == 1 for e in retry_events)

        assert (
            evaluate(
                flaky_retry_task(marker_path=marker_path, log_path=log_path),
                event_bus=event_collector.bus,
            )
            == "ok"
        )

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["attempt", "attempt"]
        assert event_collector.cached()

    def test_task_retries_fail_after_final_attempt(self):
        log_path = "always-fail.log"

        with pytest.raises(RuntimeError, match="still broken"):
            evaluate(always_fail_retry_task(log_path=log_path))

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "attempt",
            "attempt",
            "attempt",
        ]

    def test_retry_on_narrows_by_exception_class(self):
        log_path = "retry-ioerror.log"

        # IOError matches — task is retried.
        with pytest.raises(IOError, match="transient io"):
            evaluate(retry_only_ioerror_task(log_path=log_path, kind="ioerror"))
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "attempt",
            "attempt",
            "attempt",
        ]

        # ValueError does not match — task fails immediately without retry.
        log_path2 = "retry-skip.log"
        with pytest.raises(ValueError, match="unexpected value"):
            evaluate(retry_only_ioerror_task(log_path=log_path2, kind="value"))
        assert Path(log_path2).read_text(encoding="utf-8").splitlines() == ["attempt"]

    def test_retry_backoff_delays_attempts(self):
        import time as _time

        marker_path = "backoff.marker"
        log_path = "backoff.log"

        started = _time.monotonic()
        assert (
            evaluate(flaky_with_backoff_task(marker_path=marker_path, log_path=log_path)) == "ok"
        )
        elapsed = _time.monotonic() - started

        # Two retries with base=0.05s and multiplier=2: delays 0.05 + 0.10 = 0.15s minimum.
        assert elapsed >= 0.15
        lines = Path(log_path).read_text(encoding="utf-8").splitlines()
        assert len(lines) == 3

        stamps = [float(line.split(":", 1)[1]) for line in lines]
        assert stamps[1] - stamps[0] >= 0.04
        assert stamps[2] - stamps[1] >= 0.09

    def test_shell_retry_on_exit_codes_narrows(self, tmp_path: Path):
        marker = tmp_path / "shell-retry.marker"
        output = tmp_path / "shell-retry.out"
        log = tmp_path / "shell-retry.log"

        # Exit 7 matches — retried and succeeds.
        result = evaluate(
            shell_retry_on_exit_code_task(
                marker_path=str(marker), output_path=str(output), log_path=str(log)
            )
        )
        assert result == file(str(output))
        assert output.read_text(encoding="utf-8") == "payload"

        # Exit 7 does not match (policy expects 137) — fails immediately with no retry.
        output2 = tmp_path / "shell-retry2.out"
        log2 = tmp_path / "shell-retry2.log"
        with pytest.raises(Exception):
            evaluate(
                shell_retry_skip_mismatched_exit_code_task(
                    output_path=str(output2), log_path=str(log2)
                )
            )

    def test_cached_upstream_tasks_do_not_deadlock_newly_unblocked_downstream(
        self,
        event_collector: EventCollector,
    ) -> None:
        log_path = "fanin-cache.log"
        expr = sum_keyword_pair_task(
            left=logged_work_task(x=1, log_path=log_path),
            right=logged_work_task(x=2, log_path=log_path),
        )

        assert evaluate(expr) == 5

        assert evaluate(expr, event_bus=event_collector.bus) == 5

        log_lines = Path(log_path).read_text(encoding="utf-8").splitlines()

        assert sorted(log_lines) == ["work:1", "work:2"]
        # Three tasks (left, right, and the parent) should each report a cache hit.
        cache_hits = [e for e in event_collector.events if type(e).__name__ == "TaskCacheHit"]
        assert len(cache_hits) == 3


class TestFailurePolicy:
    """``on_failure="ignore"`` and ``keep_going``: what a failure stops."""

    def _lines(self, log_path: str) -> list[str]:
        return Path(log_path).read_text(encoding="utf-8").splitlines()

    def test_fail_fast_stops_the_tasks_downstream_of_the_failure(self) -> None:
        log_path = "fail-fast-downstream.log"

        with pytest.raises(RuntimeError, match="no retry configured"):
            evaluate(
                read_text_task(path=always_fail_once_task(log_path=log_path)),
            )

        assert self._lines(log_path) == ["attempt"]

    def test_an_ignored_failure_lets_sibling_branches_finish(
        self,
        event_collector: EventCollector,
    ) -> None:
        log_path = "ignore-siblings.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1, event_bus=event_collector.bus)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                [
                    ignored_failure_task(log_path=log_path),
                    logged_work_task(x=1, log_path=log_path),
                    logged_work_task(x=2, log_path=log_path),
                ]
            )

        assert sorted(self._lines(log_path)) == ["ignored-attempt", "work:1", "work:2"]
        assert [node.task_def.name for node, _ in evaluator.ignored_failures] == [
            ignored_failure_task.name
        ]
        failed = [event for event in event_collector.events if isinstance(event, TaskFailed)]
        assert [event.ignored for event in failed] == [True]

    def test_dependents_of_an_ignored_failure_are_skipped(
        self,
        event_collector: EventCollector,
    ) -> None:
        log_path = "ignore-downstream.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1, event_bus=event_collector.bus)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                double_task(
                    value=ignored_failure_task(log_path=log_path),
                    log_path=log_path,
                )
            )

        assert self._lines(log_path) == ["ignored-attempt"]
        states = {node.task_def.name: node.state for node in evaluator.task_nodes.values()}
        assert states == {
            ignored_failure_task.name: "failed",
            double_task.name: "skipped",
        }
        skipped = [event for event in event_collector.events if isinstance(event, TaskSkipped)]
        assert [event.blocked_by_task_name for event in skipped] == [ignored_failure_task.name]
        assert not [
            event
            for event in event_collector.events
            if isinstance(event, TaskCompleted) and event.task_name == double_task.name
        ]

    def test_a_skip_names_the_failure_behind_the_skips_above_it(self) -> None:
        log_path = "ignore-cascade.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                double_task(
                    value=double_task(
                        value=ignored_failure_task(log_path=log_path),
                        log_path=log_path,
                    ),
                    log_path=log_path,
                )
            )

        skipped = [node for node in evaluator.task_nodes.values() if node.state == "skipped"]
        assert len(skipped) == 2
        assert {node.blocked_by_task_name for node in skipped} == {ignored_failure_task.name}

    def test_a_diamond_skips_only_the_join_below_the_failure(self) -> None:
        log_path = "ignore-diamond.log"
        evaluator = ConcurrentEvaluator(jobs=2, cores=2)

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(
                join_pair_task(
                    left=ignored_failure_task(log_path=log_path),
                    right=logged_work_task(x=1, log_path=log_path),
                    log_path=log_path,
                )
            )

        assert sorted(self._lines(log_path)) == ["ignored-attempt", "work:1"]
        states = {node.task_def.name: node.state for node in evaluator.task_nodes.values()}
        assert states == {
            ignored_failure_task.name: "failed",
            logged_work_task.name: "completed",
            join_pair_task.name: "skipped",
        }

    def test_retries_are_exhausted_before_a_failure_is_ignored(self) -> None:
        log_path = "ignore-retries.log"
        evaluator = ConcurrentEvaluator(jobs=1, cores=1)

        with pytest.raises(RootSkippedError, match="ignored_failure_with_retries_task"):
            evaluator.evaluate(ignored_failure_with_retries_task(log_path=log_path))

        assert self._lines(log_path) == ["attempt", "attempt", "attempt"]
        assert len(evaluator.ignored_failures) == 1

    def test_a_task_skipped_by_its_own_expansion_reports_the_work_it_did(
        self,
        tmp_path: Path,
        event_collector: EventCollector,
    ) -> None:
        """The one skipped task that really ran: nothing may claim it did not.

        A task whose body returns further task expressions has run by the time
        it waits on them, so a failure inside its expansion leaves it started,
        attempted, logged — and resultless.
        """
        log_path = "expansion-skip.log"
        ledger = Ledger.start(root=tmp_path, run_id=make_run_id(workflow_path=tmp_path / "w.py"))
        ledger.bus.subscribe(event_collector.bus.emit)
        evaluator = ConcurrentEvaluator(
            jobs=1, cores=1, run_dir=ledger.run_dir, event_bus=ledger.bus
        )

        with pytest.raises(RootSkippedError, match="ignored_failure_task"):
            evaluator.evaluate(expand_into_a_failure_task(log_path=log_path))

        parent = evaluator.task_nodes[0]
        assert parent.state == "skipped"
        assert parent.blocked_by_task_name == ignored_failure_task.name
        assert "expanded" in self._lines(log_path)
        assert [event.task_name for event in event_collector.started()] == [
            expand_into_a_failure_task.name,
            logged_work_task.name,
            ignored_failure_task.name,
        ]

        summary = ledger.finish(status="failed")
        ledger.close()
        row = summary.tasks[0]
        assert row.status == "skipped"
        assert row.started_at is not None
        assert row.started is True
        # The attempt it made is reported like any other task's, rather than
        # erased to "—" the way a task that never started is.
        assert row.attempts == 1
        assert row.attempts_label != "—"
        assert row.cache_label == "miss"
        assert row.skipped_because == {
            "task_id": "task_0002",
            "task_name": ignored_failure_task.name,
        }

    def test_a_failure_landing_after_the_run_stopped_is_not_called_ignored(
        self,
        event_collector: EventCollector,
    ) -> None:
        """The flag records what happened, not what the policy would have done."""
        log_path = "late-failure.log"
        marker_path = "fatal.marker"
        evaluator = ConcurrentEvaluator(jobs=2, cores=2, event_bus=event_collector.bus)

        with pytest.raises(RuntimeError, match="fatal boom"):
            evaluator.evaluate(
                [
                    fatal_failure_marker_task(marker_path=marker_path, log_path=log_path),
                    ignored_failure_after_marker_task(marker_path=marker_path, log_path=log_path),
                ]
            )

        assert sorted(self._lines(log_path)) == ["fatal", "late"]
        assert evaluator.ignored_failures == ()
        failed = [event for event in event_collector.events if isinstance(event, TaskFailed)]
        assert len(failed) == 2
        assert not any(event.ignored for event in failed)

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

        assert sorted(self._lines(log_path)) == ["attempt", "work:1"]
        assert len(evaluator.ignored_failures) == 1


class TestShellTask:
    def test_shell_task_executes_and_returns_output_file(self, tmp_path: Path):
        output = tmp_path / "out.txt"
        log = tmp_path / "logs" / "command.log"

        result = evaluate(shell_write_output_task(output_path=str(output), log_path=str(log)))

        assert result == file(str(output))
        assert output.read_text(encoding="utf-8") == "payload"
        assert "captured stdout" in log.read_text(encoding="utf-8")
        assert "captured stderr" in log.read_text(encoding="utf-8")

    def test_shell_task_requires_declared_output_to_exist(self, tmp_path: Path):
        output = tmp_path / "missing.txt"

        with pytest.raises(FileNotFoundError, match="did not create output"):
            evaluate(shell_missing_output_task(output_path=str(output)))

    def test_shell_task_supports_multiple_outputs_and_creates_parent_dirs(self, tmp_path: Path):
        output_one = tmp_path / "results" / "reads" / "sample_1.fastq.gz"
        output_two = tmp_path / "results" / "reads" / "sample_2.fastq.gz"
        marker = tmp_path / "command.log"

        result = evaluate(
            shell_write_multiple_outputs_task(
                output_one=str(output_one),
                output_two=str(output_two),
                marker_path=str(marker),
            )
        )

        assert result == (file(str(output_one)), file(str(output_two)))
        assert output_one.read_text(encoding="utf-8") == "left"
        assert output_two.read_text(encoding="utf-8") == "right"
        assert marker.read_text(encoding="utf-8").splitlines() == ["run"]

    def test_shell_task_supports_list_outputs(self, tmp_path: Path):
        output_one = tmp_path / "list" / "sample_1.fastq.gz"
        output_two = tmp_path / "list" / "sample_2.fastq.gz"

        result = evaluate(
            shell_write_list_outputs_task(
                output_one=str(output_one),
                output_two=str(output_two),
            )
        )

        assert result == [file(str(output_one)), file(str(output_two))]

    def test_python_task_supports_list_output_values_for_scalar_file_annotation(
        self, tmp_path: Path
    ):
        output_one = tmp_path / "python-list" / "sample_1.fastq.gz"
        output_two = tmp_path / "python-list" / "sample_2.fastq.gz"
        output_paths = [str(output_one), str(output_two)]

        first = evaluate(write_multiple_files_with_scalar_annotation(output_paths=output_paths))
        second = evaluate(write_multiple_files_with_scalar_annotation(output_paths=output_paths))

        assert first == [file(str(output_one)), file(str(output_two))]
        assert second == [file(str(output_one)), file(str(output_two))]

    def test_shell_task_requires_all_declared_outputs_to_exist(self, tmp_path: Path):
        output_one = tmp_path / "results" / "sample_1.fastq.gz"
        output_two = tmp_path / "results" / "sample_2.fastq.gz"

        with pytest.raises(FileNotFoundError, match=str(output_two)):
            evaluate(
                shell_missing_multiple_outputs_task(
                    output_one=str(output_one),
                    output_two=str(output_two),
                )
            )

    def test_shell_task_retries_allow_transient_failures(self, tmp_path: Path):
        marker = tmp_path / "flaky-shell.marker"
        output = tmp_path / "flaky-shell.txt"
        log = tmp_path / "flaky-shell.log"

        result = evaluate(
            flaky_shell_task(
                marker_path=str(marker),
                output_path=str(output),
                log_path=str(log),
            )
        )

        assert result == file(str(output))
        assert output.read_text(encoding="utf-8") == "payload"
        assert "transient shell failure" in log.read_text(encoding="utf-8")

    def test_multi_output_shell_task_is_restored_from_cache(self, tmp_path: Path):
        output_one = tmp_path / "cached" / "sample_1.fastq.gz"
        output_two = tmp_path / "cached" / "sample_2.fastq.gz"
        marker = tmp_path / "cached-runs.log"

        first = evaluate(
            shell_write_multiple_outputs_task(
                output_one=str(output_one),
                output_two=str(output_two),
                marker_path=str(marker),
            )
        )
        second = evaluate(
            shell_write_multiple_outputs_task(
                output_one=str(output_one),
                output_two=str(output_two),
                marker_path=str(marker),
            )
        )

        assert first == (file(str(output_one)), file(str(output_two)))
        assert second == first
        assert marker.read_text(encoding="utf-8").splitlines() == ["run"]


class _FakeShellExecutor:
    def __init__(self) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


class _FakeProcessPoolExecutor(ProcessPoolExecutor):
    def __init__(self) -> None:
        self.shutdown_calls: list[tuple[bool, bool]] = []

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        self.shutdown_calls.append((wait, cancel_futures))


class _FakeTrackedProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid


class TestInterruptHandling:
    def test_interrupt_running_work_terminates_subprocesses_and_shuts_down_executors(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        evaluator = ConcurrentEvaluator()
        future_one = Future()
        future_two = Future()
        tracked_one = _FakeTrackedProcess(pid=101)
        tracked_two = _FakeTrackedProcess(pid=202)
        shell_executor = _FakeShellExecutor()
        python_executor = _FakeProcessPoolExecutor()
        terminated: list[int] = []
        pool_shutdowns: list[ProcessPoolExecutor] = []

        evaluator._running_futures = {
            future_one: (0, "shell"),
            future_two: (1, "python"),
        }
        evaluator._shell_runner._active_subprocesses = {
            tracked_one.pid: tracked_one,  # type: ignore[assignment]
            tracked_two.pid: tracked_two,  # type: ignore[assignment]
        }
        executors = Executors(jobs=evaluator.jobs, staging_jobs=evaluator._staging_jobs)
        executors._shell = shell_executor  # type: ignore[assignment]
        executors._python = python_executor
        evaluator._executors = executors

        monkeypatch.setattr(
            evaluator._shell_runner,
            "_terminate_subprocess",
            lambda *, process: terminated.append(process.pid),
        )
        monkeypatch.setattr(
            executors,
            "_terminate_process_pool_workers",
            lambda *, executor: pool_shutdowns.append(executor),
        )

        evaluator._interrupt_running_work()

        assert future_one.cancelled()
        assert future_two.cancelled()
        assert terminated == [101, 202]
        assert shell_executor.shutdown_calls == [(False, True)]
        assert python_executor.shutdown_calls == [(False, True)]
        assert pool_shutdowns == [python_executor]

    def test_run_subprocess_unregisters_process_after_completion(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        evaluator = ConcurrentEvaluator()
        popen_calls: list[tuple[object, dict[str, object]]] = []

        class FakePopen:
            def __init__(self, argv: object, **kwargs: object) -> None:
                popen_calls.append((argv, kwargs))
                self.pid = 31337
                self.returncode = 0

            def communicate(self) -> tuple[str, str]:
                assert 31337 in evaluator._shell_runner._active_subprocesses
                return ("stdout", "stderr")

        monkeypatch.setattr("ginkgo.runtime.task_runners.shell.subprocess.Popen", FakePopen)

        completed = evaluator._shell_runner.run_subprocess(argv=["echo", "hi"], use_shell=False)

        assert completed.returncode == 0
        assert completed.stdout == "stdout"
        assert completed.stderr == "stderr"
        assert evaluator._shell_runner._active_subprocesses == {}
        assert popen_calls[0][0] == ["echo", "hi"]

        if Path("/").anchor == "/":
            assert popen_calls[0][1]["start_new_session"] is True


class TestWorkerPayload:
    def test_build_worker_payload_does_not_contain_import_roots(self, tmp_path: Path) -> None:
        evaluator = ConcurrentEvaluator()
        expr = add_one_task(x=1)
        node_id = evaluator._register_expr(expr)
        node = evaluator._nodes[node_id]
        node.transport_path = tmp_path / "transport"
        node.transport_path.mkdir()
        node.resolved_args = {"x": 1}
        node.execution_args = {"x": 1}

        payload = evaluator._build_worker_payload(node=node)

        assert "ginkgo_import_roots" not in payload
        assert "sys_path" not in payload


class TestSecrets:
    def test_secret_rotation_keeps_cache_key_stable_and_redacts_manifest(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")

        first_resolver = build_secret_resolver(
            project_root=tmp_path,
            config={},
            environ={"API_TOKEN": "first-token"},
        )
        recorder = Ledger.start(root=tmp_path, run_id=make_run_id(workflow_path=workflow_path))
        expr = reveal_secret_task(
            secret_value=secret("API_TOKEN"),
            output_path="result.txt",
        )
        evaluator = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            run_dir=recorder.run_dir,
            event_bus=recorder.bus,
            secret_resolver=first_resolver,
        )

        first_result = evaluator.evaluate(expr)
        first_summary = recorder.finish()
        first_task = first_summary.tasks[0]
        cache_key = first_task.cache_key
        with CacheIndex.open(path=tmp_path / ".ginkgo" / "ginkgo.db") as index:
            entry = index.entry(cache_key)
        assert entry is not None

        assert Path(first_result).read_text(encoding="utf-8") == "first-token"
        assert first_task.inputs["secret_value"]["redacted"] is True
        assert "first-token" not in recorder.run_dir.manifest_path.read_text(encoding="utf-8")
        assert "first-token" not in str(entry["inputs"])

        second_recorder = Ledger.start(
            root=tmp_path, run_id=make_run_id(workflow_path=workflow_path)
        )
        second_resolver = build_secret_resolver(
            project_root=tmp_path,
            config={},
            environ={"API_TOKEN": "rotated-token"},
        )
        second_evaluator = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            run_dir=second_recorder.run_dir,
            event_bus=second_recorder.bus,
            secret_resolver=second_resolver,
        )

        second_result = second_evaluator.evaluate(expr)
        second_tasks = second_recorder.tasks()
        second_task = next(iter(second_tasks.values()))

        assert second_task["status"] == "cached"
        assert second_task["cache_key"] == cache_key
        assert Path(second_result).read_text(encoding="utf-8") == "first-token"

    def test_secret_values_are_redacted_from_logs_and_errors(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = Ledger.start(root=tmp_path, run_id=make_run_id(workflow_path=workflow_path))
        evaluator = ConcurrentEvaluator(
            jobs=1,
            cores=1,
            run_dir=recorder.run_dir,
            event_bus=recorder.bus,
            secret_resolver=build_secret_resolver(
                project_root=tmp_path,
                config={},
                environ={"API_TOKEN": "super-secret"},
            ),
        )

        with pytest.raises(RuntimeError, match="failure:\\[REDACTED\\]"):
            evaluator.evaluate(fail_with_secret_task(secret_value=secret("API_TOKEN")))

        tasks = recorder.tasks()
        task = next(iter(tasks.values()))
        stdout_log = recorder.path / task["stdout_log"]
        stderr_log = recorder.path / task["stderr_log"]

        assert "[REDACTED]" in stdout_log.read_text(encoding="utf-8")
        assert "super-secret" not in stdout_log.read_text(encoding="utf-8")
        assert "super-secret" not in stderr_log.read_text(encoding="utf-8")
        recorder.finish()
        assert "super-secret" not in recorder.run_dir.manifest_path.read_text(encoding="utf-8")


class TestValidation:
    def test_file_inputs_must_exist(self, tmp_path: Path):
        missing = tmp_path / "missing.txt"

        with pytest.raises(FileNotFoundError, match="must exist and be a file"):
            evaluate(read_text_task(path=str(missing)))

    def test_folder_inputs_must_not_contain_spaces(self, tmp_path: Path):
        spaced_dir = tmp_path / "has space"
        spaced_dir.mkdir()

        with pytest.raises(ValueError, match="must not contain spaces"):
            evaluate(list_dir_task(path=str(spaced_dir)))


class TestTmpDirLifecycle:
    def test_tmp_dir_is_created_and_removed_on_success(self):
        report_path = Path("scratch-path.txt")

        result = evaluate(write_scratch_task(report_path=str(report_path)))
        scratch_path = Path(report_path.read_text(encoding="utf-8"))

        assert result == str(scratch_path)
        assert not scratch_path.exists()

    def test_tmp_dir_is_kept_on_failure(self):
        report_path = Path("scratch-path.txt")

        with pytest.raises(RuntimeError, match="boom"):
            evaluate(fail_in_scratch_task(report_path=str(report_path)))

        scratch_path = Path(report_path.read_text(encoding="utf-8"))
        assert scratch_path.exists()
        shutil.rmtree(scratch_path)
