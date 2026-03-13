"""Unit tests for the evaluator runtime."""

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from ginkgo import evaluate, file, folder, shell_task, task, tmp_dir
from ginkgo.runtime.evaluator import CycleError, _ConcurrentEvaluator


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task()
def add_one_task(x: int) -> int:
    return x + 1


@task()
def logged_work_task(x: int, log_path: str) -> int:
    _append_line(log_path, f"work:{x}")
    return x + 1


@task(retries=2)
def flaky_retry_task(marker_path: str, log_path: str) -> str:
    _append_line(log_path, "attempt")
    marker = Path(marker_path)
    failures = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
    if failures < 1:
        marker.write_text(str(failures + 1), encoding="utf-8")
        raise RuntimeError("transient failure")
    return "ok"


@task(retries=2)
def always_fail_retry_task(log_path: str) -> str:
    _append_line(log_path, "attempt")
    raise RuntimeError("still broken")


@task()
def always_fail_once_task(log_path: str) -> str:
    _append_line(log_path, "attempt")
    raise RuntimeError("no retry configured")


@task()
def shell_write_output_task(output_path: str, log_path: str) -> file:
    return shell_task(
        cmd=(
            "printf 'captured stdout\\n'; "
            "printf 'captured stderr\\n' 1>&2; "
            f"printf 'payload' > {output_path}"
        ),
        output=output_path,
        log=log_path,
    )


@task()
def shell_missing_output_task(output_path: str) -> file:
    return shell_task(cmd="true", output=output_path)


@task(retries=2)
def flaky_shell_task(marker_path: str, output_path: str, log_path: str) -> file:
    return shell_task(
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


@task()
def read_text_task(path: file) -> str:
    return Path(path).read_text()


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
def dataframe_task(log_path: str, start: int) -> object:
    _append_line(log_path, f"df:{start}")
    return pd.DataFrame({"sample": [start, start + 1], "value": [start * 2, start * 2 + 1]})


@task()
def dataframe_total_task(df: object) -> int:
    return int(df["value"].sum())


@task()
def passthrough_task(value: object | None = None) -> object:
    return value


@task()
def build_dynamic_cycle_task() -> object:
    first = passthrough_task()
    second = passthrough_task(value=first)
    first.args["value"] = second
    return first


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

    def test_structured_logs_are_emitted_to_stderr(self, capsys: pytest.CaptureFixture[str]):
        log_path = "work-events.log"
        result = evaluate(logged_work_task(x=2, log_path=log_path))
        captured = capsys.readouterr()

        assert result == 3
        assert '"status": "running"' in captured.err
        assert '"status": "succeeded"' in captured.err
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["work:2"]

    def test_second_run_is_served_from_cache(self, capsys: pytest.CaptureFixture[str]):
        log_path = "work-events.log"

        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3
        capsys.readouterr()

        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3
        captured = capsys.readouterr()

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["work:2"]
        assert '"status": "cached"' in captured.err
        assert (Path(".ginkgo") / "cache").exists()

    def test_local_task_fails_immediately_at_runtime(self):
        @task()
        def local_task(x: int) -> int:
            return x + 1

        with pytest.raises(TypeError, match="top-level function"):
            evaluate(local_task(x=1))

    def test_numpy_array_can_cross_the_python_task_boundary(self):
        result = evaluate(array_task(start=2))

        assert np.array_equal(result, np.array([2, 3, 4]))

    def test_numpy_array_can_flow_into_downstream_python_task(self):
        result = evaluate(sum_array_task(values=array_task(start=2)))

        assert result == 9

    def test_dataframe_results_are_cached_and_restored(
        self,
        capsys: pytest.CaptureFixture[str],
    ):
        log_path = "dataframe-events.log"

        first = evaluate(dataframe_task(log_path=log_path, start=3))
        capsys.readouterr()

        second = evaluate(dataframe_task(log_path=log_path, start=3))
        captured = capsys.readouterr()

        assert first.equals(second)
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["df:3"]
        assert '"status": "cached"' in captured.err

    def test_dataframe_can_flow_into_downstream_python_task(self):
        result = evaluate(dataframe_total_task(df=dataframe_task(log_path="df.log", start=4)))

        assert result == 17

    def test_validate_rejects_direct_expression_cycles(self):
        first = passthrough_task()
        second = passthrough_task(value=first)
        first.args["value"] = second

        evaluator = _ConcurrentEvaluator()
        with pytest.raises(
            CycleError,
            match="Detected cycle in workflow graph: test_evaluator.passthrough_task -> "
            "test_evaluator.passthrough_task -> test_evaluator.passthrough_task",
        ):
            evaluator.validate(first)

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
        capsys: pytest.CaptureFixture[str],
    ):
        marker_path = "flaky.marker"
        log_path = "flaky.log"

        assert evaluate(flaky_retry_task(marker_path=marker_path, log_path=log_path)) == "ok"
        captured = capsys.readouterr()

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["attempt", "attempt"]
        assert '"status": "waiting"' in captured.err
        assert '"attempt": 1' in captured.err

        assert evaluate(flaky_retry_task(marker_path=marker_path, log_path=log_path)) == "ok"
        cached = capsys.readouterr()

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["attempt", "attempt"]
        assert '"status": "cached"' in cached.err

    def test_task_retries_fail_after_final_attempt(self):
        log_path = "always-fail.log"

        with pytest.raises(RuntimeError, match="still broken"):
            evaluate(always_fail_retry_task(log_path=log_path))

        assert Path(log_path).read_text(encoding="utf-8").splitlines() == [
            "attempt",
            "attempt",
            "attempt",
        ]


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
