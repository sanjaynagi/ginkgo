"""Explicit per-task thread declaration tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from ginkgo import evaluate, flow, shell, task
from ginkgo.runtime.task_runners.shell import build_shell_subprocess_env


@task(threads=4)
def _big_task() -> str:
    return "ok"


@flow
def _big_flow():
    return _big_task()


@task(threads=3)
def _report_threads(threads: int = 1) -> int:
    return threads


@flow
def _report_flow():
    return _report_threads()


@task("shell", threads=5)
def _shell_step_default() -> str:
    return shell(cmd="true", output="out.txt")


@task("shell", threads=6, export_thread_env=True)
def _shell_step_export() -> str:
    return shell(cmd="true", output="out.txt")


@task("shell", threads=2)
def _shell_step_inherit() -> str:
    return shell(cmd="true", output="out.txt")


@task("shell", threads=7)
def _write_threads(out_path: str) -> str:
    return shell(
        cmd=f'sh -c "echo $GINKGO_THREADS > {out_path}"',
        output=out_path,
    )


@flow
def _write_threads_flow(out_path: str):
    return _write_threads(out_path=out_path)


class TestSchedulerReadsDecoratorThreads:
    def test_oversize_threads_rejected_against_cores_budget(self) -> None:
        with pytest.raises(ValueError, match="requires 4 cores but only 1 are available"):
            evaluate(_big_flow(), jobs=1, cores=1)

    def test_decorator_threads_value_injected_into_function(self) -> None:
        result = evaluate(_report_flow(), jobs=1, cores=4)
        assert result == 3


class TestShellSubprocessEnv:
    def test_ginkgo_threads_always_set(self) -> None:
        env = build_shell_subprocess_env(task_def=_shell_step_default)
        assert env["GINKGO_THREADS"] == "5"
        assert env.get("OMP_NUM_THREADS", "") != "5"

    def test_export_thread_env_sets_blas_vars(self) -> None:
        env = build_shell_subprocess_env(task_def=_shell_step_export)
        assert env["GINKGO_THREADS"] == "6"
        assert env["OMP_NUM_THREADS"] == "6"
        assert env["MKL_NUM_THREADS"] == "6"
        assert env["OPENBLAS_NUM_THREADS"] == "6"
        assert env["NUMEXPR_NUM_THREADS"] == "6"

    def test_inherits_existing_environment(self) -> None:
        env = build_shell_subprocess_env(task_def=_shell_step_inherit)
        assert "PATH" in env or "HOME" in env


class TestShellTaskPropagatesThreads:
    def test_shell_subprocess_sees_ginkgo_threads(self, tmp_path: Path) -> None:
        out_path = tmp_path / "threads.txt"
        evaluate(_write_threads_flow(out_path=str(out_path)), jobs=1, cores=8)
        assert out_path.read_text(encoding="utf-8").strip() == "7"
