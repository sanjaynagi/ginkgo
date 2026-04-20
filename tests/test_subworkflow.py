"""Tests for kind='subworkflow' task dispatch."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

from ginkgo import SubWorkflowExpr, SubWorkflowResult, subworkflow, task
from ginkgo.runtime.caching.provenance import (
    RunProvenanceRecorder,
    load_manifest,
    make_run_id,
)
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.task_runners.subworkflow import (
    DEPTH_ENV,
    PARENT_RUN_ENV,
    SubWorkflowError,
    SubWorkflowRecursionError,
    _extract_child_run_id,
)


# Tasks defined at module scope so the task validator accepts them.


@task(kind="subworkflow")
def call_child_task(*, workflow_path: str, region: str) -> SubWorkflowResult:
    return subworkflow(workflow_path, params={"region": region})


@task(kind="subworkflow")
def call_child_no_params_task(*, workflow_path: str) -> SubWorkflowResult:
    return subworkflow(workflow_path)


@task(kind="subworkflow")
def call_child_wrong_return(*, workflow_path: str) -> SubWorkflowResult:
    return {"not": "a SubWorkflowExpr"}  # type: ignore[return-value]


class TestSubWorkflowConstructor:
    def test_basic_expr(self) -> None:
        expr = subworkflow("workflows/child.py", params={"x": 1})
        assert isinstance(expr, SubWorkflowExpr)
        assert expr.path == "workflows/child.py"
        assert expr.params == {"x": 1}
        assert expr.config == ()

    def test_accepts_path_object(self, tmp_path: Path) -> None:
        expr = subworkflow(tmp_path / "child.py")
        assert expr.path == str(tmp_path / "child.py")

    def test_rejects_empty_path(self) -> None:
        with pytest.raises(ValueError, match="path"):
            subworkflow("")

    def test_config_accepts_single_path(self) -> None:
        expr = subworkflow("child.py", config="overrides.yaml")
        assert expr.config == ("overrides.yaml",)

    def test_config_accepts_sequence(self) -> None:
        expr = subworkflow("child.py", config=["a.yaml", "b.yaml"])
        assert expr.config == ("a.yaml", "b.yaml")


class TestChildRunIdParsing:
    def test_extracts_run_id(self) -> None:
        text = "loading...\nGINKGO_CHILD_RUN_ID=20260420_121212_000001_abcd1234\ndone\n"
        assert _extract_child_run_id(text) == "20260420_121212_000001_abcd1234"

    def test_returns_none_when_missing(self) -> None:
        assert _extract_child_run_id("no marker here") is None

    def test_prefers_last_match(self) -> None:
        text = "GINKGO_CHILD_RUN_ID=first\nGINKGO_CHILD_RUN_ID=second\n"
        assert _extract_child_run_id(text) == "second"


class TestRecursionGuard:
    def test_rejects_excessive_depth(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        child = tmp_path / "child.py"
        child.write_text("", encoding="utf-8")

        monkeypatch.setenv(DEPTH_ENV, "8")

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "parent.py"),
            workflow_path=tmp_path / "parent.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        runner = evaluator._subworkflow_runner

        class _FakeNode:
            class _TaskDef:
                name = "test.call_child_task"

            task_def = _TaskDef()

        with pytest.raises(SubWorkflowRecursionError):
            runner.run_subworkflow(
                node=_FakeNode(),
                subworkflow_expr=subworkflow(str(child)),
            )


class TestEvaluatorDispatch:
    def _make_recorder(self, tmp_path: Path) -> RunProvenanceRecorder:
        return RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "parent.py"),
            workflow_path=tmp_path / "parent.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )

    def test_dispatch_captures_child_run_id(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("# placeholder\n", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)

        captured: dict[str, Any] = {}

        def fake_run_subprocess(
            *,
            argv: str | list[str],
            use_shell: bool,
            on_stdout: Any = None,
            on_stderr: Any = None,
            env: dict[str, str] | None = None,
        ) -> subprocess.CompletedProcess[str]:
            captured["argv"] = argv
            captured["env"] = env
            stdout = "GINKGO_CHILD_RUN_ID=fake_child_run_id_123\n"
            if on_stdout is not None:
                on_stdout(stdout)
            return subprocess.CompletedProcess(
                args=argv,
                returncode=0,
                stdout=stdout,
                stderr="",
            )

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        result = evaluator.evaluate(call_child_task(workflow_path=str(child_path), region="emea"))

        assert isinstance(result, SubWorkflowResult)
        assert result.run_id == "fake_child_run_id_123"
        assert result.status == "success"

        cmd = captured["argv"] if isinstance(captured["argv"], str) else " ".join(captured["argv"])
        assert "ginkgo.cli" in cmd
        assert str(child_path) in cmd
        assert "--config" in cmd  # params dict produced a temp config

        env = captured["env"]
        assert env[PARENT_RUN_ENV] == recorder.run_id
        assert env[DEPTH_ENV] == "1"

        manifest = load_manifest(recorder.run_dir)
        (task_entry,) = manifest["tasks"].values()
        assert task_entry["sub_run_id"] == "fake_child_run_id_123"
        assert task_entry["status"] == "succeeded"

    def test_dispatch_fails_on_non_zero_exit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)

        def fake_run_subprocess(**kwargs: Any) -> subprocess.CompletedProcess[str]:
            stdout = "GINKGO_CHILD_RUN_ID=child_fail_id\n"
            on_stdout = kwargs.get("on_stdout")
            if on_stdout is not None:
                on_stdout(stdout)
            return subprocess.CompletedProcess(
                args=kwargs.get("argv", ""),
                returncode=2,
                stdout=stdout,
                stderr="something broke\n",
            )

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        with pytest.raises(SubWorkflowError) as exc_info:
            evaluator.evaluate(call_child_no_params_task(workflow_path=str(child_path)))

        assert exc_info.value.exit_code == 2
        assert exc_info.value.child_run_id == "child_fail_id"

        manifest = load_manifest(recorder.run_dir)
        (task_entry,) = manifest["tasks"].values()
        assert task_entry["status"] == "failed"
        assert task_entry["sub_run_id"] == "child_fail_id"

    def test_wrong_return_type_rejected(self, tmp_path: Path) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)
        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)

        with pytest.raises(TypeError, match="subworkflow"):
            evaluator.evaluate(call_child_wrong_return(workflow_path=str(child_path)))

    def test_missing_run_id_marker_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)

        def fake_run_subprocess(**kwargs: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=kwargs.get("argv", ""),
                returncode=0,
                stdout="no marker in this output\n",
                stderr="",
            )

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        with pytest.raises(RuntimeError, match="GINKGO_CHILD_RUN_ID"):
            evaluator.evaluate(call_child_no_params_task(workflow_path=str(child_path)))


class TestEndToEnd:
    """Full-stack test that actually invokes ``python -m ginkgo.cli run`` once."""

    def test_parent_workflow_invokes_child_subprocess(self, tmp_path: Path) -> None:
        # Child workflow writes a marker so we can confirm it actually ran.
        child = tmp_path / "child.py"
        child.write_text(
            textwrap.dedent(
                """
                from pathlib import Path
                from ginkgo import flow, task


                @task()
                def touch(marker_path: str) -> str:
                    Path(marker_path).write_text("child ran", encoding="utf-8")
                    return marker_path


                @flow
                def main():
                    return touch(marker_path=str(Path.cwd() / "child_marker.txt"))
                """
            ),
            encoding="utf-8",
        )

        parent = tmp_path / "parent.py"
        parent.write_text(
            textwrap.dedent(
                f"""
                from ginkgo import flow, task, subworkflow, SubWorkflowResult


                @task(kind="subworkflow")
                def run_child() -> SubWorkflowResult:
                    return subworkflow({str(child)!r})


                @flow
                def main():
                    return run_child()
                """
            ),
            encoding="utf-8",
        )

        env = os.environ.copy()
        env.pop("GINKGO_CALLED_FROM_PARENT_RUN", None)
        env.pop("GINKGO_CALL_DEPTH", None)

        result = subprocess.run(
            [sys.executable, "-m", "ginkgo.cli", "run", str(parent)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        )

        assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
        assert (tmp_path / "child_marker.txt").read_text(encoding="utf-8") == "child ran"

        runs_dir = tmp_path / ".ginkgo" / "runs"
        run_dirs = sorted(p for p in runs_dir.iterdir() if p.is_dir())
        # Two runs: parent + child.
        assert len(run_dirs) == 2
