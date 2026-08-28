"""Tests for kind='subworkflow' task dispatch."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml

from ginkgo import SubWorkflowDirective, SubWorkflowResult, subworkflow, task
from ginkgo.runtime.caching.provenance import make_run_id
from ginkgo.runtime.evaluator import ConcurrentEvaluator
from ginkgo.runtime.task_runners.subworkflow import (
    DEPTH_ENV,
    PARENT_RUN_ID_ENV,
    PARENT_TASK_ID_ENV,
    SubWorkflowError,
    SubWorkflowRecursionError,
)

from tests.conftest import Ledger


def _record_child_run(*, root: Path, run_id: str, parent_run_id: str) -> None:
    """Record the run a child ``ginkgo run`` would have recorded for itself.

    The parent finds its child by asking the ledger who names it as a parent,
    so a stubbed subprocess has to leave that row behind.
    """
    child = Ledger.start(
        root=root,
        run_id=run_id,
        parent_run_id=parent_run_id,
        parent_task_id="task_0000",
    )
    child.finish()
    child.close()


# Tasks defined at module scope so the task validator accepts them.


@task(kind="subworkflow")
def call_child_task(*, workflow_path: str, region: str) -> SubWorkflowResult:
    return subworkflow(workflow_path, params={"region": region})


@task(kind="subworkflow")
def call_child_no_params_task(*, workflow_path: str) -> SubWorkflowResult:
    return subworkflow(workflow_path)


@task(kind="subworkflow")
def call_child_wrong_return(*, workflow_path: str) -> SubWorkflowResult:
    return {"not": "a SubWorkflowDirective"}  # type: ignore[return-value]


class TestSubWorkflowConstructor:
    def test_basic_expr(self) -> None:
        expr = subworkflow("workflows/child.py", params={"x": 1})
        assert isinstance(expr, SubWorkflowDirective)
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


class TestRecursionGuard:
    def test_rejects_excessive_depth(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        child = tmp_path / "child.py"
        child.write_text("", encoding="utf-8")

        monkeypatch.setenv(DEPTH_ENV, "8")

        recorder = Ledger.start(
            root=tmp_path, run_id=make_run_id(workflow_path=tmp_path / "parent.py")
        )

        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )
        runner = evaluator._subworkflow_runner

        class _FakeNode:
            class _TaskDef:
                name = "test.call_child_task"

            task_def = _TaskDef()

        with pytest.raises(SubWorkflowRecursionError):
            runner.run_subworkflow(
                node=_FakeNode(),
                directive=subworkflow(str(child)),
            )


class TestEvaluatorDispatch:
    def _make_recorder(self, tmp_path: Path) -> Ledger:
        return Ledger.start(
            root=tmp_path, run_id=make_run_id(workflow_path=tmp_path / "parent.py")
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
            usage_callback: Any = None,
        ) -> subprocess.CompletedProcess[str]:
            captured["argv"] = argv
            captured["env"] = env
            _record_child_run(
                root=tmp_path,
                run_id="fake_child_run_id_123",
                parent_run_id=recorder.run_id,
            )
            if on_stdout is not None:
                on_stdout("child output\n")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")

        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)

        result = evaluator.evaluate(call_child_task(workflow_path=str(child_path), region="emea"))

        assert isinstance(result, SubWorkflowResult)
        assert result.run_id == "fake_child_run_id_123"
        assert result.status == "success"

        cmd = captured["argv"] if isinstance(captured["argv"], str) else " ".join(captured["argv"])
        assert "ginkgo.cli" in cmd
        assert str(child_path) in cmd
        assert "--config" in cmd  # params dict produced a temp config

        env = captured["env"]
        assert env[PARENT_RUN_ID_ENV] == recorder.run_id
        assert env[PARENT_TASK_ID_ENV] == "task_0000"
        assert env[DEPTH_ENV] == "1"

        task_entry = recorder.task()
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
            _record_child_run(root=tmp_path, run_id="child_fail_id", parent_run_id=recorder.run_id)
            return subprocess.CompletedProcess(
                args=kwargs.get("argv", ""),
                returncode=2,
                stdout="",
                stderr="something broke\n",
            )

        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)

        with pytest.raises(SubWorkflowError) as exc_info:
            evaluator.evaluate(call_child_no_params_task(workflow_path=str(child_path)))

        assert exc_info.value.exit_code == 2
        assert exc_info.value.child_run_id == "child_fail_id"

        task_entry = recorder.task()
        assert task_entry["status"] == "failed"
        assert task_entry["sub_run_id"] == "child_fail_id"

    def test_wrong_return_type_rejected(self, tmp_path: Path) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)
        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )

        with pytest.raises(TypeError, match="subworkflow"):
            evaluator.evaluate(call_child_wrong_return(workflow_path=str(child_path)))

    def test_a_child_that_recorded_no_run_raises(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = self._make_recorder(tmp_path)

        def fake_run_subprocess(**kwargs: Any) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                args=kwargs.get("argv", ""), returncode=0, stdout="", stderr=""
            )

        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)

        with pytest.raises(RuntimeError, match="recorded no run"):
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
        env.pop("GINKGO_PARENT_RUN_ID", None)
        env.pop("GINKGO_PARENT_TASK_ID", None)
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


@task(kind="subworkflow")
def call_child_with_region(*, workflow_path: str, region: str) -> SubWorkflowResult:
    return subworkflow(workflow_path, params={"region": region})


_CHILD_DECLARING_PARAMS = (
    """
import ginkgo
from pathlib import Path
from ginkgo import file, flow, task

region = ginkgo.param("region")
depth = ginkgo.param("depth", type=int, default=7)

@task()
def write_region(r: str, d: int, output_path: str) -> file:
    out = Path(output_path)
    out.write_text(f"{r}/{d}", encoding="utf-8")
    return out

@flow
def main():
    return write_region(r=region, d=depth, output_path="child-result.txt")
""".strip()
    + "\n"
)


class TestSubworkflowParams:
    def test_params_are_written_as_a_params_table(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child receives params under [params], not as top-level config keys."""
        child_path = tmp_path / "child.py"
        child_path.write_text("", encoding="utf-8")

        recorder = Ledger.start(root=tmp_path, run_id="parent_run")
        written: dict[str, Any] = {}

        def fake_run_subprocess(**kwargs: Any) -> subprocess.CompletedProcess[str]:
            argv = kwargs.get("argv", "")
            cmd = argv if isinstance(argv, str) else " ".join(argv)
            # Read the temp config while it still exists; it is deleted on return.
            config_path = Path(cmd.split("--config")[1].strip().strip("'\""))
            written["payload"] = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            _record_child_run(root=tmp_path, run_id="child_run_1", parent_run_id="parent_run")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")

        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )
        monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", fake_run_subprocess)

        evaluator.evaluate(
            call_child_with_region(workflow_path=str(child_path), region="2L:1-100")
        )

        assert written["payload"] == {"params": {"region": "2L:1-100"}}

    @pytest.mark.integration
    def test_child_resolves_parent_params_through_declared_parameters(self) -> None:
        """Parent params reach the child's ginkgo.param, layering over its own table."""
        workspace = Path.cwd()
        child_path = workspace / "child.py"
        child_path.write_text(_CHILD_DECLARING_PARAMS, encoding="utf-8")
        # The child's own table supplies depth; the parent supplies only region,
        # so depth must survive the layering rather than fall back to its default.
        (workspace / "ginkgo.toml").write_text("[params]\ndepth = 42\n", encoding="utf-8")

        recorder = Ledger.start(root=workspace, run_id="parent_run")
        evaluator = ConcurrentEvaluator(
            run_dir=recorder.run_dir, event_bus=recorder.bus, jobs=1, cores=1
        )

        result = evaluator.evaluate(
            call_child_with_region(workflow_path=str(child_path), region="2L:1-100")
        )

        assert isinstance(result, SubWorkflowResult)
        assert result.status == "success"
        assert (workspace / "child-result.txt").read_text(encoding="utf-8") == "2L:1-100/42"
