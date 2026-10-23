"""Tests for kind='subworkflow' task dispatch."""

from __future__ import annotations

import subprocess
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from ginkgo import SubWorkflowDirective, SubWorkflowResult, subworkflow, task
from ginkgo.runtime.evaluator import ConcurrentEvaluator, RootSkippedError
from ginkgo.runtime.task_runners.subworkflow import (
    DEPTH_ENV,
    PARENT_RUN_ID_ENV,
    PARENT_TASK_ID_ENV,
    SubWorkflowError,
    SubWorkflowRecursionError,
)
from tests.conftest import Ledger, run_cli

# Tasks defined at module scope so the task validator accepts them.


@task(kind="subworkflow")
def call_child_no_params_task(*, workflow_path: str) -> SubWorkflowResult:
    return subworkflow(workflow_path)


@task(kind="subworkflow", on_failure="ignore")
def call_optional_child_task(*, workflow_path: str) -> SubWorkflowResult:
    return subworkflow(workflow_path)


@task(kind="subworkflow")
def call_child_wrong_return(*, workflow_path: str) -> SubWorkflowResult:
    return {"not": "a SubWorkflowDirective"}  # type: ignore[return-value]


@task(kind="subworkflow")
def call_child_with_region(*, workflow_path: str, region: str) -> SubWorkflowResult:
    return subworkflow(workflow_path, params={"region": region})


@task(kind="subworkflow")
def call_child_with_config(*, workflow_path: str, config_path: str) -> SubWorkflowResult:
    return subworkflow(workflow_path, params={"region": "from-params"}, config=config_path)


@pytest.fixture(autouse=True)
def top_level_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run as a top-level ``ginkgo run``, even inside a sub-workflow's tests."""
    for name in (PARENT_RUN_ID_ENV, PARENT_TASK_ID_ENV, DEPTH_ENV):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def parent(tmp_path: Path):
    """A parent run's ledger and evaluator, closed after the test."""
    ledger = Ledger.start(root=tmp_path, run_id="parent_run")
    evaluator = ConcurrentEvaluator(run_dir=ledger.run_dir, event_bus=ledger.bus, jobs=1, cores=1)
    yield ledger, evaluator
    ledger.close()


@dataclass
class _StubbedChild:
    """Stands in for the child ``ginkgo run`` the parent would spawn.

    For states a real child cannot be made to produce on demand: a child that
    records no run, or a parent asserting on the command it built.
    """

    ledger: Ledger
    returncode: int = 0
    records_run: bool = True
    commands: list[str] = field(default_factory=list)

    def run_subprocess(self, **kwargs) -> subprocess.CompletedProcess[str]:
        argv = kwargs["argv"]
        self.commands.append(argv if isinstance(argv, str) else " ".join(argv))
        if self.records_run:
            child = Ledger.start(
                root=self.ledger.db.parent.parent,
                run_id="child_run",
                parent_run_id=self.ledger.run_id,
                parent_task_id="task_0000",
            )
            child.finish()
            child.close()
        return subprocess.CompletedProcess(argv, self.returncode, stdout="", stderr="")


@pytest.fixture
def stubbed_child(parent, monkeypatch: pytest.MonkeyPatch) -> _StubbedChild:
    ledger, evaluator = parent
    stub = _StubbedChild(ledger=ledger)
    monkeypatch.setattr(evaluator._shell_runner, "run_subprocess", stub.run_subprocess)
    Path("child.py").write_text("", encoding="utf-8")
    return stub


class TestSubWorkflowConstructor:
    def test_records_path_params_and_config(self, tmp_path: Path) -> None:
        for name in ("child.py", "a.yaml", "b.yaml"):
            Path(name).write_text("x = 1\n", encoding="utf-8")

        expr = subworkflow("child.py", params={"x": 1})
        assert isinstance(expr, SubWorkflowDirective)
        assert (expr.path, expr.params, expr.config) == ("child.py", {"x": 1}, ())
        assert subworkflow(tmp_path / "child.py").path == str(tmp_path / "child.py")
        assert subworkflow("child.py", config="a.yaml").config == ("a.yaml",)
        assert subworkflow("child.py", config=["a.yaml", "b.yaml"]).config == ("a.yaml", "b.yaml")

    @pytest.mark.parametrize(
        ("path", "error", "match"),
        [("", ValueError, "path"), ("missing.py", FileNotFoundError, "missing.py")],
    )
    def test_rejects_an_empty_or_missing_child(self, path: str, error: type, match: str) -> None:
        with pytest.raises(error, match=match):
            subworkflow(path)


class TestChildSourceHash:
    @pytest.fixture(autouse=True)
    def _child(self) -> None:
        Path("child/modules").mkdir(parents=True)
        Path("child/flow.py").write_text("x = 1\n", encoding="utf-8")
        Path("child/modules/helpers.py").write_text("y = 1\n", encoding="utf-8")
        Path("overrides.yaml").write_text("a: 1\n", encoding="utf-8")

    def _hash(self) -> str:
        return subworkflow("child/flow.py", config="overrides.yaml").source_hash

    @pytest.mark.parametrize(
        "edited", ["child/flow.py", "child/modules/helpers.py", "overrides.yaml"]
    )
    def test_an_edit_to_the_child_changes_the_hash(self, edited: str) -> None:
        before = self._hash()
        Path(edited).write_text("edited\n", encoding="utf-8")
        assert self._hash() != before

    def test_files_the_child_writes_do_not_change_the_hash(self) -> None:
        before = self._hash()
        for written in ("results/out.txt", ".ginkgo/cache.py", "modules/__pycache__/helpers.py"):
            path = Path("child", written)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("z = 1\n", encoding="utf-8")
        assert self._hash() == before


class TestEvaluatorDispatch:
    def test_excessive_depth_is_refused_before_spawning(
        self, parent, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, evaluator = parent
        Path("child.py").write_text("", encoding="utf-8")
        monkeypatch.setenv(DEPTH_ENV, "8")

        with pytest.raises(SubWorkflowRecursionError):
            evaluator.evaluate(call_child_no_params_task(workflow_path="child.py"))

    def test_wrong_return_type_rejected(self, parent) -> None:
        _, evaluator = parent
        Path("child.py").write_text("", encoding="utf-8")

        with pytest.raises(TypeError, match="subworkflow"):
            evaluator.evaluate(call_child_wrong_return(workflow_path="child.py"))

    def test_a_child_that_recorded_no_run_raises(self, parent, stubbed_child) -> None:
        stubbed_child.records_run = False

        with pytest.raises(RuntimeError, match="recorded no run"):
            parent[1].evaluate(call_child_no_params_task(workflow_path="child.py"))

    def test_an_ignored_child_failure_still_records_the_child_run(
        self, parent, stubbed_child
    ) -> None:
        """``on_failure="ignore"`` costs nothing in discoverability."""
        ledger, evaluator = parent
        stubbed_child.returncode = 2

        with pytest.raises(RootSkippedError, match="call_optional_child_task"):
            evaluator.evaluate(call_optional_child_task(workflow_path="child.py"))

        assert len(evaluator.ignored_failures) == 1
        entry = ledger.task()
        assert (entry["status"], entry["ignored"], entry["sub_run_id"]) == (
            "failed",
            True,
            "child_run",
        )

    def test_params_layer_over_forwarded_config(self, parent, stubbed_child) -> None:
        # Config files layer in order, so the params file must come last.
        Path("child.toml").write_text('[params]\nregion = "from-config"\n', encoding="utf-8")

        parent[1].evaluate(
            call_child_with_config(workflow_path="child.py", config_path="child.toml")
        )

        (command,) = stubbed_child.commands
        configs = [part.split()[0].strip("'\"") for part in command.split("--config")[1:]]
        assert configs[0] == "child.toml"
        assert configs[-1].endswith("child-parameters.yaml")


_FAILING_CHILD = """
from ginkgo import flow, task

@task()
def boom() -> int:
    raise RuntimeError("child broke")

@flow
def main():
    return boom()
"""

_CHILD_DECLARING_PARAMS = """
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
"""


@pytest.mark.integration
class TestRealChild:
    """Each test spawns a real child ``ginkgo run``."""

    def test_child_resolves_parent_params_and_links_back_to_the_parent(self, parent) -> None:
        """Parent params reach the child's ginkgo.param, layering over its own table."""
        ledger, evaluator = parent
        Path("child.py").write_text(_CHILD_DECLARING_PARAMS, encoding="utf-8")
        # The child's own table supplies depth; the parent supplies only region,
        # so depth must survive the layering rather than fall back to its default.
        Path("ginkgo.toml").write_text("[params]\ndepth = 42\n", encoding="utf-8")

        result = evaluator.evaluate(call_child_with_region(workflow_path="child.py", region="2L"))

        assert isinstance(result, SubWorkflowResult)
        assert result.status == "success"
        assert Path("child-result.txt").read_text(encoding="utf-8") == "2L/42"
        # The parent found its child through the ledger, so the child recorded
        # the parent run and task it was handed.
        entry = ledger.task()
        assert (entry["status"], entry["sub_run_id"]) == ("succeeded", result.run_id)

    def test_a_failing_child_fails_the_parent_task(self, parent) -> None:
        ledger, evaluator = parent
        Path("child.py").write_text(_FAILING_CHILD, encoding="utf-8")

        with pytest.raises(SubWorkflowError) as exc_info:
            evaluator.evaluate(call_child_no_params_task(workflow_path="child.py"))

        assert exc_info.value.exit_code == 1
        assert exc_info.value.child_run_id is not None
        entry = ledger.task()
        assert (entry["status"], entry["sub_run_id"]) == ("failed", exc_info.value.child_run_id)

    def test_editing_the_child_reruns_the_parent_task(self) -> None:
        child_source = textwrap.dedent(
            """
            from pathlib import Path
            from ginkgo import flow, task

            @task()
            def touch(marker_path: str) -> str:
                Path(marker_path).write_text("VERSION", encoding="utf-8")
                return marker_path

            @flow
            def main():
                return touch(marker_path=str(Path.cwd() / "child_marker.txt"))
            """
        )
        Path("child").mkdir()
        Path("child/flow.py").write_text(child_source.replace("VERSION", "v1"), encoding="utf-8")
        Path("parent.py").write_text(
            textwrap.dedent(
                """
                from ginkgo import flow, task, subworkflow, SubWorkflowResult

                @task(kind="subworkflow")
                def run_child() -> SubWorkflowResult:
                    return subworkflow("child/flow.py")

                @flow
                def main():
                    return run_child()
                """
            ),
            encoding="utf-8",
        )

        first = run_cli("run", "parent.py")
        assert first.returncode == 0, first.stderr
        assert Path("child_marker.txt").read_text(encoding="utf-8") == "v1"
        assert len([p for p in Path(".ginkgo/runs").iterdir() if p.is_dir()]) == 2

        Path("child/flow.py").write_text(child_source.replace("VERSION", "v2"), encoding="utf-8")
        second = run_cli("run", "parent.py")
        assert second.returncode == 0, second.stderr
        assert Path("child_marker.txt").read_text(encoding="utf-8") == "v2"
