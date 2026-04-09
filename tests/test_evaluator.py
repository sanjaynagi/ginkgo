"""Unit tests for the evaluator runtime."""

from concurrent.futures import Future, ProcessPoolExecutor
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ginkgo import (
    AssetRef,
    asset,
    evaluate,
    file,
    folder,
    notebook,
    script,
    secret,
    shell,
    task,
    tmp_dir,
)
from ginkgo.pixi import PixiRegistry
from ginkgo.runtime.backend import LocalBackend
from ginkgo.runtime.evaluator import CycleError, _ConcurrentEvaluator
from tests.conftest import EventCollector
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.runtime.events import EventBus, TaskNotice
from ginkgo.runtime.caching.provenance import RunProvenanceRecorder, load_manifest, make_run_id
from ginkgo.runtime.environment.secrets import build_secret_resolver


def _append_line(path: str, line: str) -> None:
    with Path(path).open("a", encoding="utf-8") as handle:
        handle.write(f"{line}\n")


@task("notebook")
def notebook_ipynb_task(*, notebook_path: str, value: int) -> Path:
    """Run an ipynb notebook by path."""
    return notebook(notebook_path)


@task("notebook")
def notebook_ipynb_with_output_task(*, notebook_path: str, output_path: str) -> Path:
    """Run an ipynb notebook that declares an output file."""
    return notebook(notebook_path, outputs=output_path)


@task("notebook")
def notebook_marimo_task(*, notebook_path: str, sample_id: str) -> Path:
    """Run a marimo notebook by path."""
    return notebook(notebook_path)


@task("notebook", env="test_env")
def notebook_ipynb_env_task(*, notebook_path: str, value: int) -> Path:
    """Run an ipynb notebook inside a managed task environment."""
    return notebook(notebook_path)


@task("script")
def python_script_task(*, script_path: str, output_path: str) -> Path:
    """Run a Python script by path, writing to output_path."""
    return script(script_path, outputs=output_path)


@task()
def add_one_task(x: int) -> int:
    return x + 1


@task()
def logged_work_task(x: int, log_path: str) -> int:
    _append_line(log_path, f"work:{x}")
    return x + 1


@task()
def sum_keyword_pair_task(*, left: int, right: int) -> int:
    return left + right


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


@task(kind="shell")
def shell_write_output_task(output_path: str, log_path: str) -> file:
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
    output_one: str,
    output_two: str,
    marker_path: str,
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
def flaky_shell_task(marker_path: str, output_path: str, log_path: str) -> file:
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


@task()
def python_returns_shell_task(output_path: str) -> file:
    return shell(cmd=f"printf 'payload' > {output_path}", output=output_path)


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
def reveal_secret_task(secret_value: str, output_path: str) -> file:
    print(f"stdout:{secret_value}")
    print(f"stderr:{secret_value}", file=sys.stderr)
    Path(output_path).write_text(secret_value, encoding="utf-8")
    return file(output_path)


@task()
def fail_with_secret_task(secret_value: str) -> str:
    print(f"task-secret:{secret_value}")
    raise RuntimeError(f"failure:{secret_value}")


@task()
def write_asset_task(output_path: str, text: str = "payload") -> file:
    Path(output_path).write_text(text, encoding="utf-8")
    return asset(output_path, name="prepared_data")


@task()
def transform_asset_task(source: object, output_path: str) -> file:
    assert isinstance(source, AssetRef)
    payload = Path(source.artifact_path).read_text(encoding="utf-8").upper()
    Path(output_path).write_text(payload, encoding="utf-8")
    return asset(output_path, name="transformed_data")


@task()
def read_asset_task(source: object, log_path: str) -> str:
    assert isinstance(source, AssetRef)
    Path(log_path).write_text(source.version_id, encoding="utf-8")
    return Path(source.artifact_path).read_text(encoding="utf-8")


@task(kind="shell")
def shell_asset_task(output_path: str) -> file:
    return shell(
        cmd=f"printf 'shell-payload' > {output_path}",
        output=asset(output_path, name="shell_asset"),
    )


@task(kind="shell")
def shell_asset_with_payload_task(output_path: str, payload: str) -> file:
    return shell(
        cmd=f"printf '{payload}' > {output_path}",
        output=asset(output_path, name="shell_payload_asset"),
    )


@task()
def nested_asset_inputs_task(sources: list[object], output_path: str) -> str:
    first = sources[0]
    assert isinstance(first, AssetRef)
    Path(output_path).write_text(first.version_id, encoding="utf-8")
    return first.version_id


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


_PIXI_TEST_ENV = "race_env"


@task(env=_PIXI_TEST_ENV, kind="shell")
def pixi_shell_output_task(output_path: str) -> file:
    return shell(
        cmd=f"printf 'payload' > {output_path}",
        output=output_path,
    )


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

    def test_second_run_is_served_from_cache(self) -> None:
        log_path = "work-events.log"

        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3
        assert evaluate(logged_work_task(x=2, log_path=log_path)) == 3

        # The second run reuses the cached result, so the task body only ran once.
        assert Path(log_path).read_text(encoding="utf-8").splitlines() == ["work:2"]
        assert (Path(".ginkgo") / "cache").exists()

    def test_ipynb_notebook_task_records_html_and_uses_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nb_path = tmp_path / "report.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        expr = notebook_ipynb_task(notebook_path=str(nb_path), value=7)

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )

        calls: list[str] = []

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            calls.append(command)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "ipykernel install" in command:
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(
                    args=argv, returncode=0, stdout="install ok\n", stderr=""
                )
            if "papermill" in command:
                output_path = recorder.run_dir / "notebooks" / "task_0000.ipynb"
                output_path.write_text("executed", encoding="utf-8")
                return subprocess.CompletedProcess(
                    args=argv, returncode=0, stdout="papermill ok\n", stderr=""
                )
            if "nbconvert" in command:
                html_path = recorder.run_dir / "notebooks" / "task_0000.html"
                html_path.write_text("<html>report</html>", encoding="utf-8")
                return subprocess.CompletedProcess(
                    args=argv, returncode=0, stdout="render ok\n", stderr=""
                )
            raise AssertionError(command)

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)
        result = evaluator.evaluate(expr)
        manifest = load_manifest(recorder.run_dir)

        assert result == Path(recorder.run_dir / "notebooks" / "task_0000.html")
        assert len(calls) == 4
        assert "import ipykernel" in calls[0]
        assert "ipykernel install" in calls[1]
        assert "JUPYTER_PATH=" in calls[2]
        assert " -k ginkgo-" in calls[2]
        assert manifest["tasks"]["task_0000"]["task_type"] == "notebook"
        assert manifest["tasks"]["task_0000"]["render_status"] == "succeeded"
        assert manifest["tasks"]["task_0000"]["rendered_html"] == "notebooks/task_0000.html"
        assert manifest["tasks"]["task_0000"]["managed_kernel_name"].startswith("ginkgo-")

        # Re-evaluating with the same notebook hits cache.
        cached = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(
            cached._shell_runner,
            "_run_subprocess",
            lambda **_: (_ for _ in ()).throw(AssertionError("cache miss")),
        )
        assert cached.evaluate(expr) == Path(recorder.run_dir / "notebooks" / "task_0000.html")

    def test_cached_notebook_with_declared_output_replays_rendered_html(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cache hits on notebook tasks with declared outputs must still
        populate ``rendered_html`` so the run summary can list the notebook.
        """
        nb_path = tmp_path / "report.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        output_path = tmp_path / "output.txt"
        expr = notebook_ipynb_with_output_task(
            notebook_path=str(nb_path), output_path=str(output_path)
        )

        first_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )

        def fake_run_subprocess(
            *,
            argv: str | list[str],
            use_shell: bool,
            on_stdout: Any = None,
            on_stderr: Any = None,
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "ipykernel install" in command:
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "papermill" in command:
                executed = first_recorder.run_dir / "notebooks" / "task_0000.ipynb"
                executed.write_text("executed", encoding="utf-8")
                # Honour the declared output by creating it during execution.
                output_path.write_text("declared output", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "nbconvert" in command:
                html = first_recorder.run_dir / "notebooks" / "task_0000.html"
                html.write_text("<html>report</html>", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            raise AssertionError(command)

        first_evaluator = _ConcurrentEvaluator(provenance=first_recorder, jobs=1, cores=1)
        monkeypatch.setattr(first_evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)
        first_result = first_evaluator.evaluate(expr)
        first_manifest = load_manifest(first_recorder.run_dir)
        first_html = first_recorder.run_dir / "notebooks" / "task_0000.html"

        assert first_result == output_path
        assert first_manifest["tasks"]["task_0000"]["rendered_html"] == "notebooks/task_0000.html"

        # Second run reuses the same cwd-scoped cache but a fresh recorder
        # (and therefore a fresh run_dir). The notebook body must be skipped
        # entirely yet rendered_html still has to surface in the new manifest.
        second_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py") + "-2",
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )
        cached_evaluator = _ConcurrentEvaluator(provenance=second_recorder, jobs=1, cores=1)
        monkeypatch.setattr(
            cached_evaluator._shell_runner,
            "_run_subprocess",
            lambda **_: (_ for _ in ()).throw(AssertionError("cache miss")),
        )

        cached_result = cached_evaluator.evaluate(expr)
        second_manifest = load_manifest(second_recorder.run_dir)
        replayed_html = second_manifest["tasks"]["task_0000"].get("rendered_html")

        assert cached_result == output_path
        assert second_manifest["tasks"]["task_0000"].get("cached") is True
        assert isinstance(replayed_html, str)
        # Absolute path pointing back at the original run's HTML artifact.
        assert Path(replayed_html) == first_html
        assert (second_recorder.run_dir / replayed_html).resolve() == first_html.resolve()

    def test_ipynb_notebook_task_uses_env_managed_kernel(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nb_path = tmp_path / "report.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        env_dir = tmp_path / "envs" / "test_env"
        env_dir.mkdir(parents=True)
        (env_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'test-env'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        expr = notebook_ipynb_env_task(notebook_path=str(nb_path), value=7)

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )
        registry = PixiRegistry(project_root=tmp_path)
        monkeypatch.setattr(registry, "prepare", lambda *, env: env_dir / "pixi.toml")
        monkeypatch.setattr(registry, "lock_hash", lambda *, env: "lock-hash-123")
        monkeypatch.setattr(
            registry,
            "exec_argv",
            lambda *, env, cmd: ["bash", "-c", cmd],
        )

        calls: list[str] = []

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            calls.append(command)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "ipykernel install" in command:
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "papermill" in command:
                output_path = recorder.run_dir / "notebooks" / "task_0000.ipynb"
                output_path.write_text("executed", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "nbconvert" in command:
                html_path = recorder.run_dir / "notebooks" / "task_0000.html"
                html_path.write_text("<html>report</html>", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            raise AssertionError(command)

        evaluator = _ConcurrentEvaluator(
            provenance=recorder,
            jobs=1,
            cores=1,
            backend=LocalBackend(pixi_registry=registry),
        )
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        result = evaluator.evaluate(expr)

        assert result == Path(recorder.run_dir / "notebooks" / "task_0000.html")
        assert any("python -c 'import ipykernel'" in call for call in calls)
        assert any("python -m ipykernel install" in call for call in calls)
        assert any("JUPYTER_PATH=" in call and "papermill" in call for call in calls)
        assert any("nbconvert" in call for call in calls)

    def test_ipynb_notebook_task_emits_kernel_install_notice(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nb_path = tmp_path / "report.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        expr = notebook_ipynb_task(notebook_path=str(nb_path), value=7)

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )
        events: list[object] = []
        bus = EventBus()
        bus.subscribe(events.append)

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "ipykernel install" in command:
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "papermill" in command:
                output_path = recorder.run_dir / "notebooks" / "task_0000.ipynb"
                output_path.write_text("executed", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "nbconvert" in command:
                html_path = recorder.run_dir / "notebooks" / "task_0000.html"
                html_path.write_text("<html>report</html>", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            raise AssertionError(command)

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1, event_bus=bus)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        result = evaluator.evaluate(expr)

        assert result == Path(recorder.run_dir / "notebooks" / "task_0000.html")
        notices = [event for event in events if isinstance(event, TaskNotice)]
        assert len(notices) == 1
        assert notices[0].message == "📦 Installing ipykernel for local..."

    def test_ipynb_notebook_task_fails_early_when_ipykernel_is_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nb_path = tmp_path / "report.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        expr = notebook_ipynb_task(notebook_path=str(nb_path), value=7)
        evaluator = _ConcurrentEvaluator(jobs=1, cores=1)

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(
                    args=argv,
                    returncode=1,
                    stdout="",
                    stderr="ModuleNotFoundError: No module named 'ipykernel'",
                )
            raise AssertionError(command)

        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        with pytest.raises(RuntimeError, match="ipykernel"):
            evaluator.evaluate(expr)

    def test_concurrent_ipynb_notebooks_install_kernel_once_per_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        env_dir = tmp_path / "envs" / "test_env"
        env_dir.mkdir(parents=True)
        (env_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'test-env'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        notebooks = []
        for index in range(2):
            nb_path = tmp_path / f"report_{index}.ipynb"
            nb_path.write_text(
                '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
                encoding="utf-8",
            )
            notebooks.append(notebook_ipynb_env_task(notebook_path=str(nb_path), value=index + 1))

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=2,
            cores=2,
        )
        registry = PixiRegistry(project_root=tmp_path)
        monkeypatch.setattr(registry, "prepare", lambda *, env: env_dir / "pixi.toml")
        monkeypatch.setattr(registry, "lock_hash", lambda *, env: "lock-hash-123")
        monkeypatch.setattr(registry, "exec_argv", lambda *, env, cmd: ["bash", "-c", cmd])

        install_calls = 0

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            nonlocal install_calls
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "ipykernel install" in command:
                install_calls += 1
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "papermill" in command:
                match = re.search(r"(/[^ ]+/notebooks/task_\d+\.ipynb)", command)
                assert match is not None
                output_path = Path(match.group(1))
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text("executed", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            if "nbconvert" in command:
                match = re.search(
                    r"--output-dir (/[^ ]+/notebooks) (/[^ ]+/notebooks/task_\d+\.ipynb)", command
                )
                assert match is not None
                html_dir = Path(match.group(1))
                executed_name = Path(match.group(2)).stem
                html_dir.mkdir(parents=True, exist_ok=True)
                (html_dir / f"{executed_name}.html").write_text(
                    "<html>report</html>", encoding="utf-8"
                )
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="", stderr="")
            raise AssertionError(command)

        evaluator = _ConcurrentEvaluator(
            provenance=recorder,
            jobs=2,
            cores=2,
            backend=LocalBackend(pixi_registry=registry),
        )
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)

        result = evaluator.evaluate(notebooks)

        assert len(result) == 2
        assert install_calls == 1

    def test_marimo_notebook_render_failure_writes_fallback_html(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nb_path = tmp_path / "explore.py"
        nb_path.write_text("print('marimo')\n", encoding="utf-8")
        expr = notebook_marimo_task(notebook_path=str(nb_path), sample_id="s1")

        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=tmp_path / "workflow.py"),
            workflow_path=tmp_path / "workflow.py",
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
        )

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            command = str(argv)
            if "export html" in command:
                return subprocess.CompletedProcess(
                    args=argv, returncode=2, stdout="", stderr="render blew up"
                )
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="run ok", stderr="")

        evaluator = _ConcurrentEvaluator(provenance=recorder, jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)
        result = evaluator.evaluate(expr)

        html_path = Path(result)
        manifest = load_manifest(recorder.run_dir)
        assert html_path.is_file()
        assert "HTML export failed" in html_path.read_text(encoding="utf-8")
        assert manifest["tasks"]["task_0000"]["render_status"] == "failed"
        assert manifest["tasks"]["task_0000"]["render_error"] == "render blew up"

    def test_script_task_runs_and_validates_outputs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        script_path = tmp_path / "fit.py"
        script_path.write_text("# placeholder script\n", encoding="utf-8")
        output_path = tmp_path / "out.txt"
        expr = python_script_task(script_path=str(script_path), output_path=str(output_path))

        def fake_run_subprocess(
            *, argv: str | list[str], use_shell: bool, on_stdout: Any = None, on_stderr: Any = None
        ) -> subprocess.CompletedProcess[str]:
            # Simulate the script creating its declared output.
            output_path.write_text("done\n", encoding="utf-8")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")

        evaluator = _ConcurrentEvaluator(jobs=1, cores=1)
        monkeypatch.setattr(evaluator._shell_runner, "_run_subprocess", fake_run_subprocess)
        result = evaluator.evaluate(expr)

        assert Path(result).is_file()

    def test_notebook_cache_invalidates_when_source_changes(self, tmp_path: Path) -> None:
        nb_path = tmp_path / "nb.ipynb"
        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        expr_v1 = notebook_ipynb_task(notebook_path=str(nb_path), value=1)

        def fake_subprocess_ok(
            *, argv: str | list[str], use_shell: bool
        ) -> subprocess.CompletedProcess[str]:
            command = " ".join(argv) if isinstance(argv, list) else str(argv)
            if "import ipykernel" in command:
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")
            if "ipykernel install" in command:
                kernel_root = tmp_path / ".ginkgo" / "jupyter" / "share" / "jupyter" / "kernels"
                kernel_name = command.split("--name", 1)[1].strip().split()[0]
                kernel_dir = kernel_root / kernel_name
                kernel_dir.mkdir(parents=True, exist_ok=True)
                (kernel_dir / "kernel.json").write_text("{}", encoding="utf-8")
                return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")
            if "papermill" in command:
                (tmp_path / "task_0000.ipynb").write_text("x", encoding="utf-8")
                # Write to the actual notebook artifacts dir.
                for p in [tmp_path / ".ginkgo" / "notebooks"]:
                    p.mkdir(parents=True, exist_ok=True)
                    (p / "task_0000.ipynb").write_text("x", encoding="utf-8")
            if "nbconvert" in command:
                for p in [tmp_path / ".ginkgo" / "notebooks"]:
                    (p / "task_0000.html").write_text("<html/>", encoding="utf-8")
            return subprocess.CompletedProcess(args=argv, returncode=0, stdout="ok", stderr="")

        ev1 = _ConcurrentEvaluator(jobs=1, cores=1)
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(ev1._shell_runner, "_run_subprocess", fake_subprocess_ok)
        try:
            ev1.evaluate(expr_v1)
        finally:
            monkeypatch.undo()

        # Modify notebook source — cache key must differ.
        nb_path.write_text(
            '{"cells": [{"source": "changed"}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
            encoding="utf-8",
        )
        # The cache keys should differ because the notebook source changed.
        from ginkgo.runtime.caching.cache import CacheStore
        from pathlib import Path as _Path

        cache = CacheStore(root=_Path(".ginkgo") / "cache")
        key_v1, _ = cache.build_cache_key(
            task_def=expr_v1.task_def,
            resolved_args={"notebook_path": str(nb_path), "value": 1},
        )
        # Recompute v1's key inline to compare — both keys are the same task_def
        # so the difference comes from the source hash stored in the sentinel.
        from ginkgo.core.notebook import notebook as make_notebook

        nb_path.write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        sentinel_v1 = make_notebook(str(nb_path))
        nb_path.write_text(
            '{"cells": [{"source": "changed"}], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}',
            encoding="utf-8",
        )
        sentinel_v2 = make_notebook(str(nb_path))
        assert sentinel_v1.source_hash != sentinel_v2.source_hash

    def test_local_task_fails_immediately_at_runtime(self):
        @task()
        def local_task(x: int) -> int:
            return x + 1

        with pytest.raises(TypeError, match="top-level function"):
            evaluate(local_task(x=1))

    def test_python_tasks_must_not_return_shell_payloads(self, tmp_path: Path) -> None:
        output = tmp_path / "payload.txt"

        with pytest.raises(TypeError, match="Use @task\\(kind='shell'\\)|appropriate task kind"):
            evaluate(python_returns_shell_task(output_path=str(output)))

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

    def test_pixi_environment_is_prepared_once_before_shell_fan_out(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        env_dir = tmp_path / "envs" / _PIXI_TEST_ENV
        env_dir.mkdir(parents=True)
        manifest = env_dir / "pixi.toml"
        manifest.write_text(
            "[workspace]\nname = 'race-env'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )

        install_calls: list[list[str]] = []
        real_subprocess_run = subprocess.run

        def fake_pixi_install(argv: list[str], **_: object) -> subprocess.CompletedProcess[str]:
            if argv[:2] != ["pixi", "install"]:
                return real_subprocess_run(
                    argv, check=False, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
            install_calls.append(argv)
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

        registry = PixiRegistry(project_root=tmp_path)
        monkeypatch.setattr("ginkgo.envs.pixi._require_pixi", lambda: None)
        monkeypatch.setattr("ginkgo.envs.pixi.subprocess.run", fake_pixi_install)
        monkeypatch.setattr(
            registry,
            "exec_argv",
            lambda *, env, cmd: ["bash", "-c", cmd],
        )

        outputs = [tmp_path / f"pixi-shell-{index}.txt" for index in range(3)]
        result = evaluate(
            [pixi_shell_output_task(output_path=str(path)) for path in outputs],
            pixi_registry=registry,
        )

        assert result == [file(str(path)) for path in outputs]
        assert install_calls == [["pixi", "install", "--manifest-path", str(manifest.resolve())]]
        for path in outputs:
            assert path.read_text(encoding="utf-8") == "payload"


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
        evaluator = _ConcurrentEvaluator()
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
        evaluator._shell_executor = shell_executor
        evaluator._python_executor = python_executor

        monkeypatch.setattr(
            evaluator._shell_runner,
            "_terminate_subprocess",
            lambda *, process: terminated.append(process.pid),
        )
        monkeypatch.setattr(
            evaluator,
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
        evaluator = _ConcurrentEvaluator()
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

        completed = evaluator._shell_runner._run_subprocess(argv=["echo", "hi"], use_shell=False)

        assert completed.returncode == 0
        assert completed.stdout == "stdout"
        assert completed.stderr == "stderr"
        assert evaluator._shell_runner._active_subprocesses == {}
        assert popen_calls[0][0] == ["echo", "hi"]

        if Path("/").anchor == "/":
            assert popen_calls[0][1]["start_new_session"] is True


class TestPixiWorkerPayload:
    def test_build_worker_payload_does_not_contain_import_roots(self, tmp_path: Path) -> None:
        evaluator = _ConcurrentEvaluator()
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
        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        expr = reveal_secret_task(
            secret_value=secret("API_TOKEN"),
            output_path="result.txt",
        )
        evaluator = _ConcurrentEvaluator(
            jobs=1,
            cores=1,
            provenance=recorder,
            secret_resolver=first_resolver,
        )

        first_result = evaluator.evaluate(expr)
        first_manifest = load_manifest(recorder.run_dir)
        first_task = next(iter(first_manifest["tasks"].values()))
        cache_key = first_task["cache_key"]
        meta_path = tmp_path / ".ginkgo" / "cache" / cache_key / "meta.json"

        assert Path(first_result).read_text(encoding="utf-8") == "first-token"
        assert first_task["inputs"]["secret_value"]["redacted"] is True
        assert "first-token" not in recorder.manifest_path.read_text(encoding="utf-8")
        assert "first-token" not in meta_path.read_text(encoding="utf-8")

        second_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        second_resolver = build_secret_resolver(
            project_root=tmp_path,
            config={},
            environ={"API_TOKEN": "rotated-token"},
        )
        second_evaluator = _ConcurrentEvaluator(
            jobs=1,
            cores=1,
            provenance=second_recorder,
            secret_resolver=second_resolver,
        )

        second_result = second_evaluator.evaluate(expr)
        second_manifest = load_manifest(second_recorder.run_dir)
        second_task = next(iter(second_manifest["tasks"].values()))

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
        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        evaluator = _ConcurrentEvaluator(
            jobs=1,
            cores=1,
            provenance=recorder,
            secret_resolver=build_secret_resolver(
                project_root=tmp_path,
                config={},
                environ={"API_TOKEN": "super-secret"},
            ),
        )

        with pytest.raises(RuntimeError, match="failure:\\[REDACTED\\]"):
            evaluator.evaluate(fail_with_secret_task(secret_value=secret("API_TOKEN")))

        manifest = load_manifest(recorder.run_dir)
        task = next(iter(manifest["tasks"].values()))
        stdout_log = recorder.run_dir / task["stdout_log"]
        stderr_log = recorder.run_dir / task["stderr_log"]

        assert "[REDACTED]" in stdout_log.read_text(encoding="utf-8")
        assert "super-secret" not in stdout_log.read_text(encoding="utf-8")
        assert "super-secret" not in stderr_log.read_text(encoding="utf-8")
        assert "super-secret" not in recorder.manifest_path.read_text(encoding="utf-8")


class TestAssets:
    def test_python_asset_result_registers_catalog_and_lineage(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=recorder)

        result = evaluator.evaluate(
            transform_asset_task(
                source=write_asset_task(output_path="prepared.txt", text="hello"),
                output_path="transformed.txt",
            )
        )

        assert isinstance(result, AssetRef)
        assert Path(result.artifact_path).read_text(encoding="utf-8") == "HELLO"
        assert (tmp_path / "prepared.txt").is_file()
        assert not (tmp_path / "prepared.txt").is_symlink()
        assert (tmp_path / "transformed.txt").is_file()
        assert not (tmp_path / "transformed.txt").is_symlink()

        manifest = load_manifest(recorder.run_dir)
        tasks = list(manifest["tasks"].values())
        assert tasks[0]["assets"][0]["asset_key"] == "file:prepared_data"
        assert tasks[1]["assets"][0]["asset_key"] == "file:transformed_data"

        store = AssetStore(root=tmp_path / ".ginkgo" / "assets")
        lineage = store.lineage_for(key=result.key, version_id=result.version_id)
        assert lineage is not None
        assert len(lineage.parents) == 1
        assert lineage.parents[0].name == "prepared_data"

    def test_asset_ref_cache_identity_drives_downstream_cache(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        expr = read_asset_task(
            source=write_asset_task(output_path="prepared.txt", text="hello"),
            log_path="consumer.log",
        )

        first_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        first_evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=first_recorder)
        assert first_evaluator.evaluate(expr) == "hello"

        second_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        second_evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=second_recorder)
        assert second_evaluator.evaluate(expr) == "hello"

        manifest = load_manifest(second_recorder.run_dir)
        statuses = [task["status"] for task in manifest["tasks"].values()]
        assert statuses == ["cached", "cached"]

    def test_shell_task_can_register_asset_output(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)

        result = evaluate(shell_asset_task(output_path="shell.txt"))

        assert isinstance(result, AssetRef)
        assert Path(result.artifact_path).read_text(encoding="utf-8") == "shell-payload"

    def test_shell_rerun_leaves_writable_output_materialization(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")

        first_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        first_evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=first_recorder)
        first_result = first_evaluator.evaluate(
            shell_asset_with_payload_task(output_path="shell.txt", payload="first")
        )

        assert isinstance(first_result, AssetRef)
        assert (tmp_path / "shell.txt").is_file()
        assert not (tmp_path / "shell.txt").is_symlink()

        second_recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        second_evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=second_recorder)
        second_result = second_evaluator.evaluate(
            shell_asset_with_payload_task(output_path="shell.txt", payload="second")
        )

        assert isinstance(second_result, AssetRef)
        assert Path(second_result.artifact_path).read_text(encoding="utf-8") == "second"
        assert (tmp_path / "shell.txt").is_file()
        assert not (tmp_path / "shell.txt").is_symlink()

    def test_nested_asset_refs_are_serialized_in_cache_metadata(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.chdir(tmp_path)
        workflow_path = tmp_path / "workflow.py"
        workflow_path.write_text("# placeholder\n", encoding="utf-8")
        recorder = RunProvenanceRecorder(
            run_id=make_run_id(workflow_path=workflow_path),
            workflow_path=workflow_path,
            root_dir=tmp_path / ".ginkgo" / "runs",
            jobs=1,
            cores=1,
            memory=None,
            params={},
        )
        evaluator = _ConcurrentEvaluator(jobs=1, cores=1, provenance=recorder)

        result = evaluator.evaluate(
            nested_asset_inputs_task(
                sources=[write_asset_task(output_path="prepared.txt", text="hello")],
                output_path="asset_versions.txt",
            )
        )

        manifest = load_manifest(recorder.run_dir)
        consumer_task = next(
            task
            for task in manifest["tasks"].values()
            if str(task["task"]).endswith(".nested_asset_inputs_task")
        )
        meta_path = tmp_path / ".ginkgo" / "cache" / consumer_task["cache_key"] / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))

        assert result
        assert meta["inputs"]["sources"]["type"] == "list"
        first_item = meta["inputs"]["sources"]["items"][0]
        assert first_item["type"] == "asset_ref"
        assert first_item["asset"] == "file:prepared_data"


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
