"""Integration tests for the longer example workflows."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pandas as pd
import pytest

from ginkgo.cli.workspace import discover_default_workflow
from ginkgo.cli.commands.run import run_workflow
from ginkgo.envs.container import ContainerBackend
from ginkgo.runtime.evaluator import _ConcurrentEvaluator
from ginkgo.runtime.provenance import latest_run_dir, load_manifest


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = REPO_ROOT / "examples"


def _copy_example(*, name: str, destination_root: Path) -> Path:
    """Copy an example workflow into the isolated test workspace."""
    source = EXAMPLES_ROOT / name
    destination = destination_root / name
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            ".ginkgo",
            "results",
            "logs",
            "__pycache__",
            ".pytest_cache",
        ),
    )
    return destination


def _run_example(*, example_dir: Path) -> tuple[Path, dict[str, object]]:
    """Execute an example workflow and load its newest run manifest."""
    exit_code = run_workflow(
        workflow_path=discover_default_workflow(project_root=example_dir),
        config_paths=[],
        jobs=4,
        cores=4,
        memory=None,
        dry_run=False,
    )
    assert exit_code == 0

    runs_root = example_dir / ".ginkgo" / "runs"
    run_dir = latest_run_dir(runs_root)
    assert run_dir is not None
    return run_dir, load_manifest(run_dir)


@contextmanager
def _mock_docker() -> Iterator[None]:
    """Mock Docker runtime so container shell tasks execute locally.

    Docker argv is intercepted at the evaluator's ``_run_subprocess`` level:
    the shell command is extracted and executed directly via ``bash -c``,
    bypassing the container runtime while producing real file outputs.
    """
    original_run_subprocess = _ConcurrentEvaluator._run_subprocess

    def _patched_run_subprocess(
        self_eval: Any,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        # Docker argv: ["docker", "run", ..., "bash", "-c", "<cmd>"]
        if isinstance(argv, list) and argv and argv[0] == "docker":
            cmd = argv[-1]
            completed = subprocess.run(
                cmd,
                shell=True,
                text=True,
                capture_output=True,
            )
            if on_stdout is not None and completed.stdout:
                on_stdout(completed.stdout)
            if on_stderr is not None and completed.stderr:
                on_stderr(completed.stderr)
            return subprocess.CompletedProcess(
                args=argv,
                returncode=completed.returncode,
                stdout=completed.stdout or "",
                stderr=completed.stderr or "",
            )
        return original_run_subprocess(
            self_eval,
            argv=argv,
            use_shell=use_shell,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    with (
        patch.object(_ConcurrentEvaluator, "_run_subprocess", _patched_run_subprocess),
        patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
        patch.object(ContainerBackend, "_image_exists_locally", return_value=True),
        patch.object(ContainerBackend, "_resolve_digest", return_value="sha256:fake_test_digest"),
    ):
        yield


@contextmanager
def _mock_notebook_tools() -> Iterator[None]:
    """Mock notebook tooling so example workflows do not need real installs."""
    original_run_subprocess = _ConcurrentEvaluator._run_subprocess

    def _patched_run_subprocess(
        self_eval: Any,
        *,
        argv: str | list[str],
        use_shell: bool,
        on_stdout: Any = None,
        on_stderr: Any = None,
    ) -> subprocess.CompletedProcess[str]:
        if isinstance(argv, str) and "papermill" in argv:
            parts = shlex.split(argv)
            output_path = Path(parts[4])
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("executed notebook", encoding="utf-8")
            if on_stdout is not None:
                on_stdout("papermill ok\n")
            return subprocess.CompletedProcess(
                args=argv,
                returncode=0,
                stdout="papermill ok\n",
                stderr="",
            )

        if isinstance(argv, str) and "nbconvert" in argv:
            parts = shlex.split(argv)
            output_stem = parts[parts.index("--output") + 1]
            output_dir = Path(parts[parts.index("--output-dir") + 1])
            html_path = output_dir / f"{output_stem}.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text("<html><body>notebook report</body></html>", encoding="utf-8")
            if on_stdout is not None:
                on_stdout("nbconvert ok\n")
            return subprocess.CompletedProcess(
                args=argv,
                returncode=0,
                stdout="nbconvert ok\n",
                stderr="",
            )

        return original_run_subprocess(
            self_eval,
            argv=argv,
            use_shell=use_shell,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    with patch.object(_ConcurrentEvaluator, "_run_subprocess", _patched_run_subprocess):
        yield


@pytest.mark.integration
class TestExamples:
    def test_init_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="init", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        with _mock_docker(), _mock_notebook_tools():
            first_run_dir, first_manifest = _run_example(example_dir=example_dir)

        notebook_task = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".render_overview_notebook")
        )
        notebook_html = first_run_dir / str(notebook_task["rendered_html"])

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 15
        assert notebook_task["task_type"] == "notebook"
        assert notebook_task["render_status"] == "succeeded"
        assert notebook_html.is_file()
        assert (example_dir / "results" / "summary.json").is_file()
        assert (example_dir / "results" / "delivery_manifest.md").is_file()
        assert any(
            task.get("assets")
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".write_seed_card")
        )

        with _mock_docker(), _mock_notebook_tools():
            _, second_manifest = _run_example(example_dir=example_dir)
        with _mock_docker(), _mock_notebook_tools():
            _, third_manifest = _run_example(example_dir=example_dir)

        assert second_manifest["status"] == "succeeded"
        assert any(task["status"] == "cached" for task in second_manifest["tasks"].values())
        assert third_manifest["status"] == "succeeded"
        assert all(task["status"] == "cached" for task in third_manifest["tasks"].values())

    def test_bioinfo_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        profile = os.environ.get("GINKGO_REMOTE_OCI_PROFILE")
        if not profile:
            pytest.skip("Set GINKGO_REMOTE_OCI_PROFILE to run the OCI bioinfo example")

        example_dir = _copy_example(name="bioinfo", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        with _mock_docker():
            _, first_manifest = _run_example(example_dir=example_dir)

        filtered_fastqs = sorted(
            (example_dir / "results" / "filtered").glob("*.filtered.fastq.gz")
        )
        qc_tables = sorted((example_dir / "results" / "qc").glob("*.stats.tsv"))
        count_files = sorted((example_dir / "results" / "read_counts").glob("*.counts.tsv"))
        summary = pd.read_csv(example_dir / "results" / "summary.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(filtered_fastqs) == 4
        assert len(qc_tables) == 2
        assert len(count_files) == 2
        assert sorted(summary["sample_id"].unique().tolist()) == ["ERR3058522", "ERR3058532"]
        assert len(summary) == 4
        assert "read_count_r1" in summary.columns
        assert any(
            task.get("assets")
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".filter_fastq")
        )

        with _mock_docker():
            _, second_manifest = _run_example(example_dir=example_dir)

        assert second_manifest["status"] == "succeeded"
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())

    def test_chem_example_expands_series_packets(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="chem", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)

        packet_outputs = sorted((example_dir / "results" / "series").glob("*_packet.md"))
        packet_names = [path.stem for path in packet_outputs]
        packet_planner = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".plan_series_packets")
        )
        register = pd.read_csv(example_dir / "results" / "candidate_register.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 11
        assert len(packet_outputs) == 4
        assert packet_planner["status"] == "succeeded"
        assert len(packet_planner["dynamic_dependency_ids"]) == 4
        assert "benzimidazole_packet" in packet_names
        assert "indazole_packet" in packet_names
        assert "advance" in register["advance_recommendation"].tolist()
        assert (example_dir / "results" / "delivery_manifest.txt").is_file()
        assert "Series Packets" in (example_dir / "results" / "portfolio_summary.md").read_text(
            encoding="utf-8"
        )

        _, second_manifest = _run_example(example_dir=example_dir)
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())

    def test_retail_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="retail", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        with _mock_notebook_tools():
            first_run_dir, first_manifest = _run_example(example_dir=example_dir)
        region_outputs = sorted((example_dir / "results" / "regions").glob("*.csv"))
        notebook_task = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".render_channel_performance_notebook")
        )
        notebook_html = first_run_dir / str(notebook_task["rendered_html"])

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 14
        assert len(region_outputs) == 4
        assert notebook_task["task_type"] == "notebook"
        assert notebook_task["render_status"] == "succeeded"
        assert notebook_html.is_file()
        assert (example_dir / "results" / "delivery_manifest.txt").is_file()
        assert "Top region:" in (example_dir / "results" / "executive_report.md").read_text(
            encoding="utf-8"
        )

        with _mock_notebook_tools():
            second_run_dir, second_manifest = _run_example(example_dir=example_dir)
        assert second_run_dir is not None
        assert second_manifest["status"] == "succeeded"
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())

    def test_newsroom_example_expands_dynamic_packets(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="news", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)

        packet_outputs = sorted((example_dir / "results" / "desk_packets").glob("*_packet.md"))
        packet_names = [path.stem for path in packet_outputs]
        packet_planner = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["task"]).endswith(".plan_desk_packets")
        )

        assert len(packet_outputs) == 5
        assert packet_planner["status"] == "succeeded"
        assert len(packet_planner["dynamic_dependency_ids"]) == 5
        assert "business_packet" in packet_names
        assert "politics_packet" in packet_names
        assert "Desk Packets" in (example_dir / "results" / "newsroom_digest.md").read_text(
            encoding="utf-8"
        )

        _, second_manifest = _run_example(example_dir=example_dir)
        assert any(task["status"] == "cached" for task in second_manifest["tasks"].values())
        assert (example_dir / "results" / "delivery_manifest.txt").is_file()

    def test_supplychain_example_runs_scenarios_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="supplychain", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)
        scenario_outputs = sorted((example_dir / "results" / "scenarios").glob("*.json"))
        scorecard = pd.read_csv(example_dir / "results" / "resilience_scorecard.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 13
        assert len(scenario_outputs) == 4
        assert sorted(scorecard["scenario_id"].tolist()) == [
            "fuel_spike",
            "port_delay",
            "supplier_outage",
            "weather_shock",
        ]
        assert (example_dir / "results" / "artifact_manifest.txt").is_file()
        assert "Scenario Scorecard" in (example_dir / "results" / "operations_brief.md").read_text(
            encoding="utf-8"
        )

        _, second_manifest = _run_example(example_dir=example_dir)
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())

    def test_ml_example_runs_candidate_fanout_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="ml", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)
        candidate_outputs = sorted((example_dir / "results" / "candidates").glob("*.json"))
        scorecard = pd.read_csv(example_dir / "results" / "candidate_scorecard.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 13
        assert len(candidate_outputs) == 4
        assert sorted(scorecard["model_name"].tolist()) == [
            "baseline_logit",
            "expansion_focus",
            "precision_guard",
            "retention_boost",
        ]
        assert (example_dir / "results" / "delivery_manifest.txt").is_file()
        assert "Champion Model Card" in (example_dir / "results" / "model_card.md").read_text(
            encoding="utf-8"
        )

        _, second_manifest = _run_example(example_dir=example_dir)
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())
