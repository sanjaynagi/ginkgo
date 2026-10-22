"""Integration tests for the longer example workflows."""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pandas as pd
import pytest

from ginkgo.cli.commands.init import write_starter_project
from ginkgo.cli.workspace import discover_default_workflow
from ginkgo.cli.commands.run import run_workflow
from ginkgo.core.shell import shell_argv
from ginkgo.envs.container import ContainerBackend
from ginkgo.plugins.registry import PluginRegistry, use_plugin_registry
from tests.conftest import latest_run_view


REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES_ROOT = REPO_ROOT / "examples"


def _materialize_example(*, name: str, destination_root: Path) -> Path:
    """Place one example workflow in the isolated test workspace.

    ``init`` is not a checked-in example: it is the starter project the packaged
    templates scaffold, materialised here from that single source so it cannot
    drift from what ``ginkgo init`` gives users. Every other name is copied from
    ``examples/``.
    """
    destination = destination_root / name
    if name == "init":
        return write_starter_project(root=destination)

    source = EXAMPLES_ROOT / name
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
    """Execute an example workflow and return its newest run's projections."""
    exit_code = run_workflow(
        workflow_path=discover_default_workflow(project_root=example_dir),
        config_paths=[],
        jobs=4,
        cores=4,
        memory=None,
        dry_run=False,
    )
    assert exit_code == 0

    return latest_run_view(root=example_dir)


@contextmanager
def _mock_docker() -> Iterator[None]:
    """Mock Docker runtime so container shell tasks execute locally.

    Docker argv is intercepted at the shell runner's ``run_subprocess`` level:
    the shell command is extracted and executed directly under strict bash,
    bypassing the container runtime while producing real file outputs.
    """
    from ginkgo.runtime.task_runners.shell import ShellRunner

    original_run_subprocess = ShellRunner.run_subprocess

    def _patched_run_subprocess(
        self_runner: Any,
        *,
        argv: list[str],
        on_stdout: Any = None,
        on_stderr: Any = None,
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        # Run the command the container would have run on the host instead.
        if argv[0] == "docker":
            argv = shell_argv(shell="bash", cmd=argv[-1])
        return original_run_subprocess(
            self_runner,
            argv=argv,
            on_stdout=on_stdout,
            on_stderr=on_stderr,
        )

    with (
        patch.object(ShellRunner, "run_subprocess", _patched_run_subprocess),
        patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
        patch.object(ContainerBackend, "_image_exists_locally", return_value=True),
        patch.object(ContainerBackend, "_resolve_digest", return_value="sha256:fake_test_digest"),
    ):
        yield


@pytest.fixture
def installed_plugins() -> Iterator[PluginRegistry]:
    """Run against the installed plugins rather than core's test stubs."""
    with use_plugin_registry(PluginRegistry.discover()) as registry:
        yield registry


@pytest.mark.integration
class TestExamples:
    @pytest.mark.skipif(
        importlib.util.find_spec("ginkgo_pixi") is None
        or importlib.util.find_spec("ginkgo_notebook") is None,
        reason="the init scaffold needs the ginkgo-pixi and ginkgo-notebook plugins",
    )
    def test_init_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        installed_plugins: PluginRegistry,
    ) -> None:
        example_dir = _materialize_example(name="init", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        with _mock_docker():
            first_run_dir, first_manifest = _run_example(example_dir=example_dir)

        notebook_task = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".render_overview_notebook")
        )
        notebook_html = first_run_dir / str(notebook_task["rendered_html"])

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 15
        assert notebook_task["kind"] == "notebook"
        assert notebook_html.is_file()
        assert (example_dir / "results" / "summary.json").is_file()
        assert (example_dir / "results" / "delivery_manifest.md").is_file()
        seed_card_outputs = [
            output
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".write_seed_card")
            for output in task.get("outputs", [])
        ]
        assert seed_card_outputs
        assert all(output.get("artifact_id") for output in seed_card_outputs)

        with _mock_docker():
            _, second_manifest = _run_example(example_dir=example_dir)
        with _mock_docker():
            _, third_manifest = _run_example(example_dir=example_dir)

        assert second_manifest["status"] == "succeeded"
        assert any(task["status"] == "cached" for task in second_manifest["tasks"].values())
        assert third_manifest["status"] == "succeeded"
        assert all(task["status"] == "cached" for task in third_manifest["tasks"].values())

    @pytest.mark.skipif(
        importlib.util.find_spec("ginkgo_pixi") is None
        or importlib.util.find_spec("ginkgo_oci") is None,
        reason="the bioinfo example needs the ginkgo-pixi and ginkgo-oci plugins",
    )
    def test_bioinfo_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        installed_plugins: PluginRegistry,
    ) -> None:
        profile = os.environ.get("GINKGO_REMOTE_OCI_PROFILE")
        if not profile:
            pytest.skip("Set GINKGO_REMOTE_OCI_PROFILE to run the OCI bioinfo example")

        example_dir = _materialize_example(name="bioinfo", destination_root=tmp_path)
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
        assert all(
            output.get("artifact_id")
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".filter_fastq")
            for output in task.get("outputs", [])
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
        example_dir = _materialize_example(name="chem", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)

        packet_outputs = sorted((example_dir / "results" / "series").glob("*_packet.md"))
        packet_names = [path.stem for path in packet_outputs]
        packet_planner = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".plan_series_packets")
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

    @pytest.mark.skipif(
        importlib.util.find_spec("ginkgo_notebook") is None,
        reason="the retail example's notebook task needs the ginkgo-notebook plugin",
    )
    def test_retail_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        installed_plugins: PluginRegistry,
    ) -> None:
        example_dir = _materialize_example(name="retail", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        first_run_dir, first_manifest = _run_example(example_dir=example_dir)
        region_outputs = sorted((example_dir / "results" / "regions").glob("*.csv"))
        notebook_task = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".render_channel_performance_notebook")
        )
        notebook_html = first_run_dir / str(notebook_task["rendered_html"])

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 14
        assert len(region_outputs) == 4
        assert notebook_task["kind"] == "notebook"
        assert notebook_html.is_file()
        assert (example_dir / "results" / "delivery_manifest.txt").is_file()
        assert "Top region:" in (example_dir / "results" / "executive_report.md").read_text(
            encoding="utf-8"
        )

        second_run_dir, second_manifest = _run_example(example_dir=example_dir)
        assert second_run_dir is not None
        assert second_manifest["status"] == "succeeded"
        assert all(task["status"] == "cached" for task in second_manifest["tasks"].values())

    def test_newsroom_example_expands_dynamic_packets(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _materialize_example(name="news", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)

        packet_outputs = sorted((example_dir / "results" / "desk_packets").glob("*_packet.md"))
        packet_names = [path.stem for path in packet_outputs]
        packet_planner = next(
            task
            for task in first_manifest["tasks"].values()
            if str(task["name"]).endswith(".plan_desk_packets")
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
        example_dir = _materialize_example(name="supplychain", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)
        scenario_outputs = sorted((example_dir / "results" / "scenarios").glob("*.json"))
        scorecard = pd.read_csv(example_dir / "results" / "resilience_scorecard.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 19
        assert len(scenario_outputs) == 10
        assert sorted(scorecard["scenario_id"].tolist()) == [
            "customs_holdup",
            "demand_surge",
            "fuel_spike",
            "labour_strike",
            "peak_season",
            "port_delay",
            "rail_disruption",
            "raw_material_shortage",
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
        example_dir = _materialize_example(name="ml", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        _, first_manifest = _run_example(example_dir=example_dir)
        scorecard = pd.read_csv(example_dir / "results" / "candidate_scorecard.csv")

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 13

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
