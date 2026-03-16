"""Integration tests for the longer example workflows."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from ginkgo.cli.workspace import discover_default_workflow
from ginkgo.cli.commands.run import run_workflow
from ginkgo.runtime.provenance import latest_run_dir, load_manifest


REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_ROOT = REPO_ROOT / "examples"


def _copy_example(*, name: str, destination_root: Path) -> Path:
    """Copy an example workflow into the isolated test workspace."""
    source = EXAMPLES_ROOT / name
    destination = destination_root / name
    shutil.copytree(source, destination)
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


class TestExamples:
    def test_chemistry_screening_example_expands_series_packets(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="chemistry_screening", destination_root=tmp_path)
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

    def test_retail_analytics_example_runs_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="retail_analytics", destination_root=tmp_path)
        monkeypatch.chdir(example_dir)

        first_run_dir, first_manifest = _run_example(example_dir=example_dir)
        region_outputs = sorted((example_dir / "results" / "regions").glob("*.csv"))

        assert first_manifest["status"] == "succeeded"
        assert len(first_manifest["tasks"]) == 13
        assert len(region_outputs) == 4
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
        example_dir = _copy_example(name="newsroom", destination_root=tmp_path)
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

    def test_supply_chain_example_runs_scenarios_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="supply_chain", destination_root=tmp_path)
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

    def test_ml_model_ops_example_runs_candidate_fanout_and_caches(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        example_dir = _copy_example(name="ml_model_ops", destination_root=tmp_path)
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
