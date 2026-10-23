"""The longer example workflows run end to end, then rerun from cache."""

from __future__ import annotations

import importlib.util
import os
import shutil
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pandas as pd
import pytest

from ginkgo.cli.commands.init import write_starter_project
from ginkgo.cli.commands.run import run_workflow
from ginkgo.cli.workspace import discover_default_workflow
from ginkgo.core.shell import shell_argv
from ginkgo.envs.container import ContainerBackend
from ginkgo.plugins.registry import PluginRegistry, use_plugin_registry
from ginkgo.runtime.task_runners.shell import ShellRunner
from tests.conftest import latest_run_view

EXAMPLES_ROOT = Path(__file__).resolve().parents[2] / "examples"


@dataclass(frozen=True)
class Example:
    """What one example must produce, and how its rerun must behave."""

    name: str
    task_count: int | None = None
    plugins: tuple[str, ...] = ()
    globs: dict[str, int] = field(default_factory=dict)
    files: tuple[str, ...] = ()
    phrases: dict[str, str] = field(default_factory=dict)
    columns: dict[str, tuple[str, set[str]]] = field(default_factory=dict)
    # (task-name suffix, number of dynamic children) for a planner that expands.
    planner: tuple[str, int] | None = None
    # Task-name suffix of a notebook task whose page must be rendered.
    notebook: str | None = None
    # Task-name suffix of a task whose file outputs must be stored artifacts.
    stored: str | None = None
    docker: bool = False
    reruns: int = 1
    all_cached: bool = True


EXAMPLES = [
    Example(
        name="init",
        task_count=15,
        plugins=("ginkgo_pixi", "ginkgo_notebook"),
        files=("results/summary.json", "results/delivery_manifest.md"),
        notebook=".render_overview_notebook",
        stored=".write_seed_card",
        docker=True,
        # The second run replays some tasks; only the third is wholly cached.
        reruns=2,
    ),
    Example(
        name="bioinfo",
        plugins=("ginkgo_pixi", "ginkgo_oci"),
        globs={
            "results/filtered/*.filtered.fastq.gz": 4,
            "results/qc/*.stats.tsv": 2,
            "results/read_counts/*.counts.tsv": 2,
        },
        columns={"results/summary.csv": ("sample_id", {"ERR3058522", "ERR3058532"})},
        stored=".filter_fastq",
        docker=True,
    ),
    Example(
        name="chem",
        task_count=11,
        globs={"results/series/*_packet.md": 4},
        files=("results/delivery_manifest.txt",),
        phrases={"results/portfolio_summary.md": "Series Packets"},
        planner=(".plan_series_packets", 4),
    ),
    Example(
        name="retail",
        task_count=14,
        plugins=("ginkgo_notebook",),
        globs={"results/regions/*.csv": 4},
        files=("results/delivery_manifest.txt",),
        phrases={"results/executive_report.md": "Top region:"},
        notebook=".render_channel_performance_notebook",
    ),
    Example(
        name="news",
        globs={"results/desk_packets/*_packet.md": 5},
        files=("results/delivery_manifest.txt",),
        phrases={"results/newsroom_digest.md": "Desk Packets"},
        planner=(".plan_desk_packets", 5),
        all_cached=False,
    ),
    Example(
        name="supplychain",
        task_count=19,
        globs={"results/scenarios/*.json": 10},
        files=("results/artifact_manifest.txt",),
        phrases={"results/operations_brief.md": "Scenario Scorecard"},
        columns={
            "results/resilience_scorecard.csv": (
                "scenario_id",
                {
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
                },
            )
        },
    ),
    Example(
        name="ml",
        task_count=13,
        files=("results/delivery_manifest.txt",),
        phrases={"results/model_card.md": "Champion Model Card"},
        columns={
            "results/candidate_scorecard.csv": (
                "model_name",
                {"baseline_logit", "expansion_focus", "precision_guard", "retention_boost"},
            )
        },
    ),
]


@contextmanager
def _host_docker() -> Iterator[None]:
    """Run container shell tasks on the host, as the container would have run them."""
    original = ShellRunner.run_subprocess

    def run_subprocess(self: Any, *, argv: list[str], **kwargs: Any):
        if argv[0] == "docker":
            argv = shell_argv(shell="bash", cmd=argv[-1])
        return original(
            self, argv=argv, on_stdout=kwargs.get("on_stdout"), on_stderr=kwargs.get("on_stderr")
        )

    with (
        patch.object(ShellRunner, "run_subprocess", run_subprocess),
        patch("ginkgo.envs.container.shutil.which", return_value="/usr/bin/docker"),
        patch.object(ContainerBackend, "_image_exists_locally", return_value=True),
        patch.object(ContainerBackend, "_resolve_digest", return_value="sha256:fake"),
    ):
        yield


def _run(example: Example, example_dir: Path) -> tuple[Path, dict[str, Any]]:
    """Run the example's default workflow and return its newest run's projections."""
    with _host_docker() if example.docker else nullcontext():
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


def _task(manifest: dict[str, Any], suffix: str) -> dict[str, Any]:
    return next(t for t in manifest["tasks"].values() if str(t["name"]).endswith(suffix))


@pytest.mark.integration
@pytest.mark.parametrize("example", EXAMPLES, ids=lambda example: example.name)
def test_an_example_runs_and_its_rerun_is_cached(
    example: Example, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = [name for name in example.plugins if importlib.util.find_spec(name) is None]
    if missing:
        pytest.skip(f"the {example.name} example needs {', '.join(missing)}")
    if example.name == "bioinfo" and not os.environ.get("GINKGO_REMOTE_OCI_PROFILE"):
        pytest.skip("Set GINKGO_REMOTE_OCI_PROFILE to run the OCI bioinfo example")

    example_dir = tmp_path / example.name
    if example.name == "init":
        # The scaffold `ginkgo init` writes, so the test cannot drift from it.
        write_starter_project(root=example_dir)
    else:
        shutil.copytree(
            EXAMPLES_ROOT / example.name,
            example_dir,
            ignore=shutil.ignore_patterns(".ginkgo", "results", "logs", "__pycache__"),
        )
    monkeypatch.chdir(example_dir)
    registry = PluginRegistry.discover() if example.plugins else None

    with use_plugin_registry(registry) if registry else nullcontext():
        run_dir, manifest = _run(example, example_dir)

        assert manifest["status"] == "succeeded"
        if example.task_count is not None:
            assert len(manifest["tasks"]) == example.task_count
        for pattern, count in example.globs.items():
            assert len(list(example_dir.glob(pattern))) == count, pattern
        for name in example.files:
            assert (example_dir / name).is_file(), name
        for name, phrase in example.phrases.items():
            assert phrase in (example_dir / name).read_text(encoding="utf-8")
        for name, (column, values) in example.columns.items():
            assert set(pd.read_csv(example_dir / name)[column]) == values
        if example.planner:
            suffix, children = example.planner
            assert len(_task(manifest, suffix)["dynamic_dependency_ids"]) == children
        if example.notebook:
            notebook = _task(manifest, example.notebook)
            assert notebook["kind"] == "notebook"
            assert (run_dir / str(notebook["rendered_html"])).is_file()
        if example.stored:
            outputs = [
                output
                for task in manifest["tasks"].values()
                if str(task["name"]).endswith(example.stored)
                for output in task.get("outputs", [])
            ]
            assert outputs and all(output.get("artifact_id") for output in outputs)

        for _ in range(example.reruns):
            _, rerun = _run(example, example_dir)

    assert rerun["status"] == "succeeded"
    statuses = [task["status"] == "cached" for task in rerun["tasks"].values()]
    assert all(statuses) if example.all_cached else any(statuses)
