"""Executor reporting in the Rich run header."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

from rich.console import Console

from ginkgo.cli.renderers.models import CliRunSummary
from ginkgo.cli.renderers.run import CliRunRenderer


def _header(tmp_path: Path, **summary_kwargs) -> str:
    console = Console(file=StringIO(), width=200, force_terminal=False)
    summary = CliRunSummary(
        run_id="r1",
        mode="default",
        run_dir=tmp_path,
        cores=4,
        **summary_kwargs,
    )
    renderer = CliRunRenderer(console=console, summary=summary)
    renderer.start(planned_tasks=[(0, "mod.task_a", "", "")])
    return renderer._layout.render_resource_info_line().plain


def test_local_run_reports_cores(tmp_path: Path) -> None:
    assert "Running locally on 4 Cores" in _header(tmp_path)


def test_default_executor_names_the_backend(tmp_path: Path) -> None:
    assert "Running on gpu-k8s (Kubernetes)" in _header(
        tmp_path, executor_label="gpu-k8s (Kubernetes)"
    )


def test_pinned_executors_show_even_on_a_local_run(tmp_path: Path) -> None:
    # A run defaulting to local still dispatches executor= tasks remotely;
    # the header must not claim everything stayed on this machine.
    header = _header(tmp_path, pinned_executors=("cheap-batch", "gpu-k8s"))
    assert "Running locally on 4 Cores" in header
    assert "→ cheap-batch, gpu-k8s" in header


def test_pinned_executors_show_alongside_a_default(tmp_path: Path) -> None:
    header = _header(
        tmp_path,
        executor_label="Kubernetes",
        pinned_executors=("cheap-batch",),
    )
    assert "Running on Kubernetes" in header
    assert "→ cheap-batch" in header
