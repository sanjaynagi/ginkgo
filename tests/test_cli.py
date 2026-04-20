"""Phase 6 CLI and provenance integration tests."""

from __future__ import annotations

from collections import Counter
import re
import subprocess
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import yaml
from rich.console import Console

from ginkgo.core.asset import AssetKey, make_asset_version
from ginkgo.runtime.artifacts.artifact_store import LocalArtifactStore
from ginkgo.runtime.artifacts.asset_store import AssetStore
from ginkgo.cli import (
    _core_unit_label,
    _environment_label,
    _time_of_day_spinner,
    _truncate_task_label,
)
from ginkgo.cli.renderers.common import _MultiStateBar


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = REPO_ROOT / ".pixi" / "envs" / "default" / "bin" / "python"


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PYTHON), "-m", "ginkgo.cli", *args],
        cwd=cwd,
        check=False,
        text=True,
        capture_output=True,
    )


def _extract_run_dir(output: str) -> Path:
    match = re.search(r"Run directory: (.+)", output)
    if match is None:
        raise AssertionError(f"Run directory not found in output:\n{output}")
    return Path(match.group(1).strip())


def _seed_asset(*, cwd: Path, name: str, text: str, run_id: str, alias: str | None = None) -> str:
    asset_store = AssetStore(root=cwd / ".ginkgo" / "assets")
    artifact_store = LocalArtifactStore(root=cwd / ".ginkgo" / "artifacts")
    source = cwd / f"{name}.txt"
    source.write_text(text, encoding="utf-8")
    record = artifact_store.store(src_path=source)
    version = make_asset_version(
        key=AssetKey(namespace="file", name=name),
        kind="file",
        artifact_id=record.artifact_id,
        content_hash=record.digest_hex,
        run_id=run_id,
        producer_task="tests.seed",
    )
    asset_store.register_version(version=version)
    if alias is not None:
        asset_store.set_alias(key=version.key, alias=alias, version_id=version.version_id)
    return version.version_id


class TestCliRunAndCache:
    def test_run_writes_manifest_params_and_cache_metadata(self) -> None:
        Path("ginkgo.toml").write_text('message = "default"\nextra = "base"\n', encoding="utf-8")
        Path("override-1.toml").write_text('message = "first"\n', encoding="utf-8")
        Path("override-2.toml").write_text(
            'message = "second"\nextra = "override"\n', encoding="utf-8"
        )
        Path("workflow.py").write_text(
            """
import ginkgo
from pathlib import Path
from ginkgo import flow, task

cfg = ginkgo.config("ginkgo.toml")

@task()
def write_message(message: str, output_path: str) -> str:
    Path(output_path).write_text(message, encoding="utf-8")
    return output_path

@flow
def main():
    return write_message(message=cfg["message"], output_path="result.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        first = _run_cli(
            "run",
            "workflow.py",
            "--config",
            "override-1.toml",
            "--config",
            "override-2.toml",
            cwd=Path.cwd(),
        )
        assert first.returncode == 0, first.stderr
        assert re.search(
            r"🌿 ginkgo run workflow\.py \([0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{8}\)",
            first.stdout,
        )
        assert "📦 Loading workflow...  done" in first.stdout
        assert "🌱 Building expression tree...  1 tasks" in first.stdout
        assert re.search(r"Running locally on \d+ Cores", first.stdout)
        assert "write_message" in first.stdout
        assert "Environment" in first.stdout
        assert "local" in first.stdout
        assert "Time" in first.stdout
        assert "✓ succeeded" in first.stdout
        assert "1/1 complete" in first.stdout
        assert "⏱ Completed in " in first.stdout
        assert "Run Summary" not in first.stdout
        assert '{"status":' not in first.stdout

        first_run_dir = _extract_run_dir(first.stdout)
        first_manifest = yaml.safe_load(
            (first_run_dir / "manifest.yaml").read_text(encoding="utf-8")
        )
        first_params = yaml.safe_load((first_run_dir / "params.yaml").read_text(encoding="utf-8"))
        first_task = next(iter(first_manifest["tasks"].values()))

        assert Path("result.txt").read_text(encoding="utf-8") == "second"
        assert first_manifest["status"] == "succeeded"
        assert first_task["status"] == "succeeded"
        assert first_task["cached"] is False
        assert first_task["inputs"]["message"] == "second"
        assert first_params == {"extra": "override", "message": "second"}

        second = _run_cli(
            "run",
            "workflow.py",
            "--config",
            "override-1.toml",
            "--config",
            "override-2.toml",
            cwd=Path.cwd(),
        )
        assert second.returncode == 0, second.stderr

        second_run_dir = _extract_run_dir(second.stdout)
        second_manifest = yaml.safe_load(
            (second_run_dir / "manifest.yaml").read_text(encoding="utf-8")
        )
        second_task = next(iter(second_manifest["tasks"].values()))

        assert "↺ cached" in second.stdout
        assert "1/1 complete" in second.stdout
        assert second_task["status"] == "cached"
        assert second_task["cached"] is True

        cache_key = second_task["cache_key"]
        listed = _run_cli("cache", "ls", cwd=Path.cwd())
        assert listed.returncode == 0
        assert "Cache Key" in listed.stdout
        assert "Task" in listed.stdout
        assert "Size" in listed.stdout
        assert "Age" in listed.stdout
        assert cache_key in listed.stdout.replace("\n", "")

        cleared = _run_cli("cache", "clear", cache_key, cwd=Path.cwd())
        assert cleared.returncode == 0
        assert "🌿 ginkgo cache clear" in cleared.stdout
        assert f"✓ Removed cache entry {cache_key}" in cleared.stdout
        assert not (Path(".ginkgo") / "cache" / cache_key).exists()

    def test_run_notebook_reports_managed_ipykernel_install(self) -> None:
        Path("report.ipynb").write_text(
            '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}', encoding="utf-8"
        )
        Path("workflow.py").write_text(
            """
from ginkgo import flow, notebook, task

@task("notebook")
def render_report() -> str:
    return notebook("report.ipynb")

@flow
def main():
    return render_report()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "📦 Installing ipykernel for local..." in result.stdout

    def test_cache_ls_empty_state_is_styled(self) -> None:
        result = _run_cli("cache", "ls", cwd=Path.cwd())
        assert result.returncode == 0
        assert "🌿 ginkgo cache ls" in result.stdout
        assert "No cache entries found." in result.stdout

    def test_run_manifest_records_retry_attempts(self) -> None:
        Path("retry_workflow.py").write_text(
            """
from pathlib import Path
from ginkgo import flow, task

@task(retries=2)
def flaky(marker_path: str) -> str:
    marker = Path(marker_path)
    failures = int(marker.read_text(encoding="utf-8")) if marker.exists() else 0
    if failures < 1:
        marker.write_text(str(failures + 1), encoding="utf-8")
        raise RuntimeError("transient failure")
    return "ok"

@flow
def main():
    return flaky(marker_path="retry.marker")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "retry_workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr

        run_dir = _extract_run_dir(result.stdout)
        manifest = yaml.safe_load((run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        task = next(iter(manifest["tasks"].values()))

        assert task["status"] == "succeeded"
        assert task["retries"] == 2
        assert task["max_attempts"] == 3
        assert task["attempt"] == 2
        assert task["attempts"] == 2

    def test_cache_prune_dry_run_reports_old_entries_without_deleting(self) -> None:
        cache_root = Path(".ginkgo") / "cache"
        old_entry = cache_root / "old123"
        fresh_entry = cache_root / "fresh456"
        old_entry.mkdir(parents=True)
        fresh_entry.mkdir(parents=True)
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(days=100)).isoformat()
        fresh_ts = (now - timedelta(days=5)).isoformat()
        (old_entry / "meta.json").write_text(
            f'{{"function":"demo.old","timestamp":"{old_ts}"}}',
            encoding="utf-8",
        )
        (fresh_entry / "meta.json").write_text(
            f'{{"function":"demo.fresh","timestamp":"{fresh_ts}"}}',
            encoding="utf-8",
        )
        (old_entry / "output.json").write_text("{}", encoding="utf-8")
        (fresh_entry / "output.json").write_text("{}", encoding="utf-8")

        result = _run_cli("cache", "prune", "--older-than", "30d", "--dry-run", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo cache prune" in result.stdout
        assert "would be removed" in result.stdout
        assert "old123" in result.stdout
        assert old_entry.exists()
        assert fresh_entry.exists()

    def test_cache_prune_removes_only_entries_older_than_cutoff(self) -> None:
        cache_root = Path(".ginkgo") / "cache"
        old_entry = cache_root / "old123"
        fresh_entry = cache_root / "fresh456"
        old_entry.mkdir(parents=True)
        fresh_entry.mkdir(parents=True)
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(days=100)).isoformat()
        fresh_ts = (now - timedelta(days=5)).isoformat()
        (old_entry / "meta.json").write_text(
            f'{{"function":"demo.old","timestamp":"{old_ts}"}}',
            encoding="utf-8",
        )
        (fresh_entry / "meta.json").write_text(
            f'{{"function":"demo.fresh","timestamp":"{fresh_ts}"}}',
            encoding="utf-8",
        )

        result = _run_cli("cache", "prune", "--older-than", "30d", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "Removed" in result.stdout
        assert not old_entry.exists()
        assert fresh_entry.exists()

    def test_cache_prune_rejects_invalid_duration(self) -> None:
        result = _run_cli("cache", "prune", "--older-than", "bad", cwd=Path.cwd())
        assert result.returncode == 1
        assert "Invalid duration for --older-than" in result.stderr

    def test_cache_prune_requires_at_least_one_policy(self) -> None:
        result = _run_cli("cache", "prune", cwd=Path.cwd())
        assert result.returncode != 0
        combined = result.stdout + result.stderr
        assert "--older-than" in combined

    def test_cache_prune_by_max_entries_removes_oldest(self) -> None:
        cache_root = Path(".ginkgo") / "cache"
        now = datetime.now(timezone.utc)
        entries = []
        for index, age_days in enumerate([100, 60, 30, 5]):
            entry = cache_root / f"entry{index}"
            entry.mkdir(parents=True)
            ts = (now - timedelta(days=age_days)).isoformat()
            (entry / "meta.json").write_text(
                f'{{"function":"demo.x","timestamp":"{ts}"}}',
                encoding="utf-8",
            )
            (entry / "output.json").write_text("{}", encoding="utf-8")
            entries.append(entry)

        result = _run_cli("cache", "prune", "--max-entries", "2", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert not entries[0].exists()
        assert not entries[1].exists()
        assert entries[2].exists()
        assert entries[3].exists()

    def test_cache_prune_by_max_size_removes_oldest_until_under_budget(self) -> None:
        cache_root = Path(".ginkgo") / "cache"
        now = datetime.now(timezone.utc)
        entries = []
        payload = "x" * 1024
        for index, age_days in enumerate([100, 60, 5]):
            entry = cache_root / f"sz{index}"
            entry.mkdir(parents=True)
            ts = (now - timedelta(days=age_days)).isoformat()
            (entry / "meta.json").write_text(
                f'{{"function":"demo.x","timestamp":"{ts}"}}',
                encoding="utf-8",
            )
            (entry / "output.json").write_text(payload, encoding="utf-8")
            entries.append(entry)

        # Each entry holds ~1KB; cap total at 3KB so only the oldest must drop.
        result = _run_cli("cache", "prune", "--max-size", "3KB", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert not entries[0].exists()
        assert entries[1].exists()
        assert entries[2].exists()

    def test_notebooks_lists_pairs_in_most_recent_run_order(self) -> None:
        older_run = Path(".ginkgo") / "runs" / "20260301_090000_000000_aaaaaaaa"
        newer_run = Path(".ginkgo") / "runs" / "20260302_090000_000000_bbbbbbbb"
        older_run.mkdir(parents=True)
        newer_run.mkdir(parents=True)

        older_notebook = older_run / "notebooks" / "task_0000.ipynb"
        older_html = older_run / "notebooks" / "task_0000.html"
        newer_notebook = newer_run / "notebooks" / "task_0001.ipynb"
        newer_html = newer_run / "notebooks" / "task_0001.html"
        older_notebook.parent.mkdir(parents=True)
        newer_notebook.parent.mkdir(parents=True)
        older_notebook.write_text("{}", encoding="utf-8")
        older_html.write_text("<html></html>", encoding="utf-8")
        newer_notebook.write_text("{}", encoding="utf-8")
        newer_html.write_text("<html></html>", encoding="utf-8")

        older_manifest = {
            "run_id": older_run.name,
            "workflow": "workflow.py",
            "status": "succeeded",
            "started_at": "2026-03-01T09:00:00+00:00",
            "finished_at": "2026-03-01T09:01:00+00:00",
            "tasks": {
                "task_0000": {
                    "task": "demo.render_old",
                    "task_type": "notebook",
                    "executed_notebook": "notebooks/task_0000.ipynb",
                    "rendered_html": "notebooks/task_0000.html",
                }
            },
        }
        newer_manifest = {
            "run_id": newer_run.name,
            "workflow": "workflow.py",
            "status": "succeeded",
            "started_at": "2026-03-02T09:00:00+00:00",
            "finished_at": "2026-03-02T09:01:00+00:00",
            "tasks": {
                "task_0001": {
                    "task": "demo.render_new",
                    "task_type": "notebook",
                    "executed_notebook": "notebooks/task_0001.ipynb",
                    "rendered_html": "notebooks/task_0001.html",
                }
            },
        }
        (older_run / "manifest.yaml").write_text(
            yaml.safe_dump(older_manifest, sort_keys=False),
            encoding="utf-8",
        )
        (newer_run / "manifest.yaml").write_text(
            yaml.safe_dump(newer_manifest, sort_keys=False),
            encoding="utf-8",
        )

        result = _run_cli("notebooks", cwd=Path.cwd())

        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo notebooks" in result.stdout
        assert "render_new" in result.stdout
        assert "render_old" in result.stdout
        assert result.stdout.index("render_new") < result.stdout.index("render_old")
        assert str(newer_html.resolve()) in result.stdout
        assert str(newer_notebook.resolve()) in result.stdout

    def test_notebooks_empty_state_is_styled(self) -> None:
        result = _run_cli("notebooks", cwd=Path.cwd())
        assert result.returncode == 0
        assert "🌿 ginkgo notebooks" in result.stdout
        assert "No executed notebooks found." in result.stdout


class TestCliAssets:
    def test_asset_ls_empty_state_is_styled(self) -> None:
        result = _run_cli("asset", "ls", cwd=Path.cwd())
        assert result.returncode == 0
        assert "🌿 ginkgo asset ls" in result.stdout
        assert "No assets found." in result.stdout

    def test_asset_ls_versions_and_inspect(self) -> None:
        first_version = _seed_asset(
            cwd=Path.cwd(),
            name="prepared_data",
            text="hello",
            run_id="run-1",
        )
        second_version = _seed_asset(
            cwd=Path.cwd(),
            name="prepared_data",
            text="goodbye",
            run_id="run-2",
            alias="latest",
        )

        listed = _run_cli("asset", "ls", cwd=Path.cwd())
        assert listed.returncode == 0, listed.stderr
        assert "Asset Key" in listed.stdout
        assert "file:prepared_data" in listed.stdout
        assert second_version in listed.stdout

        versions = _run_cli("asset", "versions", "file:prepared_data", cwd=Path.cwd())
        assert versions.returncode == 0, versions.stderr
        assert "🌿 ginkgo asset versions" in versions.stdout
        assert first_version in versions.stdout
        assert second_version in versions.stdout
        assert "latest" in versions.stdout

        inspected = _run_cli("asset", "inspect", "file:prepared_data@latest", cwd=Path.cwd())
        assert inspected.returncode == 0, inspected.stderr
        assert "🌿 ginkgo asset inspect" in inspected.stdout
        assert "Asset Key: file:prepared_data" in inspected.stdout
        assert f"Version: {second_version}" in inspected.stdout
        assert "Artifact Path:" in inspected.stdout


class TestCliEnv:
    def test_env_ls_empty_state_is_styled(self) -> None:
        result = _run_cli("env", "ls", cwd=Path.cwd())
        assert result.returncode == 0
        assert "🌿 ginkgo env ls" in result.stdout
        assert "No Pixi environments found under envs/." in result.stdout

    def test_env_ls_and_clear_manage_project_local_pixi_installs(self) -> None:
        analysis_dir = Path("envs") / "analysis_tools"
        analysis_dir.mkdir(parents=True)
        (analysis_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'analysis-tools'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        analysis_install = analysis_dir / ".pixi" / "envs" / "default"
        analysis_install.mkdir(parents=True)
        (analysis_install / "marker.txt").write_text("installed", encoding="utf-8")

        bioinfo_dir = Path("envs") / "bioinfo_tools"
        bioinfo_dir.mkdir(parents=True)
        (bioinfo_dir / "pixi.toml").write_text(
            "[workspace]\nname = 'bioinfo-tools'\nchannels = []\nplatforms = []\n",
            encoding="utf-8",
        )
        bioinfo_install = bioinfo_dir / ".pixi" / "envs" / "default"
        bioinfo_install.mkdir(parents=True)
        (bioinfo_install / "marker.txt").write_text("installed", encoding="utf-8")

        listed = _run_cli("env", "ls", cwd=Path.cwd())
        assert listed.returncode == 0, listed.stderr
        assert "analysis_tools" in listed.stdout
        assert "bioinfo_tools" in listed.stdout
        assert "yes" in listed.stdout

        preview = _run_cli("env", "clear", "--all", "--dry-run", cwd=Path.cwd())
        assert preview.returncode == 0, preview.stderr
        assert "would be removed" in preview.stdout
        assert analysis_dir.joinpath(".pixi").exists()
        assert bioinfo_dir.joinpath(".pixi").exists()

        cleared_one = _run_cli("env", "clear", "analysis_tools", cwd=Path.cwd())
        assert cleared_one.returncode == 0, cleared_one.stderr
        assert "🌿 ginkgo env clear" in cleared_one.stdout
        assert "✓ Removed 1 Pixi env" in cleared_one.stdout
        assert not analysis_dir.joinpath(".pixi").exists()
        assert bioinfo_dir.joinpath(".pixi").exists()

        cleared_all = _run_cli("env", "clear", "--all", cwd=Path.cwd())
        assert cleared_all.returncode == 0, cleared_all.stderr
        assert "✓ Removed 1 Pixi env" in cleared_all.stdout
        assert not bioinfo_dir.joinpath(".pixi").exists()

    def test_env_clear_requires_exactly_one_target(self) -> None:
        result = _run_cli("env", "clear", cwd=Path.cwd())
        assert result.returncode == 1
        assert "Specify exactly one of <env> or --all." in result.stderr


class TestCliDebug:
    def test_debug_reports_failed_task_inputs_and_log_tail(self) -> None:
        Path("failing_workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def explode(sample: str) -> str:
    print(f"about-to-fail:{sample}")
    raise RuntimeError("boom")

@flow
def main():
    return explode(sample="sample_1")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        failed = _run_cli("run", "failing_workflow.py", cwd=Path.cwd())
        assert failed.returncode == 1
        assert re.search(
            r"🌿 ginkgo run failing_workflow\.py \([0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{8}\)",
            failed.stdout,
        )
        assert "explode" in failed.stdout
        assert "✖ Failed in " in failed.stdout
        assert "Run Summary" not in failed.stdout
        assert "Failure Details: explode" in failed.stdout
        assert "Reason" in failed.stdout
        assert "boom" in failed.stdout
        assert "Log tail" in failed.stdout
        assert "about-to-fail:sample_1" in failed.stdout
        assert "1/1 complete" in failed.stdout
        assert '{"status":' not in failed.stdout
        assert failed.stdout.index("CPU avg ") < failed.stdout.index("Failure Details: explode")

        run_dir = _extract_run_dir(failed.stderr)
        manifest = yaml.safe_load((run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        task = next(iter(manifest["tasks"].values()))
        assert manifest["status"] == "failed"
        assert task["status"] == "failed"

        debug = _run_cli("debug", run_dir.name, cwd=Path.cwd())
        assert debug.returncode == 0
        assert "Debug Report" in debug.stdout
        assert "Failed Task: explode" in debug.stdout
        assert "Task" in debug.stdout
        assert "sample_1" in debug.stdout
        assert "Exit code" in debug.stdout
        assert "Inputs" in debug.stdout
        assert "Log tail" in debug.stdout
        assert "about-to-fail:sample_1" in debug.stdout

    def test_debug_no_failures_uses_styled_empty_state(self) -> None:
        Path("ok_workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def ok() -> str:
    return "ok"

@flow
def main():
    return ok()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        completed = _run_cli("run", "ok_workflow.py", cwd=Path.cwd())
        assert completed.returncode == 0, completed.stderr
        run_dir = _extract_run_dir(completed.stdout)

        debug = _run_cli("debug", run_dir.name, cwd=Path.cwd())
        assert debug.returncode == 0
        assert f"🌿 ginkgo debug {run_dir.name}" in debug.stdout
        assert f"✓ No failed tasks found in {run_dir.name}" in debug.stdout

    def test_verbose_run_includes_failure_inputs_and_error(self) -> None:
        Path("failing_verbose_workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def explode(sample: str, attempt: int) -> str:
    print(f"about-to-fail:{sample}:{attempt}")
    raise RuntimeError("boom")

@flow
def main():
    return explode(sample="sample_1", attempt=3)
""".strip()
            + "\n",
            encoding="utf-8",
        )

        failed = _run_cli("run", "failing_verbose_workflow.py", "--verbose", cwd=Path.cwd())
        assert failed.returncode == 1
        assert "Failure Details: explode" in failed.stdout
        assert "Inputs" in failed.stdout
        assert "sample: sample_1" in failed.stdout
        assert "attempt: 3" in failed.stdout
        assert "Reason" in failed.stdout
        assert "boom" in failed.stdout

    def test_cli_error_rendering_escapes_bracketed_text(self) -> None:
        Path("markup_error_workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def explode() -> str:
    raise RuntimeError("solver failed at [/tmp/example/pixi.toml:1:1]")

@flow
def main():
    return explode()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "markup_error_workflow.py", cwd=Path.cwd())
        assert result.returncode == 1
        assert "MarkupError" not in result.stderr
        assert "solver failed at [/tmp/example/pixi.toml:1:1]" in result.stderr


class TestCliDryRun:
    def test_run_dry_run_validates_without_execution(self) -> None:
        Path("workflow.py").write_text(
            """
from pathlib import Path
from ginkgo import flow, task

@task()
def write_marker(path: str) -> str:
    Path(path).write_text("executed", encoding="utf-8")
    return path

@flow
def main():
    return write_marker(path="should-not-exist.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--dry-run", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo run workflow.py --dry-run" in result.stdout
        assert "✓ workflow.py (dry-run) - 1 tasks validated" in result.stdout
        assert not Path("should-not-exist.txt").exists()
        runs_root = Path(".ginkgo") / "runs"
        assert not runs_root.exists() or list(runs_root.iterdir()) == []

    def test_run_dry_run_fails_for_invalid_workflow(self) -> None:
        Path("invalid_workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def first() -> str:
    return "first"

@task()
def second() -> str:
    return "second"

@flow
def main():
    left = first()
    right = second()
    left.args["value"] = right
    right.args["value"] = left
    return left
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "invalid_workflow.py", "--dry-run", cwd=Path.cwd())
        assert result.returncode == 1
        assert "Detected cycle in workflow graph" in result.stderr
        assert "Run directory:" not in result.stdout

    def test_test_dry_run_discovers_hidden_workflows_without_execution(self) -> None:
        tests_dir = Path(".tests")
        tests_dir.mkdir()
        (tests_dir / "dry_run_flow.py").write_text(
            """
from pathlib import Path
from ginkgo import flow, task

@task()
def write_marker(path: str) -> str:
    Path(path).write_text("executed", encoding="utf-8")
    return path

@flow
def main():
    return write_marker(path="should-not-exist.txt")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("test", "--dry-run", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo test --dry-run" in result.stdout
        assert "✓ dry_run_flow.py (dry-run) - 1 tasks validated" in result.stdout
        assert "✓ Validated 1 test workflow" in result.stdout
        assert not Path("should-not-exist.txt").exists()

    def test_test_execution_prints_header_and_summary(self) -> None:
        tests_dir = Path(".tests")
        tests_dir.mkdir()
        (tests_dir / "exec_flow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("test", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo test" in result.stdout
        assert "🌿 ginkgo run exec_flow.py" in result.stdout
        assert "✓ Completed 1 test workflow" in result.stdout


class TestCliInit:
    def test_init_creates_project_scaffold(self) -> None:
        result = _run_cli("init", "demo-project", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🌿 ginkgo init demo-project" in result.stdout
        assert "✓ Initialized project scaffold at" in result.stdout
        assert "Created:" in result.stdout
        assert "demo_project/workflow.py" in result.stdout
        assert "README.md" in result.stdout
        assert "ginkgo test --dry-run" in result.stdout

        project_dir = Path("demo-project")
        assert (project_dir / "pixi.toml").is_file()
        assert (project_dir / "ginkgo.toml").is_file()
        assert (project_dir / "README.md").is_file()
        assert (project_dir / "demo_project" / "__init__.py").is_file()
        assert (project_dir / "demo_project" / "workflow.py").is_file()
        assert (project_dir / "demo_project" / "modules" / "prep.py").is_file()
        assert (project_dir / "demo_project" / "modules" / "analysis.py").is_file()
        assert (project_dir / "demo_project" / "modules" / "reporting.py").is_file()
        assert (project_dir / "demo_project" / "envs" / "analysis_tools" / "pixi.toml").is_file()
        assert (project_dir / "demo_project" / "scripts" / "build_brief.py").is_file()
        assert (project_dir / "demo_project" / "notebooks" / "overview.ipynb").is_file()
        assert (project_dir / "skills" / "index.md").is_file()
        assert (project_dir / "skills" / "commands.md").is_file()
        assert (project_dir / "skills" / "project.md").is_file()
        assert (project_dir / "skills" / "workflow-patterns.md").is_file()
        assert (project_dir / "skills" / "local.md").is_file()
        assert (project_dir / "tests" / "workflows" / "smoke.py").is_file()
        assert not (project_dir / "agents.ginkgo.md").exists()
        assert not (project_dir / "__init__.py").exists()

        workflow_text = (project_dir / "demo_project" / "workflow.py").read_text(encoding="utf-8")
        readme_text = (project_dir / "README.md").read_text(encoding="utf-8")
        skills_index_text = (project_dir / "skills" / "index.md").read_text(encoding="utf-8")
        commands_text = (project_dir / "skills" / "commands.md").read_text(encoding="utf-8")
        patterns_text = (project_dir / "skills" / "workflow-patterns.md").read_text(
            encoding="utf-8"
        )
        assert "@flow" in workflow_text
        assert "from demo_project.modules.pipeline import main" not in workflow_text
        assert "expand(" in workflow_text
        assert "ginkgo run --agent" in readme_text
        assert "demo_project/workflow.py" in readme_text
        assert "See `skills/index.md`" in readme_text
        assert "This project uses Ginkgo" in skills_index_text
        assert "`project.md`:" in skills_index_text
        assert "JSONL runtime events" in commands_text
        assert '@task(kind="shell")' in patterns_text
        assert "oci://registry/path:tag" in patterns_text

    def test_init_can_skip_skills(self) -> None:
        result = _run_cli("init", "demo-project", "--no-skills", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr

        project_dir = Path("demo-project")
        assert not (project_dir / "skills").exists()

    def test_init_can_create_skills_only_for_existing_project(self) -> None:
        project_dir = Path("demo-project")
        package_dir = project_dir / "demo_project"
        (package_dir / "modules").mkdir(parents=True)
        (project_dir / "tests" / "workflows").mkdir(parents=True)
        (package_dir / "workflow.py").write_text("workflow\n", encoding="utf-8")

        result = _run_cli("init", "demo-project", "--skills-only", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert (project_dir / "skills" / "index.md").is_file()
        assert not (project_dir / "README.md").exists()

    def test_init_skills_only_refuses_to_overwrite_without_force(self) -> None:
        project_dir = Path("demo-project")
        skills_dir = project_dir / "skills"
        skills_dir.mkdir(parents=True)
        (skills_dir / "index.md").write_text("existing\n", encoding="utf-8")

        result = _run_cli("init", "demo-project", "--skills-only", cwd=Path.cwd())
        assert result.returncode == 1
        assert "✖ Refusing to overwrite existing scaffold files without --force:" in result.stderr
        assert (skills_dir / "index.md").read_text(encoding="utf-8") == "existing\n"

    def test_init_rejects_incompatible_skills_flags(self) -> None:
        result = _run_cli("init", "demo-project", "--skills-only", "--no-skills", cwd=Path.cwd())
        assert result.returncode == 1
        assert "✖ Cannot combine --no-skills with --skills-only." in result.stderr

    def test_init_refuses_to_overwrite_without_force(self) -> None:
        project_dir = Path("demo-project")
        project_dir.mkdir()
        package_dir = project_dir / "demo_project"
        package_dir.mkdir()
        (package_dir / "workflow.py").write_text("existing\n", encoding="utf-8")

        result = _run_cli("init", "demo-project", cwd=Path.cwd())
        assert result.returncode == 1
        assert "✖ Refusing to overwrite existing scaffold files without --force:" in result.stderr
        assert (package_dir / "workflow.py").read_text(encoding="utf-8") == "existing\n"


class TestCliWorkflowDiscovery:
    def test_run_autodiscovers_canonical_package_workflow(self) -> None:
        package_dir = Path("demo_project")
        package_dir.mkdir()
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
        (package_dir / "workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert re.search(
            r"🌿 ginkgo run workflow\.py \([0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{8}\)",
            result.stdout,
        )
        assert "✓ succeeded" in result.stdout


class TestCliOutputModes:
    def test_run_verbose_surfaces_verbose_mode_in_summary(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--verbose", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert re.search(
            r"🌿 ginkgo run workflow\.py \([0-9]{8}_[0-9]{6}_[0-9]{6}_[0-9a-f]{8}\)",
            result.stdout,
        )
        assert "Run Summary" not in result.stdout
        assert re.search(r"Running locally on \d+ Cores", result.stdout)
        assert "🧭 Verbose mode:" in result.stdout
        assert "config overlays=0" in result.stdout
        assert "🗂 Run directory:" in result.stdout
        assert "produce" in result.stdout
        assert "1/1 complete" in result.stdout

    def test_run_uses_singular_core_label_for_one_core(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--cores", "1", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "Running locally on 1 core" in result.stdout

    def test_run_surfaces_memory_budget_and_resource_summary(self) -> None:
        Path("workflow.py").write_text(
            """
import time
from ginkgo import flow, task

@task()
def produce() -> str:
    time.sleep(1.2)
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--memory", "32", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "🧠 Memory budget: 32 GiB" in result.stdout
        assert "CPU avg " in result.stdout
        assert "RSS avg " in result.stdout

        run_dir = _extract_run_dir(result.stdout)
        manifest = yaml.safe_load((run_dir / "manifest.yaml").read_text(encoding="utf-8"))
        assert manifest["memory"] == 32
        assert manifest["resources"]["status"] == "completed"
        assert manifest["resources"]["sample_count"] >= 1


class TestCliMappedLabels:
    def test_map_tasks_use_first_parameter_value_as_runtime_label(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def fastq_stats(sample_id: str, read_count: int) -> str:
    return f"{sample_id}:{read_count}"

@flow
def main():
    return fastq_stats().map(
        sample_id=["sample_a", "sample_b"],
        read_count=[10, 20],
    )
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "fastq_stats[sample_a]" in result.stdout
        assert "fastq_stats[sample_b]" in result.stdout
        assert "fastq_stats[2]" not in result.stdout
        assert "2/2 complete" in result.stdout

    def test_product_map_tasks_use_named_parameter_grid_runtime_labels(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def train(sample: str, lr: float) -> str:
    return f"{sample}:{lr}"

@flow
def main():
    return train().product_map(
        sample=["sample_a", "sample_b"],
        lr=[0.01, 0.1],
    )
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "train[sample=sample_a,lr=0.01]" in result.stdout
        assert "train[sample=sample_a,lr=0.1]" in result.stdout
        assert "train[sample=sample_b,lr=0.01]" in result.stdout
        assert "train[sample=sample_b,lr=0.1]" in result.stdout
        assert "4/4 complete" in result.stdout

    def test_map_then_product_map_composes_runtime_labels(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def train(sample: str, lr: float, epochs: int) -> str:
    return f"{sample}:{lr}:{epochs}"

@flow
def main():
    return train().map(sample=["sample_a"]).product_map(
        lr=[0.01, 0.1],
        epochs=[10, 50],
    )
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "train[sample_a,lr=0.01,epochs=10]" in result.stdout
        assert "train[sample_a,lr=0.01,epochs=50]" in result.stdout
        assert "train[sample_a,lr=0.1,epochs=10]" in result.stdout
        assert "train[sample_a,lr=0.1,epochs=50]" in result.stdout


class TestCliSpinnerSelection:
    def test_time_of_day_spinner_uses_earth_in_day_and_moon_at_night(self) -> None:
        assert _time_of_day_spinner(datetime(2026, 3, 12, 9, 0, 0)) == "earth"
        assert _time_of_day_spinner(datetime(2026, 3, 12, 22, 0, 0)) == "moon"


class TestCliLabelTruncation:
    def test_truncate_task_label_preserves_suffix_and_fanout_index(self) -> None:
        label = "extremely_long_alignment_and_qc_summary_task_name[12]"
        truncated = _truncate_task_label(label, max_width=24)
        assert truncated.endswith("name[12]")
        assert truncated.startswith("extrem")
        assert len(truncated) <= 24


class TestCliEnvironmentLabels:
    def test_environment_label_formats_local_and_pixi_tasks(self) -> None:
        assert _environment_label(None) == "local"
        assert _environment_label("bioinfo_tools") == "pixi:bioinfo_tools"


class TestCliCoreLabels:
    def test_core_unit_label_handles_singular_and_plural(self) -> None:
        assert _core_unit_label(1) == "core"
        assert _core_unit_label(2) == "Cores"


class TestCliSecrets:
    def test_secrets_list_and_validate(self, monkeypatch) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, secret, task

@task()
def echo_token(token: str) -> str:
    return token

@flow
def main():
    return echo_token(token=secret("API_TOKEN"))
""".strip()
            + "\n",
            encoding="utf-8",
        )

        listed = _run_cli("secrets", "list", "workflow.py", cwd=Path.cwd())
        assert listed.returncode == 0, listed.stderr
        assert "env:API_TOKEN" in listed.stdout

        monkeypatch.delenv("API_TOKEN", raising=False)
        missing = _run_cli("secrets", "validate", "workflow.py", cwd=Path.cwd())
        assert missing.returncode == 1
        assert "env:API_TOKEN" in missing.stdout

        monkeypatch.setenv("API_TOKEN", "ok")
        validated = _run_cli("secrets", "validate", "workflow.py", cwd=Path.cwd())
        assert validated.returncode == 0, validated.stderr
        assert "env:API_TOKEN" in validated.stdout

    def test_doctor_reports_missing_secret(self, monkeypatch) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, secret, task

@task()
def echo_token(token: str) -> str:
    return token

@flow
def main():
    return echo_token(token=secret("MISSING_TOKEN"))
""".strip()
            + "\n",
            encoding="utf-8",
        )

        monkeypatch.delenv("MISSING_TOKEN", raising=False)
        result = _run_cli("doctor", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 1
        assert "Missing secrets: env:MISSING_TOKEN" in result.stderr


class TestCliGroupedProgressBars:
    def test_grouped_bar_renders_waiting_as_empty_space(self) -> None:
        console = Console(width=80, record=True)
        bar = _MultiStateBar(counts=Counter({"waiting": 4}), total=4, width=8)

        rendered = list(bar.__rich_console__(console, console.options))

        assert len(rendered) == 1
        assert rendered[0].plain == " " * 8

    def test_grouped_bar_uses_slab_fill_for_active_segments(self) -> None:
        console = Console(width=80, record=True)
        bar = _MultiStateBar(
            counts=Counter({"running": 2, "staging": 1, "cached": 1}),
            total=4,
            width=8,
        )

        rendered = list(bar.__rich_console__(console, console.options))

        assert len(rendered) == 1
        assert "█" in rendered[0].plain


class TestCliAgentAndInspection:
    def test_run_agent_emits_jsonl_events(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--agent", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        event_types = {event["event"] for event in events}
        assert "run_started" in event_types
        assert "run_validated" in event_types
        assert "task_started" in event_types
        assert "task_completed" in event_types
        assert "run_completed" in event_types

    def test_run_agent_verbose_emits_task_log_events(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def produce() -> str:
    print("streamed stdout line")
    return "ok"

@flow
def main():
    return produce()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--agent", "--verbose", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        task_logs = [event for event in events if event["event"] == "task_log"]
        assert task_logs
        assert any(event["stream"] == "stdout" for event in task_logs)
        assert any("streamed stdout line" in event["chunk"] for event in task_logs)

    def test_inspect_workflow_returns_static_graph_json(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def one() -> str:
    return "one"

@task()
def two(value: str) -> str:
    return value

@flow
def main():
    return two(value=one())
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("inspect", "workflow", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        payload = json.loads(result.stdout)
        assert payload["task_count"] == 2
        assert payload["edge_count"] == 1
        assert {task["task_name"] for task in payload["tasks"]} == {"one", "two"}

    def test_debug_json_and_inspect_run_return_machine_readable_failure_data(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def explode(value: str) -> str:
    raise RuntimeError(f"boom:{value}")

@flow
def main():
    return explode(value="sample")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        run = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert run.returncode == 1
        run_dir = _extract_run_dir(run.stderr)

        debug = _run_cli("debug", run_dir.name, "--json", cwd=Path.cwd())
        assert debug.returncode == 0, debug.stderr
        debug_payload = json.loads(debug.stdout)
        assert debug_payload["status"] == "failed"
        assert debug_payload["failures"][0]["task_name"] == "explode"

        inspect = _run_cli("inspect", "run", run_dir.name, cwd=Path.cwd())
        assert inspect.returncode == 0, inspect.stderr
        inspect_payload = json.loads(inspect.stdout)
        assert inspect_payload["status"] == "failed"
        assert inspect_payload["tasks"][0]["failure"]["kind"] == "scheduler_error"

    def test_doctor_json_reports_machine_readable_diagnostics(self, monkeypatch) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, secret, task

@task()
def echo_token(token: str) -> str:
    return token

@flow
def main():
    return echo_token(token=secret("MISSING_TOKEN"))
""".strip()
            + "\n",
            encoding="utf-8",
        )

        monkeypatch.delenv("MISSING_TOKEN", raising=False)
        result = _run_cli("doctor", "workflow.py", "--json", cwd=Path.cwd())
        assert result.returncode == 1
        payload = json.loads(result.stdout)
        assert payload[0]["code"] == "MISSING_SECRET"

    def test_cache_explain_reports_rerun_reason(self) -> None:
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task(version="v1")
def produce(value: str) -> str:
    return value

@flow
def main():
    return produce(value="a")
""".strip()
            + "\n",
            encoding="utf-8",
        )

        first = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert first.returncode == 0, first.stderr
        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task(version="v2")
def produce(value: str) -> str:
    return value

@flow
def main():
    return produce(value="a")
""".strip()
            + "\n",
            encoding="utf-8",
        )
        second = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert second.returncode == 0, second.stderr
        run_dir = _extract_run_dir(second.stdout)

        explain = _run_cli("cache", "explain", "--run", run_dir.name, cwd=Path.cwd())
        assert explain.returncode == 0, explain.stderr
        payload = json.loads(explain.stdout)
        assert payload["tasks"][0]["reason"] in {
            "version_bump",
            "cache_key_changed",
            "source_hash_changed",
        }


class TestCliRunProfile:
    def test_run_profile_emits_table_and_persists_snapshot(self) -> None:
        from ginkgo.runtime.caching.provenance import load_manifest

        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def hello() -> str:
    return "ok"

@flow
def main():
    return hello()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", "--profile", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "Runtime Profile" in result.stdout
        assert "scheduler_dispatch" in result.stdout
        assert "evaluator_validate" in result.stdout

        run_dir = _extract_run_dir(result.stdout)
        manifest = load_manifest(run_dir)
        profile = manifest["timings"]["profile"]
        assert "scheduler_dispatch" in profile
        assert profile["scheduler_dispatch"]["seconds"] > 0
        assert profile["scheduler_dispatch"]["count"] >= 1

        inspect = _run_cli("inspect", "run", run_dir.name, cwd=Path.cwd())
        assert inspect.returncode == 0, inspect.stderr
        inspect_payload = json.loads(inspect.stdout)
        assert "scheduler_dispatch" in inspect_payload["timings"]["profile"]

    def test_run_without_profile_does_not_emit_table_or_snapshot(self) -> None:
        from ginkgo.runtime.caching.provenance import load_manifest

        Path("workflow.py").write_text(
            """
from ginkgo import flow, task

@task()
def hello() -> str:
    return "ok"

@flow
def main():
    return hello()
""".strip()
            + "\n",
            encoding="utf-8",
        )

        result = _run_cli("run", "workflow.py", cwd=Path.cwd())
        assert result.returncode == 0, result.stderr
        assert "Runtime Profile" not in result.stdout

        run_dir = _extract_run_dir(result.stdout)
        manifest = load_manifest(run_dir)
        assert manifest["timings"]["profile"] == {}
