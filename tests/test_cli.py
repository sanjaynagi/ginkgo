"""Phase 6 CLI and provenance integration tests."""

from __future__ import annotations

import re
import subprocess
from datetime import datetime
from pathlib import Path

import yaml

from ginkgo.cli import (
    _core_unit_label,
    _environment_label,
    _time_of_day_spinner,
    _truncate_task_label,
)


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
            r"🌿 ginkgo run workflow\.py \([0-9]{8}_[0-9]{6}_[0-9a-f]{8}\)", first.stdout
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
        (old_entry / "meta.json").write_text(
            '{"function":"demo.old","timestamp":"2026-01-01T00:00:00+00:00"}',
            encoding="utf-8",
        )
        (fresh_entry / "meta.json").write_text(
            '{"function":"demo.fresh","timestamp":"2026-03-13T00:00:00+00:00"}',
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
        (old_entry / "meta.json").write_text(
            '{"function":"demo.old","timestamp":"2026-01-01T00:00:00+00:00"}',
            encoding="utf-8",
        )
        (fresh_entry / "meta.json").write_text(
            '{"function":"demo.fresh","timestamp":"2026-03-13T00:00:00+00:00"}',
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
            r"🌿 ginkgo run failing_workflow\.py \([0-9]{8}_[0-9]{6}_[0-9a-f]{8}\)",
            failed.stdout,
        )
        assert "explode" in failed.stdout
        assert "✖ Failed in " in failed.stdout
        assert "Run Summary" not in failed.stdout
        assert "Failure Details: explode" in failed.stdout
        assert "Log tail" in failed.stdout
        assert "about-to-fail:sample_1" in failed.stdout
        assert "1/1 complete" in failed.stdout
        assert '{"status":' not in failed.stdout

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
        assert "Error" in failed.stdout
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
        assert "workflow.py" in result.stdout
        assert "agents.ginkgo.md" in result.stdout
        assert "ginkgo test --dry-run" in result.stdout

        project_dir = Path("demo-project")
        assert (project_dir / "workflow.py").is_file()
        assert (project_dir / "ginkgo.toml").is_file()
        assert (project_dir / ".tests" / "smoke.py").is_file()
        assert (project_dir / "envs" / "analysis_tools" / "pixi.toml").is_file()
        assert (project_dir / "agents.ginkgo.md").is_file()

        workflow_text = (project_dir / "workflow.py").read_text(encoding="utf-8")
        assert "@flow" in workflow_text
        assert "ginkgo.config" in workflow_text
        assert 'ginkgo.config("ginkgo.toml")' in workflow_text

    def test_init_refuses_to_overwrite_without_force(self) -> None:
        project_dir = Path("demo-project")
        project_dir.mkdir()
        (project_dir / "workflow.py").write_text("existing\n", encoding="utf-8")

        result = _run_cli("init", "demo-project", cwd=Path.cwd())
        assert result.returncode == 1
        assert "✖ Refusing to overwrite existing scaffold files without --force:" in result.stderr
        assert (project_dir / "workflow.py").read_text(encoding="utf-8") == "existing\n"


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
            r"🌿 ginkgo run workflow\.py \([0-9]{8}_[0-9]{6}_[0-9a-f]{8}\)", result.stdout
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
