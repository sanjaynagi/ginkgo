"""CLI and provenance integration tests."""

from __future__ import annotations

import ast
import importlib.util
import json
import re
import shutil
import subprocess
import sys
import tomllib
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from rich.console import Console

import ginkgo
from ginkgo.cli.commands.init import GINKGO_BRANCH, GINKGO_REPO_URL, _iter_template_files
from ginkgo.cli.renderers.common import (
    _MultiStateBar,
    _core_unit_label,
    _time_of_day_spinner,
    _truncate_task_label,
    environment_label,
)
from ginkgo.envs.interpreter import source_import_roots
from ginkgo.runtime.caching.index import CacheIndex
from ginkgo.workspace_layout import WorkspaceLayout
from tests.cli.conftest import (
    SIMPLE_FLOW,
    manifest_of,
    run_dir_of,
    run_ok,
    unwrapped,
    write,
)
from tests.conftest import run_cli

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_HEADER = r"🌿 ginkgo run {} \([0-9]{{8}}_[0-9]{{6}}_[0-9]{{6}}_[0-9a-f]{{8}}\)"
CACHE = Path(".ginkgo") / "cache"


def _seed_cache_entry(
    name: str,
    *,
    age_days: int,
    function: str = "demo.x",
    payload: str = "{}",
    last_hit_days: int | None = None,
) -> Path:
    """Create a cache entry aged ``age_days`` days — bytes and row — for prune tests."""
    entry = CACHE / name
    entry.mkdir(parents=True)
    (entry / "output.json").write_text(payload, encoding="utf-8")
    now = datetime.now(timezone.utc)
    with CacheIndex.open(path=WorkspaceLayout.sibling_of(CACHE).db) as index:
        index.record_entry(
            cache_key=name,
            meta={
                "function": function,
                "created_at": (now - timedelta(days=age_days)).isoformat(),
            },
            artifact_ids={},
            size_bytes=len(payload.encode("utf-8")),
            run_id=None,
        )
        if last_hit_days is not None:
            index.record_hit(name, at=(now - timedelta(days=last_hit_days)).isoformat())
    return entry


def _task(run_dir: Path) -> dict:
    """Return the only task of a run's manifest."""
    return next(iter(manifest_of(run_dir)["tasks"].values()))


def test_version_flag_reports_pyproject_version() -> None:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    result = run_ok("--version")
    assert result.stdout.strip() == f"ginkgo {pyproject['project']['version']}"


def test_init_help_describes_every_flag() -> None:
    help_text = unwrapped(run_ok("init", "--help").stdout)
    assert "--no-skills Write the project scaffold without the agent skills." in help_text
    assert "--skills-only Write only the agent skills, not the project scaffold." in help_text
    assert "--force Overwrite scaffold files that already exist." in help_text


@pytest.mark.parametrize(
    ("args", "subcommands"),
    [
        (("cache",), "{ls,stats,clear,explain,prune}"),
        (("inspect",), "{workflow}"),
        (("runs",), "{ls,show,status,wait}"),
        (("export",), "{events,manifest}"),
        (("secrets",), "{list,validate}"),
        ((), ""),
    ],
)
def test_command_group_without_subcommand_shows_help(args: tuple, subcommands: str) -> None:
    result = run_cli(*args)
    assert result.returncode == 2
    assert f"usage: ginkgo {' '.join(args)}".strip() in result.stderr
    assert subcommands in result.stderr
    assert "the following arguments are required" not in result.stderr


def test_missing_positional_keeps_its_precise_error() -> None:
    """Only a missing subcommand becomes help; other missing arguments do not."""
    result = run_cli("why")
    assert result.returncode == 2
    assert "the following arguments are required" in result.stderr


class TestCliRunAndCache:
    def test_run_writes_manifest_params_and_cache_metadata(self) -> None:
        write("ginkgo.toml", 'message = "default"\nextra = "base"\n')
        write("override-1.toml", 'message = "first"\n')
        write("override-2.toml", 'message = "second"\nextra = "override"\n')
        write(
            "workflow.py",
            """
            import ginkgo
            from pathlib import Path
            from ginkgo import flow, task, untracked

            cfg = ginkgo.config("ginkgo.toml")

            @task()
            def write_message(message: str, output_path: untracked) -> str:
                Path(output_path).write_text(message, encoding="utf-8")
                return output_path

            @flow
            def main():
                return write_message(message=cfg["message"], output_path="result.txt")
            """,
        )
        args = ("run", "workflow.py", "--config", "override-1.toml", "--config", "override-2.toml")

        first = run_ok(*args)
        assert re.search(RUN_HEADER.format(r"workflow\.py"), first.stdout)
        # Load-bearing content only — decorative spinner/clock lines are not pinned.
        assert re.search(r"Running locally on \d+ Cores", first.stdout)
        assert "write_message" in first.stdout and "1/1 complete" in first.stdout
        assert "Run Summary" not in first.stdout and '{"status":' not in first.stdout
        first_manifest = manifest_of(run_dir_of(first.stdout))
        first_task = next(iter(first_manifest["tasks"].values()))
        assert Path("result.txt").read_text(encoding="utf-8") == "second"
        assert first_manifest["status"] == "succeeded"
        assert (first_task["status"], first_task["cached"]) == ("succeeded", False)
        assert first_task["inputs"]["message"] == "second"
        assert first_manifest["params"] == {"extra": "override", "message": "second"}

        second = run_ok(*args)
        second_task = _task(run_dir_of(second.stdout))
        assert "cached" in second.stdout and "1/1 complete" in second.stdout
        assert (second_task["status"], second_task["cached"]) == ("cached", True)

    def test_cache_ls_and_clear_one_entry(self) -> None:
        write("workflow.py", SIMPLE_FLOW)
        cache_key = _task(run_dir_of(run_ok("run", "workflow.py").stdout))["cache_key"]

        listed = run_ok("cache", "ls").stdout
        for column in ("Cache Key", "Task", "Size", "Age"):
            assert column in listed
        assert cache_key in listed.replace("\n", "")

        assert f"Removed cache entry {cache_key}" in run_ok("cache", "clear", cache_key).stdout
        assert not (CACHE / cache_key).exists()

    def test_cache_ls_empty_state_is_styled(self) -> None:
        result = run_ok("cache", "ls")
        assert "🌿 ginkgo cache ls" in result.stdout
        assert "No cache entries found." in result.stdout

    def test_run_manifest_records_retry_attempts(self) -> None:
        write(
            "retry_workflow.py",
            """
            from pathlib import Path
            from ginkgo import flow, task

            @task(retries=2)
            def flaky(marker_path: str) -> str:
                marker = Path(marker_path)
                if not marker.exists():
                    marker.write_text("1", encoding="utf-8")
                    raise RuntimeError("transient failure")
                return "ok"

            @flow
            def main():
                return flaky(marker_path="retry.marker")
            """,
        )
        task = _task(run_dir_of(run_ok("run", "retry_workflow.py").stdout))
        assert (task["status"], task["max_attempts"], task["attempts"]) == ("succeeded", 3, 2)

    @pytest.mark.parametrize(
        ("args", "removed", "said"),
        [
            (("--older-than", "30d", "--dry-run"), set(), "would be removed"),
            (("--older-than", "30d"), {"e100", "e60"}, "Removed"),
            (("--max-entries", "2"), {"e100", "e60"}, "Removed"),
            (("--max-size", "2KiB"), {"e100", "e60"}, "Removed"),
        ],
    )
    def test_cache_prune_removes_the_oldest_entries(
        self, args: tuple, removed: set, said: str
    ) -> None:
        entries = {
            f"e{age}": _seed_cache_entry(f"e{age}", age_days=age, payload="x" * 1024)
            for age in (100, 60, 20, 5)
        }
        result = run_ok("cache", "prune", *args)
        assert "🌿 ginkgo cache prune" in result.stdout and said in result.stdout
        assert {name for name, path in entries.items() if not path.exists()} == removed

    def test_cache_prune_least_recently_hit_gives_up_the_unused_first(self) -> None:
        """Age is not use: an old entry that keeps hitting outlives a young idle one."""
        old_but_used = _seed_cache_entry("used", age_days=100, last_hit_days=1)
        young_but_idle = _seed_cache_entry("idle", age_days=5)
        run_ok("cache", "prune", "--max-entries", "1", "--least-recently-hit")
        assert old_but_used.exists() and not young_but_idle.exists()

    def test_cache_stats_counts_entries_and_bytes(self) -> None:
        _seed_cache_entry("a", age_days=1, payload="x" * 100)
        _seed_cache_entry("b", age_days=2, payload="y" * 50, last_hit_days=1)
        stats = json.loads(run_ok("cache", "stats", "--json").stdout)
        assert (stats["entries"], stats["total_bytes"]) == (2, 150)
        assert (stats["never_hit"], stats["never_hit_bytes"]) == (1, 100)
        assert stats["hit_histogram"] == {"0": 1, "1": 1}
        assert stats["top_functions"][0]["entries"] == 2

    def test_cache_sizes_count_and_prune_frees_artifact_bytes(self) -> None:
        write(
            "workflow.py",
            """
            from pathlib import Path
            from ginkgo import Out, file, flow, folder, task

            @task()
            def make(dest: Out[file], tree: Out[folder]) -> tuple[file, folder]:
                Path(dest).write_text("x" * 1000, encoding="utf-8")
                Path(tree).mkdir(exist_ok=True)
                Path(tree, "member.txt").write_text("y" * 100, encoding="utf-8")
                return dest, tree

            @flow
            def main():
                return make(dest="out.txt", tree="tree")
            """,
        )
        run_ok("run", "workflow.py")
        stats = json.loads(run_ok("cache", "stats", "--json").stdout)
        (entry,) = json.loads(run_ok("cache", "ls", "--json").stdout)
        assert stats["artifact_bytes"] == entry["artifact_bytes"] == 1100
        assert entry["size_bytes"] == entry["output_bytes"] + 1100
        assert stats["total_bytes"] == stats["top_functions"][0]["bytes"] == entry["size_bytes"]
        assert "Artifact store: 1.1 KiB" in run_ok("cache", "stats").stdout

        assert "1 entries" in run_ok("cache", "prune", "--max-size", "1KB", "--dry-run").stdout
        assert "Removed 1 cache entry" in run_ok("cache", "prune", "--max-size", "1KB").stdout
        assert list((Path(".ginkgo") / "artifacts" / "blobs").iterdir()) == []
        run_ok("db", "check")

    def test_a_large_value_blob_is_counted_and_kept_while_named(self) -> None:
        """SC5-3: a large value lives in the artifact store, and survives a sibling's clear."""
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def big() -> bytes:
                return b"z" * (300 * 1024)

            @task()
            def small() -> int:
                return 1

            @flow
            def main():
                return [big(), small()]
            """,
        )
        run_ok("run", "workflow.py")
        entries = {e["task_name"]: e for e in json.loads(run_ok("cache", "ls", "--json").stdout)}
        assert entries["big"]["size_bytes"] > 300 * 1024

        run_ok("cache", "clear", entries["small"]["cache_key"])
        rerun = run_ok("run", "workflow.py", "--agent-output")
        run_ok("db", "check")
        statuses = {
            event["task_name"].rsplit(".", 1)[-1]: event["status"]
            for event in map(json.loads, rerun.stdout.splitlines())
            if event.get("event") == "task_completed"
        }
        assert statuses == {"big": "cached", "small": "success"}

    @pytest.mark.parametrize("args", [("prune", "--older-than", "30d"), ("clear", "--all")])
    def test_prune_and_clear_all_remove_orphaned_directories(self, args: tuple) -> None:
        """EV5-10: nothing is left behind for `db check` to flag."""
        known = _seed_cache_entry("old", age_days=100)
        orphan = CACHE / "orphan"
        orphan.mkdir()
        result = run_ok("cache", *args)
        assert "Removed 1 cache entry and 1 orphaned directory" in unwrapped(result.stdout)
        assert not known.exists() and not orphan.exists()
        run_ok("db", "check")

    def test_cache_clear_orphans_removes_directories_with_no_row(self) -> None:
        known = _seed_cache_entry("known", age_days=1)
        orphan = CACHE / "orphan"
        orphan.mkdir()
        assert "Removed 1 orphaned cache director" in run_ok("cache", "clear", "--orphans").stdout
        assert known.exists() and not orphan.exists()

    def test_cache_clear_task_removes_only_that_tasks_entries(self) -> None:
        first = _seed_cache_entry("a1", age_days=1, function="m.a", payload="x" * 2048)
        second = _seed_cache_entry("a2", age_days=1, function="m.a")
        other = _seed_cache_entry("b1", age_days=1, function="m.b")

        result = run_ok("cache", "clear", "--task", "a")
        assert "Removed 2 cache entries" in result.stdout
        # The rows go too, but their space is only returned by `db vacuum`.
        assert "KiB freed on disk)" in result.stdout
        assert "ginkgo db vacuum" in unwrapped(result.stdout)
        assert not first.exists() and not second.exists() and other.exists()

        missing = run_cli("cache", "clear", "--task", "missing")
        assert missing.returncode != 0
        assert "No cache entries found for task: missing" in missing.stdout + missing.stderr


class TestCliWhy:
    def test_a_materialized_path_names_its_producer(self) -> None:
        write(
            "workflow.py",
            """
            from pathlib import Path
            from ginkgo import Out, file, flow, task

            @task()
            def write_report(output_path: Out[file]) -> file:
                Path(output_path).write_text("report", encoding="utf-8")
                return file(output_path)

            @flow
            def main():
                return write_report(output_path="results/report.txt")
            """,
        )
        run_ok("run", "workflow.py")

        text = run_ok("why", "results/report.txt").stdout
        assert "🌿 ginkgo why" in text and "write_report" in text
        payload = json.loads(run_ok("why", "results/report.txt", "--json").stdout)
        assert payload["task_name"].endswith("write_report")
        assert payload["path"] == str(Path("results/report.txt").resolve())

        unknown = run_cli("why", "results/absent.txt")
        assert unknown.returncode == 1
        assert "Nothing in the ledger produced" in unknown.stderr


class TestCliDebug:
    def test_a_failed_run_reports_inputs_and_log_tail_everywhere(self) -> None:
        write(
            "failing_workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def explode(sample: str, attempt: int) -> str:
                print(f"about-to-fail:{sample}")
                raise RuntimeError("boom at [/tmp/example/pixi.toml:1:1]")

            @flow
            def main():
                return explode(sample="sample_1", attempt=3)
            """,
        )
        failed = run_cli("run", "failing_workflow.py")
        assert failed.returncode == 1
        out = failed.stdout
        assert re.search(RUN_HEADER.format(r"failing_workflow\.py"), out)
        for phrase in (
            "✖ Failed in ",
            "Failure Details: explode",
            "Reason",
            "boom",
            "Log tail",
            "about-to-fail:sample_1",
            "0/1 complete (1 failed)",
        ):
            assert phrase in out
        assert "Run Summary" not in out and '{"status":' not in out
        assert out.index("CPU avg ") < out.index("Failure Details: explode")
        # Bracketed text in an error is printed literally, not read as Rich markup.
        assert "MarkupError" not in failed.stderr
        assert "boom at [/tmp/example/pixi.toml:1:1]" in unwrapped(failed.stderr)

        run_dir = run_dir_of(failed.stderr)
        assert manifest_of(run_dir)["status"] == "failed"
        assert _task(run_dir)["status"] == "failed"

        debug = run_ok("debug", run_dir.name).stdout
        for phrase in ("Debug Report", "Failed Task: explode", "sample_1", "Inputs", "Log tail"):
            assert phrase in debug
        assert "about-to-fail:sample_1" in debug
        # A Python exception has no exit code, so there is no row for one.
        assert "Exit code" not in debug

        debug_json = json.loads(run_ok("debug", run_dir.name, "--json").stdout)
        assert debug_json["status"] == "failed"
        assert debug_json["failures"][0]["task_name"] == "explode"
        shown = json.loads(run_ok("runs", "show", run_dir.name, "--json").stdout)
        assert shown["status"] == "failed"
        assert shown["tasks"][0]["failure"]["kind"] == "user_code_error"

        verbose = run_cli("run", "failing_workflow.py", "--verbose").stdout
        assert "sample: sample_1" in verbose and "attempt: 3" in verbose

    def test_debug_no_failures_uses_styled_empty_state(self) -> None:
        write("ok_workflow.py", SIMPLE_FLOW)
        run_id = run_dir_of(run_ok("run", "ok_workflow.py").stdout).name
        debug = run_ok("debug", run_id).stdout
        assert f"🌿 ginkgo debug {run_id}" in debug
        assert f"✓ No failed tasks found in {run_id}" in debug


_DROPPED_FLOW = """
from ginkgo import flow, task

@task()
def label(text: str) -> str:
    return text

@flow
def main():
    label(text="dropped")
    {tail}
"""


class TestCliDryRun:
    def test_run_dry_run_previews_plan_without_executing(self) -> None:
        write(
            "workflow.py",
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
            """,
        )
        out = run_ok("run", "workflow.py", "--dry-run").stdout
        assert "🌿 ginkgo run workflow.py --dry-run" in out
        assert "Dry run" in out and "1 task" in out and "Wave 1" in out
        # Nothing is unknown, so there is no legend for it.
        assert "cannot be known without running" not in out
        # The plan labels a node exactly as the live run table does.
        assert "• write_marker  [will run: no prior entry]" in out
        assert "no tasks executed" in out
        assert not Path("should-not-exist.txt").exists()
        runs_root = Path(".ginkgo") / "runs"
        assert not runs_root.exists() or list(runs_root.iterdir()) == []

    def test_dropped_calls_are_reported_by_dry_run_and_run(self) -> None:
        write("workflow.py", _DROPPED_FLOW.format(tail='return label(text="kept")'))

        dry = run_ok("run", "workflow.py", "--dry-run")
        assert "1 task" in dry.stdout and "! label()" in dry.stdout
        assert "Dropped (not reachable from the flow return value)" in unwrapped(dry.stdout)
        # The plan section is the only report; the run-header warning is suppressed.
        assert "⚠" not in dry.stderr

        stderr = unwrapped(run_ok("run", "workflow.py").stderr)
        assert "not reachable from the flow return value" in stderr
        # The fix doctor offers is shown too, not just the symptom (F9).
        assert "Return its result from the flow" in stderr

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_a_flow_whose_every_call_was_dropped_fails(self, dry_run: bool) -> None:
        """N1: a flow that forgot to return its only call must not pass with nothing run."""
        write("workflow.py", _DROPPED_FLOW.format(tail=""))
        result = run_cli("run", "workflow.py", *(("--dry-run",) if dry_run else ()))
        assert result.returncode == 1
        output = unwrapped(result.stdout + result.stderr)
        assert "label() is not reachable from the flow return value" in output
        assert "No task would run" in output
        assert "Return the result of each call the flow needs" in output

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_dropped_producer_is_named_before_its_consumer_fails_validation(
        self, dry_run: bool
    ) -> None:
        """#341: the dropped-call warning explains the `file` input's "must exist" error."""
        write(
            "workflow.py",
            """
            from pathlib import Path
            from ginkgo import Out, file, flow, task

            @task()
            def produce(out: Out[file]) -> None:
                Path(out).write_text("x")

            @task()
            def consume(p: file) -> str:
                return Path(p).read_text()

            @flow
            def main():
                produce(out="p.txt")
                return consume(p="p.txt")
            """,
        )
        result = run_cli("run", "workflow.py", *(("--dry-run",) if dry_run else ()))
        assert result.returncode == 1
        stderr = unwrapped(result.stderr)
        warning = stderr.index("produce() is not reachable from the flow return value")
        assert warning < stderr.index("must exist and be a file")

    @pytest.mark.parametrize(
        ("body", "args", "code"),
        [
            # #332: a returned partial call must not run green with zero tasks.
            ('return join(left="a")', (), 1),
            ('return join(left="a")', ("--dry-run",), 1),
            ('join(left="a")\n    return join(left="a", right="b")', (), 0),
        ],
    )
    def test_a_partial_call_is_named(self, body: str, args: tuple, code: int) -> None:
        write(
            "workflow.py",
            f"""
from ginkgo import flow, task

@task()
def join(left: str, right: str) -> str:
    return left + right

@flow
def main():
    {body}
""",
        )
        result = run_cli("run", "workflow.py", *args)
        assert result.returncode == code
        assert "join() is missing required argument(s): right" in unwrapped(
            result.stdout + result.stderr
        )

    def test_run_dry_run_groups_waves_and_expands_fanout(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def prepare() -> str:
                return "ready"

            @task()
            def analyse(base: str, sample: str) -> str:
                return f"{base}:{sample}"

            @flow
            def main():
                return analyse(base=prepare()).map(sample=["alpha", "beta", "gamma"])
            """,
        )
        out = run_ok("run", "workflow.py", "--dry-run").stdout
        for phrase in (
            "Dry run",
            "4 tasks",
            "Wave 1",
            "Wave 2",
            "• prepare ",
            "no tasks executed",
        ):
            assert phrase in out
        for sample in ("alpha", "beta", "gamma"):
            assert f"analyse[{sample}]" in out
        # The leaf's cache state is determinable; branches downstream of it are not.
        assert "[will run: no prior entry]" in out
        assert "? [unknown: after upstream]  an upstream task runs first" in out

    def test_inspect_and_a_dry_run_after_a_real_run(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def upstream() -> int:
                return 21

            @task()
            def downstream(value: int) -> int:
                return value * 2

            @flow
            def main():
                return downstream(value=upstream())
            """,
        )
        payload = json.loads(run_ok("inspect", "workflow", "workflow.py").stdout)
        assert (payload["task_count"], payload["edge_count"]) == (2, 1)
        assert {task["task_name"] for task in payload["tasks"]} == {"upstream", "downstream"}

        run_ok("run", "workflow.py")
        preview = run_ok("run", "workflow.py", "--dry-run").stdout
        assert "all cached" in preview and preview.count("[cached]") == 2
        assert "[will run]" not in preview

    def test_run_dry_run_fails_for_invalid_workflow(self) -> None:
        write(
            "workflow.py",
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
                left, right = first(), second()
                left.args["value"] = right
                right.args["value"] = left
                return left
            """,
        )
        result = run_cli("run", "workflow.py", "--dry-run")
        assert result.returncode == 1
        assert "Detected cycle in workflow graph" in result.stderr
        assert "Run directory:" not in result.stdout


_TEMPLATE_ROOT = REPO_ROOT / "src" / "ginkgo" / "templates" / "init" / "base"


def _template_ginkgo_symbols() -> set[str]:
    """Return every name the scaffold templates reach for on the ginkgo package."""
    symbols: set[str] = set()
    for path in sorted(_TEMPLATE_ROOT.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module == "ginkgo" and not node.level:
                symbols.update(alias.name for alias in node.names)
            elif (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "ginkgo"
            ):
                symbols.add(node.attr)
    return symbols


def _third_party_imports(*, project_dir: Path) -> set[str]:
    """Return the root modules a scaffolded project imports from outside the stdlib.

    Extraction is shared with ``ginkgo.envs.interpreter``, which asks the same
    question of a user's project at runtime, so the two cannot drift.
    """
    sources = [path.read_text(encoding="utf-8") for path in sorted(project_dir.rglob("*.py"))]
    roots = {root for text in sources for root in source_import_roots(text=text)}
    return {root for root in roots if root not in sys.stdlib_module_names and root != "workflow"}


class TestCliInit:
    def test_template_walk_skips_bytecode_caches(self, tmp_path: Path) -> None:
        write(tmp_path / "workflow" / "flow.py")
        (tmp_path / "workflow" / "__pycache__").mkdir()
        (tmp_path / "workflow" / "__pycache__" / "flow.cpython-312.pyc").write_bytes(b"")
        (tmp_path / "stray.pyc").write_bytes(b"")
        assert [str(rel) for _, rel in _iter_template_files(tmp_path)] == ["workflow/flow.py"]

    def test_init_creates_project_scaffold(self) -> None:
        out = run_ok("init", "demo-project").stdout
        for phrase in (
            "🌿 ginkgo init demo-project",
            "✓ Initialized project scaffold at",
            "Created:",
            "workflow/flow.py",
            "README.md",
            "ginkgo run --dry-run",
        ):
            assert phrase in out

        project = Path("demo-project")
        for relative in (
            "ginkgo.toml",
            "workflow/__init__.py",
            "workflow/modules/prep.py",
            "workflow/envs/analysis_tools/pixi.toml",
            "workflow/scripts/build_brief.py",
            "skills/project.md",
            "skills/local.md",
            "tests/workflows/smoke.py",
        ):
            assert (project / relative).is_file(), relative
        for absent in ("demo_project", "agents.ginkgo.md", "__init__.py"):
            assert not (project / absent).exists()

        def text(relative: str) -> str:
            return (project / relative).read_text(encoding="utf-8")

        assert "@flow" in text("workflow/flow.py")
        assert "from .modules.analysis import" in text("workflow/flow.py")
        assert "expand(" in text("workflow/flow.py")
        assert "ginkgo run --agent-output" in text("README.md")
        assert "See `skills/index.md`" in text("README.md")
        assert "This project uses Ginkgo" in text("skills/index.md")
        assert "JSONL runtime events" in text("skills/commands.md")
        assert '@task(kind="shell")' in text("skills/workflow-patterns.md")
        assert "gs://bucket/reads.bam" in text("skills/workflow-patterns.md")
        assert "skills/index.md" in text("AGENTS.md")
        # The marimo notebook reads each argument of the task that renders it.
        assert 'args.get("summary-path")' in text("workflow/notebooks/overview.py")
        assert 'args.get("run-label")' in text("workflow/notebooks/overview.py")

        # The manifest installs everything the scaffold imports: Python and
        # notebook task bodies run in the CLI's interpreter and cannot declare env=.
        manifest = tomllib.loads(text("pixi.toml"))
        declared = {
            name.replace("-", "_")
            for name in set(manifest["dependencies"]) | set(manifest["pypi-dependencies"])
        }
        assert manifest["workspace"]["name"] == "demo-project"
        assert {"ginkgo_workflow", "ginkgo_pixi"} <= declared
        # The ginkgo import comes from the ginkgo-workflow distribution.
        imports = {
            "ginkgo_workflow" if name == "ginkgo" else name
            for name in _third_party_imports(project_dir=project)
        }
        assert imports <= declared
        assert manifest["tasks"]["run"] == "ginkgo run"
        # #284: the manifest tracks a branch; the committed pixi.lock pins the commit.
        requirement = manifest["pypi-dependencies"]["ginkgo-workflow"]
        assert (requirement["git"], requirement["branch"]) == (GINKGO_REPO_URL, GINKGO_BRANCH)
        assert "rev" not in requirement and "tag" not in requirement
        patterns = {
            line.strip()
            for line in text(".gitignore").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        assert {".pixi/", ".ginkgo/", "results/", "logs/", "__pycache__/"} <= patterns
        assert not any("lock" in pattern for pattern in patterns)

    def test_templates_only_use_ginkgo_names_this_source_exports(self) -> None:
        """A scaffolded project installs ginkgo from `main`, so its templates must run on it."""
        required = _template_ginkgo_symbols()
        assert required, "no ginkgo symbols found in the templates — the scan is broken"
        missing = sorted(name for name in required if not hasattr(ginkgo, name))
        assert not missing, f"the starter templates use names ginkgo does not export: {missing}"

    @pytest.mark.integration
    def test_init_scaffold_runs_end_to_end(self) -> None:
        for plugin in ("ginkgo_pixi", "ginkgo_notebook"):
            if importlib.util.find_spec(plugin) is None:
                pytest.skip(f"the scaffold needs the {plugin} plugin")
        # NC-5: the starter needs no container runtime, only pixi.
        if shutil.which("pixi") is None:
            pytest.skip("running the scaffold end to end needs pixi")
        run_ok("init", "demo-project")
        # A real process: the scaffold needs the installed plugins, not the test registry.
        result = subprocess.run(
            [sys.executable, "-m", "ginkgo.cli", "run"],
            cwd=Path("demo-project").resolve(),
            check=False,
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, result.stderr
        assert "Rendered pages (1)" in result.stdout

    def test_init_into_the_current_directory_omits_the_cd_step(self) -> None:
        out = run_ok("init", ".").stdout
        assert "Next steps: run ginkgo run --dry-run" in out
        assert "Next steps: cd" not in out

    def test_init_can_skip_skills(self) -> None:
        run_ok("init", "demo-project", "--no-skills")
        assert not Path("demo-project/skills").exists()
        assert not Path("demo-project/AGENTS.md").exists()

    def test_skills_only_adds_skills_to_an_existing_project(self) -> None:
        write("demo-project/workflow/flow.py", "workflow\n")
        write("demo-project/AGENTS.md", "ours\n")
        run_ok("init", "demo-project", "--skills-only")
        assert Path("demo-project/skills/index.md").is_file()
        assert not Path("demo-project/README.md").exists()
        assert Path("demo-project/AGENTS.md").read_text(encoding="utf-8") == "ours\n"

    @pytest.mark.parametrize(
        ("existing", "args"),
        [("skills/index.md", ("--skills-only",)), ("workflow/flow.py", ())],
    )
    def test_init_refuses_to_overwrite_without_force(self, existing: str, args: tuple) -> None:
        path = write(Path("demo-project") / existing, "existing\n")
        result = run_cli("init", "demo-project", *args)
        assert result.returncode == 1
        assert "✖ Refusing to overwrite existing scaffold files without --force:" in result.stderr
        assert path.read_text(encoding="utf-8") == "existing\n"

    def test_init_rejects_incompatible_skills_flags(self) -> None:
        result = run_cli("init", "demo-project", "--skills-only", "--no-skills")
        assert result.returncode == 1
        assert "✖ Cannot combine --no-skills with --skills-only." in result.stderr


class TestCliWorkflowDiscovery:
    @pytest.mark.parametrize(
        ("path", "args", "shown"),
        [
            ("workflow/flow.py", (), r"workflow/flow\.py \(auto-discovered\)"),
            # Discovery keys on the file name, not the directory it sits in.
            ("analysis/flow.py", (), r"analysis/flow\.py \(auto-discovered\)"),
            ("anything.py", ("anything.py",), r"anything\.py"),
        ],
    )
    def test_run_finds_the_workflow(self, path: str, args: tuple, shown: str) -> None:
        write(path, SIMPLE_FLOW)
        result = run_ok("run", *args)
        assert re.search(RUN_HEADER.format(shown), result.stdout)
        assert "✓ succeeded" in result.stdout

    def test_run_reports_what_discovery_looked_for_when_nothing_matches(self) -> None:
        write("workflow.py", SIMPLE_FLOW)
        result = run_cli("run")
        assert result.returncode == 1
        stderr = unwrapped(result.stderr)
        assert "no flow.py was found" in stderr
        assert "Create workflow/flow.py" in stderr and "pass an explicit path" in stderr


class TestCliOutputModes:
    def test_run_verbose_on_one_core_with_a_memory_budget(self) -> None:
        write(
            "workflow.py",
            """
            import time
            from ginkgo import flow, task

            @task()
            def produce() -> str:
                time.sleep(1.2)  # long enough for the resource sampler to take a sample
                return "ok"

            @flow
            def main():
                return produce()
            """,
        )
        out = run_ok("run", "workflow.py", "--verbose", "--cores", "1", "--memory", "32").stdout
        assert re.search(RUN_HEADER.format(r"workflow\.py"), out)
        for phrase in (
            "Running locally on 1 core",
            "🧭 Verbose mode:",
            "config overlays=0",
            "🗂 Run directory:",
            "1/1 complete",
            "🧠 Memory budget: 32 GiB",
            "CPU avg ",
            "RSS avg ",
        ):
            assert phrase in out
        assert "Run Summary" not in out
        manifest = manifest_of(run_dir_of(out))
        assert manifest["memory"] == 32
        assert manifest["resources"]["status"] == "completed"
        assert manifest["resources"]["sample_count"] >= 1


class TestCliMappedLabels:
    def test_fan_outs_label_each_branch_by_its_arguments(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def stats(sample_id: str, read_count: int) -> str:
                return f"{sample_id}:{read_count}"

            @task()
            def train(sample: str, lr: float) -> str:
                return f"{sample}:{lr}"

            @task()
            def tune(sample: str, lr: float, epochs: int) -> str:
                return f"{sample}:{lr}:{epochs}"

            @flow
            def main():
                return [
                    stats().map(sample_id=["sample_a", "sample_b"], read_count=[10, 20]),
                    train().product_map(sample=["a", "b"], lr=[0.01, 0.1]),
                    tune().map(sample=["s"]).product_map(lr=[0.01, 0.1], epochs=[10, 50]),
                ]
            """,
        )
        out = run_ok("run", "workflow.py").stdout
        assert "stats[sample_a]" in out and "stats[sample_b]" in out
        assert "stats[2]" not in out
        for sample in ("a", "b"):
            for lr in ("0.01", "0.1"):
                assert f"train[sample={sample},lr={lr}]" in out
                assert f"tune[s,lr={lr},epochs=10]" in out
        assert "10/10 complete" in out

        listed = run_ok("cache", "ls").stdout
        assert "Label" in listed
        assert sorted(re.findall(r"stats\[\w+\]", listed)) == [
            "stats[sample_a]",
            "stats[sample_b]",
        ]

    def test_dynamic_children_are_recorded_under_their_live_labels(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def child(x: str) -> str:
                return x

            @task()
            def spawn() -> object:
                return [child(x="a"), child(x="b"), child(x="c")]

            @flow
            def main():
                return spawn()
            """,
        )
        result = run_ok("run", "workflow.py")
        live_labels = re.findall(r"│ (child(?:\[\w\])?) +│", result.stdout)
        assert live_labels == ["child[a]", "child[b]", "child[c]"]

        shown = run_ok("runs", "show", run_dir_of(result.stdout).name).stdout
        assert re.findall(r"│ (child(?:\[\w\])?) +│", shown) == live_labels
        rows = json.loads(run_ok("history", "child[b]", "--json").stdout)
        # The spawning task is node 0, so the second child is node 2.
        assert [(row["task_id"], row["display_label"]) for row in rows] == [
            ("task_0002", "child[b]")
        ]

    def test_branches_the_run_never_prepared_keep_their_labels(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def boom() -> str:
                raise ValueError("upstream broke")

            @task()
            def consume(up: str, region: str) -> str:
                return f"{up}:{region}"

            @flow
            def main():
                return consume(up=boom()).map(region=["north", "south"])
            """,
        )
        result = run_cli("run", "workflow.py")
        assert result.returncode == 1
        shown = run_ok("runs", "show", run_dir_of(result.stdout + result.stderr).name).stdout
        assert "consume[north]" in shown and "consume[south]" in shown


class TestCliGridSweepOutputPaths:
    """End-to-end cover for the grid sweep of issue #198."""

    def _write_workflow(self, fan_out: str) -> None:
        write(
            "workflow.py",
            f"""
import json
from pathlib import Path
from ginkgo import expand, file, flow, per_branch, task

TEMPS = [300, 400]
DENS = [0.01, 0.02]

@task()
def simulate(temperature: float, defect_density: float, output_path: str) -> file:
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps({{"t": temperature, "d": defect_density}}))
    return file(output_path)

@flow
def main():
    return simulate().product_map(temperature=TEMPS, defect_density=DENS, output_path={fan_out})
""",
        )

    def test_expand_output_paths_fail_loudly_instead_of_mislabelling_files(self) -> None:
        self._write_workflow('expand("results/{t}_{d}.json", t=TEMPS, d=DENS)')
        result = run_cli("run", "workflow.py")
        assert result.returncode != 0
        assert "output_path" in result.stdout + result.stderr
        assert "per_branch" in result.stdout + result.stderr
        assert not Path("results").exists()

    def test_per_branch_output_paths_write_one_matching_file_per_cell(self) -> None:
        self._write_workflow('per_branch("results/{temperature}_{defect_density}.json")')
        assert "4 tasks executed" in run_ok("run", "workflow.py").stdout
        written = sorted(Path("results").iterdir())
        assert [path.name for path in written] == [
            "300_0.01.json",
            "300_0.02.json",
            "400_0.01.json",
            "400_0.02.json",
        ]
        for path in written:
            payload = json.loads(path.read_text(encoding="utf-8"))
            assert path.name == f"{payload['t']}_{payload['d']}.json"


def test_renderer_label_helpers() -> None:
    assert _time_of_day_spinner(datetime(2026, 3, 12, 9, 0, 0)) == "earth"
    assert _time_of_day_spinner(datetime(2026, 3, 12, 22, 0, 0)) == "moon"
    assert (_core_unit_label(1), _core_unit_label(2)) == ("core", "Cores")
    assert environment_label(None) == "local"
    # Core serves no bare names, so there is no backend label to prefix.
    assert environment_label("bioinfo_tools") == "bioinfo_tools"
    assert environment_label("docker://ubuntu:24.04") == "docker://ubuntu:24.04"

    truncated = _truncate_task_label(
        "extremely_long_alignment_and_qc_summary_task_name[12]", max_width=24
    )
    assert truncated.startswith("extrem") and truncated.endswith("name[12]")
    assert len(truncated) <= 24


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ({"waiting": 4}, lambda plain: plain == " " * 8),
        ({"running": 2, "staging": 1, "cached": 1}, lambda plain: "█" in plain),
    ],
)
def test_grouped_progress_bar_segments(counts: dict, expected) -> None:
    console = Console(width=80, record=True)
    bar = _MultiStateBar(counts=Counter(counts), total=4, width=8)
    (rendered,) = list(bar.__rich_console__(console, console.options))
    assert expected(rendered.plain)


_SECRET_FLOW = """
from ginkgo import flow, secret, task

@task()
def echo_token(token: str) -> str:
    return token

@flow
def main():
    return echo_token(token=secret("API_TOKEN"))
"""


def test_secrets_list_and_validate(monkeypatch: pytest.MonkeyPatch) -> None:
    write("workflow.py", _SECRET_FLOW)
    assert "env:API_TOKEN" in run_ok("secrets", "list", "workflow.py").stdout

    monkeypatch.delenv("API_TOKEN", raising=False)
    missing = run_cli("secrets", "validate", "workflow.py")
    assert missing.returncode == 1 and "env:API_TOKEN" in missing.stdout

    monkeypatch.setenv("API_TOKEN", "ok")
    assert "env:API_TOKEN" in run_ok("secrets", "validate", "workflow.py").stdout


class TestCliAgentAndInspection:
    def test_run_agent_verbose_emits_jsonl_events_and_task_logs(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def produce() -> str:
                print("streamed stdout line")
                return "ok"

            @flow
            def main():
                return produce()
            """,
        )
        result = run_ok("run", "workflow.py", "--agent-output", "--verbose")
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        assert {
            "run_started",
            "run_validated",
            "task_started",
            "task_completed",
            "run_completed",
        } <= {event["event"] for event in events}
        logs = [event for event in events if event["event"] == "task_log"]
        assert any(
            event["stream"] == "stdout" and "streamed stdout line" in event["chunk"]
            for event in logs
        )

    def test_cache_explain_takes_a_positional_a_flag_or_the_latest_run(self) -> None:
        source = """
            from ginkgo import flow, task

            @task(version="{version}")
            def produce(value: str) -> str:
                return value

            @flow
            def main():
                return produce(value="a")
            """
        write("workflow.py", source.format(version="v1"))
        run_ok("run", "workflow.py")
        write("workflow.py", source.format(version="v2"))
        run_id = run_dir_of(run_ok("run", "workflow.py").stdout).name

        positional = json.loads(run_ok("cache", "explain", run_id, "--json").stdout)
        assert positional["run_id"] == run_id
        assert positional["tasks"][0]["reason"] in {
            "version_bump",
            "cache_key_changed",
            "source_hash_changed",
        }
        flag = json.loads(run_ok("cache", "explain", "--run", run_id, "--json").stdout)
        assert flag == positional
        assert json.loads(run_ok("cache", "explain", "--json").stdout) == positional

    @pytest.mark.parametrize(
        ("args", "code", "message"),
        [
            (("run-a", "--run", "run-b"), 2, "Conflicting run ids"),
            # Both forms naming the same run is not a usage error, only a missing run.
            (("run-a", "--run", "run-a"), 1, "Run not found: run-a"),
            ((), 1, "No runs recorded"),
        ],
    )
    def test_cache_explain_rejections(self, args: tuple, code: int, message: str) -> None:
        result = run_cli("cache", "explain", *args)
        assert result.returncode == code
        assert message in result.stderr


_PARAM_WORKFLOW = """
import ginkgo
from pathlib import Path
from ginkgo import file, flow, task

n_reps = ginkgo.param("n_reps", type=int, default=3, help="Number of replicates")
label = ginkgo.param("label", default="base", help="Run label")

@task()
def write_it(n: int, tag: str, output_path: str) -> file:
    out = Path(output_path)
    out.write_text(f"{tag}:{n}", encoding="utf-8")
    return out

@flow
def main():
    return write_it(n=n_reps, tag=label, output_path="result.txt")
"""

# Reads the parameter from a module global inside the task body, which a worker
# subprocess only sees correctly if the run's parameters cross the boundary.
_GLOBAL_PARAM_WORKFLOW = """
import ginkgo
from pathlib import Path
from ginkgo import file, flow, task

tag = ginkgo.param("tag", default="DEFAULT")

@task()
def write_global(output_path: str) -> file:
    out = Path(output_path)
    out.write_text(f"tag={tag}", encoding="utf-8")
    return out

@flow
def main():
    return write_global(output_path="result.txt")
"""

_REQUIRED_PARAM_WORKFLOW = """
import ginkgo
from pathlib import Path
from ginkgo import file, flow, task

region = ginkgo.param("region", help="Genome region")

@task()
def write_region(r: str, output_path: str) -> file:
    out = Path(output_path)
    out.write_text(r, encoding="utf-8")
    return out

@flow
def main():
    return write_region(r=region, output_path="result.txt")
"""

_CONFIG = '[params]\nn_reps = 9\nlabel = "from-config"\n'


def _result() -> str:
    return Path("result.txt").read_text(encoding="utf-8")


class TestCliWorkflowParams:
    @pytest.mark.parametrize(
        ("path", "config", "args", "expected"),
        [
            ("workflow.py", None, ("workflow.py", "--n-reps", "24", "--label", "wide"), "wide:24"),
            ("workflow.py", None, ("workflow.py",), "base:3"),
            ("workflow.py", _CONFIG, ("workflow.py",), "from-config:9"),
            ("workflow.py", _CONFIG, ("workflow.py", "--n-reps", "99"), "from-config:99"),
            ("workflow.py", None, ("workflow.py", "--n-reps", "-2"), "base:-2"),
            # A parameter value must not be captured by the optional workflow positional.
            ("flow.py", None, ("--label", "discovered"), "discovered:3"),
        ],
    )
    def test_parameter_values_cli_over_config_over_default(
        self, path: str, config: str | None, args: tuple, expected: str
    ) -> None:
        write(path, _PARAM_WORKFLOW)
        if config is not None:
            write("ginkgo.toml", config)
        result = run_ok("run", *args)
        assert _result() == expected
        assert "from a module global" not in result.stdout + result.stderr

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (("--nope", "1"), "unrecognized arguments: --nope 1"),
            # Abbreviations are unsupported, and must not be read as parameters.
            (("--job", "1"), "--job"),
            (("--n-reps", "many"), "Cannot read 'many' as int for parameter 'n_reps'"),
            (("--dryrun",), "--dryrun: did you mean --dry-run?"),
            (("--n-rep",), "--n-rep: did you mean --n-reps?"),
        ],
    )
    def test_a_bad_parameter_flag_is_refused(self, args: tuple, message: str) -> None:
        write("workflow.py", _PARAM_WORKFLOW)
        result = run_cli("run", "workflow.py", *args)
        assert result.returncode == 1
        assert message in result.stdout + result.stderr
        assert not Path("result.txt").exists()

    def test_params_and_sources_recorded_in_provenance(self) -> None:
        write("workflow.py", _PARAM_WORKFLOW)
        write("ginkgo.toml", '[params]\nlabel = "cfg"\n')
        manifest = manifest_of(run_dir_of(run_ok("run", "workflow.py", "--n-reps", "7").stdout))
        assert manifest["params"] == {"n_reps": 7, "label": "cfg"}
        assert manifest["param_sources"] == {"n_reps": "cli", "label": "config"}

    def test_required_parameter(self) -> None:
        write("workflow.py", _REQUIRED_PARAM_WORKFLOW)
        missing = run_cli("run", "workflow.py")
        assert missing.returncode == 1
        assert "is required but was not supplied" in missing.stdout + missing.stderr
        assert not Path("result.txt").exists()

        run_ok("run", "workflow.py", "--region", "2L:1-100")
        assert _result() == "2L:1-100"

        # Inspecting or checking a workflow does not need the value.
        payload = json.loads(run_ok("inspect", "workflow", "workflow.py").stdout)
        assert payload["task_count"] == 1
        assert (payload["params"][0]["required"], payload["params"][0]["supplied"]) == (
            True,
            False,
        )
        run_ok("doctor", "workflow.py")

    def test_a_parameter_read_from_a_module_global(self) -> None:
        write("workflow.py", _GLOBAL_PARAM_WORKFLOW)
        result = run_ok("run", "workflow.py", "--tag", "FROM_CLI")
        # The worker sees the CLI value, and the run warns about the pattern.
        assert _result() == "tag=FROM_CLI"
        combined = unwrapped(result.stdout + result.stderr)
        assert "reads parameter 'tag' from a module global" in combined
        assert "Pass it as an argument instead." in combined

        payload = json.loads(run_ok("doctor", "workflow.py", "--json").stdout)
        assert payload["ok"] is True
        codes = {(item["severity"], item["code"]) for item in payload["diagnostics"]}
        assert ("warning", "param_read_from_global") in codes

    def test_parameter_passed_as_task_argument_invalidates_cache(self) -> None:
        write("workflow.py", _PARAM_WORKFLOW)
        run_ok("run", "workflow.py", "--n-reps", "1")
        assert "1 task executed, 0 cached" in run_ok("run", "workflow.py", "--n-reps", "2").stdout
        assert _result() == "base:2"

    def test_inline_equals_and_repeated_values(self) -> None:
        write(
            "workflow.py",
            """
            import ginkgo
            from pathlib import Path
            from ginkgo import file, flow, task

            items = ginkgo.param("items", multiple=True, default=())

            @task()
            def write_items(values: tuple, output_path: str) -> file:
                Path(output_path).write_text(",".join(values), encoding="utf-8")
                return Path(output_path)

            @flow
            def main():
                return write_items(values=items, output_path="result.txt")
            """,
        )
        run_ok("run", "workflow.py", "--items=a", "--items", "b")
        assert _result() == "a,b"

    def test_run_help_lists_parameters_with_their_effective_defaults(self) -> None:
        write("workflow.py", _PARAM_WORKFLOW)
        write("ginkgo.toml", "[params]\nn_reps = 7\n")
        write("extra.toml", '[params]\nlabel = "override"\n')
        out = run_ok("run", "workflow.py", "--config", "extra.toml", "--help").stdout
        assert "parameters declared by workflow.py" in out
        assert "--n-reps INT" in out and "--label STR" in out
        assert "Number of replicates (default: 7, from ginkgo.toml)" in out
        assert "Run label (default: 'override', from extra.toml)" in out

        write("flow.py", _PARAM_WORKFLOW)
        out = run_ok("run", "--help").stdout
        assert "parameters declared by flow.py (auto-discovered):" in out
        assert "--n-reps INT" in out

        # An unimportable workflow still gets the usage text.
        write("broken.py", "import nonexistent_module_xyz\n")
        out = run_ok("run", "broken.py", "--help").stdout
        assert "--dry-run" in out and "Could not import broken.py" in out

    def test_run_help_keeps_bracketed_text_verbatim(self) -> None:
        text = unwrapped(run_ok("run", "--help").stdout)
        assert "[workflow]" in text and "[remote.executors]" in text and "[remote.k8s]" in text

        write(
            "workflow.py",
            """
            import ginkgo
            from ginkgo import flow, task

            n = ginkgo.param("n", type=int, default=1, help="number [/x] of [bold] things")
            tag = ginkgo.param("tag", default="[red]")

            @task()
            def echo(n: int, tag: str) -> str:
                return f"{tag}{n}"

            @flow
            def main():
                return echo(n=n, tag=tag)
            """,
        )
        out = run_ok("run", "workflow.py", "--help").stdout
        assert "number [/x] of [bold] things" in out and "(default: '[red]')" in out

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (("cache", "ls", "--bogus"), "unrecognized arguments: --bogus"),
            (("runs", "ls", "--limt", "5"), "--limt: did you mean --limit?"),
        ],
    )
    def test_other_commands_still_reject_unknown_flags(self, args: tuple, message: str) -> None:
        result = run_cli(*args)
        assert result.returncode == 2
        assert message in result.stderr

    def test_inspect_workflow_exposes_declared_parameters(self) -> None:
        write("workflow.py", _PARAM_WORKFLOW)
        payload = json.loads(run_ok("inspect", "workflow", "workflow.py").stdout)
        n_reps, label = sorted(payload["params"], key=lambda item: item["name"] != "n_reps")
        assert (n_reps["type"], n_reps["default"], n_reps["flag"]) == ("int", 3, "--n-reps")
        assert label["help"] == "Run label"

    @pytest.mark.parametrize(
        ("base", "override", "expected"),
        [
            # A base key absent from every override must survive the merge.
            ('kept = "base"\nreplaced = "base"\n', 'replaced = "override"\n', "base/override"),
            # An override setting one parameter must not drop the others in [params].
            (
                '[params]\nkept = "base"\nreplaced = "base"\n',
                '[params]\nreplaced = "override"\n',
                "base/override",
            ),
        ],
    )
    def test_config_override_layers_over_project_config(
        self, base: str, override: str, expected: str
    ) -> None:
        write("ginkgo.toml", base)
        write("override.toml", override)
        source = "cfg[{0!r}]" if not base.startswith("[params]") else "ginkgo.param({0!r})"
        write(
            "workflow.py",
            f"""
import ginkgo
from pathlib import Path
from ginkgo import file, flow, task

cfg = ginkgo.config("ginkgo.toml")
kept = {source.format("kept")}
replaced = {source.format("replaced")}

@task()
def write_both(kept: str, replaced: str, output_path: str) -> file:
    Path(output_path).write_text(f"{{kept}}/{{replaced}}", encoding="utf-8")
    return Path(output_path)

@flow
def main():
    return write_both(kept=kept, replaced=replaced, output_path="result.txt")
""",
        )
        run_ok("run", "workflow.py", "--config", "override.toml")
        assert _result() == expected

    @pytest.mark.parametrize(
        ("config", "lookup", "args", "phrases"),
        [
            # --config supplies values, not the file the workflow asked for.
            ("typo.toml", '["replaced"]', ("--config", "override.toml"), ["typo.toml"]),
            # The report names the section and the keys, and keeps the file:line.
            (
                "ginkgo.toml",
                '["qc"]["min_lenght"]',
                (),
                [
                    "'min_lenght' is not a key in [qc] of ginkgo.toml",
                    "Available: min_length, min_quality",
                    "ConfigKeyError at",
                    "workflow.py:12 in main",
                ],
            ),
        ],
    )
    def test_a_config_lookup_that_cannot_succeed_is_reported(
        self, config: str, lookup: str, args: tuple, phrases: list[str]
    ) -> None:
        write("ginkgo.toml", "[qc]\nmin_length = 50\nmin_quality = 20\n")
        write("override.toml", 'replaced = "from-override"\n')
        write(
            "workflow.py",
            f"""
import ginkgo
from ginkgo import flow, task

cfg = ginkgo.config("{config}")

@task()
def echo(value: str) -> str:
    return value

@flow
def main():
    return echo(value=cfg{lookup})
""",
        )
        result = run_cli("run", "workflow.py", *args)
        assert result.returncode == 1
        output = unwrapped(result.stdout + result.stderr)
        assert all(phrase in output for phrase in phrases)
        assert "Traceback" not in output

    def test_recorded_params_are_the_effective_values_only(self) -> None:
        """The raw [params] table must not sit beside the values that superseded it."""
        write(
            "workflow.py",
            """
            import ginkgo
            from ginkgo import flow, task

            cfg = ginkgo.config("ginkgo.toml")
            label = ginkgo.param("label", default="base")

            @task()
            def echo(tag: str, other: str) -> str:
                return f"{tag}/{other}"

            @flow
            def main():
                return echo(tag=label, other=cfg["other"])
            """,
        )
        write(
            "ginkgo.toml",
            'other = "kept"\n\n[params]\nlabel = "stale"\n\n[secrets.dotenv]\nenabled = false\n',
        )
        result = run_ok("run", "workflow.py", "--label", "effective")
        params = manifest_of(run_dir_of(result.stdout))["params"]
        assert (params["label"], params["other"]) == ("effective", "kept")
        # Neither the raw [params] table nor ginkgo's own tables are parameters.
        assert "params" not in params and "secrets" not in params


_FAILURE_POLICY_WORKFLOW = """
from pathlib import Path
from ginkgo import flow, task

@task(on_failure="ignore")
def load(sample: str) -> str:
    if sample == "bad":
        raise RuntimeError("malformed input")
    return sample

@task()
def analyse(sample: str) -> str:
    Path(f"{sample}.done").write_text("analysed", encoding="utf-8")
    return sample

@flow
def main():
    return [analyse(sample=load(sample=name)) for name in ("good", "bad")]
"""

_FAIL_FAST_WORKFLOW = """
from ginkgo import flow, task

@task()
def load(sample: str) -> str:
    raise RuntimeError("malformed input")

@task()
def analyse(sample: str) -> str:
    return sample

@flow
def main():
    return analyse(sample=load(sample="bad"))
"""


class TestFailureOutput:
    def test_identical_branch_failures_share_one_panel(self) -> None:
        write(
            "workflow.py",
            """
            from ginkgo import flow, task

            @task()
            def clean(region: str) -> str:
                raise ValueError("malformed region")

            @flow
            def main():
                return clean().map(region=["north", "south", "east"])
            """,
        )
        # One slot per branch, so all three fail whatever the CPU count.
        result = run_cli("run", "workflow.py", "--jobs", "3")
        assert result.returncode == 1
        assert result.stdout.count("Failure Details:") == 1
        assert "(×3)" in result.stdout and "Exit code" not in result.stdout

    def test_a_fatal_failure_exits_one_and_logs_from_the_users_frame(self) -> None:
        write("workflow.py", _FAIL_FAST_WORKFLOW)
        result = run_cli("run", "workflow.py")
        assert result.returncode == 1
        assert "malformed input" in result.stdout + result.stderr

        (log_path,) = (run_dir_of(result.stderr) / "logs").glob("*load*stderr.log")
        # Piped output prints the log path whole.
        assert str(log_path) in result.stdout
        frames = [
            line
            for line in log_path.read_text(encoding="utf-8").splitlines()
            if line.lstrip().startswith("File ")
        ]
        assert frames and "workflow.py" in frames[0]
        assert not any("ginkgo/runtime/worker.py" in frame for frame in frames)

    def test_a_task_notice_names_the_task_and_the_parameter(self) -> None:
        write(
            "workflow.py",
            """
            from pathlib import Path
            from ginkgo import flow, task

            @task()
            def append(log_path: str) -> str:
                Path(log_path).write_text("line", encoding="utf-8")
                return log_path

            @flow
            def main():
                return append(log_path="run.log")
            """,
        )
        write("run.log")
        text = unwrapped(run_ok("run", "workflow.py").stdout)
        assert "append: wrote " in text and "(parameter 'log_path')" in text
        assert "Annotate 'log_path' `Out[file]`" in text

    def test_an_ignored_failure_exits_three_and_reports_the_skips(self) -> None:
        write("workflow.py", _FAILURE_POLICY_WORKFLOW)
        result = run_cli("run", "workflow.py")
        assert result.returncode == 3, result.stderr
        assert Path("good.done").exists()
        combined = result.stdout + result.stderr
        for phrase in (
            "1 failed (1 ignored), 1 skipped",
            "Failed, run continued (1)",
            "Left unrun by a failure (1)",
            "blocked by load",
        ):
            assert phrase in combined

        run_dir = run_dir_of(combined)
        manifest = manifest_of(run_dir)
        tasks = manifest["tasks"].values()
        assert manifest["status"] == "failed"
        assert Counter(task["status"] for task in tasks) == Counter(
            {"succeeded": 2, "failed": 1, "skipped": 1}
        )
        skipped = next(task for task in tasks if task["status"] == "skipped")
        ignored = next(task for task in tasks if task["status"] == "failed")
        assert skipped["skipped_because"]["task_name"].endswith(".load")
        assert ignored["ignored"] is True

        debug = run_ok("debug", run_dir.name).stdout
        assert "the run continued past this failure" in debug

    def test_keep_going_is_a_ginkgo_flag_not_a_workflow_parameter(self) -> None:
        write("workflow.py", _FAIL_FAST_WORKFLOW)
        result = run_cli("run", "workflow.py", "--keep-going")
        assert result.returncode == 3, result.stderr
        combined = result.stdout + result.stderr
        assert "unrecognized arguments" not in combined
        assert "1 failed (1 ignored), 1 skipped" in combined
