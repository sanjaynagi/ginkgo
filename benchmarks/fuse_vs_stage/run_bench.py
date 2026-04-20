"""Run the FUSE-vs-stage remote-input-access benchmark on GCP Batch.

Invokes the benchmark flow twice — once with
``GINKGO_FUSE_BENCH_ACCESS=stage`` and once with ``=fuse`` — using the
same bucket, prefix, and workspace. After each run, parses the
resulting provenance (manifest + events + cache/output.json) to
extract per-task timing and bytes read, then writes a combined JSON
result under ``benchmarks/results/fuse_vs_stage/<timestamp>.json``.

The workspace at ``benchmarks/fuse_vs_stage/workspace/`` must contain
a ``workflow/flow.py`` defining the benchmark flow and a
``ginkgo.toml`` configured with ``[remote.batch]`` pointing at the
project's privileged fuse image.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE = REPO_ROOT / "benchmarks" / "fuse_vs_stage" / "workspace"
WORKFLOW = WORKSPACE / "workflow" / "flow.py"
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results" / "fuse_vs_stage"


def run_once(*, access: str, bucket: str, prefix: str, executor: str) -> dict[str, Any]:
    """Run the benchmark flow once under the given access policy.

    Returns a dict with top-level keys:
    - ``access``: the requested access mode
    - ``wall_time_seconds``: total wall-clock time for ``run_workflow``
    - ``status``: manifest status
    - ``tasks``: mapping task name → {elapsed, bytes_read, file_size,
      access_policy, stage_bytes, mount_ok}
    """
    from ginkgo.cli.commands.run import run_workflow
    from ginkgo.cli.common import RUNS_ROOT

    env_backup = {
        k: os.environ.get(k)
        for k in (
            "GINKGO_FUSE_BENCH_ACCESS",
            "GINKGO_FUSE_BENCH_BUCKET",
            "GINKGO_FUSE_BENCH_PREFIX",
        )
    }
    os.environ["GINKGO_FUSE_BENCH_ACCESS"] = access
    os.environ["GINKGO_FUSE_BENCH_BUCKET"] = bucket
    os.environ["GINKGO_FUSE_BENCH_PREFIX"] = prefix

    original_cwd = Path.cwd()
    os.chdir(WORKSPACE)
    start = time.perf_counter()
    try:
        rc = run_workflow(
            workflow_path=WORKFLOW,
            config_paths=[],
            jobs=None,
            cores=None,
            memory=None,
            dry_run=False,
            output_mode="default",
            trust_workspace=True,
            profile=False,
            executor=executor,
        )
        wall = time.perf_counter() - start
    finally:
        os.chdir(original_cwd)
        for k, v in env_backup.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    run_dir = _latest_run_dir(WORKSPACE / RUNS_ROOT)
    return {
        "access": access,
        "wall_time_seconds": wall,
        "exit_code": rc,
        "run_dir": str(run_dir),
        **_parse_run(run_dir=run_dir),
    }


def _latest_run_dir(runs_root: Path) -> Path:
    """Return the most-recently-modified run directory under ``runs_root``."""
    if not runs_root.is_dir():
        raise FileNotFoundError(f"no runs at {runs_root}")
    entries = [p for p in runs_root.iterdir() if p.is_dir()]
    if not entries:
        raise FileNotFoundError(f"no runs in {runs_root}")
    return max(entries, key=lambda p: p.stat().st_mtime)


def _decode_ginkgo_value(value: Any) -> Any:
    """Decode the Ginkgo-encoded dict/list wrapper used in cache outputs."""
    if isinstance(value, dict) and value.get("__ginkgo_type__") == "dict":
        return {item["key"]: _decode_ginkgo_value(item["value"]) for item in value["items"]}
    if isinstance(value, dict) and value.get("__ginkgo_type__") == "list":
        return [_decode_ginkgo_value(v) for v in value["items"]]
    if isinstance(value, list):
        return [_decode_ginkgo_value(v) for v in value]
    return value


def _parse_iso(ts: str) -> float:
    """Parse a Ginkgo event ISO timestamp into a float POSIX time."""
    return datetime.fromisoformat(ts).timestamp()


def _parse_run(*, run_dir: Path) -> dict[str, Any]:
    """Extract per-task benchmark measurements from a run directory.

    Combines three sources:

    - ``manifest.yaml`` for the top-line run status and ``remote_input_access``
      policy recorded via :py:meth:`update_task_extra`.
    - ``events.jsonl`` for per-task ``task_started`` / ``task_completed``
      timestamps, which enclose the full remote lifecycle on the worker
      (staging or mount, image pull, body, teardown).
    - ``.ginkgo/cache/<cache_key>/output.json`` for the benchmark payload
      (``elapsed_seconds`` covers only the Python read loop).
    """
    manifest = yaml.safe_load((run_dir / "manifest.yaml").read_text())
    status = manifest.get("status", "unknown")
    task_entries = manifest.get("tasks", {}) or {}

    events = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    cache_keys = {
        e["task_id"]: e["cache_key"]
        for e in events
        if e.get("event") in {"task_cache_miss", "task_cache_hit"} and e.get("cache_key")
    }
    started_at: dict[str, float] = {}
    completed_at: dict[str, float] = {}
    for e in events:
        ev = e.get("event")
        ts = e.get("ts")
        if not ts:
            continue
        if ev == "task_started":
            started_at[e["task_id"]] = _parse_iso(ts)
        elif ev == "task_completed":
            completed_at[e["task_id"]] = _parse_iso(ts)

    cache_root = WORKSPACE / ".ginkgo" / "cache"
    tasks: dict[str, dict[str, Any]] = {}
    for task_id, info in task_entries.items():
        task_name = info.get("task", task_id)
        short_name = task_name.rsplit(".", 1)[-1]
        key = cache_keys.get(task_id)
        payload: Any = None
        if key:
            output_path = cache_root / key / "output.json"
            if output_path.is_file():
                payload = _decode_ginkgo_value(json.loads(output_path.read_text()))

        task_wall = None
        if task_id in started_at and task_id in completed_at:
            task_wall = completed_at[task_id] - started_at[task_id]

        remote_access = info.get("remote_input_access") or {}
        tasks[short_name] = {
            "task_id": task_id,
            "cache_key": key,
            "status": info.get("status"),
            "task_wall_seconds": task_wall,
            "payload": payload,
            "remote_input_access": remote_access,
        }

    return {"status": status, "tasks": tasks}


def main() -> int:
    """Parse args, run fuse + stage, dump combined results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default="ginkgo-phase9-benchmarks-f02462a0")
    parser.add_argument("--prefix", default="1gb")
    parser.add_argument("--executor", default="batch", choices=["batch", "k8s", "local"])
    parser.add_argument(
        "--modes",
        default="stage,fuse",
        help="comma-separated access modes to run (default: stage,fuse)",
    )
    parser.add_argument("--output", default=None, help="override output json path")
    args = parser.parse_args()

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]

    results: dict[str, Any] = {
        "bucket": args.bucket,
        "prefix": args.prefix,
        "executor": args.executor,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "runs": {},
    }

    for mode in modes:
        print(f"\n=== running access={mode} ===\n", flush=True)
        results["runs"][mode] = run_once(
            access=mode,
            bucket=args.bucket,
            prefix=args.prefix,
            executor=args.executor,
        )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(args.output) if args.output else RESULTS_DIR / f"{stamp}_{args.prefix}.json"
    out_path.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nWrote {out_path}")

    _print_summary(results=results)
    return 0


def _print_summary(*, results: dict[str, Any]) -> None:
    """Print a head-to-head comparison across modes with prep / read split.

    ``task_wall`` captures the full remote task lifecycle (VM boot +
    image pull + stage-download or fuse-mount + body + teardown).
    ``read_seconds`` is the pure in-body read loop.
    ``prep`` = wall − read, i.e. everything the task paid before it got
    to read bytes.
    """
    runs = results["runs"]
    patterns = ("sparse", "sequential", "tabular")
    mode_tasks = {
        mode: {name: info for name, info in run["tasks"].items()} for mode, run in runs.items()
    }

    print("\n=== summary (per-task) ===")
    header = f"{'pattern':<12}{'mode':<8}{'wall':>8}{'prep':>8}{'read':>8}{'bytes_read':>14}"
    print(header)
    for pattern in patterns:
        task_name = f"read_{pattern}"
        for mode in runs:
            info = mode_tasks.get(mode, {}).get(task_name)
            if not info:
                continue
            payload = info.get("payload") or {}
            wall = info.get("task_wall_seconds") or 0.0
            read = payload.get("elapsed_seconds", 0.0)
            prep = max(0.0, wall - read)
            bytes_read = int(payload.get("bytes_read") or 0)
            print(f"{pattern:<12}{mode:<8}{wall:>8.1f}{prep:>8.1f}{read:>8.2f}{bytes_read:>14,}")

    if "stage" in runs and "fuse" in runs:
        print("\n=== fuse vs stage (lower = faster) ===")
        print(
            f"{'pattern':<12}{'stage_wall':>12}{'fuse_wall':>12}{'wall_speedup':>14}"
            f"{'stage_read':>12}{'fuse_read':>12}{'read_speedup':>14}"
        )
        for pattern in patterns:
            task_name = f"read_{pattern}"
            s_info = mode_tasks.get("stage", {}).get(task_name)
            f_info = mode_tasks.get("fuse", {}).get(task_name)
            if not s_info or not f_info:
                continue
            s_wall = s_info.get("task_wall_seconds") or 0.0
            f_wall = f_info.get("task_wall_seconds") or 0.0
            s_read = (s_info.get("payload") or {}).get("elapsed_seconds", 0.0)
            f_read = (f_info.get("payload") or {}).get("elapsed_seconds", 0.0)
            ws = s_wall / f_wall if f_wall else float("inf")
            rs = s_read / f_read if f_read else float("inf")
            print(
                f"{pattern:<12}{s_wall:>12.1f}{f_wall:>12.1f}{ws:>13.2f}x"
                f"{s_read:>12.2f}{f_read:>12.2f}{rs:>13.2f}x"
            )


if __name__ == "__main__":
    sys.exit(main())
