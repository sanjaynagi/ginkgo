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
            trust_mtimes=True,
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

    summary = _latest_run()
    return {
        "access": access,
        "wall_time_seconds": wall,
        "exit_code": rc,
        "run_dir": str(summary.run_dir),
        **_parse_run(summary=summary),
    }


def _latest_run():
    """Return the newest run in the benchmark workspace, from the ledger."""
    import ginkgo.query as ginkgo_query
    from ginkgo.workspace_layout import WorkspaceLayout

    with ginkgo_query.open(WorkspaceLayout(root=WORKSPACE / ".ginkgo")) as store:
        run_id = store.latest_run_id()
        if run_id is None:
            raise FileNotFoundError(f"no runs recorded in {WORKSPACE}")
        return store.run(run_id)


def _decode_ginkgo_value(value: Any) -> Any:
    """Decode the Ginkgo-encoded dict/list wrapper used in cache outputs."""
    if isinstance(value, dict) and value.get("__ginkgo_type__") == "dict":
        return {item["key"]: _decode_ginkgo_value(item["value"]) for item in value["items"]}
    if isinstance(value, dict) and value.get("__ginkgo_type__") == "list":
        return [_decode_ginkgo_value(v) for v in value["items"]]
    if isinstance(value, list):
        return [_decode_ginkgo_value(v) for v in value]
    return value


def _parse_run(*, summary: Any) -> dict[str, Any]:
    """Extract per-task benchmark measurements from one run.

    Combines two sources:

    - the ledger, for run status, cache keys, the ``task_started`` to
      ``task_completed`` span (which encloses the full remote lifecycle on the
      worker: staging or mount, image pull, body, teardown), and the recorded
      ``remote_input_access`` policy.
    - ``.ginkgo/cache/<cache_key>/output.json`` for the benchmark payload
      (``elapsed_seconds`` covers only the Python read loop).
    """
    cache_root = WORKSPACE / ".ginkgo" / "cache"
    tasks: dict[str, dict[str, Any]] = {}
    for task in summary.tasks:
        payload: Any = None
        if task.cache_key:
            output_path = cache_root / task.cache_key / "output.json"
            if output_path.is_file():
                payload = _decode_ginkgo_value(json.loads(output_path.read_text()))

        tasks[task.base_name] = {
            "task_id": task.task_key,
            "cache_key": task.cache_key,
            "status": task.status,
            "task_wall_seconds": task.duration_s,
            "payload": payload,
            "remote_input_access": _remote_access(summary=summary, task_id=task.task_key),
        }

    return {"status": summary.status, "tasks": tasks}


def _remote_access(*, summary: Any, task_id: str) -> dict[str, Any]:
    """Return one task's recorded remote-input access statistics."""
    for event in summary_events(summary=summary, task_id=task_id):
        fields = event.payload.get("fields") or {}
        if "remote_input_access" in fields:
            return fields["remote_input_access"] or {}
    return {}


def summary_events(*, summary: Any, task_id: str) -> Any:
    """Yield the annotations recorded against one task."""
    import ginkgo.query as ginkgo_query
    from ginkgo.workspace_layout import WorkspaceLayout

    with ginkgo_query.open(WorkspaceLayout(root=WORKSPACE / ".ginkgo")) as store:
        return [
            event
            for event in store.events(summary.run_id, types=["task_annotated"])
            if event.task_id == task_id
        ]


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
