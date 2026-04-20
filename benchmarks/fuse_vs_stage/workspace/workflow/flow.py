"""FUSE-vs-stage benchmark workflow.

Three tasks, each exercising a distinct remote-read pattern against the
same remote fixture. Access mode (``stage`` or ``fuse``) is selected
per run via the ``GINKGO_FUSE_BENCH_ACCESS`` env var so the same flow
code can be submitted back-to-back without modification.

Fixtures expected under
``gs://<GINKGO_FUSE_BENCH_BUCKET>/<GINKGO_FUSE_BENCH_PREFIX>/``:

- ``sparse.bin``     — random binary, read 100 random 64 KiB windows
- ``sequential.bin`` — random binary, read whole file
- ``tabular.parquet``— 20-col parquet, project 2 columns
"""

from __future__ import annotations

import os
import random
import time
from pathlib import Path

from ginkgo import flow, remote_file, task
from ginkgo.core.types import file


ACCESS = os.environ.get("GINKGO_FUSE_BENCH_ACCESS", "stage")
BUCKET = os.environ.get("GINKGO_FUSE_BENCH_BUCKET", "ginkgo-phase9-benchmarks-f02462a0")
PREFIX = os.environ.get("GINKGO_FUSE_BENCH_PREFIX", "1gb")
BASE = f"gs://{BUCKET}/{PREFIX}"


@task(remote=True, remote_input_access=ACCESS, streaming_compatible=True)
def read_sparse(*, data: file) -> dict:
    """Read 100 random 64 KiB windows from ``data``."""
    size = Path(data).stat().st_size
    window = 64 * 1024
    count = 100
    rng = random.Random(42)
    offsets = [rng.randrange(0, max(1, size - window)) for _ in range(count)]

    start = time.perf_counter()
    total = 0
    with open(data, "rb") as handle:
        for off in offsets:
            handle.seek(off)
            buf = handle.read(window)
            total += len(buf)
    elapsed = time.perf_counter() - start
    return {
        "pattern": "sparse",
        "file_size": size,
        "windows": count,
        "window_bytes": window,
        "bytes_read": total,
        "elapsed_seconds": elapsed,
    }


@task(remote=True, remote_input_access=ACCESS, streaming_compatible=True)
def read_sequential(*, data: file) -> dict:
    """Read ``data`` sequentially end-to-end."""
    size = Path(data).stat().st_size
    start = time.perf_counter()
    total = 0
    with open(data, "rb") as handle:
        while True:
            chunk = handle.read(4 * 1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
    elapsed = time.perf_counter() - start
    return {
        "pattern": "sequential",
        "file_size": size,
        "bytes_read": total,
        "elapsed_seconds": elapsed,
    }


@task(remote=True, remote_input_access=ACCESS, streaming_compatible=True)
def read_tabular(*, data: file) -> dict:
    """Project 2 of 20 columns from a parquet file."""
    import pyarrow.parquet as pq

    size = Path(data).stat().st_size
    start = time.perf_counter()
    table = pq.read_table(str(data), columns=["c0", "c1"])
    rows = table.num_rows
    elapsed = time.perf_counter() - start
    return {
        "pattern": "tabular",
        "file_size": size,
        "rows": rows,
        "columns_read": 2,
        "bytes_read": int(table.nbytes),
        "elapsed_seconds": elapsed,
    }


@flow
def fuse_vs_stage_bench() -> dict:
    """Run all three access-pattern probes against their fixtures."""
    sparse = read_sparse(data=remote_file(f"{BASE}/sparse.bin", access=ACCESS))
    sequential = read_sequential(data=remote_file(f"{BASE}/sequential.bin", access=ACCESS))
    tabular = read_tabular(data=remote_file(f"{BASE}/tabular.parquet", access=ACCESS))
    return {
        "access": ACCESS,
        "sparse": sparse,
        "sequential": sequential,
        "tabular": tabular,
    }
