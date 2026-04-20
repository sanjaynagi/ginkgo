"""Seed synthetic benchmark data into a GCS bucket.

Generates three fixtures that exercise distinct remote-read patterns:

- ``sparse.bin``     — random binary, read 100 random 64 KiB windows
- ``sequential.bin`` — random binary, read whole file
- ``tabular.parquet``— 20-column parquet, project 2 columns

Uploads to ``gs://<bucket>/<prefix>/`` via ``gcloud storage cp``. The sparse
and sequential fixtures are streamed straight from ``/dev/urandom`` without
touching local disk; the parquet fixture is staged to a temp path first.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path


def run(cmd: list[str], *, stdin: int | None = None) -> None:
    """Run ``cmd``, echoing it, and raise on non-zero exit."""
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, stdin=stdin, check=True)


def stream_random_to_gcs(*, size_mib: int, destination: str) -> None:
    """Pipe ``/dev/urandom`` → ``gcloud storage cp`` for a sized random blob."""
    dd = subprocess.Popen(
        ["dd", "if=/dev/urandom", "bs=1M", f"count={size_mib}", "status=progress"],
        stdout=subprocess.PIPE,
    )
    assert dd.stdout is not None
    try:
        run(["gcloud", "storage", "cp", "-", destination], stdin=dd.stdout.fileno())
    finally:
        dd.stdout.close()
        dd.wait()
    if dd.returncode != 0:
        raise RuntimeError(f"dd exited {dd.returncode}")


def build_parquet(*, path: Path, target_mib: int, columns: int = 20) -> None:
    """Write a parquet with ``columns`` float columns sized near ``target_mib``."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Each float64 column ~ 8 bytes * rows. Fix rows to hit target size.
    bytes_per_row = columns * 8
    rows = (target_mib * 1024 * 1024) // bytes_per_row
    print(f"parquet: {rows} rows × {columns} cols → ~{target_mib} MiB", flush=True)

    rng = np.random.default_rng(seed=42)
    chunk_rows = 1_000_000
    writer: pq.ParquetWriter | None = None
    try:
        written = 0
        while written < rows:
            take = min(chunk_rows, rows - written)
            arrays = [pa.array(rng.random(take)) for _ in range(columns)]
            names = [f"c{i}" for i in range(columns)]
            table = pa.table(arrays, names=names)
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression="snappy")
            writer.write_table(table)
            written += take
            print(f"  wrote {written}/{rows}", flush=True)
    finally:
        if writer is not None:
            writer.close()


def main() -> int:
    """Parse args and seed all three fixtures."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True, help="e.g. ginkgo-phase9-benchmarks-f02462a0")
    parser.add_argument("--prefix", default="1gb", help="path prefix inside the bucket")
    parser.add_argument("--size-mib", type=int, default=1024, help="size of each random blob")
    parser.add_argument("--parquet-mib", type=int, default=1024, help="target parquet file size")
    parser.add_argument(
        "--skip",
        default="",
        help="comma-separated: any of sparse,sequential,tabular",
    )
    args = parser.parse_args()

    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    base = f"gs://{args.bucket}/{args.prefix}"

    if "sparse" not in skip:
        print(f"=== sparse.bin ({args.size_mib} MiB) ===")
        stream_random_to_gcs(size_mib=args.size_mib, destination=f"{base}/sparse.bin")

    if "sequential" not in skip:
        print(f"=== sequential.bin ({args.size_mib} MiB) ===")
        stream_random_to_gcs(size_mib=args.size_mib, destination=f"{base}/sequential.bin")

    if "tabular" not in skip:
        print(f"=== tabular.parquet (~{args.parquet_mib} MiB) ===")
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            build_parquet(path=tmp_path, target_mib=args.parquet_mib)
            print(f"  {tmp_path.stat().st_size / (1024 * 1024):.1f} MiB")
            run(["gcloud", "storage", "cp", str(tmp_path), f"{base}/tabular.parquet"])
        finally:
            tmp_path.unlink(missing_ok=True)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
