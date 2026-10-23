"""A run on a real Kubernetes cluster: kind, with MinIO as the object store.

The driver runs here and dispatches a task to a pod. The pod reads its input
straight from MinIO, and its output comes back through the artifact store.
CI's ``kind`` job creates the cluster, loads the worker image and
port-forwards MinIO (see ``.github/workflows/tests.yml``); elsewhere the test
is skipped.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("GINKGO_KIND") != "1", reason="needs the kind cluster"),
]

CONFIG = """
[remote.executors.kind]
type = "k8s"
image = "ginkgo-worker:kind"
namespace = "default"
pull_policy = "Never"
ephemeral_storage = "1Gi"
unschedulable_timeout = 60.0
fuse_annotations = {}

[remote.executors.kind.env]
AWS_ENDPOINT_URL = "http://minio.default.svc.cluster.local:9000"
AWS_ACCESS_KEY_ID = "minioadmin"
AWS_SECRET_ACCESS_KEY = "minioadmin"  # pragma: allowlist secret
AWS_DEFAULT_REGION = "us-east-1"

[remote.executors.kind.code]
mode = "sync"
package = "kind_workflow"

[remote.artifacts]
store = "s3://ginkgo/artifacts/"
"""

WORKFLOW = """
from pathlib import Path

from ginkgo import file, flow, remote_file, task


@task(executor="kind")
def shout(*, text: file) -> file:
    Path("shouted.txt").write_text(Path(text).read_text().upper())
    return file("shouted.txt")


@task()
def keep(*, shouted: file) -> str:
    Path("result.txt").write_text(Path(shouted).read_text())
    return "result.txt"


@flow
def main():
    return keep(shouted=shout(text=remote_file("s3://ginkgo/inputs/greeting.txt")))
"""


def _put_input() -> None:
    """Create the bucket and the task's input in MinIO."""
    import s3fs

    fs = s3fs.S3FileSystem()
    if not fs.exists("ginkgo"):
        fs.mkdir("ginkgo")
    fs.pipe("ginkgo/inputs/greeting.txt", b"hello from minio")


def test_a_task_runs_in_a_pod_and_its_output_comes_back(tmp_path: Path) -> None:
    _put_input()
    (tmp_path / "ginkgo.toml").write_text(CONFIG, encoding="utf-8")
    package = tmp_path / "kind_workflow"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "flow.py").write_text(WORKFLOW, encoding="utf-8")
    run_id = uuid.uuid4().hex

    result = subprocess.run(
        [sys.executable, "-m", "ginkgo.cli", "run", "kind_workflow/flow.py"],
        cwd=tmp_path,
        env={**os.environ, "GINKGO_PARENT_RUN_ID": run_id},
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "result.txt").read_text() == "HELLO FROM MINIO"
    jobs = subprocess.run(
        ["kubectl", "get", "jobs", "-l", f"ginkgo/parent-run-id={run_id}", "-o", "name"],
        text=True,
        capture_output=True,
        check=True,
    )
    assert jobs.stdout.strip(), "the task never ran as a Job"
