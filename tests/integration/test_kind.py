"""A run on a real Kubernetes cluster: kind, with an in-cluster S3 server.

The driver runs here and dispatches tasks to pods: a Python task, a shell
task whose command runs in a busybox image beside the worker, and a shell task
that installs a Pixi env from the code bundle and runs its command in it. The
first two read their input straight from that server, and every output comes
back through the artifact store. CI's ``kind`` job creates the cluster, loads
the worker and busybox images and port-forwards the server (see
``.github/workflows/tests.yml``); elsewhere the test is skipped.
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
AWS_ENDPOINT_URL = "http://s3.default.svc.cluster.local:5000"
AWS_ACCESS_KEY_ID = "testing"
AWS_SECRET_ACCESS_KEY = "testing"  # pragma: allowlist secret
AWS_DEFAULT_REGION = "us-east-1"

[remote.executors.kind.code]
mode = "sync"
package = "kind_workflow"

[remote.artifacts]
store = "s3://ginkgo/artifacts/"
"""

WORKFLOW = """
from pathlib import Path

from ginkgo import Out, file, flow, remote_file, shell, task

GREETING = "s3://ginkgo/inputs/greeting.txt"


@task(executor="kind")
def shout(*, text: file) -> file:
    Path("shouted.txt").write_text(Path(text).read_text().upper())
    return file("shouted.txt")


@task()
def keep(*, shouted: file) -> str:
    Path("result.txt").write_text(Path(shouted).read_text())
    return "result.txt"


# jq is in the env, not the worker image.
@task(kind="shell", env="tools", executor="kind")
def jq_version(*, out: Out[file]):
    return shell(cmd=f"jq --version > {out}")


# Its command runs in the bundle's copy of the project, as it would locally.
@task(kind="shell", env="docker://busybox:1.36", executor="kind")
def shout_in_busybox(*, text: file, shouted: Out[file]):
    return shell(cmd=f"test -f kind_workflow/flow.py; tr a-z A-Z < {text} > {shouted}")


@flow
def main():
    return [
        keep(shouted=shout(text=remote_file(GREETING))),
        shout_in_busybox(text=remote_file(GREETING), shouted="busybox.txt"),
        jq_version(out="jq-version.txt"),
    ]
"""

# One small package, so the pod's install is quick.
TOOLS_ENV = """
[workspace]
name = "tools"
channels = ["conda-forge"]
platforms = ["linux-64", "linux-aarch64", "osx-arm64"]

[dependencies]
jq = "*"
"""


def _put_input() -> None:
    """Create the bucket and the task's input in the S3 server."""
    import s3fs

    fs = s3fs.S3FileSystem()
    if not fs.exists("ginkgo"):
        fs.mkdir("ginkgo")
    fs.pipe("ginkgo/inputs/greeting.txt", b"hello from s3")


def test_tasks_run_in_pods_and_their_outputs_come_back(tmp_path: Path) -> None:
    _put_input()
    (tmp_path / "ginkgo.toml").write_text(CONFIG, encoding="utf-8")
    package = tmp_path / "kind_workflow"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "flow.py").write_text(WORKFLOW, encoding="utf-8")
    (package / "envs" / "tools").mkdir(parents=True)
    (package / "envs" / "tools" / "pixi.toml").write_text(TOOLS_ENV, encoding="utf-8")
    run_id = uuid.uuid4().hex
    env = {**os.environ, "GINKGO_PARENT_RUN_ID": run_id}
    # CI runs the driver as a service account that holds only
    # deploy/oke/driver-role.yaml, so a permission missing from it fails here.
    if driver_kubeconfig := os.environ.get("GINKGO_KIND_DRIVER_KUBECONFIG"):
        env["KUBECONFIG"] = driver_kubeconfig

    result = subprocess.run(
        [sys.executable, "-m", "ginkgo.cli", "run", "kind_workflow/flow.py"],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=600,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (tmp_path / "result.txt").read_text() == "HELLO FROM S3"
    assert (tmp_path / "jq-version.txt").read_text().startswith("jq-")
    assert (tmp_path / "busybox.txt").read_text() == "HELLO FROM S3"
    jobs = subprocess.run(
        ["kubectl", "get", "jobs", "-l", f"ginkgo/parent-run-id={run_id}", "-o", "name"],
        text=True,
        capture_output=True,
        check=True,
    )
    assert len(jobs.stdout.split()) == 3, "the tasks never ran as Jobs"
